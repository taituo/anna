use crate::executor::{Executor, RunConfig};
use crate::session::session_dir;
use crate::workflow::Workflow;
use anyhow::Result;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

#[derive(Clone)]
struct AppState {
    executor: Executor,
    plays_dir: PathBuf,
    sessions: Arc<RwLock<HashMap<String, SessionInfo>>>,
    handles: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    workflow: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_session_id: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    outputs: HashMap<String, String>,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct StartWorkflowResponse {
    id: String,
    status: String,
}

#[derive(Debug, Serialize)]
struct HookTriggerResponse {
    hook: String,
    launched: Vec<HookLaunchedWorkflow>,
}

#[derive(Debug, Serialize)]
struct HookLaunchedWorkflow {
    workflow: String,
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct WsQuery {
    id: String,
}

#[derive(Debug, Serialize)]
struct WsLogFrame {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_session_id: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    logs: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    errors: Vec<String>,
}

pub async fn run_daemon(bind: &str, plays_dir: PathBuf) -> Result<()> {
    let state = AppState {
        executor: Executor::new(),
        plays_dir,
        sessions: Arc::new(RwLock::new(HashMap::new())),
        handles: Arc::new(RwLock::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/workflows", get(list_workflows))
        .route("/workflow", post(start_workflow))
        .route("/workflow/{name}/run", post(run_registered_workflow))
        .route("/workflow/{id}", get(workflow_status).delete(stop_workflow))
        .route("/workflow/{id}/logs", get(workflow_logs))
        .route("/hook/{name}", post(trigger_hook))
        .route("/ws", get(ws_logs))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("anna-rs daemon listening on http://{}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn list_workflows(State(state): State<AppState>) -> impl IntoResponse {
    match find_workflows(&state.plays_dir).await {
        Ok(list) => Json(list).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed listing workflows: {}", err),
        )
            .into_response(),
    }
}

async fn start_workflow(State(state): State<AppState>, body: String) -> impl IntoResponse {
    let mut workflow: Workflow = match serde_yaml::from_str(&body) {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid workflow yaml: {}", err),
            )
                .into_response();
        }
    };
    if let Err(err) = workflow.validate() {
        return (
            StatusCode::BAD_REQUEST,
            format!("workflow validation failed: {}", err),
        )
            .into_response();
    }

    if workflow.workdir.is_none() {
        workflow.workdir = Some(state.plays_dir.display().to_string());
    }

    let req_id = match launch_workflow(&state, workflow).await {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start workflow: {}", err),
            )
                .into_response();
        }
    };
    (
        StatusCode::ACCEPTED,
        Json(StartWorkflowResponse {
            id: req_id,
            status: "running".to_string(),
        }),
    )
        .into_response()
}

async fn run_registered_workflow(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let path = match resolve_registered_workflow_path(&state.plays_dir, &name).await {
        Ok(Some(v)) => v,
        Ok(None) => return (StatusCode::NOT_FOUND, "workflow not found").into_response(),
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed resolving workflow: {}", err),
            )
                .into_response();
        }
    };

    let mut workflow = match Workflow::load(&path) {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid workflow '{}': {}", path.display(), err),
            )
                .into_response();
        }
    };
    if workflow.workdir.is_none() {
        workflow.workdir = Some(state.plays_dir.display().to_string());
    }

    let req_id = match launch_workflow(&state, workflow).await {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start workflow: {}", err),
            )
                .into_response();
        }
    };

    (
        StatusCode::ACCEPTED,
        Json(StartWorkflowResponse {
            id: req_id,
            status: "running".to_string(),
        }),
    )
        .into_response()
}

async fn workflow_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let sessions = state.sessions.read().await;
    match sessions.get(&id) {
        Some(info) => (StatusCode::OK, Json(info.clone())).into_response(),
        None => (StatusCode::NOT_FOUND, "session not found").into_response(),
    }
}

async fn stop_workflow(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let mut handles = state.handles.write().await;
    let stopped = if let Some(handle) = handles.remove(&id) {
        handle.abort();
        true
    } else {
        false
    };
    drop(handles);

    let mut sessions = state.sessions.write().await;
    if let Some(info) = sessions.get_mut(&id) {
        info.status = if stopped { "stopped" } else { "not_running" }.to_string();
        return (StatusCode::OK, Json(info.clone())).into_response();
    }

    (StatusCode::NOT_FOUND, "session not found").into_response()
}

async fn trigger_hook(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let hook_path = format!("/{}", name.trim_matches('/'));
    let entries = match find_workflow_entries(&state.plays_dir).await {
        Ok(v) => v,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed scanning workflows: {}", err),
            )
                .into_response();
        }
    };

    let mut launched = Vec::new();
    for entry in entries {
        if let Some(webhook) = entry.trigger_webhook.as_deref()
            && webhook.trim() == hook_path
        {
            let mut wf = match Workflow::load(&entry.path) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if wf.workdir.is_none() {
                wf.workdir = Some(state.plays_dir.display().to_string());
            }
            if let Ok(session_id) = launch_workflow(&state, wf).await {
                launched.push(HookLaunchedWorkflow {
                    workflow: entry.workflow_name,
                    session_id,
                });
            }
        }
    }

    if launched.is_empty() {
        return (StatusCode::NOT_FOUND, "no workflows for hook").into_response();
    }
    (
        StatusCode::ACCEPTED,
        Json(HookTriggerResponse {
            hook: hook_path,
            launched,
        }),
    )
        .into_response()
}

async fn workflow_logs(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let sessions = state.sessions.read().await;
    let Some(info) = sessions.get(&id) else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };
    let Some(runtime_id) = info.runtime_session_id.clone() else {
        return (StatusCode::CONFLICT, "runtime session not available yet").into_response();
    };
    drop(sessions);

    match read_session_logs(&runtime_id).await {
        Ok(logs) => (StatusCode::OK, Json(logs)).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read logs: {}", err),
        )
            .into_response(),
    }
}

async fn ws_logs(
    State(state): State<AppState>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream_logs(socket, state, query.id))
}

async fn find_workflows(root: &FsPath) -> Result<Vec<String>> {
    let mut out = find_workflow_entries(root)
        .await?
        .into_iter()
        .map(|v| v.file_name)
        .collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

async fn read_session_logs(runtime_session_id: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    let dir_path = session_dir(runtime_session_id);
    let mut dir = tokio::fs::read_dir(&dir_path).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == "log")
                .unwrap_or(false)
            && let Some(stage_id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        {
            let content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
            out.insert(stage_id, content);
        }
    }
    Ok(out)
}

async fn stream_logs(mut socket: WebSocket, state: AppState, req_id: String) {
    let mut last_payload = String::new();

    loop {
        let (payload, should_close) = build_ws_payload(&state, &req_id).await;
        if payload != last_payload {
            if socket
                .send(Message::Text(payload.clone().into()))
                .await
                .is_err()
            {
                return;
            }
            last_payload = payload;
        }

        if should_close {
            return;
        }
        sleep(Duration::from_millis(700)).await;
    }
}

async fn build_ws_payload(state: &AppState, req_id: &str) -> (String, bool) {
    let info = { state.sessions.read().await.get(req_id).cloned() };
    let Some(info) = info else {
        let frame = WsLogFrame {
            id: req_id.to_string(),
            status: "not_found".to_string(),
            runtime_session_id: None,
            logs: HashMap::new(),
            errors: vec!["session not found".to_string()],
        };
        return (to_json(frame), true);
    };

    let logs = match info.runtime_session_id.as_deref() {
        Some(runtime) => read_session_logs(runtime).await.unwrap_or_default(),
        None => HashMap::new(),
    };
    let should_close = matches!(info.status.as_str(), "done" | "failed" | "stopped");
    let frame = WsLogFrame {
        id: info.id,
        status: info.status,
        runtime_session_id: info.runtime_session_id,
        logs,
        errors: info.errors,
    };
    (to_json(frame), should_close)
}

fn to_json(frame: WsLogFrame) -> String {
    serde_json::to_string(&frame).unwrap_or_else(|_| {
        "{\"status\":\"error\",\"errors\":[\"serialization error\"]}".to_string()
    })
}

#[derive(Debug, Clone)]
struct WorkflowEntry {
    file_name: String,
    workflow_name: String,
    path: PathBuf,
    trigger_webhook: Option<String>,
}

async fn find_workflow_entries(root: &FsPath) -> Result<Vec<WorkflowEntry>> {
    let mut out = Vec::new();
    let mut dir = tokio::fs::read_dir(root).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if !path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e == "anna")
            .unwrap_or(false)
        {
            continue;
        }
        let Some(file_name) = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
        else {
            continue;
        };

        let wf = match Workflow::load(&path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        out.push(WorkflowEntry {
            file_name,
            workflow_name: wf.name,
            path,
            trigger_webhook: wf.trigger.webhook,
        });
    }
    Ok(out)
}

async fn resolve_registered_workflow_path(root: &FsPath, name: &str) -> Result<Option<PathBuf>> {
    let normalized = name.trim();
    let entries = find_workflow_entries(root).await?;
    for entry in &entries {
        if entry.file_name == normalized || entry.workflow_name == normalized {
            return Ok(Some(entry.path.clone()));
        }
    }

    if !normalized.ends_with(".anna") {
        let candidate = format!("{}.anna", normalized);
        for entry in &entries {
            if entry.file_name == candidate {
                return Ok(Some(entry.path.clone()));
            }
        }
    }
    Ok(None)
}

async fn launch_workflow(state: &AppState, workflow: Workflow) -> Result<String> {
    let req_id = crate::session::gen_session_id();
    let runtime_session_id = crate::session::gen_session_id();
    state.sessions.write().await.insert(
        req_id.clone(),
        SessionInfo {
            id: req_id.clone(),
            status: "running".to_string(),
            workflow: workflow.name.clone(),
            runtime_session_id: Some(runtime_session_id.clone()),
            outputs: HashMap::new(),
            errors: Vec::new(),
        },
    );

    let state_for_task = state.clone();
    let req_id_for_task = req_id.clone();
    let handle = tokio::spawn(async move {
        let run = state_for_task
            .executor
            .run(
                &workflow,
                RunConfig {
                    max_iterations: None,
                    session_id_override: Some(runtime_session_id.clone()),
                },
            )
            .await;

        let mut sessions = state_for_task.sessions.write().await;
        if let Some(info) = sessions.get_mut(&req_id_for_task) {
            match run {
                Ok(result) => {
                    info.status = "done".to_string();
                    info.runtime_session_id = Some(result.session_id);
                    info.outputs = result.outputs;
                    info.errors = result.errors;
                }
                Err(err) => {
                    info.status = "failed".to_string();
                    info.errors.push(err.to_string());
                }
            }
        }
        drop(sessions);
        state_for_task
            .handles
            .write()
            .await
            .remove(&req_id_for_task);
    });
    state.handles.write().await.insert(req_id.clone(), handle);
    Ok(req_id)
}

#[cfg(test)]
mod tests {
    use super::{find_workflow_entries, resolve_registered_workflow_path};

    #[tokio::test]
    async fn resolves_workflow_by_file_and_name() {
        let dir = std::env::temp_dir().join(format!(
            "anna-daemon-reg-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        let file = dir.join("demo.anna");
        tokio::fs::write(
            &file,
            "name: demo-workflow\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let by_file = resolve_registered_workflow_path(&dir, "demo.anna")
            .await
            .expect("resolve by file");
        assert_eq!(by_file.as_deref(), Some(file.as_path()));

        let by_name = resolve_registered_workflow_path(&dir, "demo-workflow")
            .await
            .expect("resolve by workflow name");
        assert_eq!(by_name.as_deref(), Some(file.as_path()));

        let by_stem = resolve_registered_workflow_path(&dir, "demo")
            .await
            .expect("resolve by stem");
        assert_eq!(by_stem.as_deref(), Some(file.as_path()));
    }

    #[tokio::test]
    async fn finds_webhook_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "anna-daemon-hook-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        tokio::fs::write(
            dir.join("hooked.anna"),
            "name: hooked\ntrigger:\n  webhook: /deploy\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let entries = find_workflow_entries(&dir).await.expect("find entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_name, "hooked");
        assert_eq!(entries[0].trigger_webhook.as_deref(), Some("/deploy"));
    }
}
