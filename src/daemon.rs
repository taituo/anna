use crate::executor::{Executor, RunConfig};
use crate::session::session_dir;
use crate::workflow::Workflow;
use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

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
        .route("/workflow/{id}", get(workflow_status).delete(stop_workflow))
        .route("/workflow/{id}/logs", get(workflow_logs))
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

    let req_id = crate::session::gen_session_id();
    state.sessions.write().await.insert(
        req_id.clone(),
        SessionInfo {
            id: req_id.clone(),
            status: "running".to_string(),
            workflow: workflow.name.clone(),
            runtime_session_id: None,
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

async fn find_workflows(root: &FsPath) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut dir = tokio::fs::read_dir(root).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == "anna")
                .unwrap_or(false)
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            out.push(name.to_string());
        }
    }
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
