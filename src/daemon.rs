use crate::executor::{Executor, RunConfig};
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
