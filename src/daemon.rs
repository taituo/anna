use crate::executor::{Executor, HitlHandler, HitlRequest, RunConfig};
use crate::session::session_dir;
use crate::workflow::Workflow;
use anyhow::Result;
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use cron::Schedule;
use humantime::parse_duration;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

#[derive(Clone)]
struct AppState {
    executor: Executor,
    plays_dir: PathBuf,
    sessions: Arc<RwLock<HashMap<String, SessionInfo>>>,
    handles: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    hitl: Arc<RwLock<HashMap<String, HitlPending>>>,
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionInfo {
    id: String,
    status: String,
    workflow: String,
    created_at: u64,
    updated_at: u64,
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

#[derive(Debug, Clone, Serialize)]
struct HitlPending {
    id: String,
    session_id: String,
    workflow: String,
    stage_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    options: Vec<String>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<String>,
    created_at: u64,
}

#[derive(Debug, Deserialize)]
struct HitlResolveBody {
    decision: String,
}

#[derive(Debug, Deserialize, Default)]
struct HitlListQuery {
    status: Option<String>,
    session_id: Option<String>,
    workflow: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct SessionsQuery {
    status: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default)]
struct TriggerScheduler {
    interval_next: HashMap<String, Instant>,
    cron_next: HashMap<String, DateTime<Utc>>,
    watch_snapshots: HashMap<String, HashMap<String, u64>>,
}

#[derive(Clone)]
struct DaemonHitl {
    pending: Arc<RwLock<HashMap<String, HitlPending>>>,
}

#[async_trait]
impl HitlHandler for DaemonHitl {
    async fn await_decision(&self, request: HitlRequest) -> Result<String> {
        let request_id = crate::session::gen_session_id();
        self.pending.write().await.insert(
            request_id.clone(),
            HitlPending {
                id: request_id.clone(),
                session_id: request.session_id,
                workflow: request.workflow,
                stage_id: request.stage_id,
                prompt: request.prompt,
                options: request.options,
                status: "pending".to_string(),
                decision: None,
                created_at: now_unix_secs(),
            },
        );

        loop {
            let state = self.pending.read().await.get(&request_id).cloned();
            let Some(current) = state else {
                return Ok("reject".to_string());
            };
            if let Some(decision) = current.decision {
                return Ok(decision);
            }
            sleep(Duration::from_millis(300)).await;
        }
    }
}

pub async fn run_daemon(bind: &str, plays_dir: PathBuf) -> Result<()> {
    let hitl = Arc::new(RwLock::new(HashMap::<String, HitlPending>::new()));
    let executor = Executor::new().with_hitl_handler(Arc::new(DaemonHitl {
        pending: hitl.clone(),
    }));
    let state = AppState {
        executor,
        plays_dir,
        sessions: Arc::new(RwLock::new(HashMap::new())),
        handles: Arc::new(RwLock::new(HashMap::new())),
        hitl,
        auth_token: daemon_auth_token(),
    };
    tokio::spawn(trigger_scheduler_loop(state.clone()));

    let app = Router::new()
        .route("/health", get(health))
        .route("/sessions", get(list_sessions))
        .route("/workflows", get(list_workflows))
        .route("/workflow", post(start_workflow))
        .route("/workflow/{name}/run", post(run_registered_workflow))
        .route("/workflow/{id}", get(workflow_status).delete(stop_workflow))
        .route("/workflow/{id}/logs", get(workflow_logs))
        .route("/hook/{name}", post(trigger_hook))
        .route("/hitl", get(list_hitl))
        .route("/hitl/{id}/resolve", post(resolve_hitl))
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

async fn list_workflows(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    match find_workflows(&state.plays_dir).await {
        Ok(list) => Json(list).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed listing workflows: {}", err),
        )
            .into_response(),
    }
}

async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionsQuery>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let mut items = state
        .sessions
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    if let Some(filter) = query.status.as_deref() {
        items.retain(|v| status_matches(&v.status, filter));
    }
    items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    if let Some(limit) = query.limit {
        items.truncate(limit);
    }
    Json(items).into_response()
}

async fn start_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
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
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
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
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let sessions = state.sessions.read().await;
    match sessions.get(&id) {
        Some(info) => (StatusCode::OK, Json(info.clone())).into_response(),
        None => (StatusCode::NOT_FOUND, "session not found").into_response(),
    }
}

async fn stop_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
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
        info.updated_at = now_unix_secs();
        return (StatusCode::OK, Json(info.clone())).into_response();
    }

    (StatusCode::NOT_FOUND, "session not found").into_response()
}

async fn trigger_hook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
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
            if let Ok(session_id) = launch_workflow_from_entry(&state, &entry, "webhook").await {
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

async fn workflow_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
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

async fn list_hitl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HitlListQuery>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let mut out = state
        .hitl
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    if let Some(filter) = query.status.as_deref() {
        out.retain(|v| status_matches(&v.status, filter));
    }
    if let Some(session_filter) = query.session_id.as_deref() {
        out.retain(|v| v.session_id == session_filter);
    }
    if let Some(workflow_filter) = query.workflow.as_deref() {
        out.retain(|v| v.workflow.eq_ignore_ascii_case(workflow_filter.trim()));
    }
    out.sort_by_key(|v| v.created_at);
    if let Some(limit) = query.limit {
        out.truncate(limit);
    }
    Json(out).into_response()
}

async fn resolve_hitl(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<HitlResolveBody>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let decision = body.decision.trim().to_string();
    if decision.is_empty() {
        return (StatusCode::BAD_REQUEST, "decision is required").into_response();
    }

    let mut pending = state.hitl.write().await;
    let Some(item) = pending.get_mut(&id) else {
        return (StatusCode::NOT_FOUND, "hitl request not found").into_response();
    };
    item.decision = Some(decision);
    item.status = "resolved".to_string();
    (StatusCode::OK, Json(item.clone())).into_response()
}

async fn ws_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    ws.on_upgrade(move |socket| stream_logs(socket, state, query.id))
}

async fn trigger_scheduler_loop(state: AppState) {
    let mut scheduler = TriggerScheduler::default();
    loop {
        let entries = match find_workflow_entries(&state.plays_dir).await {
            Ok(v) => v,
            Err(err) => {
                eprintln!("anna-rs scheduler: failed to scan workflows: {}", err);
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        run_interval_triggers(&state, &entries, &mut scheduler).await;
        run_cron_triggers(&state, &entries, &mut scheduler).await;
        run_watch_triggers(&state, &entries, &mut scheduler).await;
        sleep(Duration::from_secs(1)).await;
    }
}

async fn run_interval_triggers(
    state: &AppState,
    entries: &[WorkflowEntry],
    scheduler: &mut TriggerScheduler,
) {
    let mut seen = HashSet::new();
    let now = Instant::now();

    for entry in entries {
        let Some(raw_interval) = entry.trigger_interval.as_deref() else {
            continue;
        };
        if raw_interval.trim().is_empty() {
            continue;
        }
        let interval = match parse_duration(raw_interval) {
            Ok(v) => v,
            Err(err) => {
                eprintln!(
                    "anna-rs scheduler: invalid trigger.interval '{}' in '{}': {}",
                    raw_interval,
                    entry.path.display(),
                    err
                );
                continue;
            }
        };

        let key = trigger_key(entry, "interval", raw_interval);
        seen.insert(key.clone());
        let next = scheduler
            .interval_next
            .entry(key)
            .or_insert_with(|| now + interval);
        if now >= *next {
            if let Err(err) = launch_workflow_from_entry(state, entry, "interval").await {
                eprintln!(
                    "anna-rs scheduler: failed launching interval trigger for '{}': {}",
                    entry.path.display(),
                    err
                );
            }
            *next = Instant::now() + interval;
        }
    }

    scheduler.interval_next.retain(|k, _| seen.contains(k));
}

async fn run_cron_triggers(
    state: &AppState,
    entries: &[WorkflowEntry],
    scheduler: &mut TriggerScheduler,
) {
    let mut seen = HashSet::new();
    let now = Utc::now();

    for entry in entries {
        let Some(raw_cron) = entry.trigger_cron.as_deref() else {
            continue;
        };
        if raw_cron.trim().is_empty() {
            continue;
        }

        let schedule = match Schedule::from_str(raw_cron) {
            Ok(v) => v,
            Err(err) => {
                eprintln!(
                    "anna-rs scheduler: invalid trigger.cron '{}' in '{}': {}",
                    raw_cron,
                    entry.path.display(),
                    err
                );
                continue;
            }
        };

        let key = trigger_key(entry, "cron", raw_cron);
        seen.insert(key.clone());
        let next = scheduler.cron_next.entry(key).or_insert_with(|| {
            schedule
                .after(&now)
                .next()
                .unwrap_or_else(|| now + chrono::Duration::days(1))
        });

        if *next <= now {
            if let Err(err) = launch_workflow_from_entry(state, entry, "cron").await {
                eprintln!(
                    "anna-rs scheduler: failed launching cron trigger for '{}': {}",
                    entry.path.display(),
                    err
                );
            }
            *next = schedule
                .after(&now)
                .next()
                .unwrap_or_else(|| now + chrono::Duration::days(1));
        }
    }

    scheduler.cron_next.retain(|k, _| seen.contains(k));
}

async fn run_watch_triggers(
    state: &AppState,
    entries: &[WorkflowEntry],
    scheduler: &mut TriggerScheduler,
) {
    let mut seen = HashSet::new();
    for entry in entries {
        let Some(raw_watch) = entry.trigger_watch.as_deref() else {
            continue;
        };
        if raw_watch.trim().is_empty() {
            continue;
        }

        let base_dir = watch_base_dir(&state.plays_dir, entry);
        let pattern = resolve_watch_pattern(&base_dir, raw_watch);
        if pattern.trim().is_empty() {
            continue;
        }

        let key = trigger_key(entry, "watch", &pattern);
        seen.insert(key.clone());

        let snapshot = match collect_watch_snapshot(&pattern) {
            Ok(v) => v,
            Err(err) => {
                eprintln!(
                    "anna-rs scheduler: invalid trigger.watch '{}' in '{}': {}",
                    raw_watch,
                    entry.path.display(),
                    err
                );
                continue;
            }
        };

        let changed = match scheduler.watch_snapshots.get(&key) {
            None => false,
            Some(previous) => previous != &snapshot,
        };
        scheduler.watch_snapshots.insert(key, snapshot);

        if changed && let Err(err) = launch_workflow_from_entry(state, entry, "watch").await {
            eprintln!(
                "anna-rs scheduler: failed launching watch trigger for '{}': {}",
                entry.path.display(),
                err
            );
        }
    }

    scheduler.watch_snapshots.retain(|k, _| seen.contains(k));
}

fn watch_base_dir(plays_dir: &FsPath, entry: &WorkflowEntry) -> PathBuf {
    match entry.workflow_workdir.as_ref() {
        Some(wd) => {
            let wd_path = PathBuf::from(wd);
            if wd_path.is_absolute() {
                wd_path
            } else {
                plays_dir.join(wd_path)
            }
        }
        None => entry
            .path
            .parent()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| plays_dir.to_path_buf()),
    }
}

fn resolve_watch_pattern(base_dir: &FsPath, raw_pattern: &str) -> String {
    let trimmed = raw_pattern.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let raw_path = PathBuf::from(trimmed);
    if raw_path.is_absolute() {
        return trimmed.to_string();
    }
    if trimmed.contains("**") || trimmed.contains('/') || trimmed.contains('\\') {
        return base_dir.join(trimmed).to_string_lossy().into_owned();
    }

    base_dir
        .join("**")
        .join(trimmed)
        .to_string_lossy()
        .into_owned()
}

fn collect_watch_snapshot(pattern: &str) -> Result<HashMap<String, u64>> {
    let mut snapshot = HashMap::new();
    for item in glob::glob(pattern)? {
        let path = match item {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !path.is_file() {
            continue;
        }
        let metadata = match std::fs::metadata(&path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let modified = metadata
            .modified()
            .ok()
            .and_then(|v| v.duration_since(UNIX_EPOCH).ok())
            .map(|v| v.as_nanos() as u64)
            .unwrap_or(0);
        let fingerprint = modified ^ metadata.len().rotate_left(13);
        snapshot.insert(path.to_string_lossy().into_owned(), fingerprint);
    }
    Ok(snapshot)
}

fn trigger_key(entry: &WorkflowEntry, trigger_kind: &str, trigger_value: &str) -> String {
    format!(
        "{}::{}::{}",
        entry.path.display(),
        trigger_kind,
        trigger_value.trim()
    )
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .unwrap_or(0)
}

fn daemon_auth_token() -> Option<String> {
    std::env::var("ANNA_DAEMON_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn ensure_authorized(state: &AppState, headers: &HeaderMap) -> Option<axum::response::Response> {
    let Some(expected) = state.auth_token.as_ref() else {
        return None;
    };

    if is_authorized(headers, expected) {
        None
    } else {
        Some((StatusCode::UNAUTHORIZED, "unauthorized").into_response())
    }
}

fn is_authorized(headers: &HeaderMap, expected: &str) -> bool {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string());
    if let Some(raw) = auth_header {
        if let Some(token) = raw.strip_prefix("Bearer ") {
            if token.trim() == expected {
                return true;
            }
        }
        if raw == expected {
            return true;
        }
    }

    headers
        .get("x-anna-token")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim() == expected)
        .unwrap_or(false)
}

fn status_matches(status: &str, filter: &str) -> bool {
    status.eq_ignore_ascii_case(filter.trim())
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
    trigger_watch: Option<String>,
    trigger_cron: Option<String>,
    trigger_interval: Option<String>,
    workflow_workdir: Option<String>,
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
            trigger_watch: wf.trigger.watch,
            trigger_cron: wf.trigger.cron,
            trigger_interval: wf.trigger.interval,
            workflow_workdir: wf.workdir,
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

async fn launch_workflow_from_entry(
    state: &AppState,
    entry: &WorkflowEntry,
    trigger_source: &str,
) -> Result<String> {
    let mut wf = Workflow::load(&entry.path)?;
    if wf.workdir.is_none() {
        wf.workdir = Some(state.plays_dir.display().to_string());
    }
    let req_id = launch_workflow(state, wf).await?;
    println!(
        "anna-rs daemon trigger={} workflow='{}' request_id={}",
        trigger_source, entry.workflow_name, req_id
    );
    Ok(req_id)
}

async fn launch_workflow(state: &AppState, workflow: Workflow) -> Result<String> {
    let req_id = crate::session::gen_session_id();
    let runtime_session_id = crate::session::gen_session_id();
    let now = now_unix_secs();
    state.sessions.write().await.insert(
        req_id.clone(),
        SessionInfo {
            id: req_id.clone(),
            status: "running".to_string(),
            workflow: workflow.name.clone(),
            created_at: now,
            updated_at: now,
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
                    info.updated_at = now_unix_secs();
                    info.runtime_session_id = Some(result.session_id);
                    info.outputs = result.outputs;
                    info.errors = result.errors;
                }
                Err(err) => {
                    info.status = "failed".to_string();
                    info.updated_at = now_unix_secs();
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
    use super::{
        DaemonHitl, HitlPending, collect_watch_snapshot, find_workflow_entries, is_authorized,
        resolve_registered_workflow_path, resolve_watch_pattern, status_matches,
    };
    use crate::executor::{HitlHandler, HitlRequest};
    use axum::http::{HeaderMap, HeaderValue};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

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

    #[tokio::test]
    async fn finds_trigger_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "anna-daemon-triggers-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");
        tokio::fs::write(
            dir.join("triggered.anna"),
            "name: trig\ntrigger:\n  interval: 15s\n  cron: \"0/30 * * * * * *\"\n  watch: \"*.rs\"\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let entries = find_workflow_entries(&dir).await.expect("find entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_name, "trig");
        assert_eq!(entries[0].trigger_interval.as_deref(), Some("15s"));
        assert_eq!(entries[0].trigger_cron.as_deref(), Some("0/30 * * * * * *"));
        assert_eq!(entries[0].trigger_watch.as_deref(), Some("*.rs"));
    }

    #[test]
    fn resolve_watch_pattern_defaults_recursive_filename_glob() {
        let root = std::path::PathBuf::from("/tmp/anna-watch");
        let pattern = resolve_watch_pattern(&root, "*.go");
        assert_eq!(pattern, "/tmp/anna-watch/**/*.go");
    }

    #[test]
    fn collect_watch_snapshot_changes_when_file_updates() {
        let dir = std::env::temp_dir().join(format!(
            "anna-watch-snap-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let file = dir.join("x.txt");
        std::fs::write(&file, "a").expect("write first content");

        let pattern = format!("{}/**/*.txt", dir.display());
        let before = collect_watch_snapshot(&pattern).expect("collect before snapshot");
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&file, "bbb").expect("write updated content");
        let after = collect_watch_snapshot(&pattern).expect("collect after snapshot");
        assert_ne!(before, after);
    }

    #[tokio::test]
    async fn daemon_hitl_handler_waits_for_resolution() {
        let pending = Arc::new(RwLock::new(HashMap::<String, HitlPending>::new()));
        let handler = DaemonHitl {
            pending: pending.clone(),
        };

        let waiter = tokio::spawn(async move {
            handler
                .await_decision(HitlRequest {
                    session_id: "sess-x".to_string(),
                    workflow: "wf".to_string(),
                    stage_id: "stage".to_string(),
                    prompt: Some("approve?".to_string()),
                    options: vec!["approve".to_string(), "reject".to_string()],
                })
                .await
                .expect("hitl should resolve")
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let request_id = {
            let read = pending.read().await;
            read.keys()
                .next()
                .cloned()
                .expect("pending hitl request should exist")
        };
        {
            let mut write = pending.write().await;
            let item = write.get_mut(&request_id).expect("pending request by id");
            item.decision = Some("approve".to_string());
            item.status = "resolved".to_string();
        }

        let decision = waiter.await.expect("join waiter");
        assert_eq!(decision, "approve");
    }

    #[test]
    fn auth_accepts_bearer_and_x_anna_token_headers() {
        let mut bearer = HeaderMap::new();
        bearer.insert(
            "authorization",
            HeaderValue::from_static("Bearer secret-token"),
        );
        assert!(is_authorized(&bearer, "secret-token"));

        let mut raw_auth = HeaderMap::new();
        raw_auth.insert("authorization", HeaderValue::from_static("secret-token"));
        assert!(is_authorized(&raw_auth, "secret-token"));

        let mut x_token = HeaderMap::new();
        x_token.insert("x-anna-token", HeaderValue::from_static("secret-token"));
        assert!(is_authorized(&x_token, "secret-token"));
    }

    #[test]
    fn auth_rejects_missing_or_wrong_token() {
        let empty = HeaderMap::new();
        assert!(!is_authorized(&empty, "secret-token"));

        let mut wrong = HeaderMap::new();
        wrong.insert(
            "authorization",
            HeaderValue::from_static("Bearer wrong-token"),
        );
        assert!(!is_authorized(&wrong, "secret-token"));
    }

    #[test]
    fn status_filter_is_case_insensitive_exact_match() {
        assert!(status_matches("running", "RUNNING"));
        assert!(status_matches("failed", " failed "));
        assert!(!status_matches("running", "run"));
    }
}
