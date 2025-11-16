use crate::executor::{Executor, HitlHandler, HitlRequest, RunConfig};
use crate::policy_crypto::{hex_encode, sign_policy_revision_hmac_sha256};
use crate::providers::llm::{active_llm_adapter_name, load_llm_adapter_catalog_from_env};
use crate::session::session_dir;
use crate::workflow::Workflow;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header::{ETAG, IF_MATCH, IF_NONE_MATCH};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use cron::Schedule;
use humantime::parse_duration;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

#[derive(Clone)]
struct AppState {
    executor: Executor,
    plays_dir: PathBuf,
    registry_file: Option<PathBuf>,
    chat_intents: Arc<RwLock<HashMap<String, ChatIntentConfig>>>,
    trigger_lease: Option<TriggerLeaseConfig>,
    trigger_leader_state: Arc<RwLock<TriggerLeaderState>>,
    audit_log: Option<AuditLogConfig>,
    policy_signing_key: Option<String>,
    offline_mode: bool,
    node_capabilities: HashSet<String>,
    allowed_providers: Option<HashSet<String>>,
    owner_policy: OwnerConcurrencyPolicy,
    sessions: Arc<RwLock<HashMap<String, SessionInfo>>>,
    handles: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    hitl: Arc<RwLock<HashMap<String, HitlPending>>>,
    auth_token: Option<String>,
    retention: RetentionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionInfo {
    id: String,
    status: String,
    workflow: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
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
struct StatsResponse {
    sessions_total: usize,
    sessions_running: usize,
    sessions_done: usize,
    sessions_failed: usize,
    sessions_other: usize,
    hitl_total: usize,
    hitl_pending: usize,
    hitl_resolved: usize,
}

#[derive(Debug, Serialize)]
struct PolicyResponse {
    registry_enabled: bool,
    auth_enabled: bool,
    offline_mode: bool,
    policy_revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_signature_algorithm: Option<String>,
    chat_gateway_enabled: bool,
    chat_intents_count: usize,
    trigger_leader_election_enabled: bool,
    trigger_scheduler_leader: bool,
    trigger_scheduler_node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_scheduler_holder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_scheduler_expires_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_lease_file: Option<String>,
    node_capabilities: Vec<String>,
    provider_restriction_enabled: bool,
    allowed_providers: Vec<String>,
    owner_limits: Vec<OwnerLimitEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_default_limit: Option<usize>,
    retention_max_sessions: usize,
    retention_max_hitl: usize,
}

#[derive(Debug, Serialize)]
struct PolicyRevisionResponse {
    policy_revision: String,
    signed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_signature_algorithm: Option<String>,
}

#[derive(Debug, Serialize)]
struct OwnerLimitEntry {
    owner: String,
    max_concurrency: usize,
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
    skipped_running: Vec<String>,
    skipped_capability: Vec<HookSkippedCapability>,
    skipped_provider: Vec<HookSkippedProvider>,
    skipped_concurrency: Vec<HookSkippedConcurrency>,
}

#[derive(Debug, Serialize)]
struct HookLaunchedWorkflow {
    workflow: String,
    session_id: String,
}

#[derive(Debug, Serialize)]
struct HookSkippedCapability {
    workflow: String,
    missing_capabilities: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HookSkippedProvider {
    workflow: String,
    missing_providers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HookSkippedConcurrency {
    workflow: String,
    running: usize,
    max_concurrency: usize,
}

#[derive(Debug, Serialize)]
struct WorkflowMetaResponse {
    id: String,
    workflow: String,
    file: String,
    path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_providers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_concurrency: Option<u32>,
    running: usize,
    concurrency_blocked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_max_concurrency: Option<u32>,
    owner_running: usize,
    owner_concurrency_blocked: bool,
    available: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_providers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_webhook: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_watch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_cron: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_interval: Option<String>,
}

#[derive(Debug, Serialize)]
struct FlowCheckResponse {
    id: String,
    workflow: String,
    file: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    can_run: bool,
    running: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_concurrency: Option<u32>,
    concurrency_blocked: bool,
    owner_running: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_max_concurrency: Option<u32>,
    owner_concurrency_blocked: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_providers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RawFlowCheckResponse {
    workflow: String,
    can_run: bool,
    provider_restriction_enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_providers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_providers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ChatIntentsResponse {
    enabled: bool,
    intents: Vec<ChatIntentRoute>,
}

#[derive(Debug, Serialize)]
struct ChatIntentRoute {
    intent: String,
    workflow: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_callers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    allowed_owners: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_iterations_cap: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct ChatRunRequest {
    #[serde(default)]
    intent: String,
    #[serde(default)]
    vars: HashMap<String, String>,
    #[serde(default)]
    max_iterations: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ChatRunResponse {
    intent: String,
    workflow: String,
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_max_iterations: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ChatIntentCheckResponse {
    intent: String,
    workflow: String,
    can_run: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    caller: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    guardrail_reasons: Vec<String>,
    running: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_max_iterations: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_max_iterations: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_iterations_cap: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_concurrency: Option<u32>,
    concurrency_blocked: bool,
    owner_running: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_max_concurrency: Option<u32>,
    owner_concurrency_blocked: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    missing_providers: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ChatIntentCheckQuery {
    max_iterations: Option<u32>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    owner: Option<String>,
    workflow: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct RunRegisteredOptions {
    #[serde(default)]
    vars: HashMap<String, String>,
    #[serde(default)]
    max_iterations: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct WorkflowsMetaQuery {
    tag: Option<String>,
    owner: Option<String>,
    capability: Option<String>,
    available: Option<bool>,
    limit: Option<usize>,
}

#[derive(Debug, Default)]
struct TriggerScheduler {
    interval_next: HashMap<String, Instant>,
    cron_next: HashMap<String, DateTime<Utc>>,
    watch_snapshots: HashMap<String, HashMap<String, u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DaemonStateSnapshot {
    #[serde(default)]
    sessions: HashMap<String, SessionInfo>,
    #[serde(default)]
    hitl: HashMap<String, HitlPending>,
    #[serde(default)]
    saved_at: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum FlowRegistryDoc {
    Wrapped { flows: Vec<FlowRegistryEntry> },
    List(Vec<FlowRegistryEntry>),
}

#[derive(Debug, Clone, Deserialize)]
struct FlowRegistryEntry {
    flow_id: String,
    path: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ChatIntentDoc {
    Wrapped { intents: Vec<ChatIntentEntry> },
    List(Vec<ChatIntentEntry>),
    Map(HashMap<String, ChatIntentMapValue>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ChatIntentMapValue {
    Workflow(String),
    Config(ChatIntentConfigDoc),
}

#[derive(Debug, Clone, Deserialize)]
struct ChatIntentEntry {
    intent: String,
    #[serde(flatten)]
    config: ChatIntentConfigDoc,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatIntentConfigDoc {
    workflow: String,
    #[serde(default)]
    allowed_callers: Vec<String>,
    #[serde(default)]
    allowed_owners: Vec<String>,
    #[serde(default)]
    required_tags: Vec<String>,
    #[serde(default)]
    max_iterations_cap: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChatIntentConfig {
    workflow: String,
    allowed_callers: Vec<String>,
    allowed_owners: Vec<String>,
    required_tags: Vec<String>,
    max_iterations_cap: Option<u32>,
}

#[derive(Debug, Clone)]
struct ChatIntentGuardrailOutcome {
    reasons: Vec<String>,
    effective_max_iterations: Option<u32>,
}

#[derive(Debug, Clone)]
struct TriggerLeaseConfig {
    node_id: String,
    lease_file: PathBuf,
    ttl_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TriggerLeaseFile {
    holder: String,
    expires_at: u64,
}

#[derive(Debug, Clone)]
struct TriggerLeaderState {
    enabled: bool,
    is_leader: bool,
    node_id: String,
    holder: Option<String>,
    expires_at: Option<u64>,
    lease_file: Option<String>,
}

#[derive(Debug, Clone)]
struct AuditLogConfig {
    path: PathBuf,
    node_id: String,
}

#[derive(Debug, Clone, Copy)]
struct RetentionConfig {
    max_sessions: usize,
    max_hitl: usize,
}

#[derive(Debug, Clone, Default)]
struct OwnerConcurrencyPolicy {
    per_owner: HashMap<String, usize>,
    default_limit: Option<usize>,
}

#[derive(Clone)]
struct DaemonHitl {
    pending: Arc<RwLock<HashMap<String, HitlPending>>>,
    max_hitl: usize,
}

#[async_trait]
impl HitlHandler for DaemonHitl {
    async fn await_decision(&self, request: HitlRequest) -> Result<String> {
        let request_id = crate::session::gen_session_id();
        {
            let mut pending = self.pending.write().await;
            pending.insert(
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
            prune_hitl_in_place(&mut pending, self.max_hitl);
        }

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
    let state_file = daemon_state_file();
    let policy_snapshot_file = daemon_policy_snapshot_file();
    let registry_file = flow_registry_file();
    let node_id = daemon_node_id();
    let trigger_lease = trigger_lease_config(&node_id);
    let audit_log = daemon_audit_log_config(&node_id);
    let policy_signing_key = daemon_policy_signing_key();
    let chat_intents = daemon_chat_intents();
    let chat_reload_interval = chat_intents_reload_interval();
    let retention = daemon_retention_config();
    let node_capabilities = daemon_node_capabilities();
    let owner_policy = daemon_owner_concurrency_policy();
    let (sessions_seed, hitl_seed) = match state_file.as_ref() {
        Some(path) => load_daemon_state(path).await?,
        None => (HashMap::new(), HashMap::new()),
    };

    let hitl = Arc::new(RwLock::new(hitl_seed));
    let executor = Executor::new().with_hitl_handler(Arc::new(DaemonHitl {
        pending: hitl.clone(),
        max_hitl: retention.max_hitl,
    }));
    let offline_mode = executor.offline_mode();
    let allowed_providers = executor.allowed_providers_set();
    let chat_intents_state = Arc::new(RwLock::new(chat_intents.clone()));
    let trigger_leader_state = Arc::new(RwLock::new(initial_trigger_leader_state(
        &node_id,
        trigger_lease.as_ref(),
    )));
    let state = AppState {
        executor,
        plays_dir,
        registry_file: registry_file.clone(),
        chat_intents: chat_intents_state,
        trigger_lease: trigger_lease.clone(),
        trigger_leader_state,
        audit_log: audit_log.clone(),
        policy_signing_key,
        offline_mode,
        node_capabilities: node_capabilities.clone(),
        allowed_providers: allowed_providers.clone(),
        owner_policy: owner_policy.clone(),
        sessions: Arc::new(RwLock::new(sessions_seed)),
        handles: Arc::new(RwLock::new(HashMap::new())),
        hitl: hitl.clone(),
        auth_token: daemon_auth_token(),
        retention,
    };
    tokio::spawn(trigger_scheduler_loop(state.clone()));
    if let Some(path) = state_file {
        println!(
            "anna-rs daemon state persistence enabled at {}",
            path.display()
        );
        tokio::spawn(state_persist_loop(state.clone(), path));
    }
    if let Some(path) = policy_snapshot_file {
        println!(
            "anna-rs policy snapshot persistence enabled at {}",
            path.display()
        );
        tokio::spawn(policy_snapshot_persist_loop(state.clone(), path));
    }
    if let Some(path) = registry_file {
        println!("anna-rs flow registry enabled at {}", path.display());
    }
    if !chat_intents.is_empty() {
        let mut routes = chat_intents
            .iter()
            .map(|(intent, rule)| format!("{}={}", intent, rule.workflow))
            .collect::<Vec<_>>();
        routes.sort();
        println!("anna-rs chat intents: {}", routes.join(","));
    }
    match trigger_lease.as_ref() {
        Some(lease) => {
            println!(
                "anna-rs trigger lease: file={} ttl={}s node_id={}",
                lease.lease_file.display(),
                lease.ttl_sec,
                lease.node_id
            );
        }
        None => {
            println!("anna-rs trigger lease: disabled node_id={}", node_id);
        }
    }
    if let Some(audit) = audit_log.as_ref() {
        println!("anna-rs audit log enabled at {}", audit.path.display());
    }
    if offline_mode {
        println!("anna-rs offline mode enabled (deterministic provider ceiling active)");
    }
    if chat_intents_file().is_some() {
        if let Some(interval) = chat_reload_interval {
            println!(
                "anna-rs chat intents hot reload enabled (interval={}s)",
                interval.as_secs()
            );
            tokio::spawn(chat_intents_reload_loop(state.clone(), interval));
        } else {
            println!("anna-rs chat intents hot reload disabled");
        }
    }
    if !node_capabilities.is_empty() {
        let mut capabilities = node_capabilities.iter().cloned().collect::<Vec<_>>();
        capabilities.sort();
        println!("anna-rs node capabilities: {}", capabilities.join(","));
    }
    if let Some(allowed) = allowed_providers {
        let mut providers = allowed.into_iter().collect::<Vec<_>>();
        providers.sort();
        println!("anna-rs allowed providers policy: {}", providers.join(","));
    }
    if !owner_policy.per_owner.is_empty() || owner_policy.default_limit.is_some() {
        let mut entries = owner_policy
            .per_owner
            .iter()
            .map(|(owner, limit)| format!("{}={}", owner, limit))
            .collect::<Vec<_>>();
        entries.sort();
        if let Some(default_limit) = owner_policy.default_limit {
            entries.push(format!("*={}", default_limit));
        }
        println!("anna-rs owner concurrency policy: {}", entries.join(","));
    }
    let policy_core = build_policy_core(&state).await;
    let (startup_policy_revision, _policy_signature) =
        policy_revision_and_signature(&policy_core, state.policy_signing_key.as_deref());
    emit_audit_event(
        &state,
        "daemon_started",
        json!({
            "bind": bind,
            "registry_enabled": state.registry_file.is_some(),
            "auth_enabled": state.auth_token.is_some(),
            "offline_mode": state.offline_mode,
            "chat_intents_count": chat_intents.len(),
            "trigger_lease_enabled": trigger_lease.is_some(),
            "policy_revision": startup_policy_revision,
            "allowed_providers": sorted_set_values(state.allowed_providers.as_ref()),
            "node_capabilities": sorted_set_values(Some(&state.node_capabilities)),
        }),
    )
    .await;

    let app = Router::new()
        .route("/health", get(health))
        .route("/policy", get(policy))
        .route("/policy/revision", get(policy_revision))
        .route("/policy/snapshot", get(policy_snapshot))
        .route("/llm/adapters", get(llm_adapters))
        .route("/stats", get(stats))
        .route("/sessions", get(list_sessions))
        .route("/workflows", get(list_workflows))
        .route("/workflows/meta", get(list_workflows_meta))
        .route("/workflow", post(start_workflow))
        .route("/workflow/check", post(check_workflow_body))
        .route("/workflow/{name}/check", get(check_registered_workflow))
        .route("/workflow/{name}/run", post(run_registered_workflow))
        .route("/chat/intents", get(list_chat_intents))
        .route("/chat/{intent}/check", get(check_chat_intent))
        .route("/chat/run", post(run_chat_intent))
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

async fn policy(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let policy_core = build_policy_core(&state).await;
    let (policy_revision, policy_signature) =
        policy_revision_and_signature(&policy_core, state.policy_signing_key.as_deref());
    if !if_match_allows(&headers, &policy_revision) {
        return precondition_failed_with_etag(&policy_revision);
    }
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }

    let mut node_capabilities = state.node_capabilities.iter().cloned().collect::<Vec<_>>();
    node_capabilities.sort();
    let mut allowed_providers = state
        .allowed_providers
        .as_ref()
        .map(|set| set.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    allowed_providers.sort();
    let mut owner_limits = state
        .owner_policy
        .per_owner
        .iter()
        .map(|(owner, max_concurrency)| OwnerLimitEntry {
            owner: owner.clone(),
            max_concurrency: *max_concurrency,
        })
        .collect::<Vec<_>>();
    owner_limits.sort_by(|a, b| a.owner.cmp(&b.owner));

    let chat_intents_count = state.chat_intents.read().await.len();
    let trigger_leader_state = state.trigger_leader_state.read().await.clone();
    let etag_revision = policy_revision.clone();

    let mut response = Json(PolicyResponse {
        registry_enabled: state.registry_file.is_some(),
        auth_enabled: state.auth_token.is_some(),
        offline_mode: state.offline_mode,
        policy_revision,
        policy_signature,
        policy_signature_algorithm: state
            .policy_signing_key
            .as_ref()
            .map(|_| "hmac-sha256".to_string()),
        chat_gateway_enabled: chat_intents_count > 0,
        chat_intents_count,
        trigger_leader_election_enabled: trigger_leader_state.enabled,
        trigger_scheduler_leader: trigger_leader_state.is_leader,
        trigger_scheduler_node_id: trigger_leader_state.node_id,
        trigger_scheduler_holder: trigger_leader_state.holder,
        trigger_scheduler_expires_at: trigger_leader_state.expires_at,
        trigger_lease_file: trigger_leader_state.lease_file,
        node_capabilities,
        provider_restriction_enabled: state.allowed_providers.is_some(),
        allowed_providers,
        owner_limits,
        owner_default_limit: state.owner_policy.default_limit,
        retention_max_sessions: state.retention.max_sessions,
        retention_max_hitl: state.retention.max_hitl,
    })
    .into_response();
    set_etag_header(&mut response, &etag_revision);
    response
}

async fn policy_revision(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let policy_core = build_policy_core(&state).await;
    let (policy_revision, policy_signature) =
        policy_revision_and_signature(&policy_core, state.policy_signing_key.as_deref());
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }
    let etag_revision = policy_revision.clone();
    let mut response = Json(PolicyRevisionResponse {
        policy_revision,
        signed: policy_signature.is_some(),
        policy_signature,
        policy_signature_algorithm: state
            .policy_signing_key
            .as_ref()
            .map(|_| "hmac-sha256".to_string()),
    })
    .into_response();
    set_etag_header(&mut response, &etag_revision);
    response
}

async fn policy_snapshot(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let policy_core = build_policy_core(&state).await;
    let (policy_revision, _policy_signature) =
        policy_revision_and_signature(&policy_core, state.policy_signing_key.as_deref());
    if !if_match_allows(&headers, &policy_revision) {
        return precondition_failed_with_etag(&policy_revision);
    }
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }
    let mut response = Json(build_policy_snapshot(&state).await).into_response();
    set_etag_header(&mut response, &policy_revision);
    response
}

async fn llm_adapters(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    match load_llm_adapter_catalog_from_env() {
        Ok(Some(loaded)) => Json(json!({
            "configured": true,
            "source": loaded.path,
            "selected": active_llm_adapter_name(Some(&loaded.catalog)),
            "default": loaded.catalog.default,
            "adapters": loaded.catalog.adapters,
        }))
        .into_response(),
        Ok(None) => Json(json!({
            "configured": false,
            "source": null,
            "selected": null,
            "default": null,
            "adapters": {},
            "note": "set ANNA_LLM_ADAPTERS_FILE to enable adapter catalog"
        }))
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed reading llm adapter catalog: {}", err),
        )
            .into_response(),
    }
}

async fn stats(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let sessions = state.sessions.read().await;
    let hitl = state.hitl.read().await;

    let sessions_total = sessions.len();
    let sessions_running = sessions.values().filter(|v| v.status == "running").count();
    let sessions_done = sessions.values().filter(|v| v.status == "done").count();
    let sessions_failed = sessions.values().filter(|v| v.status == "failed").count();
    let sessions_other =
        sessions_total.saturating_sub(sessions_running + sessions_done + sessions_failed);

    let hitl_total = hitl.len();
    let hitl_pending = hitl.values().filter(|v| v.status == "pending").count();
    let hitl_resolved = hitl.values().filter(|v| v.status == "resolved").count();

    Json(StatsResponse {
        sessions_total,
        sessions_running,
        sessions_done,
        sessions_failed,
        sessions_other,
        hitl_total,
        hitl_pending,
        hitl_resolved,
    })
    .into_response()
}

async fn list_workflows(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    match find_workflows_with_registry(&state.plays_dir, state.registry_file.as_deref()).await {
        Ok(list) => Json(list).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed listing workflows: {}", err),
        )
            .into_response(),
    }
}

async fn list_workflows_meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WorkflowsMetaQuery>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let entries =
        match find_workflow_entries_with_registry(&state.plays_dir, state.registry_file.as_deref())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed listing workflows: {}", err),
                )
                    .into_response();
            }
        };
    let tag_filter = query.tag.as_deref().map(|v| v.trim().to_ascii_lowercase());
    let owner_filter = query
        .owner
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase());
    let capability_filter = query
        .capability
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase());
    let available_filter = query.available;
    let sessions = state.sessions.read().await;
    let (running_by_workflow, running_by_owner) = build_running_indexes(&sessions);
    drop(sessions);

    let mut out = entries
        .into_iter()
        .map(|entry| {
            let running = running_by_workflow
                .get(&entry.workflow_name)
                .copied()
                .unwrap_or(0);
            let owner_running = owner_key(entry.owner.as_deref())
                .and_then(|key| running_by_owner.get(&key).copied())
                .unwrap_or(0);
            let owner_max_concurrency =
                owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
            let readiness = evaluate_flow_readiness(
                &entry,
                &state.node_capabilities,
                state.allowed_providers.as_ref(),
                running,
                None,
                owner_running,
                owner_max_concurrency,
            );
            WorkflowMetaResponse {
                id: workflow_public_id(&entry),
                workflow: entry.workflow_name,
                file: entry.file_name,
                path: entry.path.display().to_string(),
                tags: entry.tags,
                required_capabilities: entry.required_capabilities,
                required_providers: entry.required_providers,
                owner: entry.owner,
                version: entry.version,
                max_concurrency: entry.max_concurrency,
                running: readiness.running,
                concurrency_blocked: readiness.concurrency_blocked,
                owner_max_concurrency: readiness.owner_max_concurrency.map(|v| v as u32),
                owner_running: readiness.owner_running,
                owner_concurrency_blocked: readiness.owner_concurrency_blocked,
                available: readiness.can_run(),
                missing_capabilities: readiness.missing_capabilities,
                missing_providers: readiness.missing_providers,
                trigger_webhook: entry.trigger_webhook,
                trigger_watch: entry.trigger_watch,
                trigger_cron: entry.trigger_cron,
                trigger_interval: entry.trigger_interval,
            }
        })
        .filter(|item| {
            matches_workflow_meta_filters(
                item,
                tag_filter.as_deref(),
                owner_filter.as_deref(),
                capability_filter.as_deref(),
                available_filter,
            )
        })
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.id.cmp(&b.id));
    if let Some(limit) = query.limit {
        out.truncate(limit);
    }
    Json(out).into_response()
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
    if let Some(owner_filter) = query.owner.as_deref() {
        let owner_filter = owner_filter.trim();
        items.retain(|v| {
            v.owner
                .as_deref()
                .map(|owner| owner.eq_ignore_ascii_case(owner_filter))
                .unwrap_or(false)
        });
    }
    if let Some(workflow_filter) = query.workflow.as_deref() {
        let workflow_filter = workflow_filter.trim();
        items.retain(|v| v.workflow.eq_ignore_ascii_case(workflow_filter));
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
    if let Some(allowed) = state.allowed_providers.as_ref() {
        let required = collect_required_providers(&workflow);
        let mut missing = required
            .into_iter()
            .filter(|provider| !allowed.contains(provider))
            .collect::<Vec<_>>();
        missing.sort();
        missing.dedup();
        if !missing.is_empty() {
            return (
                StatusCode::FORBIDDEN,
                format!(
                    "workflow requires blocked providers: {}",
                    missing.join(", ")
                ),
            )
                .into_response();
        }
    }

    let req_id = match launch_workflow(&state, workflow, None, None, "api_workflow_body").await {
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

async fn check_workflow_body(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let workflow: Workflow = match serde_yaml::from_str(&body) {
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

    let mut required_providers = collect_required_providers(&workflow);
    let mut missing_providers = state
        .allowed_providers
        .as_ref()
        .map(|allowed| {
            required_providers
                .iter()
                .filter(|provider| !allowed.contains(*provider))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    required_providers.sort();
    required_providers.dedup();
    missing_providers.sort();
    missing_providers.dedup();

    (
        StatusCode::OK,
        Json(RawFlowCheckResponse {
            workflow: workflow.name,
            can_run: missing_providers.is_empty(),
            provider_restriction_enabled: state.allowed_providers.is_some(),
            required_providers,
            missing_providers,
        }),
    )
        .into_response()
}

async fn run_registered_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let options = match parse_run_registered_options(&body) {
        Ok(v) => v,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    let entry = match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        &name,
    )
    .await
    {
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
    let req_id = match launch_registered_entry_with_options(
        &state,
        &entry,
        &name,
        options,
        "api_workflow_named",
    )
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
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

async fn list_chat_intents(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let chat_intents = state.chat_intents.read().await.clone();
    let mut intents = chat_intents
        .iter()
        .map(|(intent, rule)| ChatIntentRoute {
            intent: intent.clone(),
            workflow: rule.workflow.clone(),
            allowed_callers: rule.allowed_callers.clone(),
            allowed_owners: rule.allowed_owners.clone(),
            required_tags: rule.required_tags.clone(),
            max_iterations_cap: rule.max_iterations_cap,
        })
        .collect::<Vec<_>>();
    intents.sort_by(|a, b| a.intent.cmp(&b.intent));

    (
        StatusCode::OK,
        Json(ChatIntentsResponse {
            enabled: !intents.is_empty(),
            intents,
        }),
    )
        .into_response()
}

async fn check_chat_intent(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(intent): Path<String>,
    Query(query): Query<ChatIntentCheckQuery>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let caller = request_caller(&headers);
    let chat_intents = state.chat_intents.read().await.clone();
    if chat_intents.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "chat gateway disabled: set ANNA_CHAT_INTENTS or ANNA_CHAT_INTENTS_FILE",
        )
            .into_response();
    }

    let normalized_intent = intent.trim().to_ascii_lowercase();
    let Some(rule) = chat_intents.get(&normalized_intent).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            format!("chat intent '{}' is not configured", intent),
        )
            .into_response();
    };

    let entry = match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        &rule.workflow,
    )
    .await
    {
        Ok(Some(v)) => v,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!(
                    "chat intent '{}' maps to missing workflow '{}'",
                    intent, rule.workflow
                ),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed resolving chat intent workflow: {}", err),
            )
                .into_response();
        }
    };
    let guardrails =
        evaluate_chat_intent_guardrails(&rule, &entry, query.max_iterations, caller.as_deref());

    let (running, owner_running) = running_counts_for_entry(&state, &entry).await;
    let owner_max_concurrency = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness = evaluate_flow_readiness(
        &entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        running,
        None,
        owner_running,
        owner_max_concurrency,
    );
    let can_run = readiness.can_run() && guardrails.reasons.is_empty();
    (
        StatusCode::OK,
        Json(ChatIntentCheckResponse {
            intent: normalized_intent,
            workflow: entry.workflow_name,
            can_run,
            caller,
            guardrail_reasons: guardrails.reasons,
            running: readiness.running,
            requested_max_iterations: query.max_iterations,
            effective_max_iterations: guardrails.effective_max_iterations,
            max_iterations_cap: rule.max_iterations_cap,
            max_concurrency: readiness.max_concurrency.map(|v| v as u32),
            concurrency_blocked: readiness.concurrency_blocked,
            owner_running: readiness.owner_running,
            owner_max_concurrency: readiness.owner_max_concurrency.map(|v| v as u32),
            owner_concurrency_blocked: readiness.owner_concurrency_blocked,
            missing_capabilities: readiness.missing_capabilities,
            missing_providers: readiness.missing_providers,
        }),
    )
        .into_response()
}

async fn run_chat_intent(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let caller = request_caller(&headers);

    let chat_intents = state.chat_intents.read().await.clone();
    if chat_intents.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "chat gateway disabled: set ANNA_CHAT_INTENTS or ANNA_CHAT_INTENTS_FILE",
        )
            .into_response();
    }

    let request = match parse_chat_run_request(&body) {
        Ok(v) => v,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    let intent = request.intent.trim().to_ascii_lowercase();
    let Some(rule) = chat_intents.get(&intent).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            format!("chat intent '{}' is not configured", request.intent),
        )
            .into_response();
    };

    let entry = match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        &rule.workflow,
    )
    .await
    {
        Ok(Some(v)) => v,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!(
                    "chat intent '{}' maps to missing workflow '{}'",
                    request.intent, rule.workflow
                ),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed resolving chat intent workflow: {}", err),
            )
                .into_response();
        }
    };
    let guardrails =
        evaluate_chat_intent_guardrails(&rule, &entry, request.max_iterations, caller.as_deref());
    if !guardrails.reasons.is_empty() {
        emit_audit_event(
            &state,
            "chat_intent_blocked",
            json!({
                "intent": intent.clone(),
                "workflow": entry.workflow_name.clone(),
                "caller": caller.clone(),
                "reasons": guardrails.reasons.clone(),
                "requested_max_iterations": request.max_iterations,
            }),
        )
        .await;
        return (
            StatusCode::FORBIDDEN,
            format!(
                "chat intent '{}' blocked by guardrails: {}",
                request.intent,
                guardrails.reasons.join("; ")
            ),
        )
            .into_response();
    }

    let workflow = entry.workflow_name.clone();
    let options = RunRegisteredOptions {
        vars: request.vars,
        max_iterations: guardrails.effective_max_iterations,
    };
    let launch_source = format!("chat_intent:{}", intent);
    let req_id = match launch_registered_entry_with_options(
        &state,
        &entry,
        &rule.workflow,
        options,
        &launch_source,
    )
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    emit_audit_event(
        &state,
        "chat_intent_launched",
        json!({
            "intent": intent.clone(),
            "workflow": workflow.clone(),
            "caller": caller.clone(),
            "request_id": req_id.clone(),
            "effective_max_iterations": guardrails.effective_max_iterations,
        }),
    )
    .await;

    (
        StatusCode::ACCEPTED,
        Json(ChatRunResponse {
            intent,
            workflow,
            id: req_id,
            status: "running".to_string(),
            effective_max_iterations: guardrails.effective_max_iterations,
        }),
    )
        .into_response()
}

async fn check_registered_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let entry = match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        &name,
    )
    .await
    {
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

    let (running, owner_running) = running_counts_for_entry(&state, &entry).await;
    let owner_max_concurrency = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness = evaluate_flow_readiness(
        &entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        running,
        None,
        owner_running,
        owner_max_concurrency,
    );
    (
        StatusCode::OK,
        Json(FlowCheckResponse {
            id: workflow_public_id(&entry),
            workflow: entry.workflow_name,
            file: entry.file_name,
            path: entry.path.display().to_string(),
            owner: entry.owner,
            can_run: readiness.can_run(),
            running: readiness.running,
            max_concurrency: readiness.max_concurrency.map(|v| v as u32),
            concurrency_blocked: readiness.concurrency_blocked,
            owner_running: readiness.owner_running,
            owner_max_concurrency: readiness.owner_max_concurrency.map(|v| v as u32),
            owner_concurrency_blocked: readiness.owner_concurrency_blocked,
            missing_capabilities: readiness.missing_capabilities,
            missing_providers: readiness.missing_providers,
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
    if sessions.contains_key(&id) {
        let updated = {
            let info = sessions
                .get_mut(&id)
                .expect("session exists after contains_key check");
            info.status = if stopped { "stopped" } else { "not_running" }.to_string();
            info.updated_at = now_unix_secs();
            info.clone()
        };
        prune_sessions_in_place(&mut sessions, state.retention.max_sessions);
        drop(sessions);
        emit_audit_event(
            &state,
            "workflow_stop",
            json!({
                "request_id": updated.id,
                "workflow": updated.workflow,
                "status": updated.status,
                "stopped_task": stopped,
            }),
        )
        .await;
        return (StatusCode::OK, Json(updated)).into_response();
    }
    drop(sessions);

    emit_audit_event(
        &state,
        "workflow_stop_not_found",
        json!({
            "request_id": id,
        }),
    )
    .await;

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
    let leader_state = state.trigger_leader_state.read().await.clone();
    if leader_state.enabled && !leader_state.is_leader {
        emit_audit_event(
            &state,
            "hook_rejected_not_leader",
            json!({
                "hook": hook_path.clone(),
                "node_id": leader_state.node_id.clone(),
                "leader": leader_state.holder.clone(),
                "expires_at": leader_state.expires_at,
            }),
        )
        .await;
        return (
            StatusCode::CONFLICT,
            format!(
                "hook '{}' rejected on follower node '{}' (leader='{}')",
                hook_path,
                leader_state.node_id,
                leader_state
                    .holder
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            ),
        )
            .into_response();
    }
    let entries =
        match find_workflow_entries_with_registry(&state.plays_dir, state.registry_file.as_deref())
            .await
        {
            Ok(v) => v,
            Err(err) => {
                emit_audit_event(
                    &state,
                    "hook_scan_failed",
                    json!({
                        "hook": hook_path.clone(),
                        "error": err.to_string(),
                    }),
                )
                .await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed scanning workflows: {}", err),
                )
                    .into_response();
            }
        };

    let mut launched = Vec::new();
    let mut skipped_running = Vec::new();
    let mut skipped_capability = Vec::new();
    let mut skipped_provider = Vec::new();
    let mut skipped_concurrency = Vec::new();
    for entry in entries {
        if let Some(webhook) = entry.trigger_webhook.as_deref()
            && webhook.trim() == hook_path
        {
            match launch_workflow_from_entry(&state, &entry, "webhook").await {
                Ok(TriggerLaunchOutcome::Launched(session_id)) => {
                    launched.push(HookLaunchedWorkflow {
                        workflow: entry.workflow_name,
                        session_id,
                    })
                }
                Ok(TriggerLaunchOutcome::SkippedRunning) => {
                    skipped_running.push(entry.workflow_name)
                }
                Ok(TriggerLaunchOutcome::SkippedCapability(missing_capabilities)) => {
                    skipped_capability.push(HookSkippedCapability {
                        workflow: entry.workflow_name,
                        missing_capabilities,
                    });
                }
                Ok(TriggerLaunchOutcome::SkippedProvider(missing_providers)) => {
                    skipped_provider.push(HookSkippedProvider {
                        workflow: entry.workflow_name,
                        missing_providers,
                    });
                }
                Ok(TriggerLaunchOutcome::SkippedConcurrency {
                    running,
                    max_concurrency,
                }) => {
                    skipped_concurrency.push(HookSkippedConcurrency {
                        workflow: entry.workflow_name,
                        running,
                        max_concurrency,
                    });
                }
                Err(_) => {}
            }
        }
    }

    if launched.is_empty()
        && skipped_running.is_empty()
        && skipped_capability.is_empty()
        && skipped_provider.is_empty()
        && skipped_concurrency.is_empty()
    {
        emit_audit_event(
            &state,
            "hook_no_match",
            json!({
                "hook": hook_path.clone(),
            }),
        )
        .await;
        return (StatusCode::NOT_FOUND, "no workflows for hook").into_response();
    }
    emit_audit_event(
        &state,
        "hook_triggered",
        json!({
            "hook": hook_path.clone(),
            "launched": launched.len(),
            "skipped_running": skipped_running.len(),
            "skipped_capability": skipped_capability.len(),
            "skipped_provider": skipped_provider.len(),
            "skipped_concurrency": skipped_concurrency.len(),
        }),
    )
    .await;
    (
        StatusCode::ACCEPTED,
        Json(HookTriggerResponse {
            hook: hook_path,
            launched,
            skipped_running,
            skipped_capability,
            skipped_provider,
            skipped_concurrency,
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
    if !pending.contains_key(&id) {
        return (StatusCode::NOT_FOUND, "hitl request not found").into_response();
    }
    let updated = {
        let item = pending
            .get_mut(&id)
            .expect("hitl request exists after contains_key check");
        item.decision = Some(decision);
        item.status = "resolved".to_string();
        item.clone()
    };
    prune_hitl_in_place(&mut pending, state.retention.max_hitl);
    drop(pending);
    emit_audit_event(
        &state,
        "hitl_resolved",
        json!({
            "hitl_id": updated.id,
            "session_id": updated.session_id,
            "workflow": updated.workflow,
            "stage_id": updated.stage_id,
            "decision": updated.decision,
        }),
    )
    .await;
    (StatusCode::OK, Json(updated)).into_response()
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
        if !scheduler_should_run(&state).await {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let entries = match find_workflow_entries_with_registry(
            &state.plays_dir,
            state.registry_file.as_deref(),
        )
        .await
        {
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

async fn scheduler_should_run(state: &AppState) -> bool {
    let previous = state.trigger_leader_state.read().await.clone();

    let Some(config) = state.trigger_lease.as_ref() else {
        let next = TriggerLeaderState {
            enabled: false,
            is_leader: true,
            node_id: previous.node_id.clone(),
            holder: Some(previous.node_id.clone()),
            expires_at: None,
            lease_file: None,
        };
        if trigger_leader_state_changed(&previous, &next) {
            emit_audit_event(
                state,
                trigger_leader_transition_event(&previous, &next),
                json!({
                    "previous": {
                        "enabled": previous.enabled,
                        "is_leader": previous.is_leader,
                        "node_id": previous.node_id.clone(),
                        "holder": previous.holder.clone(),
                        "expires_at": previous.expires_at,
                        "lease_file": previous.lease_file.clone(),
                    },
                    "next": {
                        "enabled": next.enabled,
                        "is_leader": next.is_leader,
                        "node_id": next.node_id.clone(),
                        "holder": next.holder.clone(),
                        "expires_at": next.expires_at,
                        "lease_file": next.lease_file.clone(),
                    },
                }),
            )
            .await;
        }
        *state.trigger_leader_state.write().await = next;
        return true;
    };

    let snapshot = match resolve_trigger_leadership(config).await {
        Ok(v) => v,
        Err(err) => {
            eprintln!(
                "anna-rs scheduler: trigger lease refresh failed for '{}': {}",
                config.lease_file.display(),
                err
            );
            emit_audit_event(
                state,
                "trigger_leader_refresh_failed",
                json!({
                    "node_id": config.node_id,
                    "lease_file": config.lease_file.display().to_string(),
                    "error": err.to_string(),
                }),
            )
            .await;
            TriggerLeaderState {
                enabled: true,
                is_leader: false,
                node_id: config.node_id.clone(),
                holder: None,
                expires_at: None,
                lease_file: Some(config.lease_file.display().to_string()),
            }
        }
    };

    if trigger_leader_state_changed(&previous, &snapshot) {
        emit_audit_event(
            state,
            trigger_leader_transition_event(&previous, &snapshot),
            json!({
                "previous": {
                    "enabled": previous.enabled,
                    "is_leader": previous.is_leader,
                    "node_id": previous.node_id.clone(),
                    "holder": previous.holder.clone(),
                    "expires_at": previous.expires_at,
                    "lease_file": previous.lease_file.clone(),
                },
                "next": {
                    "enabled": snapshot.enabled,
                    "is_leader": snapshot.is_leader,
                    "node_id": snapshot.node_id.clone(),
                    "holder": snapshot.holder.clone(),
                    "expires_at": snapshot.expires_at,
                    "lease_file": snapshot.lease_file.clone(),
                },
            }),
        )
        .await;
    }

    let is_leader = snapshot.is_leader;
    *state.trigger_leader_state.write().await = snapshot;
    is_leader
}

fn trigger_leader_state_changed(previous: &TriggerLeaderState, next: &TriggerLeaderState) -> bool {
    previous.enabled != next.enabled
        || previous.is_leader != next.is_leader
        || previous.holder != next.holder
}

fn trigger_leader_transition_event(
    previous: &TriggerLeaderState,
    next: &TriggerLeaderState,
) -> &'static str {
    if !previous.is_leader && next.is_leader {
        "trigger_leader_acquired"
    } else if previous.is_leader && !next.is_leader {
        "trigger_leader_lost"
    } else if !previous.enabled && next.enabled {
        "trigger_leader_enabled"
    } else if previous.enabled && !next.enabled {
        "trigger_leader_disabled"
    } else {
        "trigger_leader_updated"
    }
}

async fn resolve_trigger_leadership(config: &TriggerLeaseConfig) -> Result<TriggerLeaderState> {
    let now = now_unix_secs();
    if let Some(current) = read_trigger_lease_file(&config.lease_file).await?
        && current.holder != config.node_id
        && current.expires_at > now
    {
        return Ok(TriggerLeaderState {
            enabled: true,
            is_leader: false,
            node_id: config.node_id.clone(),
            holder: Some(current.holder),
            expires_at: Some(current.expires_at),
            lease_file: Some(config.lease_file.display().to_string()),
        });
    }

    let candidate = TriggerLeaseFile {
        holder: config.node_id.clone(),
        expires_at: now.saturating_add(config.ttl_sec),
    };
    write_trigger_lease_file(&config.lease_file, &candidate).await?;

    let observed = read_trigger_lease_file(&config.lease_file).await?;
    let is_leader = observed
        .as_ref()
        .map(|lease| lease.holder == config.node_id && lease.expires_at > now)
        .unwrap_or(false);
    Ok(TriggerLeaderState {
        enabled: true,
        is_leader,
        node_id: config.node_id.clone(),
        holder: observed.as_ref().map(|lease| lease.holder.clone()),
        expires_at: observed.as_ref().map(|lease| lease.expires_at),
        lease_file: Some(config.lease_file.display().to_string()),
    })
}

async fn read_trigger_lease_file(path: &FsPath) -> Result<Option<TriggerLeaseFile>> {
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => match serde_json::from_str::<TriggerLeaseFile>(&raw) {
            Ok(v) => Ok(Some(v)),
            Err(err) => {
                eprintln!(
                    "anna-rs scheduler: ignoring invalid trigger lease file '{}': {}",
                    path.display(),
                    err
                );
                Ok(None)
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err)
            .with_context(|| format!("failed reading trigger lease file '{}'", path.display())),
    }
}

async fn write_trigger_lease_file(path: &FsPath, lease: &TriggerLeaseFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating trigger lease parent directory '{}'",
                parent.display()
            )
        })?;
    }

    let tmp_name = format!(
        "{}.{}.tmp",
        path.file_name().and_then(|v| v.to_str()).unwrap_or("lease"),
        crate::session::gen_session_id()
    );
    let tmp_path = path.with_file_name(tmp_name);
    let raw = serde_json::to_string(lease)?;
    tokio::fs::write(&tmp_path, raw)
        .await
        .with_context(|| format!("failed writing trigger lease temp '{}'", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path).await.with_context(|| {
        format!(
            "failed moving trigger lease '{}' -> '{}'",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
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
            match launch_workflow_from_entry(state, entry, "interval").await {
                Ok(TriggerLaunchOutcome::Launched(_))
                | Ok(TriggerLaunchOutcome::SkippedRunning) => {}
                Ok(TriggerLaunchOutcome::SkippedCapability(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped interval trigger '{}' due to missing capabilities: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedProvider(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped interval trigger '{}' due to blocked providers: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedConcurrency {
                    running,
                    max_concurrency,
                }) => {
                    eprintln!(
                        "anna-rs scheduler: skipped interval trigger '{}' due to concurrency limit running={} max={}",
                        entry.workflow_name, running, max_concurrency
                    );
                }
                Err(err) => {
                    eprintln!(
                        "anna-rs scheduler: failed launching interval trigger for '{}': {}",
                        entry.path.display(),
                        err
                    );
                }
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
            match launch_workflow_from_entry(state, entry, "cron").await {
                Ok(TriggerLaunchOutcome::Launched(_))
                | Ok(TriggerLaunchOutcome::SkippedRunning) => {}
                Ok(TriggerLaunchOutcome::SkippedCapability(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped cron trigger '{}' due to missing capabilities: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedProvider(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped cron trigger '{}' due to blocked providers: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedConcurrency {
                    running,
                    max_concurrency,
                }) => {
                    eprintln!(
                        "anna-rs scheduler: skipped cron trigger '{}' due to concurrency limit running={} max={}",
                        entry.workflow_name, running, max_concurrency
                    );
                }
                Err(err) => {
                    eprintln!(
                        "anna-rs scheduler: failed launching cron trigger for '{}': {}",
                        entry.path.display(),
                        err
                    );
                }
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

        if changed {
            match launch_workflow_from_entry(state, entry, "watch").await {
                Ok(TriggerLaunchOutcome::Launched(_))
                | Ok(TriggerLaunchOutcome::SkippedRunning) => {}
                Ok(TriggerLaunchOutcome::SkippedCapability(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped watch trigger '{}' due to missing capabilities: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedProvider(missing)) => {
                    eprintln!(
                        "anna-rs scheduler: skipped watch trigger '{}' due to blocked providers: {}",
                        entry.workflow_name,
                        missing.join(", ")
                    );
                }
                Ok(TriggerLaunchOutcome::SkippedConcurrency {
                    running,
                    max_concurrency,
                }) => {
                    eprintln!(
                        "anna-rs scheduler: skipped watch trigger '{}' due to concurrency limit running={} max={}",
                        entry.workflow_name, running, max_concurrency
                    );
                }
                Err(err) => {
                    eprintln!(
                        "anna-rs scheduler: failed launching watch trigger for '{}': {}",
                        entry.path.display(),
                        err
                    );
                }
            }
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

fn missing_required_capabilities(
    entry: &WorkflowEntry,
    node_capabilities: &HashSet<String>,
) -> Vec<String> {
    if entry.required_capabilities.is_empty() {
        return Vec::new();
    }
    if node_capabilities.contains("*") || node_capabilities.contains("all") {
        return Vec::new();
    }

    let mut missing = entry
        .required_capabilities
        .iter()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .filter(|required| !node_capabilities.contains(&required.to_ascii_lowercase()))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    missing.sort();
    missing.dedup();
    missing
}

fn collect_required_providers(workflow: &Workflow) -> Vec<String> {
    let mut providers = HashSet::new();
    for stage in &workflow.stages {
        if stage.workflow.is_none() {
            providers.insert(stage.provider_name().trim().to_ascii_lowercase());
        }
        if stage.vote.is_some() {
            providers.insert("llm".to_string());
        }
        if stage
            .before
            .as_deref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
            || stage
                .after
                .as_deref()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
            || stage
                .on_error
                .as_deref()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false)
        {
            providers.insert("shell".to_string());
        }
    }
    let mut out = providers
        .into_iter()
        .filter(|v| !v.trim().is_empty())
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn missing_required_providers(
    entry: &WorkflowEntry,
    allowed_providers: Option<&HashSet<String>>,
) -> Vec<String> {
    let Some(allowed) = allowed_providers else {
        return Vec::new();
    };

    let mut missing = entry
        .required_providers
        .iter()
        .map(|provider| provider.trim().to_ascii_lowercase())
        .filter(|provider| !provider.is_empty())
        .filter(|provider| !allowed.contains(provider))
        .collect::<Vec<_>>();
    missing.sort();
    missing.dedup();
    missing
}

fn normalize_max_concurrency(raw: Option<u32>) -> Option<usize> {
    raw.map(|v| v as usize).filter(|v| *v >= 1)
}

fn evaluate_flow_readiness(
    entry: &WorkflowEntry,
    node_capabilities: &HashSet<String>,
    allowed_providers: Option<&HashSet<String>>,
    running: usize,
    default_max_concurrency: Option<usize>,
    owner_running: usize,
    owner_max_concurrency: Option<usize>,
) -> FlowReadiness {
    let missing_capabilities = missing_required_capabilities(entry, node_capabilities);
    let missing_providers = missing_required_providers(entry, allowed_providers);
    let max_concurrency = normalize_max_concurrency(entry.max_concurrency)
        .or(default_max_concurrency.filter(|v| *v >= 1));
    let concurrency_blocked = max_concurrency.map(|max| running >= max).unwrap_or(false);
    let owner_concurrency_blocked = owner_max_concurrency
        .map(|max| owner_running >= max)
        .unwrap_or(false);
    FlowReadiness {
        missing_capabilities,
        missing_providers,
        max_concurrency,
        running,
        concurrency_blocked,
        owner_running,
        owner_max_concurrency,
        owner_concurrency_blocked,
    }
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

fn daemon_audit_log_config(node_id: &str) -> Option<AuditLogConfig> {
    let Ok(raw) = std::env::var("ANNA_AUDIT_LOG_FILE") else {
        return None;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(AuditLogConfig {
        path: PathBuf::from(trimmed),
        node_id: node_id.to_string(),
    })
}

fn daemon_policy_signing_key() -> Option<String> {
    let Ok(raw) = std::env::var("ANNA_POLICY_SIGNING_KEY") else {
        return None;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(trimmed.to_string())
}

fn sorted_set_values(set: Option<&HashSet<String>>) -> Vec<String> {
    let mut values = set
        .map(|v| v.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    values.sort();
    values
}

async fn emit_audit_event(state: &AppState, event: &str, data: serde_json::Value) {
    let Some(config) = state.audit_log.as_ref() else {
        return;
    };
    if let Err(err) = append_audit_event(config, event, data).await {
        eprintln!(
            "anna-rs daemon: failed writing audit event '{}': {}",
            event, err
        );
    }
}

async fn append_audit_event(
    config: &AuditLogConfig,
    event: &str,
    data: serde_json::Value,
) -> Result<()> {
    if let Some(parent) = config.path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating audit log parent directory '{}'",
                parent.display()
            )
        })?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.path)
        .await
        .with_context(|| format!("failed opening audit log '{}'", config.path.display()))?;
    let mut encoded = serde_json::to_vec(&json!({
        "ts": now_unix_secs(),
        "node_id": config.node_id,
        "event": event,
        "data": data,
    }))
    .context("failed serializing audit event")?;
    encoded.push(b'\n');
    file.write_all(&encoded)
        .await
        .with_context(|| format!("failed appending audit log '{}'", config.path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("failed flushing audit log '{}'", config.path.display()))?;
    Ok(())
}

fn daemon_node_id() -> String {
    if let Ok(raw) = std::env::var("ANNA_DAEMON_NODE_ID") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "node".to_string());
    format!("{}-{}", host, std::process::id())
}

fn trigger_lease_config(node_id: &str) -> Option<TriggerLeaseConfig> {
    let Ok(raw) = std::env::var("ANNA_TRIGGER_LEASE_FILE") else {
        return None;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
    {
        return None;
    }

    let ttl_sec = std::env::var("ANNA_TRIGGER_LEASE_TTL_SEC")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v >= 2)
        .unwrap_or(15);
    Some(TriggerLeaseConfig {
        node_id: node_id.to_string(),
        lease_file: PathBuf::from(trimmed),
        ttl_sec,
    })
}

fn initial_trigger_leader_state(
    node_id: &str,
    lease: Option<&TriggerLeaseConfig>,
) -> TriggerLeaderState {
    match lease {
        Some(lease) => TriggerLeaderState {
            enabled: true,
            is_leader: false,
            node_id: node_id.to_string(),
            holder: None,
            expires_at: None,
            lease_file: Some(lease.lease_file.display().to_string()),
        },
        None => TriggerLeaderState {
            enabled: false,
            is_leader: true,
            node_id: node_id.to_string(),
            holder: Some(node_id.to_string()),
            expires_at: None,
            lease_file: None,
        },
    }
}

fn daemon_node_capabilities() -> HashSet<String> {
    let Ok(raw) = std::env::var("ANNA_NODE_CAPABILITIES") else {
        return HashSet::new();
    };
    raw.split([',', ';', '\n', '\t', ' '])
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .collect::<HashSet<_>>()
}

fn chat_intents_reload_interval() -> Option<Duration> {
    let Ok(raw) = std::env::var("ANNA_CHAT_INTENTS_RELOAD_SEC") else {
        return Some(Duration::from_secs(2));
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed == "0"
    {
        return None;
    }
    match trimmed.parse::<u64>() {
        Ok(seconds) if seconds >= 1 => Some(Duration::from_secs(seconds)),
        _ => {
            eprintln!(
                "anna-rs daemon: invalid ANNA_CHAT_INTENTS_RELOAD_SEC='{}' (expected integer >=1, or off/false/0)",
                trimmed
            );
            Some(Duration::from_secs(2))
        }
    }
}

fn daemon_chat_intents() -> HashMap<String, ChatIntentConfig> {
    let mut out = HashMap::new();

    if let Some(path) = chat_intents_file() {
        match load_chat_intents_file(&path) {
            Ok(file_intents) => out.extend(file_intents),
            Err(err) => eprintln!("anna-rs daemon: failed loading chat intents file: {}", err),
        }
    }

    if let Ok(raw) = std::env::var("ANNA_CHAT_INTENTS") {
        // Explicit env mapping overrides file entries when keys collide.
        out.extend(parse_chat_intents_value(&raw));
    }

    out
}

fn parse_chat_intents_value(raw: &str) -> HashMap<String, ChatIntentConfig> {
    let mut out = HashMap::new();
    for item in raw.split([',', ';', '\n']) {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((intent_raw, workflow_raw)) = trimmed.split_once('=') else {
            eprintln!(
                "anna-rs daemon: ignoring invalid ANNA_CHAT_INTENTS entry '{}'",
                trimmed
            );
            continue;
        };
        insert_chat_intent_entry(
            &mut out,
            intent_raw,
            ChatIntentConfig {
                workflow: workflow_raw.to_string(),
                allowed_callers: Vec::new(),
                allowed_owners: Vec::new(),
                required_tags: Vec::new(),
                max_iterations_cap: None,
            },
            "ANNA_CHAT_INTENTS",
            trimmed,
        );
    }
    out
}

fn chat_intents_file() -> Option<PathBuf> {
    let Ok(raw) = std::env::var("ANNA_CHAT_INTENTS_FILE") else {
        return None;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

fn load_chat_intents_file(path: &FsPath) -> Result<HashMap<String, ChatIntentConfig>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading '{}'", path.display()))?;
    parse_chat_intents_doc(&raw, &path.display().to_string())
}

fn parse_chat_intents_doc(raw: &str, source: &str) -> Result<HashMap<String, ChatIntentConfig>> {
    let parsed: ChatIntentDoc = serde_yaml::from_str(raw)
        .with_context(|| format!("failed parsing chat intents from '{}'", source))?;
    let mut out = HashMap::new();
    match parsed {
        ChatIntentDoc::Wrapped { intents } | ChatIntentDoc::List(intents) => {
            for item in intents {
                let raw_entry = format!(
                    "intent='{}' workflow='{}'",
                    item.intent, item.config.workflow
                );
                insert_chat_intent_entry(
                    &mut out,
                    &item.intent,
                    ChatIntentConfig {
                        workflow: item.config.workflow,
                        allowed_callers: item.config.allowed_callers,
                        allowed_owners: item.config.allowed_owners,
                        required_tags: item.config.required_tags,
                        max_iterations_cap: item.config.max_iterations_cap,
                    },
                    source,
                    &raw_entry,
                );
            }
        }
        ChatIntentDoc::Map(map) => {
            for (intent, value) in map {
                let (config, raw_entry) = match value {
                    ChatIntentMapValue::Workflow(workflow) => {
                        let raw_entry = format!("{}={}", intent, workflow);
                        (
                            ChatIntentConfig {
                                workflow,
                                allowed_callers: Vec::new(),
                                allowed_owners: Vec::new(),
                                required_tags: Vec::new(),
                                max_iterations_cap: None,
                            },
                            raw_entry,
                        )
                    }
                    ChatIntentMapValue::Config(config) => {
                        let raw_entry =
                            format!("intent='{}' workflow='{}'", intent, config.workflow);
                        (
                            ChatIntentConfig {
                                workflow: config.workflow,
                                allowed_callers: config.allowed_callers,
                                allowed_owners: config.allowed_owners,
                                required_tags: config.required_tags,
                                max_iterations_cap: config.max_iterations_cap,
                            },
                            raw_entry,
                        )
                    }
                };
                insert_chat_intent_entry(&mut out, &intent, config, source, &raw_entry);
            }
        }
    }
    if out.is_empty() {
        bail!("chat intents source '{}' has no valid entries", source);
    }
    Ok(out)
}

fn insert_chat_intent_entry(
    out: &mut HashMap<String, ChatIntentConfig>,
    intent_raw: &str,
    config: ChatIntentConfig,
    source: &str,
    raw_entry: &str,
) {
    let intent = intent_raw.trim().to_ascii_lowercase();
    let workflow = config.workflow.trim().to_string();
    if intent.is_empty() || workflow.is_empty() {
        eprintln!(
            "anna-rs daemon: ignoring invalid chat intent entry '{}' from {}",
            raw_entry, source
        );
        return;
    }
    if config.max_iterations_cap == Some(0) {
        eprintln!(
            "anna-rs daemon: ignoring invalid chat intent entry '{}' from {} (max_iterations_cap must be >=1)",
            raw_entry, source
        );
        return;
    }

    let mut allowed_callers = config
        .allowed_callers
        .iter()
        .map(|caller| caller.trim().to_ascii_lowercase())
        .filter(|caller| !caller.is_empty())
        .collect::<Vec<_>>();
    allowed_callers.sort();
    allowed_callers.dedup();

    let mut allowed_owners = config
        .allowed_owners
        .iter()
        .filter_map(|owner| owner_key(Some(owner.as_str())))
        .collect::<Vec<_>>();
    allowed_owners.sort();
    allowed_owners.dedup();

    let mut required_tags = config
        .required_tags
        .iter()
        .map(|tag| tag.trim().to_ascii_lowercase())
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();
    required_tags.sort();
    required_tags.dedup();

    out.insert(
        intent,
        ChatIntentConfig {
            workflow,
            allowed_callers,
            allowed_owners,
            required_tags,
            max_iterations_cap: config.max_iterations_cap,
        },
    );
}

fn evaluate_chat_intent_guardrails(
    rule: &ChatIntentConfig,
    entry: &WorkflowEntry,
    requested_max_iterations: Option<u32>,
    caller: Option<&str>,
) -> ChatIntentGuardrailOutcome {
    let mut reasons = Vec::new();

    if !rule.allowed_callers.is_empty() {
        let normalized_caller = caller
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty());
        let allowed = normalized_caller
            .as_ref()
            .map(|value| rule.allowed_callers.contains(value))
            .unwrap_or(false);
        if !allowed {
            reasons.push(format!(
                "caller '{}' is not allowed (allowed callers: {})",
                normalized_caller.as_deref().unwrap_or(""),
                rule.allowed_callers.join(", ")
            ));
        }
    }

    if !rule.allowed_owners.is_empty() {
        let workflow_owner = owner_key(entry.owner.as_deref());
        let allowed = workflow_owner
            .as_ref()
            .map(|owner| rule.allowed_owners.contains(owner))
            .unwrap_or(false);
        if !allowed {
            reasons.push(format!(
                "workflow owner '{}' is not allowed (allowed owners: {})",
                entry.owner.as_deref().unwrap_or(""),
                rule.allowed_owners.join(", ")
            ));
        }
    }

    if !rule.required_tags.is_empty() {
        let entry_tags = entry
            .tags
            .iter()
            .map(|tag| tag.trim().to_ascii_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect::<HashSet<_>>();
        let mut missing = rule
            .required_tags
            .iter()
            .filter(|tag| !entry_tags.contains(*tag))
            .cloned()
            .collect::<Vec<_>>();
        missing.sort();
        missing.dedup();
        if !missing.is_empty() {
            reasons.push(format!(
                "workflow missing required chat tags: {}",
                missing.join(", ")
            ));
        }
    }

    let mut effective_max_iterations = requested_max_iterations;
    if let Some(cap) = rule.max_iterations_cap {
        match requested_max_iterations {
            Some(value) if value > cap => reasons.push(format!(
                "requested max_iterations={} exceeds chat cap={}",
                value, cap
            )),
            Some(_) => {}
            None => {
                effective_max_iterations = Some(cap);
            }
        }
    }

    ChatIntentGuardrailOutcome {
        reasons,
        effective_max_iterations,
    }
}

fn daemon_owner_concurrency_policy() -> OwnerConcurrencyPolicy {
    let Ok(raw) = std::env::var("ANNA_OWNER_MAX_CONCURRENCY") else {
        return OwnerConcurrencyPolicy::default();
    };
    let mut policy = OwnerConcurrencyPolicy::default();

    for item in raw.split([',', ';', '\n']) {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((owner_raw, limit_raw)) = trimmed.split_once('=') else {
            eprintln!(
                "anna-rs daemon: ignoring invalid ANNA_OWNER_MAX_CONCURRENCY entry '{}'",
                trimmed
            );
            continue;
        };
        let owner = owner_raw.trim().to_ascii_lowercase();
        let limit = match limit_raw.trim().parse::<usize>() {
            Ok(v) if v >= 1 => v,
            _ => {
                eprintln!(
                    "anna-rs daemon: ignoring invalid owner limit '{}={}' (expected >=1)",
                    owner_raw.trim(),
                    limit_raw.trim()
                );
                continue;
            }
        };
        if owner == "*" {
            policy.default_limit = Some(limit);
        } else if !owner.is_empty() {
            policy.per_owner.insert(owner, limit);
        }
    }

    policy
}

fn owner_limit_for(owner: Option<&str>, policy: &OwnerConcurrencyPolicy) -> Option<usize> {
    let owner = owner_key(owner)?;
    policy
        .per_owner
        .get(&owner)
        .copied()
        .or(policy.default_limit)
}

fn daemon_retention_config() -> RetentionConfig {
    RetentionConfig {
        max_sessions: env_usize_or("ANNA_DAEMON_MAX_SESSIONS", 2000),
        max_hitl: env_usize_or("ANNA_DAEMON_MAX_HITL", 2000),
    }
}

fn env_usize_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v >= 1)
        .unwrap_or(default)
}

fn flow_registry_file() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("ANNA_FLOW_REGISTRY_FILE") {
        let trimmed = raw.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("off")
            || trimmed.eq_ignore_ascii_case("false")
        {
            return None;
        }
        return Some(PathBuf::from(trimmed));
    }
    None
}

fn daemon_state_file() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("ANNA_DAEMON_STATE_FILE") {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("off") {
            return None;
        }
        return Some(PathBuf::from(trimmed));
    }
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".anna/daemon-state.json"))
}

fn daemon_policy_snapshot_file() -> Option<PathBuf> {
    let Ok(raw) = std::env::var("ANNA_POLICY_SNAPSHOT_FILE") else {
        return None;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

async fn load_daemon_state(
    path: &FsPath,
) -> Result<(HashMap<String, SessionInfo>, HashMap<String, HitlPending>)> {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), HashMap::new()));
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed reading daemon state file '{}'", path.display()));
        }
    };

    let mut parsed: DaemonStateSnapshot = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing daemon state file '{}'", path.display()))?;
    let now = now_unix_secs();
    for (id, session) in &mut parsed.sessions {
        if session.id.trim().is_empty() {
            session.id = id.clone();
        }
        if session.status == "running" {
            session.status = "interrupted".to_string();
            session.updated_at = now;
            session
                .errors
                .push("daemon restarted while session was running".to_string());
        }
    }
    for (id, hitl) in &mut parsed.hitl {
        if hitl.id.trim().is_empty() {
            hitl.id = id.clone();
        }
    }
    Ok((parsed.sessions, parsed.hitl))
}

async fn chat_intents_reload_loop(state: AppState, interval: Duration) {
    loop {
        sleep(interval).await;
        let next = daemon_chat_intents();
        let mut write = state.chat_intents.write().await;
        if *write != next {
            *write = next.clone();
            let mut routes = next
                .iter()
                .map(|(intent, rule)| format!("{}={}", intent, rule.workflow))
                .collect::<Vec<_>>();
            routes.sort();
            if routes.is_empty() {
                println!("anna-rs chat intents reloaded: <empty>");
            } else {
                println!("anna-rs chat intents reloaded: {}", routes.join(","));
            }
            drop(write);
            emit_audit_event(
                &state,
                "chat_intents_reloaded",
                json!({
                    "count": next.len(),
                    "routes": routes,
                }),
            )
            .await;
        }
    }
}

async fn state_persist_loop(state: AppState, path: PathBuf) {
    loop {
        if let Err(err) = persist_daemon_state(&state, &path).await {
            eprintln!("anna-rs daemon: failed persisting state: {}", err);
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn persist_daemon_state(state: &AppState, path: &FsPath) -> Result<()> {
    let sessions = state.sessions.read().await.clone();
    let hitl = state.hitl.read().await.clone();
    let snapshot = DaemonStateSnapshot {
        sessions,
        hitl,
        saved_at: now_unix_secs(),
    };
    let raw = serde_json::to_string_pretty(&snapshot)?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating daemon state directory '{}'",
                parent.display()
            )
        })?;
    }

    let tmp = temp_state_path(path);
    tokio::fs::write(&tmp, raw)
        .await
        .with_context(|| format!("failed writing daemon state temp file '{}'", tmp.display()))?;
    tokio::fs::rename(&tmp, path).await.with_context(|| {
        format!(
            "failed moving daemon state file '{}' -> '{}'",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

async fn policy_snapshot_persist_loop(state: AppState, path: PathBuf) {
    loop {
        if let Err(err) = persist_policy_snapshot(&state, &path).await {
            eprintln!("anna-rs daemon: failed persisting policy snapshot: {}", err);
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn persist_policy_snapshot(state: &AppState, path: &FsPath) -> Result<()> {
    let snapshot = build_policy_snapshot(state).await;
    let raw = serde_json::to_string_pretty(&snapshot).context("serialize policy snapshot json")?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating policy snapshot directory '{}'",
                parent.display()
            )
        })?;
    }

    let tmp = temp_state_path(path);
    tokio::fs::write(&tmp, raw)
        .await
        .with_context(|| format!("failed writing policy snapshot temp '{}'", tmp.display()))?;
    tokio::fs::rename(&tmp, path).await.with_context(|| {
        format!(
            "failed moving policy snapshot '{}' -> '{}'",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

async fn build_policy_snapshot(state: &AppState) -> serde_json::Value {
    let trigger = state.trigger_leader_state.read().await.clone();
    let policy_core = build_policy_core(state).await;
    let (policy_revision, policy_signature) =
        policy_revision_and_signature(&policy_core, state.policy_signing_key.as_deref());

    json!({
        "saved_at": now_unix_secs(),
        "policy_revision": policy_revision,
        "policy_signature": policy_signature,
        "policy_signature_algorithm": state.policy_signing_key.as_ref().map(|_| "hmac-sha256"),
        "registry_enabled": policy_core["registry_enabled"].clone(),
        "auth_enabled": policy_core["auth_enabled"].clone(),
        "offline_mode": policy_core["offline_mode"].clone(),
        "node_capabilities": policy_core["node_capabilities"].clone(),
        "allowed_providers": policy_core["allowed_providers"].clone(),
        "owner_limits": policy_core["owner_limits"].clone(),
        "owner_default_limit": policy_core["owner_default_limit"].clone(),
        "chat_intents": policy_core["chat_intents"].clone(),
        "trigger_policy": policy_core["trigger_policy"].clone(),
        "trigger_scheduler": {
            "enabled": trigger.enabled,
            "is_leader": trigger.is_leader,
            "node_id": trigger.node_id,
            "holder": trigger.holder,
            "expires_at": trigger.expires_at,
            "lease_file": trigger.lease_file,
        },
        "policy_core": policy_core,
    })
}

async fn build_policy_core(state: &AppState) -> serde_json::Value {
    let trigger = state.trigger_leader_state.read().await.clone();
    let chat_intents = state.chat_intents.read().await.clone();

    let mut owner_limits = state
        .owner_policy
        .per_owner
        .iter()
        .map(|(owner, max_concurrency)| {
            json!({
                "owner": owner,
                "max_concurrency": max_concurrency,
            })
        })
        .collect::<Vec<_>>();
    owner_limits.sort_by(|a, b| {
        a.get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .cmp(b.get("owner").and_then(|v| v.as_str()).unwrap_or(""))
    });

    let mut chat_routes = chat_intents.into_iter().collect::<Vec<_>>();
    chat_routes.sort_by(|a, b| a.0.cmp(&b.0));
    let mut chat_map = serde_json::Map::new();
    for (intent, rule) in chat_routes {
        chat_map.insert(
            intent,
            json!({
                "workflow": rule.workflow,
                "allowed_callers": rule.allowed_callers,
                "allowed_owners": rule.allowed_owners,
                "required_tags": rule.required_tags,
                "max_iterations_cap": rule.max_iterations_cap,
            }),
        );
    }

    json!({
        "registry_enabled": state.registry_file.is_some(),
        "auth_enabled": state.auth_token.is_some(),
        "offline_mode": state.offline_mode,
        "node_capabilities": sorted_set_values(Some(&state.node_capabilities)),
        "allowed_providers": sorted_set_values(state.allowed_providers.as_ref()),
        "owner_limits": owner_limits,
        "owner_default_limit": state.owner_policy.default_limit,
        "chat_intents": chat_map,
        "trigger_policy": {
            "leader_election_enabled": trigger.enabled,
            "node_id": trigger.node_id,
            "lease_file": trigger.lease_file,
        },
    })
}

fn policy_revision_and_signature(
    policy_core: &serde_json::Value,
    signing_key: Option<&str>,
) -> (String, Option<String>) {
    let canonical = serde_json::to_vec(policy_core).unwrap_or_else(|_| b"{}".to_vec());
    let digest = Sha256::digest(&canonical);
    let revision = hex_encode(&digest);
    let signature = signing_key
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .and_then(|key| sign_policy_revision_hmac_sha256(&revision, key));
    (revision, signature)
}

fn temp_state_path(path: &FsPath) -> PathBuf {
    let mut tmp = path.to_path_buf();
    if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
        tmp.set_file_name(format!("{}.tmp", name));
        return tmp;
    }
    path.with_extension("tmp")
}

fn prune_sessions_in_place(sessions: &mut HashMap<String, SessionInfo>, max_sessions: usize) {
    if max_sessions == 0 || sessions.len() <= max_sessions {
        return;
    }

    let mut removable = sessions
        .iter()
        .filter(|(_, s)| s.status != "running")
        .map(|(id, s)| (id.clone(), s.updated_at))
        .collect::<Vec<_>>();
    removable.sort_by_key(|(_, updated_at)| *updated_at);

    let mut remove_count = sessions.len().saturating_sub(max_sessions);
    for (id, _) in removable {
        if remove_count == 0 {
            break;
        }
        if sessions.remove(&id).is_some() {
            remove_count -= 1;
        }
    }
}

fn prune_hitl_in_place(hitl: &mut HashMap<String, HitlPending>, max_hitl: usize) {
    if max_hitl == 0 || hitl.len() <= max_hitl {
        return;
    }

    let mut removable = hitl
        .iter()
        .filter(|(_, h)| h.status == "resolved")
        .map(|(id, h)| (id.clone(), h.created_at))
        .collect::<Vec<_>>();
    removable.sort_by_key(|(_, created_at)| *created_at);

    let mut remove_count = hitl.len().saturating_sub(max_hitl);
    for (id, _) in removable {
        if remove_count == 0 {
            break;
        }
        if hitl.remove(&id).is_some() {
            remove_count -= 1;
        }
    }
}

fn set_etag_header(response: &mut axum::response::Response, revision: &str) {
    if let Ok(value) = HeaderValue::from_str(&etag_value(revision)) {
        response.headers_mut().insert(ETAG, value);
    }
}

fn etag_value(revision: &str) -> String {
    format!("\"{}\"", revision.trim())
}

fn normalize_etag_token(token: &str) -> Option<String> {
    let mut trimmed = token.trim();
    if let Some(rest) = trimmed.strip_prefix("W/") {
        trimmed = rest.trim();
    }
    let stripped = trimmed.strip_prefix('"')?.strip_suffix('"')?;
    let out = stripped.trim();
    if out.is_empty() {
        return None;
    }
    Some(out.to_string())
}

fn etag_header_matches_value(raw: &str, revision: &str) -> bool {
    let revision = revision.trim();
    raw.split(',')
        .map(str::trim)
        .any(|candidate| match candidate {
            "*" => true,
            other => normalize_etag_token(other)
                .map(|tag| tag == revision)
                .unwrap_or(false),
        })
}

fn if_none_match_matches(headers: &HeaderMap, revision: &str) -> bool {
    headers
        .get(IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|raw| etag_header_matches_value(raw, revision))
        .unwrap_or(false)
}

fn if_match_allows(headers: &HeaderMap, revision: &str) -> bool {
    let Some(raw) = headers.get(IF_MATCH).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    etag_header_matches_value(raw, revision)
}

fn not_modified_with_etag(revision: &str) -> axum::response::Response {
    let mut response = StatusCode::NOT_MODIFIED.into_response();
    set_etag_header(&mut response, revision);
    response
}

fn precondition_failed_with_etag(revision: &str) -> axum::response::Response {
    let mut response = (
        StatusCode::PRECONDITION_FAILED,
        "policy revision precondition failed",
    )
        .into_response();
    set_etag_header(&mut response, revision);
    response
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

fn request_caller(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-anna-caller")
        .or_else(|| headers.get("x-anna-role"))
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
}

fn status_matches(status: &str, filter: &str) -> bool {
    status.eq_ignore_ascii_case(filter.trim())
}

fn workflow_public_id(entry: &WorkflowEntry) -> String {
    entry
        .flow_id
        .as_ref()
        .filter(|v| !v.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| entry.file_name.clone())
}

fn owner_key(owner: Option<&str>) -> Option<String> {
    owner
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
}

fn build_running_indexes(
    sessions: &HashMap<String, SessionInfo>,
) -> (HashMap<String, usize>, HashMap<String, usize>) {
    let mut by_workflow = HashMap::new();
    let mut by_owner = HashMap::new();
    for session in sessions.values().filter(|v| v.status == "running") {
        *by_workflow.entry(session.workflow.clone()).or_insert(0) += 1;
        if let Some(owner) = owner_key(session.owner.as_deref()) {
            *by_owner.entry(owner).or_insert(0) += 1;
        }
    }
    (by_workflow, by_owner)
}

fn matches_workflow_meta_filters(
    item: &WorkflowMetaResponse,
    tag_filter: Option<&str>,
    owner_filter: Option<&str>,
    capability_filter: Option<&str>,
    available_filter: Option<bool>,
) -> bool {
    if let Some(required) = tag_filter {
        let has_tag = item
            .tags
            .iter()
            .any(|tag| tag.trim().eq_ignore_ascii_case(required));
        if !has_tag {
            return false;
        }
    }

    if let Some(required) = owner_filter {
        let owner = item
            .owner
            .as_deref()
            .map(|v| v.trim().to_ascii_lowercase())
            .unwrap_or_default();
        if owner != required {
            return false;
        }
    }

    if let Some(required) = capability_filter {
        let has_capability = item
            .required_capabilities
            .iter()
            .any(|cap| cap.trim().eq_ignore_ascii_case(required));
        if !has_capability {
            return false;
        }
    }

    if let Some(required) = available_filter
        && item.available != required
    {
        return false;
    }
    true
}

async fn find_workflows_with_registry(
    root: &FsPath,
    registry_file: Option<&FsPath>,
) -> Result<Vec<String>> {
    let mut out = find_workflow_entries_with_registry(root, registry_file)
        .await?
        .into_iter()
        .map(|v| workflow_public_id(&v))
        .collect::<Vec<_>>();
    out.sort();
    out.dedup();
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
    flow_id: Option<String>,
    workflow_name: String,
    path: PathBuf,
    tags: Vec<String>,
    required_capabilities: Vec<String>,
    required_providers: Vec<String>,
    owner: Option<String>,
    version: Option<String>,
    max_concurrency: Option<u32>,
    trigger_webhook: Option<String>,
    trigger_watch: Option<String>,
    trigger_cron: Option<String>,
    trigger_interval: Option<String>,
    workflow_workdir: Option<String>,
}

struct FlowReadiness {
    missing_capabilities: Vec<String>,
    missing_providers: Vec<String>,
    max_concurrency: Option<usize>,
    running: usize,
    concurrency_blocked: bool,
    owner_running: usize,
    owner_max_concurrency: Option<usize>,
    owner_concurrency_blocked: bool,
}

impl FlowReadiness {
    fn can_run(&self) -> bool {
        self.missing_capabilities.is_empty()
            && self.missing_providers.is_empty()
            && !self.concurrency_blocked
            && !self.owner_concurrency_blocked
    }
}

enum TriggerLaunchOutcome {
    Launched(String),
    SkippedRunning,
    SkippedCapability(Vec<String>),
    SkippedProvider(Vec<String>),
    SkippedConcurrency {
        running: usize,
        max_concurrency: usize,
    },
}

async fn load_flow_registry(path: &FsPath) -> Result<Vec<FlowRegistryEntry>> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed reading flow registry '{}'", path.display()))?;
    let parsed: FlowRegistryDoc = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed parsing flow registry '{}'", path.display()))?;

    let mut entries = match parsed {
        FlowRegistryDoc::Wrapped { flows } => flows,
        FlowRegistryDoc::List(items) => items,
    };
    if entries.is_empty() {
        bail!("flow registry '{}' has no entries", path.display());
    }

    let mut flow_ids = HashSet::new();
    for item in &mut entries {
        item.flow_id = item.flow_id.trim().to_string();
        item.path = item.path.trim().to_string();
        item.owner = item
            .owner
            .as_ref()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        item.version = item
            .version
            .as_ref()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        if item.flow_id.is_empty() {
            bail!(
                "flow registry '{}' has entry with empty flow_id",
                path.display()
            );
        }
        if item.path.is_empty() {
            bail!(
                "flow registry '{}' has entry '{}' with empty path",
                path.display(),
                item.flow_id
            );
        }
        if !flow_ids.insert(item.flow_id.clone()) {
            bail!(
                "flow registry '{}' has duplicate flow_id '{}'",
                path.display(),
                item.flow_id
            );
        }
        if item.max_concurrency == Some(0) {
            bail!(
                "flow registry '{}' has entry '{}' with invalid max_concurrency=0",
                path.display(),
                item.flow_id
            );
        }
    }
    Ok(entries)
}

fn resolve_registry_workflow_path(plays_dir: &FsPath, raw_path: &str) -> PathBuf {
    let path = PathBuf::from(raw_path.trim());
    if path.is_absolute() {
        path
    } else {
        plays_dir.join(path)
    }
}

async fn find_workflow_entries_with_registry(
    root: &FsPath,
    registry_file: Option<&FsPath>,
) -> Result<Vec<WorkflowEntry>> {
    if let Some(registry_path) = registry_file {
        let registry_entries = load_flow_registry(registry_path).await?;
        let mut out = Vec::new();
        for spec in registry_entries {
            let path = resolve_registry_workflow_path(root, &spec.path);
            let wf = Workflow::load(&path).with_context(|| {
                format!(
                    "flow registry '{}' entry '{}' points to invalid workflow '{}'",
                    registry_path.display(),
                    spec.flow_id,
                    path.display()
                )
            })?;
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    anyhow::anyhow!("invalid workflow filename in '{}'", path.display())
                })?;
            let required_providers = collect_required_providers(&wf);
            out.push(WorkflowEntry {
                file_name,
                flow_id: Some(spec.flow_id),
                workflow_name: wf.name,
                path,
                tags: spec.tags,
                required_capabilities: spec.required_capabilities,
                required_providers,
                owner: spec.owner,
                version: spec.version,
                max_concurrency: spec.max_concurrency,
                trigger_webhook: wf.trigger.webhook,
                trigger_watch: wf.trigger.watch,
                trigger_cron: wf.trigger.cron,
                trigger_interval: wf.trigger.interval,
                workflow_workdir: wf.workdir,
            });
        }
        return Ok(out);
    }

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
        let required_providers = collect_required_providers(&wf);
        out.push(WorkflowEntry {
            file_name,
            flow_id: None,
            workflow_name: wf.name,
            path,
            tags: vec![],
            required_capabilities: vec![],
            required_providers,
            owner: None,
            version: None,
            max_concurrency: None,
            trigger_webhook: wf.trigger.webhook,
            trigger_watch: wf.trigger.watch,
            trigger_cron: wf.trigger.cron,
            trigger_interval: wf.trigger.interval,
            workflow_workdir: wf.workdir,
        });
    }
    Ok(out)
}

async fn resolve_registered_workflow_entry_with_registry(
    root: &FsPath,
    registry_file: Option<&FsPath>,
    name: &str,
) -> Result<Option<WorkflowEntry>> {
    let normalized = name.trim();
    let entries = find_workflow_entries_with_registry(root, registry_file).await?;
    for entry in &entries {
        if entry.file_name == normalized
            || entry.workflow_name == normalized
            || entry.flow_id.as_deref() == Some(normalized)
        {
            return Ok(Some(entry.clone()));
        }
    }

    if !normalized.ends_with(".anna") {
        let candidate = format!("{}.anna", normalized);
        for entry in &entries {
            if entry.file_name == candidate {
                return Ok(Some(entry.clone()));
            }
        }
    }
    Ok(None)
}

async fn launch_workflow_from_entry(
    state: &AppState,
    entry: &WorkflowEntry,
    trigger_source: &str,
) -> Result<TriggerLaunchOutcome> {
    let (running, owner_running) = running_counts_for_entry(state, entry).await;
    let owner_max_concurrency = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness = evaluate_flow_readiness(
        entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        running,
        Some(1),
        owner_running,
        owner_max_concurrency,
    );
    if !readiness.missing_capabilities.is_empty() {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: missing capabilities [{}]",
            trigger_source,
            entry.workflow_name,
            readiness.missing_capabilities.join(", ")
        );
        return Ok(TriggerLaunchOutcome::SkippedCapability(
            readiness.missing_capabilities,
        ));
    }
    if !readiness.missing_providers.is_empty() {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: blocked providers [{}]",
            trigger_source,
            entry.workflow_name,
            readiness.missing_providers.join(", ")
        );
        return Ok(TriggerLaunchOutcome::SkippedProvider(
            readiness.missing_providers,
        ));
    }

    if readiness.owner_concurrency_blocked {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: owner limit running={} max={}",
            trigger_source,
            entry.workflow_name,
            readiness.owner_running,
            readiness.owner_max_concurrency.unwrap_or(0)
        );
        return Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running: readiness.owner_running,
            max_concurrency: readiness.owner_max_concurrency.unwrap_or(0),
        });
    }

    if readiness.concurrency_blocked {
        let max_concurrency = readiness.max_concurrency.unwrap_or(1);
        if max_concurrency == 1 {
            println!(
                "anna-rs daemon trigger={} workflow='{}' skipped: already running",
                trigger_source, entry.workflow_name
            );
            return Ok(TriggerLaunchOutcome::SkippedRunning);
        }
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: concurrency limit running={} max={}",
            trigger_source, entry.workflow_name, readiness.running, max_concurrency
        );
        return Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running: readiness.running,
            max_concurrency,
        });
    }

    let mut wf = Workflow::load(&entry.path)?;
    if wf.workdir.is_none() {
        wf.workdir = Some(state.plays_dir.display().to_string());
    }
    let source = format!("trigger:{}", trigger_source);
    let req_id = launch_workflow(state, wf, None, entry.owner.clone(), &source).await?;
    println!(
        "anna-rs daemon trigger={} workflow='{}' request_id={}",
        trigger_source, entry.workflow_name, req_id
    );
    Ok(TriggerLaunchOutcome::Launched(req_id))
}

async fn running_counts_for_entry(state: &AppState, entry: &WorkflowEntry) -> (usize, usize) {
    let target_owner = owner_key(entry.owner.as_deref());
    let sessions = state.sessions.read().await;
    let mut running_workflow = 0usize;
    let mut running_owner = 0usize;
    for session in sessions.values().filter(|s| s.status == "running") {
        if session.workflow == entry.workflow_name {
            running_workflow += 1;
        }
        if let Some(target_owner) = target_owner.as_deref()
            && owner_key(session.owner.as_deref()).as_deref() == Some(target_owner)
        {
            running_owner += 1;
        }
    }
    (running_workflow, running_owner)
}

async fn launch_registered_entry_with_options(
    state: &AppState,
    entry: &WorkflowEntry,
    requested_name: &str,
    options: RunRegisteredOptions,
    launch_source: &str,
) -> std::result::Result<String, axum::response::Response> {
    let (running, owner_running) = running_counts_for_entry(state, entry).await;
    let owner_max_concurrency = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness = evaluate_flow_readiness(
        entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        running,
        None,
        owner_running,
        owner_max_concurrency,
    );
    if !readiness.missing_capabilities.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "workflow '{}' requires missing capabilities: {}",
                requested_name,
                readiness.missing_capabilities.join(", ")
            ),
        )
            .into_response());
    }
    if !readiness.missing_providers.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "workflow '{}' requires blocked providers: {}",
                requested_name,
                readiness.missing_providers.join(", ")
            ),
        )
            .into_response());
    }
    if readiness.owner_concurrency_blocked {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "workflow '{}' owner concurrency limit reached: owner='{}' running={} max_concurrency={}",
                requested_name,
                entry.owner.as_deref().unwrap_or(""),
                readiness.owner_running,
                readiness.owner_max_concurrency.unwrap_or(0)
            ),
        )
            .into_response());
    }
    if readiness.concurrency_blocked {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "workflow '{}' concurrency limit reached: running={} max_concurrency={}",
                requested_name,
                readiness.running,
                readiness.max_concurrency.unwrap_or(0)
            ),
        )
            .into_response());
    }

    let mut workflow = match Workflow::load(&entry.path) {
        Ok(v) => v,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid workflow '{}': {}", entry.path.display(), err),
            )
                .into_response());
        }
    };
    if workflow.workdir.is_none() {
        workflow.workdir = Some(state.plays_dir.display().to_string());
    }
    workflow.vars.extend(options.vars);

    match launch_workflow(
        state,
        workflow,
        options.max_iterations,
        entry.owner.clone(),
        launch_source,
    )
    .await
    {
        Ok(v) => Ok(v),
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to start workflow: {}", err),
        )
            .into_response()),
    }
}

fn parse_run_registered_options(body: &str) -> Result<RunRegisteredOptions> {
    if body.trim().is_empty() {
        return Ok(RunRegisteredOptions::default());
    }
    let parsed = serde_json::from_str::<RunRegisteredOptions>(body)
        .context("invalid run options json body, expected {\"vars\":{...},\"max_iterations\":N}")?;
    Ok(parsed)
}

fn parse_chat_run_request(body: &str) -> Result<ChatRunRequest> {
    if body.trim().is_empty() {
        bail!("chat run body is required, expected {{\"intent\":\"...\"}}");
    }
    let parsed = serde_json::from_str::<ChatRunRequest>(body)
        .context("invalid chat run json body, expected {\"intent\":\"...\",\"vars\":{...},\"max_iterations\":N}")?;
    if parsed.intent.trim().is_empty() {
        bail!("chat run requires non-empty 'intent'");
    }
    Ok(parsed)
}

async fn launch_workflow(
    state: &AppState,
    workflow: Workflow,
    max_iterations: Option<u32>,
    owner: Option<String>,
    launch_source: &str,
) -> Result<String> {
    let req_id = crate::session::gen_session_id();
    let runtime_session_id = crate::session::gen_session_id();
    let workflow_name = workflow.name.clone();
    let owner_for_audit = owner.clone();
    let now = now_unix_secs();
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(
            req_id.clone(),
            SessionInfo {
                id: req_id.clone(),
                status: "running".to_string(),
                workflow: workflow_name.clone(),
                owner: owner.clone(),
                created_at: now,
                updated_at: now,
                runtime_session_id: Some(runtime_session_id.clone()),
                outputs: HashMap::new(),
                errors: Vec::new(),
            },
        );
        prune_sessions_in_place(&mut sessions, state.retention.max_sessions);
    }
    emit_audit_event(
        state,
        "workflow_launched",
        json!({
            "request_id": req_id.clone(),
            "runtime_session_id": runtime_session_id.clone(),
            "workflow": workflow_name.clone(),
            "owner": owner_for_audit.clone(),
            "source": launch_source,
            "max_iterations": max_iterations,
        }),
    )
    .await;

    let state_for_task = state.clone();
    let req_id_for_task = req_id.clone();
    let workflow_name_for_task = workflow.name.clone();
    let source_for_task = launch_source.to_string();
    let owner_for_task = owner.clone();
    let runtime_for_task = runtime_session_id.clone();
    let started = Instant::now();
    let handle = tokio::spawn(async move {
        let run = state_for_task
            .executor
            .run(
                &workflow,
                RunConfig {
                    max_iterations,
                    session_id_override: Some(runtime_for_task.clone()),
                },
            )
            .await;
        let elapsed_ms = started.elapsed().as_millis() as u64;

        let mut sessions = state_for_task.sessions.write().await;
        let event_data = match run {
            Ok(result) => {
                let runtime_id = result.session_id.clone();
                let outputs_count = result.outputs.len();
                let errors_count = result.errors.len();
                if let Some(info) = sessions.get_mut(&req_id_for_task) {
                    info.status = "done".to_string();
                    info.updated_at = now_unix_secs();
                    info.runtime_session_id = Some(runtime_id.clone());
                    info.outputs = result.outputs;
                    info.errors = result.errors;
                }
                json!({
                    "request_id": req_id_for_task.clone(),
                    "runtime_session_id": runtime_id,
                    "workflow": workflow_name_for_task,
                    "owner": owner_for_task,
                    "source": source_for_task,
                    "status": "done",
                    "elapsed_ms": elapsed_ms,
                    "outputs_count": outputs_count,
                    "errors_count": errors_count,
                })
            }
            Err(err) => {
                let message = err.to_string();
                if let Some(info) = sessions.get_mut(&req_id_for_task) {
                    info.status = "failed".to_string();
                    info.updated_at = now_unix_secs();
                    info.errors.push(message.clone());
                }
                json!({
                    "request_id": req_id_for_task.clone(),
                    "runtime_session_id": runtime_for_task,
                    "workflow": workflow_name_for_task,
                    "owner": owner_for_task,
                    "source": source_for_task,
                    "status": "failed",
                    "elapsed_ms": elapsed_ms,
                    "error": message,
                })
            }
        };
        prune_sessions_in_place(&mut sessions, state_for_task.retention.max_sessions);
        drop(sessions);
        emit_audit_event(&state_for_task, "workflow_finished", event_data).await;
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
        ChatIntentConfig, DaemonHitl, DaemonStateSnapshot, HitlPending, SessionInfo,
        TriggerLeaseConfig, WorkflowEntry, WorkflowMetaResponse, collect_required_providers,
        collect_watch_snapshot, evaluate_chat_intent_guardrails, evaluate_flow_readiness,
        find_workflow_entries_with_registry, is_authorized, load_daemon_state, load_flow_registry,
        matches_workflow_meta_filters, missing_required_capabilities, parse_chat_intents_doc,
        parse_chat_intents_value, parse_chat_run_request, parse_run_registered_options,
        persist_policy_snapshot, prune_hitl_in_place, prune_sessions_in_place,
        resolve_registered_workflow_entry_with_registry, resolve_trigger_leadership,
        resolve_watch_pattern, status_matches, temp_state_path,
    };
    use crate::executor::{Executor, HitlHandler, HitlRequest};
    use crate::workflow::{Stage, Workflow};
    use axum::http::header::{IF_MATCH, IF_NONE_MATCH};
    use axum::http::{HeaderMap, HeaderValue};
    use std::collections::{HashMap, HashSet};
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

        let by_file = resolve_registered_workflow_entry_with_registry(&dir, None, "demo.anna")
            .await
            .expect("resolve by file")
            .map(|v| v.path);
        assert_eq!(by_file.as_deref(), Some(file.as_path()));

        let by_name = resolve_registered_workflow_entry_with_registry(&dir, None, "demo-workflow")
            .await
            .expect("resolve by workflow name")
            .map(|v| v.path);
        assert_eq!(by_name.as_deref(), Some(file.as_path()));

        let by_stem = resolve_registered_workflow_entry_with_registry(&dir, None, "demo")
            .await
            .expect("resolve by stem")
            .map(|v| v.path);
        assert_eq!(by_stem.as_deref(), Some(file.as_path()));
    }

    #[tokio::test]
    async fn loads_flow_registry_and_rejects_duplicates() {
        let dir = std::env::temp_dir().join(format!(
            "anna-daemon-flow-reg-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");

        let valid = dir.join("registry.yml");
        tokio::fs::write(
            &valid,
            "flows:\n  - flow_id: alpha\n    path: a.anna\n    max_concurrency: 2\n  - flow_id: beta\n    path: b.anna\n",
        )
        .await
        .expect("write valid registry");
        let parsed = load_flow_registry(&valid)
            .await
            .expect("parse valid registry");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].flow_id, "alpha");
        assert_eq!(parsed[0].max_concurrency, Some(2));

        let duplicate = dir.join("registry-dup.yml");
        tokio::fs::write(
            &duplicate,
            "flows:\n  - flow_id: same\n    path: a.anna\n  - flow_id: same\n    path: b.anna\n",
        )
        .await
        .expect("write duplicate registry");
        let err = load_flow_registry(&duplicate)
            .await
            .expect_err("duplicate flow_id should fail");
        assert!(err.to_string().contains("duplicate flow_id"));

        let invalid_concurrency = dir.join("registry-invalid.yml");
        tokio::fs::write(
            &invalid_concurrency,
            "flows:\n  - flow_id: bad\n    path: a.anna\n    max_concurrency: 0\n",
        )
        .await
        .expect("write invalid registry");
        let err = load_flow_registry(&invalid_concurrency)
            .await
            .expect_err("max_concurrency=0 should fail");
        assert!(err.to_string().contains("max_concurrency=0"));
    }

    #[tokio::test]
    async fn registry_entries_filter_directory_scan_and_support_flow_id() {
        let dir = std::env::temp_dir().join(format!(
            "anna-daemon-registry-entries-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp dir");

        let included = dir.join("included.anna");
        tokio::fs::write(
            &included,
            "name: included-flow\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write included workflow");
        tokio::fs::write(
            dir.join("not-listed.anna"),
            "name: hidden-flow\nstages:\n  - id: hello\n    exec: \"echo hidden\"\n",
        )
        .await
        .expect("write non-listed workflow");

        let registry = dir.join("flows.yml");
        tokio::fs::write(
            &registry,
            "flows:\n  - flow_id: prod-deploy\n    path: included.anna\n",
        )
        .await
        .expect("write registry");

        let entries = find_workflow_entries_with_registry(&dir, Some(&registry))
            .await
            .expect("load registry-based entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_name, "included-flow");
        assert_eq!(entries[0].flow_id.as_deref(), Some("prod-deploy"));

        let by_flow_id =
            resolve_registered_workflow_entry_with_registry(&dir, Some(&registry), "prod-deploy")
                .await
                .expect("resolve by flow_id")
                .map(|v| v.path);
        assert_eq!(by_flow_id.as_deref(), Some(included.as_path()));
    }

    #[test]
    fn missing_capability_filter_works_with_wildcard_and_case() {
        let entry = WorkflowEntry {
            file_name: "x.anna".to_string(),
            flow_id: Some("x".to_string()),
            workflow_name: "x".to_string(),
            path: std::path::PathBuf::from("/tmp/x.anna"),
            tags: vec![],
            required_capabilities: vec!["K8S".to_string(), "vault".to_string()],
            required_providers: vec![],
            owner: None,
            version: None,
            max_concurrency: Some(2),
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };

        let node = std::collections::HashSet::from([
            "k8s".to_string(),
            "http".to_string(),
            "VaUlT".to_ascii_lowercase(),
        ]);
        let missing = missing_required_capabilities(&entry, &node);
        assert!(missing.is_empty());

        let restricted = std::collections::HashSet::from(["k8s".to_string()]);
        let missing = missing_required_capabilities(&entry, &restricted);
        assert_eq!(missing, vec!["vault".to_string()]);

        let wildcard = std::collections::HashSet::from(["*".to_string()]);
        let missing = missing_required_capabilities(&entry, &wildcard);
        assert!(missing.is_empty());
    }

    #[test]
    fn evaluate_flow_readiness_checks_concurrency_and_capabilities() {
        let entry = WorkflowEntry {
            file_name: "x.anna".to_string(),
            flow_id: Some("x".to_string()),
            workflow_name: "x".to_string(),
            path: std::path::PathBuf::from("/tmp/x.anna"),
            tags: vec![],
            required_capabilities: vec!["k8s".to_string()],
            required_providers: vec![],
            owner: None,
            version: None,
            max_concurrency: Some(2),
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };

        let caps = std::collections::HashSet::from(["k8s".to_string()]);
        let ok = evaluate_flow_readiness(&entry, &caps, None, 1, None, 0, None);
        assert!(ok.can_run());
        assert!(!ok.concurrency_blocked);
        assert!(!ok.owner_concurrency_blocked);

        let blocked = evaluate_flow_readiness(&entry, &caps, None, 2, None, 0, None);
        assert!(!blocked.can_run());
        assert!(blocked.concurrency_blocked);

        let missing_caps = std::collections::HashSet::from(["shell".to_string()]);
        let missing = evaluate_flow_readiness(&entry, &missing_caps, None, 0, None, 0, None);
        assert!(!missing.can_run());
        assert_eq!(missing.missing_capabilities, vec!["k8s".to_string()]);
    }

    #[test]
    fn evaluate_flow_readiness_blocks_missing_provider() {
        let entry = WorkflowEntry {
            file_name: "x.anna".to_string(),
            flow_id: Some("x".to_string()),
            workflow_name: "x".to_string(),
            path: std::path::PathBuf::from("/tmp/x.anna"),
            tags: vec![],
            required_capabilities: vec![],
            required_providers: vec!["shell".to_string(), "cli".to_string()],
            owner: None,
            version: None,
            max_concurrency: None,
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };

        let caps = std::collections::HashSet::new();
        let allowed = std::collections::HashSet::from(["shell".to_string()]);
        let readiness = evaluate_flow_readiness(&entry, &caps, Some(&allowed), 0, None, 0, None);
        assert!(!readiness.can_run());
        assert_eq!(readiness.missing_providers, vec!["cli".to_string()]);
    }

    #[test]
    fn collect_required_providers_detects_hooks_vote_and_stage_provider() {
        let workflow = Workflow {
            name: "providers-test".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env: HashMap::new(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![
                Stage {
                    id: "build".to_string(),
                    provider: "cli".to_string(),
                    exec: Some("echo hi".to_string()),
                    ..Default::default()
                },
                Stage {
                    id: "judge".to_string(),
                    vote: Some("pick best".to_string()),
                    each: vec!["a".to_string(), "b".to_string()],
                    ..Default::default()
                },
                Stage {
                    id: "hooks".to_string(),
                    workflow: Some("child.anna".to_string()),
                    before: Some("echo before".to_string()),
                    ..Default::default()
                },
            ],
            source_path: None,
        };

        let providers = collect_required_providers(&workflow);
        assert_eq!(
            providers,
            vec!["cli".to_string(), "llm".to_string(), "shell".to_string()]
        );
    }

    #[test]
    fn evaluate_flow_readiness_uses_default_concurrency_when_requested() {
        let entry = WorkflowEntry {
            file_name: "x.anna".to_string(),
            flow_id: Some("x".to_string()),
            workflow_name: "x".to_string(),
            path: std::path::PathBuf::from("/tmp/x.anna"),
            tags: vec![],
            required_capabilities: vec![],
            required_providers: vec![],
            owner: None,
            version: None,
            max_concurrency: None,
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };
        let caps = std::collections::HashSet::new();

        let manual = evaluate_flow_readiness(&entry, &caps, None, 100, None, 0, None);
        assert!(manual.can_run());
        assert_eq!(manual.max_concurrency, None);

        let trigger_default = evaluate_flow_readiness(&entry, &caps, None, 1, Some(1), 0, None);
        assert!(!trigger_default.can_run());
        assert!(trigger_default.concurrency_blocked);
        assert_eq!(trigger_default.max_concurrency, Some(1));
    }

    #[test]
    fn evaluate_flow_readiness_blocks_owner_limit() {
        let entry = WorkflowEntry {
            file_name: "x.anna".to_string(),
            flow_id: Some("x".to_string()),
            workflow_name: "x".to_string(),
            path: std::path::PathBuf::from("/tmp/x.anna"),
            tags: vec![],
            required_capabilities: vec![],
            required_providers: vec![],
            owner: Some("platform".to_string()),
            version: None,
            max_concurrency: Some(10),
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };
        let caps = std::collections::HashSet::new();
        let readiness = evaluate_flow_readiness(&entry, &caps, None, 0, None, 3, Some(3));
        assert!(!readiness.can_run());
        assert!(readiness.owner_concurrency_blocked);
        assert_eq!(readiness.owner_running, 3);
        assert_eq!(readiness.owner_max_concurrency, Some(3));
    }

    #[test]
    fn owner_limit_for_prefers_specific_over_default() {
        let policy = super::OwnerConcurrencyPolicy {
            per_owner: std::collections::HashMap::from([
                ("platform".to_string(), 5usize),
                ("ops".to_string(), 2usize),
            ]),
            default_limit: Some(1),
        };
        assert_eq!(super::owner_limit_for(Some("platform"), &policy), Some(5));
        assert_eq!(super::owner_limit_for(Some("ops"), &policy), Some(2));
        assert_eq!(super::owner_limit_for(Some("other"), &policy), Some(1));
        assert_eq!(super::owner_limit_for(None, &policy), None);
    }

    #[test]
    fn build_running_indexes_tracks_workflow_and_owner_counts() {
        let sessions = std::collections::HashMap::from([
            (
                "a".to_string(),
                SessionInfo {
                    id: "a".to_string(),
                    status: "running".to_string(),
                    workflow: "deploy".to_string(),
                    owner: Some("Platform".to_string()),
                    created_at: 1,
                    updated_at: 1,
                    runtime_session_id: None,
                    outputs: std::collections::HashMap::new(),
                    errors: vec![],
                },
            ),
            (
                "b".to_string(),
                SessionInfo {
                    id: "b".to_string(),
                    status: "running".to_string(),
                    workflow: "deploy".to_string(),
                    owner: Some("platform".to_string()),
                    created_at: 1,
                    updated_at: 1,
                    runtime_session_id: None,
                    outputs: std::collections::HashMap::new(),
                    errors: vec![],
                },
            ),
            (
                "c".to_string(),
                SessionInfo {
                    id: "c".to_string(),
                    status: "done".to_string(),
                    workflow: "deploy".to_string(),
                    owner: Some("platform".to_string()),
                    created_at: 1,
                    updated_at: 1,
                    runtime_session_id: None,
                    outputs: std::collections::HashMap::new(),
                    errors: vec![],
                },
            ),
        ]);
        let (by_workflow, by_owner) = super::build_running_indexes(&sessions);
        assert_eq!(by_workflow.get("deploy"), Some(&2usize));
        assert_eq!(by_owner.get("platform"), Some(&2usize));
    }

    #[test]
    fn parse_run_registered_options_supports_empty_and_json() {
        let empty = parse_run_registered_options("  ").expect("empty body should be accepted");
        assert!(empty.vars.is_empty());
        assert_eq!(empty.max_iterations, None);

        let parsed = parse_run_registered_options(
            r#"{"vars":{"ENV":"prod","REGION":"eu"},"max_iterations":2}"#,
        )
        .expect("valid json body");
        assert_eq!(
            parsed.vars,
            std::collections::HashMap::from([
                (String::from("ENV"), String::from("prod")),
                (String::from("REGION"), String::from("eu")),
            ])
        );
        assert_eq!(parsed.max_iterations, Some(2));
    }

    #[test]
    fn parse_chat_run_request_requires_intent() {
        let err = parse_chat_run_request("{}").expect_err("intent is required");
        assert!(err.to_string().contains("requires non-empty 'intent'"));

        let parsed = parse_chat_run_request(
            r#"{"intent":"deploy","vars":{"ENV":"prod"},"max_iterations":2}"#,
        )
        .expect("valid chat request");
        assert_eq!(parsed.intent, "deploy");
        assert_eq!(parsed.vars.get("ENV").map(String::as_str), Some("prod"));
        assert_eq!(parsed.max_iterations, Some(2));
    }

    #[test]
    fn parse_chat_intents_value_handles_invalid_entries() {
        let parsed = parse_chat_intents_value("deploy=prod-deploy,ops=ops-flow,bad-entry, =empty,");
        assert_eq!(
            parsed.get("deploy").map(|v| v.workflow.as_str()),
            Some("prod-deploy")
        );
        assert_eq!(
            parsed.get("ops").map(|v| v.workflow.as_str()),
            Some("ops-flow")
        );
        assert_eq!(
            parsed.get("deploy").map(|v| v.max_iterations_cap),
            Some(None)
        );
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn parse_chat_intents_doc_supports_map_wrapped_and_list() {
        let map = parse_chat_intents_doc(
            "deploy:\n  workflow: prod-deploy\n  allowed_owners: [platform]\n  required_tags: [prod]\n  max_iterations_cap: 3\ntriage: incident-triage\n",
            "inline-map",
        )
        .expect("map format should parse");
        assert_eq!(
            map.get("deploy").map(|v| v.workflow.as_str()),
            Some("prod-deploy")
        );
        assert_eq!(
            map.get("deploy").map(|v| v.allowed_owners.clone()),
            Some(vec!["platform".to_string()])
        );
        assert_eq!(
            map.get("deploy").map(|v| v.required_tags.clone()),
            Some(vec!["prod".to_string()])
        );
        assert_eq!(
            map.get("deploy").and_then(|v| v.max_iterations_cap),
            Some(3)
        );
        assert_eq!(
            map.get("triage").map(|v| v.workflow.as_str()),
            Some("incident-triage")
        );

        let wrapped = parse_chat_intents_doc(
            "intents:\n  - intent: deploy\n    workflow: prod-deploy\n    allowed_owners: [platform]\n    required_tags: [prod]\n    max_iterations_cap: 3\n  - intent: triage\n    workflow: incident-triage\n",
            "inline-wrapped",
        )
        .expect("wrapped format should parse");
        assert_eq!(wrapped, map);

        let list = parse_chat_intents_doc(
            "- intent: deploy\n  workflow: prod-deploy\n  allowed_owners: [platform]\n  required_tags: [prod]\n  max_iterations_cap: 3\n- intent: triage\n  workflow: incident-triage\n",
            "inline-list",
        )
        .expect("list format should parse");
        assert_eq!(list, map);
    }

    #[test]
    fn parse_chat_intents_doc_rejects_empty_result() {
        let err = parse_chat_intents_doc("intents: []\n", "inline-empty")
            .expect_err("empty intent list should fail");
        assert!(err.to_string().contains("has no valid entries"));
    }

    #[tokio::test]
    async fn resolve_trigger_leadership_renews_and_fails_over() {
        let dir = std::env::temp_dir().join(format!(
            "anna-trigger-lease-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp lease dir");
        let lease_file = dir.join("trigger-lease.json");

        let node_a = TriggerLeaseConfig {
            node_id: "node-a".to_string(),
            lease_file: lease_file.clone(),
            ttl_sec: 10,
        };
        let node_b = TriggerLeaseConfig {
            node_id: "node-b".to_string(),
            lease_file: lease_file.clone(),
            ttl_sec: 10,
        };

        let a = resolve_trigger_leadership(&node_a)
            .await
            .expect("node-a should acquire lease");
        assert!(a.is_leader);
        assert_eq!(a.holder.as_deref(), Some("node-a"));

        let b = resolve_trigger_leadership(&node_b)
            .await
            .expect("node-b should observe active leader");
        assert!(!b.is_leader);
        assert_eq!(b.holder.as_deref(), Some("node-a"));

        tokio::fs::write(&lease_file, r#"{"holder":"node-a","expires_at":1}"#)
            .await
            .expect("force expired lease");
        let b_takeover = resolve_trigger_leadership(&node_b)
            .await
            .expect("node-b should take over expired lease");
        assert!(b_takeover.is_leader);
        assert_eq!(b_takeover.holder.as_deref(), Some("node-b"));
    }

    #[test]
    fn trigger_leader_state_change_ignores_lease_refresh_only() {
        let previous = super::TriggerLeaderState {
            enabled: true,
            is_leader: true,
            node_id: "node-a".to_string(),
            holder: Some("node-a".to_string()),
            expires_at: Some(100),
            lease_file: Some("/tmp/lease.json".to_string()),
        };
        let next_refresh = super::TriggerLeaderState {
            expires_at: Some(200),
            ..previous.clone()
        };
        assert!(!super::trigger_leader_state_changed(
            &previous,
            &next_refresh
        ));

        let next_holder = super::TriggerLeaderState {
            holder: Some("node-b".to_string()),
            ..previous.clone()
        };
        assert!(super::trigger_leader_state_changed(&previous, &next_holder));
    }

    #[test]
    fn trigger_leader_transition_event_names_are_stable() {
        let follower = super::TriggerLeaderState {
            enabled: true,
            is_leader: false,
            node_id: "node-a".to_string(),
            holder: Some("node-b".to_string()),
            expires_at: Some(100),
            lease_file: Some("/tmp/lease.json".to_string()),
        };
        let leader = super::TriggerLeaderState {
            enabled: true,
            is_leader: true,
            node_id: "node-a".to_string(),
            holder: Some("node-a".to_string()),
            expires_at: Some(100),
            lease_file: Some("/tmp/lease.json".to_string()),
        };
        assert_eq!(
            super::trigger_leader_transition_event(&follower, &leader),
            "trigger_leader_acquired"
        );
        assert_eq!(
            super::trigger_leader_transition_event(&leader, &follower),
            "trigger_leader_lost"
        );
    }

    #[test]
    fn evaluate_chat_intent_guardrails_blocks_owner_tag_and_max_iterations() {
        let entry = WorkflowEntry {
            file_name: "deploy.anna".to_string(),
            flow_id: Some("deploy".to_string()),
            workflow_name: "deploy".to_string(),
            path: std::path::PathBuf::from("/tmp/deploy.anna"),
            tags: vec!["prod".to_string()],
            required_capabilities: vec![],
            required_providers: vec![],
            owner: Some("platform".to_string()),
            version: None,
            max_concurrency: None,
            trigger_webhook: None,
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
            workflow_workdir: None,
        };

        let ok_rule = ChatIntentConfig {
            workflow: "deploy".to_string(),
            allowed_callers: vec!["ops-bot".to_string()],
            allowed_owners: vec!["platform".to_string()],
            required_tags: vec!["prod".to_string()],
            max_iterations_cap: Some(2),
        };
        let ok = evaluate_chat_intent_guardrails(&ok_rule, &entry, None, Some("ops-bot"));
        assert!(ok.reasons.is_empty());
        assert_eq!(ok.effective_max_iterations, Some(2));

        let blocked = evaluate_chat_intent_guardrails(&ok_rule, &entry, Some(9), Some("ops-bot"));
        assert!(!blocked.reasons.is_empty());
        assert!(blocked.reasons[0].contains("max_iterations"));

        let strict_rule = ChatIntentConfig {
            workflow: "deploy".to_string(),
            allowed_callers: vec!["release-bot".to_string()],
            allowed_owners: vec!["ops".to_string()],
            required_tags: vec!["critical".to_string()],
            max_iterations_cap: None,
        };
        let blocked = evaluate_chat_intent_guardrails(&strict_rule, &entry, None, Some("ops-bot"));
        assert_eq!(blocked.reasons.len(), 3);
        assert!(
            blocked
                .reasons
                .iter()
                .any(|v| v.contains("allowed callers"))
        );
        assert!(blocked.reasons.iter().any(|v| v.contains("allowed owners")));
        assert!(
            blocked
                .reasons
                .iter()
                .any(|v| v.contains("required chat tags"))
        );
    }

    #[test]
    fn workflow_meta_filters_match_expected_values() {
        let item = WorkflowMetaResponse {
            id: "prod-deploy".to_string(),
            workflow: "deploy".to_string(),
            file: "deploy.anna".to_string(),
            path: "/tmp/deploy.anna".to_string(),
            tags: vec!["prod".to_string(), "deploy".to_string()],
            required_capabilities: vec!["k8s".to_string(), "vault".to_string()],
            required_providers: vec!["shell".to_string(), "k8s".to_string()],
            owner: Some("platform".to_string()),
            version: Some("v1".to_string()),
            max_concurrency: Some(2),
            running: 1,
            concurrency_blocked: false,
            owner_max_concurrency: Some(3),
            owner_running: 1,
            owner_concurrency_blocked: false,
            available: false,
            missing_capabilities: vec!["vault".to_string()],
            missing_providers: vec![],
            trigger_webhook: Some("/deploy".to_string()),
            trigger_watch: None,
            trigger_cron: None,
            trigger_interval: None,
        };

        assert!(matches_workflow_meta_filters(
            &item,
            Some("prod"),
            Some("platform"),
            Some("k8s"),
            Some(false)
        ));
        assert!(!matches_workflow_meta_filters(
            &item,
            Some("staging"),
            None,
            None,
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &item,
            None,
            Some("security"),
            None,
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &item,
            None,
            None,
            Some("http"),
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &item,
            None,
            None,
            None,
            Some(true)
        ));
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

        let entries = find_workflow_entries_with_registry(&dir, None)
            .await
            .expect("find entries");
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

        let entries = find_workflow_entries_with_registry(&dir, None)
            .await
            .expect("find entries");
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
            max_hitl: 32,
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
    fn request_caller_prefers_caller_then_role() {
        let mut headers = HeaderMap::new();
        headers.insert("x-anna-caller", HeaderValue::from_static("Ops-Bot"));
        assert_eq!(super::request_caller(&headers).as_deref(), Some("ops-bot"));

        headers.remove("x-anna-caller");
        headers.insert("x-anna-role", HeaderValue::from_static("Platform"));
        assert_eq!(super::request_caller(&headers).as_deref(), Some("platform"));
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
    fn etag_matching_handles_quotes_weak_tags_and_lists() {
        assert!(super::etag_header_matches_value("\"abc\"", "abc"));
        assert!(super::etag_header_matches_value("W/\"abc\"", "abc"));
        assert!(super::etag_header_matches_value(
            "\"x\", \"abc\", \"y\"",
            "abc"
        ));
        assert!(super::etag_header_matches_value("*", "abc"));
        assert!(!super::etag_header_matches_value("\"def\"", "abc"));
        assert!(!super::etag_header_matches_value("abc", "abc"));
    }

    #[test]
    fn if_match_and_if_none_match_helpers_follow_revision() {
        let mut headers = HeaderMap::new();
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("\"rev-1\""));
        assert!(super::if_none_match_matches(&headers, "rev-1"));
        assert!(!super::if_none_match_matches(&headers, "rev-2"));

        headers.clear();
        headers.insert(IF_MATCH, HeaderValue::from_static("\"rev-1\""));
        assert!(super::if_match_allows(&headers, "rev-1"));
        assert!(!super::if_match_allows(&headers, "rev-2"));

        headers.clear();
        assert!(super::if_match_allows(&headers, "any"));
    }

    #[test]
    fn status_filter_is_case_insensitive_exact_match() {
        assert!(status_matches("running", "RUNNING"));
        assert!(status_matches("failed", " failed "));
        assert!(!status_matches("running", "run"));
    }

    #[tokio::test]
    async fn load_state_marks_running_sessions_interrupted() {
        let file = std::env::temp_dir().join(format!(
            "anna-daemon-state-{}-{}.json",
            std::process::id(),
            rand::random::<u32>()
        ));
        let mut sessions = HashMap::new();
        sessions.insert(
            "a1".to_string(),
            SessionInfo {
                id: "a1".to_string(),
                status: "running".to_string(),
                workflow: "wf".to_string(),
                owner: None,
                created_at: 1,
                updated_at: 1,
                runtime_session_id: None,
                outputs: HashMap::new(),
                errors: vec![],
            },
        );
        let snapshot = DaemonStateSnapshot {
            sessions,
            hitl: HashMap::new(),
            saved_at: 1,
        };
        tokio::fs::write(
            &file,
            serde_json::to_string(&snapshot).expect("serialize snapshot"),
        )
        .await
        .expect("write state file");

        let (loaded_sessions, _hitl) = load_daemon_state(&file).await.expect("load state");
        let loaded = loaded_sessions
            .get("a1")
            .expect("session should exist after load");
        assert_eq!(loaded.status, "interrupted");
        assert!(
            loaded
                .errors
                .iter()
                .any(|e| e.contains("daemon restarted while session was running"))
        );
    }

    #[test]
    fn temp_state_path_adds_tmp_suffix() {
        let p = std::path::PathBuf::from("/tmp/anna-state.json");
        assert_eq!(
            temp_state_path(&p),
            std::path::PathBuf::from("/tmp/anna-state.json.tmp")
        );
    }

    #[test]
    fn prune_sessions_removes_oldest_non_running_first() {
        let mut sessions = HashMap::new();
        sessions.insert(
            "running".to_string(),
            SessionInfo {
                id: "running".to_string(),
                status: "running".to_string(),
                workflow: "wf".to_string(),
                owner: None,
                created_at: 1,
                updated_at: 1,
                runtime_session_id: None,
                outputs: HashMap::new(),
                errors: vec![],
            },
        );
        sessions.insert(
            "old".to_string(),
            SessionInfo {
                id: "old".to_string(),
                status: "done".to_string(),
                workflow: "wf".to_string(),
                owner: None,
                created_at: 1,
                updated_at: 1,
                runtime_session_id: None,
                outputs: HashMap::new(),
                errors: vec![],
            },
        );
        sessions.insert(
            "new".to_string(),
            SessionInfo {
                id: "new".to_string(),
                status: "failed".to_string(),
                workflow: "wf".to_string(),
                owner: None,
                created_at: 2,
                updated_at: 2,
                runtime_session_id: None,
                outputs: HashMap::new(),
                errors: vec![],
            },
        );

        prune_sessions_in_place(&mut sessions, 2);
        assert!(sessions.contains_key("running"));
        assert!(sessions.contains_key("new"));
        assert!(!sessions.contains_key("old"));
    }

    #[test]
    fn prune_hitl_prefers_resolved_items() {
        let mut hitl = HashMap::new();
        hitl.insert(
            "pending".to_string(),
            HitlPending {
                id: "pending".to_string(),
                session_id: "s".to_string(),
                workflow: "w".to_string(),
                stage_id: "x".to_string(),
                prompt: None,
                options: vec![],
                status: "pending".to_string(),
                decision: None,
                created_at: 1,
            },
        );
        hitl.insert(
            "resolved-old".to_string(),
            HitlPending {
                id: "resolved-old".to_string(),
                session_id: "s".to_string(),
                workflow: "w".to_string(),
                stage_id: "x".to_string(),
                prompt: None,
                options: vec![],
                status: "resolved".to_string(),
                decision: Some("approve".to_string()),
                created_at: 1,
            },
        );
        hitl.insert(
            "resolved-new".to_string(),
            HitlPending {
                id: "resolved-new".to_string(),
                session_id: "s".to_string(),
                workflow: "w".to_string(),
                stage_id: "x".to_string(),
                prompt: None,
                options: vec![],
                status: "resolved".to_string(),
                decision: Some("approve".to_string()),
                created_at: 2,
            },
        );

        prune_hitl_in_place(&mut hitl, 2);
        assert!(hitl.contains_key("pending"));
        assert!(hitl.contains_key("resolved-new"));
        assert!(!hitl.contains_key("resolved-old"));
    }

    #[tokio::test]
    async fn persist_policy_snapshot_writes_effective_policy() {
        let dir = std::env::temp_dir().join(format!(
            "anna-policy-snapshot-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp policy dir");
        let path = dir.join("policy.snapshot.json");
        let state = super::AppState {
            executor: Executor::new(),
            plays_dir: dir.clone(),
            registry_file: Some(dir.join("flows.registry.yml")),
            chat_intents: Arc::new(RwLock::new(HashMap::from([(
                "deploy".to_string(),
                ChatIntentConfig {
                    workflow: "prod-deploy".to_string(),
                    allowed_callers: vec!["ops-bot".to_string()],
                    allowed_owners: vec!["platform".to_string()],
                    required_tags: vec!["prod".to_string()],
                    max_iterations_cap: Some(2),
                },
            )]))),
            trigger_lease: None,
            trigger_leader_state: Arc::new(RwLock::new(super::TriggerLeaderState {
                enabled: true,
                is_leader: true,
                node_id: "node-a".to_string(),
                holder: Some("node-a".to_string()),
                expires_at: Some(123),
                lease_file: Some("/tmp/lease.json".to_string()),
            })),
            audit_log: None,
            policy_signing_key: None,
            offline_mode: true,
            node_capabilities: HashSet::from(["shell".to_string(), "vault".to_string()]),
            allowed_providers: Some(HashSet::from(["shell".to_string(), "vault".to_string()])),
            owner_policy: super::OwnerConcurrencyPolicy {
                per_owner: HashMap::from([("platform".to_string(), 3usize)]),
                default_limit: Some(1),
            },
            sessions: Arc::new(RwLock::new(HashMap::new())),
            handles: Arc::new(RwLock::new(HashMap::new())),
            hitl: Arc::new(RwLock::new(HashMap::new())),
            auth_token: Some("secret-token".to_string()),
            retention: super::RetentionConfig {
                max_sessions: 100,
                max_hitl: 100,
            },
        };
        persist_policy_snapshot(&state, &path)
            .await
            .expect("persist policy snapshot");

        let raw = tokio::fs::read_to_string(&path)
            .await
            .expect("read policy snapshot");
        let parsed: serde_json::Value =
            serde_json::from_str(&raw).expect("policy snapshot should be valid json");
        assert_eq!(parsed["offline_mode"].as_bool(), Some(true));
        assert_eq!(
            parsed["node_capabilities"],
            serde_json::json!(["shell", "vault"])
        );
        assert_eq!(
            parsed["allowed_providers"],
            serde_json::json!(["shell", "vault"])
        );
        assert_eq!(
            parsed["chat_intents"]["deploy"]["workflow"].as_str(),
            Some("prod-deploy")
        );
        assert_eq!(
            parsed["trigger_scheduler"]["is_leader"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn sorted_set_values_returns_stable_sorted_copy() {
        let set = std::collections::HashSet::from([
            "vault".to_string(),
            "shell".to_string(),
            "http".to_string(),
        ]);
        let values = super::sorted_set_values(Some(&set));
        assert_eq!(
            values,
            vec!["http".to_string(), "shell".to_string(), "vault".to_string()]
        );
    }

    #[test]
    fn policy_revision_is_stable_for_same_core() {
        let core = serde_json::json!({
            "registry_enabled": true,
            "auth_enabled": true,
            "offline_mode": false,
            "node_capabilities": ["shell", "vault"],
            "allowed_providers": ["shell", "cli"],
            "owner_limits": [{"owner":"platform","max_concurrency":3}],
            "owner_default_limit": 1,
            "chat_intents": {
                "deploy": {
                    "workflow": "prod-deploy",
                    "allowed_callers": ["ops-bot"],
                    "allowed_owners": ["platform"],
                    "required_tags": ["prod"],
                    "max_iterations_cap": 2
                }
            },
            "trigger_policy": {
                "leader_election_enabled": true,
                "node_id": "node-a",
                "lease_file": "/tmp/lease.json"
            }
        });

        let (a_rev, a_sig) = super::policy_revision_and_signature(&core, None);
        let (b_rev, b_sig) = super::policy_revision_and_signature(&core, None);
        assert_eq!(a_rev, b_rev);
        assert_eq!(a_sig, None);
        assert_eq!(b_sig, None);
        assert_eq!(a_rev.len(), 64);
    }

    #[test]
    fn policy_revision_signature_uses_hmac_sha256() {
        let core = serde_json::json!({
            "registry_enabled": true,
            "chat_intents": {},
            "owner_limits": [],
            "node_capabilities": [],
            "allowed_providers": [],
            "trigger_policy": {"leader_election_enabled": false, "node_id": "node-a", "lease_file": serde_json::Value::Null}
        });

        let (rev_a, sig_a) = super::policy_revision_and_signature(&core, Some("secret-a"));
        let (rev_b, sig_b) = super::policy_revision_and_signature(&core, Some("secret-b"));
        assert_eq!(rev_a, rev_b);
        assert!(sig_a.is_some());
        assert!(sig_b.is_some());
        assert_ne!(sig_a, sig_b);
        assert_eq!(sig_a.as_ref().map(String::len), Some(64));
    }

    #[tokio::test]
    async fn append_audit_event_writes_ndjson_line() {
        let dir = std::env::temp_dir().join(format!(
            "anna-audit-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp audit dir");
        let path = dir.join("audit.log");
        let config = super::AuditLogConfig {
            path: path.clone(),
            node_id: "node-test".to_string(),
        };
        super::append_audit_event(
            &config,
            "workflow_launched",
            serde_json::json!({"request_id":"req-1","source":"api_workflow_named"}),
        )
        .await
        .expect("append audit event");

        let raw = tokio::fs::read_to_string(&path)
            .await
            .expect("read audit log");
        let line = raw.lines().last().expect("audit line should exist");
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("audit line should be valid json");
        assert_eq!(parsed["event"].as_str(), Some("workflow_launched"));
        assert_eq!(parsed["node_id"].as_str(), Some("node-test"));
        assert_eq!(parsed["data"]["request_id"].as_str(), Some("req-1"));
        assert_eq!(
            parsed["data"]["source"].as_str(),
            Some("api_workflow_named")
        );
        assert!(parsed["ts"].as_u64().is_some());
    }
}
