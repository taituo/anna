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
use serde_json::{Value, json};
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

struct ChatIntentCheckRenderContext {
    normalized_intent: String,
    caller: Option<String>,
    requested_max_iterations: Option<u32>,
    guardrails: ChatIntentGuardrailOutcome,
    readiness: FlowReadiness,
}

struct BlockedChatIntentContext<'a> {
    intent: &'a str,
    raw_intent: &'a str,
    workflow_name: &'a str,
    caller: Option<String>,
    reasons: Vec<String>,
    requested_max_iterations: Option<u32>,
}

struct LaunchChatIntentContext {
    intent: String,
    caller_identity: Option<String>,
    workflow_name: String,
    workflow_reference: String,
    vars: HashMap<String, String>,
    effective_max_iterations: Option<u32>,
}

struct ChatIntentRunResolution {
    request: ChatRunRequest,
    intent: String,
    rule: ChatIntentConfig,
    workflow_entry: WorkflowEntry,
}

const CHAT_GATEWAY_DISABLED_MESSAGE: &str =
    "chat gateway disabled: set ANNA_CHAT_INTENTS or ANNA_CHAT_INTENTS_FILE";

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
    let app_state_daemon = AppState {
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
    tokio::spawn(trigger_scheduler_loop(app_state_daemon.clone()));
    spawn_optional_daemon_tasks(
        app_state_daemon.clone(),
        state_file.clone(),
        policy_snapshot_file.clone(),
        chat_reload_interval,
    );
    let startup_log_context = StartupLogContext {
        registry_file: registry_file.as_ref(),
        chat_intents: &chat_intents,
        trigger_lease: trigger_lease.as_ref(),
        node_id: &node_id,
        audit_log: audit_log.as_ref(),
        offline_mode,
        node_capabilities: &node_capabilities,
        allowed_providers: allowed_providers.as_ref(),
        owner_policy: &owner_policy,
    };
    log_daemon_startup_config(&startup_log_context);
    let startup_policy_core = build_policy_core(&app_state_daemon).await;
    let (startup_policy_revision, _policy_signature) =
        policy_revision_and_signature(&startup_policy_core, app_state_daemon.policy_signing_key.as_deref());
    emit_audit_event(
        &app_state_daemon,
        "daemon_started",
        json!({
            "bind": bind,
            "registry_enabled": app_state_daemon.registry_file.is_some(),
            "auth_enabled": app_state_daemon.auth_token.is_some(),
            "offline_mode": app_state_daemon.offline_mode,
            "chat_intents_count": chat_intents.len(),
            "trigger_lease_enabled": trigger_lease.is_some(),
            "policy_revision": startup_policy_revision,
            "allowed_providers": sorted_set_values(app_state_daemon.allowed_providers.as_ref()),
            "node_capabilities": sorted_set_values(Some(&app_state_daemon.node_capabilities)),
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
        .with_state(app_state_daemon);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("anna-rs daemon listening on http://{}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}

fn spawn_optional_daemon_tasks(
    state: AppState,
    state_file: Option<PathBuf>,
    policy_snapshot_file: Option<PathBuf>,
    chat_reload_interval: Option<Duration>,
) {
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
    if chat_intents_file().is_some() {
        if let Some(interval) = chat_reload_interval {
            println!(
                "anna-rs chat intents hot reload enabled (interval={}s)",
                interval.as_secs()
            );
            tokio::spawn(chat_intents_reload_loop(state, interval));
        } else {
            println!("anna-rs chat intents hot reload disabled");
        }
    }
}

struct StartupLogContext<'a> {
    registry_file: Option<&'a PathBuf>,
    chat_intents: &'a HashMap<String, ChatIntentConfig>,
    trigger_lease: Option<&'a TriggerLeaseConfig>,
    node_id: &'a str,
    audit_log: Option<&'a AuditLogConfig>,
    offline_mode: bool,
    node_capabilities: &'a HashSet<String>,
    allowed_providers: Option<&'a HashSet<String>>,
    owner_policy: &'a OwnerConcurrencyPolicy,
}

fn log_daemon_startup_config(ctx: &StartupLogContext<'_>) {
    if let Some(path) = ctx.registry_file {
        println!("anna-rs flow registry enabled at {}", path.display());
    }
    if !ctx.chat_intents.is_empty() {
        let mut routes = ctx
            .chat_intents
            .iter()
            .map(|(intent, rule)| format!("{}={}", intent, rule.workflow))
            .collect::<Vec<_>>();
        routes.sort();
        let chat_routes_joined = routes.join(",");
        println!("anna-rs chat intents: {chat_routes_joined}");
    }
    match ctx.trigger_lease {
        Some(lease) => {
            println!(
                "anna-rs trigger lease: file={} ttl={}s node_id={}",
                lease.lease_file.display(),
                lease.ttl_sec,
                lease.node_id
            );
        }
        None => {
            println!("anna-rs trigger lease: disabled node_id={}", ctx.node_id);
        }
    }
    if let Some(audit) = ctx.audit_log {
        println!("anna-rs audit log enabled at {}", audit.path.display());
    }
    if ctx.offline_mode {
        println!("anna-rs offline mode enabled (deterministic provider ceiling active)");
    }
    log_node_capabilities(ctx.node_capabilities);
    log_allowed_providers_policy(ctx.allowed_providers);
    log_owner_concurrency_policy(ctx.owner_policy);
}

fn log_node_capabilities(node_capabilities: &HashSet<String>) {
    if node_capabilities.is_empty() {
        return;
    }
    let mut capabilities = Vec::with_capacity(node_capabilities.len());
    for capability in node_capabilities {
        capabilities.push(capability.to_owned());
    }
    capabilities.sort();
    let node_caps_joined = capabilities.join(",");
    println!("anna-rs node capabilities: {node_caps_joined}");
}

fn log_allowed_providers_policy(allowed_providers: Option<&HashSet<String>>) {
    let Some(allowed) = allowed_providers else {
        return;
    };
    let mut providers = Vec::with_capacity(allowed.len());
    for provider in allowed {
        providers.push(provider.to_owned());
    }
    providers.sort();
    let providers_joined = providers.join(",");
    println!("anna-rs allowed providers policy: {providers_joined}");
}

fn log_owner_concurrency_policy(owner_policy: &OwnerConcurrencyPolicy) {
    if owner_policy.per_owner.is_empty() && owner_policy.default_limit.is_none() {
        return;
    }
    let mut entries = owner_policy
        .per_owner
        .iter()
        .map(|(owner, limit)| format!("{}={}", owner, limit))
        .collect::<Vec<_>>();
    entries.sort();
    if let Some(default_limit) = owner_policy.default_limit {
        entries.push(format!("*={}", default_limit));
    }
    let owner_limits_joined = entries.join(",");
    println!("anna-rs owner concurrency policy: {owner_limits_joined}");
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn policy(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let policy_core_body = build_policy_core(&state).await;
    let (policy_revision, policy_signature) =
        policy_revision_and_signature(&policy_core_body, state.policy_signing_key.as_deref());
    if !if_match_allows(&headers, &policy_revision) {
        return precondition_failed_with_etag(&policy_revision);
    }
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }

    let (node_caps_sorted, allowed_provider_list, owner_limits) = policy_response_vectors(&state);

    let chat_intents_count = state.chat_intents.read().await.len();
    let trigger_leader_snapshot = state.trigger_leader_state.read().await.clone();
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
        trigger_leader_election_enabled: trigger_leader_snapshot.enabled,
        trigger_scheduler_leader: trigger_leader_snapshot.is_leader,
        trigger_scheduler_node_id: trigger_leader_snapshot.node_id,
        trigger_scheduler_holder: trigger_leader_snapshot.holder,
        trigger_scheduler_expires_at: trigger_leader_snapshot.expires_at,
        trigger_lease_file: trigger_leader_snapshot.lease_file,
        node_capabilities: node_caps_sorted,
        provider_restriction_enabled: state.allowed_providers.is_some(),
        allowed_providers: allowed_provider_list,
        owner_limits,
        owner_default_limit: state.owner_policy.default_limit,
        retention_max_sessions: state.retention.max_sessions,
        retention_max_hitl: state.retention.max_hitl,
    })
    .into_response();
    set_etag_header(&mut response, &etag_revision);
    response
}

fn policy_response_vectors(
    state: &AppState,
) -> (Vec<String>, Vec<String>, Vec<OwnerLimitEntry>) {
    let mut node_caps_sorted = Vec::with_capacity(state.node_capabilities.len());
    for capability in &state.node_capabilities {
        node_caps_sorted.push(capability.to_owned());
    }
    node_caps_sorted.sort();

    let mut allowed_provider_list = state
        .allowed_providers
        .as_ref()
        .map(|set| {
            let mut provider_values = Vec::with_capacity(set.len());
            for value in set {
                provider_values.push(value.to_owned());
            }
            provider_values
        })
        .unwrap_or_default();
    allowed_provider_list.sort();

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
    (node_caps_sorted, allowed_provider_list, owner_limits)
}

async fn policy_revision(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let policy_core_revision = build_policy_core(&state).await;
    let (policy_revision, policy_signature) =
        policy_revision_and_signature(&policy_core_revision, state.policy_signing_key.as_deref());
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }
    let revision_etag = policy_revision.clone();
    let mut http_response_revision = Json(PolicyRevisionResponse {
        policy_revision,
        signed: policy_signature.is_some(),
        policy_signature,
        policy_signature_algorithm: state
            .policy_signing_key
            .as_ref()
            .map(|_| "hmac-sha256".to_string()),
    })
    .into_response();
    set_etag_header(&mut http_response_revision, &revision_etag);
    http_response_revision
}

async fn policy_snapshot(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let policy_core_snapshot = build_policy_core(&state).await;
    let (policy_revision, _policy_signature) =
        policy_revision_and_signature(&policy_core_snapshot, state.policy_signing_key.as_deref());
    if !if_match_allows(&headers, &policy_revision) {
        return precondition_failed_with_etag(&policy_revision);
    }
    if if_none_match_matches(&headers, &policy_revision) {
        return not_modified_with_etag(&policy_revision);
    }
    let mut http_response_snapshot = Json(build_policy_snapshot(&state).await).into_response();
    set_etag_header(&mut http_response_snapshot, &policy_revision);
    http_response_snapshot
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
    let hitl_entries = state.hitl.read().await;

    let sessions_total = sessions.len();
    let sessions_running = sessions.values().filter(|v| v.status == "running").count();
    let sessions_done = sessions.values().filter(|v| v.status == "done").count();
    let sessions_failed = sessions.values().filter(|v| v.status == "failed").count();
    let sessions_other =
        sessions_total.saturating_sub(sessions_running + sessions_done + sessions_failed);

    let hitl_total = hitl_entries.len();
    let hitl_pending = hitl_entries
        .values()
        .filter(|v| v.status == "pending")
        .count();
    let hitl_resolved = hitl_entries
        .values()
        .filter(|v| v.status == "resolved")
        .count();

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
    let workflow_entries =
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
    let sessions_guard_meta_read = state.sessions.read().await;
    let (running_by_workflow, running_by_owner) = build_running_indexes(&sessions_guard_meta_read);
    drop(sessions_guard_meta_read);

    let mut out = workflow_entries
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
                flow_readiness_runtime(running, None, owner_running, owner_max_concurrency),
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
        let owner_filter_value = owner_filter.trim();
        items.retain(|v| {
            v.owner
                .as_deref()
                .map(|owner| owner.eq_ignore_ascii_case(owner_filter_value))
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
    let mut workflow = match parse_workflow_body_or_response(&body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    if workflow.workdir.is_none() {
        workflow.workdir = Some(state.plays_dir.display().to_string());
    }
    if let Err(resp) = validate_allowed_providers_or_response(&workflow, state.allowed_providers.as_ref()) {
        return resp;
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

fn validate_allowed_providers_or_response(
    workflow: &Workflow,
    allowed_providers: Option<&HashSet<String>>,
) -> std::result::Result<(), axum::response::Response> {
    let Some(allowed_providers_set) = allowed_providers else {
        return Ok(());
    };
    let required_providers = collect_required_providers(workflow);
    let mut missing_required_providers = required_providers
        .into_iter()
        .filter(|provider| !allowed_providers_set.contains(provider))
        .collect::<Vec<_>>();
    missing_required_providers.sort();
    missing_required_providers.dedup();
    if missing_required_providers.is_empty() {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        format!(
            "workflow requires blocked providers: {}",
            missing_required_providers.join(", ")
        ),
    )
        .into_response())
}

async fn check_workflow_body(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let parsed_workflow_check = match parse_workflow_body_or_response(&body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let (required_providers, missing_providers) =
        collect_provider_restrictions(&parsed_workflow_check, state.allowed_providers.as_ref());

    (
        StatusCode::OK,
        Json(RawFlowCheckResponse {
            workflow: parsed_workflow_check.name,
            can_run: missing_providers.is_empty(),
            provider_restriction_enabled: state.allowed_providers.is_some(),
            required_providers,
            missing_providers,
        }),
    )
        .into_response()
}

fn parse_workflow_body_or_response(body: &str) -> std::result::Result<Workflow, axum::response::Response> {
    let parsed_workflow: Workflow = match serde_yaml::from_str(body) {
        Ok(v) => v,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid workflow yaml: {}", err),
            )
                .into_response());
        }
    };
    if let Err(err) = parsed_workflow.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("workflow validation failed: {}", err),
        )
            .into_response());
    }
    Ok(parsed_workflow)
}

fn collect_provider_restrictions(
    workflow: &Workflow,
    allowed_providers: Option<&HashSet<String>>,
) -> (Vec<String>, Vec<String>) {
    let mut workflow_required_providers = collect_required_providers(workflow);
    let mut missing_providers = allowed_providers
        .map(|allowed| {
            workflow_required_providers
                .iter()
                .filter(|provider| !allowed.contains(*provider))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    workflow_required_providers.sort();
    workflow_required_providers.dedup();
    missing_providers.sort();
    missing_providers.dedup();
    (workflow_required_providers, missing_providers)
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
    let entry = match resolve_registered_entry_or_response(&state, &name).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let launched_req_id_named = match launch_registered_entry_with_options(
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
            id: launched_req_id_named,
            status: "running".to_string(),
        }),
    )
        .into_response()
}

async fn resolve_registered_entry_or_response(
    state: &AppState,
    name: &str,
) -> std::result::Result<WorkflowEntry, axum::response::Response> {
    match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        name,
    )
    .await
    {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err((StatusCode::NOT_FOUND, "workflow not found").into_response()),
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed resolving workflow: {}", err),
        )
            .into_response()),
    }
}

fn ensure_chat_gateway_enabled_or_response(
    chat_intents: &HashMap<String, ChatIntentConfig>,
) -> std::result::Result<(), axum::response::Response> {
    if chat_intents.is_empty() {
        return Err((StatusCode::SERVICE_UNAVAILABLE, CHAT_GATEWAY_DISABLED_MESSAGE).into_response());
    }
    Ok(())
}

fn resolve_chat_intent_rule_or_response(
    chat_intents: &HashMap<String, ChatIntentConfig>,
    raw_intent: &str,
) -> std::result::Result<(String, ChatIntentConfig), axum::response::Response> {
    let normalized_intent = raw_intent.trim().to_ascii_lowercase();
    match chat_intents.get(&normalized_intent).cloned() {
        Some(rule) => Ok((normalized_intent, rule)),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("chat intent '{}' is not configured", raw_intent),
        )
            .into_response()),
    }
}

async fn resolve_chat_intent_workflow_or_response(
    state: &AppState,
    raw_intent: &str,
    rule: &ChatIntentConfig,
) -> std::result::Result<WorkflowEntry, axum::response::Response> {
    match resolve_registered_workflow_entry_with_registry(
        &state.plays_dir,
        state.registry_file.as_deref(),
        &rule.workflow,
    )
    .await
    {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            format!(
                "chat intent '{}' maps to missing workflow '{}'",
                raw_intent, rule.workflow
            ),
        )
            .into_response()),
        Err(err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed resolving chat intent workflow: {}", err),
        )
            .into_response()),
    }
}

fn chat_intent_check_response(
    workflow_entry: &WorkflowEntry,
    rule: &ChatIntentConfig,
    render_context: ChatIntentCheckRenderContext,
) -> axum::response::Response {
    let can_run =
        render_context.readiness.can_run() && render_context.guardrails.reasons.is_empty();
    (
        StatusCode::OK,
        Json(ChatIntentCheckResponse {
            intent: render_context.normalized_intent,
            workflow: workflow_entry.workflow_name.clone(),
            can_run,
            caller: render_context.caller,
            guardrail_reasons: render_context.guardrails.reasons,
            running: render_context.readiness.running,
            requested_max_iterations: render_context.requested_max_iterations,
            effective_max_iterations: render_context.guardrails.effective_max_iterations,
            max_iterations_cap: rule.max_iterations_cap,
            max_concurrency: render_context.readiness.max_concurrency.map(|v| v as u32),
            concurrency_blocked: render_context.readiness.concurrency_blocked,
            owner_running: render_context.readiness.owner_running,
            owner_max_concurrency: render_context
                .readiness
                .owner_max_concurrency
                .map(|v| v as u32),
            owner_concurrency_blocked: render_context.readiness.owner_concurrency_blocked,
            missing_capabilities: render_context.readiness.missing_capabilities,
            missing_providers: render_context.readiness.missing_providers,
        }),
    )
        .into_response()
}

async fn blocked_chat_intent_response(
    state: &AppState,
    blocked_context: BlockedChatIntentContext<'_>,
) -> axum::response::Response {
    emit_audit_event(
        state,
        "chat_intent_blocked",
        json!({
            "intent": blocked_context.intent,
            "workflow": blocked_context.workflow_name,
            "caller": blocked_context.caller,
            "reasons": blocked_context.reasons,
            "requested_max_iterations": blocked_context.requested_max_iterations,
        }),
    )
    .await;
    (
        StatusCode::FORBIDDEN,
        format!(
            "chat intent '{}' blocked by guardrails: {}",
            blocked_context.raw_intent,
            blocked_context.reasons.join("; ")
        ),
    )
        .into_response()
}

async fn launch_chat_intent_response(
    state: &AppState,
    workflow_entry: &WorkflowEntry,
    launch_context: LaunchChatIntentContext,
) -> std::result::Result<axum::response::Response, axum::response::Response> {
    let run_options = RunRegisteredOptions {
        vars: launch_context.vars,
        max_iterations: launch_context.effective_max_iterations,
    };
    let launch_source = format!("chat_intent:{}", launch_context.intent);
    let launched_req_id_chat = launch_registered_entry_with_options(
        state,
        workflow_entry,
        &launch_context.workflow_reference,
        run_options,
        &launch_source,
    )
    .await?;
    emit_audit_event(
        state,
        "chat_intent_launched",
        json!({
            "intent": launch_context.intent.clone(),
            "workflow": launch_context.workflow_name.clone(),
            "caller": launch_context.caller_identity,
            "request_id": launched_req_id_chat.clone(),
            "effective_max_iterations": launch_context.effective_max_iterations,
        }),
    )
    .await;

    Ok((
        StatusCode::ACCEPTED,
        Json(ChatRunResponse {
            intent: launch_context.intent,
            workflow: launch_context.workflow_name,
            id: launched_req_id_chat,
            status: "running".to_string(),
            effective_max_iterations: launch_context.effective_max_iterations,
        }),
    )
        .into_response())
}

async fn build_chat_intent_launch_context_or_response(
    state: &AppState,
    run_resolution: ChatIntentRunResolution,
    caller_identity: Option<String>,
) -> std::result::Result<(WorkflowEntry, LaunchChatIntentContext), axum::response::Response> {
    let ChatIntentRunResolution {
        request,
        intent,
        rule,
        workflow_entry,
    } = run_resolution;
    let run_guardrails = evaluate_chat_intent_guardrails(
        &rule,
        &workflow_entry,
        request.max_iterations,
        caller_identity.as_deref(),
    );
    if !run_guardrails.reasons.is_empty() {
        return Err(
            blocked_chat_intent_response(
                state,
                BlockedChatIntentContext {
                    intent: &intent,
                    raw_intent: &request.intent,
                    workflow_name: &workflow_entry.workflow_name,
                    caller: caller_identity,
                    reasons: run_guardrails.reasons,
                    requested_max_iterations: request.max_iterations,
                },
            )
            .await,
        );
    }
    let launch_context = LaunchChatIntentContext {
        intent,
        caller_identity,
        workflow_name: workflow_entry.workflow_name.clone(),
        workflow_reference: rule.workflow,
        vars: request.vars,
        effective_max_iterations: run_guardrails.effective_max_iterations,
    };
    Ok((workflow_entry, launch_context))
}

async fn resolve_chat_intent_run_context_or_response(
    state: &AppState,
    raw_body: &str,
    chat_intents_run_route: &HashMap<String, ChatIntentConfig>,
) -> std::result::Result<ChatIntentRunResolution, axum::response::Response> {
    let request = match parse_chat_run_request(raw_body) {
        Ok(v) => v,
        Err(err) => return Err((StatusCode::BAD_REQUEST, err.to_string()).into_response()),
    };
    let (intent, rule) =
        resolve_chat_intent_rule_or_response(chat_intents_run_route, &request.intent)?;
    let workflow_entry =
        resolve_chat_intent_workflow_or_response(state, &request.intent, &rule).await?;
    Ok(ChatIntentRunResolution {
        request,
        intent,
        rule,
        workflow_entry,
    })
}

async fn list_chat_intents(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let chat_intents_map = state.chat_intents.read().await.clone();
    let mut intents = chat_intents_map
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
    let chat_intents_check_route = state.chat_intents.read().await.clone();
    if let Err(resp) = ensure_chat_gateway_enabled_or_response(&chat_intents_check_route) {
        return resp;
    }
    let (normalized_intent, rule) =
        match resolve_chat_intent_rule_or_response(&chat_intents_check_route, &intent) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let workflow_entry_check_route =
        match resolve_chat_intent_workflow_or_response(&state, &intent, &rule).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let guardrails = evaluate_chat_intent_guardrails(
        &rule,
        &workflow_entry_check_route,
        query.max_iterations,
        caller.as_deref(),
    );
    let (running, owner_running) = running_counts_for_entry(&state, &workflow_entry_check_route).await;
    let owner_limit = owner_limit_for(workflow_entry_check_route.owner.as_deref(), &state.owner_policy);
    let flow_readiness = evaluate_flow_readiness(
        &workflow_entry_check_route,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        flow_readiness_runtime(running, None, owner_running, owner_limit),
    );
    let render_context = ChatIntentCheckRenderContext {
        normalized_intent,
        caller,
        requested_max_iterations: query.max_iterations,
        guardrails,
        readiness: flow_readiness,
    };
    chat_intent_check_response(
        &workflow_entry_check_route,
        &rule,
        render_context,
    )
}

async fn run_chat_intent(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let caller_identity = request_caller(&headers);

    let chat_intents_run_route = state.chat_intents.read().await.clone();
    if let Err(resp) = ensure_chat_gateway_enabled_or_response(&chat_intents_run_route) {
        return resp;
    }

    let run_resolution =
        match resolve_chat_intent_run_context_or_response(&state, &body, &chat_intents_run_route).await {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    let (workflow_entry_run_route, launch_context) = match build_chat_intent_launch_context_or_response(
        &state,
        run_resolution,
        caller_identity,
    )
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match launch_chat_intent_response(&state, &workflow_entry_run_route, launch_context).await {
        Ok(resp) => resp,
        Err(resp) => return resp,
    }
}

async fn check_registered_workflow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }

    let workflow_entry_registered_check = match resolve_registered_entry_or_response(&state, &name).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let (running, owner_running) = running_counts_for_entry(&state, &workflow_entry_registered_check).await;
    let owner_limit_registered =
        owner_limit_for(workflow_entry_registered_check.owner.as_deref(), &state.owner_policy);
    let flow_readiness_registered = evaluate_flow_readiness(
        &workflow_entry_registered_check,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        flow_readiness_runtime(running, None, owner_running, owner_limit_registered),
    );
    (
        StatusCode::OK,
        Json(FlowCheckResponse {
            id: workflow_public_id(&workflow_entry_registered_check),
            workflow: workflow_entry_registered_check.workflow_name,
            file: workflow_entry_registered_check.file_name,
            path: workflow_entry_registered_check.path.display().to_string(),
            owner: workflow_entry_registered_check.owner,
            can_run: flow_readiness_registered.can_run(),
            running: flow_readiness_registered.running,
            max_concurrency: flow_readiness_registered.max_concurrency.map(|v| v as u32),
            concurrency_blocked: flow_readiness_registered.concurrency_blocked,
            owner_running: flow_readiness_registered.owner_running,
            owner_max_concurrency: flow_readiness_registered
                .owner_max_concurrency
                .map(|v| v as u32),
            owner_concurrency_blocked: flow_readiness_registered.owner_concurrency_blocked,
            missing_capabilities: flow_readiness_registered.missing_capabilities,
            missing_providers: flow_readiness_registered.missing_providers,
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
    let sessions_guard_status = state.sessions.read().await;
    match sessions_guard_status.get(&id) {
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
    let stopped = stop_workflow_task_if_running(&mut handles, &id);
    drop(handles);

    let mut sessions_guard_mut = state.sessions.write().await;
    if let Some(updated) =
        update_stopped_session(&mut sessions_guard_mut, &id, stopped, state.retention.max_sessions)
    {
        drop(sessions_guard_mut);
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
    drop(sessions_guard_mut);

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

fn stop_workflow_task_if_running(
    handles: &mut HashMap<String, JoinHandle<()>>,
    request_id: &str,
) -> bool {
    if let Some(handle) = handles.remove(request_id) {
        handle.abort();
        true
    } else {
        false
    }
}

fn update_stopped_session(
    sessions: &mut HashMap<String, SessionInfo>,
    request_id: &str,
    stopped: bool,
    max_sessions: usize,
) -> Option<SessionInfo> {
    let info = sessions.get_mut(request_id)?;
    info.status = if stopped { "stopped" } else { "not_running" }.to_string();
    info.updated_at = now_unix_secs();
    let updated = info.clone();
    prune_sessions_in_place(sessions, max_sessions);
    Some(updated)
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
    if let Some(response) = reject_hook_on_follower(&state, &hook_path).await {
        return response;
    }
    let hook_entries =
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

    let hook_outcomes_summary = collect_hook_outcomes(&state, hook_entries, &hook_path).await;

    if hook_outcomes_summary.is_empty() {
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
            "launched": hook_outcomes_summary.launched.len(),
            "skipped_running": hook_outcomes_summary.skipped_running.len(),
            "skipped_capability": hook_outcomes_summary.skipped_capability.len(),
            "skipped_provider": hook_outcomes_summary.skipped_provider.len(),
            "skipped_concurrency": hook_outcomes_summary.skipped_concurrency.len(),
        }),
    )
    .await;
    (
        StatusCode::ACCEPTED,
        Json(HookTriggerResponse {
            hook: hook_path,
            launched: hook_outcomes_summary.launched,
            skipped_running: hook_outcomes_summary.skipped_running,
            skipped_capability: hook_outcomes_summary.skipped_capability,
            skipped_provider: hook_outcomes_summary.skipped_provider,
            skipped_concurrency: hook_outcomes_summary.skipped_concurrency,
        }),
    )
        .into_response()
}

struct HookOutcomes {
    launched: Vec<HookLaunchedWorkflow>,
    skipped_running: Vec<String>,
    skipped_capability: Vec<HookSkippedCapability>,
    skipped_provider: Vec<HookSkippedProvider>,
    skipped_concurrency: Vec<HookSkippedConcurrency>,
}

impl HookOutcomes {
    fn new() -> Self {
        Self {
            launched: Vec::new(),
            skipped_running: Vec::new(),
            skipped_capability: Vec::new(),
            skipped_provider: Vec::new(),
            skipped_concurrency: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.launched.is_empty()
            && self.skipped_running.is_empty()
            && self.skipped_capability.is_empty()
            && self.skipped_provider.is_empty()
            && self.skipped_concurrency.is_empty()
    }
}

async fn reject_hook_on_follower(state: &AppState, hook_path: &str) -> Option<axum::response::Response> {
    let leader_state = state.trigger_leader_state.read().await.clone();
    if !leader_state.enabled || leader_state.is_leader {
        return None;
    }
    emit_audit_event(
        state,
        "hook_rejected_not_leader",
        json!({
            "hook": hook_path.to_string(),
            "node_id": leader_state.node_id.clone(),
            "leader": leader_state.holder.clone(),
            "expires_at": leader_state.expires_at,
        }),
    )
    .await;
    Some(
        (
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
            .into_response(),
    )
}

async fn collect_hook_outcomes(
    state: &AppState,
    entries: Vec<WorkflowEntry>,
    hook_path: &str,
) -> HookOutcomes {
    let mut outcomes = HookOutcomes::new();
    for entry in entries {
        let Some(webhook) = entry.trigger_webhook.as_deref() else {
            continue;
        };
        if webhook.trim() != hook_path {
            continue;
        }
        let launch = launch_workflow_from_entry(state, &entry, "webhook").await;
        apply_hook_launch_outcome(&mut outcomes, entry.workflow_name, launch);
    }
    outcomes
}

fn apply_hook_launch_outcome(
    outcomes: &mut HookOutcomes,
    workflow_name: String,
    launch: Result<TriggerLaunchOutcome>,
) {
    match launch {
        Ok(TriggerLaunchOutcome::Launched(session_id)) => {
            outcomes.launched.push(HookLaunchedWorkflow {
                workflow: workflow_name,
                session_id,
            });
        }
        Ok(TriggerLaunchOutcome::SkippedRunning) => outcomes.skipped_running.push(workflow_name),
        Ok(TriggerLaunchOutcome::SkippedCapability(missing_capabilities)) => {
            outcomes.skipped_capability.push(HookSkippedCapability {
                workflow: workflow_name,
                missing_capabilities,
            });
        }
        Ok(TriggerLaunchOutcome::SkippedProvider(missing_providers)) => {
            outcomes.skipped_provider.push(HookSkippedProvider {
                workflow: workflow_name,
                missing_providers,
            });
        }
        Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running,
            max_concurrency,
        }) => {
            outcomes.skipped_concurrency.push(HookSkippedConcurrency {
                workflow: workflow_name,
                running,
                max_concurrency,
            });
        }
        Err(_) => {}
    }
}

async fn workflow_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = ensure_authorized(&state, &headers) {
        return resp;
    }
    let sessions_guard_logs = state.sessions.read().await;
    let Some(info) = sessions_guard_logs.get(&id) else {
        return (StatusCode::NOT_FOUND, "session not found").into_response();
    };
    let Some(runtime_id) = info.runtime_session_id.clone() else {
        return (StatusCode::CONFLICT, "runtime session not available yet").into_response();
    };
    drop(sessions_guard_logs);

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

    let mut hitl_items = state
        .hitl
        .read()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    if let Some(filter) = query.status.as_deref() {
        hitl_items.retain(|v| status_matches(&v.status, filter));
    }
    if let Some(session_filter) = query.session_id.as_deref() {
        hitl_items.retain(|v| v.session_id == session_filter);
    }
    if let Some(workflow_filter) = query.workflow.as_deref() {
        hitl_items.retain(|v| v.workflow.eq_ignore_ascii_case(workflow_filter.trim()));
    }
    hitl_items.sort_by_key(|v| v.created_at);
    if let Some(limit) = query.limit {
        hitl_items.truncate(limit);
    }
    Json(hitl_items).into_response()
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

    let mut pending_hitl_map = state.hitl.write().await;
    if !pending_hitl_map.contains_key(&id) {
        return (StatusCode::NOT_FOUND, "hitl request not found").into_response();
    }
    let resolved_hitl = {
        let item = pending_hitl_map
            .get_mut(&id)
            .expect("hitl request exists after contains_key check");
        item.decision = Some(decision);
        item.status = "resolved".to_string();
        item.clone()
    };
    prune_hitl_in_place(&mut pending_hitl_map, state.retention.max_hitl);
    drop(pending_hitl_map);
    emit_audit_event(
        &state,
        "hitl_resolved",
        json!({
            "hitl_id": resolved_hitl.id,
            "session_id": resolved_hitl.session_id,
            "workflow": resolved_hitl.workflow,
            "stage_id": resolved_hitl.stage_id,
            "decision": resolved_hitl.decision,
        }),
    )
    .await;
    (StatusCode::OK, Json(resolved_hitl)).into_response()
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

        let trigger_entries = match find_workflow_entries_with_registry(
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

        run_interval_triggers(&state, &trigger_entries, &mut scheduler).await;
        run_cron_triggers(&state, &trigger_entries, &mut scheduler).await;
        run_watch_triggers(&state, &trigger_entries, &mut scheduler).await;
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
    let lease_now_unix = now_unix_secs();
    if let Some(current) = read_trigger_lease_file(&config.lease_file).await?
        && current.holder != config.node_id
        && current.expires_at > lease_now_unix
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
        expires_at: lease_now_unix.saturating_add(config.ttl_sec),
    };
    write_trigger_lease_file(&config.lease_file, &candidate).await?;

    let observed = read_trigger_lease_file(&config.lease_file).await?;
    let lease_is_leader = observed
        .as_ref()
        .map(|lease| lease.holder == config.node_id && lease.expires_at > lease_now_unix)
        .unwrap_or(false);
    Ok(TriggerLeaderState {
        enabled: true,
        is_leader: lease_is_leader,
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
    let mut interval_seen_keys = HashSet::new();
    let interval_now = Instant::now();

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

        let interval_key = trigger_key(entry, "interval", raw_interval);
        interval_seen_keys.insert(interval_key.to_owned());
        let interval_next_run = scheduler
            .interval_next
            .entry(interval_key)
            .or_insert_with(|| interval_now + interval);
        if interval_now >= *interval_next_run {
            log_trigger_launch_outcome("interval", entry, launch_workflow_from_entry(state, entry, "interval").await);
            *interval_next_run = Instant::now() + interval;
        }
    }

    scheduler
        .interval_next
        .retain(|k, _| interval_seen_keys.contains(k));
}

async fn run_cron_triggers(
    state: &AppState,
    entries: &[WorkflowEntry],
    scheduler: &mut TriggerScheduler,
) {
    let mut cron_seen_keys = HashSet::new();
    let cron_now = Utc::now();

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

        let cron_key = trigger_key(entry, "cron", raw_cron);
        cron_seen_keys.insert(cron_key.to_owned());
        let cron_next_run = scheduler.cron_next.entry(cron_key).or_insert_with(|| {
            schedule
                .after(&cron_now)
                .next()
                .unwrap_or_else(|| cron_now + chrono::Duration::days(1))
        });

        if *cron_next_run <= cron_now {
            log_trigger_launch_outcome("cron", entry, launch_workflow_from_entry(state, entry, "cron").await);
            *cron_next_run = schedule
                .after(&cron_now)
                .next()
                .unwrap_or_else(|| cron_now + chrono::Duration::days(1));
        }
    }

    scheduler.cron_next.retain(|k, _| cron_seen_keys.contains(k));
}

async fn run_watch_triggers(
    state: &AppState,
    entries: &[WorkflowEntry],
    scheduler: &mut TriggerScheduler,
) {
    let mut watch_seen_keys = HashSet::new();
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

        let watch_key = trigger_key(entry, "watch", &pattern);
        watch_seen_keys.insert(watch_key.to_owned());

        let watch_snapshot = match collect_watch_snapshot(&pattern) {
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

        let changed = match scheduler.watch_snapshots.get(&watch_key) {
            None => false,
            Some(previous) => previous != &watch_snapshot,
        };
        scheduler.watch_snapshots.insert(watch_key, watch_snapshot);

        if changed {
            log_watch_launch_outcome(entry, launch_workflow_from_entry(state, entry, "watch").await);
        }
    }

    scheduler
        .watch_snapshots
        .retain(|k, _| watch_seen_keys.contains(k));
}

fn log_watch_launch_outcome(
    entry: &WorkflowEntry,
    outcome: Result<TriggerLaunchOutcome>,
) {
    log_trigger_launch_outcome("watch", entry, outcome);
}

fn log_trigger_launch_outcome(
    trigger_kind: &str,
    entry: &WorkflowEntry,
    outcome: Result<TriggerLaunchOutcome>,
) {
    match outcome {
        Ok(TriggerLaunchOutcome::Launched(_)) | Ok(TriggerLaunchOutcome::SkippedRunning) => {}
        Ok(TriggerLaunchOutcome::SkippedCapability(missing)) => {
            eprintln!(
                "anna-rs scheduler: skipped {trigger_kind} trigger '{}' due to missing capabilities: {}",
                entry.workflow_name,
                missing.join(", ")
            );
        }
        Ok(TriggerLaunchOutcome::SkippedProvider(missing)) => {
            eprintln!(
                "anna-rs scheduler: skipped {trigger_kind} trigger '{}' due to blocked providers: {}",
                entry.workflow_name,
                missing.join(", ")
            );
        }
        Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running,
            max_concurrency,
        }) => {
            eprintln!(
                "anna-rs scheduler: skipped {trigger_kind} trigger '{}' due to concurrency limit running={} max={}",
                entry.workflow_name, running, max_concurrency
            );
        }
        Err(err) => {
            eprintln!(
                "anna-rs scheduler: failed launching {trigger_kind} trigger for '{}': {}",
                entry.path.display(),
                err
            );
        }
    }
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

    let mut missing_capabilities_list = entry
        .required_capabilities
        .iter()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .filter(|required| !node_capabilities.contains(&required.to_ascii_lowercase()))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    missing_capabilities_list.sort();
    missing_capabilities_list.dedup();
    missing_capabilities_list
}

fn collect_required_providers(workflow: &Workflow) -> Vec<String> {
    let mut provider_set_required = HashSet::new();
    for stage in &workflow.stages {
        if stage.workflow.is_none() {
            provider_set_required.insert(stage.provider_name().trim().to_ascii_lowercase());
        }
        if stage.vote.is_some() {
            provider_set_required.insert("llm".to_string());
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
            provider_set_required.insert("shell".to_string());
        }
    }
    let mut required_provider_list = provider_set_required
        .into_iter()
        .filter(|v| !v.trim().is_empty())
        .collect::<Vec<_>>();
    required_provider_list.sort();
    required_provider_list
}

fn missing_required_providers(
    entry: &WorkflowEntry,
    allowed_providers: Option<&HashSet<String>>,
) -> Vec<String> {
    let Some(allowed) = allowed_providers else {
        return Vec::new();
    };

    let mut missing_provider_list = entry
        .required_providers
        .iter()
        .map(|provider| provider.trim().to_ascii_lowercase())
        .filter(|provider| !provider.is_empty())
        .filter(|provider| !allowed.contains(provider))
        .collect::<Vec<_>>();
    missing_provider_list.sort();
    missing_provider_list.dedup();
    missing_provider_list
}

fn normalize_max_concurrency(raw: Option<u32>) -> Option<usize> {
    raw.map(|v| v as usize).filter(|v| *v >= 1)
}

fn evaluate_flow_readiness(
    entry: &WorkflowEntry,
    node_capabilities: &HashSet<String>,
    allowed_providers: Option<&HashSet<String>>,
    runtime: FlowReadinessRuntime,
) -> FlowReadiness {
    let missing_capabilities = missing_required_capabilities(entry, node_capabilities);
    let blocked_provider_list = missing_required_providers(entry, allowed_providers);
    let max_concurrency = normalize_max_concurrency(entry.max_concurrency)
        .or(runtime.default_max_concurrency.filter(|v| *v >= 1));
    let concurrency_blocked = max_concurrency
        .map(|max| runtime.running >= max)
        .unwrap_or(false);
    let owner_concurrency_blocked = runtime
        .owner_max_concurrency
        .map(|max| runtime.owner_running >= max)
        .unwrap_or(false);
    FlowReadiness {
        missing_capabilities,
        missing_providers: blocked_provider_list,
        max_concurrency,
        running: runtime.running,
        concurrency_blocked,
        owner_running: runtime.owner_running,
        owner_max_concurrency: runtime.owner_max_concurrency,
        owner_concurrency_blocked,
    }
}

#[derive(Debug, Clone, Copy)]
struct FlowReadinessRuntime {
    running: usize,
    default_max_concurrency: Option<usize>,
    owner_running: usize,
    owner_max_concurrency: Option<usize>,
}

fn flow_readiness_runtime(
    running: usize,
    default_max_concurrency: Option<usize>,
    owner_running: usize,
    owner_max_concurrency: Option<usize>,
) -> FlowReadinessRuntime {
    FlowReadinessRuntime {
        running,
        default_max_concurrency,
        owner_running,
        owner_max_concurrency,
    }
}

fn collect_watch_snapshot(pattern: &str) -> Result<HashMap<String, u64>> {
    let mut watch_snapshot_map = HashMap::new();
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
        watch_snapshot_map.insert(path.to_string_lossy().into_owned(), fingerprint);
    }
    Ok(watch_snapshot_map)
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
    let audit_log_value = raw.trim();
    if audit_log_value.is_empty()
        || audit_log_value.eq_ignore_ascii_case("off")
        || audit_log_value.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(AuditLogConfig {
        path: PathBuf::from(audit_log_value),
        node_id: node_id.to_string(),
    })
}

fn daemon_policy_signing_key() -> Option<String> {
    let Ok(raw) = std::env::var("ANNA_POLICY_SIGNING_KEY") else {
        return None;
    };
    let signing_key_value = raw.trim();
    if signing_key_value.is_empty()
        || signing_key_value.eq_ignore_ascii_case("off")
        || signing_key_value.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(signing_key_value.to_string())
}

fn sorted_set_values(set: Option<&HashSet<String>>) -> Vec<String> {
    let mut sorted_values = Vec::new();
    if let Some(set_values) = set {
        sorted_values.reserve(set_values.len());
        for value in set_values {
            sorted_values.push(value.to_owned());
        }
    }
    sorted_values.sort();
    sorted_values
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
        let node_id_value = raw.trim();
        if !node_id_value.is_empty() {
            return node_id_value.to_string();
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
    let lease_file_value = raw.trim();
    if lease_file_value.is_empty()
        || lease_file_value.eq_ignore_ascii_case("off")
        || lease_file_value.eq_ignore_ascii_case("false")
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
        lease_file: PathBuf::from(lease_file_value),
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
    let chat_reload_value = raw.trim();
    if chat_reload_value.is_empty()
        || chat_reload_value.eq_ignore_ascii_case("off")
        || chat_reload_value.eq_ignore_ascii_case("false")
        || chat_reload_value == "0"
    {
        return None;
    }
    match chat_reload_value.parse::<u64>() {
        Ok(seconds) if seconds >= 1 => Some(Duration::from_secs(seconds)),
        _ => {
            eprintln!(
                "anna-rs daemon: invalid ANNA_CHAT_INTENTS_RELOAD_SEC='{}' (expected integer >=1, or off/false/0)",
                chat_reload_value
            );
            Some(Duration::from_secs(2))
        }
    }
}

fn daemon_chat_intents() -> HashMap<String, ChatIntentConfig> {
    let mut merged_intents = HashMap::new();

    if let Some(path) = chat_intents_file() {
        match load_chat_intents_file(&path) {
            Ok(file_intents) => merged_intents.extend(file_intents),
            Err(err) => eprintln!("anna-rs daemon: failed loading chat intents file: {}", err),
        }
    }

    if let Ok(raw) = std::env::var("ANNA_CHAT_INTENTS") {
        // Explicit env mapping overrides file entries when keys collide.
        merged_intents.extend(parse_chat_intents_value(&raw));
    }

    merged_intents
}

fn parse_chat_intents_value(raw: &str) -> HashMap<String, ChatIntentConfig> {
    let mut parsed_intents = HashMap::new();
    for item in raw.split([',', ';', '\n']) {
        let chat_intent_item_trimmed = item.trim();
        if chat_intent_item_trimmed.is_empty() {
            continue;
        }
        let Some((intent_raw, workflow_raw)) = chat_intent_item_trimmed.split_once('=') else {
            eprintln!(
                "anna-rs daemon: ignoring invalid ANNA_CHAT_INTENTS entry '{}'",
                chat_intent_item_trimmed
            );
            continue;
        };
        insert_chat_intent_entry(
            &mut parsed_intents,
            intent_raw,
            ChatIntentConfig {
                workflow: workflow_raw.to_string(),
                allowed_callers: Vec::new(),
                allowed_owners: Vec::new(),
                required_tags: Vec::new(),
                max_iterations_cap: None,
            },
            "ANNA_CHAT_INTENTS",
            chat_intent_item_trimmed,
        );
    }
    parsed_intents
}

fn chat_intents_file() -> Option<PathBuf> {
    let Ok(raw) = std::env::var("ANNA_CHAT_INTENTS_FILE") else {
        return None;
    };
    let intents_file_value = raw.trim();
    if intents_file_value.is_empty()
        || intents_file_value.eq_ignore_ascii_case("off")
        || intents_file_value.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(PathBuf::from(intents_file_value))
}

fn load_chat_intents_file(path: &FsPath) -> Result<HashMap<String, ChatIntentConfig>> {
    let chat_intents_doc_raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading '{}'", path.display()))?;
    parse_chat_intents_doc(&chat_intents_doc_raw, &path.display().to_string())
}

fn parse_chat_intents_doc(raw: &str, source: &str) -> Result<HashMap<String, ChatIntentConfig>> {
    let parsed: ChatIntentDoc = serde_yaml::from_str(raw)
        .with_context(|| format!("failed parsing chat intents from '{}'", source))?;
    let mut intent_config_map = HashMap::new();
    match parsed {
        ChatIntentDoc::Wrapped { intents } | ChatIntentDoc::List(intents) => {
            insert_list_chat_intents(&mut intent_config_map, intents, source);
        }
        ChatIntentDoc::Map(map) => insert_map_chat_intents(&mut intent_config_map, map, source),
    }
    if intent_config_map.is_empty() {
        bail!("chat intents source '{}' has no valid entries", source);
    }
    Ok(intent_config_map)
}

fn insert_list_chat_intents(
    out: &mut HashMap<String, ChatIntentConfig>,
    intents: Vec<ChatIntentEntry>,
    source: &str,
) {
    for item in intents {
        let raw_entry_label = format!("intent='{}' workflow='{}'", item.intent, item.config.workflow);
        insert_chat_intent_entry(
            out,
            &item.intent,
            chat_intent_config_from_doc(item.config),
            source,
            &raw_entry_label,
        );
    }
}

fn chat_intent_config_from_doc(doc: ChatIntentConfigDoc) -> ChatIntentConfig {
    ChatIntentConfig {
        workflow: doc.workflow,
        allowed_callers: doc.allowed_callers,
        allowed_owners: doc.allowed_owners,
        required_tags: doc.required_tags,
        max_iterations_cap: doc.max_iterations_cap,
    }
}

fn parse_chat_intent_map_entry(
    intent: &str,
    value: ChatIntentMapValue,
) -> (ChatIntentConfig, String) {
    match value {
        ChatIntentMapValue::Workflow(workflow) => (
            ChatIntentConfig {
                workflow: workflow.clone(),
                allowed_callers: Vec::new(),
                allowed_owners: Vec::new(),
                required_tags: Vec::new(),
                max_iterations_cap: None,
            },
            format!("{}={}", intent, workflow),
        ),
        ChatIntentMapValue::Config(config_doc) => {
            let map_raw_entry_label = format!("intent='{}' workflow='{}'", intent, config_doc.workflow);
            (chat_intent_config_from_doc(config_doc), map_raw_entry_label)
        }
    }
}

fn insert_map_chat_intents(
    out: &mut HashMap<String, ChatIntentConfig>,
    map: HashMap<String, ChatIntentMapValue>,
    source: &str,
) {
    for (intent, value) in map {
        let (config, map_entry_raw) = parse_chat_intent_map_entry(&intent, value);
        insert_chat_intent_entry(out, &intent, config, source, &map_entry_raw);
    }
}

fn normalize_sorted_dedup(values: &[String]) -> Vec<String> {
    let mut canonical_values = values
        .iter()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    canonical_values.sort();
    canonical_values.dedup();
    canonical_values
}

fn normalize_owner_list(values: &[String]) -> Vec<String> {
    let mut canonical_owners = values
        .iter()
        .filter_map(|owner| owner_key(Some(owner.as_str())))
        .collect::<Vec<_>>();
    canonical_owners.sort();
    canonical_owners.dedup();
    canonical_owners
}

fn normalized_chat_intent_entry_or_none(
    intent_raw: &str,
    mut config: ChatIntentConfig,
    source: &str,
    raw_entry: &str,
) -> Option<(String, ChatIntentConfig)> {
    let normalized_intent_key = intent_raw.trim().to_ascii_lowercase();
    config.workflow = config.workflow.trim().to_string();
    if normalized_intent_key.is_empty() || config.workflow.is_empty() {
        eprintln!(
            "anna-rs daemon: ignoring invalid chat intent entry '{}' from {}",
            raw_entry, source
        );
        return None;
    }
    if config.max_iterations_cap == Some(0) {
        eprintln!(
            "anna-rs daemon: ignoring invalid chat intent entry '{}' from {} (max_iterations_cap must be >=1)",
            raw_entry, source
        );
        return None;
    }
    config.allowed_callers = normalize_sorted_dedup(&config.allowed_callers);
    config.allowed_owners = normalize_owner_list(&config.allowed_owners);
    config.required_tags = normalize_sorted_dedup(&config.required_tags);
    Some((normalized_intent_key, config))
}

fn insert_chat_intent_entry(
    out: &mut HashMap<String, ChatIntentConfig>,
    intent_raw: &str,
    config: ChatIntentConfig,
    source: &str,
    raw_entry: &str,
) {
    let Some((normalized_intent_key, normalized_config)) =
        normalized_chat_intent_entry_or_none(intent_raw, config, source, raw_entry)
    else {
        return;
    };
    out.insert(normalized_intent_key, normalized_config);
}

fn evaluate_chat_intent_guardrails(
    rule: &ChatIntentConfig,
    entry: &WorkflowEntry,
    requested_max_iterations: Option<u32>,
    caller: Option<&str>,
) -> ChatIntentGuardrailOutcome {
    let mut reasons = Vec::new();
    guardrail_allowed_caller_reasons(rule, caller, &mut reasons);
    guardrail_allowed_owner_reasons(rule, entry, &mut reasons);
    guardrail_required_tag_reasons(rule, entry, &mut reasons);
    let guardrail_effective_iterations =
        guardrail_effective_max_iterations(rule.max_iterations_cap, requested_max_iterations, &mut reasons);
    ChatIntentGuardrailOutcome {
        reasons,
        effective_max_iterations: guardrail_effective_iterations,
    }
}

fn guardrail_allowed_caller_reasons(
    rule: &ChatIntentConfig,
    caller: Option<&str>,
    reasons: &mut Vec<String>,
) {
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
}

fn guardrail_allowed_owner_reasons(
    rule: &ChatIntentConfig,
    entry: &WorkflowEntry,
    reasons: &mut Vec<String>,
) {
    if !rule.allowed_owners.is_empty() {
        let workflow_owner = owner_key(entry.owner.as_deref());
        let owner_is_allowed = workflow_owner
            .as_ref()
            .map(|owner| rule.allowed_owners.contains(owner))
            .unwrap_or(false);
        if !owner_is_allowed {
            reasons.push(format!(
                "workflow owner '{}' is not allowed (allowed owners: {})",
                entry.owner.as_deref().unwrap_or(""),
                rule.allowed_owners.join(", ")
            ));
        }
    }
}

fn guardrail_required_tag_reasons(
    rule: &ChatIntentConfig,
    entry: &WorkflowEntry,
    reasons: &mut Vec<String>,
) {
    if !rule.required_tags.is_empty() {
        let entry_tags = entry
            .tags
            .iter()
            .map(|tag| tag.trim().to_ascii_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect::<HashSet<_>>();
        let mut missing_required_tags = rule
            .required_tags
            .iter()
            .filter(|tag| !entry_tags.contains(*tag))
            .cloned()
            .collect::<Vec<_>>();
        missing_required_tags.sort();
        missing_required_tags.dedup();
        if !missing_required_tags.is_empty() {
            reasons.push(format!(
                "workflow missing required chat tags: {}",
                missing_required_tags.join(", ")
            ));
        }
    }
}

fn guardrail_effective_max_iterations(
    max_iterations_cap: Option<u32>,
    requested_max_iterations: Option<u32>,
    reasons: &mut Vec<String>,
) -> Option<u32> {
    let mut resolved_max_iterations = requested_max_iterations;
    if let Some(cap) = max_iterations_cap {
        match requested_max_iterations {
            Some(value) if value > cap => reasons.push(format!(
                "requested max_iterations={} exceeds chat cap={}",
                value, cap
            )),
            Some(_) => {}
            None => {
                resolved_max_iterations = Some(cap);
            }
        }
    }
    resolved_max_iterations
}

fn daemon_owner_concurrency_policy() -> OwnerConcurrencyPolicy {
    let Ok(raw) = std::env::var("ANNA_OWNER_MAX_CONCURRENCY") else {
        return OwnerConcurrencyPolicy::default();
    };
    let mut policy = OwnerConcurrencyPolicy::default();

    for item in raw.split([',', ';', '\n']) {
        let owner_limit_item = item.trim();
        if owner_limit_item.is_empty() {
            continue;
        }
        let Some((owner_raw, limit_raw)) = owner_limit_item.split_once('=') else {
            eprintln!(
                "anna-rs daemon: ignoring invalid ANNA_OWNER_MAX_CONCURRENCY entry '{}'",
                owner_limit_item
            );
            continue;
        };
        let owner_key_value = owner_raw.trim().to_ascii_lowercase();
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
        if owner_key_value == "*" {
            policy.default_limit = Some(limit);
        } else if !owner_key_value.is_empty() {
            policy.per_owner.insert(owner_key_value, limit);
        }
    }

    policy
}

fn owner_limit_for(owner: Option<&str>, policy: &OwnerConcurrencyPolicy) -> Option<usize> {
    let normalized_owner = owner_key(owner)?;
    policy
        .per_owner
        .get(&normalized_owner)
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
        let flow_registry_value = raw.trim();
        if flow_registry_value.is_empty()
            || flow_registry_value.eq_ignore_ascii_case("off")
            || flow_registry_value.eq_ignore_ascii_case("false")
        {
            return None;
        }
        return Some(PathBuf::from(flow_registry_value));
    }
    None
}

fn daemon_state_file() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("ANNA_DAEMON_STATE_FILE") {
        let state_file_value = raw.trim();
        if state_file_value.is_empty() || state_file_value.eq_ignore_ascii_case("off") {
            return None;
        }
        return Some(PathBuf::from(state_file_value));
    }
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".anna/daemon-state.json"))
}

fn daemon_policy_snapshot_file() -> Option<PathBuf> {
    let Ok(raw) = std::env::var("ANNA_POLICY_SNAPSHOT_FILE") else {
        return None;
    };
    let policy_snapshot_value = raw.trim();
    if policy_snapshot_value.is_empty()
        || policy_snapshot_value.eq_ignore_ascii_case("off")
        || policy_snapshot_value.eq_ignore_ascii_case("false")
    {
        return None;
    }
    Some(PathBuf::from(policy_snapshot_value))
}

async fn load_daemon_state(
    path: &FsPath,
) -> Result<(HashMap<String, SessionInfo>, HashMap<String, HitlPending>)> {
    let daemon_state_raw = match tokio::fs::read_to_string(path).await {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HashMap::new(), HashMap::new()));
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed reading daemon state file '{}'", path.display()));
        }
    };

    let mut daemon_state_snapshot: DaemonStateSnapshot = serde_json::from_str(&daemon_state_raw)
        .with_context(|| format!("failed parsing daemon state file '{}'", path.display()))?;
    let recovered_at = now_unix_secs();
    for (id, session) in &mut daemon_state_snapshot.sessions {
        if session.id.trim().is_empty() {
            session.id = id.to_owned();
        }
        if session.status == "running" {
            session.status = "interrupted".to_string();
            session.updated_at = recovered_at;
            session
                .errors
                .push("daemon restarted while session was running".to_string());
        }
    }
    for (id, hitl) in &mut daemon_state_snapshot.hitl {
        if hitl.id.trim().is_empty() {
            hitl.id = id.to_owned();
        }
    }
    Ok((daemon_state_snapshot.sessions, daemon_state_snapshot.hitl))
}

async fn chat_intents_reload_loop(state: AppState, interval: Duration) {
    loop {
        sleep(interval).await;
        let reloaded_intents = daemon_chat_intents();
        let mut write = state.chat_intents.write().await;
        if *write != reloaded_intents {
            *write = reloaded_intents.to_owned();
            let mut reloaded_routes = reloaded_intents
                .iter()
                .map(|(intent, rule)| format!("{}={}", intent, rule.workflow))
                .collect::<Vec<_>>();
            reloaded_routes.sort();
            if reloaded_routes.is_empty() {
                println!("anna-rs chat intents reloaded: <empty>");
            } else {
                let reloaded_routes_joined = reloaded_routes.join(",");
                println!("anna-rs chat intents reloaded: {reloaded_routes_joined}");
            }
            drop(write);
            emit_audit_event(
                &state,
                "chat_intents_reloaded",
                json!({
                    "count": reloaded_intents.len(),
                    "routes": reloaded_routes,
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
    let persisted_sessions = state.sessions.read().await.clone();
    let persisted_hitl = state.hitl.read().await.clone();
    let persisted_state_snapshot = DaemonStateSnapshot {
        sessions: persisted_sessions,
        hitl: persisted_hitl,
        saved_at: now_unix_secs(),
    };
    let daemon_state_raw_json = serde_json::to_string_pretty(&persisted_state_snapshot)?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating daemon state directory '{}'",
                parent.display()
            )
        })?;
    }

    let daemon_state_tmp_path = temp_state_path(path);
    tokio::fs::write(&daemon_state_tmp_path, daemon_state_raw_json)
        .await
        .with_context(|| format!("failed writing daemon state temp file '{}'", daemon_state_tmp_path.display()))?;
    tokio::fs::rename(&daemon_state_tmp_path, path).await.with_context(|| {
        format!(
            "failed moving daemon state file '{}' -> '{}'",
            daemon_state_tmp_path.display(),
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
    let policy_snapshot_doc = build_policy_snapshot(state).await;
    let policy_snapshot_raw_json =
        serde_json::to_string_pretty(&policy_snapshot_doc).context("serialize policy snapshot json")?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating policy snapshot directory '{}'",
                parent.display()
            )
        })?;
    }

    let policy_snapshot_tmp_path = temp_state_path(path);
    tokio::fs::write(&policy_snapshot_tmp_path, policy_snapshot_raw_json)
        .await
        .with_context(|| format!("failed writing policy snapshot temp '{}'", policy_snapshot_tmp_path.display()))?;
    tokio::fs::rename(&policy_snapshot_tmp_path, path).await.with_context(|| {
        format!(
            "failed moving policy snapshot '{}' -> '{}'",
            policy_snapshot_tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

async fn build_policy_snapshot(state: &AppState) -> serde_json::Value {
    let trigger_state_snapshot = state.trigger_leader_state.read().await.clone();
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
            "enabled": trigger_state_snapshot.enabled,
            "is_leader": trigger_state_snapshot.is_leader,
            "node_id": trigger_state_snapshot.node_id,
            "holder": trigger_state_snapshot.holder,
            "expires_at": trigger_state_snapshot.expires_at,
            "lease_file": trigger_state_snapshot.lease_file,
        },
        "policy_core": policy_core,
    })
}

async fn build_policy_core(state: &AppState) -> serde_json::Value {
    let trigger_state_core = state.trigger_leader_state.read().await.clone();
    let chat_intents_snapshot = state.chat_intents.read().await.clone();
    let owner_limit_entries_json = owner_limit_entries_json(state);
    let chat_map = chat_intents_policy_map(chat_intents_snapshot);

    json!({
        "registry_enabled": state.registry_file.is_some(),
        "auth_enabled": state.auth_token.is_some(),
        "offline_mode": state.offline_mode,
        "node_capabilities": sorted_set_values(Some(&state.node_capabilities)),
        "allowed_providers": sorted_set_values(state.allowed_providers.as_ref()),
        "owner_limits": owner_limit_entries_json,
        "owner_default_limit": state.owner_policy.default_limit,
        "chat_intents": chat_map,
        "trigger_policy": {
            "leader_election_enabled": trigger_state_core.enabled,
            "node_id": trigger_state_core.node_id,
            "lease_file": trigger_state_core.lease_file,
        },
    })
}

fn owner_limit_entries_json(state: &AppState) -> Vec<Value> {
    let mut owner_entries = state
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
    owner_entries.sort_by(|a, b| {
        a.get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .cmp(b.get("owner").and_then(|v| v.as_str()).unwrap_or(""))
    });
    owner_entries
}

fn chat_intents_policy_map(
    chat_intents_snapshot: HashMap<String, ChatIntentConfig>,
) -> serde_json::Map<String, Value> {
    let mut chat_routes = chat_intents_snapshot.into_iter().collect::<Vec<_>>();
    chat_routes.sort_by(|a, b| a.0.cmp(&b.0));
    let mut intent_map = serde_json::Map::new();
    for (intent, rule) in chat_routes {
        intent_map.insert(
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
    intent_map
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
    let mut tmp_path_state = path.to_path_buf();
    if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
        tmp_path_state.set_file_name(format!("{}.tmp", name));
        return tmp_path_state;
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

    let mut removable_hitl = hitl
        .iter()
        .filter(|(_, h)| h.status == "resolved")
        .map(|(id, h)| (id.clone(), h.created_at))
        .collect::<Vec<_>>();
    removable_hitl.sort_by_key(|(_, created_at)| *created_at);

    let mut hitl_remove_count = hitl.len().saturating_sub(max_hitl);
    for (id, _) in removable_hitl {
        if hitl_remove_count == 0 {
            break;
        }
        if hitl.remove(&id).is_some() {
            hitl_remove_count -= 1;
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

fn normalize_etag_value(raw_etag: &str) -> Option<String> {
    let mut normalized_etag_candidate = raw_etag.trim();
    if let Some(rest) = normalized_etag_candidate.strip_prefix("W/") {
        normalized_etag_candidate = rest.trim();
    }
    let stripped = normalized_etag_candidate.strip_prefix('"')?.strip_suffix('"')?;
    let normalized_etag = stripped.trim();
    if normalized_etag.is_empty() {
        return None;
    }
    Some(normalized_etag.to_string())
}

fn etag_header_matches_value(raw: &str, revision: &str) -> bool {
    let revision_trimmed = revision.trim();
    raw.split(',')
        .map(str::trim)
        .any(|candidate| match candidate {
            "*" => true,
            other => normalize_etag_value(other)
                .map(|tag| tag == revision_trimmed)
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
    let mut not_modified_response = StatusCode::NOT_MODIFIED.into_response();
    set_etag_header(&mut not_modified_response, revision);
    not_modified_response
}

fn precondition_failed_with_etag(revision: &str) -> axum::response::Response {
    let mut precondition_response = (
        StatusCode::PRECONDITION_FAILED,
        "policy revision precondition failed",
    )
        .into_response();
    set_etag_header(&mut precondition_response, revision);
    precondition_response
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
        .map(ToOwned::to_owned)
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
        *by_workflow.entry(session.workflow.to_owned()).or_insert(0) += 1;
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
        let normalized_owner_filter = item
            .owner
            .as_deref()
            .map(|v| v.trim().to_ascii_lowercase())
            .unwrap_or_default();
        if normalized_owner_filter != required {
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
    let mut workflow_ids = find_workflow_entries_with_registry(root, registry_file)
        .await?
        .into_iter()
        .map(|v| workflow_public_id(&v))
        .collect::<Vec<_>>();
    workflow_ids.sort();
    workflow_ids.dedup();
    Ok(workflow_ids)
}

async fn read_session_logs(runtime_session_id: &str) -> Result<HashMap<String, String>> {
    let mut session_logs = HashMap::new();
    let dir_path = session_dir(runtime_session_id);
    let mut read_dir_handle = tokio::fs::read_dir(&dir_path).await?;
    while let Some(entry) = read_dir_handle.next_entry().await? {
        let log_file_path = entry.path();
        if log_file_path.is_file()
            && log_file_path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == "log")
                .unwrap_or(false)
            && let Some(stage_id) = log_file_path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(str::to_owned)
        {
            let content = tokio::fs::read_to_string(&log_file_path).await.unwrap_or_default();
            session_logs.insert(stage_id, content);
        }
    }
    Ok(session_logs)
}

async fn stream_logs(mut socket: WebSocket, state: AppState, req_id: String) {
    let mut last_payload = String::new();

    loop {
        let (payload, should_close) = build_ws_payload(&state, &req_id).await;
        if payload != last_payload {
            if socket
                .send(Message::Text(payload.as_str().into()))
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
    let session_info_opt = { state.sessions.read().await.get(req_id).cloned() };
    let Some(info) = session_info_opt else {
        let ws_frame_not_found = WsLogFrame {
            id: req_id.to_string(),
            status: "not_found".to_string(),
            runtime_session_id: None,
            logs: HashMap::new(),
            errors: vec!["session not found".to_string()],
        };
        return (to_json(ws_frame_not_found), true);
    };

    let logs = match info.runtime_session_id.as_deref() {
        Some(runtime) => read_session_logs(runtime).await.unwrap_or_default(),
        None => HashMap::new(),
    };
    let should_close = matches!(info.status.as_str(), "done" | "failed" | "stopped");
    let ws_frame = WsLogFrame {
        id: info.id,
        status: info.status,
        runtime_session_id: info.runtime_session_id,
        logs,
        errors: info.errors,
    };
    (to_json(ws_frame), should_close)
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
    let registry_raw_doc = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed reading flow registry '{}'", path.display()))?;
    let registry_doc: FlowRegistryDoc = serde_yaml::from_str(&registry_raw_doc)
        .with_context(|| format!("failed parsing flow registry '{}'", path.display()))?;

    let mut registry_entries_value = match registry_doc {
        FlowRegistryDoc::Wrapped { flows } => flows,
        FlowRegistryDoc::List(items) => items,
    };
    if registry_entries_value.is_empty() {
        bail!("flow registry '{}' has no entries", path.display());
    }

    let mut seen_flow_ids = HashSet::new();
    for registry_entry in &mut registry_entries_value {
        normalize_registry_entry_fields(registry_entry);
        validate_registry_entry_or_bail(registry_entry, path, &mut seen_flow_ids)?;
    }
    Ok(registry_entries_value)
}

fn normalize_registry_entry_fields(registry_entry: &mut FlowRegistryEntry) {
    registry_entry.flow_id = registry_entry.flow_id.trim().to_string();
    registry_entry.path = registry_entry.path.trim().to_string();
    registry_entry.owner = registry_entry
        .owner
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    registry_entry.version = registry_entry
        .version
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
}

fn validate_registry_entry_or_bail(
    registry_entry: &FlowRegistryEntry,
    path: &FsPath,
    seen_flow_ids: &mut HashSet<String>,
) -> Result<()> {
    if registry_entry.flow_id.is_empty() {
        bail!(
            "flow registry '{}' has entry with empty flow_id",
            path.display()
        );
    }
    if registry_entry.path.is_empty() {
        bail!(
            "flow registry '{}' has entry '{}' with empty path",
            path.display(),
            registry_entry.flow_id
        );
    }
    if !seen_flow_ids.insert(registry_entry.flow_id.to_owned()) {
        bail!(
            "flow registry '{}' has duplicate flow_id '{}'",
            path.display(),
            registry_entry.flow_id
        );
    }
    if registry_entry.max_concurrency == Some(0) {
        bail!(
            "flow registry '{}' has entry '{}' with invalid max_concurrency=0",
            path.display(),
            registry_entry.flow_id
        );
    }
    Ok(())
}

fn resolve_registry_workflow_path(plays_dir: &FsPath, raw_path: &str) -> PathBuf {
    let resolved_path = PathBuf::from(raw_path.trim());
    if resolved_path.is_absolute() {
        resolved_path
    } else {
        plays_dir.join(resolved_path)
    }
}

async fn find_workflow_entries_with_registry(
    root: &FsPath,
    registry_file: Option<&FsPath>,
) -> Result<Vec<WorkflowEntry>> {
    if let Some(registry_path) = registry_file {
        let loaded_registry_entries = load_flow_registry(registry_path).await?;
        let mut registry_workflow_entries = Vec::new();
        for spec in loaded_registry_entries {
            let registry_workflow_path = resolve_registry_workflow_path(root, &spec.path);
            let registry_workflow = Workflow::load(&registry_workflow_path).with_context(|| {
                format!(
                    "flow registry '{}' entry '{}' points to invalid workflow '{}'",
                    registry_path.display(),
                    spec.flow_id,
                    registry_workflow_path.display()
                )
            })?;
            let file_name = registry_workflow_path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    anyhow::anyhow!("invalid workflow filename in '{}'", registry_workflow_path.display())
                })?;
            let registry_required_providers = collect_required_providers(&registry_workflow);
            registry_workflow_entries.push(WorkflowEntry {
                file_name,
                flow_id: Some(spec.flow_id),
                workflow_name: registry_workflow.name,
                path: registry_workflow_path,
                tags: spec.tags,
                required_capabilities: spec.required_capabilities,
                required_providers: registry_required_providers,
                owner: spec.owner,
                version: spec.version,
                max_concurrency: spec.max_concurrency,
                trigger_webhook: registry_workflow.trigger.webhook,
                trigger_watch: registry_workflow.trigger.watch,
                trigger_cron: registry_workflow.trigger.cron,
                trigger_interval: registry_workflow.trigger.interval,
                workflow_workdir: registry_workflow.workdir,
            });
        }
        return Ok(registry_workflow_entries);
    }

    let mut discovered_workflow_entries = Vec::new();
    let mut root_dir_reader = tokio::fs::read_dir(root).await?;
    while let Some(entry) = root_dir_reader.next_entry().await? {
        let workflow_path = entry.path();
        if !workflow_path.is_file() {
            continue;
        }
        if !workflow_path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e == "anna")
            .unwrap_or(false)
        {
            continue;
        }
        let Some(file_name) = workflow_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
        else {
            continue;
        };

        let discovered_workflow = match Workflow::load(&workflow_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let discovered_required_providers = collect_required_providers(&discovered_workflow);
        discovered_workflow_entries.push(WorkflowEntry {
            file_name,
            flow_id: None,
            workflow_name: discovered_workflow.name,
            path: workflow_path,
            tags: vec![],
            required_capabilities: vec![],
            required_providers: discovered_required_providers,
            owner: None,
            version: None,
            max_concurrency: None,
            trigger_webhook: discovered_workflow.trigger.webhook,
            trigger_watch: discovered_workflow.trigger.watch,
            trigger_cron: discovered_workflow.trigger.cron,
            trigger_interval: discovered_workflow.trigger.interval,
            workflow_workdir: discovered_workflow.workdir,
        });
    }
    Ok(discovered_workflow_entries)
}

async fn resolve_registered_workflow_entry_with_registry(
    root: &FsPath,
    registry_file: Option<&FsPath>,
    name: &str,
) -> Result<Option<WorkflowEntry>> {
    let normalized = name.trim();
    let registry_or_discovered_entries = find_workflow_entries_with_registry(root, registry_file).await?;
    for entry in &registry_or_discovered_entries {
        if entry.file_name == normalized
            || entry.workflow_name == normalized
            || entry.flow_id.as_deref() == Some(normalized)
        {
            return Ok(Some(entry.to_owned()));
        }
    }

    if !normalized.ends_with(".anna") {
        let candidate_file_name = format!("{}.anna", normalized);
        for entry in &registry_or_discovered_entries {
            if entry.file_name == candidate_file_name {
                return Ok(Some(entry.to_owned()));
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
    let owner_limit_launch = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness_launch = evaluate_flow_readiness(
        entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        flow_readiness_runtime(running, Some(1), owner_running, owner_limit_launch),
    );
    if !readiness_launch.missing_capabilities.is_empty() {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: missing capabilities [{}]",
            trigger_source,
            entry.workflow_name,
            readiness_launch.missing_capabilities.join(", ")
        );
        return Ok(TriggerLaunchOutcome::SkippedCapability(
            readiness_launch.missing_capabilities,
        ));
    }
    if !readiness_launch.missing_providers.is_empty() {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: blocked providers [{}]",
            trigger_source,
            entry.workflow_name,
            readiness_launch.missing_providers.join(", ")
        );
        return Ok(TriggerLaunchOutcome::SkippedProvider(
            readiness_launch.missing_providers,
        ));
    }

    if readiness_launch.owner_concurrency_blocked {
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: owner limit running={} max={}",
            trigger_source,
            entry.workflow_name,
            readiness_launch.owner_running,
            readiness_launch.owner_max_concurrency.unwrap_or(0)
        );
        return Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running: readiness_launch.owner_running,
            max_concurrency: readiness_launch.owner_max_concurrency.unwrap_or(0),
        });
    }

    if readiness_launch.concurrency_blocked {
        let readiness_max_concurrency = readiness_launch.max_concurrency.unwrap_or(1);
        if readiness_max_concurrency == 1 {
            println!(
                "anna-rs daemon trigger={} workflow='{}' skipped: already running",
                trigger_source, entry.workflow_name
            );
            return Ok(TriggerLaunchOutcome::SkippedRunning);
        }
        println!(
            "anna-rs daemon trigger={} workflow='{}' skipped: concurrency limit running={} max={}",
            trigger_source, entry.workflow_name, readiness_launch.running, readiness_max_concurrency
        );
        return Ok(TriggerLaunchOutcome::SkippedConcurrency {
            running: readiness_launch.running,
            max_concurrency: readiness_max_concurrency,
        });
    }

    let mut workflow_to_launch = Workflow::load(&entry.path)?;
    if workflow_to_launch.workdir.is_none() {
        workflow_to_launch.workdir = Some(state.plays_dir.display().to_string());
    }
    let source = format!("trigger:{}", trigger_source);
    let launched_req_id_trigger =
        launch_workflow(state, workflow_to_launch, None, entry.owner.clone(), &source).await?;
    println!(
        "anna-rs daemon trigger={} workflow='{}' request_id={}",
        trigger_source, entry.workflow_name, launched_req_id_trigger
    );
    Ok(TriggerLaunchOutcome::Launched(launched_req_id_trigger))
}

async fn running_counts_for_entry(state: &AppState, entry: &WorkflowEntry) -> (usize, usize) {
    let target_owner = owner_key(entry.owner.as_deref());
    let sessions_guard_running = state.sessions.read().await;
    let mut running_workflow = 0usize;
    let mut running_owner = 0usize;
    for session in sessions_guard_running.values().filter(|s| s.status == "running") {
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
    let owner_limit_registered_launch = owner_limit_for(entry.owner.as_deref(), &state.owner_policy);
    let readiness_registered_launch = evaluate_flow_readiness(
        entry,
        &state.node_capabilities,
        state.allowed_providers.as_ref(),
        flow_readiness_runtime(running, None, owner_running, owner_limit_registered_launch),
    );
    if !readiness_registered_launch.missing_capabilities.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "workflow '{}' requires missing capabilities: {}",
                requested_name,
                readiness_registered_launch.missing_capabilities.join(", ")
            ),
        )
            .into_response());
    }
    if !readiness_registered_launch.missing_providers.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "workflow '{}' requires blocked providers: {}",
                requested_name,
                readiness_registered_launch.missing_providers.join(", ")
            ),
        )
            .into_response());
    }
    if readiness_registered_launch.owner_concurrency_blocked {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "workflow '{}' owner concurrency limit reached: owner='{}' running={} max_concurrency={}",
                requested_name,
                entry.owner.as_deref().unwrap_or(""),
                readiness_registered_launch.owner_running,
                readiness_registered_launch.owner_max_concurrency.unwrap_or(0)
            ),
        )
            .into_response());
    }
    if readiness_registered_launch.concurrency_blocked {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "workflow '{}' concurrency limit reached: running={} max_concurrency={}",
                requested_name,
                readiness_registered_launch.running,
                readiness_registered_launch.max_concurrency.unwrap_or(0)
            ),
        )
            .into_response());
    }

    let mut loaded_workflow_registered = match Workflow::load(&entry.path) {
        Ok(v) => v,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid workflow '{}': {}", entry.path.display(), err),
            )
                .into_response());
        }
    };
    if loaded_workflow_registered.workdir.is_none() {
        loaded_workflow_registered.workdir = Some(state.plays_dir.display().to_string());
    }
    loaded_workflow_registered.vars.extend(options.vars);

    match launch_workflow(
        state,
        loaded_workflow_registered,
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
    let run_options_parsed = serde_json::from_str::<RunRegisteredOptions>(body)
        .context("invalid run options json body, expected {\"vars\":{...},\"max_iterations\":N}")?;
    Ok(run_options_parsed)
}

fn parse_chat_run_request(body: &str) -> Result<ChatRunRequest> {
    if body.trim().is_empty() {
        bail!("chat run body is required, expected {{\"intent\":\"...\"}}");
    }
    let chat_run_parsed = serde_json::from_str::<ChatRunRequest>(body)
        .context("invalid chat run json body, expected {\"intent\":\"...\",\"vars\":{...},\"max_iterations\":N}")?;
    if chat_run_parsed.intent.trim().is_empty() {
        bail!("chat run requires non-empty 'intent'");
    }
    Ok(chat_run_parsed)
}

async fn launch_workflow(
    state: &AppState,
    workflow: Workflow,
    max_iterations: Option<u32>,
    owner: Option<String>,
    launch_source: &str,
) -> Result<String> {
    let new_req_id = crate::session::gen_session_id();
    let runtime_session_id = crate::session::gen_session_id();
    let workflow_name = workflow.name.clone();
    let owner_for_audit = owner.clone();
    let launched_at_unix = now_unix_secs();
    {
        let mut sessions_guard_launch_insert = state.sessions.write().await;
        sessions_guard_launch_insert.insert(
            new_req_id.clone(),
            SessionInfo {
                id: new_req_id.clone(),
                status: "running".to_string(),
                workflow: workflow_name.clone(),
                owner: owner.clone(),
                created_at: launched_at_unix,
                updated_at: launched_at_unix,
                runtime_session_id: Some(runtime_session_id.clone()),
                outputs: HashMap::new(),
                errors: Vec::new(),
            },
        );
        prune_sessions_in_place(&mut sessions_guard_launch_insert, state.retention.max_sessions);
    }
    emit_audit_event(
        state,
        "workflow_launched",
        json!({
            "request_id": new_req_id.clone(),
            "runtime_session_id": runtime_session_id.clone(),
            "workflow": workflow_name.clone(),
            "owner": owner_for_audit.clone(),
            "source": launch_source,
            "max_iterations": max_iterations,
        }),
    )
    .await;

    let state_for_task = state.clone();
    let req_id_for_task = new_req_id.clone();
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

        let mut sessions_guard_task_update = state_for_task.sessions.write().await;
        let event_data = match run {
            Ok(result) => {
                let runtime_id = result.session_id.clone();
                let outputs_count = result.outputs.len();
                let errors_count = result.errors.len();
                if let Some(info) = sessions_guard_task_update.get_mut(&req_id_for_task) {
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
                if let Some(info) = sessions_guard_task_update.get_mut(&req_id_for_task) {
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
        prune_sessions_in_place(
            &mut sessions_guard_task_update,
            state_for_task.retention.max_sessions,
        );
        drop(sessions_guard_task_update);
        emit_audit_event(&state_for_task, "workflow_finished", event_data).await;
        state_for_task
            .handles
            .write()
            .await
            .remove(&req_id_for_task);
    });
    state.handles.write().await.insert(new_req_id.clone(), handle);
    Ok(new_req_id)
}

#[cfg(test)]
mod tests {
    use super::{
        ChatIntentConfig, DaemonHitl, DaemonStateSnapshot, HitlPending, SessionInfo,
        TriggerLeaseConfig, WorkflowEntry, WorkflowMetaResponse, collect_required_providers,
        collect_watch_snapshot, evaluate_chat_intent_guardrails, evaluate_flow_readiness,
        flow_readiness_runtime,
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
        let dir_resolve = std::env::temp_dir().join(format!(
            "anna-daemon-reg-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir_resolve)
            .await
            .expect("create temp dir");
        let workflow_file = dir_resolve.join("demo.anna");
        tokio::fs::write(
            &workflow_file,
            "name: demo-workflow\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let by_file = resolve_registered_workflow_entry_with_registry(&dir_resolve, None, "demo.anna")
            .await
            .expect("resolve by file")
            .map(|v| v.path);
        assert_eq!(by_file.as_deref(), Some(workflow_file.as_path()));

        let by_name =
            resolve_registered_workflow_entry_with_registry(&dir_resolve, None, "demo-workflow")
            .await
            .expect("resolve by workflow name")
            .map(|v| v.path);
        assert_eq!(by_name.as_deref(), Some(workflow_file.as_path()));

        let by_stem = resolve_registered_workflow_entry_with_registry(&dir_resolve, None, "demo")
            .await
            .expect("resolve by stem")
            .map(|v| v.path);
        assert_eq!(by_stem.as_deref(), Some(workflow_file.as_path()));
    }

    #[tokio::test]
    async fn loads_flow_registry_and_rejects_duplicates() {
        let dir_registry = std::env::temp_dir().join(format!(
            "anna-daemon-flow-reg-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir_registry)
            .await
            .expect("create temp dir");

        let valid = dir_registry.join("registry.yml");
        tokio::fs::write(
            &valid,
            "flows:\n  - flow_id: alpha\n    path: a.anna\n    max_concurrency: 2\n  - flow_id: beta\n    path: b.anna\n",
        )
        .await
        .expect("write valid registry");
        let parsed_registry = load_flow_registry(&valid)
            .await
            .expect("parse valid registry");
        assert_eq!(parsed_registry.len(), 2);
        assert_eq!(parsed_registry[0].flow_id, "alpha");
        assert_eq!(parsed_registry[0].max_concurrency, Some(2));

        let duplicate = dir_registry.join("registry-dup.yml");
        tokio::fs::write(
            &duplicate,
            "flows:\n  - flow_id: same\n    path: a.anna\n  - flow_id: same\n    path: b.anna\n",
        )
        .await
        .expect("write duplicate registry");
        let duplicate_registry_error = load_flow_registry(&duplicate)
            .await
            .expect_err("duplicate flow_id should fail");
        assert!(duplicate_registry_error.to_string().contains("duplicate flow_id"));

        let invalid_concurrency = dir_registry.join("registry-invalid.yml");
        tokio::fs::write(
            &invalid_concurrency,
            "flows:\n  - flow_id: bad\n    path: a.anna\n    max_concurrency: 0\n",
        )
        .await
        .expect("write invalid registry");
        let invalid_concurrency_error = load_flow_registry(&invalid_concurrency)
            .await
            .expect_err("max_concurrency=0 should fail");
        assert!(invalid_concurrency_error.to_string().contains("max_concurrency=0"));
    }

    #[tokio::test]
    async fn registry_entries_filter_directory_scan_and_support_flow_id() {
        let dir_registry_entries = std::env::temp_dir().join(format!(
            "anna-daemon-registry-entries-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir_registry_entries)
            .await
            .expect("create temp dir");

        let included = dir_registry_entries.join("included.anna");
        tokio::fs::write(
            &included,
            "name: included-flow\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write included workflow");
        tokio::fs::write(
            dir_registry_entries.join("not-listed.anna"),
            "name: hidden-flow\nstages:\n  - id: hello\n    exec: \"echo hidden\"\n",
        )
        .await
        .expect("write non-listed workflow");

        let registry = dir_registry_entries.join("flows.yml");
        tokio::fs::write(
            &registry,
            "flows:\n  - flow_id: prod-deploy\n    path: included.anna\n",
        )
        .await
        .expect("write registry");

        let workflow_entries_registry =
            find_workflow_entries_with_registry(&dir_registry_entries, Some(&registry))
            .await
            .expect("load registry-based entries");
        assert_eq!(workflow_entries_registry.len(), 1);
        assert_eq!(workflow_entries_registry[0].workflow_name, "included-flow");
        assert_eq!(
            workflow_entries_registry[0].flow_id.as_deref(),
            Some("prod-deploy")
        );

        let by_flow_id =
            resolve_registered_workflow_entry_with_registry(
                &dir_registry_entries,
                Some(&registry),
                "prod-deploy",
            )
                .await
                .expect("resolve by flow_id")
                .map(|v| v.path);
        assert_eq!(by_flow_id.as_deref(), Some(included.as_path()));
    }

    #[test]
    fn missing_capability_filter_works_with_wildcard_and_case() {
        let workflow_entry_capability = WorkflowEntry {
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
        let missing_supported_caps = missing_required_capabilities(&workflow_entry_capability, &node);
        assert!(missing_supported_caps.is_empty());

        let restricted = std::collections::HashSet::from(["k8s".to_string()]);
        let missing_restricted_caps =
            missing_required_capabilities(&workflow_entry_capability, &restricted);
        assert_eq!(missing_restricted_caps, vec!["vault".to_string()]);

        let wildcard = std::collections::HashSet::from(["*".to_string()]);
        let missing_with_wildcard =
            missing_required_capabilities(&workflow_entry_capability, &wildcard);
        assert!(missing_with_wildcard.is_empty());
    }

    #[test]
    fn evaluate_flow_readiness_checks_concurrency_and_capabilities() {
        let workflow_entry_readiness = WorkflowEntry {
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

        let readiness_caps = std::collections::HashSet::from(["k8s".to_string()]);
        let readiness_ok = evaluate_flow_readiness(
            &workflow_entry_readiness,
            &readiness_caps,
            None,
            flow_readiness_runtime(1, None, 0, None),
        );
        assert!(readiness_ok.can_run());
        assert!(!readiness_ok.concurrency_blocked);
        assert!(!readiness_ok.owner_concurrency_blocked);

        let readiness_blocked = evaluate_flow_readiness(
            &workflow_entry_readiness,
            &readiness_caps,
            None,
            flow_readiness_runtime(2, None, 0, None),
        );
        assert!(!readiness_blocked.can_run());
        assert!(readiness_blocked.concurrency_blocked);

        let missing_caps = std::collections::HashSet::from(["shell".to_string()]);
        let readiness_missing = evaluate_flow_readiness(
            &workflow_entry_readiness,
            &missing_caps,
            None,
            flow_readiness_runtime(0, None, 0, None),
        );
        assert!(!readiness_missing.can_run());
        assert_eq!(readiness_missing.missing_capabilities, vec!["k8s".to_string()]);
    }

    #[test]
    fn evaluate_flow_readiness_blocks_missing_provider() {
        let workflow_entry_provider = WorkflowEntry {
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

        let provider_caps = std::collections::HashSet::new();
        let provider_allowlist = std::collections::HashSet::from(["shell".to_string()]);
        let provider_readiness = evaluate_flow_readiness(
            &workflow_entry_provider,
            &provider_caps,
            Some(&provider_allowlist),
            flow_readiness_runtime(0, None, 0, None),
        );
        assert!(!provider_readiness.can_run());
        assert_eq!(provider_readiness.missing_providers, vec!["cli".to_string()]);
    }

    #[test]
    fn collect_required_providers_detects_hooks_vote_and_stage_provider() {
        let provider_detection_workflow = Workflow {
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

        let detected_providers = collect_required_providers(&provider_detection_workflow);
        assert_eq!(
            detected_providers,
            vec!["cli".to_string(), "llm".to_string(), "shell".to_string()]
        );
    }

    #[test]
    fn evaluate_flow_readiness_uses_default_concurrency_when_requested() {
        let workflow_entry_default_concurrency = WorkflowEntry {
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
        let default_concurrency_caps = std::collections::HashSet::new();

        let manual = evaluate_flow_readiness(
            &workflow_entry_default_concurrency,
            &default_concurrency_caps,
            None,
            flow_readiness_runtime(100, None, 0, None),
        );
        assert!(manual.can_run());
        assert_eq!(manual.max_concurrency, None);

        let trigger_default = evaluate_flow_readiness(
            &workflow_entry_default_concurrency,
            &default_concurrency_caps,
            None,
            flow_readiness_runtime(1, Some(1), 0, None),
        );
        assert!(!trigger_default.can_run());
        assert!(trigger_default.concurrency_blocked);
        assert_eq!(trigger_default.max_concurrency, Some(1));
    }

    #[test]
    fn evaluate_flow_readiness_blocks_owner_limit() {
        let workflow_entry_owner_limit = WorkflowEntry {
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
        let owner_limit_caps = std::collections::HashSet::new();
        let owner_limit_readiness = evaluate_flow_readiness(
            &workflow_entry_owner_limit,
            &owner_limit_caps,
            None,
            flow_readiness_runtime(0, None, 3, Some(3)),
        );
        assert!(!owner_limit_readiness.can_run());
        assert!(owner_limit_readiness.owner_concurrency_blocked);
        assert_eq!(owner_limit_readiness.owner_running, 3);
        assert_eq!(owner_limit_readiness.owner_max_concurrency, Some(3));
    }

    #[test]
    fn owner_limit_for_prefers_specific_over_default() {
        let owner_policy_fixture = super::OwnerConcurrencyPolicy {
            per_owner: std::collections::HashMap::from([
                ("platform".to_string(), 5usize),
                ("ops".to_string(), 2usize),
            ]),
            default_limit: Some(1),
        };
        assert_eq!(super::owner_limit_for(Some("platform"), &owner_policy_fixture), Some(5));
        assert_eq!(super::owner_limit_for(Some("ops"), &owner_policy_fixture), Some(2));
        assert_eq!(super::owner_limit_for(Some("other"), &owner_policy_fixture), Some(1));
        assert_eq!(super::owner_limit_for(None, &owner_policy_fixture), None);
    }

    #[test]
    fn build_running_indexes_tracks_workflow_and_owner_counts() {
        let sessions_fixture = std::collections::HashMap::from([
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
        let (by_workflow, by_owner) = super::build_running_indexes(&sessions_fixture);
        assert_eq!(by_workflow.get("deploy"), Some(&2usize));
        assert_eq!(by_owner.get("platform"), Some(&2usize));
    }

    #[test]
    fn parse_run_registered_options_supports_empty_and_json() {
        let empty_run_options =
            parse_run_registered_options("  ").expect("empty body should be accepted");
        assert!(empty_run_options.vars.is_empty());
        assert_eq!(empty_run_options.max_iterations, None);

        let parsed_run_options = parse_run_registered_options(
            r#"{"vars":{"ENV":"prod","REGION":"eu"},"max_iterations":2}"#,
        )
        .expect("valid json body");
        assert_eq!(
            parsed_run_options.vars,
            std::collections::HashMap::from([
                (String::from("ENV"), String::from("prod")),
                (String::from("REGION"), String::from("eu")),
            ])
        );
        assert_eq!(parsed_run_options.max_iterations, Some(2));
    }

    #[test]
    fn parse_chat_run_request_requires_intent() {
        let missing_intent_error = parse_chat_run_request("{}").expect_err("intent is required");
        assert!(missing_intent_error.to_string().contains("requires non-empty 'intent'"));

        let parsed_chat_request = parse_chat_run_request(
            r#"{"intent":"deploy","vars":{"ENV":"prod"},"max_iterations":2}"#,
        )
        .expect("valid chat request");
        assert_eq!(parsed_chat_request.intent, "deploy");
        assert_eq!(
            parsed_chat_request.vars.get("ENV").map(String::as_str),
            Some("prod")
        );
        assert_eq!(parsed_chat_request.max_iterations, Some(2));
    }

    #[test]
    fn parse_chat_intents_value_handles_invalid_entries() {
        let parsed_intents_value =
            parse_chat_intents_value("deploy=prod-deploy,ops=ops-flow,bad-entry, =empty,");
        assert_eq!(
            parsed_intents_value.get("deploy").map(|v| v.workflow.as_str()),
            Some("prod-deploy")
        );
        assert_eq!(
            parsed_intents_value.get("ops").map(|v| v.workflow.as_str()),
            Some("ops-flow")
        );
        assert_eq!(
            parsed_intents_value.get("deploy").map(|v| v.max_iterations_cap),
            Some(None)
        );
        assert_eq!(parsed_intents_value.len(), 2);
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
        let empty_doc_error = parse_chat_intents_doc("intents: []\n", "inline-empty")
            .expect_err("empty intent list should fail");
        assert!(empty_doc_error.to_string().contains("has no valid entries"));
    }

    #[tokio::test]
    async fn resolve_trigger_leadership_renews_and_fails_over() {
        let lease_test_dir = std::env::temp_dir().join(format!(
            "anna-trigger-lease-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&lease_test_dir)
            .await
            .expect("create temp lease dir");
        let lease_file = lease_test_dir.join("trigger-lease.json");

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
        let prior_state = super::TriggerLeaderState {
            enabled: true,
            is_leader: true,
            node_id: "node-a".to_string(),
            holder: Some("node-a".to_string()),
            expires_at: Some(100),
            lease_file: Some("/tmp/lease.json".to_string()),
        };
        let next_refresh = super::TriggerLeaderState {
            expires_at: Some(200),
            ..prior_state.clone()
        };
        assert!(!super::trigger_leader_state_changed(
            &prior_state,
            &next_refresh
        ));

        let next_holder = super::TriggerLeaderState {
            holder: Some("node-b".to_string()),
            ..prior_state.clone()
        };
        assert!(super::trigger_leader_state_changed(&prior_state, &next_holder));
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
        let guardrail_entry = WorkflowEntry {
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
        let guardrails_ok =
            evaluate_chat_intent_guardrails(&ok_rule, &guardrail_entry, None, Some("ops-bot"));
        assert!(guardrails_ok.reasons.is_empty());
        assert_eq!(guardrails_ok.effective_max_iterations, Some(2));

        let blocked_max_iterations = evaluate_chat_intent_guardrails(
            &ok_rule,
            &guardrail_entry,
            Some(9),
            Some("ops-bot"),
        );
        assert!(!blocked_max_iterations.reasons.is_empty());
        assert!(blocked_max_iterations.reasons[0].contains("max_iterations"));

        let strict_rule = ChatIntentConfig {
            workflow: "deploy".to_string(),
            allowed_callers: vec!["release-bot".to_string()],
            allowed_owners: vec!["ops".to_string()],
            required_tags: vec!["critical".to_string()],
            max_iterations_cap: None,
        };
        let blocked_strict =
            evaluate_chat_intent_guardrails(&strict_rule, &guardrail_entry, None, Some("ops-bot"));
        assert_eq!(blocked_strict.reasons.len(), 3);
        assert!(
            blocked_strict
                .reasons
                .iter()
                .any(|v| v.contains("allowed callers"))
        );
        assert!(
            blocked_strict
                .reasons
                .iter()
                .any(|v| v.contains("allowed owners"))
        );
        assert!(
            blocked_strict
                .reasons
                .iter()
                .any(|v| v.contains("required chat tags"))
        );
    }

    #[test]
    fn workflow_meta_filters_match_expected_values() {
        let workflow_meta_item = WorkflowMetaResponse {
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
            &workflow_meta_item,
            Some("prod"),
            Some("platform"),
            Some("k8s"),
            Some(false)
        ));
        assert!(!matches_workflow_meta_filters(
            &workflow_meta_item,
            Some("staging"),
            None,
            None,
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &workflow_meta_item,
            None,
            Some("security"),
            None,
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &workflow_meta_item,
            None,
            None,
            Some("http"),
            None
        ));
        assert!(!matches_workflow_meta_filters(
            &workflow_meta_item,
            None,
            None,
            None,
            Some(true)
        ));
    }

    #[tokio::test]
    async fn finds_webhook_metadata() {
        let hook_temp_dir = std::env::temp_dir().join(format!(
            "anna-daemon-hook-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&hook_temp_dir)
            .await
            .expect("create temp dir");
        tokio::fs::write(
            hook_temp_dir.join("hooked.anna"),
            "name: hooked\ntrigger:\n  webhook: /deploy\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let hook_entries_test = find_workflow_entries_with_registry(&hook_temp_dir, None)
            .await
            .expect("find entries");
        assert_eq!(hook_entries_test.len(), 1);
        assert_eq!(hook_entries_test[0].workflow_name, "hooked");
        assert_eq!(
            hook_entries_test[0].trigger_webhook.as_deref(),
            Some("/deploy")
        );
    }

    #[tokio::test]
    async fn finds_trigger_metadata() {
        let trigger_temp_dir = std::env::temp_dir().join(format!(
            "anna-daemon-triggers-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&trigger_temp_dir)
            .await
            .expect("create temp dir");
        tokio::fs::write(
            trigger_temp_dir.join("triggered.anna"),
            "name: trig\ntrigger:\n  interval: 15s\n  cron: \"0/30 * * * * * *\"\n  watch: \"*.rs\"\nstages:\n  - id: hello\n    exec: \"echo hi\"\n",
        )
        .await
        .expect("write workflow");

        let trigger_entries_test = find_workflow_entries_with_registry(&trigger_temp_dir, None)
            .await
            .expect("find entries");
        assert_eq!(trigger_entries_test.len(), 1);
        assert_eq!(trigger_entries_test[0].workflow_name, "trig");
        assert_eq!(
            trigger_entries_test[0].trigger_interval.as_deref(),
            Some("15s")
        );
        assert_eq!(
            trigger_entries_test[0].trigger_cron.as_deref(),
            Some("0/30 * * * * * *")
        );
        assert_eq!(trigger_entries_test[0].trigger_watch.as_deref(), Some("*.rs"));
    }

    #[test]
    fn resolve_watch_pattern_defaults_recursive_filename_glob() {
        let root = std::path::PathBuf::from("/tmp/anna-watch");
        let watch_pattern_default = resolve_watch_pattern(&root, "*.go");
        assert_eq!(watch_pattern_default, "/tmp/anna-watch/**/*.go");
    }

    #[test]
    fn collect_watch_snapshot_changes_when_file_updates() {
        let watch_snapshot_dir = std::env::temp_dir().join(format!(
            "anna-watch-snap-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::create_dir_all(&watch_snapshot_dir).expect("create temp dir");
        let watch_file_path = watch_snapshot_dir.join("x.txt");
        std::fs::write(&watch_file_path, "a").expect("write first content");

        let watch_glob_pattern = format!("{}/**/*.txt", watch_snapshot_dir.display());
        let before = collect_watch_snapshot(&watch_glob_pattern).expect("collect before snapshot");
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&watch_file_path, "bbb").expect("write updated content");
        let after = collect_watch_snapshot(&watch_glob_pattern).expect("collect after snapshot");
        assert_ne!(before, after);
    }

    #[tokio::test]
    async fn daemon_hitl_handler_waits_for_resolution() {
        let pending_map = Arc::new(RwLock::new(HashMap::<String, HitlPending>::new()));
        let handler = DaemonHitl {
            pending: pending_map.clone(),
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
        let pending_request_id_test = {
            let pending_read_guard = pending_map.read().await;
            pending_read_guard
                .keys()
                .next()
                .cloned()
                .expect("pending hitl request should exist")
        };
        {
            let mut pending_write_guard = pending_map.write().await;
            let pending_item_mut = pending_write_guard
                .get_mut(&pending_request_id_test)
                .expect("pending request by id");
            pending_item_mut.decision = Some("approve".to_string());
            pending_item_mut.status = "resolved".to_string();
        }

        let resolved_decision = waiter.await.expect("join waiter");
        assert_eq!(resolved_decision, "approve");
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
        let mut caller_headers = HeaderMap::new();
        caller_headers.insert("x-anna-caller", HeaderValue::from_static("Ops-Bot"));
        assert_eq!(
            super::request_caller(&caller_headers).as_deref(),
            Some("ops-bot")
        );

        caller_headers.remove("x-anna-caller");
        caller_headers.insert("x-anna-role", HeaderValue::from_static("Platform"));
        assert_eq!(
            super::request_caller(&caller_headers).as_deref(),
            Some("platform")
        );
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
        let state_file_path = std::env::temp_dir().join(format!(
            "anna-daemon-state-{}-{}.json",
            std::process::id(),
            rand::random::<u32>()
        ));
        let mut session_map_load = HashMap::new();
        session_map_load.insert(
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
        let state_snapshot_doc = DaemonStateSnapshot {
            sessions: session_map_load,
            hitl: HashMap::new(),
            saved_at: 1,
        };
        tokio::fs::write(
            &state_file_path,
            serde_json::to_string(&state_snapshot_doc).expect("serialize snapshot"),
        )
        .await
        .expect("write state file");

        let (loaded_sessions, _hitl) =
            load_daemon_state(&state_file_path).await.expect("load state");
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
        let mut session_map_prune = HashMap::new();
        session_map_prune.insert(
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
        session_map_prune.insert(
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
        session_map_prune.insert(
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

        prune_sessions_in_place(&mut session_map_prune, 2);
        assert!(session_map_prune.contains_key("running"));
        assert!(session_map_prune.contains_key("new"));
        assert!(!session_map_prune.contains_key("old"));
    }

    #[test]
    fn prune_hitl_prefers_resolved_items() {
        let mut hitl_map_prune = HashMap::new();
        hitl_map_prune.insert(
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
        hitl_map_prune.insert(
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
        hitl_map_prune.insert(
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

        prune_hitl_in_place(&mut hitl_map_prune, 2);
        assert!(hitl_map_prune.contains_key("pending"));
        assert!(hitl_map_prune.contains_key("resolved-new"));
        assert!(!hitl_map_prune.contains_key("resolved-old"));
    }

    #[tokio::test]
    async fn persist_policy_snapshot_writes_effective_policy() {
        let policy_snapshot_dir = std::env::temp_dir().join(format!(
            "anna-policy-snapshot-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&policy_snapshot_dir)
            .await
            .expect("create temp policy dir");
        let policy_snapshot_path = policy_snapshot_dir.join("policy.snapshot.json");
        let app_state_snapshot = super::AppState {
            executor: Executor::new(),
            plays_dir: policy_snapshot_dir.clone(),
            registry_file: Some(policy_snapshot_dir.join("flows.registry.yml")),
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
        persist_policy_snapshot(&app_state_snapshot, &policy_snapshot_path)
            .await
            .expect("persist policy snapshot");

        let persisted_snapshot_raw = tokio::fs::read_to_string(&policy_snapshot_path)
            .await
            .expect("read policy snapshot");
        let persisted_snapshot_doc: serde_json::Value =
            serde_json::from_str(&persisted_snapshot_raw)
                .expect("policy snapshot should be valid json");
        assert_eq!(persisted_snapshot_doc["offline_mode"].as_bool(), Some(true));
        assert_eq!(
            persisted_snapshot_doc["node_capabilities"],
            serde_json::json!(["shell", "vault"])
        );
        assert_eq!(
            persisted_snapshot_doc["allowed_providers"],
            serde_json::json!(["shell", "vault"])
        );
        assert_eq!(
            persisted_snapshot_doc["chat_intents"]["deploy"]["workflow"].as_str(),
            Some("prod-deploy")
        );
        assert_eq!(
            persisted_snapshot_doc["trigger_scheduler"]["is_leader"].as_bool(),
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
        let sorted_values_copy = super::sorted_set_values(Some(&set));
        assert_eq!(
            sorted_values_copy,
            vec!["http".to_string(), "shell".to_string(), "vault".to_string()]
        );
    }

    #[test]
    fn policy_revision_is_stable_for_same_core() {
        let policy_core_fixture = serde_json::json!({
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

        let (a_rev, a_sig) = super::policy_revision_and_signature(&policy_core_fixture, None);
        let (b_rev, b_sig) = super::policy_revision_and_signature(&policy_core_fixture, None);
        assert_eq!(a_rev, b_rev);
        assert_eq!(a_sig, None);
        assert_eq!(b_sig, None);
        assert_eq!(a_rev.len(), 64);
    }

    #[test]
    fn policy_revision_signature_uses_hmac_sha256() {
        let policy_core_for_sig = serde_json::json!({
            "registry_enabled": true,
            "chat_intents": {},
            "owner_limits": [],
            "node_capabilities": [],
            "allowed_providers": [],
            "trigger_policy": {"leader_election_enabled": false, "node_id": "node-a", "lease_file": serde_json::Value::Null}
        });

        let (rev_a, sig_a) =
            super::policy_revision_and_signature(&policy_core_for_sig, Some("secret-a"));
        let (rev_b, sig_b) =
            super::policy_revision_and_signature(&policy_core_for_sig, Some("secret-b"));
        assert_eq!(rev_a, rev_b);
        assert!(sig_a.is_some());
        assert!(sig_b.is_some());
        assert_ne!(sig_a, sig_b);
        assert_eq!(sig_a.as_ref().map(String::len), Some(64));
    }

    #[tokio::test]
    async fn append_audit_event_writes_ndjson_line() {
        let audit_dir = std::env::temp_dir().join(format!(
            "anna-audit-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&audit_dir)
            .await
            .expect("create temp audit dir");
        let audit_log_path = audit_dir.join("audit.log");
        let config = super::AuditLogConfig {
            path: audit_log_path.clone(),
            node_id: "node-test".to_string(),
        };
        super::append_audit_event(
            &config,
            "workflow_launched",
            serde_json::json!({"request_id":"req-1","source":"api_workflow_named"}),
        )
        .await
        .expect("append audit event");

        let audit_log_raw = tokio::fs::read_to_string(&audit_log_path)
            .await
            .expect("read audit log");
        let line = audit_log_raw.lines().last().expect("audit line should exist");
        let audit_log_entry: serde_json::Value =
            serde_json::from_str(line).expect("audit line should be valid json");
        assert_eq!(audit_log_entry["event"].as_str(), Some("workflow_launched"));
        assert_eq!(audit_log_entry["node_id"].as_str(), Some("node-test"));
        assert_eq!(audit_log_entry["data"]["request_id"].as_str(), Some("req-1"));
        assert_eq!(
            audit_log_entry["data"]["source"].as_str(),
            Some("api_workflow_named")
        );
        assert!(audit_log_entry["ts"].as_u64().is_some());
    }
}
