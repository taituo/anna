use anna_rs::executor::{Executor, RunConfig};
use anna_rs::providers::llm::{active_llm_adapter_name, load_llm_adapter_catalog_from_env};
use anna_rs::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::json;
use sha2::Sha256;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Debug, Parser)]
#[command(
    name = "anna-rs",
    version,
    about = "Rust-based Anna workflow runtime (MVP)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a workflow file
    Run {
        /// Path to .anna workflow file
        workflow: PathBuf,
        /// Override workflow vars (repeatable: --var KEY=VALUE)
        #[arg(long = "var")]
        vars: Vec<String>,
        /// Stop after N iterations in continuous mode
        #[arg(long)]
        max_iterations: Option<u32>,
        /// Print parsed workflow and exit
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Validate workflow syntax and structure
    Validate {
        /// Path to .anna workflow file
        workflow: PathBuf,
    },
    /// Run HTTP daemon API
    Daemon {
        /// Bind address (host:port)
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Directory used to list local .anna workflows
        #[arg(long)]
        plays_dir: Option<PathBuf>,
    },
    /// Run MCP stdio tool server
    Mcp {
        /// Daemon base URL used by MCP tools
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Submit workflow YAML file to daemon
    Submit {
        /// Path to .anna workflow file
        workflow: PathBuf,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// List registered workflows from daemon playbook directory
    Workflows {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// List registered workflows with metadata and capability availability
    WorkflowsMeta {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional tag filter
        #[arg(long)]
        tag: Option<String>,
        /// Optional owner filter
        #[arg(long)]
        owner: Option<String>,
        /// Optional required capability filter
        #[arg(long)]
        capability: Option<String>,
        /// Optional availability filter
        #[arg(long)]
        available: Option<bool>,
        /// Optional max rows
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Run registered workflow by name/file stem on daemon
    RunNamed {
        /// Workflow name, file name, or file stem
        name: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Override workflow vars (repeatable: --var KEY=VALUE)
        #[arg(long = "var")]
        vars: Vec<String>,
        /// Stop after N iterations in continuous mode
        #[arg(long)]
        max_iterations: Option<u32>,
        /// Run precheck (/workflow/{name}/check) before launch
        #[arg(long, default_value_t = false)]
        precheck: bool,
    },
    /// Check whether a registered workflow can run right now
    CanRun {
        /// Workflow name, file name, flow_id, or file stem
        name: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Check whether a chat intent can run now
    CanChat {
        /// Chat intent name
        intent: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional max iterations value to precheck against chat guardrails
        #[arg(long)]
        max_iterations: Option<u32>,
        /// Optional caller identity for chat guardrails (sent as x-anna-caller)
        #[arg(long)]
        caller: Option<String>,
    },
    /// Check whether a workflow YAML file can run under current daemon policy
    CanRunYaml {
        /// Path to .anna workflow file
        workflow: PathBuf,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Check daemon workflow status by request id
    Status {
        /// Request id
        id: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// List daemon sessions
    Sessions {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional status filter, e.g. running|done|failed
        #[arg(long)]
        status: Option<String>,
        /// Optional owner filter (registry owner)
        #[arg(long)]
        owner: Option<String>,
        /// Optional workflow name filter
        #[arg(long)]
        workflow: Option<String>,
        /// Optional max rows
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show daemon stats summary
    Stats {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Show daemon policy/capability configuration summary
    Policy {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional If-Match header value for revision precondition
        #[arg(long)]
        if_match: Option<String>,
        /// Optional If-None-Match header value for cache validation
        #[arg(long)]
        if_none_match: Option<String>,
    },
    /// Show daemon policy revision/hash (+ optional signature when configured)
    PolicyRevision {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional If-None-Match header value for cache validation
        #[arg(long)]
        if_none_match: Option<String>,
    },
    /// Show daemon effective policy snapshot (same shape as ANNA_POLICY_SNAPSHOT_FILE)
    PolicySnapshot {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional If-Match header value for revision precondition
        #[arg(long)]
        if_match: Option<String>,
        /// Optional If-None-Match header value for cache validation
        #[arg(long)]
        if_none_match: Option<String>,
    },
    /// Sync daemon effective policy snapshot to local file atomically
    PolicySync {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Output file for local policy snapshot cache
        #[arg(long)]
        output: Option<PathBuf>,
        /// Retries when revision changes between /policy/revision and /policy/snapshot
        #[arg(long, default_value_t = 3)]
        retries: usize,
    },
    /// Verify daemon policy revision signature (HMAC-SHA256)
    PolicyVerify {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Verification key (fallback: ANNA_POLICY_VERIFY_KEY, then ANNA_POLICY_SIGNING_KEY)
        #[arg(long)]
        key: Option<String>,
        /// Do not fail when daemon policy revision is unsigned
        #[arg(long, default_value_t = false)]
        allow_unsigned: bool,
    },
    /// Show local or daemon LLM adapter catalog
    LlmAdapters {
        /// Print JSON instead of compact text table
        #[arg(long, default_value_t = false)]
        json: bool,
        /// Query daemon adapter catalog instead of local environment
        #[arg(long)]
        daemon: Option<String>,
    },
    /// Wait until workflow reaches terminal state
    Wait {
        /// Request id
        id: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Poll interval in milliseconds
        #[arg(long, default_value_t = 1000)]
        poll_ms: u64,
        /// Optional timeout in seconds
        #[arg(long)]
        timeout_sec: Option<u64>,
    },
    /// Stop daemon workflow by request id
    Stop {
        /// Request id
        id: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Fetch daemon workflow stage logs by request id
    Logs {
        /// Request id
        id: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Trigger webhook hook by name (maps to /hook/{name})
    Hook {
        /// Hook name without leading slash, e.g. deploy
        name: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// List chat intent routes configured in daemon
    ChatIntents {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Run workflow through chat intent route
    Chat {
        /// Chat intent name (must exist in daemon intent map)
        intent: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional caller identity for chat guardrails (sent as x-anna-caller)
        #[arg(long)]
        caller: Option<String>,
        /// Override workflow vars (repeatable: --var KEY=VALUE)
        #[arg(long = "var")]
        vars: Vec<String>,
        /// Stop after N iterations in continuous mode
        #[arg(long)]
        max_iterations: Option<u32>,
    },
    /// Manage pending human-in-the-loop requests
    Hitl {
        #[command(subcommand)]
        command: HitlCommands,
    },
}

#[derive(Debug, Subcommand)]
enum HitlCommands {
    /// List pending/resolved HITL requests
    List {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Optional status filter, e.g. pending|resolved
        #[arg(long)]
        status: Option<String>,
        /// Optional workflow session id filter
        #[arg(long)]
        session_id: Option<String>,
        /// Optional workflow name filter
        #[arg(long)]
        workflow: Option<String>,
        /// Optional max rows
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Resolve HITL request by id
    Resolve {
        /// HITL request id
        id: String,
        /// Decision value, e.g. approve|reject
        decision: String,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run {
            workflow,
            vars,
            max_iterations,
            dry_run,
        } => {
            let mut wf = Workflow::load(&workflow)?;
            let overrides = parse_var_overrides(vars)?;
            wf.vars.extend(overrides);

            if dry_run {
                println!("{}", serde_yaml::to_string(&wf)?);
                return Ok(());
            }

            let executor = Executor::new();
            let result = executor
                .run(
                    &wf,
                    RunConfig {
                        max_iterations,
                        session_id_override: None,
                    },
                )
                .await
                .with_context(|| format!("workflow '{}' failed", wf.name))?;

            println!("session={}", result.session_id);
            for stage in &wf.stages {
                let ok = result.success.get(&stage.id).copied().unwrap_or(false);
                println!("{} {}", if ok { "ok" } else { "skip/fail" }, stage.id);
            }
            if !result.errors.is_empty() {
                println!("\nerrors:");
                for err in &result.errors {
                    println!("- {}", err);
                }
                return Err(anyhow!("workflow completed with errors"));
            }
            Ok(())
        }
        Commands::Validate { workflow } => {
            let wf = Workflow::load(&workflow)?;
            println!("valid workflow '{}' (stages={})", wf.name, wf.stages.len());
            Ok(())
        }
        Commands::Daemon { bind, plays_dir } => {
            let root = match plays_dir {
                Some(v) => v,
                None => std::env::current_dir().context("failed to read current dir")?,
            };
            anna_rs::daemon::run_daemon(&bind, root).await?;
            Ok(())
        }
        Commands::Mcp { daemon } => {
            anna_rs::mcp::run_stdio_server(anna_rs::mcp::McpConfig {
                daemon_url: normalize_daemon_url(&daemon),
                daemon_token: daemon_auth_token(),
            })
            .await
        }
        Commands::Submit { workflow, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let body = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("failed reading '{}'", workflow.display()))?;
            let client = Client::new();
            let response = with_daemon_auth(client.post(format!("{}/workflow", daemon)))
                .body(body)
                .send()
                .await
                .with_context(|| format!("failed submitting workflow to {}", daemon))?;
            print_response(response).await
        }
        Commands::Workflows { daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.get(format!("{}/workflows", daemon)))
                .send()
                .await
                .with_context(|| format!("failed querying workflows at {}", daemon))?;
            print_response(response).await
        }
        Commands::WorkflowsMeta {
            daemon,
            tag,
            owner,
            capability,
            available,
            limit,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let mut request = with_daemon_auth(client.get(format!("{}/workflows/meta", daemon)));
            if let Some(tag) = tag {
                request = request.query(&[("tag", tag)]);
            }
            if let Some(owner) = owner {
                request = request.query(&[("owner", owner)]);
            }
            if let Some(capability) = capability {
                request = request.query(&[("capability", capability)]);
            }
            if let Some(available) = available {
                request = request.query(&[("available", available)]);
            }
            if let Some(limit) = limit {
                request = request.query(&[("limit", limit)]);
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("failed querying workflow metadata at {}", daemon))?;
            print_response(response).await
        }
        Commands::RunNamed {
            name,
            daemon,
            vars,
            max_iterations,
            precheck,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            if precheck {
                let response =
                    with_daemon_auth(client.get(format!("{}/workflow/{}/check", daemon, name)))
                        .send()
                        .await
                        .with_context(|| {
                            format!(
                                "failed running precheck for workflow '{}' at {}",
                                name, daemon
                            )
                        })?;
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .context("failed reading precheck response body")?;
                if !status.is_success() {
                    if body.trim().is_empty() {
                        return Err(anyhow!(
                            "precheck request failed with status {} for '{}'",
                            status,
                            name
                        ));
                    }
                    return Err(anyhow!(
                        "precheck request failed with status {} for '{}': {}",
                        status,
                        name,
                        body
                    ));
                }
                let parsed: serde_json::Value = serde_json::from_str(&body)
                    .context("daemon returned non-json precheck response")?;
                let can_run = parsed
                    .get("can_run")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !can_run {
                    println!("{}", serde_json::to_string_pretty(&parsed)?);
                    return Err(anyhow!("precheck blocked workflow '{}'", name));
                }
            }
            let overrides = parse_var_overrides(vars)?;
            let mut request =
                with_daemon_auth(client.post(format!("{}/workflow/{}/run", daemon, name)));
            if !overrides.is_empty() || max_iterations.is_some() {
                request = request.json(&json!({
                    "vars": overrides,
                    "max_iterations": max_iterations
                }));
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("failed launching workflow '{}' at {}", name, daemon))?;
            print_response(response).await
        }
        Commands::CanRun { name, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response =
                with_daemon_auth(client.get(format!("{}/workflow/{}/check", daemon, name)))
                    .send()
                    .await
                    .with_context(|| {
                        format!("failed checking workflow '{}' at {}", name, daemon)
                    })?;
            print_response(response).await
        }
        Commands::CanChat {
            intent,
            daemon,
            max_iterations,
            caller,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let mut request = with_optional_caller(
                with_daemon_auth(client.get(format!("{}/chat/{}/check", daemon, intent))),
                caller.as_deref(),
            );
            if let Some(max_iterations) = max_iterations {
                request = request.query(&[("max_iterations", max_iterations)]);
            }
            let response = request.send().await.with_context(|| {
                format!("failed checking chat intent '{}' at {}", intent, daemon)
            })?;
            print_response(response).await
        }
        Commands::CanRunYaml { workflow, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let body = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| {
                    format!("failed reading workflow file '{}'", workflow.display())
                })?;
            let client = Client::new();
            let response = with_daemon_auth(client.post(format!("{}/workflow/check", daemon)))
                .body(body)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed checking workflow yaml '{}' at {}",
                        workflow.display(),
                        daemon
                    )
                })?;
            print_response(response).await
        }
        Commands::Status { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.get(format!("{}/workflow/{}", daemon, id)))
                .send()
                .await
                .with_context(|| format!("failed querying workflow '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Sessions {
            daemon,
            status,
            owner,
            workflow,
            limit,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let mut req = with_daemon_auth(client.get(format!("{}/sessions", daemon)));
            if let Some(status) = status {
                req = req.query(&[("status", status)]);
            }
            if let Some(owner) = owner {
                req = req.query(&[("owner", owner)]);
            }
            if let Some(workflow) = workflow {
                req = req.query(&[("workflow", workflow)]);
            }
            if let Some(limit) = limit {
                req = req.query(&[("limit", limit)]);
            }
            let response = req
                .send()
                .await
                .with_context(|| format!("failed querying sessions at {}", daemon))?;
            print_response(response).await
        }
        Commands::Stats { daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.get(format!("{}/stats", daemon)))
                .send()
                .await
                .with_context(|| format!("failed querying stats at {}", daemon))?;
            print_response(response).await
        }
        Commands::Policy {
            daemon,
            if_match,
            if_none_match,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_optional_etag_preconditions(
                with_daemon_auth(client.get(format!("{}/policy", daemon))),
                if_match.as_deref(),
                if_none_match.as_deref(),
            )
            .send()
            .await
            .with_context(|| format!("failed querying policy at {}", daemon))?;
            print_response(response).await
        }
        Commands::PolicyRevision {
            daemon,
            if_none_match,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_optional_etag_preconditions(
                with_daemon_auth(client.get(format!("{}/policy/revision", daemon))),
                None,
                if_none_match.as_deref(),
            )
            .send()
            .await
            .with_context(|| format!("failed querying policy revision at {}", daemon))?;
            print_response(response).await
        }
        Commands::PolicySnapshot {
            daemon,
            if_match,
            if_none_match,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_optional_etag_preconditions(
                with_daemon_auth(client.get(format!("{}/policy/snapshot", daemon))),
                if_match.as_deref(),
                if_none_match.as_deref(),
            )
            .send()
            .await
            .with_context(|| format!("failed querying policy snapshot at {}", daemon))?;
            print_response(response).await
        }
        Commands::PolicySync {
            daemon,
            output,
            retries,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let output = output.unwrap_or_else(default_local_policy_snapshot_path);
            sync_policy_snapshot(&daemon, &output, retries).await
        }
        Commands::PolicyVerify {
            daemon,
            key,
            allow_unsigned,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            verify_policy_revision_signature(&daemon, key.as_deref(), allow_unsigned).await
        }
        Commands::LlmAdapters { json, daemon } => {
            if let Some(daemon) = daemon {
                let daemon = normalize_daemon_url(&daemon);
                let client = Client::new();
                let response = with_daemon_auth(client.get(format!("{}/llm/adapters", daemon)))
                    .send()
                    .await
                    .with_context(|| {
                        format!("failed querying daemon llm adapters at {}", daemon)
                    })?;
                return print_response(response).await;
            }
            let loaded = load_llm_adapter_catalog_from_env()?;
            match loaded {
                Some(loaded) => {
                    let selected = active_llm_adapter_name(Some(&loaded.catalog));
                    let mut names = loaded.catalog.adapters.keys().cloned().collect::<Vec<_>>();
                    names.sort();
                    if json {
                        let payload = json!({
                            "configured": true,
                            "source": loaded.path,
                            "selected": selected,
                            "default": loaded.catalog.default,
                            "adapters": loaded.catalog.adapters,
                        });
                        println!("{}", serde_json::to_string_pretty(&payload)?);
                    } else {
                        println!("source: {}", loaded.path);
                        println!(
                            "selected: {}",
                            selected.unwrap_or_else(|| "<none>".to_string())
                        );
                        println!(
                            "default: {}",
                            loaded
                                .catalog
                                .default
                                .unwrap_or_else(|| "<none>".to_string())
                        );
                        if names.is_empty() {
                            println!("adapters: <none>");
                        } else {
                            println!("adapters:");
                            for name in names {
                                println!("  - {}", name);
                            }
                        }
                    }
                    Ok(())
                }
                None => {
                    if json {
                        let payload = json!({
                            "configured": false,
                            "source": null,
                            "selected": null,
                            "default": null,
                            "adapters": {},
                            "note": "set ANNA_LLM_ADAPTERS_FILE to enable adapter catalog"
                        });
                        println!("{}", serde_json::to_string_pretty(&payload)?);
                    } else {
                        println!("LLM adapter catalog not configured.");
                        println!("Set ANNA_LLM_ADAPTERS_FILE to enable adapter routing.");
                    }
                    Ok(())
                }
            }
        }
        Commands::Wait {
            id,
            daemon,
            poll_ms,
            timeout_sec,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            wait_for_workflow(&daemon, &id, poll_ms.max(50), timeout_sec).await
        }
        Commands::Stop { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.delete(format!("{}/workflow/{}", daemon, id)))
                .send()
                .await
                .with_context(|| format!("failed stopping workflow '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Logs { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.get(format!("{}/workflow/{}/logs", daemon, id)))
                .send()
                .await
                .with_context(|| format!("failed reading workflow logs '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Hook { name, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.post(format!(
                "{}/hook/{}",
                daemon,
                name.trim_matches('/')
            )))
            .send()
            .await
            .with_context(|| format!("failed triggering hook '{}' at {}", name, daemon))?;
            print_response(response).await
        }
        Commands::ChatIntents { daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = with_daemon_auth(client.get(format!("{}/chat/intents", daemon)))
                .send()
                .await
                .with_context(|| format!("failed listing chat intents at {}", daemon))?;
            print_response(response).await
        }
        Commands::Chat {
            intent,
            daemon,
            caller,
            vars,
            max_iterations,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let overrides = parse_var_overrides(vars)?;
            let client = Client::new();
            let response = with_optional_caller(
                with_daemon_auth(client.post(format!("{}/chat/run", daemon))),
                caller.as_deref(),
            )
            .json(&json!({
                "intent": intent,
                "vars": overrides,
                "max_iterations": max_iterations
            }))
            .send()
            .await
            .with_context(|| format!("failed running chat intent at {}", daemon))?;
            print_response(response).await
        }
        Commands::Hitl { command } => match command {
            HitlCommands::List {
                daemon,
                status,
                session_id,
                workflow,
                limit,
            } => {
                let daemon = normalize_daemon_url(&daemon);
                let client = Client::new();
                let mut req = with_daemon_auth(client.get(format!("{}/hitl", daemon)));
                if let Some(status) = status {
                    req = req.query(&[("status", status)]);
                }
                if let Some(session_id) = session_id {
                    req = req.query(&[("session_id", session_id)]);
                }
                if let Some(workflow) = workflow {
                    req = req.query(&[("workflow", workflow)]);
                }
                if let Some(limit) = limit {
                    req = req.query(&[("limit", limit)]);
                }
                let response = req
                    .send()
                    .await
                    .with_context(|| format!("failed querying hitl at {}", daemon))?;
                print_response(response).await
            }
            HitlCommands::Resolve {
                id,
                decision,
                daemon,
            } => {
                let daemon = normalize_daemon_url(&daemon);
                let client = Client::new();
                let response =
                    with_daemon_auth(client.post(format!("{}/hitl/{}/resolve", daemon, id)))
                        .json(&json!({ "decision": decision }))
                        .send()
                        .await
                        .with_context(|| format!("failed resolving hitl '{}' at {}", id, daemon))?;
                print_response(response).await
            }
        },
    }
}

fn parse_var_overrides(raw: Vec<String>) -> Result<HashMap<String, String>> {
    let mut vars = HashMap::new();
    for pair in raw {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --var '{}', expected KEY=VALUE", pair))?;
        if k.trim().is_empty() {
            return Err(anyhow!("invalid --var '{}': empty key", pair));
        }
        vars.insert(k.to_string(), v.to_string());
    }
    Ok(vars)
}

fn normalize_daemon_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn daemon_auth_token() -> Option<String> {
    std::env::var("ANNA_DAEMON_TOKEN")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn with_daemon_auth(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match daemon_auth_token() {
        Some(token) => builder.bearer_auth(token),
        None => builder,
    }
}

fn with_optional_caller(
    builder: reqwest::RequestBuilder,
    caller: Option<&str>,
) -> reqwest::RequestBuilder {
    match caller.map(|v| v.trim()).filter(|v| !v.is_empty()) {
        Some(caller) => builder.header("x-anna-caller", caller),
        None => builder,
    }
}

fn with_optional_etag_preconditions(
    builder: reqwest::RequestBuilder,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut req = builder;
    if let Some(value) = if_match.map(str::trim).filter(|v| !v.is_empty()) {
        req = req.header("if-match", value);
    }
    if let Some(value) = if_none_match.map(str::trim).filter(|v| !v.is_empty()) {
        req = req.header("if-none-match", value);
    }
    req
}

async fn print_response(response: reqwest::Response) -> Result<()> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading response body")?;

    if status == reqwest::StatusCode::NOT_MODIFIED {
        println!("{}", status);
        return Ok(());
    }

    if status.is_success() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
            println!("{}", serde_json::to_string_pretty(&json)?);
        } else if !body.trim().is_empty() {
            println!("{}", body);
        } else {
            println!("{}", status);
        }
        return Ok(());
    }

    if body.trim().is_empty() {
        Err(anyhow!("request failed with status {}", status))
    } else {
        Err(anyhow!("request failed with status {}: {}", status, body))
    }
}

async fn sync_policy_snapshot(daemon: &str, output: &Path, retries: usize) -> Result<()> {
    let client = Client::new();
    let previous_revision = read_local_policy_revision(output).await?;
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let mut revision_request =
            with_daemon_auth(client.get(format!("{}/policy/revision", daemon)));
        if let Some(previous_revision) = previous_revision.as_deref() {
            revision_request =
                with_optional_etag_preconditions(revision_request, None, Some(previous_revision));
        }

        let revision_response = revision_request
            .send()
            .await
            .with_context(|| format!("failed querying policy revision at {}", daemon))?;
        let revision_status = revision_response.status();

        if revision_status == reqwest::StatusCode::NOT_MODIFIED {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "not_modified",
                    "path": output.display().to_string(),
                    "policy_revision": previous_revision,
                    "attempts": attempt,
                }))?
            );
            return Ok(());
        }

        let revision_etag = etag_revision(revision_response.headers());
        let revision_body = revision_response
            .text()
            .await
            .context("failed reading policy revision response body")?;
        if !revision_status.is_success() {
            if revision_body.trim().is_empty() {
                return Err(anyhow!(
                    "policy revision request failed with status {}",
                    revision_status
                ));
            }
            return Err(anyhow!(
                "policy revision request failed with status {}: {}",
                revision_status,
                revision_body
            ));
        }

        let revision_json: serde_json::Value = serde_json::from_str(&revision_body)
            .context("daemon returned non-json policy revision response")?;
        let remote_revision = policy_revision_from_json(&revision_json)
            .or(revision_etag)
            .ok_or_else(|| anyhow!("policy revision response missing 'policy_revision'"))?;

        let snapshot_response = with_optional_etag_preconditions(
            with_daemon_auth(client.get(format!("{}/policy/snapshot", daemon))),
            Some(&remote_revision),
            None,
        )
        .send()
        .await
        .with_context(|| format!("failed querying policy snapshot at {}", daemon))?;
        let snapshot_status = snapshot_response.status();
        let snapshot_etag = etag_revision(snapshot_response.headers());
        let snapshot_body = snapshot_response
            .text()
            .await
            .context("failed reading policy snapshot response body")?;

        if snapshot_status == reqwest::StatusCode::PRECONDITION_FAILED {
            if attempt <= retries {
                continue;
            }
            let current_revision = snapshot_etag
                .or_else(|| policy_revision_from_raw_json(&snapshot_body).ok().flatten());
            if snapshot_body.trim().is_empty() {
                if let Some(current_revision) = current_revision {
                    return Err(anyhow!(
                        "policy snapshot precondition failed after {} attempts; current revision '{}'",
                        attempt,
                        current_revision
                    ));
                }
                return Err(anyhow!(
                    "policy snapshot precondition failed after {} attempts",
                    attempt
                ));
            }
            if let Some(current_revision) = current_revision {
                return Err(anyhow!(
                    "policy snapshot precondition failed after {} attempts (current revision '{}'): {}",
                    attempt,
                    current_revision,
                    snapshot_body
                ));
            }
            return Err(anyhow!(
                "policy snapshot precondition failed after {} attempts: {}",
                attempt,
                snapshot_body
            ));
        }

        if !snapshot_status.is_success() {
            if snapshot_body.trim().is_empty() {
                return Err(anyhow!(
                    "policy snapshot request failed with status {}",
                    snapshot_status
                ));
            }
            return Err(anyhow!(
                "policy snapshot request failed with status {}: {}",
                snapshot_status,
                snapshot_body
            ));
        }

        let snapshot_json: serde_json::Value = serde_json::from_str(&snapshot_body)
            .context("daemon returned non-json policy snapshot response")?;
        let snapshot_revision = policy_revision_from_json(&snapshot_json)
            .or(snapshot_etag)
            .ok_or_else(|| anyhow!("policy snapshot response missing 'policy_revision'"))?;
        if snapshot_revision != remote_revision {
            if attempt <= retries {
                continue;
            }
            return Err(anyhow!(
                "policy snapshot revision mismatch after {} attempts (revision endpoint='{}', snapshot='{}')",
                attempt,
                remote_revision,
                snapshot_revision
            ));
        }

        write_json_atomic(output, &snapshot_json).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "synced",
                "path": output.display().to_string(),
                "previous_policy_revision": previous_revision,
                "policy_revision": remote_revision,
                "attempts": attempt,
            }))?
        );
        return Ok(());
    }
}

async fn verify_policy_revision_signature(
    daemon: &str,
    key: Option<&str>,
    allow_unsigned: bool,
) -> Result<()> {
    let client = Client::new();
    let response = with_daemon_auth(client.get(format!("{}/policy/revision", daemon)))
        .send()
        .await
        .with_context(|| format!("failed querying policy revision at {}", daemon))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading policy revision response body")?;
    if !status.is_success() {
        if body.trim().is_empty() {
            return Err(anyhow!(
                "policy revision request failed with status {}",
                status
            ));
        }
        return Err(anyhow!(
            "policy revision request failed with status {}: {}",
            status,
            body
        ));
    }

    let payload: serde_json::Value =
        serde_json::from_str(&body).context("daemon returned non-json policy revision response")?;
    let policy_revision = policy_revision_from_json(&payload)
        .ok_or_else(|| anyhow!("policy revision response missing 'policy_revision'"))?;
    let policy_signature = policy_signature_from_json(&payload);
    let signed = payload
        .get("signed")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || policy_signature.is_some();
    let signature_algorithm = payload
        .get("policy_signature_algorithm")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("hmac-sha256")
        .to_string();

    if !signed || policy_signature.is_none() {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "unsigned",
                "verified": false,
                "policy_revision": policy_revision,
                "signature_algorithm": serde_json::Value::Null,
            }))?
        );
        if allow_unsigned {
            return Ok(());
        }
        return Err(anyhow!(
            "policy revision is unsigned; set --allow-unsigned to ignore"
        ));
    }

    if !signature_algorithm.eq_ignore_ascii_case("hmac-sha256") {
        return Err(anyhow!(
            "unsupported policy signature algorithm '{}'",
            signature_algorithm
        ));
    }

    let key = resolve_policy_verify_key(key).ok_or_else(|| {
        anyhow!(
            "missing verification key; pass --key or set ANNA_POLICY_VERIFY_KEY / ANNA_POLICY_SIGNING_KEY"
        )
    })?;
    let expected_signature = sign_policy_revision_hex(&policy_revision, &key)
        .ok_or_else(|| anyhow!("failed to compute policy signature"))?;
    let received_signature = policy_signature.unwrap_or_default();
    let verified = received_signature.eq_ignore_ascii_case(&expected_signature);

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": if verified { "verified" } else { "invalid_signature" },
            "verified": verified,
            "policy_revision": policy_revision,
            "signature_algorithm": signature_algorithm,
            "received_signature": received_signature,
            "expected_signature": expected_signature,
        }))?
    );

    if verified {
        Ok(())
    } else {
        Err(anyhow!("policy signature verification failed"))
    }
}

fn default_local_policy_snapshot_path() -> PathBuf {
    if let Ok(raw) = std::env::var("ANNA_POLICY_LOCAL_SNAPSHOT_FILE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed).join(".anna/policy.snapshot.json");
        }
    }
    PathBuf::from("policy.snapshot.json")
}

async fn read_local_policy_revision(path: &Path) -> Result<Option<String>> {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed reading local policy snapshot '{}'", path.display())
            });
        }
    };
    policy_revision_from_raw_json(&raw)
        .with_context(|| format!("failed parsing local policy snapshot '{}'", path.display()))
}

fn policy_revision_from_raw_json(raw: &str) -> Result<Option<String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(raw).context("policy snapshot is not valid json")?;
    Ok(policy_revision_from_json(&parsed))
}

fn policy_revision_from_json(value: &serde_json::Value) -> Option<String> {
    value
        .get("policy_revision")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn policy_signature_from_json(value: &serde_json::Value) -> Option<String> {
    value
        .get("policy_signature")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn resolve_policy_verify_key(key: Option<&str>) -> Option<String> {
    key.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            std::env::var("ANNA_POLICY_VERIFY_KEY")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .or_else(|| {
            std::env::var("ANNA_POLICY_SIGNING_KEY")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
}

fn sign_policy_revision_hex(revision: &str, key: &str) -> Option<String> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).ok()?;
    mac.update(revision.as_bytes());
    let bytes = mac.finalize().into_bytes();
    Some(hex_encode(&bytes))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn etag_revision(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .and_then(normalize_etag_value)
}

fn normalize_etag_value(value: &str) -> Option<String> {
    let mut trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return None;
    }
    if let Some(weak) = trimmed
        .strip_prefix("W/")
        .or_else(|| trimmed.strip_prefix("w/"))
    {
        trimmed = weak.trim();
    }
    let normalized = trimmed.trim_matches('"').trim();
    if normalized.is_empty() || normalized == "*" {
        return None;
    }
    Some(normalized.to_string())
}

async fn write_json_atomic(path: &Path, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating local policy snapshot directory '{}'",
                parent.display()
            )
        })?;
    }
    let raw = serde_json::to_string_pretty(value).context("serialize policy snapshot json")?;
    let tmp = temp_json_path(path);
    tokio::fs::write(&tmp, raw).await.with_context(|| {
        format!(
            "failed writing local policy snapshot temp '{}'",
            tmp.display()
        )
    })?;
    tokio::fs::rename(&tmp, path).await.with_context(|| {
        format!(
            "failed moving local policy snapshot '{}' -> '{}'",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn temp_json_path(path: &Path) -> PathBuf {
    if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
        let unique = anna_rs::session::gen_session_id();
        return path.with_file_name(format!("{}.{}.tmp", name, unique));
    }
    path.with_extension("tmp")
}

async fn wait_for_workflow(
    daemon: &str,
    id: &str,
    poll_ms: u64,
    timeout_sec: Option<u64>,
) -> Result<()> {
    let client = Client::new();
    let daemon = normalize_daemon_url(daemon);
    let started = Instant::now();
    let timeout = timeout_sec.map(Duration::from_secs);
    let mut previous_status = String::new();

    loop {
        if let Some(limit) = timeout
            && started.elapsed() >= limit
        {
            return Err(anyhow!(
                "timeout waiting for workflow '{}' after {}s",
                id,
                limit.as_secs()
            ));
        }

        let response = with_daemon_auth(client.get(format!("{}/workflow/{}", daemon, id)))
            .send()
            .await
            .with_context(|| format!("failed querying workflow '{}' at {}", id, daemon))?;
        let status_code = response.status();
        let body = response
            .text()
            .await
            .context("failed reading workflow status response")?;
        if !status_code.is_success() {
            if body.trim().is_empty() {
                return Err(anyhow!(
                    "request failed with status {} while waiting for '{}'",
                    status_code,
                    id
                ));
            }
            return Err(anyhow!(
                "request failed with status {} while waiting for '{}': {}",
                status_code,
                id,
                body
            ));
        }

        let parsed: serde_json::Value =
            serde_json::from_str(&body).context("daemon returned non-json workflow status")?;
        let status = parsed
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        if status != previous_status {
            println!("status={}", status);
            previous_status = status.clone();
        }

        if is_terminal_status(&status) {
            println!("{}", serde_json::to_string_pretty(&parsed)?);
            if status == "done" {
                return Ok(());
            }
            return Err(anyhow!(
                "workflow '{}' finished with non-success status '{}'",
                id,
                status
            ));
        }

        sleep(Duration::from_millis(poll_ms)).await;
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "done" | "failed" | "stopped" | "not_running"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        is_terminal_status, normalize_etag_value, policy_revision_from_json,
        policy_signature_from_json, resolve_policy_verify_key, sign_policy_revision_hex,
    };
    use serde_json::json;

    #[test]
    fn terminal_statuses_match_expected_values() {
        assert!(is_terminal_status("done"));
        assert!(is_terminal_status("FAILED"));
        assert!(is_terminal_status("stopped"));
        assert!(is_terminal_status("not_running"));
        assert!(!is_terminal_status("running"));
    }

    #[test]
    fn normalize_etag_value_handles_quotes_and_weak_tags() {
        assert_eq!(normalize_etag_value("\"rev-1\""), Some("rev-1".to_string()));
        assert_eq!(
            normalize_etag_value("W/\"rev-2\""),
            Some("rev-2".to_string())
        );
        assert_eq!(
            normalize_etag_value("w/\"rev-3\""),
            Some("rev-3".to_string())
        );
        assert_eq!(normalize_etag_value("*"), None);
        assert_eq!(normalize_etag_value(""), None);
    }

    #[test]
    fn policy_revision_from_json_reads_non_empty_value() {
        let value = json!({
            "policy_revision": "abc123",
        });
        assert_eq!(
            policy_revision_from_json(&value),
            Some("abc123".to_string())
        );
        let missing = json!({});
        assert_eq!(policy_revision_from_json(&missing), None);
        let empty = json!({ "policy_revision": "   " });
        assert_eq!(policy_revision_from_json(&empty), None);
    }

    #[test]
    fn policy_signature_from_json_reads_non_empty_value() {
        let value = json!({
            "policy_signature": "sig-abc",
        });
        assert_eq!(
            policy_signature_from_json(&value),
            Some("sig-abc".to_string())
        );
        assert_eq!(policy_signature_from_json(&json!({})), None);
        assert_eq!(
            policy_signature_from_json(&json!({ "policy_signature": "  " })),
            None
        );
    }

    #[test]
    fn sign_policy_revision_hex_is_stable_and_keyed() {
        let first = sign_policy_revision_hex("rev-1", "secret-a").expect("signature");
        let same = sign_policy_revision_hex("rev-1", "secret-a").expect("signature");
        let different = sign_policy_revision_hex("rev-1", "secret-b").expect("signature");
        assert_eq!(first, same);
        assert_ne!(first, different);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn resolve_policy_verify_key_prefers_explicit_input() {
        let key = resolve_policy_verify_key(Some("  my-key  "));
        assert_eq!(key, Some("my-key".to_string()));
    }
}
