use anna_rs::executor::{Executor, RunConfig};
use anna_rs::policy_crypto::{
    sign_policy_revision_hmac_sha256, verify_policy_revision_hmac_sha256,
};
use anna_rs::policy_sync::{
    default_local_policy_snapshot_path, ensure_http_success, etag_revision,
    policy_revision_from_json, policy_signature_algorithm_from_json, policy_signature_from_json,
};
use anna_rs::providers::llm::{active_llm_adapter_name, load_llm_adapter_catalog_from_env};
use anna_rs::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::json;
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
        /// Verify policy signature before writing snapshot
        #[arg(long, default_value_t = false)]
        verify: bool,
        /// Verification key (fallback: ANNA_POLICY_VERIFY_KEY, then ANNA_POLICY_SIGNING_KEY)
        #[arg(long)]
        key: Option<String>,
        /// Verification key file path (read, trim, and use as key)
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Do not fail verify mode when daemon policy snapshot is unsigned
        #[arg(long, default_value_t = false)]
        allow_unsigned: bool,
    },
    /// Verify daemon policy revision signature (HMAC-SHA256)
    PolicyVerify {
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
        /// Verification key (fallback: ANNA_POLICY_VERIFY_KEY, then ANNA_POLICY_SIGNING_KEY)
        #[arg(long)]
        key: Option<String>,
        /// Verification key file path (read, trim, and use as key)
        #[arg(long)]
        key_file: Option<PathBuf>,
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

macro_rules! dispatch_cli_command {
    ($command:expr) => {
        match $command {
        Commands::Run {
            workflow,
            vars,
            max_iterations,
            dry_run,
        } => {
            let mut wf = Workflow::load(&workflow)?;
            let run_overrides = parse_var_overrides(vars)?;
            wf.vars.extend(run_overrides);

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
            let validated_workflow = Workflow::load(&workflow)?;
            println!(
                "valid workflow '{}' (stages={})",
                validated_workflow.name,
                validated_workflow.stages.len()
            );
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
            let submit_body = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("failed reading '{}'", workflow.display()))?;
            print_response(
                with_daemon_auth(
                    Client::new().post(format!("{}/workflow", normalize_daemon_url(&daemon))),
                )
                .body(submit_body)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed submitting workflow to {}",
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Workflows { daemon } => {
            print_response(
                with_daemon_auth(
                    Client::new().get(format!("{}/workflows", normalize_daemon_url(&daemon))),
                )
                .send()
                .await
                .with_context(|| {
                    format!("failed querying workflows at {}", normalize_daemon_url(&daemon))
                })?,
            )
            .await
        }
        Commands::WorkflowsMeta {
            daemon,
            tag,
            owner,
            capability,
            available,
            limit,
        } => {
            let mut metadata_request = with_daemon_auth(
                Client::new().get(format!(
                    "{}/workflows/meta",
                    normalize_daemon_url(&daemon)
                )),
            );
            if let Some(tag) = tag {
                metadata_request = metadata_request.query(&[("tag", tag)]);
            }
            if let Some(owner) = owner {
                metadata_request = metadata_request.query(&[("owner", owner)]);
            }
            if let Some(capability) = capability {
                metadata_request = metadata_request.query(&[("capability", capability)]);
            }
            if let Some(available) = available {
                metadata_request = metadata_request.query(&[("available", available)]);
            }
            if let Some(limit) = limit {
                metadata_request = metadata_request.query(&[("limit", limit)]);
            }
            print_response(
                metadata_request.send().await.with_context(|| {
                    format!(
                        "failed querying workflow metadata at {}",
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::RunNamed {
            name,
            daemon,
            vars,
            max_iterations,
            precheck,
        } => {
            if precheck {
                let precheck_response = with_daemon_auth(Client::new().get(format!(
                    "{}/workflow/{}/check",
                    normalize_daemon_url(&daemon),
                    name
                )))
                        .send()
                        .await
                        .with_context(|| {
                            format!(
                                "failed running precheck for workflow '{}' at {}",
                                name,
                                normalize_daemon_url(&daemon)
                            )
                        })?;
                let precheck_status = precheck_response.status();
                let precheck_body = precheck_response
                    .text()
                    .await
                    .context("failed reading precheck response body")?;
                if !precheck_status.is_success() {
                    if precheck_body.trim().is_empty() {
                        return Err(anyhow!(
                            "precheck request failed with status {} for '{}'",
                            precheck_status,
                            name
                        ));
                    }
                    return Err(anyhow!(
                        "precheck request failed with status {} for '{}': {}",
                        precheck_status,
                        name,
                        precheck_body
                    ));
                }
                let precheck_payload: serde_json::Value = serde_json::from_str(&precheck_body)
                    .context("daemon returned non-json precheck response")?;
                let can_run = precheck_payload
                    .get("can_run")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !can_run {
                    println!("{}", serde_json::to_string_pretty(&precheck_payload)?);
                    return Err(anyhow!("precheck blocked workflow '{}'", name));
                }
            }
            let run_named_overrides = parse_var_overrides(vars)?;
            let mut run_named_request = with_daemon_auth(Client::new().post(format!(
                "{}/workflow/{}/run",
                normalize_daemon_url(&daemon),
                name
            )));
            if !run_named_overrides.is_empty() || max_iterations.is_some() {
                run_named_request = run_named_request.json(&json!({
                    "vars": run_named_overrides,
                    "max_iterations": max_iterations
                }));
            }
            print_response(
                run_named_request.send().await.with_context(|| {
                    format!(
                        "failed launching workflow '{}' at {}",
                        name,
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::CanRun { name, daemon } => {
            print_response(
                with_daemon_auth(Client::new().get(format!(
                    "{}/workflow/{}/check",
                    normalize_daemon_url(&daemon),
                    name
                )))
                    .send()
                    .await
                    .with_context(|| {
                        format!(
                            "failed checking workflow '{}' at {}",
                            name,
                            normalize_daemon_url(&daemon)
                        )
                    })?,
            )
            .await
        }
        Commands::CanChat {
            intent,
            daemon,
            max_iterations,
            caller,
        } => {
            let mut can_chat_request = with_optional_caller(
                with_daemon_auth(Client::new().get(format!(
                    "{}/chat/{}/check",
                    normalize_daemon_url(&daemon),
                    intent
                ))),
                caller.as_deref(),
            );
            if let Some(max_iterations) = max_iterations {
                can_chat_request = can_chat_request.query(&[("max_iterations", max_iterations)]);
            }
            print_response(can_chat_request.send().await.with_context(|| {
                format!(
                    "failed checking chat intent '{}' at {}",
                    intent,
                    normalize_daemon_url(&daemon)
                )
            })?)
            .await
        }
        Commands::CanRunYaml { workflow, daemon } => {
            let workflow_yaml = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| {
                    format!("failed reading workflow file '{}'", workflow.display())
                })?;
            print_response(
                with_daemon_auth(Client::new().post(format!(
                    "{}/workflow/check",
                    normalize_daemon_url(&daemon)
                )))
                .body(workflow_yaml)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed checking workflow yaml '{}' at {}",
                        workflow.display(),
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Status { id, daemon } => {
            print_response(
                with_daemon_auth(
                    Client::new().get(format!("{}/workflow/{}", normalize_daemon_url(&daemon), id)),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed querying workflow '{}' at {}",
                        id,
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Sessions {
            daemon,
            status,
            owner,
            workflow,
            limit,
        } => {
            let mut sessions_request = with_daemon_auth(
                Client::new().get(format!("{}/sessions", normalize_daemon_url(&daemon))),
            );
            if let Some(status) = status {
                sessions_request = sessions_request.query(&[("status", status)]);
            }
            if let Some(owner) = owner {
                sessions_request = sessions_request.query(&[("owner", owner)]);
            }
            if let Some(workflow) = workflow {
                sessions_request = sessions_request.query(&[("workflow", workflow)]);
            }
            if let Some(limit) = limit {
                sessions_request = sessions_request.query(&[("limit", limit)]);
            }
            print_response(
                sessions_request.send().await.with_context(|| {
                    format!("failed querying sessions at {}", normalize_daemon_url(&daemon))
                })?,
            )
            .await
        }
        Commands::Stats { daemon } => {
            print_response(
                with_daemon_auth(
                    Client::new().get(format!("{}/stats", normalize_daemon_url(&daemon))),
                )
                .send()
                .await
                .with_context(|| {
                    format!("failed querying stats at {}", normalize_daemon_url(&daemon))
                })?,
            )
            .await
        }
        Commands::Policy {
            daemon,
            if_match,
            if_none_match,
        } => {
            print_response(
                with_optional_etag_preconditions(
                    with_daemon_auth(
                        Client::new().get(format!("{}/policy", normalize_daemon_url(&daemon))),
                    ),
                    if_match.as_deref(),
                    if_none_match.as_deref(),
                )
                .send()
                .await
                .with_context(|| {
                    format!("failed querying policy at {}", normalize_daemon_url(&daemon))
                })?,
            )
            .await
        }
        Commands::PolicyRevision {
            daemon,
            if_none_match,
        } => {
            print_response(
                with_optional_etag_preconditions(
                    with_daemon_auth(Client::new().get(format!(
                        "{}/policy/revision",
                        normalize_daemon_url(&daemon)
                    ))),
                    None,
                    if_none_match.as_deref(),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed querying policy revision at {}",
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::PolicySnapshot {
            daemon,
            if_match,
            if_none_match,
        } => {
            print_response(
                with_optional_etag_preconditions(
                    with_daemon_auth(Client::new().get(format!(
                        "{}/policy/snapshot",
                        normalize_daemon_url(&daemon)
                    ))),
                    if_match.as_deref(),
                    if_none_match.as_deref(),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed querying policy snapshot at {}",
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::PolicySync {
            daemon,
            output,
            retries,
            verify,
            key,
            key_file,
            allow_unsigned,
        } => {
            let policy_sync_daemon = normalize_daemon_url(&daemon);
            let policy_sync_output = output.unwrap_or_else(default_local_policy_snapshot_path);
            let policy_sync_key =
                resolve_inline_or_file_key(key.as_deref(), key_file.as_deref())?;
            let sync_options = PolicySyncOptions {
                output: &policy_sync_output,
                retries,
                verify,
                key: policy_sync_key.as_deref(),
                allow_unsigned,
            };
            sync_policy_snapshot(&policy_sync_daemon, &sync_options).await
        }
        Commands::PolicyVerify {
            daemon,
            key,
            key_file,
            allow_unsigned,
        } => {
            let policy_verify_daemon = normalize_daemon_url(&daemon);
            let policy_verify_key =
                resolve_inline_or_file_key(key.as_deref(), key_file.as_deref())?;
            verify_policy_revision_signature(
                &policy_verify_daemon,
                policy_verify_key.as_deref(),
                allow_unsigned,
            )
            .await
        }
        Commands::LlmAdapters { json, daemon } => {
            if let Some(daemon) = daemon {
                let adapters_daemon = normalize_daemon_url(&daemon);
                return print_response(
                    with_daemon_auth(Client::new().get(format!("{}/llm/adapters", adapters_daemon)))
                        .send()
                        .await
                        .with_context(|| {
                            format!("failed querying daemon llm adapters at {}", adapters_daemon)
                        })?,
                )
                .await;
            }
            let loaded = load_llm_adapter_catalog_from_env()?;
            match loaded {
                Some(loaded) => {
                    let selected = active_llm_adapter_name(Some(&loaded.catalog));
                    let mut names = loaded.catalog.adapters.keys().cloned().collect::<Vec<_>>();
                    names.sort();
                    if json {
                        let llm_adapters_payload = json!({
                            "configured": true,
                            "source": loaded.path,
                            "selected": selected,
                            "default": loaded.catalog.default,
                            "adapters": loaded.catalog.adapters,
                        });
                        println!("{}", serde_json::to_string_pretty(&llm_adapters_payload)?);
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
                        let llm_not_configured_payload = json!({
                            "configured": false,
                            "source": null,
                            "selected": null,
                            "default": null,
                            "adapters": {},
                            "note": "set ANNA_LLM_ADAPTERS_FILE to enable adapter catalog"
                        });
                        println!("{}", serde_json::to_string_pretty(&llm_not_configured_payload)?);
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
            let wait_daemon = normalize_daemon_url(&daemon);
            wait_for_workflow(&wait_daemon, &id, poll_ms.max(50), timeout_sec).await
        }
        Commands::Stop { id, daemon } => {
            print_response(
                with_daemon_auth(Client::new().delete(format!(
                    "{}/workflow/{}",
                    normalize_daemon_url(&daemon),
                    id
                )))
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed stopping workflow '{}' at {}",
                        id,
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Logs { id, daemon } => {
            print_response(
                with_daemon_auth(Client::new().get(format!(
                    "{}/workflow/{}/logs",
                    normalize_daemon_url(&daemon),
                    id
                )))
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed reading workflow logs '{}' at {}",
                        id,
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Hook { name, daemon } => {
            print_response(
                with_daemon_auth(Client::new().post(format!(
                    "{}/hook/{}",
                    normalize_daemon_url(&daemon),
                    name.trim_matches('/')
                )))
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed triggering hook '{}' at {}",
                        name,
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::ChatIntents { daemon } => {
            print_response(
                with_daemon_auth(
                    Client::new().get(format!("{}/chat/intents", normalize_daemon_url(&daemon))),
                )
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed listing chat intents at {}",
                        normalize_daemon_url(&daemon)
                    )
                })?,
            )
            .await
        }
        Commands::Chat {
            intent,
            daemon,
            caller,
            vars,
            max_iterations,
        } => {
            let chat_overrides = parse_var_overrides(vars)?;
            print_response(
                with_optional_caller(
                    with_daemon_auth(
                        Client::new().post(format!("{}/chat/run", normalize_daemon_url(&daemon))),
                    ),
                    caller.as_deref(),
                )
                .json(&json!({
                    "intent": intent,
                    "vars": chat_overrides,
                    "max_iterations": max_iterations
                }))
                .send()
                .await
                .with_context(|| {
                    format!("failed running chat intent at {}", normalize_daemon_url(&daemon))
                })?,
            )
            .await
        }
        Commands::Hitl { command } => match command {
            HitlCommands::List {
                daemon,
                status,
                session_id,
                workflow,
                limit,
            } => {
                let mut hitl_list_request = with_daemon_auth(
                    Client::new().get(format!("{}/hitl", normalize_daemon_url(&daemon))),
                );
                if let Some(status) = status {
                    hitl_list_request = hitl_list_request.query(&[("status", status)]);
                }
                if let Some(session_id) = session_id {
                    hitl_list_request = hitl_list_request.query(&[("session_id", session_id)]);
                }
                if let Some(workflow) = workflow {
                    hitl_list_request = hitl_list_request.query(&[("workflow", workflow)]);
                }
                if let Some(limit) = limit {
                    hitl_list_request = hitl_list_request.query(&[("limit", limit)]);
                }
                print_response(
                    hitl_list_request.send().await.with_context(|| {
                        format!("failed querying hitl at {}", normalize_daemon_url(&daemon))
                    })?,
                )
                .await
            }
            HitlCommands::Resolve {
                id,
                decision,
                daemon,
            } => {
                print_response(
                    with_daemon_auth(Client::new().post(format!(
                        "{}/hitl/{}/resolve",
                        normalize_daemon_url(&daemon),
                        id
                    )))
                    .json(&json!({ "decision": decision }))
                    .send()
                    .await
                    .with_context(|| {
                        format!(
                            "failed resolving hitl '{}' at {}",
                            id,
                            normalize_daemon_url(&daemon)
                        )
                    })?,
                )
                .await
            }
        },
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    dispatch_cli_command!(cli.command)
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

fn resolve_inline_or_file_key(
    inline: Option<&str>,
    key_file: Option<&Path>,
) -> Result<Option<String>> {
    if let Some(inline) = inline.map(str::trim).filter(|v| !v.is_empty()) {
        return Ok(Some(inline.to_string()));
    }
    let Some(path) = key_file else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed reading verification key file '{}'", path.display()))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!(
            "verification key file '{}' is empty",
            path.display()
        ));
    }
    Ok(Some(trimmed.to_string()))
}

fn daemon_auth_token() -> Option<String> {
    let token_value = std::env::var("ANNA_DAEMON_TOKEN").ok()?;
    let token = token_value.trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
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

struct PolicySyncOptions<'a> {
    output: &'a Path,
    retries: usize,
    verify: bool,
    key: Option<&'a str>,
    allow_unsigned: bool,
}

async fn sync_policy_snapshot(daemon: &str, options: &PolicySyncOptions<'_>) -> Result<()> {
    let client = Client::new();
    let previous_revision = read_local_policy_revision(options.output).await?;
    let sync_ctx = PolicySyncAttemptCtx {
        client: &client,
        daemon,
        output: options.output,
        previous_revision: previous_revision.as_deref(),
        options,
    };
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let revision_outcome = fetch_remote_policy_revision(&client, daemon, previous_revision.as_deref()).await?;
        let Some(remote_revision) = revision_outcome else {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "not_modified",
                    "path": options.output.display().to_string(),
                    "policy_revision": previous_revision,
                    "attempts": attempt,
                }))?
            );
            return Ok(());
        };
        if let Some(payload) = sync_policy_snapshot_attempt(&sync_ctx, attempt, &remote_revision).await? {
            println!("{}", payload);
            return Ok(());
        }
    }
}

struct PolicySyncAttemptCtx<'a> {
    client: &'a Client,
    daemon: &'a str,
    output: &'a Path,
    previous_revision: Option<&'a str>,
    options: &'a PolicySyncOptions<'a>,
}

async fn sync_policy_snapshot_attempt(
    ctx: &PolicySyncAttemptCtx<'_>,
    attempt: usize,
    remote_revision: &str,
) -> Result<Option<String>> {
    let mut snapshot = fetch_policy_snapshot_response(ctx.client, ctx.daemon, remote_revision).await?;
    if should_retry_after_precondition_failure(
        &snapshot,
        attempt,
        ctx.options.retries,
        remote_revision,
    )? {
        return Ok(None);
    }
    ensure_snapshot_success(snapshot.status, &snapshot.body)?;
    let snapshot_json = parse_snapshot_json(&snapshot.body)?;
    let snapshot_revision = resolve_snapshot_revision(&snapshot_json, snapshot.etag.take())?;
    if should_retry_after_revision_mismatch(
        remote_revision,
        &snapshot_revision,
        attempt,
        ctx.options.retries,
    )? {
        return Ok(None);
    }
    let signature_check = verify_sync_snapshot_signature(
        ctx.options.verify,
        remote_revision,
        &snapshot_json,
        ctx.options.key,
        ctx.options.allow_unsigned,
    )?;
    write_json_atomic(ctx.output, &snapshot_json).await?;
    let payload = sync_success_payload(
        ctx.output,
        ctx.previous_revision,
        remote_revision,
        attempt,
        signature_check.as_ref(),
    )?;
    Ok(Some(payload))
}

fn verify_sync_snapshot_signature(
    verify: bool,
    remote_revision: &str,
    snapshot_json: &serde_json::Value,
    key: Option<&str>,
    allow_unsigned: bool,
) -> Result<Option<PolicySignatureCheck>> {
    if !verify {
        return Ok(None);
    }
    let check = verify_policy_signature_fields(
        remote_revision,
        policy_signature_from_json(snapshot_json).as_deref(),
        policy_signature_algorithm_from_json(snapshot_json).as_deref(),
        key,
        allow_unsigned,
    )?;
    if !check.verified && check.status != "unsigned" {
        return Err(anyhow!(
            "policy snapshot signature verification failed (received='{}', expected='{}')",
            check.received_signature.as_deref().unwrap_or(""),
            check.expected_signature.as_deref().unwrap_or("")
        ));
    }
    Ok(Some(check))
}

fn sync_success_payload(
    output: &Path,
    previous_revision: Option<&str>,
    remote_revision: &str,
    attempt: usize,
    signature_check: Option<&PolicySignatureCheck>,
) -> Result<String> {
    let mut sync_payload = json!({
        "status": "synced",
        "path": output.display().to_string(),
        "previous_policy_revision": previous_revision,
        "policy_revision": remote_revision,
        "attempts": attempt,
    });
    if let Some(check) = signature_check {
        sync_payload["signature_verification"] = policy_signature_check_json(check);
    }
    Ok(serde_json::to_string_pretty(&sync_payload)?)
}

struct PolicySnapshotResponse {
    status: reqwest::StatusCode,
    etag: Option<String>,
    body: String,
}

async fn fetch_remote_policy_revision(
    client: &Client,
    daemon: &str,
    previous_revision: Option<&str>,
) -> Result<Option<String>> {
    let mut revision_request = with_daemon_auth(client.get(format!("{}/policy/revision", daemon)));
    if let Some(previous_revision) = previous_revision {
        revision_request =
            with_optional_etag_preconditions(revision_request, None, Some(previous_revision));
    }

    let revision_response = revision_request
        .send()
        .await
        .with_context(|| format!("failed querying policy revision at {}", daemon))?;
    let revision_status = revision_response.status();
    if revision_status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }

    let revision_etag = etag_revision(revision_response.headers());
    let revision_body = revision_response
        .text()
        .await
        .context("failed reading policy revision response body")?;
    ensure_http_success(revision_status, &revision_body, "policy revision request")?;
    let revision_json = parse_revision_json(&revision_body)?;
    let remote_revision = resolve_policy_revision(
        &revision_json,
        revision_etag,
        "policy revision response missing 'policy_revision'",
    )?;
    Ok(Some(remote_revision))
}

async fn fetch_policy_snapshot_response(
    client: &Client,
    daemon: &str,
    remote_revision: &str,
) -> Result<PolicySnapshotResponse> {
    let snapshot_response = with_optional_etag_preconditions(
        with_daemon_auth(client.get(format!("{}/policy/snapshot", daemon))),
        Some(remote_revision),
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
    Ok(PolicySnapshotResponse {
        status: snapshot_status,
        etag: snapshot_etag,
        body: snapshot_body,
    })
}

fn should_retry_after_precondition_failure(
    snapshot: &PolicySnapshotResponse,
    attempt: usize,
    retries: usize,
    remote_revision: &str,
) -> Result<bool> {
    if snapshot.status != reqwest::StatusCode::PRECONDITION_FAILED {
        return Ok(false);
    }
    if attempt <= retries {
        return Ok(true);
    }
    let current_revision = snapshot
        .etag
        .clone()
        .or_else(|| policy_revision_from_raw_json(&snapshot.body).ok().flatten());
    if snapshot.body.trim().is_empty() {
        if let Some(current_revision) = current_revision {
            return Err(anyhow!(
                "policy snapshot precondition failed after {} attempts (revision endpoint='{}', current='{}')",
                attempt,
                remote_revision,
                current_revision
            ));
        }
        return Err(anyhow!(
            "policy snapshot precondition failed after {} attempts (revision endpoint='{}')",
            attempt,
            remote_revision
        ));
    }
    if let Some(current_revision) = current_revision {
        return Err(anyhow!(
            "policy snapshot precondition failed after {} attempts (revision endpoint='{}', current='{}'): {}",
            attempt,
            remote_revision,
            current_revision,
            snapshot.body
        ));
    }
    Err(anyhow!(
        "policy snapshot precondition failed after {} attempts (revision endpoint='{}'): {}",
        attempt,
        remote_revision,
        snapshot.body
    ))
}

fn should_retry_after_revision_mismatch(
    remote_revision: &str,
    snapshot_revision: &str,
    attempt: usize,
    retries: usize,
) -> Result<bool> {
    if snapshot_revision == remote_revision {
        return Ok(false);
    }
    if attempt <= retries {
        return Ok(true);
    }
    Err(anyhow!(
        "policy snapshot revision mismatch after {} attempts (revision endpoint='{}', snapshot='{}')",
        attempt,
        remote_revision,
        snapshot_revision
    ))
}

fn parse_revision_json(raw: &str) -> Result<serde_json::Value> {
    serde_json::from_str(raw).context("daemon returned non-json policy revision response")
}

fn parse_snapshot_json(raw: &str) -> Result<serde_json::Value> {
    serde_json::from_str(raw).context("daemon returned non-json policy snapshot response")
}

fn resolve_policy_revision(
    payload: &serde_json::Value,
    etag: Option<String>,
    missing_message: &str,
) -> Result<String> {
    policy_revision_from_json(payload)
        .or(etag)
        .ok_or_else(|| anyhow!(missing_message.to_string()))
}

fn resolve_snapshot_revision(payload: &serde_json::Value, etag: Option<String>) -> Result<String> {
    resolve_policy_revision(payload, etag, "policy snapshot response missing 'policy_revision'")
}

fn ensure_snapshot_success(status: reqwest::StatusCode, body: &str) -> Result<()> {
    ensure_http_success(status, body, "policy snapshot request")
}

async fn verify_policy_revision_signature(
    daemon: &str,
    key: Option<&str>,
    allow_unsigned: bool,
) -> Result<()> {
    let verification_payload = fetch_policy_revision_payload(daemon).await?;
    let policy_revision = resolve_revision_from_payload(
        &verification_payload,
        "policy revision response missing 'policy_revision'",
    )?;
    let verification_check = verify_policy_signature_fields(
        &policy_revision,
        policy_signature_from_json(&verification_payload).as_deref(),
        policy_signature_algorithm_from_json(&verification_payload).as_deref(),
        key,
        allow_unsigned,
    )?;
    print_policy_signature_check(&policy_revision, &verification_check)?;

    if verification_check.verified || verification_check.status == "unsigned" {
        Ok(())
    } else {
        Err(anyhow!("policy signature verification failed"))
    }
}

async fn read_local_policy_revision(path: &Path) -> Result<Option<String>> {
    let snapshot_raw = match tokio::fs::read_to_string(path).await {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed reading local policy snapshot '{}'", path.display())
            });
        }
    };
    policy_revision_from_raw_json(&snapshot_raw)
        .with_context(|| format!("failed parsing local policy snapshot '{}'", path.display()))
}

fn policy_revision_from_raw_json(raw: &str) -> Result<Option<String>> {
    let parsed_snapshot: serde_json::Value =
        serde_json::from_str(raw).context("policy snapshot is not valid json")?;
    Ok(policy_revision_from_json(&parsed_snapshot))
}

#[derive(Debug, Clone)]
struct PolicySignatureCheck {
    status: &'static str,
    verified: bool,
    signature_algorithm: Option<String>,
    received_signature: Option<String>,
    expected_signature: Option<String>,
}

fn policy_signature_check_json(check: &PolicySignatureCheck) -> serde_json::Value {
    json!({
        "status": check.status,
        "verified": check.verified,
        "signature_algorithm": check.signature_algorithm,
        "received_signature": check.received_signature,
        "expected_signature": check.expected_signature,
    })
}

fn verify_policy_signature_fields(
    revision: &str,
    signature: Option<&str>,
    signature_algorithm: Option<&str>,
    key: Option<&str>,
    allow_unsigned: bool,
) -> Result<PolicySignatureCheck> {
    let received_signature = require_received_signature(signature, allow_unsigned)?;
    let Some(received_signature) = received_signature else {
        return Ok(unsigned_signature_status());
    };
    let signature_algorithm = parse_signature_algorithm(signature_algorithm)?;
    let signing_key = resolve_policy_verify_key(key).ok_or_else(|| {
        anyhow!(
            "missing verification key; pass --key or set ANNA_POLICY_VERIFY_KEY / ANNA_POLICY_SIGNING_KEY"
        )
    })?;
    let expected_signature = sign_policy_revision_hmac_sha256(revision, &signing_key)
        .ok_or_else(|| anyhow!("failed to compute policy signature"))?;
    let verified = verify_policy_revision_hmac_sha256(revision, &received_signature, &signing_key)
        .ok_or_else(|| anyhow!("policy signature is not valid hexadecimal"))?;
    Ok(PolicySignatureCheck {
        status: signature_status_from_verified(verified),
        verified,
        signature_algorithm: Some(signature_algorithm),
        received_signature: Some(received_signature),
        expected_signature: Some(expected_signature),
    })
}

async fn fetch_policy_revision_payload(daemon: &str) -> Result<serde_json::Value> {
    let verify_client = Client::new();
    let verify_response = with_daemon_auth(verify_client.get(format!("{}/policy/revision", daemon)))
        .send()
        .await
        .with_context(|| format!("failed querying policy revision at {}", daemon))?;
    let verify_status = verify_response.status();
    let verify_body = verify_response
        .text()
        .await
        .context("failed reading policy revision response body")?;
    ensure_http_success(verify_status, &verify_body, "policy revision request")?;
    parse_revision_json(&verify_body)
}

fn resolve_revision_from_payload(payload: &serde_json::Value, missing_message: &str) -> Result<String> {
    resolve_policy_revision(payload, None, missing_message)
}

fn print_policy_signature_check(revision: &str, check: &PolicySignatureCheck) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": check.status,
            "verified": check.verified,
            "policy_revision": revision,
            "signature_algorithm": check.signature_algorithm,
            "received_signature": check.received_signature,
            "expected_signature": check.expected_signature,
        }))?
    );
    Ok(())
}

fn require_received_signature(
    signature: Option<&str>,
    allow_unsigned: bool,
) -> Result<Option<String>> {
    let signature = signature.map(str::trim).filter(|v| !v.is_empty());
    if let Some(signature) = signature {
        return Ok(Some(signature.to_string()));
    }
    if allow_unsigned {
        return Ok(None);
    }
    Err(anyhow!(
        "policy revision is unsigned; set --allow-unsigned to ignore"
    ))
}

fn parse_signature_algorithm(signature_algorithm: Option<&str>) -> Result<String> {
    let parsed_algorithm = signature_algorithm
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("hmac-sha256")
        .to_string();
    if parsed_algorithm.eq_ignore_ascii_case("hmac-sha256") {
        return Ok(parsed_algorithm);
    }
    Err(anyhow!(
        "unsupported policy signature algorithm '{}'",
        parsed_algorithm
    ))
}

fn unsigned_signature_status() -> PolicySignatureCheck {
    PolicySignatureCheck {
        status: "unsigned",
        verified: false,
        signature_algorithm: None,
        received_signature: None,
        expected_signature: None,
    }
}

fn signature_status_from_verified(verified: bool) -> &'static str {
    if verified {
        "verified"
    } else {
        "invalid_signature"
    }
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

async fn write_json_atomic(path: &Path, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating local policy snapshot directory '{}'",
                parent.display()
            )
        })?;
    }
    let serialized_json =
        serde_json::to_string_pretty(value).context("serialize policy snapshot json")?;
    let tmp = temp_json_path(path);
    tokio::fs::write(&tmp, serialized_json).await.with_context(|| {
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
    let wait_client = Client::new();
    let daemon = normalize_daemon_url(daemon);
    let started = Instant::now();
    let timeout = timeout_sec.map(Duration::from_secs);
    let mut previous_status = String::new();

    loop {
        if let Some(limit_sec) = wait_timeout_secs(started, timeout) {
            return Err(anyhow!(
                "timeout waiting for workflow '{}' after {}s",
                id,
                limit_sec
            ));
        }

        let status_payload = fetch_workflow_status_payload(&wait_client, &daemon, id).await?;
        let workflow_status = workflow_status_value(&status_payload);

        if workflow_status != previous_status {
            println!("status={}", workflow_status);
            previous_status = workflow_status.to_owned();
        }

        if let Some(terminal_result) = handle_terminal_status(id, &workflow_status, &status_payload)? {
            return terminal_result;
        }

        sleep(Duration::from_millis(poll_ms)).await;
    }
}

async fn fetch_workflow_status_payload(client: &Client, daemon: &str, id: &str) -> Result<serde_json::Value> {
    let workflow_response = with_daemon_auth(client.get(format!("{}/workflow/{}", daemon, id)))
        .send()
        .await
        .with_context(|| format!("failed querying workflow '{}' at {}", id, daemon))?;
    let status_code = workflow_response.status();
    let workflow_body = workflow_response
        .text()
        .await
        .context("failed reading workflow status response")?;
    ensure_http_success(
        status_code,
        &workflow_body,
        &format!("request while waiting for '{}'", id),
    )?;
    serde_json::from_str(&workflow_body).context("daemon returned non-json workflow status")
}

fn workflow_status_value(status_payload: &serde_json::Value) -> String {
    status_payload
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

fn handle_terminal_status(
    id: &str,
    workflow_status: &str,
    status_payload: &serde_json::Value,
) -> Result<Option<Result<()>>> {
    if !is_terminal_status(workflow_status) {
        return Ok(None);
    }
    println!("{}", serde_json::to_string_pretty(status_payload)?);
    if workflow_status == "done" {
        return Ok(Some(Ok(())));
    }
    Ok(Some(Err(anyhow!(
        "workflow '{}' finished with non-success status '{}'",
        id,
        workflow_status
    ))))
}

fn wait_timeout_secs(started: Instant, timeout: Option<Duration>) -> Option<u64> {
    let limit = timeout?;
    if started.elapsed() >= limit {
        return Some(limit.as_secs());
    }
    None
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
        is_terminal_status, resolve_inline_or_file_key, resolve_policy_verify_key,
        verify_policy_signature_fields,
    };
    use anna_rs::policy_sync::{
        normalize_etag_value, policy_revision_from_json, policy_signature_algorithm_from_json,
        policy_signature_from_json,
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
        let revision_payload = json!({
            "policy_revision": "abc123",
        });
        assert_eq!(
            policy_revision_from_json(&revision_payload),
            Some("abc123".to_string())
        );
        let missing = json!({});
        assert_eq!(policy_revision_from_json(&missing), None);
        let empty = json!({ "policy_revision": "   " });
        assert_eq!(policy_revision_from_json(&empty), None);
    }

    #[test]
    fn policy_signature_from_json_reads_non_empty_value() {
        let signature_payload = json!({
            "policy_signature": "sig-abc",
        });
        assert_eq!(
            policy_signature_from_json(&signature_payload),
            Some("sig-abc".to_string())
        );
        assert_eq!(policy_signature_from_json(&json!({})), None);
        assert_eq!(
            policy_signature_from_json(&json!({ "policy_signature": "  " })),
            None
        );
    }

    #[test]
    fn policy_signature_algorithm_from_json_reads_non_empty_value() {
        let algorithm_payload = json!({
            "policy_signature_algorithm": "hmac-sha256",
        });
        assert_eq!(
            policy_signature_algorithm_from_json(&algorithm_payload),
            Some("hmac-sha256".to_string())
        );
        assert_eq!(policy_signature_algorithm_from_json(&json!({})), None);
    }

    #[test]
    fn resolve_policy_verify_key_prefers_explicit_input() {
        let explicit_key = resolve_policy_verify_key(Some("  my-key  "));
        assert_eq!(explicit_key, Some("my-key".to_string()));
    }

    #[test]
    fn resolve_inline_or_file_key_reads_trimmed_file_value() {
        let path = std::env::temp_dir().join(format!(
            "anna-policy-key-{}-{}.txt",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::write(&path, "  file-key  \n").expect("write key file");
        let file_key = resolve_inline_or_file_key(None, Some(&path))
            .expect("resolve key")
            .expect("key should exist");
        assert_eq!(file_key, "file-key");
        std::fs::remove_file(&path).expect("remove key file");
    }

    #[test]
    fn verify_policy_signature_fields_handles_unsigned_and_invalid() {
        let unsigned = verify_policy_signature_fields("rev-1", None, None, None, true)
            .expect("unsigned allowed should pass");
        assert_eq!(unsigned.status, "unsigned");
        assert!(!unsigned.verified);

        let invalid = verify_policy_signature_fields(
            "rev-1",
            Some("deadbeef"),
            Some("hmac-sha256"),
            Some("secret-a"),
            false,
        )
        .expect("invalid signature should return check");
        assert_eq!(invalid.status, "invalid_signature");
        assert!(!invalid.verified);
    }

}
