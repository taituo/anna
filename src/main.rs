use anna_rs::executor::{Executor, RunConfig};
use anna_rs::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
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
    },
    /// Check whether a registered workflow can run right now
    CanRun {
        /// Workflow name, file name, flow_id, or file stem
        name: String,
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
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
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
            limit,
        } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let mut req = with_daemon_auth(client.get(format!("{}/sessions", daemon)));
            if let Some(status) = status {
                req = req.query(&[("status", status)]);
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

async fn print_response(response: reqwest::Response) -> Result<()> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading response body")?;

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
    use super::is_terminal_status;

    #[test]
    fn terminal_statuses_match_expected_values() {
        assert!(is_terminal_status("done"));
        assert!(is_terminal_status("FAILED"));
        assert!(is_terminal_status("stopped"));
        assert!(is_terminal_status("not_running"));
        assert!(!is_terminal_status("running"));
    }
}
