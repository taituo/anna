use anna_rs::executor::{Executor, RunConfig};
use anna_rs::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

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
    /// Submit workflow YAML file to daemon
    Submit {
        /// Path to .anna workflow file
        workflow: PathBuf,
        /// Daemon base URL
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        daemon: String,
    },
    /// Run registered workflow by name/file stem on daemon
    RunNamed {
        /// Workflow name, file name, or file stem
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
        Commands::Submit { workflow, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let body = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("failed reading '{}'", workflow.display()))?;
            let client = Client::new();
            let response = client
                .post(format!("{}/workflow", daemon))
                .body(body)
                .send()
                .await
                .with_context(|| format!("failed submitting workflow to {}", daemon))?;
            print_response(response).await
        }
        Commands::RunNamed { name, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = client
                .post(format!("{}/workflow/{}/run", daemon, name))
                .send()
                .await
                .with_context(|| format!("failed launching workflow '{}' at {}", name, daemon))?;
            print_response(response).await
        }
        Commands::Status { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = client
                .get(format!("{}/workflow/{}", daemon, id))
                .send()
                .await
                .with_context(|| format!("failed querying workflow '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Stop { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = client
                .delete(format!("{}/workflow/{}", daemon, id))
                .send()
                .await
                .with_context(|| format!("failed stopping workflow '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Logs { id, daemon } => {
            let daemon = normalize_daemon_url(&daemon);
            let client = Client::new();
            let response = client
                .get(format!("{}/workflow/{}/logs", daemon, id))
                .send()
                .await
                .with_context(|| format!("failed reading workflow logs '{}' at {}", id, daemon))?;
            print_response(response).await
        }
        Commands::Hitl { command } => match command {
            HitlCommands::List { daemon } => {
                let daemon = normalize_daemon_url(&daemon);
                let client = Client::new();
                let response = client
                    .get(format!("{}/hitl", daemon))
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
                let response = client
                    .post(format!("{}/hitl/{}/resolve", daemon, id))
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
