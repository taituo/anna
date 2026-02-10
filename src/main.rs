use anna_rs::executor::{Executor, RunConfig};
use anna_rs::workflow::Workflow;
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
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
                .run(&wf, RunConfig { max_iterations })
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
