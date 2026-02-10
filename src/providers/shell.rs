use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult, runtime_env};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

#[derive(Debug, Default, Clone)]
pub struct ShellProvider;

#[async_trait]
impl Provider for ShellProvider {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let exec = stage.exec.as_deref().ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' is missing 'exec' command", stage.id),
            )
        })?;
        let cmd_string = subst(exec, vars, outputs);

        let mut command = Command::new("sh");
        command
            .arg("-lc")
            .arg(cmd_string)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(dir) = stage.workdir.as_ref().or(workflow.workdir.as_ref()) {
            command.current_dir(dir);
        }

        for (k, v) in &workflow.env {
            command.env(k, subst(v, vars, outputs));
        }
        for (k, v) in &stage.env {
            command.env(k, subst(v, vars, outputs));
        }
        for (k, v) in runtime_env(stage, workflow, outputs) {
            command.env(k, v);
        }

        let output = run_with_timeout(command, timeout).await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let msg = if stderr.is_empty() { stdout } else { stderr };
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!("shell command failed in stage '{}': {}", stage.id, msg),
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout)
            .trim_end()
            .to_string())
    }
}

async fn run_with_timeout(
    mut command: Command,
    timeout: Option<Duration>,
) -> ProviderResult<std::process::Output> {
    let future = command.output();
    match timeout {
        Some(dur) => match tokio::time::timeout(dur, future).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(err)) => Err(map_spawn_error(err)),
            Err(_) => Err(ProviderError::new(
                "provider_timeout",
                format!("provider command exceeded timeout {:?}", dur),
            )),
        },
        None => future.await.map_err(map_spawn_error),
    }
}

fn map_spawn_error(err: std::io::Error) -> ProviderError {
    match err.kind() {
        std::io::ErrorKind::NotFound => ProviderError::new("provider_not_found", err.to_string()),
        _ => ProviderError::new("provider_start_failed", err.to_string()),
    }
}
