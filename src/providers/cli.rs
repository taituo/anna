use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

#[derive(Debug, Default, Clone)]
pub struct CliProvider;

#[async_trait]
impl Provider for CliProvider {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let exec = stage.exec.as_ref().ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' requires 'exec' for provider=cli", stage.id),
            )
        })?;
        let exec = subst(exec, vars, outputs);

        let mut command = Command::new(exec);
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if stage.stdin.is_some() {
            command.stdin(Stdio::piped());
        }

        if let Some(dir) = stage.workdir.as_ref().or(workflow.workdir.as_ref()) {
            command.current_dir(dir);
        }
        for arg in &stage.args {
            command.arg(subst(arg, vars, outputs));
        }
        for (k, v) in &workflow.env {
            command.env(k, subst(v, vars, outputs));
        }
        for (k, v) in &stage.env {
            command.env(k, subst(v, vars, outputs));
        }

        let mut child = command.spawn().map_err(map_spawn_error)?;
        if let Some(stdin_payload) = stage.stdin.as_ref() {
            let rendered = subst(stdin_payload, vars, outputs);
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(rendered.as_bytes()).await.map_err(|err| {
                    ProviderError::new(
                        "provider_start_failed",
                        format!("failed to write stdin for stage '{}': {}", stage.id, err),
                    )
                })?;
            }
        }

        let wait = child.wait_with_output();
        let output = match timeout {
            Some(dur) => match tokio::time::timeout(dur, wait).await {
                Ok(Ok(output)) => output,
                Ok(Err(err)) => return Err(map_spawn_error(err)),
                Err(_) => {
                    return Err(ProviderError::new(
                        "provider_timeout",
                        format!("cli provider timed out after {:?}", dur),
                    ));
                }
            },
            None => wait.await.map_err(map_spawn_error)?,
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!(
                    "cli command failed in stage '{}' (exit {:?}): {}",
                    stage.id,
                    output.status.code(),
                    stderr
                ),
            ));
        }

        let raw = String::from_utf8_lossy(&output.stdout)
            .trim_end()
            .to_string();
        match stage.parse.as_deref().unwrap_or("text") {
            "text" => Ok(raw),
            "json" => {
                let value: serde_json::Value = serde_json::from_str(&raw).map_err(|err| {
                    ProviderError::new(
                        "provider_invalid_response",
                        format!(
                            "invalid json from cli provider in stage '{}': {}",
                            stage.id, err
                        ),
                    )
                })?;
                Ok(value.to_string())
            }
            other => Err(ProviderError::new(
                "provider_invalid_response",
                format!(
                    "unsupported parse mode '{}' in stage '{}', expected text|json",
                    other, stage.id
                ),
            )),
        }
    }
}

fn map_spawn_error(err: std::io::Error) -> ProviderError {
    match err.kind() {
        std::io::ErrorKind::NotFound => ProviderError::new("provider_not_found", err.to_string()),
        _ => ProviderError::new("provider_start_failed", err.to_string()),
    }
}
