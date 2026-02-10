use crate::expr::subst;
use crate::providers::{
    Provider, ProviderError, ProviderResult, resolve_stage_secrets, runtime_env,
};
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
        for (k, v) in runtime_env(stage, workflow, outputs) {
            command.env(k, v);
        }
        for (env_key, value) in resolve_stage_secrets(stage, vars, outputs)? {
            command.env(env_key, value);
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
            let code = map_exit_code(output.status.code(), &stderr);
            return Err(ProviderError::new(
                code,
                format!(
                    "cli command failed in stage '{}' (exit {:?}): {}",
                    stage.id,
                    output.status.code(),
                    best_error_message(&stderr)
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

fn map_exit_code(exit_code: Option<i32>, stderr: &str) -> &'static str {
    if let Some(code) = parse_error_code_from_stderr(stderr) {
        return code;
    }

    match exit_code {
        Some(10) => "provider_not_found",
        Some(11) => "provider_start_failed",
        Some(12) => "provider_timeout",
        Some(13) => "provider_invalid_response",
        Some(14) => "provider_exec_failed",
        _ => "provider_exec_failed",
    }
}

fn parse_error_code_from_stderr(stderr: &str) -> Option<&'static str> {
    for line in stderr.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed = serde_json::from_str::<serde_json::Value>(line).ok()?;
        let value = parsed.get("error_code")?.as_str()?;
        return match value {
            "provider_not_found" => Some("provider_not_found"),
            "provider_start_failed" => Some("provider_start_failed"),
            "provider_timeout" => Some("provider_timeout"),
            "provider_invalid_response" => Some("provider_invalid_response"),
            "provider_exec_failed" => Some("provider_exec_failed"),
            _ => None,
        };
    }
    None
}

fn best_error_message(stderr: &str) -> String {
    for line in stderr.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line)
            && let Some(message) = parsed.get("message").and_then(|v| v.as_str())
            && !message.trim().is_empty()
        {
            return message.to_string();
        }
        return line.to_string();
    }
    "command failed".to_string()
}

fn map_spawn_error(err: std::io::Error) -> ProviderError {
    match err.kind() {
        std::io::ErrorKind::NotFound => ProviderError::new("provider_not_found", err.to_string()),
        _ => ProviderError::new("provider_start_failed", err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::CliProvider;
    use crate::providers::Provider;
    use crate::workflow::{Stage, Workflow};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn make_workflow() -> Workflow {
        Workflow {
            name: "cli-provider-test".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env: HashMap::new(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![],
            source_path: None,
        }
    }

    #[cfg(unix)]
    async fn write_exec_script(name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "anna-cli-provider-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create temp script dir");
        let path = dir.join(name);
        tokio::fs::write(&path, body)
            .await
            .expect("write test script");

        let mut perms = std::fs::metadata(&path)
            .expect("read script metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).expect("set script executable");
        path
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn maps_provider_exit_codes() {
        let script = write_exec_script(
            "fail.sh",
            "#!/bin/sh\nprintf '{\"error_code\":\"provider_invalid_response\",\"message\":\"bad json\"}\\n' >&2\nexit 13\n",
        )
        .await;
        let stage = Stage {
            id: "cli-fail".to_string(),
            provider: "cli".to_string(),
            exec: Some(script.to_string_lossy().to_string()),
            ..Default::default()
        };
        let mut outputs = HashMap::new();
        outputs.insert("SESSION".to_string(), "sess-123".to_string());

        let err = CliProvider
            .run(&stage, &make_workflow(), &HashMap::new(), &outputs, None)
            .await
            .expect_err("cli provider should fail");
        assert_eq!(err.code, "provider_invalid_response");
        assert!(err.message.contains("bad json"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn injects_runtime_env_vars() {
        let script = write_exec_script(
            "env.sh",
            "#!/bin/sh\nprintf '%s|%s|%s|%s' \"$ANNA_SESSION\" \"$ANNA_WORKFLOW\" \"$ANNA_STAGE_ID\" \"$ANNA_TRUST\"\n",
        )
        .await;
        let stage = Stage {
            id: "cli-env".to_string(),
            provider: "cli".to_string(),
            exec: Some(script.to_string_lossy().to_string()),
            trust: Some("read".to_string()),
            ..Default::default()
        };
        let mut outputs = HashMap::new();
        outputs.insert("SESSION".to_string(), "sess-999".to_string());

        let out = CliProvider
            .run(&stage, &make_workflow(), &HashMap::new(), &outputs, None)
            .await
            .expect("cli provider should succeed");
        assert_eq!(out, "sess-999|cli-provider-test|cli-env|read");
    }

    #[test]
    fn map_exit_code_handles_wrapper_codes() {
        assert_eq!(super::map_exit_code(Some(10), ""), "provider_not_found");
        assert_eq!(super::map_exit_code(Some(11), ""), "provider_start_failed");
        assert_eq!(super::map_exit_code(Some(12), ""), "provider_timeout");
        assert_eq!(
            super::map_exit_code(Some(13), ""),
            "provider_invalid_response"
        );
        assert_eq!(super::map_exit_code(Some(14), ""), "provider_exec_failed");
        assert_eq!(super::map_exit_code(Some(99), ""), "provider_exec_failed");
    }

    #[test]
    fn parse_error_code_prefers_structured_stderr() {
        let stderr = "{\"error_code\":\"provider_timeout\",\"message\":\"oops\"}";
        assert_eq!(
            super::parse_error_code_from_stderr(stderr),
            Some("provider_timeout")
        );
    }

    #[test]
    fn test_best_error_message() {
        let stderr = "{\"error_code\":\"provider_exec_failed\",\"message\":\"boom\"}";
        assert_eq!(super::best_error_message(stderr), "boom");
        assert_eq!(super::best_error_message("plain error"), "plain error");
    }
}
