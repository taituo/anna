use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct HttpProvider;

#[async_trait]
impl Provider for HttpProvider {
    async fn run(
        &self,
        stage: &Stage,
        _workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let exec = stage.exec.as_ref().ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' requires 'exec' for provider=http", stage.id),
            )
        })?;
        let rendered = subst(exec, vars, outputs);

        let (method, url) = parse_method_and_url(&rendered).map_err(|msg| {
            ProviderError::new(
                "provider_exec_failed",
                format!("http provider stage '{}': {}", stage.id, msg),
            )
        })?;

        let client = reqwest::Client::builder()
            .timeout(timeout.unwrap_or(Duration::from_secs(60)))
            .build()
            .map_err(|err| ProviderError::new("provider_start_failed", err.to_string()))?;

        let request = match method {
            "GET" => client.get(url),
            "POST" => {
                let body = stage
                    .stdin
                    .as_deref()
                    .map(|v| subst(v, vars, outputs))
                    .unwrap_or_default();
                client.post(url).body(body)
            }
            _ => {
                return Err(ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "unsupported http method '{}' for stage '{}'",
                        method, stage.id
                    ),
                ));
            }
        };

        let response = request.send().await.map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("http request failed in stage '{}': {}", stage.id, err),
            )
        })?;

        let status = response.status();
        let body = response.text().await.map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "failed reading http response in stage '{}': {}",
                    stage.id, err
                ),
            )
        })?;

        if !status.is_success() {
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!("http status {} in stage '{}': {}", status, stage.id, body),
            ));
        }

        Ok(body)
    }
}

fn parse_method_and_url(exec: &str) -> Result<(&str, &str), &'static str> {
    let trimmed = exec.trim();
    if let Some(rest) = trimmed.strip_prefix("POST ") {
        if rest.trim().is_empty() {
            return Err("POST requires url");
        }
        return Ok(("POST", rest.trim()));
    }
    if let Some(rest) = trimmed.strip_prefix("GET ") {
        if rest.trim().is_empty() {
            return Err("GET requires url");
        }
        return Ok(("GET", rest.trim()));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(("GET", trimmed));
    }
    Err("exec must be URL or 'POST <url>'")
}
