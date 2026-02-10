use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

#[derive(Debug, Clone)]
pub struct McpConfig {
    pub daemon_url: String,
    pub daemon_token: Option<String>,
}

pub async fn run_stdio_server(config: McpConfig) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();
    let mut writer = BufWriter::new(stdout);
    let client = Client::new();

    while let Some(line) = reader
        .next_line()
        .await
        .context("failed reading MCP stdin line")?
    {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let request: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(err) => {
                let response = rpc_error(Value::Null, -32700, format!("parse error: {}", err));
                writer.write_all(response.to_string().as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                continue;
            }
        };

        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").and_then(|v| v.as_str());
        let params = request.get("params").cloned().unwrap_or_else(|| json!({}));

        let Some(method) = method else {
            let response = rpc_error(id, -32600, "invalid request: missing method".to_string());
            writer.write_all(response.to_string().as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
            continue;
        };

        // MCP notifications have no id and should not emit response.
        let is_notification = request.get("id").is_none();
        if method == "initialized" && is_notification {
            continue;
        }

        let response = match method {
            "initialize" => rpc_result(id, initialize_result()),
            "initialized" => {
                if is_notification {
                    continue;
                }
                rpc_result(id, json!({}))
            }
            "ping" => rpc_result(id, json!({})),
            "tools/list" => rpc_result(id, json!({ "tools": tool_specs() })),
            "tools/call" => match handle_tools_call(&client, &config, &params).await {
                Ok(result_text) => rpc_result(
                    id,
                    json!({
                        "content": [{ "type": "text", "text": result_text }]
                    }),
                ),
                Err(err) => rpc_result(
                    id,
                    json!({
                        "isError": true,
                        "content": [{ "type": "text", "text": err.to_string() }]
                    }),
                ),
            },
            _ => rpc_error(id, -32601, format!("method not found: {}", method)),
        };

        writer.write_all(response.to_string().as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

    Ok(())
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": "anna-rs",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

fn tool_specs() -> Vec<Value> {
    vec![
        json!({
            "name": "list_flows",
            "description": "List registered Anna workflow files from daemon playbook directory",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }),
        json!({
            "name": "list_flows_meta",
            "description": "List registered workflows with metadata and capability availability",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tag": { "type": "string" },
                    "owner": { "type": "string" },
                    "capability": { "type": "string" },
                    "available": { "type": "boolean" },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "run_flow",
            "description": "Run workflow by registered name, or submit raw YAML",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "workflow_yaml": { "type": "string" },
                    "vars": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    },
                    "max_iterations": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "can_run_flow",
            "description": "Check whether a registered workflow can run now based on policy/capacity",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "session_status",
            "description": "Get workflow session status by request id",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "tail_logs",
            "description": "Fetch stage logs for workflow session by request id",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "stop_flow",
            "description": "Stop running workflow session by request id",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_sessions",
            "description": "List daemon workflow sessions",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "owner": { "type": "string" },
                    "workflow": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "stats",
            "description": "Get daemon stats summary",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }),
        json!({
            "name": "trigger_hook",
            "description": "Trigger daemon webhook hook by name",
            "inputSchema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_hitl",
            "description": "List HITL requests",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "session_id": { "type": "string" },
                    "workflow": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "resolve_hitl",
            "description": "Resolve HITL request with decision",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "decision": { "type": "string" }
                },
                "required": ["id", "decision"],
                "additionalProperties": false
            }
        }),
    ]
}

async fn handle_tools_call(client: &Client, config: &McpConfig, params: &Value) -> Result<String> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("tools/call missing params.name"))?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let daemon = normalize_daemon_url(&config.daemon_url);
    match name {
        "list_flows" => {
            let body = send(authed(
                client.get(format!("{}/workflows", daemon)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "list_flows_meta" => {
            let tag = arg_string(&args, "tag")?;
            let owner = arg_string(&args, "owner")?;
            let capability = arg_string(&args, "capability")?;
            let available = arg_bool(&args, "available")?;
            let limit = arg_u64(&args, "limit")?;

            let mut req = authed(
                client.get(format!("{}/workflows/meta", daemon)),
                &config.daemon_token,
            );
            if let Some(tag) = tag {
                req = req.query(&[("tag", tag)]);
            }
            if let Some(owner) = owner {
                req = req.query(&[("owner", owner)]);
            }
            if let Some(capability) = capability {
                req = req.query(&[("capability", capability)]);
            }
            if let Some(available) = available {
                req = req.query(&[("available", available)]);
            }
            if let Some(limit) = limit {
                req = req.query(&[("limit", limit)]);
            }

            let body = send(req).await?;
            Ok(body)
        }
        "run_flow" => {
            let workflow_name = arg_string(&args, "name")?;
            let workflow_yaml = arg_string(&args, "workflow_yaml")?;
            let vars = arg_string_map(&args, "vars")?;
            let max_iterations = arg_u32(&args, "max_iterations")?;
            match (workflow_name, workflow_yaml) {
                (Some(name), _) => {
                    let mut req = authed(
                        client.post(format!("{}/workflow/{}/run", daemon, name)),
                        &config.daemon_token,
                    );
                    if !vars.is_empty() || max_iterations.is_some() {
                        req = req.json(&json!({
                            "vars": vars,
                            "max_iterations": max_iterations,
                        }));
                    }
                    let body = send(req).await?;
                    Ok(body)
                }
                (None, Some(yaml)) => {
                    if !vars.is_empty() || max_iterations.is_some() {
                        return Err(anyhow!(
                            "run_flow with workflow_yaml does not support 'vars' or 'max_iterations'; use named flow"
                        ));
                    }
                    let body = send(
                        authed(
                            client.post(format!("{}/workflow", daemon)),
                            &config.daemon_token,
                        )
                        .body(yaml),
                    )
                    .await?;
                    Ok(body)
                }
                _ => Err(anyhow!(
                    "run_flow requires either 'name' or 'workflow_yaml'"
                )),
            }
        }
        "can_run_flow" => {
            let name = arg_required_string(&args, "name")?;
            let body = send(authed(
                client.get(format!("{}/workflow/{}/check", daemon, name)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "session_status" => {
            let id = arg_required_string(&args, "id")?;
            let body = send(authed(
                client.get(format!("{}/workflow/{}", daemon, id)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "tail_logs" => {
            let id = arg_required_string(&args, "id")?;
            let body = send(authed(
                client.get(format!("{}/workflow/{}/logs", daemon, id)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "stop_flow" => {
            let id = arg_required_string(&args, "id")?;
            let body = send(authed(
                client.delete(format!("{}/workflow/{}", daemon, id)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "list_sessions" => {
            let status = arg_string(&args, "status")?;
            let owner = arg_string(&args, "owner")?;
            let workflow = arg_string(&args, "workflow")?;
            let limit = arg_u64(&args, "limit")?;
            let mut req = authed(
                client.get(format!("{}/sessions", daemon)),
                &config.daemon_token,
            );
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
            let body = send(req).await?;
            Ok(body)
        }
        "stats" => {
            let body = send(authed(
                client.get(format!("{}/stats", daemon)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "trigger_hook" => {
            let name = arg_required_string(&args, "name")?;
            let body = send(authed(
                client.post(format!("{}/hook/{}", daemon, name.trim_matches('/'))),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "list_hitl" => {
            let status = arg_string(&args, "status")?;
            let session_id = arg_string(&args, "session_id")?;
            let workflow = arg_string(&args, "workflow")?;
            let limit = arg_u64(&args, "limit")?;

            let mut req = authed(client.get(format!("{}/hitl", daemon)), &config.daemon_token);
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
            let body = send(req).await?;
            Ok(body)
        }
        "resolve_hitl" => {
            let id = arg_required_string(&args, "id")?;
            let decision = arg_required_string(&args, "decision")?;
            let body = send(
                authed(
                    client.post(format!("{}/hitl/{}/resolve", daemon, id)),
                    &config.daemon_token,
                )
                .json(&json!({ "decision": decision })),
            )
            .await?;
            Ok(body)
        }
        other => Err(anyhow!("unknown tool '{}'", other)),
    }
}

fn arg_string(args: &Value, key: &str) -> Result<Option<String>> {
    match args.get(key) {
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(anyhow!("tool argument '{}' must be string", key)),
    }
}

fn arg_required_string(args: &Value, key: &str) -> Result<String> {
    arg_string(args, key)?.ok_or_else(|| anyhow!("tool argument '{}' is required", key))
}

fn arg_string_map(args: &Value, key: &str) -> Result<std::collections::HashMap<String, String>> {
    match args.get(key) {
        Some(Value::Object(map)) => {
            let mut out = std::collections::HashMap::new();
            for (k, v) in map {
                let Value::String(s) = v else {
                    return Err(anyhow!(
                        "tool argument '{}' values must be strings (key '{}')",
                        key,
                        k
                    ));
                };
                out.insert(k.clone(), s.clone());
            }
            Ok(out)
        }
        Some(Value::Null) | None => Ok(std::collections::HashMap::new()),
        Some(_) => Err(anyhow!("tool argument '{}' must be object", key)),
    }
}

fn arg_u64(args: &Value, key: &str) -> Result<Option<u64>> {
    match args.get(key) {
        Some(Value::Number(n)) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| anyhow!("tool argument '{}' must be non-negative integer", key)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(anyhow!(
            "tool argument '{}' must be non-negative integer",
            key
        )),
    }
}

fn arg_u32(args: &Value, key: &str) -> Result<Option<u32>> {
    match arg_u64(args, key)? {
        Some(v) => {
            let value = u32::try_from(v)
                .map_err(|_| anyhow!("tool argument '{}' is too large for u32", key))?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn arg_bool(args: &Value, key: &str) -> Result<Option<bool>> {
    match args.get(key) {
        Some(Value::Bool(v)) => Ok(Some(*v)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(anyhow!("tool argument '{}' must be boolean", key)),
    }
}

fn authed(builder: reqwest::RequestBuilder, token: &Option<String>) -> reqwest::RequestBuilder {
    match token.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        Some(token) => builder.bearer_auth(token),
        None => builder,
    }
}

async fn send(builder: reqwest::RequestBuilder) -> Result<String> {
    let response = builder.send().await.context("daemon request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading daemon response body")?;
    if status.is_success() {
        if let Ok(v) = serde_json::from_str::<Value>(&body) {
            return Ok(serde_json::to_string_pretty(&v)?);
        }
        return Ok(body);
    }
    if body.trim().is_empty() {
        return Err(anyhow!("daemon request failed with status {}", status));
    }
    Err(anyhow!(
        "daemon request failed with status {}: {}",
        status,
        body
    ))
}

fn normalize_daemon_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::{arg_bool, arg_string, arg_string_map, arg_u32, arg_u64, tool_specs};
    use serde_json::json;

    #[test]
    fn tool_specs_contains_expected_core_tools() {
        let specs = tool_specs();
        let names = specs
            .iter()
            .filter_map(|v| v.get("name").and_then(|n| n.as_str()))
            .collect::<Vec<_>>();
        assert!(names.contains(&"list_flows"));
        assert!(names.contains(&"list_flows_meta"));
        assert!(names.contains(&"run_flow"));
        assert!(names.contains(&"can_run_flow"));
        assert!(names.contains(&"session_status"));
        assert!(names.contains(&"tail_logs"));
        assert!(names.contains(&"stop_flow"));
        assert!(names.contains(&"stats"));
    }

    #[test]
    fn arg_helpers_parse_values() {
        let args = json!({ "a": "x", "n": 3, "vars": {"X": "1"}, "available": true });
        assert_eq!(
            arg_string(&args, "a").expect("a should parse"),
            Some("x".to_string())
        );
        assert_eq!(arg_u64(&args, "n").expect("n should parse"), Some(3));
        assert_eq!(arg_u32(&args, "n").expect("n should parse"), Some(3));
        assert_eq!(
            arg_bool(&args, "available").expect("available should parse"),
            Some(true)
        );
        assert_eq!(
            arg_string_map(&args, "vars").expect("vars should parse"),
            std::collections::HashMap::from([(String::from("X"), String::from("1"))])
        );
        assert_eq!(
            arg_string(&args, "missing").expect("missing should parse"),
            None
        );
    }
}
