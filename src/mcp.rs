use crate::providers::llm::{active_llm_adapter_name, load_llm_adapter_catalog_from_env};
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
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
                    "max_iterations": { "type": "integer", "minimum": 1 },
                    "precheck": { "type": "boolean" }
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
            "name": "can_run_flow_yaml",
            "description": "Check whether raw workflow YAML can run under daemon policy",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow_yaml": { "type": "string" }
                },
                "required": ["workflow_yaml"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "can_run_chat_intent",
            "description": "Check whether a chat intent can run now based on policy/capacity",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": { "type": "string" },
                    "max_iterations": { "type": "integer", "minimum": 1 },
                    "caller": { "type": "string" }
                },
                "required": ["intent"],
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
            "name": "policy",
            "description": "Get daemon policy and capability configuration summary",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "if_match": { "type": "string" },
                    "if_none_match": { "type": "string" }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "policy_revision",
            "description": "Get daemon policy revision/hash and optional signature",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "if_none_match": { "type": "string" }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "policy_snapshot",
            "description": "Get daemon effective policy snapshot",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "if_match": { "type": "string" },
                    "if_none_match": { "type": "string" }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "policy_sync",
            "description": "Sync daemon policy snapshot to local file with revision-safe preconditions",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "output": { "type": "string" },
                    "retries": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_llm_adapters",
            "description": "List local LLM adapter catalog loaded from ANNA_LLM_ADAPTERS_FILE",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }),
        json!({
            "name": "daemon_llm_adapters",
            "description": "Fetch daemon LLM adapter catalog from /llm/adapters",
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
            "name": "list_chat_intents",
            "description": "List daemon chat intent routes",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }),
        json!({
            "name": "run_chat_intent",
            "description": "Run approved workflow through daemon chat intent mapping",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": { "type": "string" },
                    "caller": { "type": "string" },
                    "vars": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    },
                    "max_iterations": { "type": "integer", "minimum": 1 }
                },
                "required": ["intent"],
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
            let precheck = arg_bool(&args, "precheck")?.unwrap_or(false);
            match (workflow_name, workflow_yaml) {
                (Some(name), _) => {
                    if precheck {
                        let parsed = check_named_flow_readiness(
                            client,
                            &daemon,
                            &config.daemon_token,
                            &name,
                        )
                        .await?;
                        let can_run = parsed
                            .get("can_run")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        if !can_run {
                            return Err(anyhow!(
                                "run_flow precheck blocked '{}': {}",
                                name,
                                serde_json::to_string_pretty(&parsed)?
                            ));
                        }
                    }
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
                    if precheck {
                        return Err(anyhow!("run_flow precheck requires named flow ('name')"));
                    }
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
        "can_run_flow_yaml" => {
            let workflow_yaml = arg_required_string(&args, "workflow_yaml")?;
            let body = send(
                authed(
                    client.post(format!("{}/workflow/check", daemon)),
                    &config.daemon_token,
                )
                .body(workflow_yaml),
            )
            .await?;
            Ok(body)
        }
        "can_run_chat_intent" => {
            let intent = arg_required_string(&args, "intent")?;
            let max_iterations = arg_u32(&args, "max_iterations")?;
            let caller = arg_string(&args, "caller")?;
            let mut req = with_optional_caller(
                authed(
                    client.get(format!("{}/chat/{}/check", daemon, intent)),
                    &config.daemon_token,
                ),
                caller.as_deref(),
            );
            if let Some(max_iterations) = max_iterations {
                req = req.query(&[("max_iterations", max_iterations)]);
            }
            let body = send(req).await?;
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
        "policy" => {
            let if_match = arg_string(&args, "if_match")?;
            let if_none_match = arg_string(&args, "if_none_match")?;
            let body = send(with_optional_etag_preconditions(
                authed(
                    client.get(format!("{}/policy", daemon)),
                    &config.daemon_token,
                ),
                if_match.as_deref(),
                if_none_match.as_deref(),
            ))
            .await?;
            Ok(body)
        }
        "policy_revision" => {
            let if_none_match = arg_string(&args, "if_none_match")?;
            let body = send(with_optional_etag_preconditions(
                authed(
                    client.get(format!("{}/policy/revision", daemon)),
                    &config.daemon_token,
                ),
                None,
                if_none_match.as_deref(),
            ))
            .await?;
            Ok(body)
        }
        "policy_snapshot" => {
            let if_match = arg_string(&args, "if_match")?;
            let if_none_match = arg_string(&args, "if_none_match")?;
            let body = send(with_optional_etag_preconditions(
                authed(
                    client.get(format!("{}/policy/snapshot", daemon)),
                    &config.daemon_token,
                ),
                if_match.as_deref(),
                if_none_match.as_deref(),
            ))
            .await?;
            Ok(body)
        }
        "policy_sync" => {
            let output = arg_string(&args, "output")?
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .map(PathBuf::from);
            let retries = arg_u64(&args, "retries")?
                .map(|v| usize::try_from(v).context("tool argument 'retries' is too large"))
                .transpose()?
                .unwrap_or(3);
            sync_policy_snapshot(client, &daemon, &config.daemon_token, output, retries).await
        }
        "list_llm_adapters" => {
            let loaded = load_llm_adapter_catalog_from_env()?;
            let payload = match loaded {
                Some(loaded) => json!({
                    "configured": true,
                    "source": loaded.path,
                    "selected": active_llm_adapter_name(Some(&loaded.catalog)),
                    "default": loaded.catalog.default,
                    "adapters": loaded.catalog.adapters,
                }),
                None => json!({
                    "configured": false,
                    "source": Value::Null,
                    "selected": Value::Null,
                    "default": Value::Null,
                    "adapters": {},
                    "note": "set ANNA_LLM_ADAPTERS_FILE to enable adapter catalog"
                }),
            };
            Ok(serde_json::to_string_pretty(&payload)?)
        }
        "daemon_llm_adapters" => {
            let body = send(authed(
                client.get(format!("{}/llm/adapters", daemon)),
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
        "list_chat_intents" => {
            let body = send(authed(
                client.get(format!("{}/chat/intents", daemon)),
                &config.daemon_token,
            ))
            .await?;
            Ok(body)
        }
        "run_chat_intent" => {
            let intent = arg_required_string(&args, "intent")?;
            let caller = arg_string(&args, "caller")?;
            let vars = arg_string_map(&args, "vars")?;
            let max_iterations = arg_u32(&args, "max_iterations")?;
            let body = send(
                with_optional_caller(
                    authed(
                        client.post(format!("{}/chat/run", daemon)),
                        &config.daemon_token,
                    ),
                    caller.as_deref(),
                )
                .json(&json!({
                    "intent": intent,
                    "vars": vars,
                    "max_iterations": max_iterations,
                })),
            )
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

async fn send(builder: reqwest::RequestBuilder) -> Result<String> {
    let response = builder.send().await.context("daemon request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading daemon response body")?;
    if status == reqwest::StatusCode::NOT_MODIFIED {
        let payload = json!({
            "status": "not_modified",
            "status_code": status.as_u16(),
        });
        return Ok(serde_json::to_string_pretty(&payload)?);
    }
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

async fn sync_policy_snapshot(
    client: &Client,
    daemon: &str,
    token: &Option<String>,
    output: Option<PathBuf>,
    retries: usize,
) -> Result<String> {
    let output = output.unwrap_or_else(default_local_policy_snapshot_path);
    let previous_revision = read_local_policy_revision(&output).await?;
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let mut revision_request = authed(client.get(format!("{}/policy/revision", daemon)), token);
        if let Some(previous_revision) = previous_revision.as_deref() {
            revision_request =
                with_optional_etag_preconditions(revision_request, None, Some(previous_revision));
        }

        let revision_response = revision_request
            .send()
            .await
            .context("daemon request failed (policy revision)")?;
        let revision_status = revision_response.status();
        if revision_status == reqwest::StatusCode::NOT_MODIFIED {
            let payload = json!({
                "status": "not_modified",
                "path": output.display().to_string(),
                "policy_revision": previous_revision,
                "attempts": attempt,
            });
            return Ok(serde_json::to_string_pretty(&payload)?);
        }

        let revision_etag = etag_revision(revision_response.headers());
        let revision_body = revision_response
            .text()
            .await
            .context("failed reading daemon policy revision response body")?;
        if !revision_status.is_success() {
            if revision_body.trim().is_empty() {
                return Err(anyhow!(
                    "daemon policy revision request failed with status {}",
                    revision_status
                ));
            }
            return Err(anyhow!(
                "daemon policy revision request failed with status {}: {}",
                revision_status,
                revision_body
            ));
        }

        let revision_json: Value = serde_json::from_str(&revision_body)
            .context("daemon policy revision response was not valid json")?;
        let remote_revision = policy_revision_from_json(&revision_json)
            .or(revision_etag)
            .ok_or_else(|| anyhow!("daemon policy revision response missing 'policy_revision'"))?;

        let snapshot_response = with_optional_etag_preconditions(
            authed(client.get(format!("{}/policy/snapshot", daemon)), token),
            Some(&remote_revision),
            None,
        )
        .send()
        .await
        .context("daemon request failed (policy snapshot)")?;
        let snapshot_status = snapshot_response.status();
        let snapshot_etag = etag_revision(snapshot_response.headers());
        let snapshot_body = snapshot_response
            .text()
            .await
            .context("failed reading daemon policy snapshot response body")?;

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
                    "daemon policy snapshot request failed with status {}",
                    snapshot_status
                ));
            }
            return Err(anyhow!(
                "daemon policy snapshot request failed with status {}: {}",
                snapshot_status,
                snapshot_body
            ));
        }

        let snapshot_json: Value = serde_json::from_str(&snapshot_body)
            .context("daemon policy snapshot response was not valid json")?;
        let snapshot_revision = policy_revision_from_json(&snapshot_json)
            .or(snapshot_etag)
            .ok_or_else(|| anyhow!("daemon policy snapshot response missing 'policy_revision'"))?;
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

        write_json_atomic(&output, &snapshot_json).await?;
        let payload = json!({
            "status": "synced",
            "path": output.display().to_string(),
            "previous_policy_revision": previous_revision,
            "policy_revision": remote_revision,
            "attempts": attempt,
        });
        return Ok(serde_json::to_string_pretty(&payload)?);
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
    let parsed: Value = serde_json::from_str(raw).context("policy snapshot is not valid json")?;
    Ok(policy_revision_from_json(&parsed))
}

fn policy_revision_from_json(value: &Value) -> Option<String> {
    value
        .get("policy_revision")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
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

async fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
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
        let unique = crate::session::gen_session_id();
        return path.with_file_name(format!("{}.{}.tmp", name, unique));
    }
    path.with_extension("tmp")
}

async fn check_named_flow_readiness(
    client: &Client,
    daemon: &str,
    token: &Option<String>,
    name: &str,
) -> Result<Value> {
    let response = authed(
        client.get(format!("{}/workflow/{}/check", daemon, name)),
        token,
    )
    .send()
    .await
    .context("daemon precheck request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed reading daemon precheck response body")?;
    if !status.is_success() {
        if body.trim().is_empty() {
            return Err(anyhow!(
                "daemon precheck request failed with status {}",
                status
            ));
        }
        return Err(anyhow!(
            "daemon precheck request failed with status {}: {}",
            status,
            body
        ));
    }

    serde_json::from_str::<Value>(&body).context("daemon precheck response was not valid json")
}

fn normalize_daemon_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        arg_bool, arg_string, arg_string_map, arg_u32, arg_u64, normalize_etag_value,
        policy_revision_from_json, tool_specs,
    };
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
        assert!(names.contains(&"can_run_flow_yaml"));
        assert!(names.contains(&"can_run_chat_intent"));
        assert!(names.contains(&"session_status"));
        assert!(names.contains(&"tail_logs"));
        assert!(names.contains(&"stop_flow"));
        assert!(names.contains(&"stats"));
        assert!(names.contains(&"policy"));
        assert!(names.contains(&"policy_revision"));
        assert!(names.contains(&"policy_snapshot"));
        assert!(names.contains(&"policy_sync"));
        assert!(names.contains(&"list_llm_adapters"));
        assert!(names.contains(&"daemon_llm_adapters"));
        assert!(names.contains(&"list_chat_intents"));
        assert!(names.contains(&"run_chat_intent"));
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

    #[test]
    fn etag_and_policy_revision_helpers_are_stable() {
        assert_eq!(
            normalize_etag_value("W/\"rev-1\""),
            Some("rev-1".to_string())
        );
        assert_eq!(normalize_etag_value("*"), None);

        let payload = json!({
            "policy_revision": "abc123"
        });
        assert_eq!(
            policy_revision_from_json(&payload),
            Some("abc123".to_string())
        );
    }
}
