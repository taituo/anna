use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

#[path = "vault_http.rs"]
mod vault_http;
#[path = "vault_config.rs"]
mod vault_config;
#[path = "vault_store.rs"]
mod vault_store;

#[derive(Debug, Default, Clone)]
/// Provider that reads/writes secrets via file or Vault-compatible HTTP APIs.
pub struct VaultProvider;

#[derive(Debug, Clone)]
struct VaultConfig {
    backend: VaultBackend,
    kv_file: PathBuf,
    allow_prefixes: Option<Vec<String>>,
    read_only: bool,
    http: Option<VaultHttpConfig>,
}

#[derive(Debug, Clone, Copy)]
enum VaultBackend {
    File,
    Http,
}

#[derive(Debug, Clone)]
struct VaultHttpConfig {
    addr: String,
    auth: VaultAuthConfig,
    namespace: Option<String>,
    mount: String,
    kv_version: u8,
}

#[derive(Debug, Clone)]
enum VaultAuthConfig {
    Token(String),
    AppRole {
        role_id: String,
        secret_id: String,
        auth_path: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum RenderMode {
    Text,
    Json,
}

#[derive(Debug, Clone)]
enum VaultCommand {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },
    List { prefix: Option<String> },
}

impl VaultCommand {
    fn is_mutating(&self) -> bool {
        matches!(self, Self::Put { .. } | Self::Delete { .. })
    }
}

#[derive(Debug, Clone)]
enum VaultOpResult {
    Get {
        key: String,
        value: String,
    },
    Put {
        key: String,
    },
    Delete {
        key: String,
        deleted: bool,
    },
    List {
        prefix: Option<String>,
        keys: Vec<String>,
    },
}

#[async_trait]
impl Provider for VaultProvider {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let command_tokens = parse_command_tokens(stage, vars, outputs)?;
        let mode = parse_render_mode(stage, vars, outputs)?;
        let config = vault_config::read_config(stage, workflow, vars, outputs)?;
        let command = parse_vault_command(&command_tokens, stage, vars, outputs)?;

        match &command {
            VaultCommand::Get { key }
            | VaultCommand::Put { key, .. }
            | VaultCommand::Delete { key } => {
                ensure_key_allowed(&config, stage, key)?;
            }
            VaultCommand::List { prefix } => {
                if let Some(prefix) = prefix.as_ref()
                    && !prefix.trim().is_empty()
                    && !key_allowed(&config, prefix)
                {
                    return Err(ProviderError::new(
                        "provider_exec_failed",
                        format!(
                            "vault prefix '{}' is blocked by allowlist in stage '{}'",
                            prefix, stage.id
                        ),
                    ));
                }
            }
        }

        if config.read_only && command.is_mutating() {
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!("vault provider is read-only in stage '{}'", stage.id),
            ));
        }

        let result = match config.backend {
            VaultBackend::File => execute_file_command(&config, command).await?,
            VaultBackend::Http => {
                let http_config = config.http.as_ref().ok_or_else(|| {
                    ProviderError::new(
                        "provider_start_failed",
                        format!(
                            "vault http backend missing configuration in stage '{}'",
                            stage.id
                        ),
                    )
                })?;
                vault_http::execute_http_command(http_config, command, timeout, stage).await?
            }
        };

        render_result(result, mode).map_err(|err| {
            ProviderError::new(
                "provider_invalid_response",
                format!(
                    "failed rendering vault output in stage '{}': {}",
                    stage.id, err
                ),
            )
        })
    }
}

fn store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn parse_command_tokens(
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<Vec<String>> {
    if !stage.args.is_empty() {
        return Ok(stage
            .args
            .iter()
            .map(|arg| subst(arg, vars, outputs))
            .collect());
    }

    let exec = stage.exec.as_deref().ok_or_else(|| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "stage '{}' requires 'args' or 'exec' for provider=vault",
                stage.id
            ),
        )
    })?;
    let rendered_exec = subst(exec, vars, outputs);
    let tokens = shell_words::split(rendered_exec.trim()).map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!("failed parsing vault exec in stage '{}': {}", stage.id, err),
        )
    })?;

    if tokens.is_empty() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!("vault command is empty in stage '{}'", stage.id),
        ));
    }
    Ok(tokens)
}

fn parse_render_mode(
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<RenderMode> {
    let rendered = stage
        .parse
        .as_deref()
        .map(|v| subst(v, vars, outputs))
        .unwrap_or_else(|| "text".to_string());
    match rendered.as_str() {
        "text" => Ok(RenderMode::Text),
        "json" => Ok(RenderMode::Json),
        other => Err(ProviderError::new(
            "provider_invalid_response",
            format!(
                "unsupported parse mode '{}' in stage '{}', expected text|json",
                other, stage.id
            ),
        )),
    }
}

fn parse_vault_command(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultCommand> {
    if tokens.is_empty() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!("vault command is empty in stage '{}'", stage.id),
        ));
    }
    let op = tokens[0].to_ascii_lowercase();
    match op.as_str() {
        "get" => Ok(VaultCommand::Get {
            key: required_key(tokens, stage)?,
        }),
        "put" | "set" => parse_put_command(tokens, stage, vars, outputs),
        "delete" | "del" | "rm" => Ok(VaultCommand::Delete {
            key: required_key(tokens, stage)?,
        }),
        "list" => Ok(VaultCommand::List {
            prefix: parse_list_prefix(tokens),
        }),
        other => Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "unsupported vault operation '{}' in stage '{}'; expected get|put|delete|list",
                other, stage.id
            ),
        )),
    }
}

fn parse_put_command(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultCommand> {
    let key = required_key(tokens, stage)?;
    let value = parse_value_from_tokens_or_stdin(tokens, stage, vars, outputs)?.ok_or_else(|| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault put requires value in args or stdin for stage '{}'",
                stage.id
            ),
        )
    })?;
    Ok(VaultCommand::Put { key, value })
}

fn parse_list_prefix(tokens: &[String]) -> Option<String> {
    tokens
        .get(1)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn required_key(tokens: &[String], stage: &Stage) -> ProviderResult<String> {
    tokens
        .get(1)
        .cloned()
        .map(|v| normalize_key(&v))
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault operation requires key in stage '{}'", stage.id),
            )
        })
}

fn parse_value_from_tokens_or_stdin(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<Option<String>> {
    if tokens.len() >= 3 {
        return Ok(Some(tokens[2..].join(" ")));
    }
    Ok(stage.stdin.as_deref().map(|v| subst(v, vars, outputs)))
}

fn normalize_key(raw: &str) -> String {
    raw.trim().trim_matches('/').to_string()
}

fn key_allowed(config: &VaultConfig, key: &str) -> bool {
    match config.allow_prefixes.as_ref() {
        None => true,
        Some(prefixes) => prefixes.iter().any(|prefix| key.starts_with(prefix)),
    }
}

fn ensure_key_allowed(config: &VaultConfig, stage: &Stage, key: &str) -> ProviderResult<()> {
    if key_allowed(config, key) {
        return Ok(());
    }
    Err(ProviderError::new(
        "provider_exec_failed",
        format!(
            "vault key '{}' is blocked by allowlist in stage '{}'",
            key, stage.id
        ),
    ))
}

async fn execute_file_command(
    config: &VaultConfig,
    command: VaultCommand,
) -> ProviderResult<VaultOpResult> {
    let guard = store_lock().lock().await;
    let mut store = vault_store::load_store(&config.kv_file).await?;
    let file_result = match command {
        VaultCommand::Get { key } => {
            let secret_value = store.get(&key).cloned().ok_or_else(|| {
                ProviderError::new(
                    "provider_secret_not_found",
                    format!("vault key '{}' not found", key),
                )
            })?;
            Ok(VaultOpResult::Get {
                key,
                value: secret_value,
            })
        }
        VaultCommand::Put { key, value } => {
            store.insert(key.clone(), value);
            vault_store::save_store(&config.kv_file, &store).await?;
            Ok(VaultOpResult::Put { key })
        }
        VaultCommand::Delete { key } => {
            let deleted = store.remove(&key).is_some();
            vault_store::save_store(&config.kv_file, &store).await?;
            Ok(VaultOpResult::Delete { key, deleted })
        }
        VaultCommand::List { prefix } => {
            let mut keys = store
                .keys()
                .filter(|key| {
                    if let Some(prefix) = prefix.as_ref()
                        && !key.starts_with(prefix)
                    {
                        return false;
                    }
                    key_allowed(config, key)
                })
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            Ok(VaultOpResult::List { prefix, keys })
        }
    };
    drop(guard);
    file_result
}

fn render_result(result: VaultOpResult, mode: RenderMode) -> Result<String, serde_json::Error> {
    match mode {
        RenderMode::Text => Ok(match result {
            VaultOpResult::Get { value, .. } => value,
            VaultOpResult::Put { .. } => "ok".to_string(),
            VaultOpResult::Delete { .. } => "ok".to_string(),
            VaultOpResult::List { keys, .. } => keys.join("\n"),
        }),
        RenderMode::Json => {
            let rendered_payload = match result {
                VaultOpResult::Get { key, value } => {
                    json!({"op":"get","key":key,"value":value})
                }
                VaultOpResult::Put { key } => {
                    json!({"op":"put","key":key,"status":"ok"})
                }
                VaultOpResult::Delete { key, deleted } => {
                    json!({"op":"delete","key":key,"deleted":deleted})
                }
                VaultOpResult::List { prefix, keys } => {
                    json!({"op":"list","prefix":prefix,"keys":keys})
                }
            };
            serde_json::to_string(&rendered_payload)
        }
    }
}


#[cfg(test)]
#[path = "vault_tests.rs"]
mod tests;
