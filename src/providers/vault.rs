use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Default, Clone)]
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
    token: String,
    namespace: Option<String>,
    mount: String,
    kv_version: u8,
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
        let tokens = parse_command_tokens(stage, vars, outputs)?;
        let mode = parse_render_mode(stage, vars, outputs)?;
        let config = read_config(stage, workflow, vars, outputs)?;
        let command = parse_vault_command(&tokens, stage, vars, outputs)?;

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
                let http = config.http.as_ref().ok_or_else(|| {
                    ProviderError::new(
                        "provider_start_failed",
                        format!(
                            "vault http backend missing configuration in stage '{}'",
                            stage.id
                        ),
                    )
                })?;
                execute_http_command(http, command, timeout, stage).await?
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
    let rendered = subst(exec, vars, outputs);
    let tokens = shell_words::split(rendered.trim()).map_err(|err| {
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
        "get" => {
            let key = required_key(tokens, stage)?;
            Ok(VaultCommand::Get { key })
        }
        "put" | "set" => {
            let key = required_key(tokens, stage)?;
            let value = parse_value_from_tokens_or_stdin(tokens, stage, vars, outputs)?
                .ok_or_else(|| {
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
        "delete" | "del" | "rm" => {
            let key = required_key(tokens, stage)?;
            Ok(VaultCommand::Delete { key })
        }
        "list" => {
            let prefix = tokens
                .get(1)
                .cloned()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            Ok(VaultCommand::List { prefix })
        }
        other => Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "unsupported vault operation '{}' in stage '{}'; expected get|put|delete|list",
                other, stage.id
            ),
        )),
    }
}

fn read_config(
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultConfig> {
    let backend = parse_backend(
        resolve_setting("ANNA_VAULT_BACKEND", stage, workflow, vars, outputs)
            .as_deref()
            .unwrap_or("file"),
        stage,
    )?;

    let kv_file = resolve_setting("ANNA_VAULT_KV_FILE", stage, workflow, vars, outputs)
        .map(PathBuf::from)
        .unwrap_or_else(default_vault_kv_file);

    let allow_prefixes = resolve_setting("ANNA_VAULT_PREFIX_ALLOW", stage, workflow, vars, outputs)
        .map(|raw| {
            raw.split(',')
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let read_only = resolve_setting("ANNA_VAULT_READ_ONLY", stage, workflow, vars, outputs)
        .map(|v| is_truthy(&v))
        .unwrap_or(false);

    let http = match backend {
        VaultBackend::File => None,
        VaultBackend::Http => Some(read_http_config(stage, workflow, vars, outputs)?),
    };

    Ok(VaultConfig {
        backend,
        kv_file,
        allow_prefixes,
        read_only,
        http,
    })
}

fn parse_backend(raw: &str, stage: &Stage) -> ProviderResult<VaultBackend> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "file" => Ok(VaultBackend::File),
        "http" | "vault" | "openbao" => Ok(VaultBackend::Http),
        other => Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "unsupported ANNA_VAULT_BACKEND '{}' in stage '{}', expected file|http|vault|openbao",
                other, stage.id
            ),
        )),
    }
}

fn read_http_config(
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultHttpConfig> {
    let addr =
        resolve_setting("ANNA_VAULT_ADDR", stage, workflow, vars, outputs).ok_or_else(|| {
            ProviderError::new(
                "provider_start_failed",
                format!(
                    "ANNA_VAULT_ADDR is required for vault http backend in stage '{}'",
                    stage.id
                ),
            )
        })?;

    let token =
        resolve_setting("ANNA_VAULT_TOKEN", stage, workflow, vars, outputs).ok_or_else(|| {
            ProviderError::new(
                "provider_start_failed",
                format!(
                    "ANNA_VAULT_TOKEN is required for vault http backend in stage '{}'",
                    stage.id
                ),
            )
        })?;

    let mount = resolve_setting("ANNA_VAULT_MOUNT", stage, workflow, vars, outputs)
        .unwrap_or_else(|| "secret".to_string());
    let mount = mount.trim().trim_matches('/').to_string();
    if mount.is_empty() {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!("ANNA_VAULT_MOUNT cannot be empty for stage '{}'", stage.id),
        ));
    }

    let kv_version = resolve_setting("ANNA_VAULT_KV_VERSION", stage, workflow, vars, outputs)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "2".to_string());
    let kv_version = kv_version.parse::<u8>().map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "invalid ANNA_VAULT_KV_VERSION '{}' in stage '{}': {}",
                kv_version, stage.id, err
            ),
        )
    })?;
    if kv_version != 1 && kv_version != 2 {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "unsupported ANNA_VAULT_KV_VERSION '{}' in stage '{}', expected 1 or 2",
                kv_version, stage.id
            ),
        ));
    }

    let namespace = resolve_setting("ANNA_VAULT_NAMESPACE", stage, workflow, vars, outputs)
        .filter(|v| !v.trim().is_empty());

    Ok(VaultHttpConfig {
        addr,
        token,
        namespace,
        mount,
        kv_version,
    })
}

fn resolve_setting(
    key: &str,
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> Option<String> {
    if let Some(v) = stage.env.get(key) {
        let rendered = subst(v, vars, outputs);
        let trimmed = rendered.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(v) = workflow.env.get(key) {
        let rendered = subst(v, vars, outputs);
        let trimmed = rendered.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn default_vault_kv_file() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME")
        && !home.is_empty()
    {
        return Path::new(&home).join(".anna/vault-kv.json");
    }
    PathBuf::from("/tmp/anna-vault-kv.json")
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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
    let _guard = store_lock().lock().await;
    let mut store = load_store(&config.kv_file).await?;
    match command {
        VaultCommand::Get { key } => {
            let value = store.get(&key).cloned().ok_or_else(|| {
                ProviderError::new(
                    "provider_secret_not_found",
                    format!("vault key '{}' not found", key),
                )
            })?;
            Ok(VaultOpResult::Get { key, value })
        }
        VaultCommand::Put { key, value } => {
            store.insert(key.clone(), value);
            save_store(&config.kv_file, &store).await?;
            Ok(VaultOpResult::Put { key })
        }
        VaultCommand::Delete { key } => {
            let deleted = store.remove(&key).is_some();
            save_store(&config.kv_file, &store).await?;
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
    }
}

async fn execute_http_command(
    config: &VaultHttpConfig,
    command: VaultCommand,
    timeout: Option<Duration>,
    stage: &Stage,
) -> ProviderResult<VaultOpResult> {
    let client = reqwest::Client::builder()
        .timeout(timeout.unwrap_or(Duration::from_secs(60)))
        .build()
        .map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!(
                    "failed creating vault http client in stage '{}': {}",
                    stage.id, err
                ),
            )
        })?;

    match command {
        VaultCommand::Get { key } => http_get(&client, config, stage, key).await,
        VaultCommand::Put { key, value } => http_put(&client, config, stage, key, value).await,
        VaultCommand::Delete { key } => http_delete(&client, config, stage, key).await,
        VaultCommand::List { prefix } => http_list(&client, config, stage, prefix).await,
    }
}

async fn http_get(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
    key: String,
) -> ProviderResult<VaultOpResult> {
    let url = build_get_url(config, &key);
    let response = with_vault_headers(client.get(&url), config)
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault get request failed in stage '{}': {}", stage.id, err),
            )
        })?;

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault get response in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    if status == StatusCode::NOT_FOUND {
        return Err(ProviderError::new(
            "provider_secret_not_found",
            format!("vault key '{}' not found in stage '{}'", key, stage.id),
        ));
    }
    if !status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault get failed in stage '{}' with status {}: {}",
                stage.id, status, body
            ),
        ));
    }

    let payload: Value = serde_json::from_str(&body).map_err(|err| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault get returned invalid json in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    let value = extract_value_from_get(&payload, config.kv_version).ok_or_else(|| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault get response missing value for key '{}' in stage '{}'",
                key, stage.id
            ),
        )
    })?;

    Ok(VaultOpResult::Get { key, value })
}

async fn http_put(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
    key: String,
    value: String,
) -> ProviderResult<VaultOpResult> {
    let url = build_put_url(config, &key);
    let body = if config.kv_version == 2 {
        json!({"data": {"value": value}})
    } else {
        json!({"value": value})
    };

    let response = with_vault_headers(client.post(&url), config)
        .json(&body)
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault put request failed in stage '{}': {}", stage.id, err),
            )
        })?;

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault put response in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    if !status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault put failed in stage '{}' with status {}: {}",
                stage.id, status, body
            ),
        ));
    }

    Ok(VaultOpResult::Put { key })
}

async fn http_delete(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
    key: String,
) -> ProviderResult<VaultOpResult> {
    let url = build_delete_url(config, &key);
    let response = with_vault_headers(client.delete(&url), config)
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "vault delete request failed in stage '{}': {}",
                    stage.id, err
                ),
            )
        })?;

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault delete response in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    if status == StatusCode::NOT_FOUND {
        return Ok(VaultOpResult::Delete {
            key,
            deleted: false,
        });
    }
    if !status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault delete failed in stage '{}' with status {}: {}",
                stage.id, status, body
            ),
        ));
    }

    Ok(VaultOpResult::Delete { key, deleted: true })
}

async fn http_list(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
    prefix: Option<String>,
) -> ProviderResult<VaultOpResult> {
    let url = build_list_url(config, prefix.as_deref());
    let list_method = Method::from_bytes(b"LIST").map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "failed creating LIST method in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    let mut response = with_vault_headers(client.request(list_method, &url), config)
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault list request failed in stage '{}': {}", stage.id, err),
            )
        })?;

    if matches!(
        response.status(),
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::BAD_REQUEST
    ) {
        response = with_vault_headers(client.get(&url).query(&[("list", "true")]), config)
            .send()
            .await
            .map_err(|err| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "vault list fallback request failed in stage '{}': {}",
                        stage.id, err
                    ),
                )
            })?;
    }

    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault list response in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    if status == StatusCode::NOT_FOUND {
        return Ok(VaultOpResult::List {
            prefix,
            keys: Vec::new(),
        });
    }
    if !status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault list failed in stage '{}' with status {}: {}",
                stage.id, status, body
            ),
        ));
    }

    let payload: Value = serde_json::from_str(&body).map_err(|err| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault list returned invalid json in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;

    let mut keys = payload
        .pointer("/data/keys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    keys.sort();

    Ok(VaultOpResult::List { prefix, keys })
}

fn extract_value_from_get(payload: &Value, kv_version: u8) -> Option<String> {
    let candidate = if kv_version == 2 {
        payload
            .pointer("/data/data/value")
            .or_else(|| payload.pointer("/data/value"))
    } else {
        payload
            .pointer("/data/value")
            .or_else(|| payload.pointer("/data"))
    }?;

    match candidate {
        Value::String(v) => Some(v.clone()),
        Value::Number(_) | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Some(candidate.to_string())
        }
        Value::Null => None,
    }
}

fn with_vault_headers(
    builder: reqwest::RequestBuilder,
    config: &VaultHttpConfig,
) -> reqwest::RequestBuilder {
    let mut builder = builder.header("X-Vault-Token", &config.token);
    if let Some(namespace) = config.namespace.as_ref().filter(|v| !v.trim().is_empty()) {
        builder = builder.header("X-Vault-Namespace", namespace);
    }
    builder
}

fn build_get_url(config: &VaultHttpConfig, key: &str) -> String {
    if config.kv_version == 2 {
        join_addr(
            &config.addr,
            &format!("v1/{}/data/{}", config.mount, normalize_key(key)),
        )
    } else {
        join_addr(
            &config.addr,
            &format!("v1/{}/{}", config.mount, normalize_key(key)),
        )
    }
}

fn build_put_url(config: &VaultHttpConfig, key: &str) -> String {
    build_get_url(config, key)
}

fn build_delete_url(config: &VaultHttpConfig, key: &str) -> String {
    build_get_url(config, key)
}

fn build_list_url(config: &VaultHttpConfig, prefix: Option<&str>) -> String {
    let prefix = prefix.map(normalize_key).unwrap_or_default();
    if config.kv_version == 2 {
        if prefix.is_empty() {
            join_addr(&config.addr, &format!("v1/{}/metadata", config.mount))
        } else {
            join_addr(
                &config.addr,
                &format!("v1/{}/metadata/{}", config.mount, prefix),
            )
        }
    } else if prefix.is_empty() {
        join_addr(&config.addr, &format!("v1/{}", config.mount))
    } else {
        join_addr(&config.addr, &format!("v1/{}/{}", config.mount, prefix))
    }
}

fn join_addr(addr: &str, path: &str) -> String {
    format!(
        "{}/{}",
        addr.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

async fn load_store(path: &Path) -> ProviderResult<HashMap<String, String>> {
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => serde_json::from_str::<HashMap<String, String>>(&raw).map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!("invalid vault store json '{}': {}", path.display(), err),
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(ProviderError::new(
            "provider_start_failed",
            format!("failed reading vault store '{}': {}", path.display(), err),
        )),
    }
}

async fn save_store(path: &Path, store: &HashMap<String, String>) -> ProviderResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!(
                    "failed creating vault store parent '{}': {}",
                    parent.display(),
                    err
                ),
            )
        })?;
    }

    let raw = serde_json::to_string_pretty(store).map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "failed serializing vault store '{}': {}",
                path.display(),
                err
            ),
        )
    })?;

    tokio::fs::write(path, raw).await.map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!("failed writing vault store '{}': {}", path.display(), err),
        )
    })
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
            let payload = match result {
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
            serde_json::to_string(&payload)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VaultProvider;
    use crate::providers::Provider;
    use crate::workflow::{Stage, Workflow};
    use axum::extract::{Path as AxumPath, State};
    use axum::http::{HeaderMap, Method, StatusCode};
    use axum::{Json, Router, routing::any, routing::get};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn make_workflow_with_env(env: HashMap<String, String>) -> Workflow {
        Workflow {
            name: "vault-provider-test".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env,
            workdir: None,
            trigger: Default::default(),
            stages: vec![],
            source_path: None,
        }
    }

    fn stage_with_args(id: &str, args: Vec<&str>) -> Stage {
        Stage {
            id: id.to_string(),
            provider: "vault".to_string(),
            args: args.into_iter().map(|v| v.to_string()).collect(),
            ..Default::default()
        }
    }

    fn temp_vault_file() -> PathBuf {
        std::env::temp_dir().join(format!(
            "anna-vault-provider-{}-{}.json",
            std::process::id(),
            rand::random::<u32>()
        ))
    }

    #[tokio::test]
    async fn put_get_list_delete_roundtrip_text() {
        let file = temp_vault_file();
        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
            ("ANNA_VAULT_KV_FILE".to_string(), file.display().to_string()),
        ]));

        let out = VaultProvider
            .run(
                &stage_with_args("put", vec!["put", "kv/dev/token", "abc123"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("put should succeed");
        assert_eq!(out, "ok");

        let out = VaultProvider
            .run(
                &stage_with_args("get", vec!["get", "kv/dev/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("get should succeed");
        assert_eq!(out, "abc123");

        let out = VaultProvider
            .run(
                &stage_with_args("list", vec!["list", "kv/dev"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("list should succeed");
        assert_eq!(out, "kv/dev/token");

        let out = VaultProvider
            .run(
                &stage_with_args("delete", vec!["delete", "kv/dev/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("delete should succeed");
        assert_eq!(out, "ok");

        let err = VaultProvider
            .run(
                &stage_with_args("get-missing", vec!["get", "kv/dev/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect_err("missing key should fail");
        assert_eq!(err.code, "provider_secret_not_found");

        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn json_mode_outputs_structured_payload() {
        let file = temp_vault_file();
        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
            ("ANNA_VAULT_KV_FILE".to_string(), file.display().to_string()),
        ]));

        let mut put = stage_with_args("put", vec!["put", "kv/prod/key", "secret"]);
        put.parse = Some("json".to_string());
        let out = VaultProvider
            .run(&put, &workflow, &HashMap::new(), &HashMap::new(), None)
            .await
            .expect("put json should succeed");
        let parsed: Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(parsed.get("op").and_then(|v| v.as_str()), Some("put"));

        let mut list = stage_with_args("list", vec!["list", "kv/prod"]);
        list.parse = Some("json".to_string());
        let out = VaultProvider
            .run(&list, &workflow, &HashMap::new(), &HashMap::new(), None)
            .await
            .expect("list json should succeed");
        let parsed: Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(parsed.get("op").and_then(|v| v.as_str()), Some("list"));
        let keys = parsed
            .get("keys")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(keys.len(), 1);

        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn allowlist_blocks_disallowed_keys() {
        let file = temp_vault_file();
        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
            ("ANNA_VAULT_KV_FILE".to_string(), file.display().to_string()),
            (
                "ANNA_VAULT_PREFIX_ALLOW".to_string(),
                "kv/prod/".to_string(),
            ),
        ]));

        let err = VaultProvider
            .run(
                &stage_with_args("put", vec!["put", "kv/dev/key", "x"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect_err("disallowed key should fail");
        assert_eq!(err.code, "provider_exec_failed");
        assert!(err.message.contains("blocked by allowlist"));

        let out = VaultProvider
            .run(
                &stage_with_args("put-ok", vec!["put", "kv/prod/key", "x"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("allowed key should work");
        assert_eq!(out, "ok");

        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn read_only_blocks_mutation_ops() {
        let file = temp_vault_file();
        tokio::fs::write(&file, "{\"kv/prod/token\":\"abc\"}")
            .await
            .expect("seed vault file");

        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
            ("ANNA_VAULT_KV_FILE".to_string(), file.display().to_string()),
            ("ANNA_VAULT_READ_ONLY".to_string(), "true".to_string()),
        ]));

        let out = VaultProvider
            .run(
                &stage_with_args("get", vec!["get", "kv/prod/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("get should succeed in read-only");
        assert_eq!(out, "abc");

        let err = VaultProvider
            .run(
                &stage_with_args("put", vec!["put", "kv/prod/token", "new"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect_err("put should be blocked in read-only");
        assert_eq!(err.code, "provider_exec_failed");
        assert!(err.message.contains("read-only"));

        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn http_backend_roundtrip_kv_v2() {
        let addr = spawn_mock_vault_server().await;
        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
            ("ANNA_VAULT_ADDR".to_string(), addr),
            ("ANNA_VAULT_TOKEN".to_string(), "test-token".to_string()),
            ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
            ("ANNA_VAULT_KV_VERSION".to_string(), "2".to_string()),
        ]));

        let out = VaultProvider
            .run(
                &stage_with_args("put", vec!["put", "kv/prod/token", "hello"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("http put should succeed");
        assert_eq!(out, "ok");

        let out = VaultProvider
            .run(
                &stage_with_args("get", vec!["get", "kv/prod/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("http get should succeed");
        assert_eq!(out, "hello");

        let out = VaultProvider
            .run(
                &stage_with_args("list", vec!["list", "kv/prod"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("http list should succeed");
        assert_eq!(out, "kv/prod/token");

        let out = VaultProvider
            .run(
                &stage_with_args("delete", vec!["delete", "kv/prod/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect("http delete should succeed");
        assert_eq!(out, "ok");
    }

    #[tokio::test]
    async fn http_backend_requires_addr_and_token() {
        let workflow = make_workflow_with_env(HashMap::from([
            ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
            ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
        ]));
        let err = VaultProvider
            .run(
                &stage_with_args("get", vec!["get", "kv/prod/token"]),
                &workflow,
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
            .expect_err("http backend should require config");
        assert_eq!(err.code, "provider_start_failed");
        assert!(err.message.contains("ANNA_VAULT_ADDR"));
    }

    #[derive(Default)]
    struct MockVaultState {
        store: Mutex<HashMap<String, String>>,
    }

    async fn spawn_mock_vault_server() -> String {
        let state = Arc::new(MockVaultState::default());
        let app = Router::new()
            .route(
                "/v1/secret/data/{*path}",
                get(mock_get).post(mock_put).delete(mock_delete),
            )
            .route("/v1/secret/metadata", any(mock_list_root))
            .route("/v1/secret/metadata/{*path}", any(mock_list))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock vault listener");
        let addr = listener
            .local_addr()
            .expect("read mock vault listener addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{}", addr)
    }

    fn authorize(headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
        let token = headers
            .get("X-Vault-Token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if token != "test-token" {
            return Err((StatusCode::FORBIDDEN, "forbidden".to_string()));
        }
        Ok(())
    }

    async fn mock_get(
        State(state): State<Arc<MockVaultState>>,
        headers: HeaderMap,
        AxumPath(path): AxumPath<String>,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        authorize(&headers)?;
        let key = path.trim_matches('/').to_string();
        let store = state.store.lock().await;
        let Some(value) = store.get(&key) else {
            return Err((StatusCode::NOT_FOUND, "not found".to_string()));
        };
        Ok(Json(json!({"data": {"data": {"value": value}}})))
    }

    async fn mock_put(
        State(state): State<Arc<MockVaultState>>,
        headers: HeaderMap,
        AxumPath(path): AxumPath<String>,
        Json(body): Json<Value>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        authorize(&headers)?;
        let key = path.trim_matches('/').to_string();
        let value = body
            .pointer("/data/value")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let mut store = state.store.lock().await;
        store.insert(key, value);
        Ok(StatusCode::NO_CONTENT)
    }

    async fn mock_delete(
        State(state): State<Arc<MockVaultState>>,
        headers: HeaderMap,
        AxumPath(path): AxumPath<String>,
    ) -> Result<StatusCode, (StatusCode, String)> {
        authorize(&headers)?;
        let key = path.trim_matches('/').to_string();
        let mut store = state.store.lock().await;
        if store.remove(&key).is_some() {
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err((StatusCode::NOT_FOUND, "not found".to_string()))
        }
    }

    async fn mock_list_root(
        method: Method,
        State(state): State<Arc<MockVaultState>>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        mock_list_impl(method, state, headers, "").await
    }

    async fn mock_list(
        method: Method,
        State(state): State<Arc<MockVaultState>>,
        headers: HeaderMap,
        AxumPath(path): AxumPath<String>,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        mock_list_impl(method, state, headers, &path).await
    }

    async fn mock_list_impl(
        method: Method,
        state: Arc<MockVaultState>,
        headers: HeaderMap,
        prefix: &str,
    ) -> Result<Json<Value>, (StatusCode, String)> {
        authorize(&headers)?;
        if method.as_str() != "LIST" && method != Method::GET {
            return Err((
                StatusCode::METHOD_NOT_ALLOWED,
                "method not allowed".to_string(),
            ));
        }

        let prefix = prefix.trim_matches('/').to_string();
        let store = state.store.lock().await;
        let mut keys = store
            .keys()
            .filter(|key| prefix.is_empty() || key.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(Json(json!({"data": {"keys": keys}})))
    }
}
