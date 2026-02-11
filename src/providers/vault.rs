use crate::expr::subst;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Default, Clone)]
pub struct VaultProvider;

#[derive(Debug, Clone)]
struct VaultConfig {
    kv_file: PathBuf,
    allow_prefixes: Option<Vec<String>>,
    read_only: bool,
}

#[derive(Debug, Clone, Copy)]
enum RenderMode {
    Text,
    Json,
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
        _timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let tokens = parse_command_tokens(stage, vars, outputs)?;
        let mode = parse_render_mode(stage, vars, outputs)?;
        let config = read_config(stage, workflow, vars, outputs);

        let _guard = store_lock().lock().await;
        let mut store = load_store(&config.kv_file).await?;

        let op = tokens[0].to_ascii_lowercase();
        let result = match op.as_str() {
            "get" => {
                let key = required_key(&tokens, stage)?;
                ensure_key_allowed(&config, stage, &key)?;
                let value = store.get(&key).cloned().ok_or_else(|| {
                    ProviderError::new(
                        "provider_secret_not_found",
                        format!("vault key '{}' not found in stage '{}'", key, stage.id),
                    )
                })?;
                VaultOpResult::Get { key, value }
            }
            "put" | "set" => {
                if config.read_only {
                    return Err(ProviderError::new(
                        "provider_exec_failed",
                        format!("vault provider is read-only in stage '{}'", stage.id),
                    ));
                }
                let key = required_key(&tokens, stage)?;
                ensure_key_allowed(&config, stage, &key)?;
                let value = match parse_value_from_tokens_or_stdin(&tokens, stage, vars, outputs)? {
                    Some(v) => v,
                    None => {
                        return Err(ProviderError::new(
                            "provider_exec_failed",
                            format!(
                                "vault put requires value in args or stdin for stage '{}'",
                                stage.id
                            ),
                        ));
                    }
                };
                store.insert(key.clone(), value);
                save_store(&config.kv_file, &store).await?;
                VaultOpResult::Put { key }
            }
            "delete" | "del" | "rm" => {
                if config.read_only {
                    return Err(ProviderError::new(
                        "provider_exec_failed",
                        format!("vault provider is read-only in stage '{}'", stage.id),
                    ));
                }
                let key = required_key(&tokens, stage)?;
                ensure_key_allowed(&config, stage, &key)?;
                let deleted = store.remove(&key).is_some();
                save_store(&config.kv_file, &store).await?;
                VaultOpResult::Delete { key, deleted }
            }
            "list" => {
                let prefix = tokens.get(1).cloned();
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

                let mut keys = store
                    .keys()
                    .filter(|key| {
                        if let Some(prefix) = prefix.as_ref() {
                            if !key.starts_with(prefix) {
                                return false;
                            }
                        }
                        key_allowed(&config, key)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                keys.sort();
                VaultOpResult::List { prefix, keys }
            }
            other => {
                return Err(ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "unsupported vault operation '{}' in stage '{}'; expected get|put|delete|list",
                        other, stage.id
                    ),
                ));
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

fn read_config(
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> VaultConfig {
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

    VaultConfig {
        kv_file,
        allow_prefixes,
        read_only,
    }
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
    use serde_json::Value;
    use std::collections::HashMap;
    use std::path::PathBuf;

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
        let workflow = make_workflow_with_env(HashMap::from([(
            "ANNA_VAULT_KV_FILE".to_string(),
            file.display().to_string(),
        )]));

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
        let workflow = make_workflow_with_env(HashMap::from([(
            "ANNA_VAULT_KV_FILE".to_string(),
            file.display().to_string(),
        )]));

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
}
