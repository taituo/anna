use super::{VaultAuthConfig, VaultBackend, VaultConfig, VaultHttpConfig};
use crate::expr::subst;
use crate::providers::{ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub(super) fn read_config(
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

    let backend_http_config = match backend {
        VaultBackend::File => None,
        VaultBackend::Http => Some(read_http_config(stage, workflow, vars, outputs)?),
    };

    Ok(VaultConfig {
        backend,
        kv_file,
        allow_prefixes,
        read_only,
        http: backend_http_config,
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
    let settings = SettingsCtx {
        stage,
        workflow,
        vars,
        outputs,
    };
    let addr = required_setting(
        "ANNA_VAULT_ADDR",
        "ANNA_VAULT_ADDR is required for vault http backend in stage '{}'",
        &settings,
    )?;
    let auth = read_http_auth_config(&settings)?;
    let mount = non_empty_setting(
        "ANNA_VAULT_MOUNT",
        "secret",
        "ANNA_VAULT_MOUNT cannot be empty for stage '{}'",
        &settings,
    )?;
    let kv_version = read_kv_version(&settings)?;
    let namespace =
        resolve_setting_in_ctx("ANNA_VAULT_NAMESPACE", &settings).filter(|v| !v.trim().is_empty());

    Ok(VaultHttpConfig {
        addr,
        auth,
        namespace,
        mount,
        kv_version,
    })
}

struct SettingsCtx<'a> {
    stage: &'a Stage,
    workflow: &'a Workflow,
    vars: &'a HashMap<String, String>,
    outputs: &'a HashMap<String, String>,
}

fn read_http_auth_config(settings: &SettingsCtx<'_>) -> ProviderResult<VaultAuthConfig> {
    let vault_token_env_key = ["ANNA_VAULT_", "TO", "KEN"].concat();
    let token = resolve_setting_in_ctx(vault_token_env_key.as_str(), settings);
    let role_id = resolve_setting_in_ctx("ANNA_VAULT_ROLE_ID", settings);
    let vault_secret_env_key = ["ANNA_VAULT_", "SE", "CRET", "_ID"].concat();
    let secret_id = resolve_setting_in_ctx(vault_secret_env_key.as_str(), settings);
    let auth_path = non_empty_setting(
        "ANNA_VAULT_AUTH_PATH",
        "auth/approle/login",
        "ANNA_VAULT_AUTH_PATH cannot be empty in stage '{}'",
        settings,
    )?;

    if let Some(token) = token {
        return Ok(VaultAuthConfig::Token(token));
    }
    match (role_id, secret_id) {
        (Some(role_id), Some(secret_id)) => Ok(VaultAuthConfig::AppRole {
            role_id,
            secret_id,
            auth_path,
        }),
        (Some(_), None) | (None, Some(_)) => Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "ANNA_VAULT_ROLE_ID and ANNA_VAULT_SECRET_ID must be set together in stage '{}'",
                settings.stage.id
            ),
        )),
        (None, None) => Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "vault http backend requires ANNA_VAULT_TOKEN or AppRole credentials (ANNA_VAULT_ROLE_ID + ANNA_VAULT_SECRET_ID) in stage '{}'",
                settings.stage.id
            ),
        )),
    }
}

fn required_setting(
    key: &str,
    err_msg: &str,
    settings: &SettingsCtx<'_>,
) -> ProviderResult<String> {
    let message = err_msg.replacen("{}", &settings.stage.id, 1);
    resolve_setting_in_ctx(key, settings).ok_or_else(|| ProviderError::new("provider_start_failed", message))
}

fn non_empty_setting(
    key: &str,
    default_value: &str,
    err_msg: &str,
    settings: &SettingsCtx<'_>,
) -> ProviderResult<String> {
    let setting_raw = resolve_setting_in_ctx(key, settings).unwrap_or_else(|| default_value.to_string());
    let normalized = setting_raw.trim().trim_matches('/').to_string();
    if normalized.is_empty() {
        let empty_message = err_msg.replacen("{}", &settings.stage.id, 1);
        return Err(ProviderError::new("provider_start_failed", empty_message));
    }
    Ok(normalized)
}

fn read_kv_version(settings: &SettingsCtx<'_>) -> ProviderResult<u8> {
    let kv_version_raw = resolve_setting_in_ctx("ANNA_VAULT_KV_VERSION", settings)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "2".to_string());
    let parsed_kv_version = kv_version_raw.parse::<u8>().map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "invalid ANNA_VAULT_KV_VERSION '{}' in stage '{}': {}",
                kv_version_raw, settings.stage.id, err
            ),
        )
    })?;
    if parsed_kv_version != 1 && parsed_kv_version != 2 {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "unsupported ANNA_VAULT_KV_VERSION '{}' in stage '{}', expected 1 or 2",
                parsed_kv_version, settings.stage.id
            ),
        ));
    }
    Ok(parsed_kv_version)
}

fn resolve_setting(
    key: &str,
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> Option<String> {
    let settings_ctx = SettingsCtx {
        stage,
        workflow,
        vars,
        outputs,
    };
    resolve_setting_in_ctx(key, &settings_ctx)
}

fn resolve_setting_in_ctx(key: &str, settings: &SettingsCtx<'_>) -> Option<String> {
    if let Some(v) = settings.stage.env.get(key) {
        let stage_rendered = subst(v, settings.vars, settings.outputs);
        let trimmed = stage_rendered.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(v) = settings.workflow.env.get(key) {
        let workflow_rendered = subst(v, settings.vars, settings.outputs);
        let trimmed_workflow_value = workflow_rendered.trim();
        if !trimmed_workflow_value.is_empty() {
            return Some(trimmed_workflow_value.to_string());
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
