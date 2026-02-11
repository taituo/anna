use crate::expr::subst;
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub mod cli;
pub mod http;
pub mod k8s;
pub mod llm;
pub mod shell;
pub mod vault;

pub type ProviderResult<T> = Result<T, ProviderError>;

#[derive(Debug, Clone)]
pub struct ProviderError {
    pub code: &'static str,
    pub message: String,
}

impl ProviderError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl Display for ProviderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ProviderError {}

#[async_trait]
pub trait Provider: Send + Sync {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String>;
}

#[derive(Clone, Default)]
pub struct ProviderRegistry {
    providers: HashMap<String, Arc<dyn Provider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    pub fn register<P>(&mut self, name: &str, provider: P)
    where
        P: Provider + 'static,
    {
        self.providers.insert(name.to_string(), Arc::new(provider));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Provider>> {
        self.providers.get(name).cloned()
    }
}

pub fn runtime_env(
    stage: &Stage,
    workflow: &Workflow,
    outputs: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut env = HashMap::new();
    if let Some(session) = outputs.get("SESSION")
        && !session.trim().is_empty()
    {
        env.insert("ANNA_SESSION".to_string(), session.clone());
    }
    env.insert("ANNA_WORKFLOW".to_string(), workflow.name.clone());
    env.insert("ANNA_STAGE_ID".to_string(), stage.id.clone());
    env.insert(
        "ANNA_TRUST".to_string(),
        stage.trust.clone().unwrap_or_else(|| "none".to_string()),
    );
    env
}

pub fn resolve_stage_secrets(
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<HashMap<String, String>> {
    if stage.secrets.is_empty() {
        return Ok(HashMap::new());
    }

    let file_secrets = load_file_secrets()?;
    let mut resolved = HashMap::new();
    for (raw_left, raw_right) in &stage.secrets {
        let left = subst(raw_left, vars, outputs);
        let right = subst(raw_right, vars, outputs);
        let (env_key, secret_ref) = normalize_secret_mapping(&left, &right);

        if env_key.trim().is_empty() {
            return Err(ProviderError::new(
                "provider_start_failed",
                format!(
                    "stage '{}' has secrets mapping with empty env key for '{}'",
                    stage.id, secret_ref
                ),
            ));
        }
        let value = resolve_secret_ref(&stage.id, &secret_ref, &file_secrets)?;
        resolved.insert(env_key, value);
    }
    Ok(resolved)
}

fn normalize_secret_mapping(left: &str, right: &str) -> (String, String) {
    let left_env = looks_like_env_key(left);
    let right_env = looks_like_env_key(right);
    let left_secret = looks_like_secret_ref(left);
    let right_secret = looks_like_secret_ref(right);

    if left_env && right_secret {
        return (left.to_string(), right.to_string());
    }
    if right_env && left_secret {
        return (right.to_string(), left.to_string());
    }

    if left_secret && !right_secret {
        return (right.to_string(), left.to_string());
    }
    if right_secret && !left_secret {
        return (left.to_string(), right.to_string());
    }

    // Backward-compatible default: secret_ref -> ENV_VAR
    (right.to_string(), left.to_string())
}

fn looks_like_env_key(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn looks_like_secret_ref(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains('/') || trimmed.contains("://")
}

fn resolve_secret_ref(
    stage_id: &str,
    secret_ref: &str,
    file_secrets: &HashMap<String, String>,
) -> ProviderResult<String> {
    let trimmed = secret_ref.trim();
    if trimmed.is_empty() {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!("stage '{}' has empty secret reference", stage_id),
        ));
    }

    if let Some(env_var) = trimmed.strip_prefix("env://") {
        return std::env::var(env_var).map_err(|_| {
            ProviderError::new(
                "provider_secret_not_found",
                format!(
                    "stage '{}' missing environment variable '{}' for secret '{}'",
                    stage_id, env_var, secret_ref
                ),
            )
        });
    }

    if let Some(path) = trimmed.strip_prefix("file://") {
        let content = std::fs::read_to_string(path).map_err(|err| {
            ProviderError::new(
                "provider_secret_not_found",
                format!(
                    "stage '{}' failed reading secret file '{}': {}",
                    stage_id, path, err
                ),
            )
        })?;
        return Ok(content.trim_end_matches(['\n', '\r']).to_string());
    }

    let vault_path = trimmed.strip_prefix("vault://").unwrap_or(trimmed);
    resolve_vault_secret(stage_id, vault_path, file_secrets)
}

fn resolve_vault_secret(
    stage_id: &str,
    secret_path: &str,
    file_secrets: &HashMap<String, String>,
) -> ProviderResult<String> {
    if let Some(v) = std::env::var_os(secret_env_var_name(secret_path)) {
        return Ok(v.to_string_lossy().to_string());
    }
    if let Some(v) = file_secrets.get(secret_path) {
        return Ok(v.clone());
    }

    Err(ProviderError::new(
        "provider_secret_not_found",
        format!(
            "stage '{}' missing secret '{}' (env={} or file={})",
            stage_id,
            secret_path,
            secret_env_var_name(secret_path),
            secrets_file_path().display()
        ),
    ))
}

fn load_file_secrets() -> ProviderResult<HashMap<String, String>> {
    let path = secrets_file_path();
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str::<HashMap<String, String>>(&raw).map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!("invalid secrets json '{}': {}", path.display(), err),
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(ProviderError::new(
            "provider_start_failed",
            format!("failed reading secrets file '{}': {}", path.display(), err),
        )),
    }
}

fn secrets_file_path() -> PathBuf {
    if let Some(p) = std::env::var_os("ANNA_SECRETS_FILE")
        && !p.is_empty()
    {
        return PathBuf::from(p);
    }
    if let Some(home) = std::env::var_os("HOME")
        && !home.is_empty()
    {
        return Path::new(&home).join(".anna/secrets.json");
    }
    PathBuf::from("/tmp/anna-secrets.json")
}

fn secret_env_var_name(secret_ref: &str) -> String {
    format!("ANNA_SECRET_{}", sanitize_secret_key(secret_ref))
}

fn sanitize_secret_key(secret_ref: &str) -> String {
    let mut out = String::with_capacity(secret_ref.len());
    for ch in secret_ref.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

pub fn default_registry() -> ProviderRegistry {
    let mut reg = ProviderRegistry::new();
    reg.register("shell", shell::ShellProvider::default());
    reg.register("cli", cli::CliProvider::default());
    reg.register("http", http::HttpProvider::default());
    reg.register("llm", llm::LlmProvider::default());
    reg.register("k8s", k8s::K8sProvider::default());
    reg.register("vault", vault::VaultProvider::default());
    reg
}

#[cfg(test)]
mod tests {
    use super::{
        looks_like_env_key, looks_like_secret_ref, normalize_secret_mapping, resolve_secret_ref,
        sanitize_secret_key, secret_env_var_name,
    };
    use std::collections::HashMap;

    #[test]
    fn secret_env_name_is_stable() {
        assert_eq!(sanitize_secret_key("kv/prod/api-key"), "KV_PROD_API_KEY");
        assert_eq!(
            secret_env_var_name("kv/prod/api-key"),
            "ANNA_SECRET_KV_PROD_API_KEY"
        );
    }

    #[test]
    fn detects_mapping_direction_for_env_and_secret_ref() {
        assert!(looks_like_env_key("OPENAI_API_KEY"));
        assert!(looks_like_secret_ref("vault://kv/prod/openai"));

        let (env_key, secret_ref) =
            normalize_secret_mapping("OPENAI_API_KEY", "vault://kv/prod/openai");
        assert_eq!(env_key, "OPENAI_API_KEY");
        assert_eq!(secret_ref, "vault://kv/prod/openai");

        let (env_key, secret_ref) = normalize_secret_mapping("kv/prod/openai", "OPENAI_API_KEY");
        assert_eq!(env_key, "OPENAI_API_KEY");
        assert_eq!(secret_ref, "kv/prod/openai");
    }

    #[test]
    fn resolve_secret_file_scheme_reads_file_contents() {
        let file = std::env::temp_dir().join(format!(
            "anna-secret-file-{}-{}.txt",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::write(&file, "top-secret\n").expect("write temp secret file");

        let value = resolve_secret_ref(
            "test-stage",
            &format!("file://{}", file.display()),
            &HashMap::new(),
        )
        .expect("file:// secret should resolve");
        assert_eq!(value, "top-secret");
    }

    #[test]
    fn resolve_env_scheme_returns_not_found_for_missing_var() {
        let err = resolve_secret_ref("test-stage", "env://ANNA_TEST_MISSING", &HashMap::new())
            .expect_err("missing env secret should fail");
        assert_eq!(err.code, "provider_secret_not_found");
    }

    #[test]
    fn resolve_vault_like_secret_from_file_map() {
        let mut file_secrets = HashMap::new();
        file_secrets.insert("kv/prod/token".to_string(), "abc123".to_string());
        let value = resolve_secret_ref("test-stage", "vault://kv/prod/token", &file_secrets)
            .expect("vault secret should resolve from file map");
        assert_eq!(value, "abc123");
    }
}
