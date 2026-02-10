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
    for (secret_ref, env_key) in &stage.secrets {
        if env_key.trim().is_empty() {
            return Err(ProviderError::new(
                "provider_start_failed",
                format!(
                    "stage '{}' has secrets mapping with empty env key for '{}'",
                    stage.id, secret_ref
                ),
            ));
        }

        let rendered_secret = subst(secret_ref, vars, outputs);
        let value = if let Some(v) = std::env::var_os(secret_env_var_name(&rendered_secret)) {
            v.to_string_lossy().to_string()
        } else if let Some(v) = file_secrets.get(&rendered_secret) {
            v.clone()
        } else {
            return Err(ProviderError::new(
                "provider_secret_not_found",
                format!(
                    "stage '{}' missing secret '{}' (env={} or file={})",
                    stage.id,
                    rendered_secret,
                    secret_env_var_name(&rendered_secret),
                    secrets_file_path().display()
                ),
            ));
        };
        resolved.insert(env_key.clone(), value);
    }
    Ok(resolved)
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
    reg
}

#[cfg(test)]
mod tests {
    use super::{sanitize_secret_key, secret_env_var_name};

    #[test]
    fn secret_env_name_is_stable() {
        assert_eq!(sanitize_secret_key("kv/prod/api-key"), "KV_PROD_API_KEY");
        assert_eq!(
            secret_env_var_name("kv/prod/api-key"),
            "ANNA_SECRET_KV_PROD_API_KEY"
        );
    }
}
