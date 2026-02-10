use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
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

pub fn default_registry() -> ProviderRegistry {
    let mut reg = ProviderRegistry::new();
    reg.register("shell", shell::ShellProvider::default());
    reg.register("cli", cli::CliProvider::default());
    reg.register("http", http::HttpProvider::default());
    reg.register("llm", llm::LlmProvider::default());
    reg.register("k8s", k8s::K8sProvider::default());
    reg
}
