use super::VaultProvider;
use crate::providers::Provider;
use crate::workflow::{Stage, Workflow};
use std::collections::HashMap;

pub(super) fn make_workflow_with_env(env: HashMap<String, String>) -> Workflow {
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

pub(super) fn stage_with_args(id: &str, args: Vec<&str>) -> Stage {
    Stage {
        id: id.to_string(),
        provider: "vault".to_string(),
        args: args.into_iter().map(|v| v.to_string()).collect(),
        ..Default::default()
    }
}

pub(super) async fn run_vault(
    workflow: &Workflow,
    id: &str,
    args: Vec<&str>,
) -> crate::providers::ProviderResult<String> {
    VaultProvider
        .run(
            &stage_with_args(id, args),
            workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
}

#[path = "vault_tests_file.rs"]
mod vault_tests_file;

#[path = "vault_tests_http.rs"]
mod vault_tests_http;
