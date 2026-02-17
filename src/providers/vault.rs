use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

#[path = "vault_types.rs"]
mod vault_types;
#[path = "vault_parse.rs"]
mod vault_parse;
#[path = "vault_file.rs"]
mod vault_file;
#[path = "vault_render.rs"]
mod vault_render;
#[path = "vault_http.rs"]
mod vault_http;
#[path = "vault_config.rs"]
mod vault_config;
#[path = "vault_store.rs"]
mod vault_store;

use vault_parse::{ensure_key_allowed, key_allowed, normalize_key};
use vault_types::{
    RenderMode, VaultAuthConfig, VaultBackend, VaultCommand, VaultConfig, VaultHttpConfig,
    VaultOpResult,
};

#[derive(Debug, Default, Clone)]
/// Provider that reads/writes secrets via file or Vault-compatible HTTP APIs.
pub struct VaultProvider;

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
        let command_tokens = vault_parse::parse_command_tokens(stage, vars, outputs)?;
        let mode = vault_parse::parse_render_mode(stage, vars, outputs)?;
        let config = vault_config::read_config(stage, workflow, vars, outputs)?;
        let command = vault_parse::parse_vault_command(&command_tokens, stage, vars, outputs)?;

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
            VaultBackend::File => vault_file::execute_file_command(&config, command).await?,
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

        vault_render::render_result(result, mode).map_err(|err| {
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

#[cfg(test)]
#[path = "vault_tests.rs"]
mod tests;
