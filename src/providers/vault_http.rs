use super::{VaultCommand, VaultHttpConfig, VaultOpResult};
use crate::providers::{ProviderError, ProviderResult};
use crate::workflow::Stage;
use std::time::Duration;

#[path = "vault_http_auth.rs"]
mod auth;
#[path = "vault_http_ops.rs"]
mod ops;
#[path = "vault_http_wire.rs"]
mod wire;

pub(super) struct HttpExecCtx<'a> {
    pub(super) client: &'a reqwest::Client,
    pub(super) config: &'a VaultHttpConfig,
    pub(super) token: &'a str,
    pub(super) stage: &'a Stage,
}

pub(super) async fn execute_http_command(
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
    let auth_token = auth::resolve_http_token(&client, config, stage).await?;
    let ctx = HttpExecCtx {
        client: &client,
        config,
        token: &auth_token,
        stage,
    };
    match command {
        VaultCommand::Get { key } => ops::http_get(&ctx, key).await,
        VaultCommand::Put { key, value } => ops::http_put(&ctx, key, value).await,
        VaultCommand::Delete { key } => ops::http_delete(&ctx, key).await,
        VaultCommand::List { prefix } => ops::http_list(&ctx, prefix).await,
    }
}
