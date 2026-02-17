use super::wire;
use crate::providers::{ProviderError, ProviderResult};
use crate::workflow::Stage;
use serde_json::{Value, json};
use super::super::{VaultAuthConfig, VaultHttpConfig};

pub(super) async fn resolve_http_token(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
) -> ProviderResult<String> {
    match &config.auth {
        VaultAuthConfig::Token(token) => Ok(token.clone()),
        VaultAuthConfig::AppRole {
            role_id,
            secret_id,
            auth_path,
        } => {
            let creds = AppRoleCreds {
                role_id,
                secret_id,
                auth_path,
            };
            login_with_approle(client, config, stage, &creds).await
        }
    }
}

struct AppRoleCreds<'a> {
    role_id: &'a str,
    secret_id: &'a str,
    auth_path: &'a str,
}

async fn login_with_approle(
    client: &reqwest::Client,
    config: &VaultHttpConfig,
    stage: &Stage,
    creds: &AppRoleCreds<'_>,
) -> ProviderResult<String> {
    let login_url = wire::join_addr(&config.addr, &format!("v1/{}", creds.auth_path));
    let login_response = wire::with_vault_headers(client.post(login_url), config, None)
        .json(&json!({"role_id": creds.role_id, "secret_id": creds.secret_id}))
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "vault AppRole login request failed in stage '{}': {}",
                    stage.id, err
                ),
            )
        })?;
    let login_status = login_response.status();
    let login_body = login_response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault AppRole login response in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;
    if !login_status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault AppRole login failed in stage '{}' with status {}: {}",
                stage.id, login_status, login_body
            ),
        ));
    }
    extract_login_token(stage, &login_body)
}

fn extract_login_token(stage: &Stage, login_body: &str) -> ProviderResult<String> {
    let login_payload: Value = serde_json::from_str(login_body).map_err(|err| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault AppRole login returned invalid json in stage '{}': {}",
                stage.id, err
            ),
        )
    })?;
    login_payload
        .pointer("/auth/client_token")
        .and_then(|v| v.as_str())
        .or_else(|| login_payload.get("client_token").and_then(|v| v.as_str()))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            ProviderError::new(
                "provider_invalid_response",
                format!(
                    "vault AppRole login response missing auth.client_token in stage '{}'",
                    stage.id
                ),
            )
        })
}
