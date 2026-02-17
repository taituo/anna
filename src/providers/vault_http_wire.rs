use super::HttpExecCtx;
use crate::providers::{ProviderError, ProviderResult};
use reqwest::{Method, StatusCode};
use serde_json::Value;
use super::super::{VaultHttpConfig, normalize_key};

pub(super) async fn send_list_request(
    ctx: &HttpExecCtx<'_>,
    list_url: &str,
) -> ProviderResult<reqwest::Response> {
    let list_method = Method::from_bytes(b"LIST").map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "failed creating LIST method in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;
    let mut list_response = with_vault_headers(
        ctx.client.request(list_method, list_url),
        ctx.config,
        Some(ctx.token),
    )
    .send()
    .await
    .map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!("vault list request failed in stage '{}': {}", ctx.stage.id, err),
        )
    })?;
    if matches!(
        list_response.status(),
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::BAD_REQUEST
    ) {
        list_response = with_vault_headers(
            ctx.client.get(list_url).query(&[("list", "true")]),
            ctx.config,
            Some(ctx.token),
        )
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "vault list fallback request failed in stage '{}': {}",
                    ctx.stage.id, err
                ),
            )
        })?;
    }
    Ok(list_response)
}

pub(super) fn parse_list_keys(stage_id: &str, response_body: &str) -> ProviderResult<Vec<String>> {
    let list_payload: Value = serde_json::from_str(response_body).map_err(|err| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault list returned invalid json in stage '{}': {}",
                stage_id, err
            ),
        )
    })?;
    Ok(list_payload
        .pointer("/data/keys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

pub(super) fn with_vault_headers(
    builder: reqwest::RequestBuilder,
    config: &VaultHttpConfig,
    token: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut builder = builder;
    if let Some(token) = token {
        builder = builder.header("X-Vault-Token", token);
    }
    if let Some(namespace) = config.namespace.as_ref().filter(|v| !v.trim().is_empty()) {
        builder = builder.header("X-Vault-Namespace", namespace);
    }
    builder
}

pub(super) fn build_get_url(config: &VaultHttpConfig, key: &str) -> String {
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

pub(super) fn build_put_url(config: &VaultHttpConfig, key: &str) -> String {
    build_get_url(config, key)
}

pub(super) fn build_delete_url(config: &VaultHttpConfig, key: &str) -> String {
    build_get_url(config, key)
}

pub(super) fn build_list_url(config: &VaultHttpConfig, prefix: Option<&str>) -> String {
    let normalized_prefix = prefix.map(normalize_key).unwrap_or_default();
    if config.kv_version == 2 {
        if normalized_prefix.is_empty() {
            join_addr(&config.addr, &format!("v1/{}/metadata", config.mount))
        } else {
            join_addr(
                &config.addr,
                &format!("v1/{}/metadata/{}", config.mount, normalized_prefix),
            )
        }
    } else if normalized_prefix.is_empty() {
        join_addr(&config.addr, &format!("v1/{}", config.mount))
    } else {
        join_addr(
            &config.addr,
            &format!("v1/{}/{}", config.mount, normalized_prefix),
        )
    }
}

pub(super) fn join_addr(addr: &str, path: &str) -> String {
    format!(
        "{}/{}",
        addr.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}
