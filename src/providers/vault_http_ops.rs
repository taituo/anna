use super::{HttpExecCtx, wire};
use crate::providers::{ProviderError, ProviderResult};
use reqwest::StatusCode;
use serde_json::{Value, json};
use super::super::VaultOpResult;

pub(super) async fn http_get(ctx: &HttpExecCtx<'_>, key: String) -> ProviderResult<VaultOpResult> {
    let url = wire::build_get_url(ctx.config, &key);
    let get_response = wire::with_vault_headers(ctx.client.get(&url), ctx.config, Some(ctx.token))
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault get request failed in stage '{}': {}", ctx.stage.id, err),
            )
        })?;
    let get_status = get_response.status();
    let get_body = get_response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault get response in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;
    validate_get_status(ctx.stage.id.as_str(), &key, get_status, &get_body)?;
    let get_payload: Value = serde_json::from_str(&get_body).map_err(|err| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault get returned invalid json in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;
    let extracted_value = extract_value_from_get(&get_payload, ctx.config.kv_version).ok_or_else(|| {
        ProviderError::new(
            "provider_invalid_response",
            format!(
                "vault get response missing value for key '{}' in stage '{}'",
                key, ctx.stage.id
            ),
        )
    })?;
    Ok(VaultOpResult::Get {
        key,
        value: extracted_value,
    })
}

pub(super) async fn http_put(
    ctx: &HttpExecCtx<'_>,
    key: String,
    value: String,
) -> ProviderResult<VaultOpResult> {
    let put_url = wire::build_put_url(ctx.config, &key);
    let put_request_body = if ctx.config.kv_version == 2 {
        json!({"data": {"value": value}})
    } else {
        json!({"value": value})
    };

    let put_response = wire::with_vault_headers(ctx.client.post(&put_url), ctx.config, Some(ctx.token))
        .json(&put_request_body)
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault put request failed in stage '{}': {}", ctx.stage.id, err),
            )
        })?;

    let put_status = put_response.status();
    let put_response_body = put_response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault put response in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;

    if !put_status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault put failed in stage '{}' with status {}: {}",
                ctx.stage.id, put_status, put_response_body
            ),
        ));
    }

    Ok(VaultOpResult::Put { key })
}

pub(super) async fn http_delete(ctx: &HttpExecCtx<'_>, key: String) -> ProviderResult<VaultOpResult> {
    let delete_url = wire::build_delete_url(ctx.config, &key);
    let delete_response = wire::with_vault_headers(ctx.client.delete(&delete_url), ctx.config, Some(ctx.token))
        .send()
        .await
        .map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "vault delete request failed in stage '{}': {}",
                    ctx.stage.id, err
                ),
            )
        })?;

    let delete_status = delete_response.status();
    let delete_response_body = delete_response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault delete response in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;

    if delete_status == StatusCode::NOT_FOUND {
        return Ok(VaultOpResult::Delete {
            key,
            deleted: false,
        });
    }
    if !delete_status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault delete failed in stage '{}' with status {}: {}",
                ctx.stage.id, delete_status, delete_response_body
            ),
        ));
    }

    Ok(VaultOpResult::Delete { key, deleted: true })
}

pub(super) async fn http_list(
    ctx: &HttpExecCtx<'_>,
    prefix: Option<String>,
) -> ProviderResult<VaultOpResult> {
    let list_url = wire::build_list_url(ctx.config, prefix.as_deref());
    let list_http_response = wire::send_list_request(ctx, &list_url).await?;
    let list_status = list_http_response.status();
    let list_response_body = list_http_response.text().await.map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "failed reading vault list response in stage '{}': {}",
                ctx.stage.id, err
            ),
        )
    })?;
    if list_status == StatusCode::NOT_FOUND {
        return Ok(VaultOpResult::List {
            prefix,
            keys: vec![],
        });
    }
    if !list_status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault list failed in stage '{}' with status {}: {}",
                ctx.stage.id, list_status, list_response_body
            ),
        ));
    }
    let mut list_keys = wire::parse_list_keys(ctx.stage.id.as_str(), &list_response_body)?;
    list_keys.sort();
    Ok(VaultOpResult::List {
        prefix,
        keys: list_keys,
    })
}

fn validate_get_status(stage_id: &str, key: &str, status: StatusCode, body: &str) -> ProviderResult<()> {
    if status == StatusCode::NOT_FOUND {
        return Err(ProviderError::new(
            "provider_secret_not_found",
            format!("vault key '{}' not found in stage '{}'", key, stage_id),
        ));
    }
    if !status.is_success() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault get failed in stage '{}' with status {}: {}",
                stage_id, status, body
            ),
        ));
    }
    Ok(())
}

fn extract_value_from_get(payload: &Value, kv_version: u8) -> Option<String> {
    let candidate = if kv_version == 2 {
        payload
            .pointer("/data/data/value")
            .or_else(|| payload.pointer("/data/value"))
    } else {
        payload
            .pointer("/data/value")
            .or_else(|| payload.pointer("/data"))
    }?;

    match candidate {
        Value::String(v) => Some(v.clone()),
        Value::Number(_) | Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Some(candidate.to_string())
        }
        Value::Null => None,
    }
}
