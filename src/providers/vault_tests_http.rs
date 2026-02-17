use super::{make_workflow_with_env, run_vault, stage_with_args};
use super::super::VaultProvider;
use crate::providers::Provider;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::{Json, Router, routing::any, routing::post};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn http_backend_roundtrip_kv_v2() {
    let mock_vault_addr = spawn_mock_vault_server().await;
    let http_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
        ("ANNA_VAULT_ADDR".to_string(), mock_vault_addr),
        ("ANNA_VAULT_TOKEN".to_string(), "test-token".to_string()),
        ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
        ("ANNA_VAULT_KV_VERSION".to_string(), "2".to_string()),
    ]));

    let put_http_output = run_vault(&http_workflow, "put", vec!["put", "kv/prod/value", "hello"])
        .await
        .expect("http put should succeed");
    assert_eq!(put_http_output, "ok");

    let get_http_output = run_vault(&http_workflow, "get", vec!["get", "kv/prod/value"])
        .await
        .expect("http get should succeed");
    assert_eq!(get_http_output, "hello");

    let list_http_output = run_vault(&http_workflow, "list", vec!["list", "kv/prod"])
        .await
        .expect("http list should succeed");
    assert_eq!(list_http_output, "kv/prod/value");

    let delete_http_output = run_vault(
        &http_workflow,
        "delete",
        vec!["delete", "kv/prod/value"],
    )
    .await
    .expect("http delete should succeed");
    assert_eq!(delete_http_output, "ok");
}

#[tokio::test]
async fn http_backend_roundtrip_kv_v2_with_approle() {
    let approle_mock_addr = spawn_mock_vault_server().await;
    let approle_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
        ("ANNA_VAULT_ADDR".to_string(), approle_mock_addr),
        ("ANNA_VAULT_ROLE_ID".to_string(), "approle-role".to_string()),
        (
            "ANNA_VAULT_SECRET_ID".to_string(),
            "approle-secret".to_string(),
        ),
        ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
        ("ANNA_VAULT_KV_VERSION".to_string(), "2".to_string()),
    ]));

    let approle_put_output = VaultProvider
        .run(
            &stage_with_args("put", vec!["put", "kv/prod/approle-token", "hello"]),
            &approle_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect("approle put should succeed");
    assert_eq!(approle_put_output, "ok");

    let approle_get_output = VaultProvider
        .run(
            &stage_with_args("get", vec!["get", "kv/prod/approle-token"]),
            &approle_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect("approle get should succeed");
    assert_eq!(approle_get_output, "hello");
}

#[tokio::test]
async fn http_backend_requires_addr_and_auth() {
    let missing_addr_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
        ("ANNA_VAULT_TOKEN".to_string(), "test-token".to_string()),
        ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
    ]));
    let missing_addr_err = VaultProvider
        .run(
            &stage_with_args("get", vec!["get", "kv/prod/token"]),
            &missing_addr_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect_err("missing addr should fail");
    assert_eq!(missing_addr_err.code, "provider_start_failed");
    assert!(missing_addr_err.message.contains("ANNA_VAULT_ADDR"));

    let missing_auth_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "http".to_string()),
        (
            "ANNA_VAULT_ADDR".to_string(),
            "http://127.0.0.1:8200".to_string(),
        ),
        ("ANNA_VAULT_MOUNT".to_string(), "secret".to_string()),
    ]));
    let missing_auth_err = VaultProvider
        .run(
            &stage_with_args("get-no-auth", vec!["get", "kv/prod/token"]),
            &missing_auth_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect_err("missing auth should fail");
    assert_eq!(missing_auth_err.code, "provider_start_failed");
    assert!(missing_auth_err.message.contains("ANNA_VAULT_TOKEN"));
}

#[derive(Clone, Default)]
struct MockVaultState {
    store: Arc<Mutex<HashMap<String, String>>>,
}

async fn spawn_mock_vault_server() -> String {
    let state = Arc::new(MockVaultState::default());
    let app = Router::new()
        .route("/v1/auth/approle/login", post(mock_approle_login))
        .route("/v1/secret/data/{*path}", any(mock_data))
        .route("/v1/secret/metadata", any(mock_list_root))
        .route("/v1/secret/metadata/{*path}", any(mock_list))
        .route("/v1/secret/{*path}", any(mock_data))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock vault server should run");
    });

    format!("http://{}", addr)
}

fn authorize(headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    let header_token = headers
        .get("X-Vault-Token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if header_token != "test-token" && header_token != "approle-token" {
        return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()));
    }
    Ok(())
}

async fn mock_approle_login(Json(body): Json<Value>) -> Result<Json<Value>, (StatusCode, String)> {
    let login_role_id = body.get("role_id").and_then(|v| v.as_str()).unwrap_or_default();
    let login_secret_id = body
        .get("secret_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if login_role_id != "approle-role" || login_secret_id != "approle-secret" {
        return Err((StatusCode::UNAUTHORIZED, "invalid approle creds".to_string()));
    }
    Ok(Json(json!({"auth": {"client_token": "approle-token"}})))
}

async fn mock_data(
    method: Method,
    State(state): State<Arc<MockVaultState>>,
    headers: HeaderMap,
    AxumPath(path): AxumPath<String>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers)?;
    match method {
        Method::GET => mock_get(state, path).await,
        Method::POST => mock_put(state, path, body).await,
        Method::DELETE => mock_delete(state, path).await,
        _ => Err((StatusCode::METHOD_NOT_ALLOWED, "method not allowed".to_string())),
    }
}

async fn mock_get(
    state: Arc<MockVaultState>,
    path: String,
) -> Result<Json<Value>, (StatusCode, String)> {
    let get_store_guard = state.store.lock().await;
    let get_value = get_store_guard
        .get(path.as_str())
        .cloned()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    Ok(Json(json!({"data": {"data": {"value": get_value}}})))
}

async fn mock_put(
    state: Arc<MockVaultState>,
    path: String,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let put_payload = body.ok_or_else(|| (StatusCode::BAD_REQUEST, "missing body".to_string()))?;
    let put_value = put_payload
        .0
        .pointer("/data/value")
        .or_else(|| put_payload.0.pointer("/value"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing value".to_string()))?
        .to_string();
    let mut put_store_guard = state.store.lock().await;
    put_store_guard.insert(path, put_value);
    Ok(Json(json!({"ok": true})))
}

async fn mock_delete(
    state: Arc<MockVaultState>,
    path: String,
) -> Result<Json<Value>, (StatusCode, String)> {
    let mut delete_store_guard = state.store.lock().await;
    let deleted = delete_store_guard.remove(path.as_str()).is_some();
    if deleted {
        Ok(Json(json!({"ok": true})))
    } else {
        Err((StatusCode::NOT_FOUND, "not found".to_string()))
    }
}

async fn mock_list_root(
    method: Method,
    State(state): State<Arc<MockVaultState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    mock_list_impl(method, state, headers, "").await
}

async fn mock_list(
    method: Method,
    State(state): State<Arc<MockVaultState>>,
    headers: HeaderMap,
    AxumPath(path): AxumPath<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    mock_list_impl(method, state, headers, &path).await
}

async fn mock_list_impl(
    method: Method,
    state: Arc<MockVaultState>,
    headers: HeaderMap,
    prefix: &str,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers)?;
    if method.as_str() != "LIST" && method != Method::GET {
        return Err((StatusCode::METHOD_NOT_ALLOWED, "method not allowed".to_string()));
    }

    let prefix_path = prefix.trim_matches('/').to_string();
    let list_store_guard = state.store.lock().await;
    let mut mock_listed_keys = list_store_guard
        .keys()
        .filter(|key| prefix_path.is_empty() || key.starts_with(&prefix_path))
        .cloned()
        .collect::<Vec<_>>();
    mock_listed_keys.sort();
    Ok(Json(json!({"data": {"keys": mock_listed_keys}})))
}
