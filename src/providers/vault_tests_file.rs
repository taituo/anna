use super::{make_workflow_with_env, run_vault, stage_with_args};
use super::super::VaultProvider;
use crate::providers::Provider;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

fn temp_vault_file() -> PathBuf {
    std::env::temp_dir().join(format!(
        "anna-vault-provider-{}-{}.json",
        std::process::id(),
        rand::random::<u32>()
    ))
}

#[tokio::test]
async fn put_get_list_delete_roundtrip_text() {
    let kv_file_path = temp_vault_file();
    let file_backend_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
        (
            "ANNA_VAULT_KV_FILE".to_string(),
            kv_file_path.display().to_string(),
        ),
    ]));

    let put_output = run_vault(
        &file_backend_workflow,
        "put",
        vec!["put", "kv/dev/value", "sample-value"],
    )
    .await
    .expect("put should succeed");
    assert_eq!(put_output, "ok");

    let get_output = run_vault(&file_backend_workflow, "get", vec!["get", "kv/dev/value"])
        .await
        .expect("get should succeed");
    assert_eq!(get_output, "sample-value");

    let list_output = run_vault(&file_backend_workflow, "list", vec!["list", "kv/dev"])
        .await
        .expect("list should succeed");
    assert_eq!(list_output, "kv/dev/value");

    let delete_output = run_vault(
        &file_backend_workflow,
        "delete",
        vec!["delete", "kv/dev/value"],
    )
    .await
    .expect("delete should succeed");
    assert_eq!(delete_output, "ok");

    let missing_get_err = run_vault(
        &file_backend_workflow,
        "get-missing",
        vec!["get", "kv/dev/value"],
    )
    .await
    .expect_err("missing key should fail");
    assert_eq!(missing_get_err.code, "provider_secret_not_found");

    tokio::fs::remove_file(kv_file_path)
        .await
        .expect("remove temp vault file");
}

#[tokio::test]
async fn json_mode_outputs_structured_payload() {
    let kv_file_path_json = temp_vault_file();
    let json_mode_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
        (
            "ANNA_VAULT_KV_FILE".to_string(),
            kv_file_path_json.display().to_string(),
        ),
    ]));

    run_vault(
        &json_mode_workflow,
        "put-json",
        vec!["put", "kv/dev/json", "123"],
    )
    .await
    .expect("put should succeed");

    let mut get_json_stage = stage_with_args("get-json", vec!["get", "kv/dev/json"]);
    get_json_stage.parse = Some("json".to_string());

    let get_json_output = VaultProvider
        .run(
            &get_json_stage,
            &json_mode_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect("get json should succeed");

    let parsed_get_json: serde_json::Value =
        serde_json::from_str(&get_json_output).expect("json output should parse");
    assert_eq!(parsed_get_json, json!({"op":"get","key":"kv/dev/json","value":"123"}));

    tokio::fs::remove_file(kv_file_path_json)
        .await
        .expect("remove temp vault file");
}

#[tokio::test]
async fn allowlist_blocks_disallowed_keys() {
    let allowlist_file = temp_vault_file();
    let allowlist_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
        (
            "ANNA_VAULT_KV_FILE".to_string(),
            allowlist_file.display().to_string(),
        ),
        (
            "ANNA_VAULT_PREFIX_ALLOW".to_string(),
            "kv/dev,kv/shared".to_string(),
        ),
    ]));

    run_vault(
        &allowlist_workflow,
        "put-allowed",
        vec!["put", "kv/dev/token", "ok"],
    )
    .await
    .expect("allowed put should succeed");

    let blocked_put_err = run_vault(
        &allowlist_workflow,
        "put-blocked",
        vec!["put", "kv/prod/token", "nope"],
    )
    .await
    .expect_err("blocked put should fail");
    assert_eq!(blocked_put_err.code, "provider_exec_failed");
    assert!(blocked_put_err.message.contains("blocked by allowlist"));

    tokio::fs::remove_file(allowlist_file)
        .await
        .expect("remove temp vault file");
}

#[tokio::test]
async fn read_only_blocks_mutation_ops() {
    let read_only_file = temp_vault_file();
    tokio::fs::write(&read_only_file, "{\"kv/prod/token\":\"abc\"}")
        .await
        .expect("seed read only vault file");

    let read_only_workflow = make_workflow_with_env(HashMap::from([
        ("ANNA_VAULT_BACKEND".to_string(), "file".to_string()),
        (
            "ANNA_VAULT_KV_FILE".to_string(),
            read_only_file.display().to_string(),
        ),
        ("ANNA_VAULT_READ_ONLY".to_string(), "true".to_string()),
    ]));

    let get_read_only_output = VaultProvider
        .run(
            &stage_with_args("get", vec!["get", "kv/prod/token"]),
            &read_only_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect("get should succeed in read-only");
    assert_eq!(get_read_only_output, "abc");

    let put_read_only_err = VaultProvider
        .run(
            &stage_with_args("put", vec!["put", "kv/prod/token", "new"]),
            &read_only_workflow,
            &HashMap::new(),
            &HashMap::new(),
            None,
        )
        .await
        .expect_err("put should be blocked in read-only");
    assert_eq!(put_read_only_err.code, "provider_exec_failed");
    assert!(put_read_only_err.message.contains("read-only"));

    tokio::fs::remove_file(read_only_file)
        .await
        .expect("remove temp vault file");
}
