use super::{VaultCommand, VaultConfig, VaultOpResult, key_allowed};
use crate::providers::{ProviderError, ProviderResult};
use std::sync::OnceLock;
use tokio::sync::Mutex;

fn store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(super) async fn execute_file_command(
    config: &VaultConfig,
    command: VaultCommand,
) -> ProviderResult<VaultOpResult> {
    let guard = store_lock().lock().await;
    let mut store = super::vault_store::load_store(&config.kv_file).await?;
    let file_result = match command {
        VaultCommand::Get { key } => {
            let secret_value = store.get(&key).cloned().ok_or_else(|| {
                ProviderError::new(
                    "provider_secret_not_found",
                    format!("vault key '{}' not found", key),
                )
            })?;
            Ok(VaultOpResult::Get {
                key,
                value: secret_value,
            })
        }
        VaultCommand::Put { key, value } => {
            store.insert(key.clone(), value);
            super::vault_store::save_store(&config.kv_file, &store).await?;
            Ok(VaultOpResult::Put { key })
        }
        VaultCommand::Delete { key } => {
            let deleted = store.remove(&key).is_some();
            super::vault_store::save_store(&config.kv_file, &store).await?;
            Ok(VaultOpResult::Delete { key, deleted })
        }
        VaultCommand::List { prefix } => {
            let mut keys = store
                .keys()
                .filter(|key| {
                    if let Some(prefix) = prefix.as_ref()
                        && !key.starts_with(prefix)
                    {
                        return false;
                    }
                    key_allowed(config, key)
                })
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            Ok(VaultOpResult::List { prefix, keys })
        }
    };
    drop(guard);
    file_result
}
