use crate::providers::{ProviderError, ProviderResult};
use std::collections::HashMap;
use std::path::Path;

pub(super) async fn load_store(path: &Path) -> ProviderResult<HashMap<String, String>> {
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => serde_json::from_str::<HashMap<String, String>>(&raw).map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!("invalid vault store json '{}': {}", path.display(), err),
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(ProviderError::new(
            "provider_start_failed",
            format!("failed reading vault store '{}': {}", path.display(), err),
        )),
    }
}

pub(super) async fn save_store(path: &Path, store: &HashMap<String, String>) -> ProviderResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|err| {
            ProviderError::new(
                "provider_start_failed",
                format!(
                    "failed creating vault store parent '{}': {}",
                    parent.display(),
                    err
                ),
            )
        })?;
    }

    let serialized_store = serde_json::to_string_pretty(store).map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "failed serializing vault store '{}': {}",
                path.display(),
                err
            ),
        )
    })?;

    tokio::fs::write(path, serialized_store).await.map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!("failed writing vault store '{}': {}", path.display(), err),
        )
    })
}
