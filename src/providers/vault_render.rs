use super::{RenderMode, VaultOpResult};
use serde_json::json;

pub(super) fn render_result(
    result: VaultOpResult,
    mode: RenderMode,
) -> Result<String, serde_json::Error> {
    match mode {
        RenderMode::Text => Ok(match result {
            VaultOpResult::Get { value, .. } => value,
            VaultOpResult::Put { .. } => "ok".to_string(),
            VaultOpResult::Delete { .. } => "ok".to_string(),
            VaultOpResult::List { keys, .. } => keys.join("\n"),
        }),
        RenderMode::Json => {
            let rendered_payload = match result {
                VaultOpResult::Get { key, value } => {
                    json!({"op":"get","key":key,"value":value})
                }
                VaultOpResult::Put { key } => {
                    json!({"op":"put","key":key,"status":"ok"})
                }
                VaultOpResult::Delete { key, deleted } => {
                    json!({"op":"delete","key":key,"deleted":deleted})
                }
                VaultOpResult::List { prefix, keys } => {
                    json!({"op":"list","prefix":prefix,"keys":keys})
                }
            };
            serde_json::to_string(&rendered_payload)
        }
    }
}
