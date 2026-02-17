use super::{RenderMode, VaultCommand, VaultConfig};
use crate::expr::subst;
use crate::providers::{ProviderError, ProviderResult};
use crate::workflow::Stage;
use std::collections::HashMap;

pub(super) fn parse_command_tokens(
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<Vec<String>> {
    if !stage.args.is_empty() {
        return Ok(stage
            .args
            .iter()
            .map(|arg| subst(arg, vars, outputs))
            .collect());
    }

    let exec = stage.exec.as_deref().ok_or_else(|| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "stage '{}' requires 'args' or 'exec' for provider=vault",
                stage.id
            ),
        )
    })?;
    let rendered_exec = subst(exec, vars, outputs);
    let tokens = shell_words::split(rendered_exec.trim()).map_err(|err| {
        ProviderError::new(
            "provider_exec_failed",
            format!("failed parsing vault exec in stage '{}': {}", stage.id, err),
        )
    })?;

    if tokens.is_empty() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!("vault command is empty in stage '{}'", stage.id),
        ));
    }
    Ok(tokens)
}

pub(super) fn parse_render_mode(
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<RenderMode> {
    let rendered = stage
        .parse
        .as_deref()
        .map(|v| subst(v, vars, outputs))
        .unwrap_or_else(|| "text".to_string());
    match rendered.as_str() {
        "text" => Ok(RenderMode::Text),
        "json" => Ok(RenderMode::Json),
        other => Err(ProviderError::new(
            "provider_invalid_response",
            format!(
                "unsupported parse mode '{}' in stage '{}', expected text|json",
                other, stage.id
            ),
        )),
    }
}

pub(super) fn parse_vault_command(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultCommand> {
    if tokens.is_empty() {
        return Err(ProviderError::new(
            "provider_exec_failed",
            format!("vault command is empty in stage '{}'", stage.id),
        ));
    }
    let op = tokens[0].to_ascii_lowercase();
    match op.as_str() {
        "get" => Ok(VaultCommand::Get {
            key: required_key(tokens, stage)?,
        }),
        "put" | "set" => parse_put_command(tokens, stage, vars, outputs),
        "delete" | "del" | "rm" => Ok(VaultCommand::Delete {
            key: required_key(tokens, stage)?,
        }),
        "list" => Ok(VaultCommand::List {
            prefix: parse_list_prefix(tokens),
        }),
        other => Err(ProviderError::new(
            "provider_exec_failed",
            format!(
                "unsupported vault operation '{}' in stage '{}'; expected get|put|delete|list",
                other, stage.id
            ),
        )),
    }
}

fn parse_put_command(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<VaultCommand> {
    let key = required_key(tokens, stage)?;
    let value = parse_value_from_tokens_or_stdin(tokens, stage, vars, outputs)?.ok_or_else(|| {
        ProviderError::new(
            "provider_exec_failed",
            format!(
                "vault put requires value in args or stdin for stage '{}'",
                stage.id
            ),
        )
    })?;
    Ok(VaultCommand::Put { key, value })
}

fn parse_list_prefix(tokens: &[String]) -> Option<String> {
    tokens
        .get(1)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn required_key(tokens: &[String], stage: &Stage) -> ProviderResult<String> {
    tokens
        .get(1)
        .cloned()
        .map(|v| normalize_key(&v))
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("vault operation requires key in stage '{}'", stage.id),
            )
        })
}

fn parse_value_from_tokens_or_stdin(
    tokens: &[String],
    stage: &Stage,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> ProviderResult<Option<String>> {
    if tokens.len() >= 3 {
        return Ok(Some(tokens[2..].join(" ")));
    }
    Ok(stage.stdin.as_deref().map(|v| subst(v, vars, outputs)))
}

pub(super) fn normalize_key(raw: &str) -> String {
    raw.trim().trim_matches('/').to_string()
}

pub(super) fn key_allowed(config: &VaultConfig, key: &str) -> bool {
    match config.allow_prefixes.as_ref() {
        None => true,
        Some(prefixes) => prefixes.iter().any(|prefix| key.starts_with(prefix)),
    }
}

pub(super) fn ensure_key_allowed(
    config: &VaultConfig,
    stage: &Stage,
    key: &str,
) -> ProviderResult<()> {
    if key_allowed(config, key) {
        return Ok(());
    }
    Err(ProviderError::new(
        "provider_exec_failed",
        format!(
            "vault key '{}' is blocked by allowlist in stage '{}'",
            key, stage.id
        ),
    ))
}
