use crate::expr::subst;
use crate::providers::cli::CliProvider;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct LlmProvider;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LlmAdapterCatalog {
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub adapters: HashMap<String, LlmAdapterSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LlmAdapterSpec {
    pub exec: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub parse: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadedLlmAdapterCatalog {
    pub path: String,
    pub catalog: LlmAdapterCatalog,
}

#[async_trait]
impl Provider for LlmProvider {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let prompt = build_prompt(stage, workflow, vars, outputs);
        let loaded_catalog = load_llm_adapter_catalog_from_env()?;
        let catalog = loaded_catalog.as_ref().map(|v| &v.catalog);
        let cli_stage = build_cli_stage(stage, workflow, vars, outputs, &prompt, catalog)?;

        let cli = CliProvider;
        cli.run(&cli_stage, workflow, vars, outputs, timeout).await
    }
}

fn build_prompt(
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> String {
    let mut prompt = stage
        .do_prompt
        .as_deref()
        .map(|v| subst(v, vars, outputs))
        .unwrap_or_default();

    if let Some(system) = stage.system.as_deref() {
        let rendered = subst(system, vars, outputs);
        prompt = format!("<system>\n{}\n</system>\n\n{}", rendered, prompt);
    }
    if !stage.context.is_empty() {
        let context_blob = build_context_blob(&stage.context, workflow, vars, outputs);
        if !context_blob.is_empty() {
            prompt = format!("{}\n\n<context>\n{}\n</context>", prompt, context_blob);
        }
    }

    prompt
}

fn build_cli_stage(
    stage: &Stage,
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
    prompt: &str,
    catalog: Option<&LlmAdapterCatalog>,
) -> ProviderResult<Stage> {
    let adapter_name = select_adapter_name(stage, catalog);
    let adapter = resolve_adapter(adapter_name.as_deref(), catalog)?;

    let wrapper = stage
        .exec
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .map(|v| subst(v, vars, outputs))
        .or_else(|| {
            adapter
                .as_ref()
                .map(|spec| subst(spec.exec.as_str(), vars, outputs))
        })
        .or_else(|| non_empty_env("ANNA_LLM_WRAPPER"))
        .unwrap_or_else(|| "./tools/anna-llm-provider".to_string());

    let model = stage
        .model
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .map(|v| subst(v, vars, outputs))
        .or_else(|| non_empty_env("ANNA_LLM_MODEL"))
        .or_else(|| {
            adapter
                .as_ref()
                .and_then(|spec| spec.model.as_deref())
                .filter(|v| !v.trim().is_empty())
                .map(|v| subst(v, vars, outputs))
        })
        .unwrap_or_else(|| "gpt-4o-mini".to_string());

    let parse_mode = stage
        .parse
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .map(|v| subst(v, vars, outputs))
        .or_else(|| {
            adapter
                .as_ref()
                .and_then(|spec| spec.parse.as_deref())
                .filter(|v| !v.trim().is_empty())
                .map(|v| subst(v, vars, outputs))
        })
        .unwrap_or_else(|| "text".to_string());
    if parse_mode != "text" && parse_mode != "json" {
        return Err(ProviderError::new(
            "provider_invalid_response",
            format!(
                "invalid parse mode '{}' for llm stage '{}', expected text|json",
                parse_mode, stage.id
            ),
        ));
    }

    let mut args = Vec::new();
    if let Some(spec) = adapter.as_ref() {
        for arg in &spec.args {
            args.push(subst(arg, vars, outputs));
        }
    }
    for arg in &stage.args {
        args.push(subst(arg, vars, outputs));
    }
    args.push("--model".to_string());
    args.push(model);
    args.push("--format".to_string());
    args.push(parse_mode.clone());

    let mut cli_stage = stage.clone();
    cli_stage.provider = "cli".to_string();
    cli_stage.exec = Some(wrapper);
    cli_stage.args = args;
    cli_stage.stdin = Some(prompt.to_string());
    cli_stage.parse = Some(parse_mode);

    if let Some(spec) = adapter.as_ref() {
        for (key, value) in &spec.env {
            cli_stage
                .env
                .entry(key.clone())
                .or_insert_with(|| subst(value, vars, outputs));
        }
    }

    if let Some(name) = adapter_name
        && !name.trim().is_empty()
    {
        cli_stage
            .env
            .entry("ANNA_LLM_ADAPTER".to_string())
            .or_insert(name);
    }

    if cli_stage.workdir.is_none() {
        cli_stage.workdir = workflow.workdir.clone();
    }

    Ok(cli_stage)
}

fn select_adapter_name(stage: &Stage, catalog: Option<&LlmAdapterCatalog>) -> Option<String> {
    stage
        .llm_adapter
        .as_ref()
        .filter(|v| !v.trim().is_empty())
        .cloned()
        .or_else(|| active_llm_adapter_name(catalog))
}

pub fn active_llm_adapter_name(catalog: Option<&LlmAdapterCatalog>) -> Option<String> {
    non_empty_env("ANNA_LLM_ADAPTER").or_else(|| {
        catalog
            .and_then(|c| c.default.as_ref())
            .filter(|v| !v.trim().is_empty())
            .cloned()
    })
}

fn resolve_adapter(
    name: Option<&str>,
    catalog: Option<&LlmAdapterCatalog>,
) -> ProviderResult<Option<LlmAdapterSpec>> {
    let Some(name) = name else {
        return Ok(None);
    };
    let Some(catalog) = catalog else {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!(
                "llm adapter '{}' requested but ANNA_LLM_ADAPTERS_FILE is not set",
                name
            ),
        ));
    };

    let Some(spec) = catalog.adapters.get(name) else {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!("unknown llm adapter '{}'", name),
        ));
    };

    if spec.exec.trim().is_empty() {
        return Err(ProviderError::new(
            "provider_start_failed",
            format!("llm adapter '{}' has empty exec", name),
        ));
    }

    Ok(Some(spec.clone()))
}

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn adapter_catalog_path() -> Option<PathBuf> {
    env::var_os("ANNA_LLM_ADAPTERS_FILE")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

pub fn load_llm_adapter_catalog_from_env() -> ProviderResult<Option<LoadedLlmAdapterCatalog>> {
    let Some(path) = adapter_catalog_path() else {
        return Ok(None);
    };

    let raw = std::fs::read_to_string(&path).map_err(|err| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "failed reading ANNA_LLM_ADAPTERS_FILE '{}': {}",
                path.display(),
                err
            ),
        )
    })?;

    let catalog = parse_adapter_catalog(&raw).map_err(|message| {
        ProviderError::new(
            "provider_start_failed",
            format!(
                "invalid llm adapter catalog '{}': {}",
                path.display(),
                message
            ),
        )
    })?;

    Ok(Some(LoadedLlmAdapterCatalog {
        path: path.display().to_string(),
        catalog,
    }))
}

fn parse_adapter_catalog(raw: &str) -> Result<LlmAdapterCatalog, String> {
    let catalog: LlmAdapterCatalog =
        serde_yaml::from_str(raw).map_err(|err| format!("failed to parse yaml: {}", err))?;
    for (name, spec) in &catalog.adapters {
        if spec.exec.trim().is_empty() {
            return Err(format!("adapter '{}' has empty exec", name));
        }
    }
    Ok(catalog)
}

fn build_context_blob(
    context_files: &[String],
    workflow: &Workflow,
    vars: &HashMap<String, String>,
    outputs: &HashMap<String, String>,
) -> String {
    let mut out = String::new();
    for file in context_files {
        let rendered = subst(file, vars, outputs);
        let path = match workflow.workdir.as_ref() {
            Some(wd) => std::path::Path::new(wd).join(&rendered),
            None => std::path::PathBuf::from(&rendered),
        };
        if let Ok(content) = std::fs::read_to_string(&path) {
            out.push_str(&format!("## {}\n{}\n\n", path.display(), content));
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        LlmAdapterCatalog, LlmAdapterSpec, build_cli_stage, build_prompt, parse_adapter_catalog,
    };
    use crate::workflow::{Stage, Workflow};
    use std::collections::HashMap;

    fn make_workflow() -> Workflow {
        Workflow {
            name: "llm-provider-test".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env: HashMap::new(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![],
            source_path: None,
        }
    }

    #[test]
    fn build_prompt_includes_system_and_context_when_present() {
        let mut stage = Stage {
            id: "review".to_string(),
            provider: "llm".to_string(),
            do_prompt: Some("Review: $input".to_string()),
            system: Some("You are strict.".to_string()),
            ..Default::default()
        };
        let wf = make_workflow();
        let context_path = std::env::temp_dir().join(format!(
            "anna-llm-context-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::write(&context_path, "line1\nline2").expect("write context file");
        stage.context = vec![context_path.to_string_lossy().to_string()];

        let mut vars = HashMap::new();
        vars.insert("input".to_string(), "abc".to_string());

        let prompt = build_prompt(&stage, &wf, &vars, &HashMap::new());
        assert!(prompt.contains("<system>"));
        assert!(prompt.contains("Review: abc"));
        assert!(prompt.contains("<context>"));
        assert!(prompt.contains("line1"));

        let _ = std::fs::remove_file(context_path);
    }

    #[test]
    fn build_cli_stage_uses_selected_adapter() {
        let stage = Stage {
            id: "llm".to_string(),
            provider: "llm".to_string(),
            llm_adapter: Some("openbao".to_string()),
            model: Some("claude-sonnet".to_string()),
            args: vec!["--mock".to_string()],
            parse: Some("json".to_string()),
            ..Default::default()
        };
        let mut adapters = HashMap::new();
        adapters.insert(
            "openbao".to_string(),
            LlmAdapterSpec {
                exec: "./tools/anna-llm-provider".to_string(),
                args: vec!["--backend-cmd".to_string(), "openbao-cli".to_string()],
                env: HashMap::from([(
                    "ANNA_LLM_BACKEND_CMD".to_string(),
                    "openbao-cli".to_string(),
                )]),
                model: Some("ignored-model".to_string()),
                parse: Some("text".to_string()),
            },
        );
        let catalog = LlmAdapterCatalog {
            default: Some("openbao".to_string()),
            adapters,
        };

        let cli_stage = build_cli_stage(
            &stage,
            &make_workflow(),
            &HashMap::new(),
            &HashMap::new(),
            "ping",
            Some(&catalog),
        )
        .expect("stage should resolve");

        assert_eq!(cli_stage.provider, "cli");
        assert_eq!(cli_stage.exec.as_deref(), Some("./tools/anna-llm-provider"));
        assert_eq!(cli_stage.stdin.as_deref(), Some("ping"));
        assert_eq!(cli_stage.parse.as_deref(), Some("json"));
        assert!(
            cli_stage
                .args
                .windows(2)
                .any(|w| w[0] == "--backend-cmd" && w[1] == "openbao-cli")
        );
        assert!(
            cli_stage
                .args
                .windows(2)
                .any(|w| w[0] == "--model" && w[1] == "claude-sonnet")
        );
        assert!(
            cli_stage
                .args
                .windows(2)
                .any(|w| w[0] == "--format" && w[1] == "json")
        );
        assert!(cli_stage.args.iter().any(|a| a == "--mock"));
        assert_eq!(
            cli_stage.env.get("ANNA_LLM_BACKEND_CMD"),
            Some(&"openbao-cli".to_string())
        );
        assert_eq!(
            cli_stage.env.get("ANNA_LLM_ADAPTER"),
            Some(&"openbao".to_string())
        );
    }

    #[test]
    fn build_cli_stage_fails_for_unknown_adapter() {
        let stage = Stage {
            id: "llm".to_string(),
            provider: "llm".to_string(),
            llm_adapter: Some("missing".to_string()),
            ..Default::default()
        };
        let catalog = LlmAdapterCatalog {
            default: None,
            adapters: HashMap::new(),
        };

        let err = build_cli_stage(
            &stage,
            &make_workflow(),
            &HashMap::new(),
            &HashMap::new(),
            "ping",
            Some(&catalog),
        )
        .expect_err("unknown adapter should fail");

        assert_eq!(err.code, "provider_start_failed");
        assert!(err.message.contains("unknown llm adapter 'missing'"));
    }

    #[test]
    fn parse_adapter_catalog_rejects_empty_exec_entries() {
        let err = parse_adapter_catalog(
            r#"
default: mock
adapters:
  mock:
    exec: ./tools/anna-llm-provider
  bad:
    exec: ""
"#,
        )
        .expect_err("catalog should fail");
        assert!(err.contains("adapter 'bad' has empty exec"));
    }
}
