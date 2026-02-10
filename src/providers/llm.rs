use crate::expr::subst;
use crate::providers::cli::CliProvider;
use crate::providers::{Provider, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct LlmProvider;

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

        let wrapper = env::var("ANNA_LLM_WRAPPER")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "./tools/anna-llm-provider".to_string());
        let model = stage
            .model
            .clone()
            .or_else(|| env::var("ANNA_LLM_MODEL").ok())
            .unwrap_or_else(|| "gpt-4o-mini".to_string());

        let mut cli_stage = stage.clone();
        cli_stage.provider = "cli".to_string();
        cli_stage.exec = Some(wrapper);
        cli_stage.args = vec![
            "--model".to_string(),
            model,
            "--format".to_string(),
            "text".to_string(),
        ];
        cli_stage.stdin = Some(prompt);
        cli_stage.parse = Some("text".to_string());

        let cli = CliProvider;
        cli.run(&cli_stage, workflow, vars, outputs, timeout).await
    }
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
