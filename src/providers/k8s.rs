use crate::expr::subst;
use crate::providers::cli::CliProvider;
use crate::providers::{Provider, ProviderError, ProviderResult};
use crate::workflow::{Stage, Workflow};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct K8sProvider;

#[async_trait]
impl Provider for K8sProvider {
    async fn run(
        &self,
        stage: &Stage,
        workflow: &Workflow,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
        timeout: Option<Duration>,
    ) -> ProviderResult<String> {
        let args = if !stage.args.is_empty() {
            stage
                .args
                .iter()
                .map(|a| subst(a, vars, outputs))
                .collect::<Vec<_>>()
        } else {
            let exec = stage.exec.as_ref().ok_or_else(|| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "stage '{}' requires either 'args' or 'exec' for provider=k8s",
                        stage.id
                    ),
                )
            })?;
            let rendered = subst(exec, vars, outputs);
            shell_words::split(&rendered).map_err(|err| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "failed to parse k8s command in stage '{}': {}",
                        stage.id, err
                    ),
                )
            })?
        };

        if args.is_empty() {
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' has empty kubectl arguments", stage.id),
            ));
        }

        let mut cli_stage = stage.clone();
        cli_stage.provider = "cli".to_string();
        cli_stage.exec = Some("kubectl".to_string());
        cli_stage.args = args;

        let cli = CliProvider;
        cli.run(&cli_stage, workflow, vars, outputs, timeout).await
    }
}
