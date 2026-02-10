use crate::expr::{eval_when, subst};
use crate::providers::{ProviderError, ProviderRegistry, default_registry};
use crate::result::RunResult;
use crate::session::write_stage_log;
use crate::workflow::{Stage, Workflow};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::time::sleep;

#[derive(Debug, Clone, Default)]
pub struct RunConfig {
    pub max_iterations: Option<u32>,
}

#[derive(Clone, Default)]
pub struct Executor {
    providers: ProviderRegistry,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            providers: default_registry(),
        }
    }

    pub async fn run(&self, workflow: &Workflow, config: RunConfig) -> Result<RunResult> {
        let mut result = RunResult::new();
        let mut iterations = 0_u32;

        loop {
            iterations += 1;
            let broken = self.run_once(workflow, &mut result).await?;

            if !workflow.is_continuous() || broken {
                break;
            }
            if let Some(max) = config.max_iterations
                && iterations >= max
            {
                break;
            }

            sleep(workflow.interval()).await;
        }

        Ok(result)
    }

    async fn run_once(&self, workflow: &Workflow, result: &mut RunResult) -> Result<bool> {
        for stage in &workflow.stages {
            if !deps_ok(stage, result) {
                continue;
            }
            if !eval_when(stage.when.as_deref(), &result.outputs, &result.success) {
                continue;
            }
            if stage.workflow.is_some() {
                return Err(anyhow!(
                    "stage '{}' uses feature not yet implemented in rust mvp (sub-workflow)",
                    stage.id
                ));
            }

            let broken = self.run_stage(workflow, stage, result).await?;
            if broken {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn run_stage(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<bool> {
        if let Some(before) = &stage.before {
            let _ = self.run_hook(workflow, stage, before, result).await;
        }

        let output = match self.execute_stage(workflow, stage, result).await {
            Ok(v) => v,
            Err(err) => {
                let error_message = err.to_string();
                write_stage_log(&result.session_id, &stage.id, &error_message).await?;
                result.set_error(&stage.id, error_message.clone());

                if let Some(on_error) = &stage.on_error {
                    let _ = self.run_hook(workflow, stage, on_error, result).await;
                }
                if let Some(after) = &stage.after {
                    let _ = self.run_hook(workflow, stage, after, result).await;
                }
                return Err(anyhow!(error_message));
            }
        };

        write_stage_log(&result.session_id, &stage.id, &output).await?;
        result.set_success(&stage.id, output.clone());

        if let Some(path) = &stage.output {
            self.write_output_file(workflow, stage, path, &output)
                .await?;
        }
        if let Some(after) = &stage.after {
            let _ = self.run_hook(workflow, stage, after, result).await;
        }
        if stage.hitl {
            result
                .outputs
                .insert(format!("{}.hitl", stage.id), "skipped".into());
        }

        let broken = match &stage.break_when {
            Some(expr) => {
                let rendered = subst(expr, &workflow.vars, &result.outputs);
                output.contains(&rendered)
            }
            None => false,
        };

        Ok(broken)
    }

    async fn execute_stage(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String, ProviderError> {
        if stage.forks.unwrap_or(0) > 0 {
            return self.execute_forks(workflow, stage, result).await;
        }
        if !stage.each.is_empty() || stage.each_from.is_some() {
            return self.execute_each(workflow, stage, result).await;
        }

        self.execute_with_retry(workflow, stage, &workflow.vars, &result.outputs)
            .await
    }

    async fn execute_forks(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String, ProviderError> {
        let forks = stage.forks.unwrap_or(0);
        if forks == 0 {
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' has invalid forks=0", stage.id),
            ));
        }

        let outputs_snapshot = result.outputs.clone();
        let mut fork_outputs = Vec::with_capacity(forks as usize);

        for i in 0..forks {
            let mut fork_stage = stage.clone();
            fork_stage.forks = None;
            fork_stage.vote = None;
            if !stage.models.is_empty() {
                fork_stage.model = Some(stage.models[(i as usize) % stage.models.len()].clone());
            }

            let out = self
                .execute_with_retry(workflow, &fork_stage, &workflow.vars, &outputs_snapshot)
                .await?;

            let key = format!("{}.{}", stage.id, i);
            result.outputs.insert(key.clone(), out.clone());
            result.success.insert(key.clone(), true);
            let _ = write_stage_log(&result.session_id, &key, &out).await;
            fork_outputs.push(out);
        }

        let joined = fork_outputs.join("\n---\n");
        result
            .outputs
            .insert(format!("{}.all", stage.id), joined.clone());

        if stage.vote.is_some() {
            let voted = self
                .execute_vote(workflow, stage, &fork_outputs, &result.outputs)
                .await?;
            result
                .outputs
                .insert(format!("{}.vote", stage.id), voted.clone());
            return Ok(voted);
        }

        Ok(joined)
    }

    async fn execute_each(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String, ProviderError> {
        let items = if !stage.each.is_empty() {
            stage
                .each
                .iter()
                .map(|v| subst(v, &workflow.vars, &result.outputs))
                .collect::<Vec<_>>()
        } else {
            let src = stage.each_from.as_ref().ok_or_else(|| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!("stage '{}' has each mode without source", stage.id),
                )
            })?;
            let raw = result.outputs.get(src).cloned().unwrap_or_default();
            split_each_items(&raw)
        };

        let outputs_snapshot = result.outputs.clone();
        let mut each_outputs = Vec::with_capacity(items.len());

        for (idx, item) in items.iter().enumerate() {
            let mut each_stage = stage.clone();
            each_stage.each.clear();
            each_stage.each_from = None;
            each_stage.vote = None;

            let mut vars = workflow.vars.clone();
            vars.insert("each".to_string(), item.clone());

            let out = self
                .execute_with_retry(workflow, &each_stage, &vars, &outputs_snapshot)
                .await?;

            let key = format!("{}.{}", stage.id, idx);
            result.outputs.insert(key.clone(), out.clone());
            result.success.insert(key.clone(), true);
            let _ = write_stage_log(&result.session_id, &key, &out).await;
            each_outputs.push(out);
        }

        let joined = each_outputs.join("\n---\n");
        result
            .outputs
            .insert(format!("{}.all", stage.id), joined.clone());

        if stage.vote.is_some() {
            let voted = self
                .execute_vote(workflow, stage, &each_outputs, &result.outputs)
                .await?;
            result
                .outputs
                .insert(format!("{}.vote", stage.id), voted.clone());
            return Ok(voted);
        }

        Ok(joined)
    }

    async fn execute_vote(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        candidates: &[String],
        outputs: &HashMap<String, String>,
    ) -> Result<String, ProviderError> {
        let vote_prompt = stage.vote.as_deref().ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' missing vote prompt", stage.id),
            )
        })?;

        let mut prompt = subst(vote_prompt, &workflow.vars, outputs);
        prompt.push_str("\n\nCandidates:\n");
        for (idx, item) in candidates.iter().enumerate() {
            prompt.push_str(&format!("\n[{}]\n{}\n", idx, item));
        }

        let vote_stage = Stage {
            id: format!("{}.vote", stage.id),
            provider: "llm".to_string(),
            do_prompt: Some(prompt),
            model: stage.model.clone(),
            system: stage.system.clone(),
            ..Default::default()
        };

        self.execute_with_retry(workflow, &vote_stage, &workflow.vars, outputs)
            .await
    }

    async fn execute_with_retry(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
    ) -> Result<String, ProviderError> {
        let timeout = stage.timeout_duration().map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' has invalid timeout: {}", stage.id, err),
            )
        })?;
        let retries = stage.retry.unwrap_or(1).max(1);
        let retry_delay = stage.retry_delay_duration().map_err(|err| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' has invalid retry_delay: {}", stage.id, err),
            )
        })?;

        let provider_name = stage.provider_name();
        let provider = self.providers.get(provider_name).ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!(
                    "unknown provider '{}' in stage '{}'",
                    provider_name, stage.id
                ),
            )
        })?;

        let mut last_error: Option<ProviderError> = None;
        for attempt in 1..=retries {
            match provider.run(stage, workflow, vars, outputs, timeout).await {
                Ok(out) => return Ok(out),
                Err(err) => {
                    last_error = Some(err);
                    if attempt < retries {
                        sleep(retry_delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' failed with unknown provider error", stage.id),
            )
        }))
    }

    async fn run_hook(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        hook_command: &str,
        result: &RunResult,
    ) -> Result<String> {
        let shell = self
            .providers
            .get("shell")
            .ok_or_else(|| anyhow!("shell provider not registered"))?;
        let mut hook_stage = stage.clone();
        hook_stage.exec = Some(hook_command.to_string());
        hook_stage.provider = "shell".to_string();
        let out = shell
            .run(&hook_stage, workflow, &workflow.vars, &result.outputs, None)
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(out)
    }

    async fn write_output_file(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        target: &str,
        output: &str,
    ) -> Result<()> {
        let mut path = PathBuf::from(target);
        if !path.is_absolute() {
            if let Some(stage_wd) = &stage.workdir {
                path = PathBuf::from(stage_wd).join(path);
            } else if let Some(wf_wd) = &workflow.workdir {
                path = PathBuf::from(wf_wd).join(path);
            }
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, output).await?;
        Ok(())
    }
}

fn deps_ok(stage: &Stage, result: &RunResult) -> bool {
    stage
        .needs
        .iter()
        .all(|need| result.success.get(need).copied().unwrap_or(false))
}

fn split_each_items(raw: &str) -> Vec<String> {
    if raw.contains("\n---\n") {
        raw.split("\n---\n")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    } else {
        raw.lines()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Executor, RunConfig};
    use crate::workflow::{Stage, Workflow};

    #[tokio::test]
    async fn forks_populate_indexed_outputs() {
        let wf = Workflow {
            name: "forks-test".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "review".into(),
                provider: "shell".into(),
                exec: Some("echo hi".into()),
                forks: Some(2),
                ..Default::default()
            }],
        };

        let res = Executor::new()
            .run(&wf, RunConfig { max_iterations: Some(1) })
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("review"), Some(&true));
        assert!(res.outputs.contains_key("review.0"));
        assert!(res.outputs.contains_key("review.1"));
        assert!(res.outputs.contains_key("review.all"));
    }

    #[tokio::test]
    async fn each_from_populates_indexed_outputs() {
        let wf = Workflow {
            name: "each-test".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![
                Stage {
                    id: "list".into(),
                    provider: "shell".into(),
                    exec: Some("printf 'a\\nb\\n'".into()),
                    ..Default::default()
                },
                Stage {
                    id: "process".into(),
                    provider: "shell".into(),
                    exec: Some("echo $each".into()),
                    each_from: Some("list".into()),
                    needs: vec!["list".into()],
                    ..Default::default()
                },
            ],
        };

        let res = Executor::new()
            .run(&wf, RunConfig { max_iterations: Some(1) })
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("list"), Some(&true));
        assert_eq!(res.success.get("process"), Some(&true));
        assert!(res.outputs.contains_key("process.0"));
        assert!(res.outputs.contains_key("process.1"));
        assert!(res.outputs.contains_key("process.all"));
    }
}
