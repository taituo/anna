use crate::expr::{eval_when, subst};
use crate::memory::MemoryStore;
use crate::providers::{ProviderError, ProviderRegistry, default_registry};
use crate::result::RunResult;
use crate::session::{add_child_session, init_session_meta, session_dir, write_stage_log};
use crate::workflow::{Stage, Workflow};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::task::JoinSet;
use tokio::time::sleep;

#[derive(Debug, Clone, Default)]
/// Execution options for one workflow run.
pub struct RunConfig {
    pub max_iterations: Option<u32>,
    pub session_id_override: Option<String>,
}

#[derive(Debug, Clone)]
/// Human-in-the-loop decision request payload.
pub struct HitlRequest {
    pub session_id: String,
    pub workflow: String,
    pub stage_id: String,
    pub prompt: Option<String>,
    pub options: Vec<String>,
}

#[async_trait]
/// Callback interface for resolving HITL decisions.
pub trait HitlHandler: Send + Sync {
    async fn await_decision(&self, request: HitlRequest) -> Result<String>;
}

#[derive(Clone, Default)]
/// Orchestrates workflow stage execution with providers and policies.
pub struct Executor {
    providers: ProviderRegistry,
    allowed_providers: Option<HashSet<String>>,
    offline_mode: bool,
    memory: MemoryStore,
    hitl: Option<Arc<dyn HitlHandler>>,
}

enum StageIterationOutcome {
    BreakWorkflow,
    StageComplete,
    ContinueLoop,
}

impl Executor {
    /// Creates a new executor with default providers and env policy.
    pub fn new() -> Self {
        let (allowed_providers, offline_mode) = allowed_providers_from_env();
        Self {
            providers: default_registry(),
            allowed_providers,
            offline_mode,
            memory: MemoryStore::default(),
            hitl: None,
        }
    }

    /// Creates a new executor with a custom memory store.
    pub(crate) fn with_memory_store(memory: MemoryStore) -> Self {
        let (allowed_providers, offline_mode) = allowed_providers_from_env();
        Self {
            providers: default_registry(),
            allowed_providers,
            offline_mode,
            memory,
            hitl: None,
        }
    }

    /// Overrides allowed providers policy.
    pub(crate) fn with_allowed_providers(mut self, providers: Option<HashSet<String>>) -> Self {
        self.allowed_providers = apply_offline_provider_ceiling(
            normalize_allowed_provider_set(providers),
            self.offline_mode,
        );
        self
    }

    /// Returns configured allowlisted providers.
    pub(crate) fn allowed_providers_set(&self) -> Option<HashSet<String>> {
        self.allowed_providers.clone()
    }

    /// Returns whether offline mode is enabled.
    pub(crate) fn offline_mode(&self) -> bool {
        self.offline_mode
    }

    /// Sets the HITL handler used by stages with `hitl: true`.
    pub(crate) fn with_hitl_handler(mut self, hitl: Arc<dyn HitlHandler>) -> Self {
        self.hitl = Some(hitl);
        self
    }

    pub async fn run(&self, workflow: &Workflow, config: RunConfig) -> Result<RunResult> {
        self.run_internal(workflow, config, None).await
    }

    #[async_recursion::async_recursion]
    async fn run_internal(
        &self,
        workflow: &Workflow,
        config: RunConfig,
        parent_id: Option<String>,
    ) -> Result<RunResult> {
        let mut result = RunResult::new();
        if let Some(session_id) = config.session_id_override.clone() {
            result.session_id = session_id.clone();
            result.outputs.insert("SESSION".to_string(), session_id);
        }
        result.parent_id = parent_id;
        init_session_meta(
            &result.session_id,
            &workflow.name,
            result.parent_id.as_deref(),
        )
        .await?;
        let mut iterations = 0_u32;

        loop {
            iterations += 1;
            if workflow.memory {
                self.memory
                    .inject_last_vars(&workflow.name, &mut result.outputs)
                    .await?;
            }
            let should_break = self.run_once(workflow, &mut result).await?;
            if workflow.memory {
                self.memory.save_snapshot(&workflow.name, &result).await?;
            }

            if !workflow.is_continuous() || should_break {
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

            let stage_broke = self.run_stage(workflow, stage, result).await?;
            if stage_broke {
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
        let parallel_stage =
            stage.forks.unwrap_or(0) > 0 || !stage.each.is_empty() || stage.each_from.is_some();
        let (runtime_stage, worktree) = if parallel_stage && stage.worktree.is_some() {
            (stage.clone(), None)
        } else {
            self.prepare_stage_with_worktree(workflow, stage, &result.outputs)
                .await?
        };

        let stage_result = self.execute_stage_loop(workflow, &runtime_stage, result).await;

        if let Some(worktree) = worktree
            && let Err(err) = cleanup_worktree(&worktree).await
        {
            result
                .errors
                .push(format!("stage '{}': {}", runtime_stage.id, err));
        }

        stage_result
    }

    async fn execute_stage_loop(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<bool> {
        let mut iteration = 0_u32;
        let loop_sleep = if stage.loop_stage {
            Some(stage.loop_interval_duration().map_err(|err| {
                anyhow!("stage '{}' has invalid loop interval: {}", stage.id, err)
            })?)
        } else {
            None
        };

        loop {
            iteration += 1;
            let outcome = self
                .run_stage_iteration(workflow, stage, result, iteration)
                .await?;
            match outcome {
                StageIterationOutcome::BreakWorkflow => return Ok(true),
                StageIterationOutcome::StageComplete => return Ok(false),
                StageIterationOutcome::ContinueLoop => {}
            }

            if let Some(delay) = loop_sleep {
                sleep(delay).await;
            }
        }
    }

    async fn run_stage_iteration(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
        iteration: u32,
    ) -> Result<StageIterationOutcome> {
        let output = self
            .execute_stage_with_nonfatal_hooks(workflow, stage, result)
            .await?;
        write_stage_log(&result.session_id, &stage.id, &output).await?;
        result.set_success(&stage.id, output.clone());

        if let Some(path) = &stage.output {
            self.write_output_file(workflow, stage, path, &output).await?;
        }
        self.run_optional_hook_nonfatal(workflow, stage, stage.after.as_deref(), "after", result)
            .await;
        self.handle_hitl_decision(workflow, stage, result).await?;

        if stage_breaks(workflow, stage, &result.outputs, &output) {
            if stage.loop_stage {
                result
                    .outputs
                    .insert(Self::stage_output_key(&stage.id, "iterations"), iteration.to_string());
            }
            return Ok(StageIterationOutcome::BreakWorkflow);
        }
        if !stage.loop_stage {
            return Ok(StageIterationOutcome::StageComplete);
        }
        if let Some(max) = stage.max_iterations
            && iteration >= max.max(1)
        {
            result
                .outputs
                .insert(Self::stage_output_key(&stage.id, "iterations"), iteration.to_string());
            return Ok(StageIterationOutcome::StageComplete);
        }
        Ok(StageIterationOutcome::ContinueLoop)
    }

    async fn execute_stage_with_nonfatal_hooks(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String> {
        self.run_optional_hook_nonfatal(workflow, stage, stage.before.as_deref(), "before", result)
            .await;
        match self.execute_stage(workflow, stage, result).await {
            Ok(output) => Ok(output),
            Err(err) => {
                let error_message = err.to_string();
                write_stage_log(&result.session_id, &stage.id, &error_message).await?;
                result.set_error(&stage.id, error_message.clone());
                self.run_optional_hook_nonfatal(
                    workflow,
                    stage,
                    stage.on_error.as_deref(),
                    "on_error",
                    result,
                )
                .await;
                self.run_optional_hook_nonfatal(
                    workflow,
                    stage,
                    stage.after.as_deref(),
                    "after",
                    result,
                )
                .await;
                Err(anyhow!(error_message))
            }
        }
    }

    async fn handle_hitl_decision(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<()> {
        if !stage.hitl {
            return Ok(());
        }
        let hitl_decision = self.resolve_hitl_decision(workflow, stage, result).await?;
        result
            .outputs
            .insert(Self::stage_output_key(&stage.id, "hitl"), hitl_decision.clone());
        if is_hitl_rejection(&hitl_decision) {
            result.success.insert(stage.id.clone(), false);
            return Err(anyhow!(
                "HITL rejected stage '{}' with decision '{}'",
                stage.id,
                hitl_decision
            ));
        }
        Ok(())
    }

    async fn run_optional_hook_nonfatal(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        hook_command: Option<&str>,
        hook_name: &str,
        result: &mut RunResult,
    ) {
        let Some(command) = hook_command else {
            return;
        };
        if let Err(err) = self.run_hook(workflow, stage, command, result).await {
            result.errors.push(format!(
                "stage '{}': {} hook failed: {}",
                stage.id, hook_name, err
            ));
        }
    }

    async fn prepare_stage_with_worktree(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        outputs: &HashMap<String, String>,
    ) -> Result<(Stage, Option<PreparedWorktree>)> {
        let Some(worktree_ref) = stage.worktree.as_deref() else {
            return Ok((stage.clone(), None));
        };

        let rendered_branch = subst(worktree_ref, &workflow.vars, outputs);
        let branch_name = rendered_branch.trim();
        if branch_name.is_empty() {
            return Err(anyhow!(
                "stage '{}' has empty rendered worktree branch",
                stage.id
            ));
        }

        let Some(repo_root) = resolve_repo_root(workflow, stage).await? else {
            return Err(anyhow!(
                "stage '{}' requested worktree '{}' but no git repository root was found",
                stage.id,
                branch_name
            ));
        };

        let session = outputs
            .get("SESSION")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let dir_name = format!(
            "worktree-{}-{}",
            sanitize_token(&stage.id),
            sanitize_token(branch_name)
        );
        let worktree_path = session_dir(&session).join(dir_name);

        if worktree_path.exists()
            && let Err(err) = tokio::fs::remove_dir_all(&worktree_path).await
        {
            return Err(anyhow!(
                "stage '{}' failed to clear existing worktree '{}': {}",
                stage.id,
                worktree_path.display(),
                err
            ));
        }
        if let Some(parent) = worktree_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        add_worktree(&repo_root, &worktree_path, branch_name)
            .await
            .map_err(|err| {
                anyhow!(
                    "stage '{}' failed to setup worktree '{}': {}",
                    stage.id,
                    branch_name,
                    err
                )
            })?;

        let mut runtime_stage = stage.clone();
        runtime_stage.workdir = Some(worktree_path.display().to_string());
        Ok((
            runtime_stage,
            Some(PreparedWorktree {
                repo_root,
                path: worktree_path,
            }),
        ))
    }

    async fn execute_with_optional_worktree(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        vars: &HashMap<String, String>,
        outputs: &HashMap<String, String>,
    ) -> Result<String, ProviderError> {
        let (runtime_stage, worktree) = self
            .prepare_stage_with_worktree(workflow, stage, outputs)
            .await
            .map_err(|err| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!("stage '{}' worktree setup failed: {}", stage.id, err),
                )
            })?;

        let run = self
            .execute_with_retry(workflow, &runtime_stage, vars, outputs)
            .await;
        let cleanup_error = if let Some(worktree) = worktree {
            cleanup_worktree(&worktree).await.err()
        } else {
            None
        };

        match (run, cleanup_error) {
            (Err(err), _) => Err(err),
            (Ok(_), Some(err)) => Err(ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' worktree cleanup failed: {}", stage.id, err),
            )),
            (Ok(out), None) => Ok(out),
        }
    }

    async fn resolve_hitl_decision(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &RunResult,
    ) -> Result<String> {
        let decision = if let Some(hitl) = &self.hitl {
            hitl.await_decision(HitlRequest {
                session_id: result.session_id.clone(),
                workflow: workflow.name.clone(),
                stage_id: stage.id.clone(),
                prompt: stage.hitl_prompt.clone(),
                options: stage.hitl_options.clone(),
            })
            .await?
        } else {
            "skipped".to_string()
        };

        let normalized = decision.trim().to_string();
        if !stage.hitl_options.is_empty()
            && !stage
                .hitl_options
                .iter()
                .any(|option| option.eq_ignore_ascii_case(&normalized))
        {
            return Err(anyhow!(
                "HITL decision '{}' is not in allowed options for stage '{}'",
                normalized,
                stage.id
            ));
        }
        Ok(normalized)
    }

    async fn execute_stage(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String, ProviderError> {
        if stage.workflow.is_some() {
            return self.execute_subworkflow(workflow, stage, result).await;
        }
        if stage.forks.unwrap_or(0) > 0 {
            return self.execute_forks(workflow, stage, result).await;
        }
        if !stage.each.is_empty() || stage.each_from.is_some() {
            return self.execute_each(workflow, stage, result).await;
        }

        self.execute_with_retry(workflow, stage, &workflow.vars, &result.outputs)
            .await
    }

    async fn execute_subworkflow(
        &self,
        workflow: &Workflow,
        stage: &Stage,
        result: &mut RunResult,
    ) -> Result<String, ProviderError> {
        let workflow_ref = stage.workflow.as_deref().ok_or_else(|| {
            ProviderError::new(
                "provider_exec_failed",
                format!("stage '{}' missing workflow path", stage.id),
            )
        })?;
        let rendered = subst(workflow_ref, &workflow.vars, &result.outputs);
        let resolved_path = resolve_subworkflow_path(workflow, stage, &rendered);

        let mut child_wf = match Workflow::load(&resolved_path) {
            Ok(v) => v,
            Err(err) => {
                return Ok(format!(
                    "SUBWORKFLOW_ERROR: failed to load '{}': {}",
                    resolved_path.display(),
                    err
                ));
            }
        };

        let mut child_vars = workflow.vars.clone();
        child_vars.extend(child_wf.vars.clone());
        for (k, v) in &stage.vars {
            child_vars.insert(k.to_owned(), subst(v, &workflow.vars, &result.outputs));
        }
        child_wf.vars = child_vars;
        if child_wf.workdir.is_none() {
            child_wf.workdir = stage.workdir.clone().or_else(|| workflow.workdir.clone());
        }

        match self
            .run_internal(
                &child_wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
                Some(result.session_id.clone()),
            )
            .await
        {
            Ok(child_result) => {
                result.add_child(child_result.session_id.clone());
                if let Err(err) =
                    add_child_session(&result.session_id, &child_result.session_id, &workflow.name)
                        .await
                {
                    result.errors.push(format!(
                        "session-meta: failed linking child '{}' -> '{}': {}",
                        result.session_id, child_result.session_id, err
                    ));
                }
                let mut subworkflow_output = String::new();
                if let Some(last_stage) = child_wf.stages.last()
                    && let Some(v) = child_result.outputs.get(&last_stage.id)
                {
                    subworkflow_output = v.clone();
                }
                if subworkflow_output.is_empty() {
                    subworkflow_output = format!("sub-workflow completed: {}", child_wf.name);
                }
                Ok(subworkflow_output)
            }
            Err(err) => Ok(format!(
                "SUBWORKFLOW_ERROR: workflow '{}' failed: {}",
                child_wf.name, err
            )),
        }
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
        let mut join_set = JoinSet::new();
        let workflow_snapshot = workflow.clone();
        for i in 0..(forks as usize) {
            let mut fork_stage = stage.to_owned();
            fork_stage.forks = None;
            fork_stage.vote = None;
            if !stage.models.is_empty() {
                fork_stage.model = Some(stage.models[i % stage.models.len()].to_owned());
            }
            if let Some(base_worktree) = stage.worktree.as_ref() {
                fork_stage.worktree = Some(format!("{}-fork-{}", base_worktree, i));
                fork_stage.workdir = None;
            }
            let fork_worker = self.to_owned();
            let workflow_for_task = workflow_snapshot.to_owned();
            let vars_for_task = workflow.vars.to_owned();
            let outputs_for_task = outputs_snapshot.to_owned();
            join_set.spawn(async move {
                let fork_output = fork_worker
                    .execute_with_optional_worktree(
                        &workflow_for_task,
                        &fork_stage,
                        &vars_for_task,
                        &outputs_for_task,
                    )
                    .await;
                (i, fork_output)
            });
        }

        let mut slots: Vec<Option<String>> = vec![None; forks as usize];
        self.collect_parallel_results(stage, &mut join_set, &mut slots, "fork")
            .await?;
        let fork_outputs = self
            .store_parallel_outputs(stage, result, slots, "fork")
            .await?;

        let joined_outputs = fork_outputs.join("\n---\n");
        result
            .outputs
            .insert(Self::stage_output_key(&stage.id, "all"), joined_outputs.clone());

        if stage.vote.is_some() {
            let voted_output = self
                .execute_vote(workflow, stage, &fork_outputs, &result.outputs)
                .await?;
            result
                .outputs
                .insert(Self::stage_output_key(&stage.id, "vote"), voted_output.clone());
            return Ok(voted_output);
        }

        Ok(joined_outputs)
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

        let each_outputs_snapshot = result.outputs.clone();
        let mut each_join_set = JoinSet::new();
        let each_workflow_snapshot = workflow.clone();
        for (idx, item) in items.iter().enumerate() {
            let mut each_stage = stage.to_owned();
            each_stage.each.clear();
            each_stage.each_from = None;
            each_stage.vote = None;
            if let Some(base_worktree) = stage.worktree.as_ref() {
                each_stage.worktree = Some(format!("{}-each-{}", base_worktree, idx));
                each_stage.workdir = None;
            }

            let mut each_vars = workflow.vars.to_owned();
            each_vars.insert("each".to_string(), item.to_owned());
            let each_worker = self.to_owned();
            let each_workflow_for_task = each_workflow_snapshot.to_owned();
            let each_outputs_for_task = each_outputs_snapshot.to_owned();
            each_join_set.spawn(async move {
                let each_output = each_worker
                    .execute_with_optional_worktree(
                        &each_workflow_for_task,
                        &each_stage,
                        &each_vars,
                        &each_outputs_for_task,
                    )
                    .await;
                (idx, each_output)
            });
        }

        let mut each_slots: Vec<Option<String>> = vec![None; items.len()];
        self.collect_parallel_results(stage, &mut each_join_set, &mut each_slots, "each")
            .await?;
        let each_outputs = self
            .store_parallel_outputs(stage, result, each_slots, "each")
            .await?;

        let each_joined = each_outputs.join("\n---\n");
        result
            .outputs
            .insert(Self::stage_output_key(&stage.id, "all"), each_joined.clone());

        if stage.vote.is_some() {
            let voted_each_output = self
                .execute_vote(workflow, stage, &each_outputs, &result.outputs)
                .await?;
            result
                .outputs
                .insert(Self::stage_output_key(&stage.id, "vote"), voted_each_output.clone());
            return Ok(voted_each_output);
        }

        Ok(each_joined)
    }

    async fn collect_parallel_results(
        &self,
        stage: &Stage,
        join_set: &mut JoinSet<(usize, Result<String, ProviderError>)>,
        slots: &mut [Option<String>],
        task_kind: &str,
    ) -> Result<(), ProviderError> {
        let mut first_error: Option<ProviderError> = None;
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((idx, Ok(out))) => slots[idx] = Some(out),
                Ok((_, Err(err))) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    join_set.abort_all();
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(ProviderError::new(
                            "provider_exec_failed",
                            format!("{} task failed in stage '{}': {}", task_kind, stage.id, err),
                        ));
                    }
                    join_set.abort_all();
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn store_parallel_outputs(
        &self,
        stage: &Stage,
        result: &mut RunResult,
        slots: Vec<Option<String>>,
        task_kind: &str,
    ) -> Result<Vec<String>, ProviderError> {
        let mut collected = Vec::with_capacity(slots.len());
        for (idx, value) in slots.into_iter().enumerate() {
            let out = value.ok_or_else(|| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "{} item {} produced no output in stage '{}'",
                        task_kind, idx, stage.id
                    ),
                )
            })?;
            let stage_item_key = format!("{}.{}", stage.id, idx);
            result
                .outputs
                .insert(stage_item_key.to_string(), out.to_string());
            result.success.insert(stage_item_key.to_string(), true);
            if let Err(err) = write_stage_log(&result.session_id, &stage_item_key, &out).await {
                result.errors.push(format!(
                    "stage '{}': failed writing {} log '{}': {}",
                    stage.id, task_kind, stage_item_key, err
                ));
            }
            collected.push(out);
        }
        Ok(collected)
    }

fn stage_output_key(stage_id: &str, suffix: &str) -> String {
    let mut key = stage_id.to_string();
    key.push('.');
    key.push_str(suffix);
    key
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
        if !provider_allowed(provider_name, &self.allowed_providers) {
            let allowed = self
                .allowed_providers
                .as_ref()
                .map(allowed_providers_display)
                .unwrap_or_else(|| "*".to_string());
            return Err(ProviderError::new(
                "provider_exec_failed",
                format!(
                    "provider '{}' is blocked in stage '{}' (allowed providers: {})",
                    provider_name, stage.id, allowed
                ),
            ));
        }

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
        let hook_output = shell
            .run(&hook_stage, workflow, &workflow.vars, &result.outputs, None)
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok(hook_output)
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

#[derive(Debug, Clone)]
struct PreparedWorktree {
    repo_root: PathBuf,
    path: PathBuf,
}

async fn resolve_repo_root(workflow: &Workflow, stage: &Stage) -> Result<Option<PathBuf>> {
    let mut candidates = Vec::new();
    if let Some(stage_wd) = stage.workdir.as_ref().filter(|v| !v.trim().is_empty()) {
        candidates.push(PathBuf::from(stage_wd));
    }
    if let Some(wf_wd) = workflow.workdir.as_ref().filter(|v| !v.trim().is_empty()) {
        candidates.push(PathBuf::from(wf_wd));
    }
    if let Some(source) = &workflow.source_path
        && let Some(parent) = source.parent()
    {
        candidates.push(parent.to_path_buf());
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd);
    }

    for base in candidates {
        let mut git_cmd = Command::new("git");
        git_cmd
            .arg("-C")
            .arg(&base)
            .arg("rev-parse")
            .arg("--show-toplevel")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        let git_output = match git_cmd.output().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !git_output.status.success() {
            continue;
        }
        let root = String::from_utf8_lossy(&git_output.stdout).trim().to_string();
        if root.is_empty() {
            continue;
        }
        return Ok(Some(PathBuf::from(root)));
    }

    Ok(None)
}

async fn add_worktree(repo_root: &Path, worktree_path: &Path, branch: &str) -> Result<()> {
    let primary = run_git_worktree_add(repo_root, worktree_path, branch, true).await;
    if primary.is_ok() {
        return Ok(());
    }

    run_git_worktree_add(repo_root, worktree_path, branch, false).await?;
    Ok(())
}

async fn run_git_worktree_add(
    repo_root: &Path,
    worktree_path: &Path,
    branch: &str,
    create_branch: bool,
) -> Result<()> {
    let mut git_add_cmd = Command::new("git");
    git_add_cmd
        .arg("-C")
        .arg(repo_root)
        .arg("worktree")
        .arg("add");
    if create_branch {
        git_add_cmd.arg("-b").arg(branch).arg(worktree_path);
    } else {
        git_add_cmd.arg(worktree_path).arg(branch);
    }
    git_add_cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let add_output = git_add_cmd.output().await?;
    if add_output.status.success() {
        return Ok(());
    }

    let add_stderr = String::from_utf8_lossy(&add_output.stderr).trim().to_string();
    let add_stdout = String::from_utf8_lossy(&add_output.stdout).trim().to_string();
    let add_error_message = if add_stderr.is_empty() {
        add_stdout
    } else {
        add_stderr
    };
    Err(anyhow!(
        "git worktree add failed (repo='{}', branch='{}'): {}",
        repo_root.display(),
        branch,
        add_error_message
    ))
}

async fn cleanup_worktree(worktree: &PreparedWorktree) -> Result<()> {
    let mut git_remove_cmd = Command::new("git");
    git_remove_cmd
        .arg("-C")
        .arg(&worktree.repo_root)
        .arg("worktree")
        .arg("remove")
        .arg("--force")
        .arg(&worktree.path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let remove_output = git_remove_cmd.output().await?;
    if remove_output.status.success() {
        return Ok(());
    }

    let remove_stderr = String::from_utf8_lossy(&remove_output.stderr)
        .trim()
        .to_string();
    let remove_stdout = String::from_utf8_lossy(&remove_output.stdout)
        .trim()
        .to_string();
    let remove_error_message = if remove_stderr.is_empty() {
        remove_stdout
    } else {
        remove_stderr
    };
    Err(anyhow!(
        "git worktree remove failed (repo='{}', path='{}'): {}",
        worktree.repo_root.display(),
        worktree.path.display(),
        remove_error_message
    ))
}

fn sanitize_token(input: &str) -> String {
    let sanitized = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.trim_matches('-').is_empty() {
        "x".to_string()
    } else {
        sanitized
    }
}

fn is_hitl_rejection(decision: &str) -> bool {
    matches!(
        decision.trim().to_ascii_lowercase().as_str(),
        "reject" | "rejected" | "deny" | "denied" | "stop" | "abort" | "no" | "false"
    )
}

fn stage_breaks(
    workflow: &Workflow,
    stage: &Stage,
    outputs: &HashMap<String, String>,
    output: &str,
) -> bool {
    let Some(expr) = stage.break_when.as_deref() else {
        return false;
    };
    let break_expr_rendered = subst(expr, &workflow.vars, outputs);
    output.contains(&break_expr_rendered)
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

fn resolve_subworkflow_path(workflow: &Workflow, stage: &Stage, target: &str) -> PathBuf {
    let target_path = PathBuf::from(target);
    if target_path.is_absolute() {
        return target_path;
    }

    if let Some(stage_wd) = &stage.workdir {
        return Path::new(stage_wd).join(target_path);
    }
    if let Some(wf_wd) = &workflow.workdir {
        return Path::new(wf_wd).join(target_path);
    }
    if let Some(source) = &workflow.source_path
        && let Some(parent) = source.parent()
    {
        return parent.join(target_path);
    }
    target_path
}

const OFFLINE_PROVIDER_ALLOWLIST: [&str; 3] = ["shell", "cli", "vault"];

fn allowed_providers_from_env() -> (Option<HashSet<String>>, bool) {
    let offline_mode = offline_mode_enabled();
    let parsed_allowed =
        parse_allowed_providers(std::env::var("ANNA_ALLOWED_PROVIDERS").ok().as_deref());
    (
        apply_offline_provider_ceiling(parsed_allowed, offline_mode),
        offline_mode,
    )
}

fn offline_mode_enabled() -> bool {
    std::env::var("ANNA_OFFLINE_MODE")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn apply_offline_provider_ceiling(
    providers: Option<HashSet<String>>,
    offline_mode: bool,
) -> Option<HashSet<String>> {
    if !offline_mode {
        return providers;
    }
    let offline_allowlist = OFFLINE_PROVIDER_ALLOWLIST
        .into_iter()
        .map(|v| v.to_string())
        .collect::<HashSet<_>>();
    match providers {
        None => Some(offline_allowlist),
        Some(mut set) => {
            set.retain(|provider| offline_allowlist.contains(provider));
            Some(set)
        }
    }
}

fn normalize_allowed_provider_set(providers: Option<HashSet<String>>) -> Option<HashSet<String>> {
    let Some(providers) = providers else {
        return None;
    };
    let mut normalized_set = HashSet::new();
    for raw in providers {
        let normalized_provider_name = raw.trim().to_ascii_lowercase();
        if normalized_provider_name.is_empty() {
            continue;
        }
        if normalized_provider_name == "*" || normalized_provider_name == "all" {
            return None;
        }
        normalized_set.insert(normalized_provider_name);
    }
    if normalized_set.is_empty() {
        return None;
    }
    Some(normalized_set)
}

fn parse_allowed_providers(raw: Option<&str>) -> Option<HashSet<String>> {
    let raw_value = raw.map(str::trim).filter(|v| !v.is_empty())?;
    let mut allowed_set = HashSet::new();
    for token in raw_value.split(',') {
        let parsed_provider = token.trim().to_ascii_lowercase();
        if parsed_provider.is_empty() {
            continue;
        }
        if parsed_provider == "*" || parsed_provider == "all" {
            return None;
        }
        allowed_set.insert(parsed_provider);
    }
    if allowed_set.is_empty() {
        return None;
    }
    Some(allowed_set)
}

fn provider_allowed(provider_name: &str, allowed: &Option<HashSet<String>>) -> bool {
    match allowed {
        None => true,
        Some(set) => set.contains(&provider_name.to_ascii_lowercase()),
    }
}

fn allowed_providers_display(allowed: &HashSet<String>) -> String {
    let mut names = Vec::with_capacity(allowed.len());
    for name in allowed {
        names.push(name.to_owned());
    }
    names.sort();
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Executor, HitlHandler, HitlRequest, RunConfig, apply_offline_provider_ceiling,
        parse_allowed_providers, sanitize_token,
    };
    use crate::memory::MemoryStore;
    use crate::session::session_dir;
    use crate::workflow::{Stage, Workflow};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct FixedHitl {
        decision: String,
    }

    #[async_trait]
    impl HitlHandler for FixedHitl {
        async fn await_decision(&self, _request: HitlRequest) -> anyhow::Result<String> {
            Ok(self.decision.clone())
        }
    }

    #[tokio::test]
    async fn forks_populate_indexed_outputs() {
        let forks_workflow = Workflow {
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
            source_path: None,
        };

        let forks_result = Executor::new()
            .run(
                &forks_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(forks_result.success.get("review"), Some(&true));
        assert!(forks_result.outputs.contains_key("review.0"));
        assert!(forks_result.outputs.contains_key("review.1"));
        assert!(forks_result.outputs.contains_key("review.all"));
    }

    #[tokio::test]
    async fn each_from_populates_indexed_outputs() {
        let each_workflow = Workflow {
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
            source_path: None,
        };

        let each_result = Executor::new()
            .run(
                &each_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(each_result.success.get("list"), Some(&true));
        assert_eq!(each_result.success.get("process"), Some(&true));
        assert!(each_result.outputs.contains_key("process.0"));
        assert!(each_result.outputs.contains_key("process.1"));
        assert!(each_result.outputs.contains_key("process.all"));
    }

    #[tokio::test]
    async fn forks_run_concurrently() {
        let forks_concurrent_workflow = Workflow {
            name: "forks-concurrent".into(),
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
                exec: Some("sleep 0.4; echo done".into()),
                forks: Some(5),
                ..Default::default()
            }],
            source_path: None,
        };

        let started_at = Instant::now();
        let forks_concurrent_result = Executor::new()
            .run(
                &forks_concurrent_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");
        let elapsed_time = started_at.elapsed();

        assert_eq!(forks_concurrent_result.success.get("review"), Some(&true));
        assert!(
            elapsed_time < Duration::from_millis(1600),
            "forks should run concurrently; elapsed={:?}",
            elapsed_time
        );
    }

    #[tokio::test]
    async fn each_runs_concurrently() {
        let each_concurrent_workflow = Workflow {
            name: "each-concurrent".into(),
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
                    exec: Some("printf 'a\\nb\\nc\\nd\\ne\\n'".into()),
                    ..Default::default()
                },
                Stage {
                    id: "process".into(),
                    provider: "shell".into(),
                    exec: Some("sleep 0.4; echo $each".into()),
                    each_from: Some("list".into()),
                    needs: vec!["list".into()],
                    ..Default::default()
                },
            ],
            source_path: None,
        };

        let each_started_at = Instant::now();
        let each_concurrent_result = Executor::new()
            .run(
                &each_concurrent_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");
        let each_elapsed = each_started_at.elapsed();

        assert_eq!(each_concurrent_result.success.get("process"), Some(&true));
        assert!(
            each_elapsed < Duration::from_millis(1600),
            "each should run concurrently; elapsed={:?}",
            each_elapsed
        );
    }

    #[tokio::test]
    async fn subworkflow_uses_last_stage_output() {
        let base = std::env::temp_dir().join(format!(
            "anna-subwf-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base)
            .await
            .expect("create temp dir");
        let parent_path = base.join("parent.anna");
        let child_path = base.join("child.anna");

        tokio::fs::write(
            &child_path,
            "name: child\nstages:\n  - id: done\n    provider: shell\n    exec: \"echo child-ok\"\n",
        )
        .await
        .expect("write child workflow");
        tokio::fs::write(&parent_path, "name: parent\nstages: []\n")
            .await
            .expect("write parent placeholder");

        let subworkflow_parent = Workflow {
            name: "parent".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(base.display().to_string()),
            trigger: Default::default(),
            stages: vec![Stage {
                id: "call".into(),
                workflow: Some("child.anna".into()),
                ..Default::default()
            }],
            source_path: Some(parent_path),
        };

        let subworkflow_result = Executor::new()
            .run(
                &subworkflow_parent,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(subworkflow_result.success.get("call"), Some(&true));
        assert_eq!(
            subworkflow_result.outputs.get("call"),
            Some(&"child-ok".to_string())
        );
        assert_eq!(subworkflow_result.children.len(), 1);
    }

    #[tokio::test]
    async fn memory_injects_previous_stage_output() {
        let mem_root = std::env::temp_dir().join(format!(
            "anna-mem-int-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let memory_executor = Executor::with_memory_store(MemoryStore::new(mem_root, 10));

        let wf1 = Workflow {
            name: "memory-wf".into(),
            mode: "once".into(),
            memory: true,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "save".into(),
                provider: "shell".into(),
                exec: Some("echo first".into()),
                ..Default::default()
            }],
            source_path: None,
        };
        memory_executor.run(
            &wf1,
            RunConfig {
                max_iterations: Some(1),
                session_id_override: None,
            },
        )
        .await
        .expect("first run should succeed");

        let wf2 = Workflow {
            name: "memory-wf".into(),
            mode: "once".into(),
            memory: true,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "use_mem".into(),
                provider: "shell".into(),
                exec: Some("echo prev:$memory.save".into()),
                ..Default::default()
            }],
            source_path: None,
        };
        let memory_result = memory_executor
            .run(
                &wf2,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("second run should succeed");

        assert_eq!(memory_result.success.get("use_mem"), Some(&true));
        assert_eq!(
            memory_result.outputs.get("use_mem"),
            Some(&"prev:first".to_string())
        );
    }

    #[tokio::test]
    async fn hitl_decision_is_recorded() {
        let hitl_ok_workflow = Workflow {
            name: "hitl-ok".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "approve_me".into(),
                provider: "shell".into(),
                exec: Some("echo done".into()),
                hitl: true,
                hitl_options: vec!["approve".into(), "reject".into()],
                ..Default::default()
            }],
            source_path: None,
        };

        let hitl_ok_executor = Executor::new().with_hitl_handler(Arc::new(FixedHitl {
            decision: "approve".to_string(),
        }));
        let hitl_ok_result = hitl_ok_executor
            .run(
                &hitl_ok_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(hitl_ok_result.success.get("approve_me"), Some(&true));
        assert_eq!(
            hitl_ok_result.outputs.get("approve_me.hitl"),
            Some(&"approve".to_string())
        );
    }

    #[tokio::test]
    async fn hitl_reject_fails_workflow() {
        let hitl_reject_workflow = Workflow {
            name: "hitl-reject".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "must_approve".into(),
                provider: "shell".into(),
                exec: Some("echo done".into()),
                hitl: true,
                hitl_options: vec!["approve".into(), "reject".into()],
                ..Default::default()
            }],
            source_path: None,
        };

        let hitl_reject_executor = Executor::new().with_hitl_handler(Arc::new(FixedHitl {
            decision: "reject".to_string(),
        }));
        let hitl_reject_err = hitl_reject_executor
            .run(
                &hitl_reject_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect_err("workflow should fail on hitl rejection");
        assert!(hitl_reject_err.to_string().contains("HITL rejected"));
    }

    #[tokio::test]
    async fn stage_worktree_runs_in_isolated_checkout_and_cleans_up() {
        let repo = std::env::temp_dir().join(format!(
            "anna-worktree-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&repo)
            .await
            .expect("create temp repo");
        tokio::fs::write(repo.join("README.md"), "init\n")
            .await
            .expect("write init file");

        let git_init_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("init")
            .arg("-b")
            .arg("main")
            .status()
            .expect("git init should run");
        assert!(git_init_status.success(), "git init should succeed");

        let git_add_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("add")
            .arg("README.md")
            .status()
            .expect("git add should run");
        assert!(git_add_status.success(), "git add should succeed");

        let git_commit_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("-c")
            .arg("user.email=test@example.com")
            .arg("-c")
            .arg("user.name=Anna Test")
            .arg("commit")
            .arg("-m")
            .arg("init")
            .status()
            .expect("git commit should run");
        assert!(git_commit_status.success(), "git commit should succeed");

        let branch = format!("feat-{}", rand::random::<u16>());
        let worktree_workflow = Workflow {
            name: "worktree-test".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(repo.display().to_string()),
            trigger: Default::default(),
            stages: vec![Stage {
                id: "wt".into(),
                provider: "shell".into(),
                exec: Some("pwd".into()),
                worktree: Some(branch.clone()),
                ..Default::default()
            }],
            source_path: None,
        };

        let worktree_result = Executor::new()
            .run(
                &worktree_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(worktree_result.success.get("wt"), Some(&true));
        let worktree_output = worktree_result.outputs.get("wt").cloned().unwrap_or_default();
        let expected_name = format!(
            "worktree-{}-{}",
            sanitize_token("wt"),
            sanitize_token(&branch)
        );
        assert!(
            worktree_output.contains(&expected_name),
            "pwd output should include worktree dir name '{}', got '{}'",
            expected_name,
            worktree_output
        );

        let cleaned_worktree_path = session_dir(&worktree_result.session_id).join(expected_name);
        assert!(
            !cleaned_worktree_path.exists(),
            "worktree path should be cleaned up: {}",
            cleaned_worktree_path.display()
        );
    }

    #[tokio::test]
    async fn forks_with_worktree_use_distinct_paths() {
        let forks_repo = std::env::temp_dir().join(format!(
            "anna-worktree-forks-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&forks_repo)
            .await
            .expect("create temp repo");
        tokio::fs::write(forks_repo.join("README.md"), "init\n")
            .await
            .expect("write init file");

        let forks_git_init_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&forks_repo)
            .arg("init")
            .arg("-b")
            .arg("main")
            .status()
            .expect("git init should run");
        assert!(forks_git_init_status.success(), "git init should succeed");

        let forks_git_add_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&forks_repo)
            .arg("add")
            .arg("README.md")
            .status()
            .expect("git add should run");
        assert!(forks_git_add_status.success(), "git add should succeed");

        let forks_git_commit_status = std::process::Command::new("git")
            .arg("-C")
            .arg(&forks_repo)
            .arg("-c")
            .arg("user.email=test@example.com")
            .arg("-c")
            .arg("user.name=Anna Test")
            .arg("commit")
            .arg("-m")
            .arg("init")
            .status()
            .expect("git commit should run");
        assert!(forks_git_commit_status.success(), "git commit should succeed");

        let forks_worktree_workflow = Workflow {
            name: "worktree-forks-test".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(forks_repo.display().to_string()),
            trigger: Default::default(),
            stages: vec![Stage {
                id: "wtf".into(),
                provider: "shell".into(),
                exec: Some("pwd".into()),
                forks: Some(2),
                worktree: Some("feat".into()),
                ..Default::default()
            }],
            source_path: None,
        };

        let forks_worktree_result = Executor::new()
            .run(
                &forks_worktree_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        let fork_out0 = forks_worktree_result
            .outputs
            .get("wtf.0")
            .cloned()
            .unwrap_or_default();
        let fork_out1 = forks_worktree_result
            .outputs
            .get("wtf.1")
            .cloned()
            .unwrap_or_default();
        assert_ne!(fork_out0, fork_out1, "fork worktrees should be distinct");

        let expected0 = format!(
            "worktree-{}-{}",
            sanitize_token("wtf"),
            sanitize_token("feat-fork-0")
        );
        let expected1 = format!(
            "worktree-{}-{}",
            sanitize_token("wtf"),
            sanitize_token("feat-fork-1")
        );
        assert!(
            fork_out0.contains(&expected0),
            "fork 0 path mismatch: {}",
            fork_out0
        );
        assert!(
            fork_out1.contains(&expected1),
            "fork 1 path mismatch: {}",
            fork_out1
        );

        let path0 = session_dir(&forks_worktree_result.session_id).join(expected0);
        let path1 = session_dir(&forks_worktree_result.session_id).join(expected1);
        assert!(!path0.exists(), "fork 0 worktree should be cleaned");
        assert!(!path1.exists(), "fork 1 worktree should be cleaned");
    }

    #[tokio::test]
    async fn stage_loop_respects_max_iterations() {
        let loop_workdir = std::env::temp_dir().join(format!(
            "anna-stage-loop-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&loop_workdir)
            .await
            .expect("create temp workdir");

        let max_iterations_workflow = Workflow {
            name: "stage-loop-max".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(loop_workdir.display().to_string()),
            trigger: Default::default(),
            stages: vec![Stage {
                id: "looped".into(),
                provider: "shell".into(),
                exec: Some(
                    "n=$(cat loop-counter.txt 2>/dev/null || echo 0); n=$((n+1)); echo \"$n\" > loop-counter.txt; echo \"$n\""
                        .into(),
                ),
                loop_stage: true,
                interval: Some("1ms".into()),
                max_iterations: Some(3),
                ..Default::default()
            }],
            source_path: None,
        };

        let max_iterations_result = Executor::new()
            .run(
                &max_iterations_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(max_iterations_result.success.get("looped"), Some(&true));
        assert_eq!(
            max_iterations_result.outputs.get("looped"),
            Some(&"3".to_string())
        );
        assert_eq!(
            max_iterations_result.outputs.get("looped.iterations"),
            Some(&"3".to_string())
        );
    }

    #[tokio::test]
    async fn stage_loop_break_when_stops_workflow_progress() {
        let break_workdir = std::env::temp_dir().join(format!(
            "anna-stage-break-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&break_workdir)
            .await
            .expect("create temp workdir");

        let break_when_workflow = Workflow {
            name: "stage-loop-break".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(break_workdir.display().to_string()),
            trigger: Default::default(),
            stages: vec![
                Stage {
                    id: "test".into(),
                    provider: "shell".into(),
                    exec: Some(
                        "n=$(cat break-counter.txt 2>/dev/null || echo 0); n=$((n+1)); echo \"$n\" > break-counter.txt; echo \"RUN-$n\""
                            .into(),
                    ),
                    loop_stage: true,
                    interval: Some("1ms".into()),
                    max_iterations: Some(10),
                    break_when: Some("RUN-2".into()),
                    ..Default::default()
                },
                Stage {
                    id: "after".into(),
                    provider: "shell".into(),
                    exec: Some("echo SHOULD_NOT_RUN".into()),
                    needs: vec!["test".into()],
                    ..Default::default()
                },
            ],
            source_path: None,
        };

        let break_when_result = Executor::new()
            .run(
                &break_when_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(break_when_result.success.get("test"), Some(&true));
        assert_eq!(break_when_result.outputs.get("test"), Some(&"RUN-2".to_string()));
        assert_eq!(
            break_when_result.outputs.get("test.iterations"),
            Some(&"2".to_string())
        );
        assert_eq!(break_when_result.success.get("after"), None);
        assert_eq!(break_when_result.outputs.get("after"), None);
    }

    #[test]
    fn parse_allowed_providers_handles_case_and_wildcards() {
        let parsed =
            parse_allowed_providers(Some(" shell,HTTP , cli,shell ")).expect("policy should parse");
        assert!(parsed.contains("shell"));
        assert!(parsed.contains("http"));
        assert!(parsed.contains("cli"));
        assert_eq!(parsed.len(), 3);

        assert!(parse_allowed_providers(Some("*")).is_none());
        assert!(parse_allowed_providers(Some("all,http")).is_none());
    }

    #[test]
    fn offline_ceiling_defaults_to_deterministic_providers() {
        let offline_allowed = apply_offline_provider_ceiling(None, true)
            .expect("offline mode should always set provider ceiling");
        assert_eq!(offline_allowed.len(), 3);
        assert!(offline_allowed.contains("shell"));
        assert!(offline_allowed.contains("cli"));
        assert!(offline_allowed.contains("vault"));
    }

    #[test]
    fn offline_ceiling_intersects_explicit_allowlist() {
        let intersected_allowed = apply_offline_provider_ceiling(
            Some(HashSet::from(["shell".to_string(), "http".to_string()])),
            true,
        )
        .expect("offline mode should always set provider ceiling");
        assert_eq!(intersected_allowed, HashSet::from(["shell".to_string()]));

        let empty_intersection =
            apply_offline_provider_ceiling(Some(HashSet::from(["http".to_string()])), true)
            .expect("offline mode should always set provider ceiling");
        assert!(empty_intersection.is_empty());
    }

    #[tokio::test]
    async fn blocked_provider_fails_stage() {
        let blocked_provider_workflow = Workflow {
            name: "blocked-provider".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: None,
            trigger: Default::default(),
            stages: vec![Stage {
                id: "s1".into(),
                provider: "shell".into(),
                exec: Some("echo hi".into()),
                ..Default::default()
            }],
            source_path: None,
        };

        let blocked_provider_executor = Executor::new()
            .with_allowed_providers(Some(HashSet::from(["http".to_string(), "cli".to_string()])));
        let blocked_provider_err = blocked_provider_executor
            .run(
                &blocked_provider_workflow,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect_err("blocked provider should fail");
        let blocked_message = blocked_provider_err.to_string();
        assert!(blocked_message.contains("provider 'shell' is blocked"));
        assert!(blocked_message.contains("allowed providers: cli,http"));
    }
}
