use crate::expr::{eval_when, subst};
use crate::memory::MemoryStore;
use crate::providers::{ProviderError, ProviderRegistry, default_registry};
use crate::result::RunResult;
use crate::session::{add_child_session, init_session_meta, session_dir, write_stage_log};
use crate::workflow::{Stage, Workflow};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;
use tokio::task::JoinSet;
use tokio::time::sleep;

#[derive(Debug, Clone, Default)]
pub struct RunConfig {
    pub max_iterations: Option<u32>,
    pub session_id_override: Option<String>,
}

#[derive(Clone, Default)]
pub struct Executor {
    providers: ProviderRegistry,
    memory: MemoryStore,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            providers: default_registry(),
            memory: MemoryStore::default(),
        }
    }

    pub fn with_memory_store(memory: MemoryStore) -> Self {
        Self {
            providers: default_registry(),
            memory,
        }
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
            let broken = self.run_once(workflow, &mut result).await?;
            if workflow.memory {
                self.memory.save_snapshot(&workflow.name, &result).await?;
            }

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
        let (runtime_stage, worktree) = self
            .prepare_stage_with_worktree(workflow, stage, &result.outputs)
            .await?;

        let stage_result = async {
            let mut iteration = 0_u32;
            let loop_sleep = if runtime_stage.loop_stage {
                Some(runtime_stage.loop_interval_duration().map_err(|err| {
                    anyhow!(
                        "stage '{}' has invalid loop interval: {}",
                        runtime_stage.id,
                        err
                    )
                })?)
            } else {
                None
            };

            loop {
                iteration += 1;
                if let Some(before) = &runtime_stage.before {
                    let _ = self
                        .run_hook(workflow, &runtime_stage, before, result)
                        .await;
                }

                let output = match self.execute_stage(workflow, &runtime_stage, result).await {
                    Ok(v) => v,
                    Err(err) => {
                        let error_message = err.to_string();
                        write_stage_log(&result.session_id, &runtime_stage.id, &error_message)
                            .await?;
                        result.set_error(&runtime_stage.id, error_message.clone());

                        if let Some(on_error) = &runtime_stage.on_error {
                            let _ = self
                                .run_hook(workflow, &runtime_stage, on_error, result)
                                .await;
                        }
                        if let Some(after) = &runtime_stage.after {
                            let _ = self.run_hook(workflow, &runtime_stage, after, result).await;
                        }
                        return Err(anyhow!(error_message));
                    }
                };

                write_stage_log(&result.session_id, &runtime_stage.id, &output).await?;
                result.set_success(&runtime_stage.id, output.clone());

                if let Some(path) = &runtime_stage.output {
                    self.write_output_file(workflow, &runtime_stage, path, &output)
                        .await?;
                }
                if let Some(after) = &runtime_stage.after {
                    let _ = self.run_hook(workflow, &runtime_stage, after, result).await;
                }
                if runtime_stage.hitl {
                    result
                        .outputs
                        .insert(format!("{}.hitl", runtime_stage.id), "skipped".into());
                }

                let broken = match &runtime_stage.break_when {
                    Some(expr) => {
                        let rendered = subst(expr, &workflow.vars, &result.outputs);
                        output.contains(&rendered)
                    }
                    None => false,
                };
                if broken {
                    if runtime_stage.loop_stage {
                        result.outputs.insert(
                            format!("{}.iterations", runtime_stage.id),
                            iteration.to_string(),
                        );
                    }
                    return Ok(true);
                }

                if !runtime_stage.loop_stage {
                    return Ok(false);
                }
                if let Some(max) = runtime_stage.max_iterations
                    && iteration >= max.max(1)
                {
                    result.outputs.insert(
                        format!("{}.iterations", runtime_stage.id),
                        iteration.to_string(),
                    );
                    return Ok(false);
                }

                if let Some(delay) = loop_sleep {
                    sleep(delay).await;
                }
            }
        }
        .await;

        if let Some(worktree) = worktree
            && let Err(err) = cleanup_worktree(&worktree).await
        {
            result
                .errors
                .push(format!("stage '{}': {}", runtime_stage.id, err));
        }

        stage_result
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

        let branch = subst(worktree_ref, &workflow.vars, outputs);
        let branch = branch.trim();
        if branch.is_empty() {
            return Err(anyhow!(
                "stage '{}' has empty rendered worktree branch",
                stage.id
            ));
        }

        let Some(repo_root) = resolve_repo_root(workflow, stage).await? else {
            return Err(anyhow!(
                "stage '{}' requested worktree '{}' but no git repository root was found",
                stage.id,
                branch
            ));
        };

        let session = outputs
            .get("SESSION")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let dir_name = format!(
            "worktree-{}-{}",
            sanitize_token(&stage.id),
            sanitize_token(branch)
        );
        let worktree_path = session_dir(&session).join(dir_name);

        if worktree_path.exists() {
            let _ = tokio::fs::remove_dir_all(&worktree_path).await;
        }
        if let Some(parent) = worktree_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        add_worktree(&repo_root, &worktree_path, branch)
            .await
            .map_err(|err| {
                anyhow!(
                    "stage '{}' failed to setup worktree '{}': {}",
                    stage.id,
                    branch,
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
            child_vars.insert(k.clone(), subst(v, &workflow.vars, &result.outputs));
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
                let mut output = String::new();
                if let Some(last_stage) = child_wf.stages.last()
                    && let Some(v) = child_result.outputs.get(&last_stage.id)
                {
                    output = v.clone();
                }
                if output.is_empty() {
                    output = format!("sub-workflow completed: {}", child_wf.name);
                }
                Ok(output)
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
        for i in 0..forks {
            let mut fork_stage = stage.clone();
            fork_stage.forks = None;
            fork_stage.vote = None;
            if !stage.models.is_empty() {
                fork_stage.model = Some(stage.models[(i as usize) % stage.models.len()].clone());
            }
            let worker = self.clone();
            let workflow_for_task = workflow_snapshot.clone();
            let vars_for_task = workflow.vars.clone();
            let outputs_for_task = outputs_snapshot.clone();
            join_set.spawn(async move {
                let out = worker
                    .execute_with_retry(
                        &workflow_for_task,
                        &fork_stage,
                        &vars_for_task,
                        &outputs_for_task,
                    )
                    .await;
                (i, out)
            });
        }

        let mut slots: Vec<Option<String>> = vec![None; forks as usize];
        let mut first_error: Option<ProviderError> = None;
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((idx, Ok(out))) => {
                    slots[idx as usize] = Some(out);
                }
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
                            format!("fork task failed in stage '{}': {}", stage.id, err),
                        ));
                    }
                    join_set.abort_all();
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        let mut fork_outputs = Vec::with_capacity(forks as usize);
        for (idx, value) in slots.into_iter().enumerate() {
            let out = value.ok_or_else(|| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!("fork {} produced no output in stage '{}'", idx, stage.id),
                )
            })?;
            let key = format!("{}.{}", stage.id, idx);
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
        let mut join_set = JoinSet::new();
        let workflow_snapshot = workflow.clone();
        for (idx, item) in items.iter().enumerate() {
            let mut each_stage = stage.clone();
            each_stage.each.clear();
            each_stage.each_from = None;
            each_stage.vote = None;

            let mut vars = workflow.vars.clone();
            vars.insert("each".to_string(), item.clone());
            let worker = self.clone();
            let workflow_for_task = workflow_snapshot.clone();
            let outputs_for_task = outputs_snapshot.clone();
            join_set.spawn(async move {
                let out = worker
                    .execute_with_retry(&workflow_for_task, &each_stage, &vars, &outputs_for_task)
                    .await;
                (idx, out)
            });
        }

        let mut slots: Vec<Option<String>> = vec![None; items.len()];
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
                            format!("each task failed in stage '{}': {}", stage.id, err),
                        ));
                    }
                    join_set.abort_all();
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        let mut each_outputs = Vec::with_capacity(items.len());
        for (idx, value) in slots.into_iter().enumerate() {
            let out = value.ok_or_else(|| {
                ProviderError::new(
                    "provider_exec_failed",
                    format!(
                        "each item {} produced no output in stage '{}'",
                        idx, stage.id
                    ),
                )
            })?;
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
        let mut cmd = Command::new("git");
        cmd.arg("-C")
            .arg(&base)
            .arg("rev-parse")
            .arg("--show-toplevel")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        let output = match cmd.output().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !output.status.success() {
            continue;
        }
        let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
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
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(repo_root).arg("worktree").arg("add");
    if create_branch {
        cmd.arg("-b").arg(branch).arg(worktree_path);
    } else {
        cmd.arg(worktree_path).arg(branch);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = cmd.output().await?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let msg = if stderr.is_empty() { stdout } else { stderr };
    Err(anyhow!(
        "git worktree add failed (repo='{}', branch='{}'): {}",
        repo_root.display(),
        branch,
        msg
    ))
}

async fn cleanup_worktree(worktree: &PreparedWorktree) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(&worktree.repo_root)
        .arg("worktree")
        .arg("remove")
        .arg("--force")
        .arg(&worktree.path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = cmd.output().await?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let msg = if stderr.is_empty() { stdout } else { stderr };
    Err(anyhow!(
        "git worktree remove failed (repo='{}', path='{}'): {}",
        worktree.repo_root.display(),
        worktree.path.display(),
        msg
    ))
}

fn sanitize_token(input: &str) -> String {
    let out = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    if out.trim_matches('-').is_empty() {
        "x".to_string()
    } else {
        out
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

#[cfg(test)]
mod tests {
    use super::{Executor, RunConfig, sanitize_token};
    use crate::memory::MemoryStore;
    use crate::session::session_dir;
    use crate::workflow::{Stage, Workflow};
    use std::time::{Duration, Instant};

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
            source_path: None,
        };

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
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
            source_path: None,
        };

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("list"), Some(&true));
        assert_eq!(res.success.get("process"), Some(&true));
        assert!(res.outputs.contains_key("process.0"));
        assert!(res.outputs.contains_key("process.1"));
        assert!(res.outputs.contains_key("process.all"));
    }

    #[tokio::test]
    async fn forks_run_concurrently() {
        let wf = Workflow {
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

        let started = Instant::now();
        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");
        let elapsed = started.elapsed();

        assert_eq!(res.success.get("review"), Some(&true));
        assert!(
            elapsed < Duration::from_millis(1600),
            "forks should run concurrently; elapsed={:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn each_runs_concurrently() {
        let wf = Workflow {
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

        let started = Instant::now();
        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");
        let elapsed = started.elapsed();

        assert_eq!(res.success.get("process"), Some(&true));
        assert!(
            elapsed < Duration::from_millis(1600),
            "each should run concurrently; elapsed={:?}",
            elapsed
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

        let wf = Workflow {
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

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("call"), Some(&true));
        assert_eq!(res.outputs.get("call"), Some(&"child-ok".to_string()));
        assert_eq!(res.children.len(), 1);
    }

    #[tokio::test]
    async fn memory_injects_previous_stage_output() {
        let mem_root = std::env::temp_dir().join(format!(
            "anna-mem-int-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let exec = Executor::with_memory_store(MemoryStore::new(mem_root, 10));

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
        let _ = exec
            .run(
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
        let res = exec
            .run(
                &wf2,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("second run should succeed");

        assert_eq!(res.success.get("use_mem"), Some(&true));
        assert_eq!(res.outputs.get("use_mem"), Some(&"prev:first".to_string()));
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

        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("init")
            .arg("-b")
            .arg("main")
            .status()
            .expect("git init should run");
        assert!(status.success(), "git init should succeed");

        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("add")
            .arg("README.md")
            .status()
            .expect("git add should run");
        assert!(status.success(), "git add should succeed");

        let status = std::process::Command::new("git")
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
        assert!(status.success(), "git commit should succeed");

        let branch = format!("feat-{}", rand::random::<u16>());
        let wf = Workflow {
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

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("wt"), Some(&true));
        let output = res.outputs.get("wt").cloned().unwrap_or_default();
        let expected_name = format!(
            "worktree-{}-{}",
            sanitize_token("wt"),
            sanitize_token(&branch)
        );
        assert!(
            output.contains(&expected_name),
            "pwd output should include worktree dir name '{}', got '{}'",
            expected_name,
            output
        );

        let worktree_path = session_dir(&res.session_id).join(expected_name);
        assert!(
            !worktree_path.exists(),
            "worktree path should be cleaned up: {}",
            worktree_path.display()
        );
    }

    #[tokio::test]
    async fn stage_loop_respects_max_iterations() {
        let wd = std::env::temp_dir().join(format!(
            "anna-stage-loop-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&wd)
            .await
            .expect("create temp workdir");

        let wf = Workflow {
            name: "stage-loop-max".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(wd.display().to_string()),
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

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("looped"), Some(&true));
        assert_eq!(res.outputs.get("looped"), Some(&"3".to_string()));
        assert_eq!(res.outputs.get("looped.iterations"), Some(&"3".to_string()));
    }

    #[tokio::test]
    async fn stage_loop_break_when_stops_workflow_progress() {
        let wd = std::env::temp_dir().join(format!(
            "anna-stage-break-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&wd)
            .await
            .expect("create temp workdir");

        let wf = Workflow {
            name: "stage-loop-break".into(),
            mode: "once".into(),
            memory: false,
            tags: vec![],
            vars: Default::default(),
            env: Default::default(),
            workdir: Some(wd.display().to_string()),
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

        let res = Executor::new()
            .run(
                &wf,
                RunConfig {
                    max_iterations: Some(1),
                    session_id_override: None,
                },
            )
            .await
            .expect("workflow should run");

        assert_eq!(res.success.get("test"), Some(&true));
        assert_eq!(res.outputs.get("test"), Some(&"RUN-2".to_string()));
        assert_eq!(res.outputs.get("test.iterations"), Some(&"2".to_string()));
        assert_eq!(res.success.get("after"), None);
        assert_eq!(res.outputs.get("after"), None);
    }
}
