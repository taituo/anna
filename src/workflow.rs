use anyhow::{Context, Result, bail};
use humantime::parse_duration;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn default_mode() -> String {
    "once".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Trigger configuration for webhook/watch/cron/interval launches.
pub struct TriggerConfig {
    #[serde(default)]
    pub webhook: Option<String>,
    #[serde(default)]
    pub watch: Option<String>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub interval: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Single workflow stage definition.
pub struct Stage {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(rename = "llm_adapter", default)]
    pub llm_adapter: Option<String>,
    #[serde(default)]
    pub exec: Option<String>,
    #[serde(rename = "do", default)]
    pub do_prompt: Option<String>,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default)]
    pub context: Vec<String>,
    #[serde(default)]
    pub secrets: HashMap<String, String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub worktree: Option<String>,
    #[serde(default)]
    pub trust: Option<String>,
    #[serde(default)]
    pub forks: Option<u32>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub when: Option<String>,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(rename = "loop", default)]
    pub loop_stage: bool,
    #[serde(default)]
    pub interval: Option<String>,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub retry: Option<u32>,
    #[serde(rename = "retry_delay", default)]
    pub retry_delay: Option<String>,
    #[serde(rename = "break_when", default)]
    pub break_when: Option<String>,
    #[serde(rename = "max_iterations", default)]
    pub max_iterations: Option<u32>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub each: Vec<String>,
    #[serde(rename = "each_from", default)]
    pub each_from: Option<String>,
    #[serde(default)]
    pub vote: Option<String>,
    #[serde(rename = "hitl", default)]
    pub hitl: bool,
    #[serde(rename = "hitl_prompt", default)]
    pub hitl_prompt: Option<String>,
    #[serde(rename = "hitl_options", default)]
    pub hitl_options: Vec<String>,
    #[serde(default)]
    pub before: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(rename = "on_error", default)]
    pub on_error: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub vars: HashMap<String, String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub stdin: Option<String>,
    #[serde(default)]
    pub parse: Option<String>,
}

impl Stage {
    /// Returns the effective provider name, defaulting to `shell`.
    pub fn provider_name(&self) -> &str {
        if self.provider.trim().is_empty() {
            "shell"
        } else {
            self.provider.as_str()
        }
    }

    /// Parses optional stage timeout as `Duration`.
    pub fn timeout_duration(&self) -> Result<Option<Duration>> {
        parse_optional_duration(self.timeout.as_deref())
    }

    /// Parses stage retry delay, defaulting to one second.
    pub fn retry_delay_duration(&self) -> Result<Duration> {
        match self.retry_delay.as_deref() {
            Some(s) => parse_duration(s)
                .with_context(|| format!("invalid retry_delay for stage '{}': {}", self.id, s)),
            None => Ok(Duration::from_secs(1)),
        }
    }

    /// Parses loop interval, defaulting to one second.
    pub fn loop_interval_duration(&self) -> Result<Duration> {
        match self.interval.as_deref() {
            Some(s) => parse_duration(s)
                .with_context(|| format!("invalid interval for stage '{}': {}", self.id, s)),
            None => Ok(Duration::from_secs(1)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Full workflow definition loaded from `.anna` yaml.
pub struct Workflow {
    pub name: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub memory: bool,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub vars: HashMap<String, String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub trigger: TriggerConfig,
    #[serde(default)]
    pub stages: Vec<Stage>,
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

impl Workflow {
    /// Loads, parses and validates a workflow from disk.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read workflow '{}'", path.display()))?;
        let mut wf: Workflow = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse yaml '{}'", path.display()))?;
        wf.source_path = Some(path.to_path_buf());
        wf.validate()?;
        Ok(wf)
    }

    /// Validates workflow shape and stage dependencies.
    pub fn validate(&self) -> Result<()> {
        validate_workflow_shape(self)?;
        let stage_id_set = collect_and_validate_stage_ids(&self.stages)?;
        validate_stage_dependencies(&self.stages, &stage_id_set)?;
        Ok(())
    }

    /// Returns true if any stage is marked as loop stage.
    pub fn has_loop(&self) -> bool {
        self.stages.iter().any(|s| s.loop_stage)
    }

    /// Returns true when workflow mode is `continuous`.
    pub fn is_continuous(&self) -> bool {
        self.mode == "continuous"
    }

    /// Returns effective loop interval for continuous mode.
    pub fn interval(&self) -> Duration {
        for stage in &self.stages {
            if stage.loop_stage
                && let Some(i) = &stage.interval
                && let Ok(d) = parse_duration(i)
            {
                return d;
            }
        }
        Duration::from_secs(10)
    }
}

fn validate_workflow_shape(workflow: &Workflow) -> Result<()> {
    if workflow.name.trim().is_empty() {
        bail!("workflow name is required");
    }
    if workflow.mode != "once" && workflow.mode != "continuous" {
        bail!("workflow mode must be 'once' or 'continuous'");
    }
    if workflow.stages.is_empty() {
        bail!("workflow must contain at least one stage");
    }
    Ok(())
}

fn collect_and_validate_stage_ids(stages: &[Stage]) -> Result<HashSet<String>> {
    let mut seen_stage_ids = HashSet::new();
    for stage in stages {
        if stage.id.trim().is_empty() {
            bail!("stage id is required");
        }
        if !seen_stage_ids.insert(stage.id.to_owned()) {
            bail!("duplicate stage id '{}'", stage.id);
        }
        validate_stage_trust(stage)?;
    }
    Ok(seen_stage_ids)
}

fn validate_stage_trust(stage: &Stage) -> Result<()> {
    if let Some(trust) = &stage.trust
        && trust != "none"
        && trust != "read"
        && trust != "all"
    {
        bail!("invalid trust '{}' in stage '{}'", trust, stage.id);
    }
    Ok(())
}

fn validate_stage_dependencies(stages: &[Stage], known_stage_ids: &HashSet<String>) -> Result<()> {
    let mut prior_stages = HashSet::new();
    for stage in stages {
        for need in &stage.needs {
            if !known_stage_ids.contains(need) {
                bail!(
                    "stage '{}' references unknown dependency '{}'",
                    stage.id,
                    need
                );
            }
            if !prior_stages.contains(need) {
                bail!(
                    "stage '{}' dependency '{}' must reference an earlier stage",
                    stage.id,
                    need
                );
            }
        }
        prior_stages.insert(stage.id.to_owned());
    }
    Ok(())
}

/// Parses an optional human duration string.
pub fn parse_optional_duration(raw: Option<&str>) -> Result<Option<Duration>> {
    match raw {
        Some(v) => Ok(Some(
            parse_duration(v).with_context(|| format!("invalid duration '{}'", v))?,
        )),
        None => Ok(None),
    }
}


#[cfg(test)]
#[path = "workflow_tests.rs"]
mod tests;
