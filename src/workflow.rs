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
pub struct Stage {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: Option<String>,
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
    pub fn provider_name(&self) -> &str {
        if self.provider.trim().is_empty() {
            "shell"
        } else {
            self.provider.as_str()
        }
    }

    pub fn timeout_duration(&self) -> Result<Option<Duration>> {
        parse_optional_duration(self.timeout.as_deref())
    }

    pub fn retry_delay_duration(&self) -> Result<Duration> {
        match self.retry_delay.as_deref() {
            Some(s) => parse_duration(s)
                .with_context(|| format!("invalid retry_delay for stage '{}': {}", self.id, s)),
            None => Ok(Duration::from_secs(1)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read workflow '{}'", path.display()))?;
        let mut wf: Workflow = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse yaml '{}'", path.display()))?;
        wf.source_path = Some(path.to_path_buf());
        wf.validate()?;
        Ok(wf)
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("workflow name is required");
        }
        if self.mode != "once" && self.mode != "continuous" {
            bail!("workflow mode must be 'once' or 'continuous'");
        }
        if self.stages.is_empty() {
            bail!("workflow must contain at least one stage");
        }

        let mut known_stage_ids = HashSet::new();
        for stage in &self.stages {
            if stage.id.trim().is_empty() {
                bail!("stage id is required");
            }
            if !known_stage_ids.insert(stage.id.clone()) {
                bail!("duplicate stage id '{}'", stage.id);
            }
            if let Some(trust) = &stage.trust
                && trust != "none"
                && trust != "read"
                && trust != "all"
            {
                bail!("invalid trust '{}' in stage '{}'", trust, stage.id);
            }
        }

        let mut prior_stages = HashSet::new();
        for stage in &self.stages {
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
            prior_stages.insert(stage.id.clone());
        }
        Ok(())
    }

    pub fn has_loop(&self) -> bool {
        self.stages.iter().any(|s| s.loop_stage)
    }

    pub fn is_continuous(&self) -> bool {
        self.mode == "continuous" || self.has_loop()
    }

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

pub fn parse_optional_duration(raw: Option<&str>) -> Result<Option<Duration>> {
    match raw {
        Some(v) => Ok(Some(
            parse_duration(v).with_context(|| format!("invalid duration '{}'", v))?,
        )),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::{Stage, Workflow};
    use std::collections::HashMap;

    fn base_workflow(stages: Vec<Stage>) -> Workflow {
        Workflow {
            name: "wf".to_string(),
            mode: "once".to_string(),
            memory: false,
            tags: vec![],
            vars: HashMap::new(),
            env: HashMap::new(),
            workdir: None,
            trigger: Default::default(),
            stages,
            source_path: None,
        }
    }

    #[test]
    fn allows_dependency_on_prior_stage() {
        let wf = base_workflow(vec![
            Stage {
                id: "a".to_string(),
                exec: Some("echo a".to_string()),
                ..Default::default()
            },
            Stage {
                id: "b".to_string(),
                needs: vec!["a".to_string()],
                exec: Some("echo b".to_string()),
                ..Default::default()
            },
        ]);
        wf.validate().expect("workflow should validate");
    }

    #[test]
    fn rejects_dependency_on_later_stage() {
        let wf = base_workflow(vec![
            Stage {
                id: "b".to_string(),
                needs: vec!["a".to_string()],
                exec: Some("echo b".to_string()),
                ..Default::default()
            },
            Stage {
                id: "a".to_string(),
                exec: Some("echo a".to_string()),
                ..Default::default()
            },
        ]);
        let err = wf.validate().expect_err("workflow should fail validation");
        assert!(err.to_string().contains("must reference an earlier stage"));
    }
}
