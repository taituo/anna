use crate::workflow::{Stage, Workflow};
use anyhow::{Context, Result, bail};
use humantime::parse_duration;
use std::collections::HashSet;
use std::time::Duration;

pub(super) fn default_mode() -> String {
    "once".to_string()
}

pub(super) fn validate_workflow_shape(workflow: &Workflow) -> Result<()> {
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

pub(super) fn collect_and_validate_stage_ids(stages: &[Stage]) -> Result<HashSet<String>> {
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

pub(super) fn validate_stage_dependencies(
    stages: &[Stage],
    known_stage_ids: &HashSet<String>,
) -> Result<()> {
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
pub(super) fn parse_optional_duration(raw: Option<&str>) -> Result<Option<Duration>> {
    match raw {
        Some(v) => Ok(Some(
            parse_duration(v).with_context(|| format!("invalid duration '{}'", v))?,
        )),
        None => Ok(None),
    }
}
