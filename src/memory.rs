use crate::result::RunResult;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub ts: u64,
    pub out: HashMap<String, String>,
    pub ok: HashMap<String, bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowMemory {
    pub name: String,
    pub entries: Vec<MemoryEntry>,
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    root: PathBuf,
    max_entries: usize,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            root: default_memory_root(),
            max_entries: 10,
        }
    }
}

impl MemoryStore {
    pub fn new(root: PathBuf, max_entries: usize) -> Self {
        Self { root, max_entries }
    }

    pub async fn load(&self, workflow_name: &str) -> Result<WorkflowMemory> {
        let path = self.memory_file(workflow_name);
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => {
                let parsed: WorkflowMemory = serde_json::from_str(&content)
                    .with_context(|| format!("invalid memory file json '{}'", path.display()))?;
                Ok(parsed)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(WorkflowMemory {
                name: workflow_name.to_string(),
                entries: Vec::new(),
            }),
            Err(err) => Err(err).with_context(|| format!("failed reading '{}'", path.display())),
        }
    }

    pub async fn save_snapshot(&self, workflow_name: &str, result: &RunResult) -> Result<()> {
        let mut memory = self.load(workflow_name).await?;
        let mut out = HashMap::new();
        for (k, v) in &result.outputs {
            if k == "SESSION" || k.starts_with("memory.") {
                continue;
            }
            out.insert(k.clone(), v.clone());
        }
        let entry = MemoryEntry {
            ts: now_unix_secs(),
            out,
            ok: result.success.clone(),
        };
        memory.entries.push(entry);
        if memory.entries.len() > self.max_entries {
            let extra = memory.entries.len() - self.max_entries;
            memory.entries.drain(0..extra);
        }
        memory.name = workflow_name.to_string();

        let path = self.memory_file(workflow_name);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed creating memory dir '{}'", parent.display()))?;
        }
        let raw = serde_json::to_string_pretty(&memory)?;
        tokio::fs::write(&path, raw)
            .await
            .with_context(|| format!("failed writing memory file '{}'", path.display()))?;
        Ok(())
    }

    pub async fn inject_last_vars(
        &self,
        workflow_name: &str,
        outputs: &mut HashMap<String, String>,
    ) -> Result<()> {
        let memory = self.load(workflow_name).await?;
        let Some(last) = memory.entries.last() else {
            return Ok(());
        };
        for (k, v) in &last.out {
            outputs.insert(format!("memory.{}", k), v.clone());
        }
        Ok(())
    }

    pub fn memory_file(&self, workflow_name: &str) -> PathBuf {
        self.root
            .join(format!("{}.json", sanitize_name(workflow_name)))
    }
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn default_memory_root() -> PathBuf {
    match std::env::var("HOME") {
        Ok(home) if !home.trim().is_empty() => Path::new(&home).join(".anna/memory"),
        _ => Path::new("/tmp/anna-memory").to_path_buf(),
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::MemoryStore;
    use crate::result::RunResult;

    #[tokio::test]
    async fn save_and_inject_memory_vars() {
        let tmp = std::env::temp_dir().join(format!("anna-mem-test-{}", std::process::id()));
        let store = MemoryStore::new(tmp, 10);

        let mut run = RunResult::new();
        run.outputs.insert("check".to_string(), "ok".to_string());
        run.success.insert("check".to_string(), true);
        store
            .save_snapshot("wf", &run)
            .await
            .expect("save should work");

        let mut outputs = std::collections::HashMap::new();
        store
            .inject_last_vars("wf", &mut outputs)
            .await
            .expect("inject should work");
        assert_eq!(outputs.get("memory.check"), Some(&"ok".to_string()));
    }
}
