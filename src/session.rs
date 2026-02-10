use anyhow::{Context, Result};
use rand::TryRngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn gen_session_id() -> String {
    let mut b = [0_u8; 4];
    if OsRng.try_fill_bytes(&mut b).is_ok() {
        return b.iter().map(|v| format!("{:02x}", v)).collect::<String>();
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:08x}", (nanos & 0xffff_ffff) as u32)
}

pub fn session_dir(session_id: &str) -> PathBuf {
    Path::new("/tmp/anna").join(session_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMeta {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub workflow: String,
    #[serde(default)]
    pub children: Vec<String>,
}

fn meta_path(session_id: &str) -> PathBuf {
    session_dir(session_id).join("_meta.json")
}

pub async fn write_session_meta(meta: &SessionMeta) -> Result<()> {
    let dir = session_dir(&meta.session_id);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("failed to create session dir '{}'", dir.display()))?;
    let raw = serde_json::to_string_pretty(meta)?;
    let path = meta_path(&meta.session_id);
    tokio::fs::write(&path, raw)
        .await
        .with_context(|| format!("failed to write session meta '{}'", path.display()))?;
    Ok(())
}

pub async fn read_session_meta(session_id: &str) -> Result<SessionMeta> {
    let path = meta_path(session_id);
    let raw = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read session meta '{}'", path.display()))?;
    let meta: SessionMeta = serde_json::from_str(&raw)
        .with_context(|| format!("invalid session meta '{}'", path.display()))?;
    Ok(meta)
}

pub async fn init_session_meta(
    session_id: &str,
    workflow: &str,
    parent_id: Option<&str>,
) -> Result<()> {
    let meta = SessionMeta {
        session_id: session_id.to_string(),
        parent_id: parent_id.map(ToOwned::to_owned),
        workflow: workflow.to_string(),
        children: Vec::new(),
    };
    write_session_meta(&meta).await
}

pub async fn add_child_session(
    parent_session_id: &str,
    child_session_id: &str,
    parent_workflow: &str,
) -> Result<()> {
    let mut meta = match read_session_meta(parent_session_id).await {
        Ok(v) => v,
        Err(_) => SessionMeta {
            session_id: parent_session_id.to_string(),
            parent_id: None,
            workflow: parent_workflow.to_string(),
            children: Vec::new(),
        },
    };

    if !meta.children.iter().any(|v| v == child_session_id) {
        meta.children.push(child_session_id.to_string());
    }
    write_session_meta(&meta).await
}

pub async fn write_stage_log(session_id: &str, stage_id: &str, content: &str) -> Result<()> {
    let dir = session_dir(session_id);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("failed to create session dir '{}'", dir.display()))?;
    let path = dir.join(format!("{}.log", stage_id));
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("failed to write stage log '{}'", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{add_child_session, init_session_meta, read_session_meta};

    #[tokio::test]
    async fn session_meta_tracks_children() {
        let session = format!("meta-test-{}-{}", std::process::id(), rand::random::<u32>());
        init_session_meta(&session, "parent-workflow", None)
            .await
            .expect("init parent session meta");

        add_child_session(&session, "child-a", "parent-workflow")
            .await
            .expect("add child session");

        let meta = read_session_meta(&session)
            .await
            .expect("read session meta");
        assert_eq!(meta.session_id, session);
        assert_eq!(meta.workflow, "parent-workflow");
        assert_eq!(meta.children, vec!["child-a".to_string()]);
    }
}
