use anyhow::{Context, Result};
use rand::TryRngCore;
use rand::rngs::OsRng;
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
