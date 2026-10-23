//! The services of the machine: one small file for each, so `brokapi services` can list them.
//!
//! A file is made when a service starts and removed when it stops. A service that was killed leaves its file: whoever
//! reads the registry notices that the process is gone and removes it.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// What the registry knows of a service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// The config file.
    pub config: PathBuf,
    /// The state directory. It identifies the service: one directory, one service.
    pub state_dir: PathBuf,
    /// The process id.
    pub pid: u32,
    /// Where it listens (`unix:...` or `tcp:...`).
    pub address: String,
    /// When it started, in seconds since 1970.
    pub started_unix: u64,
}

/// The directory of the registry: `BROKAPI_REGISTRY_DIR`, else the state directory of the user (Linux) or the local
/// data directory (macOS, Windows), `services` in it.
pub fn dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("BROKAPI_REGISTRY_DIR") {
        return PathBuf::from(dir);
    }
    match directories::ProjectDirs::from("", "", "brokapi") {
        Some(d) => d
            .state_dir()
            .unwrap_or_else(|| d.data_local_dir())
            .join("services"),
        None => std::env::temp_dir().join("brokapi-services"),
    }
}

/// A short stable name for a state directory (FNV-1a of its path).
fn file_for(state_dir: &Path) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in state_dir.to_string_lossy().as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}.json")
}

/// Records a running service.
pub fn register(entry: &Entry) -> std::io::Result<()> {
    let dir = dir();
    std::fs::create_dir_all(&dir)?;
    let json = serde_json::to_vec_pretty(entry).map_err(std::io::Error::other)?;
    let path = dir.join(file_for(&entry.state_dir));
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, json)?;
    std::fs::rename(tmp, path)
}

/// Removes the record of a service, if it is this process's.
pub fn unregister(state_dir: &Path, pid: u32) {
    let path = dir().join(file_for(state_dir));
    if let Some(entry) = read(&path)
        && entry.pid == pid
    {
        let _ = std::fs::remove_file(path);
    }
}

fn read(path: &Path) -> Option<Entry> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// The record of the service of this state directory, if that service lives. A record of a dead one is removed.
pub fn find(state_dir: &Path) -> Option<Entry> {
    let path = dir().join(file_for(state_dir));
    let entry = read(&path)?;
    if super::proc::is_brokapi(entry.pid) {
        Some(entry)
    } else {
        let _ = std::fs::remove_file(path);
        None
    }
}

/// All services that live, oldest first. Records of dead ones are removed.
pub fn list() -> Vec<Entry> {
    let mut out = Vec::new();
    let Ok(read_dir) = std::fs::read_dir(dir()) else {
        return out;
    };
    for item in read_dir.flatten() {
        let path = item.path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        match read(&path) {
            Some(e) if super::proc::is_brokapi(e.pid) => out.push(e),
            _ => {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    out.sort_by_key(|e| e.started_unix);
    out
}
