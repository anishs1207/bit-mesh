use crate::manifest::{ContentId, Manifest, hash_bytes, validate_content_id};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub manifest: ContentId,
    pub expires_at: u64,
    pub replication: usize,
    pub payer: Option<String>,
    pub payment_signature: Option<String>,
}

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
    quota: u64,
    write_lock: Arc<Mutex<()>>,
}
impl Store {
    pub fn open(root: impl Into<PathBuf>, quota: u64) -> Result<Self> {
        let root = root.into();
        for dir in ["chunks", "manifests", "leases"] {
            fs::create_dir_all(root.join(dir))?;
        }
        Ok(Self {
            root,
            quota,
            write_lock: Arc::new(Mutex::new(())),
        })
    }
    fn checked_id(id: &str) -> Result<&str> {
        validate_content_id(id)?;
        Ok(id)
    }
    pub fn chunk_path(&self, id: &str) -> Result<PathBuf> {
        Ok(self.root.join("chunks").join(Self::checked_id(id)?))
    }
    pub fn used(&self) -> Result<u64> {
        Ok(fs::read_dir(self.root.join("chunks"))?
            .filter_map(Result::ok)
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum())
    }
    pub fn quota(&self) -> u64 {
        self.quota
    }

    pub fn available(&self) -> Result<u64> {
        Ok(self.quota.saturating_sub(self.used()?))
    }
    pub fn put_chunk(&self, expected: &str, bytes: &[u8]) -> Result<bool> {
        if hash_bytes(bytes) != expected {
            bail!("chunk hash mismatch");
        }
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("storage write lock poisoned"))?;
        let path = self.chunk_path(expected)?;
        if path.exists() {
            return Ok(false);
        }
        if self.used()?.saturating_add(bytes.len() as u64) > self.quota {
            bail!("storage quota exceeded");
        }
        atomic_write(&path, bytes)?;
        Ok(true)
    }
    pub fn get_chunk(&self, id: &str) -> Result<Vec<u8>> {
        let bytes =
            fs::read(self.chunk_path(id)?).with_context(|| format!("chunk {id} not found"))?;
        if hash_bytes(&bytes) != id {
            bail!("stored chunk {id} is corrupt");
        }
        Ok(bytes)
    }
    pub fn has_chunk(&self, id: &str) -> bool {
        self.chunk_path(id).is_ok_and(|p| p.exists())
    }
    pub fn has_manifest(&self, id: &str) -> bool {
        Self::checked_id(id).is_ok_and(|id| self.root.join("manifests").join(id).exists())
    }
    pub fn put_manifest(&self, manifest: &Manifest) -> Result<ContentId> {
        manifest.validate()?;
        let id = manifest.id()?;
        atomic_write(
            &self.root.join("manifests").join(&id),
            &serde_json::to_vec_pretty(manifest)?,
        )?;
        Ok(id)
    }
    pub fn get_manifest(&self, id: &str) -> Result<Manifest> {
        let path = self.root.join("manifests").join(Self::checked_id(id)?);
        let manifest: Manifest = serde_json::from_slice(&fs::read(path)?)?;
        manifest.validate()?;
        if manifest.id()? != id {
            bail!("manifest id mismatch");
        }
        Ok(manifest)
    }
    pub fn put_lease(&self, lease: &Lease) -> Result<()> {
        if lease.replication == 0 {
            bail!("lease replication must be at least one");
        }
        if lease.expires_at <= SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() {
            bail!("lease expiry must be in the future");
        }
        if lease.payer.is_some() != lease.payment_signature.is_some() {
            bail!("payer and payment signature must be supplied together");
        }
        self.get_manifest(&lease.manifest)
            .context("lease references an unavailable manifest")?;
        atomic_write(
            &self
                .root
                .join("leases")
                .join(Self::checked_id(&lease.manifest)?),
            &serde_json::to_vec_pretty(lease)?,
        )
    }
    pub fn active_leases(&self) -> Result<Vec<Lease>> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut leases = Vec::new();
        for entry in fs::read_dir(self.root.join("leases"))? {
            let entry = entry?;
            let bytes = fs::read(entry.path())?;
            let lease: Lease = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid lease metadata in {}", entry.path().display()))?;
            if lease.expires_at > now {
                leases.push(lease);
            }
        }
        Ok(leases)
    }
    pub fn garbage_collect(&self) -> Result<(usize, u64)> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("storage write lock poisoned"))?;
        let mut live = HashSet::new();
        for lease in self.active_leases()? {
            if let Ok(m) = self.get_manifest(&lease.manifest) {
                live.extend(m.chunks.into_iter().map(|c| c.hash));
            }
        }
        let mut count = 0;
        let mut bytes = 0;
        for entry in fs::read_dir(self.root.join("chunks"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if !live.contains(&name) {
                bytes += entry.metadata()?.len();
                fs::remove_file(entry.path())?;
                count += 1;
            }
        }
        Ok((count, bytes))
    }
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    if path.exists() {
        return Ok(());
    }
    let suffix = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let temp = path.with_extension(format!("tmp-{}-{suffix}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        if !path.exists() {
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_corruption_and_deduplicates() {
        let root = std::env::temp_dir().join(format!("bitmesh-test-{}", std::process::id()));
        let s = Store::open(&root, 100).unwrap();
        let id = hash_bytes(b"hello");
        assert!(s.put_chunk(&id, b"hello").unwrap());
        assert!(!s.put_chunk(&id, b"hello").unwrap());
        assert!(s.put_chunk(&id, b"bad").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn concurrent_writes_cannot_exceed_quota() {
        let root = std::env::temp_dir().join(format!(
            "bitmesh-quota-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let store = Store::open(&root, 5).unwrap();
        let mut workers = Vec::new();
        for bytes in [b"aaaa".to_vec(), b"bbbb".to_vec()] {
            let store = store.clone();
            workers.push(std::thread::spawn(move || {
                store.put_chunk(&hash_bytes(&bytes), &bytes)
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(Result::is_ok)
            .count();
        assert_eq!(successes, 1);
        assert_eq!(store.used().unwrap(), 4);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn garbage_collection_fails_closed_on_bad_lease() {
        let root = std::env::temp_dir().join(format!(
            "bitmesh-gc-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let store = Store::open(&root, 100).unwrap();
        fs::write(root.join("leases").join("broken"), b"not json").unwrap();
        assert!(store.garbage_collect().is_err());
        let _ = fs::remove_dir_all(root);
    }
}
