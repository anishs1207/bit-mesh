use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub type ContentId = String;

pub fn validate_content_id(id: &str) -> Result<()> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("content id must be a lowercase SHA-256 hex digest");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRef {
    pub hash: ContentId,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub version: u8,
    pub name: String,
    pub size: u64,
    pub chunk_size: usize,
    pub chunks: Vec<ChunkRef>,
    pub created_at: u64,
}

pub fn hash_bytes(bytes: &[u8]) -> ContentId {
    hex::encode(Sha256::digest(bytes))
}

impl Manifest {
    pub fn from_file(path: &Path, chunk_size: usize) -> Result<(Self, Vec<Vec<u8>>)> {
        let mut chunks = Vec::new();
        let manifest = Self::from_file_with(path, chunk_size, |_, bytes| {
            chunks.push(bytes.to_vec());
            Ok(())
        })?;
        Ok((manifest, chunks))
    }

    pub fn from_file_with(
        path: &Path,
        chunk_size: usize,
        mut on_chunk: impl FnMut(&ChunkRef, &[u8]) -> Result<()>,
    ) -> Result<Self> {
        if chunk_size == 0 {
            bail!("chunk size must be greater than zero");
        }
        let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut refs = Vec::new();
        let mut total = 0u64;
        loop {
            let mut buf = vec![0; chunk_size];
            let mut read = 0;
            while read < chunk_size {
                let n = file.read(&mut buf[read..])?;
                if n == 0 {
                    break;
                }
                read += n;
            }
            if read == 0 {
                break;
            }
            buf.truncate(read);
            total += read as u64;
            let reference = ChunkRef {
                hash: hash_bytes(&buf),
                size: read as u64,
            };
            on_chunk(&reference, &buf)?;
            refs.push(reference);
        }
        Ok(Self {
            version: 1,
            name: path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("file")
                .to_owned(),
            size: total,
            chunk_size,
            chunks: refs,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        })
    }

    pub fn id(&self) -> Result<ContentId> {
        Ok(hash_bytes(&serde_json::to_vec(self)?))
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("unsupported manifest version {}", self.version);
        }
        if self.chunk_size == 0 {
            bail!("chunk size must be greater than zero");
        }
        let mut total = 0u64;
        for (index, chunk) in self.chunks.iter().enumerate() {
            validate_content_id(&chunk.hash)
                .with_context(|| format!("invalid chunk id at index {index}"))?;
            if chunk.size == 0 || chunk.size > self.chunk_size as u64 {
                bail!("invalid chunk size at index {index}");
            }
            total = total
                .checked_add(chunk.size)
                .context("manifest size overflow")?;
        }
        if total != self.size {
            bail!("manifest size is {}, but chunks total {total}", self.size);
        }
        if self.size == 0 && !self.chunks.is_empty() {
            bail!("empty files cannot contain chunks");
        }
        Ok(())
    }

    pub fn verify_chunk(&self, index: usize, bytes: &[u8]) -> bool {
        self.chunks
            .get(index)
            .is_some_and(|c| c.size == bytes.len() as u64 && c.hash == hash_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_hash_and_verification() {
        assert_eq!(
            hash_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let m = Manifest {
            version: 1,
            name: "x".into(),
            size: 3,
            chunk_size: 3,
            chunks: vec![ChunkRef {
                hash: hash_bytes(b"abc"),
                size: 3,
            }],
            created_at: 0,
        };
        assert!(m.verify_chunk(0, b"abc"));
        assert!(!m.verify_chunk(0, b"abd"));
        m.validate().unwrap();
    }

    #[test]
    fn rejects_malformed_manifest() {
        let mut m = Manifest {
            version: 1,
            name: "x".into(),
            size: 4,
            chunk_size: 3,
            chunks: vec![ChunkRef {
                hash: hash_bytes(b"abc"),
                size: 3,
            }],
            created_at: 0,
        };
        assert!(m.validate().is_err());
        m.size = 3;
        m.chunks[0].hash.make_ascii_uppercase();
        assert!(m.validate().is_err());
    }

    #[test]
    fn streams_chunks_without_retaining_file_bytes() {
        let path = std::env::temp_dir().join(format!("bitmesh-stream-test-{}", std::process::id()));
        std::fs::write(&path, b"abcdefgh").unwrap();
        let mut seen = Vec::new();
        let manifest = Manifest::from_file_with(&path, 3, |reference, bytes| {
            assert_eq!(reference.hash, hash_bytes(bytes));
            seen.push(bytes.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![b"abc".to_vec(), b"def".to_vec(), b"gh".to_vec()]);
        assert_eq!(manifest.size, 8);
        let _ = std::fs::remove_file(path);
    }
}
