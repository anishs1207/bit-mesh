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
        if chunk_size == 0 {
            bail!("chunk size must be greater than zero");
        }
        let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut chunks = Vec::new();
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
            refs.push(ChunkRef {
                hash: hash_bytes(&buf),
                size: read as u64,
            });
            chunks.push(buf);
        }
        Ok((
            Self {
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
            },
            chunks,
        ))
    }

    pub fn id(&self) -> Result<ContentId> {
        Ok(hash_bytes(&serde_json::to_vec(self)?))
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
    }
}
