use crate::{
    dht::{Dht, Peer},
    manifest::{ContentId, Manifest, validate_content_id},
    store::{Lease, Store},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration as StdDuration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    time::{Duration, timeout},
};

const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_TTL: StdDuration = StdDuration::from_secs(15 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    GetChunk(ContentId),
    HasChunk(ContentId),
    GetManifest(ContentId),
    PutChunk(ContentId, Vec<u8>),
    PutManifest(Manifest),
    PutLease(Lease),
    Status,
    FindProviders(ContentId),
    AnnounceProvider { content: ContentId, peer: Peer },
    Ping,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeStatus {
    pub used_bytes: u64,
    pub quota_bytes: u64,
    pub available_bytes: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Chunk(Vec<u8>),
    Manifest(Manifest),
    Present(bool),
    Stored,
    Status(NodeStatus),
    Providers(Vec<Peer>),
    Pong,
    Error(String),
}
async fn send_frame(stream: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_FRAME_SIZE {
        bail!("frame too large");
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}
async fn recv_frame<T: for<'a> Deserialize<'a>>(stream: &mut TcpStream) -> Result<T> {
    let len = stream.read_u32().await? as usize;
    if len > MAX_FRAME_SIZE {
        bail!("frame too large");
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
pub async fn request(addr: SocketAddr, request: Request) -> Result<Response> {
    timeout(REQUEST_TIMEOUT, async {
        let mut stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect to {addr}"))?;
        send_frame(&mut stream, &request).await?;
        recv_frame(&mut stream).await
    })
    .await
    .with_context(|| format!("request to {addr} timed out"))?
}
pub async fn request_peer(peer: &Peer, request: Request) -> Result<Response> {
    match self::request(peer.address, request.clone()).await {
        Ok(response) => Ok(response),
        Err(direct_error) => match peer.relay {
            Some(relay) => self::request(relay, request).await.with_context(|| {
                format!("direct connection failed: {direct_error}; relay {relay} failed")
            }),
            None => Err(direct_error),
        },
    }
}
pub async fn serve(addr: SocketAddr, store: Store, max_connections: usize) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let local = Peer::from_address(addr, store.quota());
    let dht = Arc::new(Mutex::new(Dht::new(local.id, 20)));
    let limit = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();
        let dht = dht.clone();
        let local = local.clone();
        let permit = limit.clone().acquire_owned().await?;
        tokio::spawn(async move {
            let response = match timeout(REQUEST_TIMEOUT, recv_frame::<Request>(&mut stream)).await
            {
                Err(_) => Err(anyhow::anyhow!("request timed out")),
                Ok(request) => match request {
                    Ok(Request::GetChunk(id)) => store.get_chunk(&id).map(Response::Chunk),
                    Ok(Request::HasChunk(id)) => Ok(Response::Present(store.has_chunk(&id))),
                    Ok(Request::GetManifest(id)) => store.get_manifest(&id).map(Response::Manifest),
                    Ok(Request::PutChunk(id, bytes)) => {
                        store.put_chunk(&id, &bytes).map(|_| Response::Stored)
                    }
                    Ok(Request::PutManifest(m)) => store.put_manifest(&m).map(|_| Response::Stored),
                    Ok(Request::PutLease(lease)) => {
                        store.put_lease(&lease).map(|_| Response::Stored)
                    }
                    Ok(Request::Status) => (|| {
                        Ok(Response::Status(NodeStatus {
                            used_bytes: store.used()?,
                            quota_bytes: store.quota(),
                            available_bytes: store.available()?,
                        }))
                    })(),
                    Ok(Request::FindProviders(id)) => {
                        if let Err(error) = validate_content_id(&id) {
                            Err(error)
                        } else {
                            let mut providers = dht.lock().await.providers(&id);
                            if store.has_chunk(&id) || store.has_manifest(&id) {
                                providers.retain(|peer| peer.id != local.id);
                                providers.insert(0, local.clone());
                            }
                            Ok(Response::Providers(providers))
                        }
                    }
                    Ok(Request::AnnounceProvider { content, peer }) => {
                        match validate_content_id(&content) {
                            Err(error) => Err(error),
                            Ok(()) => {
                                let mut dht = dht.lock().await;
                                dht.add_peer(peer.clone());
                                dht.announce(content, peer, PROVIDER_TTL);
                                Ok(Response::Stored)
                            }
                        }
                    }
                    Ok(Request::Ping) => Ok(Response::Pong),
                    Err(e) => Err(e),
                },
            }
            .unwrap_or_else(|e| Response::Error(e.to_string()));
            let _ = send_frame(&mut stream, &response).await;
            drop(permit);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkRef, hash_bytes};
    use std::{fs, net::TcpListener as StdTcpListener};

    #[tokio::test]
    async fn loopback_transfer_and_status() {
        let root = std::env::temp_dir().join(format!(
            "bitmesh-network-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let store = Store::open(&root, 1024).unwrap();
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);

        let server = tokio::spawn(serve(address, store.clone(), 4));
        for _ in 0..20 {
            if matches!(request(address, Request::Ping).await, Ok(Response::Pong)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let bytes = b"verified network chunk".to_vec();
        let id = hash_bytes(&bytes);
        assert!(matches!(
            request(address, Request::PutChunk(id.clone(), bytes.clone())).await,
            Ok(Response::Stored)
        ));
        assert!(matches!(
            request(address, Request::FindProviders(hash_bytes(&bytes))).await,
            Ok(Response::Providers(providers)) if providers.first().is_some_and(|p| p.address == address)
        ));
        assert!(matches!(
            request(address, Request::GetChunk(id.clone())).await,
            Ok(Response::Chunk(received)) if received == bytes
        ));
        assert!(matches!(
            request(address, Request::Status).await,
            Ok(Response::Status(NodeStatus { used_bytes, quota_bytes: 1024, .. }))
                if used_bytes == bytes.len() as u64
        ));
        assert!(matches!(
            request(address, Request::PutChunk(id, b"corrupt".to_vec())).await,
            Ok(Response::Error(_))
        ));

        let manifest = Manifest {
            version: 1,
            name: "sample".into(),
            size: bytes.len() as u64,
            chunk_size: bytes.len(),
            chunks: vec![ChunkRef {
                hash: hash_bytes(&bytes),
                size: bytes.len() as u64,
            }],
            created_at: 0,
        };
        let lease = Lease {
            manifest: manifest.id().unwrap(),
            expires_at: u64::MAX,
            replication: 2,
            payer: None,
            payment_signature: None,
        };
        assert!(matches!(
            request(address, Request::PutManifest(manifest)).await,
            Ok(Response::Stored)
        ));
        assert!(matches!(
            request(address, Request::PutLease(lease)).await,
            Ok(Response::Stored)
        ));
        assert_eq!(store.garbage_collect().unwrap().0, 0);
        assert!(store.has_chunk(&hash_bytes(&bytes)));

        server.abort();
        let _ = server.await;
        let _ = fs::remove_dir_all(root);
    }
}
