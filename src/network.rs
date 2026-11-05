use crate::{
    dht::{Dht, NodeId, Peer},
    manifest::{ContentId, Manifest, validate_content_id},
    solana::{self, PaymentClaim},
    store::{Lease, Store},
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration as StdDuration};
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
    FindNode(ContentId),
    AnnounceProvider {
        content: ContentId,
        peer: Peer,
    },
    RegisterPeer(Peer),
    RegisterTunnel(Peer),
    Relay {
        target: NodeId,
        request: Box<Request>,
    },
    Ping,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeStatus {
    pub used_bytes: u64,
    pub quota_bytes: u64,
    pub available_bytes: u64,
    #[serde(default)]
    pub provider_pubkey: Option<String>,
}

#[derive(Clone)]
pub struct PaymentConfig {
    pub rpc: String,
    pub provider_pubkey: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Chunk(Vec<u8>),
    Manifest(Manifest),
    Present(bool),
    Stored,
    Status(NodeStatus),
    Providers(Vec<Peer>),
    Peers(Vec<Peer>),
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
            Some(relay) => self::request(
                relay,
                Request::Relay {
                    target: peer.id,
                    request: Box::new(request),
                },
            )
            .await
            .with_context(|| {
                format!("direct connection failed: {direct_error}; relay {relay} failed")
            }),
            None => Err(direct_error),
        },
    }
}

type Tunnels = Arc<Mutex<HashMap<NodeId, Arc<Mutex<TcpStream>>>>>;

async fn maintain_tunnel(local: Peer, relay: SocketAddr, local_endpoint: SocketAddr) {
    loop {
        let result: Result<()> = async {
            let mut stream = TcpStream::connect(relay).await?;
            send_frame(&mut stream, &Request::RegisterTunnel(local.clone())).await?;
            if !matches!(recv_frame::<Response>(&mut stream).await?, Response::Stored) {
                bail!("relay refused tunnel registration");
            }
            loop {
                let request = recv_frame::<Request>(&mut stream).await?;
                let response = match self::request(local_endpoint, request).await {
                    Ok(response) => response,
                    Err(error) => Response::Error(error.to_string()),
                };
                send_frame(&mut stream, &response).await?;
            }
        }
        .await;
        if let Err(error) = result {
            eprintln!("relay tunnel {relay} disconnected: {error:#}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
pub async fn serve(addr: SocketAddr, store: Store, max_connections: usize) -> Result<()> {
    serve_with_peers(addr, store, max_connections, &[]).await
}

pub async fn serve_with_peers(
    addr: SocketAddr,
    store: Store,
    max_connections: usize,
    bootstrap: &[SocketAddr],
) -> Result<()> {
    serve_with_relay(addr, store, max_connections, bootstrap, None).await
}

pub async fn serve_with_relay(
    addr: SocketAddr,
    store: Store,
    max_connections: usize,
    bootstrap: &[SocketAddr],
    relay: Option<SocketAddr>,
) -> Result<()> {
    serve_configured(addr, store, max_connections, bootstrap, relay, None).await
}

pub async fn serve_configured(
    addr: SocketAddr,
    store: Store,
    max_connections: usize,
    bootstrap: &[SocketAddr],
    relay: Option<SocketAddr>,
    payment: Option<PaymentConfig>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut local = Peer::from_address(addr, store.quota());
    local.relay = relay;
    let mut routing = Dht::new(local.id, 20);
    for peer in store.routing_peers()? {
        if peer.id == Peer::from_address(peer.address, 0).id {
            routing.add_peer(peer);
        }
    }
    for address in bootstrap {
        routing.add_peer(Peer::from_address(*address, 0));
    }
    let dht = Arc::new(Mutex::new(routing));
    let tunnels: Tunnels = Arc::new(Mutex::new(HashMap::new()));
    if let Some(relay_address) = relay {
        let local_endpoint = if addr.ip().is_unspecified() {
            SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                addr.port(),
            )
        } else {
            addr
        };
        tokio::spawn(maintain_tunnel(
            local.clone(),
            relay_address,
            local_endpoint,
        ));
    }
    for address in bootstrap {
        if let Ok(Response::Peers(peers)) =
            request(*address, Request::FindNode(hex::encode(local.id))).await
        {
            let mut routing = dht.lock().await;
            for peer in peers {
                routing.add_peer(peer);
            }
        }
        let _ = request(*address, Request::RegisterPeer(local.clone())).await;
    }
    {
        let refresh_dht = dht.clone();
        let refresh_store = store.clone();
        let refresh_local = local.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            interval.tick().await;
            loop {
                interval.tick().await;
                let peers = refresh_dht.lock().await.known_peers();
                for peer in peers {
                    if matches!(request_peer(&peer, Request::Ping).await, Ok(Response::Pong)) {
                        let _ =
                            request_peer(&peer, Request::RegisterPeer(refresh_local.clone())).await;
                    } else {
                        refresh_dht.lock().await.remove_peer(&peer.id);
                    }
                }
                let peers = refresh_dht.lock().await.known_peers();
                if let Err(error) = refresh_store.save_routing_peers(&peers) {
                    eprintln!("routing save failed: {error:#}");
                }
            }
        });
    }
    let limit = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();
        let dht = dht.clone();
        let tunnels = tunnels.clone();
        let local = local.clone();
        let payment = payment.clone();
        let permit = limit.clone().acquire_owned().await?;
        tokio::spawn(async move {
            let incoming = timeout(REQUEST_TIMEOUT, recv_frame::<Request>(&mut stream)).await;
            if let Ok(Ok(Request::RegisterTunnel(peer))) = &incoming {
                if peer.id != Peer::from_address(peer.address, 0).id {
                    let _ = send_frame(
                        &mut stream,
                        &Response::Error("peer ID does not match address".into()),
                    )
                    .await;
                    return;
                }
                dht.lock().await.add_peer(peer.clone());
                if send_frame(&mut stream, &Response::Stored).await.is_ok() {
                    tunnels
                        .lock()
                        .await
                        .insert(peer.id, Arc::new(Mutex::new(stream)));
                }
                return;
            }
            let response = match incoming {
                Err(_) => Err(anyhow::anyhow!("request timed out")),
                Ok(request) => match request {
                    Ok(Request::GetChunk(id)) => store.get_chunk(&id).map(Response::Chunk),
                    Ok(Request::HasChunk(id)) => Ok(Response::Present(store.has_chunk(&id))),
                    Ok(Request::GetManifest(id)) => store.get_manifest(&id).map(Response::Manifest),
                    Ok(Request::PutChunk(id, bytes)) => {
                        match if payment.is_some() {
                            store.has_active_lease_for_chunk(&id)
                        } else {
                            Ok(true)
                        } {
                            Ok(true) => store.put_chunk(&id, &bytes).map(|_| Response::Stored),
                            Ok(false) => {
                                Err(anyhow::anyhow!("chunk has no active paid storage lease"))
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Ok(Request::PutManifest(m)) => store.put_manifest(&m).map(|_| Response::Stored),
                    Ok(Request::PutLease(lease)) => {
                        if let Some(config) = &payment {
                            match (&lease.payment_account, &lease.payer) {
                                (Some(account), Some(payer)) => {
                                    match solana::verify_lease(
                                        &config.rpc,
                                        &PaymentClaim {
                                            account,
                                            payer,
                                            provider: &config.provider_pubkey,
                                            manifest: &lease.manifest,
                                            expires_at: lease.expires_at,
                                        },
                                    )
                                    .await
                                    {
                                        Ok(()) => store.put_lease(&lease).map(|_| Response::Stored),
                                        Err(error) => Err(error),
                                    }
                                }
                                _ => Err(anyhow::anyhow!(
                                    "paid storage requires a verified Anchor lease account"
                                )),
                            }
                        } else {
                            store.put_lease(&lease).map(|_| Response::Stored)
                        }
                    }
                    Ok(Request::Status) => (|| {
                        Ok(Response::Status(NodeStatus {
                            used_bytes: store.used()?,
                            quota_bytes: store.quota(),
                            available_bytes: store.available()?,
                            provider_pubkey: payment.as_ref().map(|p| p.provider_pubkey.clone()),
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
                    Ok(Request::FindNode(id)) => {
                        if let Err(error) = validate_content_id(&id) {
                            Err(error)
                        } else {
                            let mut peers = dht.lock().await.closest(&crate::dht::key_id(&id));
                            peers.push(local.clone());
                            Ok(Response::Peers(peers))
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
                    Ok(Request::RegisterPeer(peer)) => {
                        if peer.id != Peer::from_address(peer.address, 0).id {
                            Err(anyhow::anyhow!("peer ID does not match address"))
                        } else {
                            dht.lock().await.add_peer(peer);
                            Ok(Response::Stored)
                        }
                    }
                    Ok(Request::Relay { target, request }) => {
                        if matches!(
                            *request,
                            Request::Relay { .. }
                                | Request::RegisterPeer(_)
                                | Request::RegisterTunnel(_)
                        ) {
                            Err(anyhow::anyhow!(
                                "nested relay or peer registration is forbidden"
                            ))
                        } else {
                            let tunnel = tunnels.lock().await.get(&target).cloned();
                            if let Some(tunnel) = tunnel {
                                let mut stream = tunnel.lock().await;
                                let result = async {
                                    send_frame(&mut stream, &*request).await?;
                                    timeout(REQUEST_TIMEOUT, recv_frame(&mut stream))
                                        .await
                                        .map_err(|_| anyhow::anyhow!("relay tunnel timed out"))?
                                }
                                .await;
                                if result.is_err() {
                                    tunnels.lock().await.remove(&target);
                                }
                                result
                            } else {
                                match dht.lock().await.peer(&target) {
                                    Some(peer) => self::request(peer.address, *request).await,
                                    None => Err(anyhow::anyhow!("relay target is not registered")),
                                }
                            }
                        }
                    }
                    Ok(Request::RegisterTunnel(_)) => {
                        Err(anyhow::anyhow!("invalid tunnel registration"))
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
            payment_account: None,
            replica_payments: HashMap::new(),
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

    #[tokio::test]
    async fn outbound_tunnel_forwards_chunk_requests() {
        let root = std::env::temp_dir().join(format!("bitmesh-relay-test-{}", std::process::id()));
        let relay_probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let relay = relay_probe.local_addr().unwrap();
        drop(relay_probe);
        let node_probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let node = node_probe.local_addr().unwrap();
        drop(node_probe);
        let relay_store = Store::open(root.join("relay"), 1024).unwrap();
        let node_store = Store::open(root.join("node"), 1024).unwrap();
        let bytes = b"through outbound tunnel";
        let id = hash_bytes(bytes);
        node_store.put_chunk(&id, bytes).unwrap();
        let relay_task = tokio::spawn(serve(relay, relay_store, 8));
        for _ in 0..50 {
            if matches!(request(relay, Request::Ping).await, Ok(Response::Pong)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let node_task = tokio::spawn(async move {
            serve_with_relay(node, node_store, 8, &[relay], Some(relay)).await
        });
        let mut received = false;
        for _ in 0..100 {
            if matches!(request(relay, Request::Relay {
                target: Peer::from_address(node, 0).id,
                request: Box::new(Request::GetChunk(id.clone())),
            }).await, Ok(Response::Chunk(found)) if found == bytes)
            {
                received = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(received);
        node_task.abort();
        relay_task.abort();
        let _ = fs::remove_dir_all(root);
    }
}
