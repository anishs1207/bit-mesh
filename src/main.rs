use anyhow::{Context, Result, bail};
use bit_mesh::{
    DEFAULT_CHUNK_SIZE,
    dht::{Peer, distance, key_id},
    manifest::{ChunkRef, Manifest},
    network::{self, Request, Response},
    solana::{self, PaymentClaim},
    store::{Lease, Store},
};
use clap::{Parser, Subcommand};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, Semaphore},
};

#[derive(Default)]
struct PeerMetric {
    completed_bytes: u64,
    elapsed_ms: u128,
    failures: usize,
    in_flight: usize,
}

type PeerMetrics = Arc<Mutex<HashMap<[u8; 32], PeerMetric>>>;

async fn fetch_from_sources(
    chunk: &ChunkRef,
    sources: &[Peer],
    retries: usize,
    metrics: &PeerMetrics,
) -> Result<Vec<u8>> {
    let mut attempted = HashMap::<[u8; 32], usize>::new();
    for _ in 0..retries.max(1) {
        let peer = {
            let mut state = metrics.lock().await;
            let selected = sources
                .iter()
                .min_by_key(|peer| {
                    let metric = state.get(&peer.id);
                    let failures = metric.map_or(0, |m| m.failures);
                    let in_flight = metric.map_or(0, |m| m.in_flight);
                    let speed = metric.map_or(0, |m| {
                        m.completed_bytes.saturating_mul(1000) / (m.elapsed_ms.max(1) as u64)
                    });
                    (
                        attempted.get(&peer.id).copied().unwrap_or(0),
                        failures,
                        in_flight,
                        std::cmp::Reverse(speed),
                    )
                })
                .cloned();
            if let Some(peer) = &selected {
                state.entry(peer.id).or_default().in_flight += 1;
            }
            selected
        };
        let Some(peer) = peer else { break };
        *attempted.entry(peer.id).or_default() += 1;
        let started = Instant::now();
        let result = network::request_peer(&peer, Request::GetChunk(chunk.hash.clone())).await;
        let valid = match result {
            Ok(Response::Chunk(bytes))
                if bytes.len() as u64 == chunk.size
                    && bit_mesh::manifest::hash_bytes(&bytes) == chunk.hash =>
            {
                Some(bytes)
            }
            _ => None,
        };
        {
            let mut state = metrics.lock().await;
            let metric = state.entry(peer.id).or_default();
            metric.in_flight = metric.in_flight.saturating_sub(1);
            if valid.is_some() {
                metric.completed_bytes += chunk.size;
                metric.elapsed_ms += started.elapsed().as_millis().max(1);
            } else {
                metric.failures += 1;
            }
        }
        if let Some(bytes) = valid {
            return Ok(bytes);
        }
    }
    bail!("chunk {} is unavailable from valid replicas", chunk.hash)
}

#[derive(Parser)]
#[command(version, about = "Content-addressed peer-to-peer storage")]
struct Cli {
    #[arg(long, default_value = ".bitmesh")]
    data: PathBuf,
    #[arg(long, default_value_t = 10 * 1024 * 1024 * 1024u64)]
    quota: u64,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "127.0.0.1:7400")]
        listen: SocketAddr,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        #[arg(long)]
        relay: Option<SocketAddr>,
        #[arg(long)]
        solana_rpc: Option<String>,
        #[arg(long)]
        provider_pubkey: Option<String>,
        #[arg(long)]
        repair_interval_seconds: Option<u64>,
    },
    Prepare {
        file: PathBuf,
        #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
        chunk_size: usize,
    },
    Upload {
        file: PathBuf,
        #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
        chunk_size: usize,
        #[arg(long)]
        prepared_manifest: Option<String>,
        #[arg(long, default_value_t = 2)]
        replication: usize,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        #[arg(long, default_value_t = 604800)]
        lease_seconds: u64,
        #[arg(long)]
        payer: Option<String>,
        #[arg(long)]
        solana_rpc: Option<String>,
        #[arg(long = "payment-account")]
        payment_accounts: Vec<String>,
    },
    Download {
        manifest: String,
        output: PathBuf,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        #[arg(long, default_value_t = 8)]
        parallel: usize,
        #[arg(long, default_value_t = 3)]
        retries: usize,
    },
    Inspect {
        manifest: String,
    },
    Repair {
        manifest: String,
        #[arg(long, default_value_t = 2)]
        replication: usize,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        #[arg(long = "payment-account")]
        payment_accounts: Vec<String>,
        #[arg(long)]
        solana_rpc: Option<String>,
    },
    Status {
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
    },
    Gc,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let store = Store::open(&cli.data, cli.quota)?;
    match cli.command {
        Command::Serve {
            listen,
            peers,
            relay,
            solana_rpc,
            provider_pubkey,
            repair_interval_seconds,
        } => {
            let payment = match (solana_rpc, provider_pubkey) {
                (Some(rpc), Some(provider_pubkey)) => Some(network::PaymentConfig {
                    rpc,
                    provider_pubkey,
                }),
                (None, None) => None,
                _ => bail!("serve requires both --solana-rpc and --provider-pubkey"),
            };
            if let Some(seconds) = repair_interval_seconds {
                if seconds == 0 || peers.is_empty() {
                    bail!("automatic repair needs a positive interval and at least one peer");
                }
                let repair_store = store.clone();
                let repair_peers = peers.clone();
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(seconds));
                    interval.tick().await;
                    loop {
                        interval.tick().await;
                        match repair_store.active_leases() {
                            Ok(leases) => {
                                for lease in leases {
                                    match repair_store.get_manifest(&lease.manifest) {
                                        Ok(manifest) => {
                                            if let Err(error) = repair(
                                                &repair_store,
                                                &manifest,
                                                lease.replication,
                                                &repair_peers,
                                            )
                                            .await
                                            {
                                                eprintln!(
                                                    "repair {} failed: {error:#}",
                                                    lease.manifest
                                                );
                                            }
                                        }
                                        Err(error) => {
                                            eprintln!("repair {} failed: {error:#}", lease.manifest)
                                        }
                                    }
                                }
                            }
                            Err(error) => eprintln!("repair scan failed: {error:#}"),
                        }
                    }
                });
            }
            println!("bitmesh node listening on {listen}");
            network::serve_configured(listen, store, 128, &peers, relay, payment).await
        }
        Command::Prepare { file, chunk_size } => {
            let manifest = Manifest::from_file_with(&file, chunk_size, |reference, bytes| {
                store.put_chunk(&reference.hash, bytes)?;
                Ok(())
            })?;
            println!("{}", store.put_manifest(&manifest)?);
            Ok(())
        }
        Command::Upload {
            file,
            chunk_size,
            prepared_manifest,
            replication,
            peers,
            lease_seconds,
            payer,
            solana_rpc,
            payment_accounts,
        } => {
            if solana_rpc.is_some() && prepared_manifest.is_none() {
                bail!(
                    "paid upload requires --prepared-manifest; run prepare before creating Anchor leases"
                );
            }
            if payer.is_some() != solana_rpc.is_some() {
                bail!("--payer and --solana-rpc must be supplied together");
            }
            let prepared = prepared_manifest
                .as_deref()
                .map(|id| store.get_manifest(id))
                .transpose()?;
            let read_chunk_size = prepared.as_ref().map_or(chunk_size, |m| m.chunk_size);
            let generated =
                Manifest::from_file_with(&file, read_chunk_size, |reference, bytes| {
                    store.put_chunk(&reference.hash, bytes)?;
                    Ok(())
                })?;
            let manifest = if let Some(manifest) = prepared {
                if generated.chunks != manifest.chunks || generated.size != manifest.size {
                    bail!("file bytes do not match prepared manifest");
                }
                manifest
            } else {
                generated
            };
            let id = store.put_manifest(&manifest)?;
            let expiry = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs()
                .saturating_add(lease_seconds);
            let mut paid = parse_payment_accounts(&payment_accounts)?;
            if solana_rpc.is_some() != !paid.is_empty() {
                bail!("upload requires both --solana-rpc and --payment-account for paid storage");
            }
            if let Some(rpc) = &solana_rpc {
                let payer = payer.as_deref().context("paid upload requires --payer")?;
                let contacts = contact_map(&id, &peers).await;
                let mut verified = HashMap::new();
                for (peer, account) in &paid {
                    let provider = match request_address(*peer, &contacts, Request::Status).await {
                        Ok(Response::Status(status)) => status.provider_pubkey,
                        _ => None,
                    };
                    if let Some(provider) = provider {
                        match solana::verify_lease(
                            rpc,
                            &PaymentClaim {
                                account,
                                payer,
                                provider: &provider,
                                manifest: &id,
                                expires_at: expiry,
                            },
                        )
                        .await
                        {
                            Ok(()) => {
                                verified.insert(*peer, account.clone());
                            }
                            Err(error) => eprintln!("payment lease for {peer} rejected: {error:#}"),
                        }
                    }
                }
                paid = verified;
                if paid.is_empty() {
                    bail!("no paid storage peers passed lease verification");
                }
            }
            let lease = Lease {
                manifest: id.clone(),
                expires_at: expiry,
                replication,
                payer: payer.clone(),
                payment_signature: None,
                payment_account: None,
                replica_payments: paid.clone(),
            };
            replicate_paid(&store, &manifest, &lease, &peers, &paid, payer.as_deref()).await?;
            store.put_lease(&lease)?;
            println!("{id}");
            Ok(())
        }
        Command::Download {
            manifest,
            output,
            peers,
            parallel,
            retries,
        } => download(&store, &manifest, &output, &peers, parallel, retries).await,
        Command::Inspect { manifest } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&store.get_manifest(&manifest)?)?
            );
            Ok(())
        }
        Command::Repair {
            manifest,
            replication,
            peers,
            payment_accounts,
            solana_rpc,
        } => {
            let m = store.get_manifest(&manifest)?;
            let mut added = parse_payment_accounts(&payment_accounts)?;
            if !added.is_empty() {
                let rpc = solana_rpc
                    .as_deref()
                    .context("new payment accounts require --solana-rpc")?;
                let local_lease = store
                    .active_leases()?
                    .into_iter()
                    .find(|lease| lease.manifest == manifest)
                    .context("paid repair requires an active local lease")?;
                let payer = local_lease
                    .payer
                    .as_deref()
                    .context("paid repair requires a local lease with payer")?;
                let contacts = contact_map(&manifest, &peers).await;
                let mut verified = HashMap::new();
                for (peer, account) in &added {
                    let provider = match request_address(*peer, &contacts, Request::Status).await {
                        Ok(Response::Status(status)) => status.provider_pubkey,
                        _ => None,
                    };
                    if let Some(provider) = provider {
                        match solana::verify_lease(
                            rpc,
                            &PaymentClaim {
                                account,
                                payer,
                                provider: &provider,
                                manifest: &manifest,
                                expires_at: local_lease.expires_at,
                            },
                        )
                        .await
                        {
                            Ok(()) => {
                                verified.insert(*peer, account.clone());
                            }
                            Err(error) => eprintln!(
                                "replacement payment lease for {peer} rejected: {error:#}"
                            ),
                        }
                    }
                }
                added = verified;
                if added.is_empty() {
                    bail!("no replacement payment accounts passed verification");
                }
            }
            repair_paid(&store, &m, replication, &peers, &added).await
        }
        Command::Status { peers } => {
            println!("local: {} / {} bytes used", store.used()?, store.quota());
            for peer in peers {
                match network::request(peer, Request::Status).await {
                    Ok(Response::Status(status)) => println!(
                        "{peer}: {} / {} bytes used ({} available)",
                        status.used_bytes, status.quota_bytes, status.available_bytes
                    ),
                    Ok(Response::Error(error)) => println!("{peer}: error: {error}"),
                    _ => println!("{peer}: unavailable"),
                }
            }
            Ok(())
        }
        Command::Gc => {
            let (count, bytes) = store.garbage_collect()?;
            println!("removed {count} chunks ({bytes} bytes)");
            Ok(())
        }
    }
}

fn parse_payment_accounts(values: &[String]) -> Result<HashMap<SocketAddr, String>> {
    let mut accounts = HashMap::new();
    for value in values {
        let (address, account) = value
            .split_once('=')
            .context("payment account must be PEER=SOLANA_ACCOUNT")?;
        accounts.insert(address.parse()?, account.to_owned());
    }
    Ok(accounts)
}

#[cfg(test)]
async fn replicate(store: &Store, m: &Manifest, lease: &Lease, peers: &[SocketAddr]) -> Result<()> {
    replicate_paid(store, m, lease, peers, &HashMap::new(), None).await
}

async fn replicate_paid(
    store: &Store,
    m: &Manifest,
    lease: &Lease,
    peers: &[SocketAddr],
    paid: &HashMap<SocketAddr, String>,
    payer: Option<&str>,
) -> Result<()> {
    let eligible: Vec<_> = peers
        .iter()
        .copied()
        .filter(|peer| paid.is_empty() || paid.contains_key(peer))
        .collect();
    let desired = lease.replication;
    if desired == 0 {
        bail!("replication must be at least one");
    }
    if desired <= 1 {
        return Ok(());
    }
    if eligible.len() + 1 < desired {
        bail!(
            "need {} remote peers for replication {desired}, got {}",
            desired - 1,
            eligible.len()
        );
    }
    let contacts = contact_map(&m.id()?, peers).await;
    let eligible = if !paid.is_empty() {
        let mut prepared = Vec::new();
        for peer in &eligible {
            if !matches!(
                request_address(*peer, &contacts, Request::PutManifest(m.clone())).await,
                Ok(Response::Stored)
            ) {
                continue;
            }
            let mut remote_lease = lease.clone();
            remote_lease.payer = payer.map(str::to_owned);
            remote_lease.payment_account = paid.get(peer).cloned();
            if !matches!(
                request_address(*peer, &contacts, Request::PutLease(remote_lease)).await,
                Ok(Response::Stored)
            ) {
                continue;
            }
            prepared.push(*peer);
        }
        if prepared.len() + 1 < desired {
            bail!("too few paid storage peers accepted their leases");
        }
        prepared
    } else {
        eligible
    };
    let mut placements: HashMap<String, Vec<SocketAddr>> = HashMap::new();
    for chunk in &m.chunks {
        let bytes = store.get_chunk(&chunk.hash)?;
        let mut copies = 1;
        let mut counted = HashSet::new();
        for peer in &eligible {
            if !counted.insert(*peer) {
                continue;
            }
            if matches!(
                request_address(*peer, &contacts, Request::HasChunk(chunk.hash.clone())).await,
                Ok(Response::Present(true))
            ) {
                copies += 1;
                placements
                    .entry(chunk.hash.clone())
                    .or_default()
                    .push(*peer);
                if copies >= desired {
                    break;
                }
                continue;
            }
            let has_capacity = matches!(
                request_address(*peer, &contacts, Request::Status).await,
                Ok(Response::Status(status)) if status.available_bytes >= chunk.size
            );
            if !has_capacity {
                continue;
            }
            if matches!(
                request_address(
                    *peer,
                    &contacts,
                    Request::PutChunk(chunk.hash.clone(), bytes.clone())
                )
                .await,
                Ok(Response::Stored)
            ) {
                copies += 1;
                placements
                    .entry(chunk.hash.clone())
                    .or_default()
                    .push(*peer);
            }
            if copies >= desired {
                break;
            }
        }
        if copies < desired {
            bail!(
                "chunk {} only has {copies}/{desired} confirmed replicas",
                chunk.hash
            );
        }
    }
    let mut manifest_holders = Vec::new();
    for peer in &eligible {
        let response = request_address(*peer, &contacts, Request::PutManifest(m.clone())).await;
        if matches!(&response, Ok(Response::Stored)) {
            manifest_holders.push(*peer);
        }
        if placements.values().any(|holders| holders.contains(peer)) {
            if !matches!(response, Ok(Response::Stored)) {
                bail!("failed to publish manifest to replica {peer}");
            }
            let mut remote_lease = lease.clone();
            if let Some(account) = paid.get(peer) {
                remote_lease.payer = payer.map(str::to_owned);
                remote_lease.payment_account = Some(account.clone());
            }
            if !matches!(
                request_address(*peer, &contacts, Request::PutLease(remote_lease)).await,
                Ok(Response::Stored)
            ) {
                bail!("failed to pin lease on replica {peer}");
            }
        }
    }
    for (content, providers) in placements {
        let routing = discover_peers(&content, peers).await;
        for seed in &routing {
            for address in &providers {
                let _ = network::request_peer(
                    seed,
                    Request::AnnounceProvider {
                        content: content.clone(),
                        peer: contacts
                            .get(address)
                            .cloned()
                            .unwrap_or_else(|| Peer::from_address(*address, 0)),
                    },
                )
                .await;
            }
        }
    }
    let manifest_id = m.id()?;
    for node in discover_peers(&manifest_id, peers).await {
        for address in &manifest_holders {
            let _ = network::request_peer(
                &node,
                Request::AnnounceProvider {
                    content: manifest_id.clone(),
                    peer: contacts
                        .get(address)
                        .cloned()
                        .unwrap_or_else(|| Peer::from_address(*address, 0)),
                },
            )
            .await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, net::TcpListener as StdTcpListener};

    fn free_address() -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn discovers_chunk_holder_through_routing_peer() {
        let root = std::env::temp_dir().join(format!("bitmesh-lookup-test-{}", std::process::id()));
        let middle = free_address();
        let holder = free_address();
        let a = Store::open(root.join("middle"), 100).unwrap();
        let b = Store::open(root.join("holder"), 100).unwrap();
        let bytes = b"through a routing peer";
        let id = bit_mesh::manifest::hash_bytes(bytes);
        b.put_chunk(&id, bytes).unwrap();
        let middle_task =
            tokio::spawn(async move { network::serve_with_peers(middle, a, 4, &[holder]).await });
        let holder_task = tokio::spawn(network::serve(holder, b, 4));
        for _ in 0..30 {
            if matches!(
                network::request(middle, Request::Ping).await,
                Ok(Response::Pong)
            ) && matches!(
                network::request(holder, Request::Ping).await,
                Ok(Response::Pong)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let sources = probe_sources(&id, &[middle]).await;
        assert!(sources.iter().any(|peer| peer.address == holder));
        middle_task.abort();
        holder_task.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn repair_restores_replica_and_pins_it() {
        let root = std::env::temp_dir().join(format!("bitmesh-repair-test-{}", std::process::id()));
        let first = free_address();
        let replacement = free_address();
        let local = Store::open(root.join("local"), 100).unwrap();
        let existing = Store::open(root.join("existing"), 100).unwrap();
        let new_store = Store::open(root.join("replacement"), 100).unwrap();
        let bytes = b"a valid surviving copy";
        let hash = bit_mesh::manifest::hash_bytes(bytes);
        local.put_chunk(&hash, bytes).unwrap();
        existing.put_chunk(&hash, bytes).unwrap();
        let manifest = Manifest {
            version: 1,
            name: "file".into(),
            size: bytes.len() as u64,
            chunk_size: bytes.len(),
            chunks: vec![ChunkRef {
                hash: hash.clone(),
                size: bytes.len() as u64,
            }],
            created_at: 0,
        };
        let id = local.put_manifest(&manifest).unwrap();
        local
            .put_lease(&Lease {
                manifest: id,
                expires_at: u64::MAX,
                replication: 3,
                payer: None,
                payment_signature: None,
                payment_account: None,
                replica_payments: HashMap::new(),
            })
            .unwrap();
        let first_task = tokio::spawn(network::serve(first, existing, 4));
        let replacement_task = tokio::spawn(network::serve(replacement, new_store.clone(), 4));
        for _ in 0..30 {
            if matches!(
                network::request(first, Request::Ping).await,
                Ok(Response::Pong)
            ) && matches!(
                network::request(replacement, Request::Ping).await,
                Ok(Response::Pong)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        repair(&local, &manifest, 3, &[first, replacement])
            .await
            .unwrap();
        assert!(new_store.has_chunk(&hash));
        assert_eq!(new_store.garbage_collect().unwrap().0, 0);
        first_task.abort();
        replacement_task.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn repair_works_when_coordinator_has_no_chunk_capacity() {
        let root =
            std::env::temp_dir().join(format!("bitmesh-no-local-copy-{}", std::process::id()));
        let holder = free_address();
        let replacement = free_address();
        let coordinator = Store::open(root.join("coordinator"), 0).unwrap();
        let holder_store = Store::open(root.join("holder"), 100).unwrap();
        let replacement_store = Store::open(root.join("replacement"), 100).unwrap();
        let bytes = b"replicated without a local copy";
        let hash = bit_mesh::manifest::hash_bytes(bytes);
        holder_store.put_chunk(&hash, bytes).unwrap();
        let manifest = Manifest {
            version: 1,
            name: "file".into(),
            size: bytes.len() as u64,
            chunk_size: bytes.len(),
            chunks: vec![ChunkRef {
                hash: hash.clone(),
                size: bytes.len() as u64,
            }],
            created_at: 0,
        };
        let id = coordinator.put_manifest(&manifest).unwrap();
        coordinator
            .put_lease(&Lease {
                manifest: id,
                expires_at: u64::MAX,
                replication: 2,
                payer: None,
                payment_signature: None,
                payment_account: None,
                replica_payments: HashMap::new(),
            })
            .unwrap();
        let holder_task = tokio::spawn(network::serve(holder, holder_store, 4));
        let replacement_task =
            tokio::spawn(network::serve(replacement, replacement_store.clone(), 4));
        for _ in 0..30 {
            if matches!(
                network::request(holder, Request::Ping).await,
                Ok(Response::Pong)
            ) && matches!(
                network::request(replacement, Request::Ping).await,
                Ok(Response::Pong)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        repair(&coordinator, &manifest, 2, &[holder, replacement])
            .await
            .unwrap();
        assert!(!coordinator.has_chunk(&hash));
        assert!(replacement_store.has_chunk(&hash));
        holder_task.abort();
        replacement_task.abort();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn upload_then_download_through_seed() {
        let root =
            std::env::temp_dir().join(format!("bitmesh-transfer-test-{}", std::process::id()));
        let source = root.join("source.bin");
        let output = root.join("output.bin");
        fs::create_dir_all(&root).unwrap();
        fs::write(&source, b"abcabcxyz").unwrap();
        let uploader = Store::open(root.join("uploader"), 100).unwrap();
        let replica = Store::open(root.join("replica"), 100).unwrap();
        let downloader = Store::open(root.join("downloader"), 100).unwrap();
        let address = free_address();
        let server = tokio::spawn(network::serve(address, replica.clone(), 4));
        for _ in 0..30 {
            if matches!(
                network::request(address, Request::Ping).await,
                Ok(Response::Pong)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let manifest = Manifest::from_file_with(&source, 3, |chunk, bytes| {
            uploader.put_chunk(&chunk.hash, bytes)?;
            Ok(())
        })
        .unwrap();
        let id = uploader.put_manifest(&manifest).unwrap();
        let lease = Lease {
            manifest: id.clone(),
            expires_at: u64::MAX,
            replication: 2,
            payer: None,
            payment_signature: None,
            payment_account: None,
            replica_payments: HashMap::new(),
        };
        replicate(&uploader, &manifest, &lease, &[address])
            .await
            .unwrap();
        uploader.put_lease(&lease).unwrap();
        assert_eq!(replica.garbage_collect().unwrap().0, 0);
        download(&downloader, &id, &output, &[address], 2, 2)
            .await
            .unwrap();
        assert_eq!(fs::read(&source).unwrap(), fs::read(&output).unwrap());
        server.abort();
        let _ = fs::remove_dir_all(root);
    }
}

async fn probe_sources(hash: &str, peers: &[SocketAddr]) -> Vec<Peer> {
    let discovered = discover_peers(hash, peers).await;
    let mut candidates: HashMap<_, _> = discovered
        .iter()
        .cloned()
        .map(|peer| (peer.id, peer))
        .collect();
    let mut lookups = tokio::task::JoinSet::new();
    for seed in discovered {
        let hash = hash.to_owned();
        lookups
            .spawn(async move { network::request_peer(&seed, Request::FindProviders(hash)).await });
    }
    while let Some(result) = lookups.join_next().await {
        if let Ok(Ok(Response::Providers(providers))) = result {
            for provider in providers {
                candidates.insert(provider.id, provider);
            }
        }
    }
    let mut tasks = tokio::task::JoinSet::new();
    for peer in candidates.into_values() {
        let hash = hash.to_owned();
        tasks.spawn(async move {
            matches!(
                network::request_peer(&peer, Request::HasChunk(hash)).await,
                Ok(Response::Present(true))
            )
            .then_some(peer)
        });
    }
    let mut sources = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Ok(Some(peer)) = result {
            sources.push(peer);
        }
    }
    sources
}

async fn discover_peers(key: &str, seeds: &[SocketAddr]) -> Vec<Peer> {
    let target = key_id(key);
    let mut known: HashMap<_, _> = seeds
        .iter()
        .copied()
        .map(|address| {
            let peer = Peer::from_address(address, 0);
            (peer.id, peer)
        })
        .collect();
    let mut queried = HashSet::new();
    while queried.len() < 64 {
        let mut next: Vec<_> = known
            .values()
            .filter(|peer| !queried.contains(&peer.id))
            .cloned()
            .collect();
        next.sort_by_key(|peer| distance(&peer.id, &target));
        next.truncate(3.min(64 - queried.len()));
        if next.is_empty() {
            break;
        }
        let mut tasks = tokio::task::JoinSet::new();
        for peer in next {
            queried.insert(peer.id);
            let key = key.to_owned();
            tasks.spawn(async move { network::request_peer(&peer, Request::FindNode(key)).await });
        }
        while let Some(result) = tasks.join_next().await {
            if let Ok(Ok(Response::Peers(peers))) = result {
                for peer in peers {
                    if peer.id == Peer::from_address(peer.address, 0).id && known.len() < 128 {
                        known
                            .entry(peer.id)
                            .and_modify(|existing| {
                                if existing.relay.is_none() && peer.relay.is_some() {
                                    *existing = peer.clone();
                                }
                            })
                            .or_insert(peer);
                    }
                }
            }
        }
    }
    known.into_values().collect()
}

async fn contact_map(key: &str, seeds: &[SocketAddr]) -> HashMap<SocketAddr, Peer> {
    discover_peers(key, seeds)
        .await
        .into_iter()
        .map(|peer| (peer.address, peer))
        .collect()
}

async fn request_address(
    address: SocketAddr,
    contacts: &HashMap<SocketAddr, Peer>,
    request: Request,
) -> Result<Response> {
    match contacts.get(&address) {
        Some(peer) => network::request_peer(peer, request).await,
        None => network::request(address, request).await,
    }
}

async fn fetch_chunk(
    store: &Store,
    chunk: &ChunkRef,
    peers: &[SocketAddr],
    retries: usize,
) -> Result<Vec<u8>> {
    if let Ok(bytes) = store.get_chunk(&chunk.hash) {
        if bytes.len() as u64 == chunk.size {
            return Ok(bytes);
        }
    }
    let sources = probe_sources(&chunk.hash, peers).await;
    for peer in sources.iter().cycle().take(retries.max(1)) {
        if let Ok(Response::Chunk(bytes)) =
            network::request_peer(peer, Request::GetChunk(chunk.hash.clone())).await
        {
            if bytes.len() as u64 == chunk.size
                && bit_mesh::manifest::hash_bytes(&bytes) == chunk.hash
            {
                return Ok(bytes);
            }
        }
    }
    bail!("chunk {} is unavailable from valid replicas", chunk.hash)
}

async fn repair(
    store: &Store,
    manifest: &Manifest,
    desired: usize,
    peers: &[SocketAddr],
) -> Result<()> {
    repair_paid(store, manifest, desired, peers, &HashMap::new()).await
}

async fn repair_paid(
    store: &Store,
    manifest: &Manifest,
    desired: usize,
    peers: &[SocketAddr],
    added: &HashMap<SocketAddr, String>,
) -> Result<()> {
    if desired == 0 {
        bail!("replication must be at least one");
    }
    let id = manifest.id()?;
    let mut lease = store
        .active_leases()?
        .into_iter()
        .find(|lease| lease.manifest == id)
        .context("repair requires an active local lease")?;
    lease.replica_payments.extend(added.clone());
    let contacts = contact_map(&id, peers).await;
    let eligible: Vec<_> = peers
        .iter()
        .copied()
        .filter(|peer| {
            lease.replica_payments.is_empty() || lease.replica_payments.contains_key(peer)
        })
        .collect();
    lease.replication = desired;
    let eligible = if !lease.replica_payments.is_empty() {
        let mut prepared = Vec::new();
        for peer in &eligible {
            if !matches!(
                request_address(*peer, &contacts, Request::PutManifest(manifest.clone())).await,
                Ok(Response::Stored)
            ) {
                continue;
            }
            let mut remote_lease = lease.clone();
            remote_lease.payment_account = lease.replica_payments.get(peer).cloned();
            if !matches!(
                request_address(*peer, &contacts, Request::PutLease(remote_lease)).await,
                Ok(Response::Stored)
            ) {
                continue;
            }
            prepared.push(*peer);
        }
        prepared
    } else {
        eligible
    };
    for chunk in &manifest.chunks {
        let bytes = fetch_chunk(store, chunk, peers, peers.len().max(1)).await?;
        if !store.has_chunk(&chunk.hash) && store.available()? >= chunk.size {
            let _ = store.put_chunk(&chunk.hash, &bytes);
        }
        let local_copy = usize::from(store.has_chunk(&chunk.hash));
        let mut sources: Vec<_> = probe_sources(&chunk.hash, peers)
            .await
            .into_iter()
            .filter(|source| eligible.contains(&source.address))
            .collect();
        let mut copies = local_copy + sources.len();
        let mut attempted: HashSet<_> = sources.iter().map(|source| source.address).collect();
        for peer in &eligible {
            if copies >= desired {
                break;
            }
            if !attempted.insert(*peer) {
                continue;
            }
            let has_capacity = matches!(
                request_address(*peer, &contacts, Request::Status).await,
                Ok(Response::Status(status)) if status.available_bytes >= chunk.size
            );
            if !has_capacity {
                continue;
            }
            if matches!(
                request_address(
                    *peer,
                    &contacts,
                    Request::PutChunk(chunk.hash.clone(), bytes.clone())
                )
                .await,
                Ok(Response::Stored)
            ) {
                copies += 1;
                sources.push(
                    contacts
                        .get(peer)
                        .cloned()
                        .unwrap_or_else(|| Peer::from_address(*peer, 0)),
                );
            }
        }
        if copies < desired {
            bail!(
                "chunk {} only has {copies}/{desired} confirmed replicas",
                chunk.hash
            );
        }
        for node in discover_peers(&chunk.hash, peers).await {
            for provider in &sources {
                let _ = network::request_peer(
                    &node,
                    Request::AnnounceProvider {
                        content: chunk.hash.clone(),
                        peer: provider.clone(),
                    },
                )
                .await;
            }
        }
    }
    for peer in &eligible {
        if matches!(
            request_address(*peer, &contacts, Request::PutManifest(manifest.clone())).await,
            Ok(Response::Stored)
        ) {
            let mut remote_lease = lease.clone();
            remote_lease.payment_account = lease.replica_payments.get(peer).cloned();
            if !matches!(
                request_address(*peer, &contacts, Request::PutLease(remote_lease)).await,
                Ok(Response::Stored)
            ) {
                bail!("failed to renew lease on replica {peer}");
            }
        }
    }
    store.put_lease(&lease)?;
    Ok(())
}

async fn download(
    store: &Store,
    manifest_id: &str,
    output: &PathBuf,
    peers: &[SocketAddr],
    parallel: usize,
    retries: usize,
) -> Result<()> {
    let manifest = match store.get_manifest(manifest_id) {
        Ok(m) => m,
        Err(_) => {
            let mut found = None;
            let mut candidates = discover_peers(manifest_id, peers).await;
            let routing = candidates.clone();
            for peer in routing {
                if let Ok(Response::Providers(providers)) =
                    network::request_peer(&peer, Request::FindProviders(manifest_id.into())).await
                {
                    candidates.extend(providers);
                }
            }
            let mut tried = HashSet::new();
            for peer in candidates {
                if tried.insert(peer.id) {
                    if let Ok(Response::Manifest(m)) =
                        network::request_peer(&peer, Request::GetManifest(manifest_id.into())).await
                    {
                        if m.validate().is_ok() && m.id().is_ok_and(|id| id == manifest_id) {
                            found = Some(m);
                            break;
                        }
                    }
                }
            }
            found.context("manifest unavailable locally and from peers")?
        }
    };
    store.put_manifest(&manifest)?;
    let sem = Arc::new(Semaphore::new(parallel.max(1)));
    let metrics: PeerMetrics = Arc::new(Mutex::new(HashMap::new()));
    let mut tasks = Vec::new();
    let discovery_limit = Arc::new(Semaphore::new(parallel.max(1) * 2));
    let mut discoveries = tokio::task::JoinSet::new();
    let mut missing = HashSet::new();
    for chunk in &manifest.chunks {
        if !store.has_chunk(&chunk.hash) && missing.insert(chunk.hash.clone()) {
            let permit = discovery_limit.clone().acquire_owned().await?;
            let hash = chunk.hash.clone();
            let seeds = peers.to_vec();
            discoveries.spawn(async move {
                let _permit = permit;
                let sources = probe_sources(&hash, &seeds).await;
                (hash, sources)
            });
        }
    }
    let mut sources_by_hash = HashMap::new();
    while let Some(result) = discoveries.join_next().await {
        let (hash, sources) = result?;
        sources_by_hash.insert(hash, sources);
    }
    let mut jobs = Vec::with_capacity(manifest.chunks.len());
    for chunk in manifest.chunks.iter().cloned() {
        let sources = sources_by_hash
            .get(&chunk.hash)
            .cloned()
            .unwrap_or_default();
        jobs.push((chunk, sources));
    }
    jobs.sort_by_key(|(_, sources)| sources.len());
    for (chunk, sources) in jobs {
        let permit = sem.clone().acquire_owned().await?;
        let store = store.clone();
        let metrics = metrics.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let bytes = if let Ok(bytes) = store.get_chunk(&chunk.hash) {
                bytes
            } else {
                fetch_from_sources(&chunk, &sources, retries, &metrics).await?
            };
            store.put_chunk(&chunk.hash, &bytes)?;
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    let mut file = File::create(output).await?;
    for chunk in &manifest.chunks {
        let bytes = store.get_chunk(&chunk.hash)?;
        file.write_all(&bytes).await?;
    }
    file.flush().await?;
    println!("wrote {} bytes to {}", manifest.size, output.display());
    Ok(())
}
