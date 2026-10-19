use anyhow::{Context, Result, bail};
use bit_mesh::{
    DEFAULT_CHUNK_SIZE,
    manifest::{ChunkRef, Manifest},
    network::{self, Request, Response},
    store::{Lease, Store},
};
use clap::{Parser, Subcommand};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, Semaphore},
};

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
    },
    Upload {
        file: PathBuf,
        #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
        chunk_size: usize,
        #[arg(long, default_value_t = 2)]
        replication: usize,
        #[arg(long, value_delimiter = ',')]
        peers: Vec<SocketAddr>,
        #[arg(long, default_value_t = 604800)]
        lease_seconds: u64,
        #[arg(long)]
        payer: Option<String>,
        #[arg(long)]
        payment_signature: Option<String>,
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
        Command::Serve { listen } => {
            println!("bitmesh node listening on {listen}");
            network::serve(listen, store, 128).await
        }
        Command::Upload {
            file,
            chunk_size,
            replication,
            peers,
            lease_seconds,
            payer,
            payment_signature,
        } => {
            let (manifest, chunks) = Manifest::from_file(&file, chunk_size)?;
            for (reference, bytes) in manifest.chunks.iter().zip(&chunks) {
                store.put_chunk(&reference.hash, bytes)?;
            }
            let id = store.put_manifest(&manifest)?;
            let expiry = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs()
                .saturating_add(lease_seconds);
            store.put_lease(&Lease {
                manifest: id.clone(),
                expires_at: expiry,
                replication,
                payer,
                payment_signature,
            })?;
            replicate(&manifest, &chunks, replication, &peers).await?;
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
        } => {
            let m = store.get_manifest(&manifest)?;
            repair(&store, &m, replication, &peers).await
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

async fn replicate(
    m: &Manifest,
    chunks: &[Vec<u8>],
    desired: usize,
    peers: &[SocketAddr],
) -> Result<()> {
    if desired == 0 {
        bail!("replication must be at least one");
    }
    if desired <= 1 {
        return Ok(());
    }
    if peers.len() + 1 < desired {
        bail!(
            "need {} remote peers for replication {desired}, got {}",
            desired - 1,
            peers.len()
        );
    }
    for (chunk, bytes) in m.chunks.iter().zip(chunks) {
        let mut copies = 1;
        for peer in peers {
            if matches!(
                network::request(*peer, Request::HasChunk(chunk.hash.clone())).await,
                Ok(Response::Present(true))
            ) {
                copies += 1;
                if copies >= desired {
                    break;
                }
                continue;
            }
            if matches!(
                network::request(*peer, Request::PutChunk(chunk.hash.clone(), bytes.clone())).await,
                Ok(Response::Stored)
            ) {
                copies += 1;
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
    for peer in peers {
        let _ = network::request(*peer, Request::PutManifest(m.clone())).await;
    }
    Ok(())
}

async fn probe_sources(hash: &str, peers: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut tasks = tokio::task::JoinSet::new();
    for peer in peers.iter().copied() {
        let hash = hash.to_owned();
        tasks.spawn(async move {
            matches!(
                network::request(peer, Request::HasChunk(hash)).await,
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
            network::request(*peer, Request::GetChunk(chunk.hash.clone())).await
        {
            if bytes.len() as u64 == chunk.size
                && bit_mesh::manifest::hash_bytes(&bytes) == chunk.hash
            {
                store.put_chunk(&chunk.hash, &bytes)?;
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
    if desired == 0 {
        bail!("replication must be at least one");
    }
    for chunk in &manifest.chunks {
        let bytes = fetch_chunk(store, chunk, peers, peers.len().max(1)).await?;
        let mut sources = probe_sources(&chunk.hash, peers).await;
        let mut copies = 1 + sources.len();
        for peer in peers {
            if copies >= desired {
                break;
            }
            if sources.contains(peer) {
                continue;
            }
            if matches!(
                network::request(*peer, Request::PutChunk(chunk.hash.clone(), bytes.clone())).await,
                Ok(Response::Stored)
            ) {
                copies += 1;
                sources.push(*peer);
            }
        }
        if copies < desired {
            bail!(
                "chunk {} only has {copies}/{desired} confirmed replicas",
                chunk.hash
            );
        }
    }
    for peer in peers {
        let _ = network::request(*peer, Request::PutManifest(manifest.clone())).await;
    }
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
            for peer in peers {
                if let Ok(Response::Manifest(m)) =
                    network::request(*peer, Request::GetManifest(manifest_id.into())).await
                {
                    if m.validate().is_ok() && m.id().is_ok_and(|id| id == manifest_id) {
                        found = Some(m);
                        break;
                    }
                }
            }
            found.context("manifest unavailable locally and from peers")?
        }
    };
    store.put_manifest(&manifest)?;
    let sem = Arc::new(Semaphore::new(parallel.max(1)));
    let results = Arc::new(Mutex::new(HashMap::new()));
    let mut tasks = Vec::new();
    let mut jobs = Vec::with_capacity(manifest.chunks.len());
    for (index, chunk) in manifest.chunks.iter().cloned().enumerate() {
        let sources = if store.has_chunk(&chunk.hash) {
            Vec::new()
        } else {
            probe_sources(&chunk.hash, peers).await
        };
        jobs.push((index, chunk, sources));
    }
    jobs.sort_by_key(|(_, _, sources)| sources.len());
    for (index, chunk, sources) in jobs {
        let permit = sem.clone().acquire_owned().await?;
        let store = store.clone();
        let results = results.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let bytes = if let Ok(bytes) = store.get_chunk(&chunk.hash) {
                Some(bytes)
            } else {
                let mut value = None;
                for peer in sources.iter().cycle().take(retries.max(1)) {
                    if let Ok(Response::Chunk(bytes)) =
                        network::request(*peer, Request::GetChunk(chunk.hash.clone())).await
                    {
                        if bytes.len() as u64 == chunk.size
                            && bit_mesh::manifest::hash_bytes(&bytes) == chunk.hash
                        {
                            value = Some(bytes);
                            break;
                        }
                    }
                }
                value
            }
            .with_context(|| format!("chunk {} unavailable", chunk.hash))?;
            store.put_chunk(&chunk.hash, &bytes)?;
            results.lock().await.insert(index, bytes);
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    let mut file = File::create(output).await?;
    let mut chunks = results.lock().await;
    for i in 0..manifest.chunks.len() {
        file.write_all(
            chunks
                .remove(&i)
                .context("scheduler lost chunk")?
                .as_slice(),
        )
        .await?;
    }
    file.flush().await?;
    println!("wrote {} bytes to {}", manifest.size, output.display());
    Ok(())
}
