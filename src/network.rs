use crate::{
    manifest::{ContentId, Manifest},
    store::Store,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    GetChunk(ContentId),
    HasChunk(ContentId),
    GetManifest(ContentId),
    PutChunk(ContentId, Vec<u8>),
    PutManifest(Manifest),
    Ping,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Chunk(Vec<u8>),
    Manifest(Manifest),
    Present(bool),
    Stored,
    Pong,
    Error(String),
}
async fn send_frame(stream: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > 64 * 1024 * 1024 {
        bail!("frame too large");
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}
async fn recv_frame<T: for<'a> Deserialize<'a>>(stream: &mut TcpStream) -> Result<T> {
    let len = stream.read_u32().await? as usize;
    if len > 64 * 1024 * 1024 {
        bail!("frame too large");
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
pub async fn request(addr: SocketAddr, request: Request) -> Result<Response> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to {addr}"))?;
    send_frame(&mut stream, &request).await?;
    recv_frame(&mut stream).await
}
pub async fn serve(addr: SocketAddr, store: Store, max_connections: usize) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let limit = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        let (mut stream, _) = listener.accept().await?;
        let store = store.clone();
        let permit = limit.clone().acquire_owned().await?;
        tokio::spawn(async move {
            let response = match recv_frame::<Request>(&mut stream).await {
                Ok(Request::GetChunk(id)) => store.get_chunk(&id).map(Response::Chunk),
                Ok(Request::HasChunk(id)) => Ok(Response::Present(store.has_chunk(&id))),
                Ok(Request::GetManifest(id)) => store.get_manifest(&id).map(Response::Manifest),
                Ok(Request::PutChunk(id, bytes)) => {
                    store.put_chunk(&id, &bytes).map(|_| Response::Stored)
                }
                Ok(Request::PutManifest(m)) => store.put_manifest(&m).map(|_| Response::Stored),
                Ok(Request::Ping) => Ok(Response::Pong),
                Err(e) => Err(e),
            }
            .unwrap_or_else(|e| Response::Error(e.to_string()));
            let _ = send_frame(&mut stream, &response).await;
            drop(permit);
        });
    }
}
