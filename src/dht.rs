use crate::manifest::ContentId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    time::{Duration, Instant},
};

pub type NodeId = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    pub id: NodeId,
    pub address: SocketAddr,
    pub relay: Option<SocketAddr>,
    pub capacity_bytes: u64,
}

impl Peer {
    pub fn from_address(address: SocketAddr, capacity_bytes: u64) -> Self {
        Self {
            id: sha2_bytes(address.to_string().as_bytes()),
            address,
            relay: None,
            capacity_bytes,
        }
    }
}

fn distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut out = [0; 32];
    for i in 0..32 {
        out[i] = a[i] ^ b[i];
    }
    out
}

pub fn key_id(key: &str) -> NodeId {
    let bytes = hex::decode(key).unwrap_or_else(|_| sha2_bytes(key.as_bytes()).to_vec());
    sha2_bytes(&bytes)
}

fn sha2_bytes(bytes: &[u8]) -> NodeId {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// Compact Kademlia routing/provider table. Network RPCs live in `network`;
/// this type implements XOR-distance selection and expiring provider records.
pub struct Dht {
    local_id: NodeId,
    k: usize,
    peers: BTreeMap<NodeId, Peer>,
    providers: HashMap<ContentId, Vec<(Peer, Instant)>>,
}

impl Dht {
    pub fn new(local_id: NodeId, k: usize) -> Self {
        Self {
            local_id,
            k: k.max(1),
            peers: BTreeMap::new(),
            providers: HashMap::new(),
        }
    }
    pub fn add_peer(&mut self, peer: Peer) {
        if peer.id != self.local_id {
            self.peers.insert(peer.id, peer);
        }
    }
    pub fn closest(&self, target: &NodeId) -> Vec<Peer> {
        let mut peers: Vec<_> = self.peers.values().cloned().collect();
        peers.sort_by_key(|p| distance(&p.id, target));
        peers.truncate(self.k);
        peers
    }
    pub fn announce(&mut self, key: ContentId, peer: Peer, ttl: Duration) {
        let expiry = Instant::now() + ttl;
        let records = self.providers.entry(key).or_default();
        records.retain(|(p, _)| p.id != peer.id);
        records.push((peer, expiry));
    }
    pub fn providers(&mut self, key: &str) -> Vec<Peer> {
        let now = Instant::now();
        let records = self.providers.entry(key.to_owned()).or_default();
        records.retain(|(_, expiry)| *expiry > now);
        records.iter().map(|(p, _)| p.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nearest_peer_uses_xor_distance() {
        let mut d = Dht::new([0; 32], 1);
        let mut a = Peer::from_address("127.0.0.1:1".parse().unwrap(), 1);
        a.id = [0xff; 32];
        let mut b = Peer::from_address("127.0.0.1:2".parse().unwrap(), 1);
        b.id = [1; 32];
        d.add_peer(a);
        d.add_peer(b.clone());
        assert_eq!(d.closest(&[0; 32])[0], b);
    }

    #[test]
    fn address_ids_are_binary_sha256_values() {
        let address = "127.0.0.1:7400".parse().unwrap();
        assert_eq!(
            Peer::from_address(address, 1).id,
            sha2_bytes(b"127.0.0.1:7400")
        );
    }
}
