use crate::manifest::ContentId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
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

pub fn distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut out = [0; 32];
    for i in 0..32 {
        out[i] = a[i] ^ b[i];
    }
    out
}

pub fn key_id(key: &str) -> NodeId {
    match hex::decode(key) {
        Ok(bytes) if bytes.len() == 32 => bytes.try_into().expect("length checked"),
        _ => sha2_bytes(key.as_bytes()),
    }
}

fn sha2_bytes(bytes: &[u8]) -> NodeId {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// Kademlia routing table with one bounded, least-recently-seen bucket per
/// distance prefix. Network RPCs live in `network`.
pub struct Dht {
    local_id: NodeId,
    k: usize,
    buckets: Vec<VecDeque<Peer>>,
    providers: HashMap<ContentId, Vec<(Peer, Instant)>>,
}

impl Dht {
    pub fn new(local_id: NodeId, k: usize) -> Self {
        Self {
            local_id,
            k: k.max(1),
            buckets: vec![VecDeque::new(); 256],
            providers: HashMap::new(),
        }
    }
    pub fn add_peer(&mut self, peer: Peer) {
        if let Some(index) = bucket_index(&self.local_id, &peer.id) {
            let bucket = &mut self.buckets[index];
            if let Some(position) = bucket.iter().position(|existing| existing.id == peer.id) {
                bucket.remove(position);
            } else if bucket.len() == self.k {
                bucket.pop_front();
            }
            bucket.push_back(peer);
        }
    }
    pub fn remove_peer(&mut self, id: &NodeId) {
        if let Some(index) = bucket_index(&self.local_id, id) {
            self.buckets[index].retain(|peer| &peer.id != id);
        }
    }
    pub fn known_peers(&self) -> Vec<Peer> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.iter().cloned())
            .collect()
    }
    pub fn peer(&self, id: &NodeId) -> Option<Peer> {
        bucket_index(&self.local_id, id).and_then(|index| {
            self.buckets[index]
                .iter()
                .find(|peer| &peer.id == id)
                .cloned()
        })
    }
    pub fn closest(&self, target: &NodeId) -> Vec<Peer> {
        let mut peers = self.known_peers();
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
        let target = key_id(key);
        let mut providers: Vec<_> = records.iter().map(|(p, _)| p.clone()).collect();
        providers.sort_by_key(|peer| distance(&peer.id, &target));
        providers.truncate(self.k);
        providers
    }
}

fn bucket_index(local: &NodeId, other: &NodeId) -> Option<usize> {
    distance(local, other)
        .iter()
        .enumerate()
        .find(|(_, byte)| **byte != 0)
        .map(|(byte_index, byte)| byte_index * 8 + byte.leading_zeros() as usize)
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

    #[test]
    fn content_digest_is_its_own_dht_key() {
        let digest = sha2_bytes(b"chunk");
        assert_eq!(key_id(&hex::encode(digest)), digest);
    }

    #[test]
    fn providers_are_bounded_and_ordered_by_distance() {
        let mut d = Dht::new([0; 32], 1);
        let key = hex::encode([0u8; 32]);
        let mut far = Peer::from_address("127.0.0.1:1".parse().unwrap(), 1);
        far.id = [0xff; 32];
        let mut close = Peer::from_address("127.0.0.1:2".parse().unwrap(), 1);
        close.id = [1; 32];
        d.announce(key.clone(), far, Duration::from_secs(10));
        d.announce(key.clone(), close.clone(), Duration::from_secs(10));
        assert_eq!(d.providers(&key), vec![close]);
    }

    #[test]
    fn bucket_eviction_keeps_recent_contact() {
        let mut d = Dht::new([0; 32], 1);
        let mut old = Peer::from_address("127.0.0.1:1".parse().unwrap(), 1);
        old.id = [0x80; 32];
        let mut recent = Peer::from_address("127.0.0.1:2".parse().unwrap(), 1);
        recent.id = [0x81; 32];
        d.add_peer(old.clone());
        d.add_peer(recent.clone());
        assert!(d.peer(&old.id).is_none());
        assert_eq!(d.peer(&recent.id), Some(recent));
    }
}
