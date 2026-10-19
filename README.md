# BitMesh

BitMesh is a Rust content-addressed storage MVP. Files are split into SHA-256 chunks, stored by hash, described by immutable manifests, replicated to peers, downloaded concurrently, and verified before assembly.

```powershell
cargo run -- --data node-b serve --listen 127.0.0.1:7401
cargo run -- --data node-a upload .\large.bin --replication 2 --peers 127.0.0.1:7401
cargo run -- --data node-c download <MANIFEST_ID> .\copy.bin --peers 127.0.0.1:7401
```

`repair` rechecks/repopulates the requested replication level; `gc` removes chunks not protected by an active lease. `--payer` and `--payment-signature` associate a lease with the Anchor escrow in `anchor/`.

The routing module implements Kademlia's XOR-distance peer selection and expiring provider records. The TCP protocol is deliberately small and suitable for local or directly reachable peers. Peer records include an optional relay address for NAT fallback; production deployments should put authentication, encryption, persistent DHT RPC, and a TURN-like relay in front of this protocol.
