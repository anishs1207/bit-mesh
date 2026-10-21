# BitMesh

BitMesh is a Rust content-addressed peer-to-peer storage MVP. It splits files into
SHA-256 chunks, deduplicates them on disk, creates immutable manifests, places
replicas according to node capacity, discovers providers through a compact
Kademlia-style routing layer, and downloads rare chunks concurrently from the
fastest responding peers. Every chunk and manifest is verified before use.

## Local demo

Start three storage nodes in separate terminals:

```powershell
cargo run -- --data node-b --quota 10737418240 serve --listen 127.0.0.1:7401
cargo run -- --data node-c --quota 10737418240 serve --listen 127.0.0.1:7402
cargo run -- --data node-d --quota 10737418240 serve --listen 127.0.0.1:7403
```

Upload a file with three total copies (the uploader plus two storage nodes):

```powershell
cargo run -- --data node-a upload .\large.bin --replication 3 --peers 127.0.0.1:7401,127.0.0.1:7402,127.0.0.1:7403
```

The command prints the manifest ID. A downloader only needs one reachable seed;
provider lookups expand that seed into the known holders for each chunk:

```powershell
cargo run -- --data node-e download <MANIFEST_ID> .\copy.bin --peers 127.0.0.1:7401 --parallel 8 --retries 3
```

To demonstrate repair, stop one replica, start an empty replacement node, and
run:

```powershell
cargo run -- --data node-a repair <MANIFEST_ID> --replication 3 --peers 127.0.0.1:7401,127.0.0.1:7402,127.0.0.1:7403
```

Repair obtains a surviving valid copy, checks reported capacity, restores the
requested number of distinct replicas, and republishes the manifest. It fails
explicitly if too few valid sources or destinations remain.

## Leases and Solana payments

Uploads create a local storage lease (seven days by default). Active leases pin
their manifest's chunks; `gc` removes unleased chunks. Inspect local and remote
capacity with `status`, and change the lifetime with `--lease-seconds`.

`anchor/programs/bitmesh-payments` contains the Anchor escrow program. Its
`create_lease` instruction derives a lease PDA from payer, provider, and manifest,
then escrows lamports. After expiry, the provider signs `settle` to receive the
payment; Anchor closes the account and refunds rent to the payer. Record the
submitted transaction alongside the local lease:

```powershell
cargo run -- --data node-a upload .\large.bin --replication 2 --peers 127.0.0.1:7401 --payer <SOLANA_PUBKEY> --payment-signature <TRANSACTION_SIGNATURE>
```

The CLI treats those fields as an association, not an RPC confirmation. A
deployment should verify the transaction and lease PDA against its chosen Solana
cluster before accepting storage.

## Design and boundaries

- Provider records expire after 15 minutes and are ordered by Kademlia XOR
  distance. Downloads query seeds, deduplicate discovered node IDs, probe them in
  parallel, prioritize rare chunks, and naturally try quick responders first.
- A peer record can carry a relay endpoint. Requests try the direct address first
  and then that endpoint, allowing a relay service to expose a NAT-bound node.
- Writes are atomic and serialized for quota accounting. Content IDs are strictly
  validated, and corrupt local or remote bytes are rejected.
- The JSON-over-TCP protocol is intentionally small. Internet deployment still
  requires authenticated/encrypted transport, signed provider announcements,
  durable routing tables, a real relay/TURN service, and on-chain transaction
  verification.

Run the core tests with `cargo test --all --all-targets`. Run the Anchor program
tests with `cargo test --manifest-path anchor/programs/bitmesh-payments/Cargo.toml`.
