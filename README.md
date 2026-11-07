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

Use `--peers` on `serve` to join nodes to a shared routing graph, for example
`node-b ... serve --listen 127.0.0.1:7401 --peers 127.0.0.1:7402`.

Upload a file with three total copies (the uploader plus two storage nodes):

```powershell
cargo run -- --data node-a upload .\large.bin --replication 3 --peers 127.0.0.1:7401,127.0.0.1:7402,127.0.0.1:7403
```

The command prints the manifest ID. A downloader only needs one reachable seed;
provider lookups expand that seed into the known manifest and chunk holders:

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

To check and repair active leases periodically, serve any lease holder's data directory:

```powershell
cargo run -- --data node-a serve --listen 127.0.0.1:7400 --peers 127.0.0.1:7401,127.0.0.1:7402,127.0.0.1:7403 --repair-interval-seconds 60
```

For paid leases, fund a replacement provider in advance with `pay`, then pass
its account to the repair coordinator with
`serve --repair-payment-account PEER=ACCOUNT`. The coordinator queries the
replacement provider and verifies the finalized Anchor account for each active
manifest before using it. Include the replacement address in `--peers`.
Payment is required for each replacement provider; a node cannot create a
payment on another user's behalf. The coordinator retains verified replacement
accounts in the repaired lease for later scans.

Add `--gc-interval-seconds 3600` to a serving node to remove chunks after
their last lease expires. Provider announcements for active, valid local
content are refreshed every five minutes so they do not disappear when their
15-minute DHT records expire.

## Leases and Solana payments

Uploads create storage leases on the uploader and confirmed replica nodes (seven
days by default). Active leases pin their manifest's chunks; `gc` removes
unleased chunks. Inspect local and remote
capacity with `status`, and change the lifetime with `--lease-seconds`.

`anchor/programs/bitmesh-payments` contains the Anchor escrow program. Its
`create_lease` instruction derives a lease PDA from payer, provider, and manifest,
then escrows lamports. After expiry, the provider signs `settle` to receive the
payment; Anchor closes the account and refunds rent to the payer. For paid
storage, prepare the file first to obtain its immutable manifest ID:

```powershell
cargo run -- --data node-a prepare .\large.bin
```

Create a separate Anchor lease account for each provider, using that manifest
ID and an expiry at least `--lease-seconds` beyond the planned upload time.
The `pay` command reads a local Solana keypair, signs the Anchor instruction,
submits it, and waits for a finalized account:

```powershell
cargo run -- --data node-a pay <MANIFEST_ID> --provider <PROVIDER_PUBKEY> --keypair .\payer.json --expires-at <UNIX_TIMESTAMP> --amount-lamports 1000000 --solana-rpc http://127.0.0.1:8899
```

Then start each provider with its Solana key and a finalized RPC endpoint, and
publish the prepared file, using the payment account printed by `pay`:

```powershell
cargo run -- --data node-b serve --listen 127.0.0.1:7401 --solana-rpc http://127.0.0.1:8899 --provider-pubkey <PROVIDER_PUBKEY>
cargo run -- --data node-a upload .\large.bin --prepared-manifest <MANIFEST_ID> --replication 2 --peers 127.0.0.1:7401 --payer <PAYER_PUBKEY> --solana-rpc http://127.0.0.1:8899 --payment-account 127.0.0.1:7401=<ANCHOR_LEASE_ACCOUNT>
```

The uploader checks the file against the prepared manifest. The uploader and
paid provider independently query the
finalized account and check its owner, Anchor discriminator, payer, provider,
manifest, escrow amount, and expiry. Each replica needs its own lease account.
To pay a replacement during repair, pass its address and lease account with
`repair --payment-account PEER=ACCOUNT --solana-rpc RPC`. Paid nodes only accept
chunk bytes referenced by an active verified lease. The built-in RPC client uses
plain HTTP, suitable for local Solana validators. For remote clusters, terminate
TLS at a trusted local proxy and point `--solana-rpc` to that proxy.
Lease holders retain the payment account map so a surviving holder can coordinate
repair if the uploader is offline, provided a paid replacement is configured.

## Design and boundaries

- Provider records expire after 15 minutes and are ordered by Kademlia XOR
  distance. Routing contacts use bounded distance buckets, persist in
  `routing.json`, and are refreshed every five minutes. Downloads follow routing
  contacts from seeds, probe providers in parallel, prioritize rare chunks, and
  rank peers by observed speed, failures, and current requests.
- A node can use `serve --relay RELAY_ADDRESS --peers RELAY_ADDRESS` to maintain
  an outbound tunnel to a publicly reachable relay node. Requests try the direct
  address first and then the tunnel. This works for nodes that cannot accept
  inbound connections; automatic hole punching is not implemented.
- Writes are atomic and serialized for quota accounting. Content IDs are strictly
  validated, and corrupt local or remote bytes are rejected.
- The JSON-over-TCP protocol is intentionally small. Internet deployment still
  requires authenticated/encrypted transport and signed peer and provider
  announcements. Relay registrations currently trust the registering peer's
  claimed address, so use trusted relays or add identity authentication before
  exposing them publicly.

Run the core tests with `cargo test --all --all-targets`. Run the Anchor program
tests with `cargo test --manifest-path anchor/programs/bitmesh-payments/Cargo.toml`.
