# Changelog

## v0.4.0 (2026-10-03)

### Erasure coding
- An upload can store each chunk as a Reed-Solomon stripe, 4 data and 2 parity shards on 6 distinct nodes, racks filled round-robin: 1.5× the bytes of the data, against 3× for copies, and any 2 shards may be lost. `chunkd put -redundancy=ec-4+2`, the gateway's `?redundancy=ec-4+2`, and a choice in the dashboard. Commit needs 5 shards on alive nodes. ADR-0022.
- Each shard is a block of its own: a 33-byte header (slot, stripe ID) and the payload, named by its SHA-256, so the block store's checks, scrubbing, GC fencing, drain and trims treat it as any block. Claims carry the 6 shard IDs, which keeps every shard GC-marked from the claim on.
- Reads ask for 4 shards at once, alive data shards first, replace a failed or unverified one with parity, hedge past a slow one after the recent p95, and decode only when a data shard is missing. With fewer than 4 readable the read fails and names the count.
- Repair rebuilds a lost shard on a node outside its stripe from 4 others: after the repair delay while the stripe has 5, at once with 4. The 4 reads hold source slots and are charged to the byte bucket. ADR-0023.
- New transport call `Caller.Gather`, the k-of-n form of `Hedge`, under the RPC conformance suite for both transports.
- **Wire changes:** additive only. `Redundancy` on begin, versions, stat, list, log and file health; shard IDs on claims and chunk records; `ShardLocation` in stat; `RebuildShard` to nodes; `rebuild_from` on repair copies. v0.3.0 data directories replay unchanged.

### Proof: erasure coding
- Codec tables: any 2 losses decode, 3 fail, every slot rebuilds, equal payloads get distinct block IDs. State, server, scheduler and node rebuild tests; sim round trips with 2 nodes and 3 nodes lost.
- `chaos --ec`: 7 nodes with about half the puts erasure-coded, 500 seeds per push.
- `docs/benchmarks/ec.md`: stored bytes and repair traffic against replication.
- Six more entries in `docs/bugs-found.md` (#25–#30).

### Demo: erasure coding
- A "Store as" choice on upload; the Replicas column counts shards of 6; the chunk grid shows which shard each node holds, parity in blue, and marks chunks a download decoded; rebuilds show their 4 sources. A shareable `ec` scenario. Existing scenario links replay unchanged.

### Resumable uploads
- A pending upload's progress is its claims in the log, so any client or gateway can resume after any restart or leader failover. `meta.upload_status` returns the claims and which are stored, or the version a committed upload created. A resumed upload sends only chunks the cluster does not hold. ADR-0024.
- `chunkd put -resume` hashes the file, begins with the hash, keeps the upload ID in the user cache directory, and continues from the stored chunks; a changed file is refused and an expired upload restarts. The gateway speaks tus core: `POST /uploads/{path}`, `HEAD` or `GET` and `PATCH /uploads/{id}`.
- **Wire changes:** additive. `BeginUploadOp.sha256` (field 10), `UploadStatus` request and response.

### API keys and quotas
- The gateway can require `Authorization: Bearer <key>` (`-keys file`, `$CHUNKD_KEYS`). The file holds the SHA-256 of each key; `chunkd keygen` makes one. A key owns the paths under `/<namespace>/`; an admin key reaches every path and the node controls. Upload IDs belong to the key whose namespace holds their path. `/cluster` and the dashboard files stay open. ADR-0025.
- A namespace has a byte quota. A begin reserves its size in the log, so concurrent uploads cannot pass it, and a refused upload (HTTP 507) reads no bytes and writes no chunks. Delete frees bytes; undelete is checked as a write.
- **Wire changes:** additive. `quota` on `BeginUploadOp` (11), `BeginUploadRequest` (8), `UndeleteOp` and `UndeleteRequest` (4).

### Versions and CLI
- `chunkd retain <path> <epochs>` and `PUT /retention/{path}?epochs=N` keep a path's retired versions longer or shorter than the cluster default; the log shows the matching expiry. ADR-0026.
- `chunkd diff <path> <from> <to>` lists the chunks that differ and the bytes a rewrite would send; `chunkd restore <path> <version>`.
- Exit codes by error class (3 not found, 4 conflict, 5 quota, 6 denied, 7 unavailable, 8 corrupt, 2 usage), a byte counter on `get`, `chunkd bench`, and `chunkd completion bash|zsh|powershell`.
- **Wire changes:** additive. `SetRetentionOp` (Op 13), `FileRecord.retain_epochs` (3); v0.3.0 data directories replay unchanged.

## v0.3.0 (2026-10-02)

### Rebalancing, drain and decommission
- Nodes can join while the cluster serves. A balancer gives every node a rack-feasible byte target. A rack holds at most ceil(RF/racks) copies of a chunk, and its nodes share them. The balancer moves chunks until each node is within its band: 10% of target, but at least two chunks. A move only ever lowers the total distance from target and never loses a rack. Bytes moved stay within ½·L1 plus one chunk per node. ADR-0020.
- Moves share the repair scheduler: repair first, then drain, then balance, with drain and balance capped at 4 of the 8 copy slots. Balancing waits until no node is suspect, or dead inside the repair delay.
- Every trim, of a repair surplus or of a move's source, is a logged intent. It is sent only after it commits, at most one is pending per chunk, and the leader rechecks before each send that the chunk keeps RF copies elsewhere. A cut-off leader can no longer trim.
- `chunkd node drain|undrain|decommission <id>`, the gateway routes `POST /nodes/{id}/drain|undrain|decommission`, and the metadata RPC `meta.node_admin`. A draining node takes no new chunks and its copies move away, keeping rack spread. Decommission is refused until every chunk has RF copies elsewhere; `-wait` retries until then. ADR-0021.
- Compose: `node-6` (rack r3) under the `extra` profile: `docker compose --profile full --profile extra up -d node-6`.
- **Wire changes:** heartbeat field 6 (`draining`) is removed and reserved; drain state is now logged. New log ops: TrimIntent (Op 10), TrimDone (11), NodeAdmin (12). New snapshot fields 7 (pending trims) and 8 (node admin states). Cluster view: per-node `balance_used`, `balance_target` and `balance_band`; health `repair_evacuated` and `repair_moved`. Every change is additive, so v0.2.0 data directories replay as they are.

### Proof: rebalancing
- `TestAddNodeConverges`, `TestDrainNeverDropsRF`, `TestDecommissionOnlyWhenSafe`, `TestDeposedLeaderCannotTrim`, `TestNewLeaderTrustsLoggedTrimDone`, `TestStaleTrimWaitsForRepair`.
- Every chaos run checks trim safety at each trim delete. Chaos schedules add nodes, drain and undrain them, kill a node mid-drain, and, over the metadata group, kill the leader mid-move. These faults come from a random stream of their own, so existing seeds keep their schedules.
- The real-mode short suite drains and decommissions node-4, adds node-6 and removes it again.
- `docs/benchmarks/rebalance.md`: bytes moved against ½·L1 when a sixth node joins r3 or r1, and time to converge.
- Seven more entries in `docs/bugs-found.md` (#18–#24).

### Demo: rebalancing
- Dashboard: an Add node button, Drain/Undrain per node, amber outlines and labels for draining and decommissioned nodes, and a usage bar with the balance target and band. Drain and balance copies are tagged in the timeline. Against a real cluster the buttons are disabled; their tooltips give the command.
- Shareable `add-node` and `drain` scenarios. The `kill-node` link replays with slightly different trim timings, because its surplus trims now go through the log.

## v0.2.0 (2026-10-02)

### Metadata high availability
- The metadata service is a group of three Raft peers (etcd raft's `RawNode`), one implementation in the simulator, the browser and the real processes. Election timers are seeded, so a failover replays from its seed. ADRs 0017–0019.
- Writes commit through the log; reads are read-index reads confirmed by a quorum, so a deposed leader cannot serve a stale read. Followers answer with the leader's name and the client follows it.
- Fencing: commands to nodes carry the Raft term. Nodes keep the highest term they have seen on disk and refuse lower ones, so a paused leader that wakes up deposed cannot trim or collect a copy.
- GC deletes are logged intents, sent only after the intent commits. Begin carries a client request ID, so a retried upload opens once.
- Snapshots bound the log; a follower that fell behind the compacted log catches up from one. Single-server membership changes are supported in the consensus layer.
- **Upgrade:** a metadata volume from v0.1 (`CHWAL001`) cannot be read. Wipe it: `docker compose down -v`.

### Proof: metadata group
- `TestKillLeaderMidUpload` in the sim and against real processes, `TestStaleLeaderCannotCommit`, `TestMinorityPartitionRejectsWrites`, and the log bounded over 10,000 operations.
- Client histories recorded and checked for linearizability with porcupine: 500 chaos seeds with leader kills, freezes, partitions and repeated elections per push, and every real-mode scenario. A failing history is written out as a visualization.
- The real-mode suite kills the metadata leader under load.
- Five more entries in `docs/bugs-found.md` (#13–#17), three in consensus and the sim group, two in the harness.

### Demo: metadata group
- Dashboard: the metadata group with each peer's role, term, commit index and a crown on the leader; kill, freeze and partition any peer, or the current leader; elections on the timeline (leader lost, new term, new leader). LIVE mode shows the same from the gateway's cluster view.
- A "Kill the metadata leader" scenario, shareable as a link and replayed in CI. The simulated cluster now runs three metadata peers, so earlier scenario links replay the same story with slightly different timings.


### Dedup, versions and delete
- A client claims each chunk before writing it; a chunk the cluster already holds with 2 copies is not sent again. The same base with ten small edits stores 220 MiB as 60 MiB.
- Commits are compare-and-swap on the file's version by default; a lost race returns a conflict. Last-writer-wins is opt-in (`chunkd put --lww`).
- Every retained version is readable: `chunkd log <path>`, `chunkd get --version N`.
- Delete writes a marker. Deleted and overwritten versions stay restorable with `chunkd undelete` for a retention window counted in logged GC epochs, not wall-clock time; then they drop and release their chunks.

### Garbage collection
- Mark-and-sweep: a chunk is kept while a retained version references it or a pending upload claims it. Unreferenced copies are deleted after a 60 s grace, fenced on the node so a copy written after the decision is kept.
- Uploads hold leases; an abandoned upload expires and its chunks are collected.
- Refcounts and claims are recounted every epoch; drift raises a metric, a log error and a timeline event, and is never corrected silently.

### Proof: dedup and GC
- 1,000 chaos seeds on every push (was 500), each ending with GC settled and no orphan copy on any node.
- GC chaos scenarios: concurrent writers on one path, delete while another upload shares its chunks, uploads stalled past a GC cycle, a node returning with long-deleted chunks.
- Three more bugs in `docs/bugs-found.md`: an upload lease shorter than the GC grace, undelete unreachable through the compose gateway, and a node back from a long outage unreachable from the gateway for up to 120 s of gRPC dial backoff.

### Demo: dedup and GC
- Dashboard: dedup savings, GC counters and epoch, each file's versions with restore, deleted files with undelete, gc and write events in the timeline.
- A "Delete and collect" scenario, shareable as a link and replayed in CI like the others.

## v0.1.0 (2026-09-29)

The first release: a replicated, self-healing file store with an interactive demo you can break in the browser or on your machine.

### Storage and metadata
- Files split into 4 MiB chunks named by their SHA-256, streamed, and stored on 3 nodes in different racks (rack spread, then least loaded).
- Metadata server with a versioned namespace, compare-and-swap uploads, tombstones and refcounts. Durable in a CRC-framed WAL with fsync and bbolt snapshots, with torn-tail recovery.
- Uploads are invisible until one commit, which needs 2 replicas reported by the nodes. Commit and delete are idempotent, so lost or duplicated responses are safe.
- Chunk locations are rebuilt from block reports, never stored. Reports carry per-incarnation sequence numbers, so reordered reports never drop or resurrect a copy.

### Failure handling
- Failure detector: alive, suspect at 3 s, dead at 10 s, with hysteresis, incarnations and a stall guard for a paused metadata server.
- Re-replication: fewest copies first, a 20 s delay so a rebooting node costs nothing, 8 copies in flight (2 per node), 40 MiB/s, and trims of extra copies that keep rack spread.
- Hedged reads: per-node latency scores, and a second request after the p95 (20–500 ms).

### Integrity
- Nodes check every chunk before serving it; clients check again. Failed copies are quarantined and replaced at once.
- A background scrubber re-reads every chunk at 8 MiB/s, so rot nobody reads is found within one pass.
- When every copy of a chunk is bad, reads fail loudly with `corrupt`.

### Proof
- Deterministic simulator: fake clock, seeded network with loss, duplication, delay, partitions and crashes. Any failure replays from its seed.
- 500 chaos seeds on every push (20,000 on demand). They check that acknowledged data stays readable, RF returns within a stated bound, rot is found, and no read returns bytes never written.
- Real-mode suite against Docker containers: kill, reboot, bit rot, pause.
- Nine bugs found by these tests, with root causes, in `docs/bugs-found.md`.

### Demo
- Dashboard in the browser (the cluster compiled to WebAssembly): fault buttons, click-to-corrupt a replica, replication health, repair queue, event timeline with filters, 1×–50× speed, a guided tour.
- Shareable links that replay exactly: `?seed=7&scenario=kill-node` (also `corrupt-chunk`, `rack-loss`, `slow-node`). Checked in CI by Playwright against timelines recorded in Go.
- The same dashboard served by the gateway at `http://localhost:8080` in LIVE mode.
- `docker compose run --rm demo`: kills a node on a real cluster and narrates the repair, with only Docker installed.

### Known limitations
One metadata server (Raft is next), no garbage collection or rebalancing yet, and no authentication. The full list is in the README.
