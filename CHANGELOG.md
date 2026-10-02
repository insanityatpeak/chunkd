# Changelog

## v0.2.0 (unreleased)

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
