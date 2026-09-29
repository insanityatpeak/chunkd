# Changelog

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
