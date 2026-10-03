# Design

This document describes what exists. Each section names the ADR that records the decision and the tests that hold it. Numbers are from the repo's tests and [benchmarks](benchmarks/README.md).

Contents: [goals](#goals-and-non-goals) · [components](#components) · [run modes](#run-modes) · [namespace](#namespace-versions-and-compare-and-swap) · [upload](#upload) · [download](#download) · [heartbeats](#heartbeats-and-block-reports) · [failure detection](#failure-detection-adr-0010) · [repair](#repair-adr-0011) · [recovery timeline](#recovery-timeline) · [hedged reads](#reads-under-slow-replicas-adr-0012) · [integrity](#integrity-adr-0013) · [GC](#garbage-collection-adr-0016) · [Raft](#metadata-group-adr-0017-to-0019) · [rebalance and drain](#rebalance-and-drain-adr-0020-and-0021) · [erasure coding](#erasure-coding-adr-0022-and-0023) · [resumable uploads](#resumable-uploads-adr-0024) · [auth and quotas](#auth-and-quotas-adr-0025) · [retention and diff](#retention-and-diff-adr-0026) · [consistency](#consistency-per-operation) · [CAP](#cap-per-plane) · [decisions](#decisions-and-trade-offs) · [prior art](#prior-art) · [what sim cannot capture](#what-sim-mode-cannot-capture) · [invariants](#invariants-checked-by-the-harness)

## Goals and non-goals

Goals, in order: no acknowledged write is lost; a read returns the bytes that were written or fails loudly; lost redundancy is rebuilt within a stated bound; every one of these is a test that replays from a seed.

Non-goals: POSIX semantics, in-place writes or appends, small-file efficiency, multi-region operation, a namespace beyond one metadata group's RAM. Where a shortcut is taken it is marked `SIMPLIFIED:` in the code or its ADR, with how GFS, HDFS, Ceph, S3, MinIO or SeaweedFS does it.

## Components

```
   CLI / browser ──HTTP──► gateway ─┐   (client library: verifies every chunk and the file)
   CLI (-meta) ──────────────────────┤
                                     │ control: begin, claim, commit, stat, list, delete, cluster
                                     ▼
                  ┌───────────────────────────────────────┐   per peer: WAL + bbolt snapshots
                  │  meta-1 (leader)  meta-2     meta-3   │   Raft group of 3 (etcd raft RawNode)
                  │  namespace, versions, refcounts,      │   replicated state: files, chunks, uploads,
                  │  pending uploads, node admin, intents │   GC and trim intents, quotas, retention
                  └──────▲────────────────────────────────┘   soft state, rebuilt: node view, chunk locations
               heartbeat 1 s,        │ (to every peer; only the leader acts)
               block reports ────────┘
      ┌──────────┬──────────┬──────────┬──────────┐
    node-1     node-2     node-3     node-4     node-5        chunks at ab/cd/<sha256>
     r1         r2         r3         r1         r2
      ▲  data: chunk.put / chunk.get (gRPC streams, 1 MiB frames), client ↔ nodes directly
```

The metadata group is never on the data path. It stores what must survive (the namespace, which chunk IDs a version has, which are claimed). It does not store where chunks are: nodes report what they hold, and the leader keeps that as soft state (ADR-0006).

Why locations are not durable: a durable map would need every report replicated through the log (a write per chunk per node per report), and it would be wrong the moment a disk dies or is wiped, because the node, not the log, is the source of truth for what is on the disk. After a leader change the new leader asks for full block reports (under a second at 1 s heartbeats). Meanwhile a read finds no replicas and the client retries. GFS and HDFS make the same choice.

## Run modes

| | sim | real |
|---|---|---|
| Processes | one | one per component |
| Transport | `sim.Net`: in-memory, seeded loss, duplication, delay, bandwidth, partitions, crashes | `grpcnet`: Deliver (one-way), Call (unary), CallStream (framed) |
| RPC client | `sim.Caller`: advances the clock until answered | `grpcnet.Caller`: goroutine per call |
| Clock | `sim.Clock`: event queue ordered by (deadline, seq) | wall clock; timers post to `runtime.Loop` |
| Chunk store | `sim.BlockStore` | `blockstore`: files, atomic rename |
| Metadata log | `sim.MetaStore` | `metastore`: CRC-framed WAL + bbolt snapshots |
| Consensus | `core/consensus` over etcd raft, on the fake clock | the same package, wall clock |
| Where | tests, `cmd/chunkd-wasm` in the browser | `docker compose up`, `internal/real/local` in tests |

Both modes run the same `internal/core/{meta,node,placement,repair,consensus,...}` code and the same `internal/client`. Core imports only `internal/iface` (Transport, Clock, BlockStore, MetaStore, Rand); `go run ./tools/task lint-imports` enforces it (ADR-0002). Each process has one event loop and core components take no locks (ADR-0004). A sim run is a pure function of its seed.

## Namespace: versions and compare-and-swap

```
files:   path → versions[] → { v, upload_id, size, sha256, chunk_ids[] | stripes, tombstone }
chunks:  chunk_id → { refcount, size }
uploads: upload_id → { path, expected_version, size, placement[][], claims, lease epoch, quota reservation }
```

Versions are logical and per path; wall-clock time decides nothing. `BeginUpload(path, expected)` is a compare-and-swap against the live version (0 means must not exist), repeated at commit, so of two writers racing on one version exactly one commits and the other gets `conflict` (ADR-0014; `TestCASConflict`, `TestChaosConcurrentWriters`). `last_writer_wins` is opt-in and documented as losing updates. A version becomes visible in the single log entry that commits it. A commit or delete repeated after a lost response returns its own earlier result, because the version records the upload that created it (bugs-found #2).

## Upload

```
Client               Meta (leader)                node-A      node-B      node-C
  │─BeginUpload(path, expected v, size, request id, redundancy, quota)─►│
  │                                      │ CAS on live version; quota check; place each chunk
  │                                      │ (rack spread, least loaded); one log entry
  │◄──upload_id, placement, min_replicas─│    a repeated request id returns the same upload
  │ read chunk i, SHA-256 → id_i
  │─ClaimChunks(upload, [(i, id_i)])────►│ logged; renews the upload's lease
  │◄──present[i]  (≥ 2 reported copies, none being GC'd: skip the write)
  │─chunk.put(id_i, bytes)────────────────────────────►│──────────►│──────────►│  in parallel
  │                                      │◄─block report "received id_i"──────┤  each node, before acking
  │◄─ack──────────────────────────────────────────────┤  (need ≥ min_replicas acks, else abort)
  │─CommitUpload(upload_id, [ids], file sha256)─►│
  │                                      │ every id claimed by this upload, and ≥ 2 live reported locations?
  │                                      │   no  → retry (client backs off up to 45 s)
  │                                      │   yes → CAS again, log entry: version visible, refcounts up
  │◄──version─────────────────────────────│   repeated commit of the same upload → same version
```

- The client fans a chunk out to all three replicas in parallel and commits at 2 of 3 (ADR-0007). Chunks are immutable and named by hash, so no ordering between replicas is needed. The cost is 3× client egress.
- Commit trusts block reports from the nodes, not the client's word.
- A claim is logged before the chunk is written. That ordering is what makes GC safe (see [GC](#garbage-collection-adr-0016)) and is also dedup: a `present` chunk is not sent (ADR-0015).
- Placement is chosen at Begin and written into the log entry, so replay does not depend on load at replay time (ADR-0008).

## Download

```
Client               Meta                  node-A      node-B
  │─Stat(path)─────────►│  read-index read on the leader
  │◄─version, sha256, [id_i → live replicas, alive first]
  │─chunk.get(id_i)──────────────────────────►│
  │◄─bytes: SHA-256 ≠ id_i ✗ ─────────────────┤
  │─chunk.get(id_i)──────────────────────────────────────►│   next replica
  │◄─bytes: SHA-256 = id_i ✓ ─────────────────────────────┤
  │ … every chunk, then file SHA-256 must equal the committed one
```

## Heartbeats and block reports

- Every node heartbeats each second to every metadata peer: rack, address, bytes and chunks stored. Only the leader acts on them. Whether a node is draining or decommissioned is logged metadata state set by the operator (ADR-0021), not part of the heartbeat.
- The ack asks for a full block report when the peer has none for the node (first contact, or after a leader change). It also carries the leader and its term, which is how a node learns of a new term (see [fencing](#metadata-group-adr-0017-to-0019)).
- Nodes send an incremental report after storing each chunk, and a full report every 30 s, which repairs any lost incremental report.
- Heartbeats and reports carry `(incarnation, seq)`; the incarnation changes on every process start. A report older than one already applied is ignored (bugs-found #7).

## Failure detection (ADR-0010)

```
          3 s silent            10 s silent
 alive ─────────────► suspect ─────────────► dead
   ▲                    │  ▲                   │
   └── 3 on-time beats ─┘  └─ beat (new incarnation: "restarted") ─┘
```

| State | Reads | New replicas | Counts for commit | Counts toward RF for repair |
|---|---|---|---|---|
| alive | first | yes | yes | yes |
| suspect | after alive replicas | no | no | yes |
| dead | no | no | no | only inside the 20 s repair delay |

- `core/detector` ticks every 500 ms. A gap over 1 s between ticks means the metadata server itself stalled; every node's last-seen moves forward by the gap, so a paused leader declares nobody dead.
- A dead or restarted node keeps its chunk locations until its next full block report (bugs-found #4).
- Detection is by one observer (the leader) and silence. Gray failure (slow but beating) is invisible to it by design, and is handled on the read path by hedging.

## Repair (ADR-0011)

```
Meta (repair.Scheduler)            target node                 source node
  │ node dead + 20 s, chunk at 2/3 │                             │
  │── replicate(chunk, source) ───►│                             │
  │                                │── chunk.get (AsyncCaller) ─►│
  │                                │◄── bytes ───────────────────┤
  │                                │ Put: SHA-256 must equal id  │
  │◄── block report (chunk) ───────┤  → copy complete            │
  │ no report in 10 s → timed out, slots freed, re-assessed      │
```

- Queue: class first (repair, then drain evacuation, then balance; ADR-0020), then fewest live copies, then FIFO. A chunk with one live copy skips the delay.
- Limits: 8 copies in flight, 2 per source node, 2 per target node, 40 MiB/s token bucket with a 4 MiB burst.
- Periodic scan every 30 s catches anything the event-driven path missed.
- Trim: when more than 3 confirmed copies exist (alive, full report since returning) and no copy is in flight, drop the copy on the rack with the most copies, then the most used node. Every trim is a logged intent (see [rebalance](#rebalance-and-drain-adr-0020-and-0021)).
- RF restore bound: `10 s (dead) + 20 s (delay) + bytes ÷ 40 MiB/s + 10 s (one copy timeout) + 5 s`.

Why delay, and why throttle. A reboot takes under a minute, and copying a node's terabytes for a machine that returns is how storage clusters hurt themselves; the 20 s delay costs 20 s at reduced redundancy and saves all of that (`TestTransientBlipNoRepair`: zero copies for 2, 15 and 25 s outages). A chunk down to one copy skips the delay. The rate and slot caps bound how much of every node's disk and NIC repair may take, so reads and writes keep running while a whole node is rebuilt. The cost of a cap is repair time: at a 5 MiB/s cap, 240 MiB lost takes 47.5 s past the 30 s floor, against 12 s at 20 MiB/s ([benchmarks/results.md](benchmarks/results.md)). The sim shares no capacity between messages, so it cannot show the effect on readers; that is not measured here.

## Recovery timeline

```
t=0      node-3 killed (or its container stopped)
t=3 s    suspect: reads try it last, no new placement, commit does not count it
t=10 s   dead: its copies still "expected back" for 20 s; zero copies are made
t=30 s   delay over: chunks at 2/3 queue for repair, fewest copies first, 8 in flight, 40 MiB/s
t=30 s+  copies land; each is confirmed by the target's block report
t=...    all chunks at 3 copies again   (bound: 30 s + bytes/40 MiB/s + 15 s)

node-3 returns with its disk:  full block report → some chunks at 4 copies
                               → TrimIntent (logged) → DeleteReplica → TrimDone (logged)
node-3 returns wiped:          its report is empty, so nothing excuses the missing copies
```

`TestKillNodeRestoresRF` measures this: 196 MiB on the dead node, RF 3 restored in 42.8 to 44.8 s against a 49.9 s bound (ADR-0011). Real mode (`kill-node-restores-rf`, compose) holds the same bound with 90 s of allowance for wall-clock jitter.

## Reads under slow replicas (ADR-0012)

The client orders replicas alive first, then suspect, each by its own latency score (EWMA, failures penalised), and reads with `Caller.Hedge`: if no verified answer arrives within the p95 of recent reads (clamped to 20 to 500 ms), the next replica is asked too. The first answer whose SHA-256 matches the chunk ID wins, so a fast corrupt replica cannot beat a slow correct one.

Measured ([results.md](benchmarks/results.md), sim, new client per read): with one node adding 2 s to every message, get p99 is 4141 ms unhedged and 235 ms hedged; with no fault the two are identical. Writes are not hedged: the same gray node moves put p50 from 188 ms to 4112 ms, because a put waits for the slowest of its replicas.

## Integrity (ADR-0013)

```
client ──chunk.get──► node: SHA-256(bytes) = id?
                        no → quarantine/<id>, report(seq, corrupt_ids) ─► meta: drop location,
                             answer CodeCorrupt; client tries next copy        Recheck: copy now (no delay)
                        yes → bytes ──► client: SHA-256 again (path not trusted)
                                         mismatch → meta.suspect hint → node.verify_chunk → node decides
scrubber (every node): each pass lists the chunks, verifies one per step,
                       paced to 8 MiB/s; same quarantine and report path
```

- Every copy the metadata server counts has passed a node's own check since it was stored or last scrubbed. A copy that fails any check stops counting at once.
- Repair never propagates rot: the source verifies on `getChunk`, the target on `Put`.
- If every copy of a chunk fails, the read returns `CodeCorrupt` ("the data is lost"), and health counts the chunk as lost.

## Garbage collection (ADR-0016)

Chunks are shared by dedup, so ownership cannot say when one is garbage; refcounts and claims can.

- **Mark** is a pure function of the log: a chunk is marked if a retained version references it or a pending upload claims it.
- **Sweep** runs each epoch (30 s) on soft state. A located chunk that stays unmarked for 60 s (the grace) is a candidate. The leader proposes a `GCIntent` naming each copy and a fence (the node's report sequence the leader has applied). Applying the intent skips chunks that are marked at that point of the log. Only after the intent commits are `DeleteReplica{term, fence}` commands sent.
- **Node** refuses a delete if the chunk was written after the fence, under the same per-chunk lock writes take.
- **Leases**: an upload's lease renews on each claim; an upload idle for 6 epochs is aborted and its claims released. The minimum lease (150 s) exceeds the grace plus two sweeps (120 s), so a stall GC notices never also expires the upload (bugs-found #10).

```
upload vs sweep                                   delete vs re-write
t0 U claims X (logged)         X marked           t0 X orphan on N; delete fenced at seq 40; message lost
t1 U writes X to N1, N2        seq 41, 17         t1 V claims X; not "present" (delete pending); writes X to N: seq 44
t2 sweep: X marked, skipped                       t2 sweep re-sends the delete with fence 40
t3 U commits: refcount(X) = 1                     t3 N: last write 44 > 40, kept
```

Without claims, t2 on the left deletes a chunk an upload is writing and t3 publishes a version with a hole. Without the fence, N on the right deletes the copy it just acknowledged. `TestGCSparesInflightUpload`, `TestChaosGCDuringSlowUpload`, `TestChaosNodeReturnsWithDeletedChunks` (which fails if the fence is removed), and a settled-GC check after every chaos seed hold this. Each epoch also recounts refcounts from the versions; drift raises an alarm and is never auto-corrected, because rewriting counts would hide the bug and could let GC delete a referenced chunk.

## Metadata group (ADR-0017 to 0019)

The group is three peers on `go.etcd.io/raft/v3` through `RawNode`, driven from the metadata event loop. `core/consensus` is the only code that touches the library and reaches the world through Clock, Transport, MetaStore and Rand, so the sim, the browser and the real processes run the same implementation (ADR-0018).

- **Timers are ours.** The library draws its election timeout from `crypto/rand`, which no seed controls, so followers are never ticked. A follower that hears nothing for a seeded 1 to 2 s campaigns with PreVote. The vote lease (a peer that heard a leader within the election timeout refuses votes) and the leader's quorum window are implemented in `core/consensus`. Safety never depends on them; liveness does.
- **Log.** One CRC-framed WAL record per `Save`, one fsync; entries and hard state are atomic. Snapshots every 1,000 applied entries; a lagging follower catches up from one (`TestLogBoundedUnder10kOps`).
- **Writes** are validated, proposed and answered when applied. Two proposals that invalidate each other are ordered by the log and the loser is rejected identically on every peer.
- **Reads** (`Stat`, `List`, `Log`, `UploadStatus`) use read-index: the leader confirms with a quorum, then waits for the commit index. Chunk locations inside a `Stat` are soft state and may be stale, which is safe: chunks are immutable and verified, so a stale location costs a hedged read.
- **Fencing.** Every command a leader sends a node (replicate, delete, verify) carries its term. A node keeps the highest term it has seen on its disk and refuses lower ones. It learns the term from heartbeat acks too, so a deposed leader is refused from the first heartbeat after an election. GC and trim deletes are decided in the log first, so a deposed leader cannot create one.

```
leader meta-1 killed (or frozen, or cut off)
t=0       followers stop hearing heartbeats
t≈1-2 s   a follower's seeded timer fires: PreVote, then vote; meta-2 wins term 4
          meta-2 appends a no-op and serves nothing until it has applied an entry of its own term
          clients: NotLeader(hint=meta-2) or unavailable → try the next peer, back off, ≤ ~11 s
          nodes: heartbeat acks now carry term 4; any command with term 3 is refused (Fenced++)
          meta-2: asks for full block reports, rebuilds locations, treats every chunk as freshly committed
                  for the upload grace; resends every pending GC delete and trim with its original fence
old leader wakes: no quorum within the election timeout, reports not ready; its proposals cannot commit
```

Tests: `TestKillLeaderMidUpload` (sim, 12 seeds), `TestKillLeaderMidUploadReal` (processes and disk), `kill-meta-leader` (compose), `TestStaleLeaderCannotCommit`, `TestMinorityPartitionRejectsWrites`, `TestPutSurvivesTheLeaderDying` (49 kill points), and porcupine over 500 sim histories per push plus every real-mode scenario.

## Rebalance and drain (ADR-0020 and 0021)

Placement spreads new chunks; nothing else moves old ones. `core/rebalance` is a pure planner: it gives every node a rack-feasible byte target (a rack holds at most ceil(RF/racks) copies of a chunk, so its nodes split that share) and moves a chunk only if the move strictly lowers the total distance from target and loses no distinct rack. Each moved byte lowers L1 by at most 2, so ½·L1 bytes is the least any plan can move, and the planner moves at most that plus one chunk per node ([rebalance.md](benchmarks/rebalance.md)).

A move is a repair-style copy, then a logged trim of the source. Moves run in the repair queue as the lowest class, only once membership has settled, and drain plus balance hold at most 4 of 8 slots, so a failure during a rebalance finds slots at once. The chunk never drops below RF on the way (`TestAddNodeConverges` checks every 500 ms).

```
trim:   leader ──TrimIntent{chunk,node}──► log (at most one pending per chunk, decided in log order)
        after commit: DeleteReplica{term} ──► node ──block report──► leader ──TrimDone──► log
        a new leader resends every pending trim; a deposed leader cannot create one
drain:  ACTIVE ──drain──► DRAINING ──decommission (only if every chunk has RF copies elsewhere)──► DECOMMISSIONED
        a draining node's copies still serve reads and are never trimmed until replaced
```

## Erasure coding (ADR-0022 and 0023)

An upload may store each chunk as a Reed-Solomon RS(4,2) stripe: 4 data and 2 parity shards of `ceil(size/4)` bytes, one block per node on 6 distinct nodes, at most 2 per rack. A shard is a block with a 33-byte header (slot, stripe ID) and a copy target of 1, so GC, fenced deletes, trims, scrub and drain treat it as any block.

```
chunk C (4 MiB) ─ stripe ID L = sha256("chunkd-ec-4+2:" ‖ sha256(C))
        │ RS(4,2)
        ▼
  [0|L|d0] [1|L|d1] [2|L|d2] [3|L|d3] [4|L|p0] [5|L|p1]     read: ask the 4 data shards at once;
     n1       n2       n3       n4       n5       n6          a missing or slow one is replaced by parity

repair of a lost shard 2:  target T (not holding any shard of L) fetches 4 others, decodes slot 2,
                           checks hash == shard ID, stores it, reports.
```

Measured ([ec.md](benchmarks/ec.md)): 1.50× stored against 3.00×; a dead node held half as many bytes under EC, and repair read 4 bytes per byte rebuilt (80.1 MiB to rebuild 20.0 MiB). Encoding cost is 0.66 ms per 4 MiB chunk. Commit needs 5 of 6 shards reported. With 3 or more shards gone a read fails with the stripe and its readable count. Dedup works within a policy only.

## Resumable uploads (ADR-0024)

Progress lives in the log, as the upload's claims, so any client or gateway can resume after any restart, a leader failover included.

```
client                          gateway / library                      metadata leader
  │ POST /uploads/f  Upload-Length: N, Upload-SHA256: h ──▶ Begin{size, sha256} ──▶ logged; placement
  │ PATCH /uploads/42  Upload-Offset: 0, chunks 0-2 ───────▶ claim, put ×3 ───────▶
  │ ✗ connection lost, or the leader dies
  │ HEAD /uploads/42 ───────────────────────────────────────▶ UploadStatus{42} ───▶ read-index read
  │◀── Upload-Offset: 3·4 MiB  (the leading run of chunks claimed and stored)      (new leader: same log)
  │ PATCH from 12 MiB ──▶ claim, put, ...; at offset = N: Commit{ids, h} ──▶ checks h, publishes
```

Appends are whole chunks. Measured ([results.md](benchmarks/results.md)): the bytes that reach the nodes are the same with or without resume, because a restarted Put claims every chunk and a stored chunk is skipped. Resume saves the request body, the local re-read and the re-hash: a cut at 90% re-sends 4 MiB of a 32 MiB file instead of 32 MiB. An upload resumes within its lease (3 minutes at the demo setting); after that it starts again and dedup still skips stored chunks (`TestResumeAfterClientCrash`, `TestResumeAcrossLeaderFailover`).

## Auth and quotas (ADR-0025)

```
client ── Authorization: Bearer k ──▶ gateway ── sha256(k) in the keys file? ──▶ no: 401
                                         │ path under /<namespace>/ ?        ──▶ no: 403
                                         │ ctx = WithQuota(limit)
                                         ▼
                                  BeginUpload{path, size, quota} ──▶ log: live(ns) + pending(ns) + size > quota ?
                                                                       yes: 507, nothing applied
```

A key owns one path segment and a byte quota. The quota is checked and reserved in the same log entry as Begin, so concurrent uploads that each fit cannot together pass it; commit swaps the reservation for a live version, abort and lease expiry release it. The check scans every file, which costs 0.7 us without a quota and 29 ms at 100,000 files with one ([results.md](benchmarks/results.md)); it runs inside the apply loop. Open mode (no keys file) is unchanged.

## Retention and diff (ADR-0026)

Retired versions stay restorable for a retention window (default demo-scale, 60 to 90 s); `SetRetentionOp` sets it per exact path. `diff` compares two manifests chunk by index and reports the bytes a rewrite would send; `restore` is an undelete of a named version, under CAS. Longer retention pins the chunks of retired versions, which the quota does not count.

## Dashboard view

`meta.Server.ClusterView(eventsAfter)` builds everything the dashboard shows in one pass: nodes with detector state and heartbeat age, per-file replication, repair counters and copies in flight, usage against balance targets, the metadata group, and the recent-event ring (detector transitions, copy start and end, trims, elections). The sim's `cluster.StateSince` calls it directly; the gateway serves it at `GET /cluster?events_after=N`. Events carry a sequence number, so the UI fetches only new ones; a sequence below the last seen means the leader changed. The view is soft state, served by the leader without a read-index round, and not linearizable.

## Consistency per operation

| Operation | Guarantee | How |
|---|---|---|
| Put / commit | Atomic and linearizable: the version is visible to every later read or not at all | One log entry; CAS on the expected version at Begin and Commit |
| Concurrent puts to one path | Exactly one commits per expected version; the others get `conflict` | CAS, ordered by the log |
| Stat, List, Log | Linearizable, except chunk locations, which are soft | Read-index on the leader |
| Get | Returns the committed bytes of one version, verified against chunk and file hashes, or fails; never partial or unwritten bytes | Chunk IDs are hashes; the file hash is in the version |
| Delete, undelete, restore | Linearizable, conditional on the expected version | Logged; tombstone is a version |
| Retry of any of the above after a lost response | Returns the original result | Request IDs on Begin; upload ID on the version |
| Quota check | Linearizable with the Begin it guards | Same log entry |
| Node admin, GC and trim intents | Linearizable; decided before any node is told | Logged, then sent with the term |
| Cluster view, health | Eventually consistent soft state | Leader's memory; not logged |
| Durability of an acknowledged put | The namespace survives the loss of any one metadata peer; reads survive any 2 lost copies of 3 (2 shards of 6 under EC) | Majority commit; commit needs 2 reported copies (5 shards) |

Porcupine checks the metadata operations for linearizability on 500 sim histories per push, and every real-mode scenario records and checks its history.

## CAP per plane

| Plane | Choice | Majority side of a partition | Minority side |
|---|---|---|---|
| Metadata | CP | serves reads and writes | refuses both (`NotLeader`) |
| Data reads | Available | any reachable valid replica, from a location from the majority or one the client already holds | the same, from locations already held |
| Data writes | CP | need `min_replicas` reported and a metadata majority to commit | fail loudly; nothing becomes visible |

A node cut off from every metadata peer but reachable by a client keeps serving chunks it holds. That is safe because chunks are immutable and hash-verified.

## Decisions and trade-offs

| Decision | Options | Choice | Why | At 100× |
|---|---|---|---|---|
| Language, stack (0001) | Go, Rust, Java | Go | race detector, porcupine, WASM in the toolchain | Fine; pool buffers, shard metadata |
| Sim and real from one core (0002, 0004) | real only with fault proxies; separate model; one core, two wirings | one core behind five interfaces, async messages, one loop | tests run the shipped code and replay from a seed | Run many seeds in parallel (as FoundationDB does) |
| Chunk size (0005) | 1, 4, 64 MiB | 4 MiB | parallelism, repair spread, tail latency; metadata is 39 MB per TB | 128 MiB chunks or sharded metadata |
| Metadata durability (0006) | bbolt per op; WAL plus snapshot | WAL plus snapshot, then Raft over the same ops | the op log is what Raft replicates | Shard the namespace |
| Write path (0007) | client fan-out; pipeline; primary-backup | client fan-out, commit at 2 of 3 | immutable hashed chunks need no ordering | Pipeline by network distance; overlap chunks |
| Placement (0008) | random; least-loaded; CRUSH | least-loaded with rack spread, chosen at Begin | simple, replayable | Failure-domain tree, capacity weights, CRUSH-like function |
| RPC (0009, 0012) | blocking calls; async | `Serve`/`Caller.Do`/`Hedge`/`AsyncCaller` | keeps determinism and the loop free | Batched and pipelined calls |
| Failure detector (0010) | one timeout; alive/suspect/dead | three states, stall guard | reads, commits and repair want different thresholds | SWIM-style membership |
| Repair (0011) | immediate; delayed and throttled | 20 s delay, 8 in flight, 40 MiB/s | a reboot costs nothing; foreground keeps its disk | Per-node rate, incremental under-replication index |
| Integrity (0013) | CRC sidecars; whole-chunk hash | SHA-256 verify on read, scrub at 8 MiB/s | the chunk ID is the checksum | Per-volume scanners |
| Versions and CAS (0014) | locks; LWW; CAS | CAS on logical versions | no clocks, no lost updates | Per-group CAS after sharding |
| Claims and dedup (0015) | upload then dedup; claim first | claim before write | one mechanism for dedup, GC safety and resume | Batch claims |
| GC (0016) | ownership; refcount only; refcount plus claims plus fence | claims, grace, logged intents, node fence | shared chunks, concurrent uploads | Incremental sweep |
| Raft library (0017) | hashicorp/raft; fork; etcd raft RawNode | etcd raft driven by our timers | one implementation in sim, WASM and real | Multi-group, batched fsync |
| One Raft everywhere (0018) | simplified sim Raft | the same package | a bug found in the sim is a bug in what ships | Same |
| Reads, fencing (0019) | lease reads; read-index | read-index, term fences, logged intents | no dependence on synchronized clocks | Batched read-index; follower reads |
| Rebalance (0020) | greedy by usage; HDFS-like iterations; bounded planner | pure planner with a proven bound, logged trims | terminates, minimal movement | Sampled block lists, capacity weights |
| Drain (0021) | manual copy; logged state | logged `NodeAdmin` | safe decommission check | Automatic long-dead handling |
| EC layout (0022) | striped; contiguous; per-chunk | per-chunk RS(4,2) striping | reuses every block mechanism | Wider codes, LRC |
| EC repair (0023) | decode at client; rebuild on target | rebuild on a target outside the stripe | stripe stays on 6 nodes | Rebuild thread pool |
| Resume (0024) | spool at gateway; S3 multipart; progress in the log | tus-style offsets from the log | any process can resume | Longer leases |
| Auth, quotas (0025) | counter; scan | scan inside the log entry | exact under concurrency | Per-namespace counter |
| Retention, diff (0026) | lifecycle rules; per path | one number per path | small, testable | Prefix and age rules |
| Benchmarks (0027) | real only; sim only; two tiers | sim for policy, real for the implementation | exactness where it can exist, honesty where not | Multi-host runs |

## Prior art

| | chunkd | GFS | HDFS | Ceph (RADOS) | S3 | MinIO | SeaweedFS |
|---|---|---|---|---|---|---|---|
| Unit | 4 MiB content-addressed chunk | 64 MB chunk | 128 MB block (default) | object, 4 MiB typical for RBD, RGW, CephFS | object, multipart parts | object split into erasure blocks | needle in a volume (30 GB default) |
| Metadata | Raft group of 3, whole namespace in RAM | one master, replicated log and checkpoints | active and standby NameNode, JournalNode quorum, namespace in RAM | no central object map; CRUSH computes placement; monitors hold the cluster map (Paxos) | partitioned index service, internals not public | none separate: metadata stored with each object | master (Raft) maps volumes to servers; a filer with a pluggable store holds the namespace |
| Locations | soft state from block reports | soft state from chunkserver reports | soft state from DataNode reports | computed (CRUSH), no lookup | internal | computed from the object name's hash | master's volume map |
| Redundancy | RF 3 across racks or RS(4,2), per upload | 3 replicas (Colossus later added Reed-Solomon) | 3 replicas with rack awareness, or EC (RS(6,3) default policy) | replicated pools or EC pools (k+m) per pool | erasure coded across zones; details not public | erasure coding per object over a set of drives; no plain replication within a set | replication by volume setting; EC for warm volumes (10+4) |
| Write path | client fan-out, commit at 2 of 3 | pipeline, primary orders mutations under a lease | pipeline | primary OSD fans out | internal | quorum write of erasure blocks | write to the volume, replicated by the volume server |
| Consistency | linearizable metadata; immutable verified data | relaxed: defined or inconsistent regions, at-least-once append | strong; single writer per file via lease | strong per object | strong read-after-write (since 2020) | strong, using distributed locks | master and filer consistent; file data per volume |
| Failure detection | master-side heartbeats, three states | master heartbeats | NameNode heartbeats, dead after about 10.5 min | peer heartbeats plus monitors; out after 10 min by default | internal | peer health checks | master heartbeats |
| Integrity | SHA-256 per chunk, scrubber, end-to-end | checksum per 64 KB block | CRC per 512 B, block scanner | CRC, deep scrub | checksums | bitrot protection with HighwayHash per block | CRC per needle |
| Repair | delayed 20 s, throttled, fewest copies first | prioritized by lost copies, throttled | NameNode schedules, throttled | recovery and backfill, throttled | internal | healing per erasure set | volume-level re-replication |

Where chunkd differs on purpose: 4 MiB chunks and content addressing (dedup and verification come free; HDFS and GFS chose large blocks to keep metadata in RAM at petabyte scale), claims before writes, and a linearizability checker in CI. Where it is simpler: one metadata group for the whole namespace, fixed-size chunks, no capacity weighting, no multi-region. The prior-art cells are from public papers and documentation as I understand them; defaults vary by version.

## What sim mode cannot capture

The sim models loss, duplication, reordering and delay of every message, partitions, crashes with logs intact, and every timer on one injected clock. It does not model:

- **Shared capacity.** Each message gets its own transfer time; links, NICs, disks and CPUs are never contended. Repair therefore cannot slow a read in the sim, and a benchmark of that must run on real hardware.
- **fsync semantics.** Torn or lost fsyncs at the consensus layer (the WAL's torn-tail recovery is tested on its own), write reordering, and a disk that lies about durability.
- **Disk behaviour.** Latency distributions, queueing, the page cache, a slow sector, a full disk.
- **Kernel and network.** TCP half-open connections and long stalls, congestion, MTU, DNS, a recreated container's old IP (bugs-found #24 was found only in compose).
- **True timing.** Clock drift between peers: all peers share one clock, so no sim test would catch a design that needs synchronized clocks. None here does. A process paused mid-instruction is reproduced by a partition, not by a pause. Garbage-collection pauses in Go are absent.
- **Scale.** One goroutine runs the cluster; hundreds of nodes and millions of chunks are not exercised.

Real mode (compose and `internal/real/local`) covers the transport, disks and process faults (kill, pause, wipe) and is slower and rerunnable, not replayable. Message-level faults in real mode need `NET_ADMIN`, which compose does not grant.

## Invariants checked by the harness

After each scenario, `cluster.AssertInvariants` checks:

1. Every upload that returned success (and was not deleted) reads back with the SHA-256 it was written with.
2. Every committed file downloads and matches its recorded file hash.
3. Every chunk of every committed file has at least `min_replicas` live reported locations (5 shards for a stripe).
4. Every committed chunk has a durable record with refcount ≥ 1.

The chaos runner (`internal/chaos`, `go run ./tools/task chaos`) adds, per seed:

5. After faults and the workload end, replication settles (no chunk under or over RF) within the bound.
6. At the end, no chunk is without a live copy. (The checker looks at the settled state, not every step: a transient dip below RF that heals itself passes, which is how bug #6 in `docs/bugs-found.md` got past it.)
7. Repair never exceeds its limits (peak in flight, per source, per target).
8. The same seed produces the same trace hash.
9. No successful read, during the run or after it, returns bytes that were never written to that path.
10. With bit rot in the schedule: after replication settles, two full scrub passes on every running node leave no rotten chunk on any disk, and RF settles again.
11. After GC settles, every copy on disk is marked, and refcounts and claims equal a recount.
12. At every trim delete, the chunk keeps RF intact copies on up nodes, unless a holder failed within the detection window.
13. With three metadata peers: every live peer's state is byte-identical, and every client history is linearizable (porcupine).

Real mode (`--mode=real --short`) runs seven scenarios against the compose cluster, among them: kill a node past the repair delay (RF restored, extras trimmed on return), a 15 s blip (zero copies), a paused container (reads continue, no data loss), `kill-meta-leader`, `add-node` and `drain-node`.
