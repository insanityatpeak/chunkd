# Design

This document describes what exists, not what is planned.

## Components

```
   CLI / browser ──HTTP──► gateway ─┐   (client library: verifies every chunk and the file)
   CLI (-meta) ──────────────────────┤
                                     │ control: begin, commit, stat, list, delete, cluster (unary gRPC)
                                     ▼
                              ┌─────────────┐   WAL + bbolt snapshots
                              │   meta-1    │   namespace, versions, refcounts, pending uploads
                              └──────▲──────┘   node view + chunk locations (rebuilt, not stored)
               heartbeat 1 s,        │
               block reports ────────┘
      ┌──────────┬──────────┬──────────┬──────────┐
    node-1     node-2     node-3     node-4     node-5        chunks at ab/cd/<sha256>
     r1         r2         r3         r1         r2
      ▲  data: chunk.put / chunk.get (gRPC streams, 1 MiB frames), client ↔ nodes directly
```

The metadata server is never on the data path.

## Run modes

| | sim | real |
|---|---|---|
| Processes | one | one per component |
| Transport | `sim.Net`: in-memory, seeded loss, duplication, delay, bandwidth, partitions, crashes | `grpcnet`: Deliver (one-way), Call (unary), CallStream (framed) |
| RPC client | `sim.Caller`: advances the clock until answered | `grpcnet.Caller`: goroutine per call |
| Clock | `sim.Clock`: event queue ordered by (deadline, seq) | wall clock; timers post to `runtime.Loop` |
| Chunk store | `sim.BlockStore` | `blockstore`: files, atomic rename |
| Metadata log | `sim.MetaStore` | `metastore`: WAL + bbolt |
| Where | tests, `cmd/chunkd-wasm` in the browser | `docker compose up`, `internal/real/local` in tests |

Both modes run the same `internal/core/{meta,node,placement,chunk}` code and the same `internal/client`.

## Upload

```
Client               Meta                         node-A      node-B      node-C
  │─BeginUpload(path, expected v, size)─►│
  │                                      │ CAS on live version; place each chunk
  │                                      │ (rack spread, least loaded); WAL append
  │◄──upload_id, placement, min_replicas─│
  │ read chunk i, SHA-256 → id_i
  │─chunk.put(id_i, bytes)────────────────────────────►│──────────►│──────────►│  in parallel
  │                                      │◄─block report "received id_i"──────┤  each node, before acking
  │◄─ack──────────────────────────────────────────────┤  (need ≥ min_replicas acks, else abort)
  │─CommitUpload(upload_id, [ids], file sha256)─►│
  │                                      │ each id has ≥ 2 live reported locations?
  │                                      │   no  → retry (client backs off up to 45 s)
  │                                      │   yes → CAS again, WAL append: version visible
  │◄──version─────────────────────────────│   repeated commit of the same upload → same version
```

## Download

```
Client               Meta                  node-A      node-B
  │─Stat(path)─────────►│
  │◄─version, sha256, [id_i → live replicas]
  │─chunk.get(id_i)──────────────────────────►│
  │◄─bytes: SHA-256 ≠ id_i ✗ ─────────────────┤
  │─chunk.get(id_i)──────────────────────────────────────►│   next replica
  │◄─bytes: SHA-256 = id_i ✓ ─────────────────────────────┤
  │ … every chunk, then file SHA-256 must equal the committed one
```

## Heartbeats and block reports

- Every node heartbeats each second: rack, address, bytes and chunks stored. Whether a node is draining or decommissioned is logged metadata state set by the operator (ADR-0021), not part of the heartbeat.
- The heartbeat ack asks for a full block report when the metadata server has none for the node (first contact, or after a metadata restart).
- Nodes send an incremental report after storing each chunk, and a full report every 30 s, which repairs any lost incremental report.
- Heartbeats carry `(incarnation, seq)`; the incarnation changes on every process start.

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

- `core/detector` ticks every 500 ms. A gap over 1 s between ticks means the metadata server itself stalled; every node's last-seen moves forward by the gap.
- A dead or restarted node keeps its chunk locations until its next full block report.

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

- Queue: fewest live copies first, then FIFO. A chunk with one live copy skips the delay.
- Limits: 8 copies in flight, 2 per source node, 2 per target node, 40 MiB/s token bucket.
- Periodic scan every 30 s catches anything the event-driven path missed.
- Trim: when more than 3 confirmed copies exist (alive, full report since returning) and no copy is in flight, drop the copy on the rack with the most copies, then the most used node. A trim with no confirmation in 10 s is retried against the same node.
- RF restore bound: `10 s (dead) + 20 s (delay) + bytes ÷ 40 MiB/s + 10 s (one copy timeout) + 5 s`.

## Reads under slow replicas (ADR-0012)

The client orders replicas alive first, then suspect, each by its own latency score (EWMA, failures penalised), and reads with `Caller.Hedge`: if no verified answer arrives within the p95 of recent reads (clamped to 20–500 ms), the next replica is asked too. The first answer whose SHA-256 matches the chunk ID wins.

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

## Dashboard view

`meta.Server.ClusterView(eventsAfter)` builds everything the dashboard shows in one pass: nodes with detector state and heartbeat age, per-file replication (chunks below RF, fewest alive copies), repair counters and copies in flight, and the recent-event ring (500 events: detector transitions, copy start and end, trims). The sim's `cluster.StateSince` calls it directly; the gateway serves it at `GET /cluster?events_after=N`. Events carry a sequence number, so the UI fetches only new ones; a sequence below the last seen means the metadata server restarted.

## Invariants checked by the sim harness

After each scenario, `cluster.AssertInvariants` checks:

1. Every upload that returned success (and was not deleted) reads back with the SHA-256 it was written with.
2. Every committed file downloads and matches its recorded file hash.
3. Every chunk of every committed file has at least `min_replicas` live reported locations.
4. Every committed chunk has a durable record with refcount ≥ 1.

The chaos runner (`internal/chaos`, `go run ./tools/task chaos`) adds, per seed:

5. After faults and the workload end, replication settles (no chunk under or over RF) within the bound.
6. At the end, no chunk is without a live copy. (The checker looks at the settled state, not every step: a transient dip below RF that heals itself passes, which is how bug #6 in `docs/bugs-found.md` got past it.)
7. Repair never exceeds its limits (peak in flight, per source, per target).
8. The same seed produces the same trace hash.
9. No successful read, during the run or after it, returns bytes that were never written to that path.
10. With bit rot in the schedule: after replication settles, two full scrub passes on every running node leave no rotten chunk on any disk, and RF settles again.

Real mode (`--mode=real --short`) runs three scenarios against the compose cluster: kill a node past the repair delay (RF restored, extras trimmed on return), a 15 s blip (zero copies), and a paused container (reads continue, no data loss).
