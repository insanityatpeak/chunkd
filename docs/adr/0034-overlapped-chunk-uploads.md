# 0034. Overlapped chunk uploads

Status: accepted
Date: 2026-10-20

## Context

`Direct.upload` stores one chunk at a time: a claim round trip to the metadata leader, then a quorum put to the chunk's replicas (or a stripe's six shard holders), then the next chunk. A file of N chunks costs N claim RTTs plus N put RTTs in series: at 4 MiB chunks (ADR-0005) a 100 MiB file is 25 of each. A slow replica is already off the critical path (quorum puts, ADR-0029); the waste is the serial round trips. `Direct.Put` carries a SIMPLIFIED note for this; HDFS keeps a window of packets in flight on its write pipeline.

Constraint: in sim, `Caller.Do` and `Caller.Quorum` advance the fake clock on the calling goroutine (ADR-0004). Two goroutines calling them at once would race the clock and break seed replay, so overlap cannot come from client goroutines.

## Options considered

| Option | For | Against |
|---|---|---|
| Leave it | Nothing new | Put latency grows linearly with chunk count and with the slowest replica of each chunk |
| Goroutine window in real mode, sequential in sim | Small change in grpcnet only | Sim and chaos never run the code path real clients use; the per-chunk ordering and abort logic is untested under faults |
| Batch claims only: one `ClaimChunks` per W chunks | Proto already takes a list; deterministic | Removes metadata RTTs only; replica puts stay serial |
| **Waves of W chunks: one claim RPC for the wave, then one new `Caller.Quorums` call that sends every chunk's puts and returns when each group has its need plus grace** | One code path in sim and real; deterministic; claims and puts both overlap | A wave waits for its slowest chunk before the next starts (lockstep, not sliding); one new `Caller` method in both transports |
| Sliding window: async `Caller.Start` handles plus `Wait(any)` | Best latency; matches HDFS | Changes the Caller contract (ADR-0009) and the sim event loop; much larger change for the gap above waves |

## Decision

Waves, `Options.Window` W, default 4 (`DefaultWindow`); 1 sends one chunk at a time, exactly as before.

- The splitter reads up to W chunks; the client claims them in one `ClaimChunksRequest`, with stripes encoded first as now.
- `Caller.Quorums(ctx, groups []QuorumGroup{Calls, Need}, grace) [][]Result` sends every call at once and returns when every group has Need successes plus grace, or has settled. Calls still in flight are `Pending`, as with `Quorum`. `Quorum` becomes `Quorums` with one group, so each transport keeps one implementation: the sim loop over all groups, grpcnet's existing goroutine fan-out. One conformance case joins ifacetest/rpc.go.
- Each chunk keeps its second round for failed calls, so a lost put still costs no replica. A chunk with fewer than its minimum after two rounds fails the put, which aborts the upload as today.
- `m.Chunk` and the commit's chunk IDs stay in index order whatever order the results arrive in.
- Resume (`resume.go`) stores the missing chunks through the same wave function.

SIMPLIFIED: lockstep waves, so a slow chunk holds its wave. HDFS slides its packet window and acks per packet; Ceph and S3 multipart clients keep N parts in flight independently.

## Consequences

- Client memory: up to W chunks plus their shards instead of one; at W = 4 and 4 MiB chunks, 16 MiB per upload (24 MiB for EC 4+2).
- The sim's RNG draws move with the new message order, so the scenario goldens will be regenerated; each keeps its defining events. Chaos runs with W = 4 so the windowed path is what faults hit.
- `TestWindowOverlapsChunks`: a 16-chunk put of 64 KiB chunks takes 1,640 ms at W = 1 and 549 ms at W = 4 replicated (3.0x), 1,751 and 485 ms for EC (3.6x). `TestWindowFailedChunkAbortsUpload`: a chunk short of its minimum fails the put and nothing becomes visible.
- `TestBenchPutWindow` (`put-window.csv`), 32 MiB files of 4 MiB chunks on 7 nodes, p50: replicated 1,103, 624, 359 and 234 ms at W = 1, 2, 4, 8; EC 943, 549, 317 and 201 ms; a gray node scales the same way, so lockstep waves do not make it worse.
- Why 4 and not 8: sim links charge each message's transfer on its own, so the sim client's uplink is never shared and these gains are the round trips saved. A real client sending 32 MiB × 3 copies over one 1 Gbit/s uplink cannot finish under about 770 ms at any window, which W = 2 to 4 already approaches; 4 leaves room for slower links at 16 MiB in flight.
  SIMPLIFIED: the sim has no per-node uplink. Ceph and HDFS clients are bounded by the client NIC; a replication pipeline (HDFS) sends one copy from the client instead of three.
- Chaos at W = 4: 1,000 default, 500 `--metas=3` (106 peers replaced) and 500 `--ec` seeds pass, with the per-step safety watcher on.
- The sim's RNG draws moved with the new message order and exposed a test that read one lost message as the deposed leader's answer (`TestMinorityPartitionRejectsWrites`); it now resends a call that timed out. The server answered `NotLeader` throughout.

## At 100x scale

Window by bytes in flight rather than chunk count, so large chunks or EC do not multiply client memory; past that, the sliding window from the options table.
