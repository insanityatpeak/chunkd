# 0016. Garbage collection: logical epochs, mark-and-sweep, fenced deletes

Status: accepted
Date: 2026-09-29

## Context

Space must come back after deletes, overwrites and abandoned uploads, without ever deleting a copy that a committed or retained version, or an upload in flight, still needs. Deletes should be undoable for a while. Nothing may depend on wall clocks agreeing across machines.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Reclaiming | Refcount reaches 0, delete at once; **mark-and-sweep against block reports** | Sweep. Block reports also show copies metadata never knew about (a node back from a long outage, a crashed upload) |
| Retention clock | Wall-clock timestamps; log-index distance; **GC epochs as logged ops** | Epochs. The server's timer proposes `AdvanceEpoch`; what it drops is decided by applying the op, identically on every replica of the log |
| Protecting in-flight uploads | Grace period only; **claims (ADR-0015) + lease + grace + commit re-check** | Claims mark chunks before any copy exists; the grace covers unclaimed arrivals (repair copies, returning nodes) |
| Delete racing a re-write | Accept it; tombstone per chunk; **conditional delete fenced by the node's report sequence** | Fence. Same idea as HDFS generation stamps and fencing tokens: the node refuses a command made from a stale view |

## Decision

**Versions.** Appending a version retires the one it supersedes at the current epoch. `AdvanceEpoch{retain, lease}` drops retired versions with `retired_at + retain <= epoch` and decrements their refcounts. It never drops the newest version of a path, even a tombstone, so version numbers are never reused. A chunk record at refcount 0 is deleted. Undelete appends a copy of a retained version under CAS; a retry finds its own result.

**Leases.** An upload's lease is the epoch of its begin or latest claim. `AdvanceEpoch` aborts uploads idle for `lease` epochs, releasing their claims. The minimum lease, `(LeaseEpochs-1) × EpochEvery` = 150 s, exceeds the grace plus two sweeps (120 s), so a stall GC notices never also expires the upload (bugs-found #10).

**Mark.** `State.Marked(id)`: a chunk record exists (a retained version references it), or a pending upload claims it. A pure function of the log.

**Sweep**, after each epoch, on soft state. For every located chunk that is unmarked, remember when it was first seen unmarked. Once that is `GCGrace` (60 s) ago, send each alive holder `DeleteReplica{gc, fence_incarnation, fence_seq}`, where the fence is the highest block-report sequence the server has applied from that node.

**Node.** Every chunk write records the sequence number of its change. A GC delete is refused if the incarnation differs, or if the chunk was written after `fence_seq`. The check and the delete run under a per-chunk lock that writes also take. The answer, deleted or kept, goes back through the ordered block-report path.

**Why the fence is enough.** A client writes a chunk only after its claim is logged. A write numbered at or below the fence happened before a report the server had applied when it decided, so the claim, if there was one, was already applied and the chunk would have been marked. A chunk still unmarked at decision time is garbage. A write numbered above the fence may belong to a later claim, so it is kept.

**In-flight deletes.** A copy with a GC delete pending does not count toward commit, dedup `present`, or repair's holders until the node answers. A lost delete or answer is re-sent next epoch with its original fence, never a newer one: a newer fence would permit deleting a copy re-written after the first send.

**Reconciliation.** Each epoch, refcounts and claim counts are recounted from the versions and pending uploads. Drift sets `chunkd_meta_refcount_drift_chunks`, logs an error and adds a timeline event. It is never corrected automatically: drift means an op applied wrongly, and rewriting counts would hide the bug and could let GC delete a referenced chunk.

### The races, step by step

Upload versus sweep:

```
t0  upload U claims X (logged)        X marked
t1  U writes X to N1, N2              reports seq 41, 17
t2  sweep: X marked, skipped
t3  U commits: refcount(X) = 1        claim released, still marked
```

Without claims, t2 sees X unreferenced and deletes it, and t3 publishes a version with a missing chunk.

Delete versus re-write:

```
t0  X unmarked on N (orphan); sweep sends delete fenced at seq 40; the message is lost
t1  upload V claims X; not present (delete pending), so V writes X to N: seq 44
t2  next sweep re-sends the delete with fence 40
t3  N: lastWrite(X) = 44 > 40, kept; answers kept_ids
```

Without the fence, N deletes the copy it just acknowledged to V. `TestChaosNodeReturnsWithDeletedChunks` builds this sequence and fails if the fence is removed.

## Consequences

- After every chaos run, once `GCSettle` has passed: every copy on disk is marked; refcounts and claims equal a recount; every file still reads back. 1,000 seeds.
- Tests: the chaos seeds (no orphan leak), `TestGCSparesInflightUpload`, `TestUploadLeaseExpires`, `TestGCReclaimsDroppedVersions`, `node.TestGCDeleteFence`, `TestReconcile`, and the four `TestChaos*` GC scenarios in `internal/sim/cluster`.
- `SIMPLIFIED:` demo-scale retention (60–90 s). S3 lifecycle rules typically keep noncurrent versions for days.
- `SIMPLIFIED:` a GC delete delayed by more than one epoch, arriving after a newer send was answered, could still remove an uncounted copy. gRPC streams deliver in order, so this needs a reconnect in between, and repair would restore the copy.
- Node memory: one sequence number per chunk written in the current incarnation.

## At 100× scale

The sweep is O(located chunks) per epoch on one server: about 1 s per 10 M chunks. GFS and HDFS spread it out, piggybacking deletes on heartbeats and scanning incrementally. Reconciliation is O(references) and would become sampled or incremental. Epochs stay cheap: one small log entry each.
