# 0014. Versioned files, compare-and-swap commits, opt-in last writer wins

Status: accepted
Date: 2026-09-29

## Context

Two clients can upload the same path at once. Each upload takes seconds (chunks go to three nodes before commit), so the window is wide. Whatever the metadata server does, one of the two writes is not the final content. The question is whether a writer learns that.

Every committed version is already immutable and numbered per path (ADR-0006). A delete appends a tombstone version. The live version is the newest one unless it is a tombstone.

## Options considered

| Option | For | Against |
|---|---|---|
| **Compare-and-swap on commit**: `Begin(path, expected)` and `Commit` both check that the live version is still `expected` | A writer that raced loses loudly (`ErrVersionConflict`) and can re-read and retry. Same model as etcd `txn`, S3 `If-Match`, GCS generation preconditions | The client must know the version it replaces (`chunkd put` reads it first unless `--expected` is given) |
| Last writer wins | No read before write | A concurrent update is lost silently. Which one survives depends on commit order, which the writers cannot see |
| Locks or leases on the path | Serialises writers | A dead lock holder blocks the path until the lease runs out, and needs a clock bound |

## Decision

CAS by default. The check runs at Begin (fail fast before sending data) and again at Commit (another writer may have committed in between). `Begin.last_writer_wins` skips both checks and is opt-in (`chunkd put --lww`, `?lww=1` on the gateway). Commit is idempotent by upload ID, so a retried commit after a lost response returns the same version instead of a conflict.

Old versions stay readable (`chunkd get --version N`, `chunkd log <path>`) until retention drops them (ADR-0016).

## Consequences

- `TestCASConflict`: two commits against the same expected version, exactly one wins, and the loser's content is never visible. `TestChaosConcurrentWriters` repeats this over 25 seeds on a lossy network and checks that the loser's chunks are collected.
- A loser's chunks are garbage the moment it fails. GC must collect them without touching chunks it shares with the winner (dedup): refcounts, not ownership.
- LWW is documented as losing updates. Nothing uses it by default.

## At 100× scale

Contention per path does not change with cluster size, but the metadata server serialises every commit. The per-path check stays O(1); the bottleneck is the single log (ADR-0017 replicates it but does not shard it). Sharding the namespace across metadata groups keeps CAS per path local to one group.
