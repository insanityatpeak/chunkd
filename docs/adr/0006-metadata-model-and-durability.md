# 0006. Metadata model and durability

Status: accepted
Date: 2026-09-29

## Context

The metadata server maps paths to versions and versions to chunks. It must survive crashes without losing acknowledged commits, never expose a half-written file, and be ready for Raft replication (Phase 5) and garbage collection (Phase 4).

## Model

```
files:   path → versions[] → { v, upload_id, size, sha256, chunk_size, chunk_ids[], committed | tombstone }
chunks:  chunk_id → { refcount, size }
uploads: upload_id → { path, expected_version, size, chunk_size, placement[][] }   pending, invisible
NOT durable: chunk locations, node load, liveness. Rebuilt from heartbeats and block reports.
```

- Versions are logical and per path. `BeginUpload(path, expected_version)` is a compare-and-swap against the live version (0 = must not exist); the check repeats at commit, so of two racing writers exactly one wins.
- A version becomes visible in the single log entry that commits it. Pending uploads never appear in `Stat` or `List`.
- `refcount` counts committed references; Phase 4 GC decrements it. The schema is refcount-ready now.
- Each version records the upload that created it, so a repeated commit returns the same version (see `docs/bugs-found.md` #2).

## Options considered for durability

| Option | For | Against |
|---|---|---|
| bbolt only, one transaction per op | Simple; bbolt fsyncs each commit | Every op rewrites B+tree pages; no op log to replicate later |
| **WAL of ops + periodic snapshot in bbolt** | Append-only fsync per op; the op log is exactly what Raft replicates in Phase 5; snapshots bound replay time | Two files to keep consistent |
| Snapshot only (GFS checkpoint + edit log) | This is the same design under other names | — |

## Decision

WAL + snapshot. `Append` writes `[len][crc32c][op]` and fsyncs before the op is applied; every 1000 ops the state is snapshotted into bbolt with its index and the WAL is rewritten without the covered entries. Recovery loads the snapshot and replays the WAL after it. A torn final record (crash mid-append) is truncated; a bad checksum before the tail is an error, never silently truncated, because it would drop acknowledged ops.

Ops are applied by a pure function of the prior state. Anything decided from live, non-durable state (placement) is written into the op, so replay and future Raft followers reach identical state without seeing the same live view.

Locations are not persisted. As in GFS and HDFS, nodes are the source of truth for what they hold; a restarted metadata server marks every node as "no report yet" and asks for a full block report in the heartbeat ack.

## Consequences

- Single metadata server: while it is down nothing can be read or written. Chunk data on nodes is untouched, and the server recovers from its own disk.
- After a restart, reads return no replicas for a chunk until its holders report (under a second with 1 s heartbeats).
- Abandoned pending uploads stay in the state until Phase 4 GC.
- `TestRestartRecoversStateAndLocations`, `TestSnapshotDuringOperationRecovers` and the metastore torn-tail tests back these claims.

## Path to high availability (Phase 5)

`MetaStore.Append` becomes "propose to Raft and wait for commit". The state machine, op format and snapshot format do not change. The three metadata containers in compose already exist for this; today meta-2 and meta-3 are idle.

## At 100× scale

The whole namespace lives in RAM on one server (as with GFS masters and HDFS NameNodes). At 100× files that is tens of GB; the answers are namespace sharding (HDFS federation) or moving metadata to a replicated key-value store (Colossus on Bigtable/Spanner).
