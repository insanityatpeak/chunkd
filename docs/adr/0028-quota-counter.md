# 0028. A per-namespace usage counter for quotas

Status: accepted
Date: 2026-10-03

## Context

ADR-0025 checks a byte quota inside the Begin log entry and counted usage by scanning every file and pending upload. `BenchmarkBegin` measured the cost: 0.7 us without a quota, 825 us at 10,000 files and 29 ms at 100,000. The scan runs on every peer inside the Raft apply loop, so at 100,000 files one limited Begin holds up every other metadata write for 29 ms.

## Options considered

| Option | For | Against |
|---|---|---|
| Keep the scan | Nothing to drift | O(files) in the apply loop |
| Index paths by namespace, keep the scan | Scans one namespace | Still O(files in the namespace); a large tenant is as slow |
| **A counter per namespace, derived from the state** | O(1); `Validate` and `Apply` read the same value on every peer | A second copy of a fact the files already hold, so it can drift |
| The same counter, stored in `MetaSnapshot` | Restore skips the recount | A proto field and a format change for a value that is one pass over the files to rebuild |

## Decision

`State.nsBytes` maps a namespace to its usage: the size of its live (newest, non-tombstone) versions plus the size each of its pending uploads reserved. It changes in three places:

- `Begin` adds the upload's size.
- `dropUpload` subtracts it. Commit, Abort and lease expiry all end an upload there.
- `appendVersion` subtracts the version it supersedes if that was live, and adds the new one if it is live. Commit, Delete and Undelete all append through it.

`hardDelete` never drops the newest version of a path, so retention changes nothing. A namespace that returns to zero is removed from the map, so it does not grow with the namespaces ever seen.

The counter is derived, like `claimed` and `requests`: `Restore` rebuilds it from the files and uploads, and it is not in the snapshot. No log entry, snapshot field or wire message changed, so old logs and snapshots replay to the same state, and goldens do not move.

`Reconcile` recounts every namespace with the old scan (`recountNamespaces`) and reports each difference in `Drift.Namespaces`. It alarms through the same `Drift` count as refcounts and never corrects the counter.

## Consequences

- A limited Begin costs the same as an unlimited one: 0.9 us at 100,000 files in `BenchmarkBegin`, against 29,379 us. See `docs/benchmarks/results.md`.
- `TestQuotaCounterMatchesRecount` runs 40 seeds of 300 random ops (begin, commit, abort, delete, undelete, epoch advance with lease expiry) and compares the counter to a recount after every op and across snapshot round trips. `TestReconcileAlarmsQuotaDrift` checks the alarm and that nothing is corrected.
- The semantics of ADR-0025 are unchanged: logical bytes, an overwrite counts both versions until the commit, retired versions are not counted.
- Every new op that moves a live version or a reservation must go through `appendVersion` or `dropUpload`, or the property test fails.

## At 100x scale

The counter lives in one metadata group's memory. With the namespace sharded by prefix (ADR-0006), a quota that spans shards needs a counter per shard and a reservation protocol between them. Stored-byte accounting by redundancy class, and the limit in a logged namespace record, remain as in ADR-0025.
