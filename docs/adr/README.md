# Architecture decision records

Format: Context, Options considered, Decision, Consequences, At 100× scale ([template](0000-template.md)). No ADR has been superseded. Three were amended by later ones; the status line of each says how.

| ADR | Title | Status | Amended by |
|---|---|---|---|
| [0001](0001-language-and-stack.md) | Language and stack | accepted | [0017](0017-raft-library-and-log-storage.md) |
| [0002](0002-sim-and-real-modes.md) | Sim and real modes from one codebase | accepted | [0017](0017-raft-library-and-log-storage.md) (clarifies the Raft example) |
| [0003](0003-static-hosting-on-github-pages.md) | Static hosting on GitHub Pages | accepted |  |
| [0004](0004-event-driven-transport.md) | Event-driven transport and deterministic executor | accepted |  |
| [0005](0005-chunk-size.md) | Chunk size: 4 MiB | accepted |  |
| [0006](0006-metadata-model-and-durability.md) | Metadata model and durability | accepted | [0017](0017-raft-library-and-log-storage.md) to [0019](0019-metadata-reads-fencing-and-failover.md) |
| [0007](0007-write-path.md) | Write path: client fan-out, commit at 2 of 3 | accepted |  |
| [0008](0008-placement.md) | Replica placement: rack spread, then least loaded | accepted |  |
| [0009](0009-rpc-layer.md) | Request/response on top of the event-driven transport | accepted |  |
| [0010](0010-failure-detector.md) | Failure detector: alive, suspect, dead | accepted |  |
| [0011](0011-re-replication.md) | Re-replication: priority queue, delay, throttle, trim | accepted |  |
| [0012](0012-async-caller-and-hedged-reads.md) | AsyncCaller for in-loop RPC, and Caller.Hedge | accepted |  |
| [0013](0013-integrity-model.md) | Integrity: verify on every read, scrub, quarantine | accepted |  |
| [0014](0014-versioned-files-and-cas.md) | Versioned files, compare-and-swap commits, opt-in last writer wins | accepted |  |
| [0015](0015-claims-and-atomic-commit.md) | Chunk claims: dedup, the GC mark for uploads, atomic commit | accepted |  |
| [0016](0016-garbage-collection.md) | Garbage collection: logical epochs, mark-and-sweep, fenced deletes | accepted |  |
| [0017](0017-raft-library-and-log-storage.md) | Raft library, self-driven timers and the log store | accepted |  |
| [0018](0018-one-raft-in-sim-and-real.md) | One Raft implementation in the sim, the browser and real mode | accepted |  |
| [0019](0019-metadata-reads-fencing-and-failover.md) | Metadata reads, term fencing and failover | accepted |  |
| [0020](0020-rebalancing.md) | Rebalancing: a central planner, rack-feasible targets, logged trims | accepted |  |
| [0021](0021-drain-and-decommission.md) | Drain and decommission: logged node states | accepted |  |
| [0022](0022-erasure-coding-layout.md) | Erasure coding: RS(4,2) stripes inside each chunk | accepted |  |
| [0023](0023-ec-repair-and-degraded-reads.md) | EC repair: node-side shard rebuild | accepted |  |
| [0024](0024-resumable-uploads.md) | Resumable uploads | accepted |  |
| [0025](0025-auth-and-quotas.md) | API keys, namespaces and byte quotas | accepted |  |
| [0026](0026-version-retention-and-diff.md) | Per-path retention and chunk-level version diff | accepted |  |
| [0027](0027-benchmark-method.md) | Benchmark method: a sim tier and a real tier | accepted |  |
