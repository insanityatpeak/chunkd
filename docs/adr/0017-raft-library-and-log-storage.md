# 0017. Raft library, self-driven timers and the log store

Status: accepted
Date: 2026-09-30

## Context

The metadata server is a single process (ADR-0006). Phase 5 makes it a group of three that keeps serving, without losing an acknowledged operation, when one dies or is cut off. The same code has to run three ways: as real processes, in the sim (one goroutine, fake clock, seeded RNG, replayable from a seed), and compiled to WASM for the dashboard. The 500 sim histories that porcupine checks are only evidence about the shipped consensus code if the sim runs that code.

## Options considered

| Option | For | Against |
|---|---|---|
| `hashicorp/raft` for real mode, a simplified sim Raft for the sim | Ships a TCP transport, a bolt log and file snapshots | Runs its own goroutines on wall-clock timers, so it cannot run on the fake clock or replay from a seed. The sim would test a second model, not the code that ships |
| Fork `hashicorp/raft` to inject the clock | One implementation | Carrying a fork of a large library, to change its scheduling model |
| **`go.etcd.io/raft/v3` `RawNode`** | A pure state machine: no goroutines, no timers, no I/O. The caller feeds it `Tick`, `Step` and `Propose`, and persists and sends what `Ready` returns. Used by etcd and CockroachDB (fork). Builds for `js/wasm` | The caller writes the transport glue, log storage and the ready loop (about 600 lines, all in `core/consensus`) |
| Own Raft implementation | Full control | The safety argument is the whole point; a hand-written Raft is the weakest possible one |

Measured for a hello-world WASM: 2.5 MB alone, 7.5 MB with `hashicorp/raft`, 7.6 MB with etcd raft. Most of the difference is protobuf and runtime, which the dashboard binary already carries.

## Decision

**Library.** `go.etcd.io/raft/v3` v3.7.0 through `RawNode`, driven from the metadata server's event loop (ADR-0004). One implementation serves sim, WASM and real mode. This replaces `hashicorp/raft` in the locked stack.

**Timers.** The library draws its randomized election timeout from `crypto/rand` (`raft.go`, `globalRand`); no seed reaches it. So followers are never ticked, and `core/consensus` owns the timers, on the injected `Clock` and `Rand`:

- A follower that has not heard from a leader for a seeded `[ElectionTimeout, 2×)` calls `Campaign`, with PreVote on. Only a leader is ticked, for its heartbeats.
- The two protections the library builds on its own election timer are done here instead, since `CheckQuorum` reads that timer: the vote lease (a peer that heard from a leader within `ElectionTimeout` drops vote requests, so an isolated peer cannot depose a working leader) and the leader's quorum window (a leader with no response from a majority within `ElectionTimeout` reports `IsLeader=false` and must not send commands).
- Granting a real vote counts as contact; granting a pre-vote does not, or the real vote that follows would meet the granter's own lease (found by the first stale-leader test).

The library's safety never depends on these timers; only liveness does.

**Log store.** `MetaStore` (`iface`) becomes the durable state of one Raft peer:

- `Save(first, entries, state)` writes entries from index `first` on, replacing any stored entry at or after it (a follower's conflicting tail), and the hard state (term, vote, commit), in one record and one fsync. Entries and hard state must be atomic: a commit index past the last stored entry stops the peer from starting.
- `SaveSnapshot(at, data)` records the state machine at `at` and drops covered entries. `State`, `Replay` and `LoadSnapshot` read back.
- The WAL is one CRC-framed record per `Save`, so a crash can tear only the last one. Nothing is rewritten in place: a record drops the earlier entries at or after its first index, and the last state record wins. etcd's WAL does the same.
- The store keeps entries only after the snapshot. The consensus layer keeps `Trailing` entries before it in memory, so a briefly lagging follower catches up from the log; after a restart it gets the snapshot instead.

**State machine.** `State.Apply` returns the validation error instead of panicking. Two ops proposed together are each valid against the state they were proposed on and can invalidate each other (two commits on one expected version); the log orders them and the loser is rejected identically on every replica.

**Snapshots.** Every `SnapshotEvery` (1,000) applied entries the FSM is snapshotted, the store drops covered entries and memory keeps `Trailing`. A snapshot sent to a lagging follower is cut fresh whenever the stored one predates a membership change: the library refuses a snapshot whose membership lacks the receiver, so a voter added after the last snapshot would never catch up (caught by `TestAddAndRemoveVoter`).

**Membership.** Three voters, bootstrapped from configuration with every peer given the same list. Single-server changes only (add or remove one voter, one at a time).

## Consequences

- One consensus implementation runs in the sim, the browser and real mode; chaos seeds and porcupine histories test what ships.
- The glue is ours to defend: ready-loop ordering (persist, send, apply, advance), snapshot and compaction, and the timers. Tests cover election, replication order, failover and restart, a stale leader, a minority partition, read-index, log bounds and snapshot catch-up, membership change, and 30 seeds of random partitions and crashes with loss and duplication that check every acknowledged proposal survives once, in one order.
- A failed `Save` panics: a peer that cannot persist what it acknowledged would break Raft's safety, not only its availability.
- The old single-node WAL format is not readable (`CHWAL002`); a data directory from before Phase 5 must be empty.
- SIMPLIFIED: single-server membership changes. Joint consensus changes several members at once; etcd and CockroachDB use it for replacing a whole failure domain.
- SIMPLIFIED: one fsync per `Save`. etcd pipelines the fsync with sending; TiKV batches many regions' writes into one.
- SIMPLIFIED: a snapshot travels as one message. etcd streams snapshots in chunks over a dedicated connection.

## At 100× scale

A single Raft group holds all metadata in one process's memory and serializes every write through one leader, which tops out at tens of thousands of operations per second and a few million files. Past that the namespace is partitioned into ranges, each its own Raft group (CockroachDB, TiKV, Ceph's monitor split from MDS ranks), with a thin routing layer above and batched heartbeats between groups on the same peers. Snapshots would stream and be cut incrementally instead of encoding the whole state, and the membership change path would need joint consensus for rack-level moves.
