# 0018. One Raft implementation in the sim, the browser and real mode

Status: accepted
Date: 2026-09-30

## Context

ADR-0002 promised two run modes from one codebase: every core component runs against sim implementations of the environment (fake clock, seeded RNG, fault-injecting network, in-memory stores) or real ones (wall clock, gRPC, disk). Phase 5 puts consensus into core. The chaos harness's value is that a failing schedule replays from its seed, and the porcupine histories are only evidence about shipped code if the sim runs the code that ships. A separate "sim Raft" would make every sim result a statement about a model.

## Options considered

| Option | For | Against |
|---|---|---|
| A simplified Raft model for the sim, a library for real mode | Sim is free of the library's scheduling | Two implementations; the sim tests the model. Which guarantees the model shares with the library has to be argued, not tested |
| The library everywhere, run on real goroutines and timers in the sim | One implementation | No replay from a seed, no fake clock, no speed control in the dashboard |
| **One library used as a state machine, driven from the environment interfaces (ADR-0017)** | The sim runs the shipped consensus code; a failing seed replays; the dashboard's 1×–50× control and pause work | The glue (ready loop, timers, storage) is ours, and its bugs are ours |

## Decision

`core/consensus` is the only place that touches the Raft library. It reaches the world through `iface.Clock`, `iface.Transport`, `iface.MetaStore` and `iface.Rand`, so the same package runs in all three settings:

| | Sim and WASM | Real |
|---|---|---|
| Time | The fake clock; election waits drawn from the seeded `Rand` | The wall clock; the same draws from the process's `Rand` |
| Messages | `sim.Net`: drop, duplicate, delay, reorder, partition, crash | gRPC unary calls (`raft.msg`), fire and forget |
| Log | `sim.MetaStore` in memory, surviving a simulated restart | `metastore`: CRC-framed WAL, fsync per save, bbolt snapshots |
| Group size | `Config.Metas`; 1 by default, 3 in the multi-peer tests | `CHUNKD_PEERS`; 3 in compose |

A group of one draws nothing from the shared RNG and leads at once, so every pre-Phase-5 seed and golden timeline replays unchanged. Client request IDs, the only other new consumer of randomness, come from their own stream and are set only when the group has more than one peer.

The multi-peer tests run the group in the sim (`TestKillLeaderMidUpload`, `TestMinorityLeaderRejectsAndStaleWriteNeverCommits`, `TestMetaGroupRestarts`, `TestMetaGroupRoundTripAndAgreement`) and over real gRPC with disk WALs (`TestKillLeaderMidUploadReal`). The sim's `AssertMetaAgree` compares every live peer's state byte for byte after each scenario.

### What the sim models, and what it does not

| Modelled | Not modelled |
|---|---|
| Loss, duplication, reordering and delay of every message; partitions in both directions; crash and restart of a peer with its log intact; a full-group restart from snapshots and logs | Torn or lost fsyncs at the consensus layer (the WAL's crash recovery is tested on its own, in `metastore`) |
| Election, replication, snapshot install, membership change, read-index, the stale leader | Clock drift between peers: all peers share one clock, so no test can catch a design that needs synchronized clocks. None here does: correctness rests on terms and log indexes |
| Timers: every one is on the injected clock | A process paused mid-instruction and resumed (a stop-the-world pause). Its effect, a leader that wakes after its quorum lapsed, is reproduced by a partition, and the quorum window is checked against the clock at the moment of use |
| | TCP's connection behaviour (half-open connections, long stalls). The real-mode tests cover the transport, not those |

## Consequences

- A consensus bug found in the sim is a bug in the shipped code, with a seed to replay.
- The library draws its own election timeout from `crypto/rand`, which no seed controls; ADR-0017 removes that by never ticking followers.
- The compose cluster and the real-mode chaos suite are the only place real timing enters; they are slower and rerunnable, not replayable.
- WASM size: the library adds about 5 MB to a hello-world binary; the dashboard binary stays under the 20 MiB budget.

## At 100× scale

The sim would model a single Raft group per process and be unable to explore multi-group interactions (range splits, leader transfers under load). A sharded metadata service would need the harness to run hundreds of groups on the same fake clock, with batched heartbeats between groups on shared peers. FoundationDB's simulator does this at cluster scale, with disk and clock faults included; that is the ceiling for this approach.
