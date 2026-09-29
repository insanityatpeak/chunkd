# 0010. Failure detector: alive, suspect, dead

Status: accepted
Date: 2026-09-29

## Context

Phase 1 had one threshold: a node was alive if heard from within 5 s. That single bit decided commits, placement, reads and (from Phase 2) repair, which want different things:

- Repair is expensive: a 4 TB node at 40 MiB/s is 29 hours of copying. Declaring a node dead on a 2 s network hiccup or a JVM-style pause must not start it.
- Reads and placement want to route around a node quickly, within seconds.
- A node that restarts comes back with a new process and a reset heartbeat sequence; a duplicated or delayed beat from the old process must not revive the new one's history.
- The metadata server itself can stall (GC pause, slow fsync, a paused container). When it wakes, every node looks silent for the length of the stall.

Timing only decides liveness, never correctness: commit counts replicas the nodes reported, and versions are compare-and-swap (ADR-0006, ADR-0007).

## Options considered

| Option | For | Against |
|---|---|---|
| One timeout (Phase 1) | Simple | One number cannot be both fast for reads and slow for repair |
| Phi-accrual (Cassandra, Akka) | Adapts to observed jitter | Threshold is harder to reason about and test; heartbeat intervals here are fixed and local |
| **Three states with fixed thresholds and hysteresis (HDFS stale/dead)** | Each consumer picks the state it needs; bounds are exact numbers the tests assert | Thresholds are tuned by hand |
| Lease-based membership | Nodes know when they are out | Needs clock-bound assumptions on nodes; nothing here acts on a node's own view |

## Decision

`core/detector` is a pure state machine over logical time, fed heartbeats and ticks by the metadata server's loop.

| From | To | When |
|---|---|---|
| alive | suspect | 3 s without a heartbeat |
| suspect | dead | 10 s without a heartbeat |
| suspect or dead | alive | 3 consecutive on-time beats (each ≤ 1.5 s after the previous) |
| any | suspect (restarted) | a beat with a new incarnation |

- Ticks every 500 ms. **Stall guard:** a gap between ticks above 2 × 500 ms means the metadata server stalled; every node's last-seen shifts forward by the gap, so a pause of meta kills nobody. Counted in `detectorStalls`.
- Heartbeats carry `(incarnation, seq)`. A beat with an old incarnation or a seq not above the last one is ignored (duplicated or reordered delivery).

| State | Serves reads | New placement | Counts for commit | Counts toward RF for repair |
|---|---|---|---|---|
| alive | yes, first | yes | yes | yes |
| suspect | yes, after alive replicas | no | no | yes |
| dead | no | no | no | no, after the repair delay (ADR-0011) |

A dead or restarted node keeps its chunk locations until its next full block report: its disk is usually intact, and dropping locations early caused needless copies (`docs/bugs-found.md` #4).

## Consequences

- Transient blips (2 s, 15 s, 25 s down) cost zero repair copies: `TestTransientBlipNoRepair` in the sim and `transient-blip-no-repair` in real mode.
- A paused metadata server does not declare the cluster dead: `detector.TestDetector` ("clock jump: a 15 s stall of the owner kills nobody" and the two cases after it) and `TestStallCounted`. In real mode, `freeze-node` pauses a node container with `docker pause`.
- A suspect node is readable, so a slow-but-alive node still serves data, and hedged reads (ADR-0012) cover its latency.
- Gray failures (a node that heartbeats but serves slowly) are invisible to the detector; the client's latency scoring handles those for reads only.

## At 100× scale

At 500 nodes a single metadata server still handles 500 heartbeats/s easily. Beyond that: heartbeats aggregated per rack, or a membership protocol (SWIM) with the metadata server consuming its verdicts. Thresholds would move from fixed numbers to per-node jitter estimates, since tail latency on a large network makes a fixed 3 s produce steady false suspicion.
