# 0008. Replica placement: rack spread, then least loaded

Status: accepted
Date: 2026-09-29

## Context

Three replicas protect against disk and node loss only if they do not share a failure domain. Nodes carry a `rack` label. Placement runs on the metadata server at `BeginUpload`, for every chunk of the file at once.

## Options considered

| Option | For | Against |
|---|---|---|
| Random | Simple, spreads load on average | Can put all replicas in one rack |
| **Rack spread, then least loaded** | Survives a rack loss; fills nodes evenly | Needs load data (heartbeats) |
| HDFS default (local node, remote rack, same remote rack) | Cheap cross-rack traffic | Two replicas share a rack |
| CRUSH (Ceph) | No central placement; multi-level failure domains | Complex; overkill for a central metadata server |

## Decision

For each chunk:

1. Eligible nodes are alive (heartbeat within `dead-after`) and not draining.
2. Each next replica is the least-loaded eligible node on a rack not yet used for this chunk. Racks repeat only when every rack is used; nodes never repeat.
3. Load is bytes used (from heartbeats) plus bytes already placed in this call, so a large upload spreads across nodes instead of piling onto the emptiest three.
4. Ties are broken by the injected `Rand`: seeded in the sim, so placement replays.
5. Fewer than `min_replicas` eligible nodes fails `BeginUpload` with `unavailable`.

The chosen placement is written into the `BeginUpload` log entry, so replay does not depend on load at replay time.

## Consequences

- Rack spread outranks load. With 5 nodes over 3 racks (the compose layout: r1 ×2, r2 ×2, r3 ×1) the single r3 node holds a replica of every chunk and fills first. The e2e run shows it: node-3 held all 5 chunks of a 20 MiB file. Balanced racks avoid this.
- Placement ignores chunks that already exist. Two identical chunks in one upload are placed twice and end up on the union of both placements (4 replicas were observed in a test). Phase 4 dedup will place on existing locations first.
- `TestPlacementProperties` checks over 300 random clusters: distinct nodes, distinct racks whenever enough racks exist, never on dead or draining nodes.

## At 100× scale

Add failure-domain levels (datacenter, row, rack, host) and weight by capacity rather than bytes used. With thousands of nodes, computing placement per chunk centrally stays cheap, but rebalancing after adding racks needs a mover (HDFS balancer) or a CRUSH-style function that moves only a minimal fraction of data.
