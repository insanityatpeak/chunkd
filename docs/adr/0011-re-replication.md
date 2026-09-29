# 0011. Re-replication: priority queue, delay, throttle, trim

Status: accepted
Date: 2026-09-29

## Context

After a node dies, its chunks sit at 2 copies (or fewer) until something copies them. Getting this wrong fails in three directions:

- **Too eager:** a node that reboots in 40 s would trigger a full copy of everything it held, then trimming of the extra copies when it returns. With 4 TB per node that is hours of wasted disk and network, repeated on every rolling restart.
- **Too aggressive:** unbounded copies saturate the survivors' disks and NICs, so client reads slow down exactly when the cluster is weakest. Repair must not become the outage.
- **Too slow, or wrong order:** a chunk down to its last copy must go before a chunk at 2 of 3.

When a node returns after repair has already run, its chunks are over-replicated and the extras must be removed without ever leaving fewer than RF confirmed copies.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Who moves the bytes | Meta proxies; source pushes; **target pulls from source** | Target pull: the node that ends up holding the chunk verifies its SHA-256 on `Put`, and completion is its ordinary block report |
| When to start | At once; **after a delay from the death** (HDFS `dfs.namenode.reconstruction`, Ceph `mon_osd_down_out_interval`) | 20 s delay, skipped when only 1 live copy remains |
| Order | FIFO; random; **fewest live copies first, then FIFO** | Priority queue |
| Limits | Global only; **global + per source + per target + byte rate** | 8 in flight, 2 per source, 2 per target, 40 MiB/s token bucket |
| Over-replication | Leave it; delete newest; **trim a confirmed extra, keep rack spread** | Victim on the rack with most copies, then most used, then highest ID |

## Decision

`core/repair.Scheduler` runs on the metadata server's loop.

- **Detection:** event-driven on detector transitions and block reports, plus a full scan every 30 s that catches anything the events missed.
- **Delay:** a chunk whose missing copies are on dead nodes waits 20 s from the death. A node returning inside the window cancels its repairs (re-checked at dispatch). A chunk with one live copy left skips the delay.
- **Queue:** heap ordered by live copies, then arrival.
- **Dispatch:** source prefers alive over suspect, then the least busy; target from the placement rules excluding current holders and busy targets. Bytes come from an integer token bucket (40 MiB/s, 4 MiB burst). Blocked items are skipped, not dropped, so one busy node does not stall the queue.
- **Completion:** the target's block report. A copy with no report after 10 s (lost command, lost report, dead target) times out, frees its slots, and the chunk is re-assessed.
- **Trim:** only confirmed holders count: alive, with a full block report since returning. A trim needs more than RF confirmed copies, never runs while a copy of the chunk is in flight, and is triggered by block reports and trim timeouts, not only by scans. A timed-out trim is retried against the same node, which confirms absent chunks too (`docs/bugs-found.md` #6).

**RF restore bound** used by every test: `dead (10 s) + delay (20 s) + bytes ÷ 40 MiB/s + copy timeout (10 s) + 5 s`. The copy timeout is in the bound because 1% message loss can stall one copy until it times out. Real mode, with less data but wall-clock jitter, uses 90 s.

## Consequences

- `TestKillNodeRestoresRF`: 196 MiB on the dead node, RF 3 restored in 42.8–44.8 s against a 49.9 s bound (seeds 1–3, about 77 copies each). Real mode: 28 s longest under-replication, 18 copies.
- `TestTransientBlipNoRepair` and `transient-blip-no-repair`: zero copies for 2, 15 and 25 s outages.
- `TestRepairThrottle`: peaks of 4 in flight, 2 per source, 2 per target, never above the limits.
- `TestReturningNodeReconciled`, `TestWipedNodeReturnsEmpty`, `TestTrimSafety`, `TestTrimRetrySameVictim`.
- 5000 chaos seeds check no acknowledged data is lost and RF is restored within the bound.
- The repair delay is a floor on time at reduced redundancy: a second failure inside the 30 s window loses data only if it hits the last copy of a chunk, and that chunk skips the delay.

## At 100× scale

At 4 TB per node, the 40 MiB/s cluster-wide cap makes one node's repair take 29 hours; the limit becomes per node (each survivor sources and sinks a share, as HDFS spreads reconstruction over all datanodes), so repair time falls as the cluster grows. The 30 s full scan is O(chunks) on the metadata server; at 10⁸ chunks it becomes a per-node under-replication index updated incrementally. Erasure-coded cold data would replace copying with reconstruction reads from k survivors.
