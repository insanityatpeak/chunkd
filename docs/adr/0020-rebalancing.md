# 0020. Rebalancing: a central planner, rack-feasible targets, logged trims

Status: accepted
Date: 2026-10-02

## Context

Placement (ADR-0008) spreads new chunks, but nothing moves existing ones. A node added to a full cluster takes only new writes, and a node being retired keeps its data. Moving a replica is copy-then-delete: the copy is a repair copy (ADR-0011), the delete is a trim. Trims today are sent straight from the leader's soft state. With three metadata peers (ADR-0019), a deposed leader in term T, not yet fenced at the node, and the new leader in T+1 can each trim a *different* copy of the same over-replicated chunk and leave it below RF. Rebalancing makes trims routine, so the delete half must be safe across a leader change.

Two more facts shape the targets. Nodes report bytes used, not capacity. And with RF 3 over three racks, every rack holds exactly one copy of each chunk: in the compose and dashboard layout (r1 ×2, r2 ×2, r3 ×1) node-3 holds every chunk, 1.67× the mean. A band around the mean cannot be met without breaking rack spread.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Who decides where replicas live | Consistent hashing (Dynamo, Cassandra: vnodes on a ring); CRUSH (Ceph); **a central planner over explicit locations** (GFS, HDFS Balancer) | Planner. Locations are already explicit (logged at `BeginUpload`, reported by nodes); a hash ring would move nearly every chunk once to adopt it. A ring moves about 1/(N+1) of replicas on a join, which is what the planner moves too, but with no hook for throttling, priority, drain or rack-aware choice of what moves |
| Load measure | Heartbeat `used_bytes`; **bytes in the leader's location map** | The location map. It is what the planner moves, it changes the moment a report lands, and it is the same view repair uses |
| Target per node | Mean of all nodes; **rack-feasible target (water-filling with a per-rack cap)** | Rack-feasible. A rack can hold at most `ceil(RF / racks)` copies of a chunk, so its share is capped at `D × ceil(RF / racks)` (D = distinct bytes); capped racks take their cap, the rest is shared by node count. A node's target is its rack's share over the rack's node count |
| Band | ±10% of target; **max(10% of target, 2 chunks)** | The floor keeps a small cluster from chasing one-chunk differences |
| Delete half of a move | Send at once, term-fenced; **a logged trim intent, sent after commit** | Logged, cloned from the GC intents of ADR-0019. A term check alone leaves the window above |
| Which trims | Only move deletes; **every trim, including repair's surplus trims** | Every trim. The race does not care why a trim was chosen |
| Sharing the repair throttle | A second scheduler with its own budget; **one queue, priority classes, a slot cap for the lower classes** | One queue. Copies cannot be preempted, so priority alone does not keep repair fast: the cap does |

## Decision

**Planner** (`core/rebalance`, pure: state in, moves out). Inputs: per node its rack, admin state and located bytes; per chunk its size and holders. A move of chunk c from src to dst is allowed only if src is above its target, `dst + size <= target(dst) + band`, dst holds no copy of c, and the move does not reduce the number of distinct racks holding c. The planner takes the most-over source, then the most-under destination, ties by ID. Every allowed move lowers L1 = Σ|used − target|, so planning terminates and never moves a copy back and forth.

**Bound.** Each moved byte lowers L1 by at most 2, so any algorithm needs at least ½·L1 bytes to reach the targets. The planner moves at most ½·L1 plus one chunk per node (granularity). Adding node-6 to r3 in the dashboard layout (40 MiB distinct, 120 MiB stored):

```
before                     after                       moved
r1: node-1 20, node-4 20   r1: 20, 20                  0
r2: node-2 20, node-5 20   r2: 20, 20                  0
r3: node-3 40              r3: node-3 20, node-6 20    20 MiB, node-3 -> node-6
L1 = 20 + 20 = 40, so ½·L1 = 20 MiB = total / 6, the theoretical minimum
```

**Execution.** Planned moves enter the repair scheduler's queue as copies. The queue orders by `(class, live copies, arrival)`: class 0 is repair (a chunk below RF), class 1 drain evacuation (ADR-0021), class 2 balance. While a class-0 item is waiting for a slot, nothing of a lower class is dispatched, and classes 1 and 2 together hold at most 4 of the 8 copy slots, so a failure during a rebalance finds slots free at once. All classes share the byte bucket (40 MiB/s) and the per-node limits (2 out, 2 in).

**Trims are logged.** A trim is chosen by a victim rule that any leader can recompute: among confirmed holders that are not leaving (ADR-0021) and whose removal keeps the chunk's distinct-rack count, the one furthest above its target, then the highest ID. For a balance move that is the source. The leader proposes `TrimIntent{chunk, node}`; applying it records a pending trim unless one is already pending for that chunk (at most one per chunk, decided in log order). Only after the intent commits does the leader send `DeleteReplica` with its term. The node's answer comes back in a block report, and the leader batches answers into `TrimDone`. A copy with a trim pending does not count as a holder. A new leader resends every pending trim. No move record is logged: a move whose copy landed and whose trim was never proposed leaves the chunk at RF+1, and whichever leader sees that trims it by the same rule.

**A deposed leader** can still send copies until a node hears the new term (at worst one surplus copy, trimmed later) and can resend trims that already committed. It cannot create a delete: its intent never commits.

## Consequences

- Every trim costs one log entry and a commit round before it is sent. Trims are batched per scan.
- The shareable dashboard runs change: trims happen one commit later. The four existing goldens are regenerated once, in the commit that logs trims.
- Tests: planner property tests (L1 strictly drops, the bound holds, no distinct rack is lost, deterministic); a two-leader double-trim test; `TestAddNodeConverges` (5 → 6, within band, bytes moved ≤ bound); a sim invariant checked at every trim (the chunk keeps RF intact copies on up nodes, unless a holder failed within the detection window); chaos kinds that add and drain nodes and kill the leader mid-rebalance.
- SIMPLIFIED: nodes are assumed equal in capacity, and the band is in bytes. HDFS balances percent of each DataNode's capacity; Ceph weights by CRUSH weight.
- SIMPLIFIED: one failure-domain level. HDFS and CRUSH balance across a tree of domains.
- SIMPLIFIED: the planner considers every chunk each scan. The HDFS Balancer works in iterations over sampled block lists.

## At 100× scale

With thousands of nodes and hundreds of millions of chunks, planning over every chunk on one server each scan becomes the cost (one pass over 100 M chunk holders is seconds). HDFS's Balancer runs outside the NameNode, picks source and destination pairs, and samples blocks from each source; the same split works here, with the meta leader only logging trims. Movement per join stays ½·L1 regardless of size. At that scale a CRUSH-style function becomes attractive again, because it removes the central location map from the read path; the price is losing the explicit control this design relies on.
