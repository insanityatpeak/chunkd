# 0023. EC repair: node-side shard rebuild

Status: accepted
Date: 2026-10-02

## Context

A replicated chunk that loses a copy is repaired by copying a surviving one. A lost shard has no surviving copy: it must be recomputed from 4 other shards of its stripe. Someone has to read those 4 shards, decode and write the result, and the repair scheduler (ADR-0011, ADR-0020) has to fit that work into its delay, priorities, slots and byte budget without a second scheduler.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Who decodes | Metadata leader; a client; **the target node** (HDFS ECWorker on the DataNode, Ceph's primary OSD) | The target node. The leader never handles data, and the target writes the result locally, so the rebuilt bytes cross the network once fewer |
| Unit of repair | Whole stripe; **one shard** | One shard: a lost node usually takes one shard of each stripe it held. Each shard is a block with a copy target of 1, so the existing per-block queue, slots and cancellation apply |
| When to rebuild | At once; **after the repair delay, at once if the stripe has no margin left** | Mirrors replication. A node back inside the 20 s delay costs nothing; a stripe down to 4 available shards (one more loss is data loss) rebuilds at once, like a chunk down to its last copy |
| Source reads | All 5 survivors; **4, then the next if one fails or is slow** | 4. Reading 5 wastes a shard's bandwidth on every repair. While reads are outstanding, every 2 s brings in the next source: a rebuild sends 8 messages, and one lost message would otherwise stall it for the whole call timeout |
| Throttle accounting | Charge the written shard; **charge the 4 shards read** | Charge the reads: they are the cost. Each source holds a source slot for the copy's lifetime |

## Decision

The scheduler's view gives every block a copy target: RF for a replicated chunk, 1 for a shard. A shard below target that still has a copy (a draining node, or a returning one) is copied as usual. A shard with no copy left is rebuilt:

```
leader                              target node T (holds no shard of the stripe)
  │  RebuildShard{shard 2, stripe L,      │
  │   sources: 0@n1 1@n2 3@n4 4@n5 5@n6}   │
  │──────────────────────────────────────▶│ get shards 0,1,3,4 (parallel)
  │                                       │ check each hash and header (slot, L)
  │                                       │ a failed source, or every 2 s waiting,
  │                                       │   → fetch the next one
  │                                       │ RS decode slot 2, wrap header, hash == ID?
  │◀──────── incremental block report ────│ store
```

The target is chosen by the placement rules with every node that holds any shard of the stripe excluded, so the stripe stays on 6 distinct nodes. The copy takes one target slot and a source slot on each of the 4 sources, and draws 4 × the shard size from the byte bucket. It completes on the target's block report like any copy. A failure report, or the copy timeout, frees the slots and the shard is queued again. A rebuild needs 4 shards with an alive holder. With fewer the stripe is counted as lost and nothing is sent. In the queue a rebuild ranks with a chunk of the same margin: a stripe with 4 shards left goes with a chunk down to its last copy, one with 5 with a chunk at 2.

Corruption follows the same path: a shard that fails verification is quarantined (ADR-0013), its block has no copy, and it is rebuilt at once (nothing died). Draining a node copies its shards as-is; no decode is needed while the copy is readable.

## Consequences

- Rebuilding a 1 MiB shard moves 4 MiB, where replicating a 4 MiB chunk moves 4 MiB: per byte of lost data EC repair costs 4× the network and disk reads. A dead node holding N bytes of shards triggers 4N bytes of reads, spread over the surviving shard holders. Measured in `docs/benchmarks/ec.md`: with the same 80 MiB stored both ways on 7 nodes, a dead node held 20.0 MiB of shards against 39.0 MiB of copies, and repair read 80.1 MiB against 39.0 MiB.
- The rebuild's 4 sources each hold a slot, so a burst of EC repairs blocks other copies sooner than replication repairs. The priority and Background rules of ADR-0020 still keep drain and balance behind repair.
- SIMPLIFIED: the balancer leaves shards where they are, and its byte targets count only replicated chunks. Ceph's balancer moves EC placement groups like any other. Drain still moves shards.
- A node can briefly hold two shards of one stripe: a copy under a pending GC delete does not count as a holder, so a rebuild may land beside it. Source slots are counted per node, so such a node is never charged past its limit (bugs-found #26), and the surplus goes when GC or a trim removes the doomed copy.
- SIMPLIFIED: rack spread is not re-checked against the stripe's other shards when choosing a rebuild target, only node distinctness. HDFS's EC placement policy re-checks racks.
- SIMPLIFIED: the node decodes on its event loop. RS(4,2) decode of a 1 MiB shard takes under a millisecond natively; HDFS runs ECWorker reconstruction on a thread pool.
- Tests: scheduler tables (rebuild vs copy, delay vs no margin, lost at 3 gone, sources and target exclusion), node rebuild with a bad source, sim kill-2 and kill-3, chaos with EC files and rot.

## At 100× scale

Repair traffic, not storage, becomes the limit: losing a 10 TB node of RS(10,4) shards reads 100 TB. Production systems cut this three ways. Locally repairable codes (Azure LRC, Facebook's HDFS-Xorbas) let most single failures read a small local group. Clay and regenerating codes read part of each helper shard. And spreading a dead node's rebuilds over every node in the cluster, as Ceph and HDFS do, keeps per-node repair rates low while the cluster-wide rate stays high.
