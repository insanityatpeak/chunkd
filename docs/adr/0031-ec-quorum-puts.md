# 0031. Erasure-coded puts return at the commit quorum

Status: accepted
Date: 2026-10-20

## Context

ADR-0029 made a replicated put return once `min_replicas` copies are stored, plus a 50 ms grace. Erasure-coded puts (`putStripe`, ADR-0022) were left out: they send all six shards and wait for every one. With one gray node (2 s added to every message) on 7 nodes, every stripe that places a shard on that node waits it out. A stripe spans 6 of the 7 nodes, so most do (`docs/benchmarks/results.md`, "Put latency by redundancy").

ADR-0029 and the README described the follow-up as "4 of 6". That does not commit: commit needs `ec.CommitShards` = 5 shards reported by alive nodes (ADR-0022), and `Begin` already returns 5 as `min_replicas` for an EC upload.

## Options considered

| Option | For | Against |
|---|---|---|
| Keep waiting for all six | Every put ends with six shards | Latency is the slowest shard's; one gray node holds most EC puts |
| Return at 4 of 6, lower commit to 4 | Fastest return | Commits a stripe with no spare: one more loss makes it unreadable, as a replicated chunk committed at one copy would be |
| **Return at 5 of 6 (`CommitShards`) plus the 50 ms grace, without cancelling the sixth** | Same call and rules as ADR-0029; a committed stripe tolerates one more loss, as 2 of 3 copies does | A sixth shard that never lands is rebuilt by repair from 4 others: 4 bytes read per byte, where a replica copy reads 1 |
| Hedge the slow shard to a spare node | Ends at six shards without repair | Needs a seventh node, adds client egress, and puts a shard outside its claimed placement (ADR-0022) |
| Placement that avoids slow nodes | Fixes the cause for both policies | Needs a slow-node detector on the metadata side; complements this, does not replace it |

## Decision

`putStripe` sends each round through `putRound` with `need = min - stored`, as `putChunk` does. `min` is the `min_replicas` from `Begin`, 5 for a stripe. A shard still `Pending` when the quorum returns is in flight, not failed, and is not retried. A failed shard is retried once, as before. `Options.PutGrace < 0` restores the wait-for-all put.

Nothing changes on the metadata server. Commit still needs 5 shards reported by alive nodes. The claim covers all six shard IDs, so a sixth shard that lands after commit is not an orphan. Repair already rebuilds a shard no node holds when at least 4 others survive (ADR-0023).

## Consequences

- With one gray node on 7 nodes, EC put p50 is 238 ms and p99 313 ms, against 4,157 ms and 4,255 ms waiting for all six; healthy rows are identical (`TestBenchPutRedundancy`, `docs/benchmarks/results.md`).
- `TestECPutNotHeldBySlowShard` holds the slowest EC put under 750 ms with a 2 s node, checks the slow shards land with no rebuild, and runs `AssertInvariants`. `TestECPutShardNeverLands` silences a node: puts still return, and repair rebuilds its shards.
- Repair already waits `UploadGrace` before rebuilding a just-committed stripe's sixth shard, so a slow shard that lands is not rebuilt.
- Healthy puts still store six shards: the 50 ms grace covers the spread between healthy nodes on the LAN model.
- A sixth shard that never lands leaves the stripe at 5 until repair rebuilds it on its next scan, at 4× the read cost of a replica copy. This happens only when the shard is lost, not when it is slow.
- A client that dies right after a put leaves stripes at 5 shards until repair; the window already existed for any shard in flight.

## At 100x scale

The client still sends 1.5× the data itself, and every stripe still touches six nodes, so one slow node affects most EC writes in a small cluster. Streaming cells to the shard writers (HDFS-EC) and a metadata-side slow-node detector that stops placing shards on a node slow for every client remove both costs.
