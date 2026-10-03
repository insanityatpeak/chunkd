# 0029. Puts return at the replica quorum

Status: accepted
Date: 2026-10-03

## Context

ADR-0007 sends each chunk to all three replicas and commits at two reported copies, but the client waited for every replica before it moved on. `TestBenchLatency` showed the cost: with one gray node (2 s added to every message) put p50 went from 188 ms to 4,111 ms, because a put waited for the slowest replica. The commit never needed that copy.

## Options considered

| Option | For | Against |
|---|---|---|
| Keep waiting for all three | Every put ends at RF 3 | Latency is the slowest replica's |
| **Wait for `min_replicas`, then a short grace, without cancelling the rest** | Healthy puts still land all three copies; a slow replica does not hold the put | A copy that is slow, abandoned or lost is not retried by the client |
| Send to two, send the third only after a delay | Less client egress | A healthy put waits for the delay or for a third send; changes the healthy path |
| Feed client latency into placement | Stops sending copies to a slow node | A client's score cannot steer the metadata server's placement; needs a slow-node detector on the metadata side |

## Decision

`iface.Caller.Quorum(ctx, calls, need, grace)` sends every call at once and returns when all have settled, or when `need` have succeeded and `grace` has passed since the need-th. Calls still running are not cancelled and are reported `Pending`: in the sim the request is already on the network, and in real mode the call keeps the caller's context, so a slow node still receives its copy.

`putChunk` calls it with `need = min_replicas` and `grace = 50 ms` (`DefaultPutGrace`). 50 ms covers the spread between healthy replicas on the LAN model, so a healthy put still stores three copies. `Options.PutGrace < 0` restores the wait-for-all put; the benchmark uses it for the "hedging off" rows.

A call that fails (an error, or its own 10 s timeout inside the quorum window) is retried once, as before. A call left `Pending` is not retried: whatever it was going to deliver arrives late or never. A copy that never arrives leaves the chunk at two reported copies, which is above `min_replicas`, and repair (ADR-0011) restores the third on its next scan. Commit is unchanged: it needs `min_replicas` copies reported by nodes believed alive, so no put is acknowledged on fewer.

Erasure-coded puts (`putStripe`) are unchanged and still wait for every shard; the same change applies with `need = 4` of 6 and is left for later.

## Consequences

- With one gray node a put takes p50 214 ms and p99 294 ms, against 4,111 ms waiting for all three and 188 ms with no fault (`docs/benchmarks/results.md`). Healthy rows are identical.
- `TestPutNotHeldBySlowReplica` holds the slowest put under 750 ms with a 2 s node, checks the slow copy lands, and runs `AssertInvariants`. The `ifacetest.RPC` conformance suite covers `Quorum` on both implementations, including that an abandoned call still reaches its node.
- Six scenario goldens changed (kill-node, corrupt-chunk, gc, kill-leader, add-node, drain) and `TestScenarioGC` expects the delete to expire at epoch 3, not 4. With the grace set to an hour every golden matches the old one byte for byte, so the difference is only the earlier return and the sim's 1% message loss: a dropped third copy used to cost a 10 s timeout and a retry, and is now repaired.
- A client that dies right after a put leaves chunks at two copies until repair; the window already existed for any copy that was in flight.
- The repair benchmarks in the same run changed because their uploads now follow a different message trajectory. `TestBenchRepairTime` now runs with no message loss, as `TestBenchRepairForeground` already did; an earlier "80 MiB/s is slot-bound" reading came from one lost repair message.

## At 100x scale

Quorum puts shorten the tail but still send three copies from the client. A pipeline or a first-replica fan-out inside the cluster removes the client's 3x egress (ADR-0007), and a metadata-side slow-node detector would stop placing new copies on a node that is slow for everyone, not for one client.
