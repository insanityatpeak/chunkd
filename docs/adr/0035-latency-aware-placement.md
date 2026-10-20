# 0035. Latency-aware placement

Status: accepted
Date: 2026-10-20

## Context

Placement ranks nodes by rack spread, then by load (ADR-0008). A gray node, alive to the failure detector but slow to serve, keeps getting new chunks. Quorum puts (ADR-0029, ADR-0031) take one slow copy off the put's latency, but the slow copy still costs something. A put whose placement names two slow nodes waits for one of them. A copy still in flight when the grace ends is topped up by repair. A reader that picks the slow copy pays for it (hedged reads bound that cost). ADR-0029 rejected feeding client latency into placement: one client's score cannot steer the metadata server, which needs its own slow-node detector.

## Options considered

| Option | For | Against |
|---|---|---|
| Leave it | Nothing new | Every gray node keeps taking a share of new data until an operator drains it |
| Clients report the latency they see | Measures the path that matters | One client's view; needs aggregation and trust across clients (rejected in ADR-0029) |
| The metadata server probes nodes | Measured where placement decides | A timer cannot call `Caller.Do` (ADR-0009); probes add traffic |
| **Nodes report service time in heartbeats: the round trip of the metadata peer's acks, and their own store write time. Each metadata peer compares a node with the median of the alive nodes and orders slow nodes after fast ones** | No new RPC; covers slow links and slow disks; one rule for sim, LAN and WAN | Self-reported; the ack RTT is to the metadata peer, not to the client |
| Fixed millisecond threshold | Simple | Wrong on either a 1 ms LAN or a 40 ms WAN |
| Exclude slow nodes, as suspect nodes are | Strongest effect | Placement can fail or lose rack spread when the slow node is the only one on its rack |

## Decision

- `Heartbeat` gains `ack_rtt_us` and `put_us`. The node keeps the send times of its last 8 heartbeats, so an ack up to 8 beats late still gives a round trip, and smooths each metadata peer's RTT with an EWMA (weight 1/4). It smooths the time `putChunk` spends writing to its store the same way. Each peer is sent its own RTT.
- `Cluster.Service` scores a node as RTT plus store time. A node turns slow once its score stays above both 3 times the median of the alive nodes and the median plus 100 ms for 3 heartbeats in a row. It turns fast once its score stays below both 2 times the median and the median plus 50 ms for as long. No node is slow while fewer than 3 alive nodes report a score. The margin keeps link jitter on a fast LAN from counting. A dead or restarted node is measured afresh. The flag is soft state, like the detector's view: each peer judges for itself, and the leader's view places.
- `placement.Place` and `Extend` order the nodes on the racks a chunk needs: fast before slow, then by load. Rack spread outranks latency: a slow node that is the only node on its rack still takes a replica of every chunk.
- `readLocations` orders alive before suspect, and fast before slow within each.
- The dashboard timeline shows a `node` event when a node turns slow or fast again.

SIMPLIFIED: the median is recomputed on every heartbeat. HDFS's NameNode collects DataNode peer latencies (`dfs.datanode.peer.stats.enabled`) and flags outliers with a median-absolute-deviation test, then the block placement policy can exclude them (`dfs.namenode.block-placement-policy.exclude-slow-nodes.enabled`). Ceph reports OSD heartbeat ping times and warns on slow ones but leaves CRUSH placement to the operator's weights.

## Consequences

- `TestGrayNodeIsPlacedLast`: on 7 nodes, node-1 (2 s per message) is judged slow 10 s after the fault. Then 20 puts, replicated and EC, place no copy or shard on it. It is judged fast 18 to 19 s after it recovers: the EWMA decays from about 4 s.
- `TestSlowNodePlacedLast`: a slow node takes no replica while its rack has a fast node, even when it is the emptiest node. When it is the only node on its rack it takes one on every chunk.
- `TestBenchPutRedundancy` (`put-latency.csv`) gains a `gray-detected` fault: the puts start 15 s after node-1 turns gray, with the gray node judged slow. Against `gray-node`, where it stays in placement, quorum put p50 goes from 238 to 216 ms replicated and from 238 to 189 ms EC, about a healthy cluster's 211 and 195 ms. Wait-all p99 goes from 4,273 to 324 ms replicated and from 4,255 to 275 ms EC.
- `meta.Config.IgnoreSlow` never judges a node slow. `TestBenchLatency`, `TestBenchPutWindow` and the `gray-node` rows set it, so they still measure hedged reads, quorum puts and the upload window against a gray node that stays in placement, and their numbers are unchanged. The two new heartbeat fields lengthen every heartbeat, which shifts sim timing: the `loss-1pct` rows of `latency.csv` and `resume.csv` moved by a few percent.
- While a node is slow it takes no new data, so its share of bytes lags. Balancing (ADR-0020) evens it out once the node is fast again.
- The signal is self-reported. A node cannot steer placement toward itself, only away. The ack RTT measures the link to the metadata peer, not to clients: a node slow only toward clients is not detected.
- Chaos with the change: 1,000 default, 500 `--metas=3` and 500 `--ec` seeds.

## At 100x scale

Refresh the median once per heartbeat interval instead of per heartbeat. Judge by the latency clients and peers see as well: nodes report per-peer transfer latency, as HDFS DataNodes do. Weight placement by score rather than using a two-level order.
