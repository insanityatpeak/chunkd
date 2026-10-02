# Erasure coding: storage and repair against replication

Source: `TestECBenchmark` (`internal/sim/cluster/ec_bench_test.go`), sim, 4 MiB chunks, 7 nodes on 3 racks, seed 5. Runs in CI. Codec figures: `go test -bench . ./internal/core/ec`.

## Setup

The same 20 files, 1 B to 8 MiB (80.1 MiB in all), are uploaded once replicated (RF 3) and once erasure-coded (RS(4,2), ADR-0022), each to its own cluster. With every chunk whole, node-3 is killed. The run ends when every chunk has 3 copies again, or every stripe 6 shards on 6 distinct nodes.

## Results

| | `replicate:3` | `ec:4+2` |
|---|---|---|
| Stored | 240.3 MiB (3.00×) | 120.2 MiB (1.50×) |
| On node-3 when killed | 39.0 MiB | 20.0 MiB |
| Repair read | 39.0 MiB | 80.1 MiB |
| Repair wrote | 39.0 MiB in 14 copies | 20.0 MiB in 30 rebuilds |
| Read per byte lost | 1× | 4× |
| Whole again after the kill | 40.5 s | 35.3 s |

The test fails if replication stores other than 3.00× or reads other than what it lost, or if EC stores more than 1.51× or reads other than 4× what it writes.

## Reading the numbers

- **Storage.** EC stores half as much: 1.5× plus a 33-byte header per shard, which rounds away at these sizes. A 1-byte file still takes 6 shards of 34 bytes.
- **Repair cost.** A dead node held half as many bytes under EC: the cluster stores half as much, over the same 7 nodes. Each lost byte costs 4 bytes read, so EC read 80.1 MiB to restore 20.0 MiB, about 2× what replication read to restore twice as much. The scheduler's byte bucket charges the reads (ADR-0023), so EC repair is throttled by what it reads.
- **Time.** Both runs are dominated by fixed waits: 10 s for the detector to call node-3 dead, and the 20 s repair delay before anything moves. The traffic after that, 39 MiB or 80 MiB at the 40 MiB/s limit, takes 1 to 2 s. The 5 s difference between the runs is in how the copies happened to queue, not in the policy. With more data per node the bucket decides, and EC repair of a lost node takes about 2× as long as replication's.
- **Rebuild latency.** Before the hedge in `node/rebuild.go`, 4 of 31 rebuilds in this run outlived the 10 s copy timeout. A rebuild sends 8 messages, so a single lost one (the sim drops 1%) stalled it for the call timeout. The node now also asks the next source every 2 s until the rebuild ends. All 30 rebuilds completed with no timeouts.

## Codec

AMD Ryzen 7 7840HS, Go 1.27, `klauspost/reedsolomon` v1.14.2 on its amd64 SIMD path, one goroutine:

| | Time per 4 MiB chunk | Throughput |
|---|---|---|
| Encode (split into 6 shards) | 0.66 ms | 6.3 GB/s |
| Decode with 2 data shards lost | 0.95 ms | 4.4 GB/s |

A degraded read adds under a millisecond of CPU per chunk, small next to fetching 4 MiB at 1 Gbit/s (about 34 ms). In the browser build the codec runs its pure-Go path, without SIMD.

Times are simulated: 1 Gbit/s links (125 MiB/s), 1 to 40 ms message delay, 1% loss.
