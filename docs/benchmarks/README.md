# Benchmarks

```
go run ./tools/task bench                 # every stage, about 6 minutes
go run ./tools/task bench -stages=report  # redo charts and results.md from data/
go run ./tools/task bench -stages=real -dir D:\scratch   # disks for the real cluster
```

Output: [results.md](results.md) (tables and charts, generated), `data/*.csv` (raw), `charts/*.svg` (drawn by `internal/bench/svg.go`). The older write-ups stay as they are: [dedup.md](dedup.md), [ec.md](ec.md), [rebalance.md](rebalance.md).

## Two tiers

| Tier | Runs | Time | Reproducible |
|---|---|---|---|
| **sim** | `TestBench*` in `internal/sim/cluster` | simulated | Exactly. Same seed, same CSV, on any machine. |
| **meta** | `BenchmarkBegin` in `internal/core/meta` | wall clock, one goroutine | Within machine noise |
| **real** | `cmd/chunkd-bench`: 1 metadata server and 5 nodes in one process, gRPC over loopback, files on disk | wall clock | Within noise: expect 10% on large files, more on small ones |

The sim tier answers questions about policy: how long repair takes, what hedging does, what a resumed upload sends. Its time is a model: 1 Gbit/s links (125 MiB/s), 1 to 40 ms message delay, 1% loss unless a table says otherwise. The real tier answers questions about this implementation on this machine.

## What the numbers are not

- **Real throughput is one client, sequential, on loopback.** There is no network between the client and the nodes, and five nodes share one CPU and one disk. It shows what the code costs, not what a cluster delivers. Put (about 59 MiB/s) writes every byte three times and hashes it on both ends, so it is far below get (about 160 to 190 MiB/s).
- **The OS page cache serves most reads.** Get numbers do not include a cold disk.
- **The sim shares no capacity between messages.** Every message gets its own transfer time, so repair traffic cannot slow a read. The foreground table in results.md therefore shows the rate cap holding and its cost in repair time, and nothing about reader impact. Measuring that needs a real network and real disks: `docker compose` on separate hosts, which this run did not do.
- **Small samples at the tail.** p99 of 90 puts is one sample. Tables give the sample count; do not read a p99 under about 300 samples as a rate.
- **Single-shot sim runs of repair time** use seed 11. They show the shape, not a confidence interval.

Run timing stages with nothing else building or testing. The numbers committed here were taken on the machine named in [results.md](results.md) with only Docker Desktop idle in the background.

For a second set of numbers on a different machine, `.github/workflows/bench.yml` (manual dispatch) runs the same command on the standard GitHub runner and uploads `docs/benchmarks` as an artifact. Hosted runners are shared and their numbers vary from run to run; compare shapes and ratios, not absolute values.

## What they show

| Question | Answer | Where |
|---|---|---|
| Throughput | Real, RF 3: put 49 to 62 MiB/s from 16 MiB up, get 157 to 195 MiB/s. A 1 MiB put is dominated by fixed cost (9 to 26 MiB/s). | results.md, throughput |
| Latency | Real, 256 KiB files: put p50 42 ms, get p50 2.6 ms, stat p50 0.5 ms. | results.md, latency |
| Hedged reads | With one gray node (2 s added), a fresh client's get p99 is 4141 ms unhedged and 235 ms hedged. With no fault they are identical, so hedging costs nothing when nothing is slow. | results.md, gray node |
| Writes are not hedged | The same gray node moves put p50 from 188 ms to 4112 ms: a put waits for the slowest of its three replicas (ADR-0007). | results.md, gray node |
| Loss | At 1% loss one call in a hundred waits out the 10 s call timeout, so p99 is the timeout. The tail is set by the timeout, not by the system's speed. | results.md, gray node |
| Repair time | A 30 s floor (10 s detector, 20 s delay), then copies. At a 5 MiB/s cap, 240 MiB lost takes 47.5 s more; at 20 MiB/s, 12 s; at 80 MiB/s also 12.8 s, because the copy slots (8 in flight) bound it before the cap does. | results.md, repair |
| Cap | Peak repair rate is 15.4 MiB/s under a 10 MiB/s cap and 43.5 MiB/s under 40 (windows are whole chunks and the bucket has a 4 MiB burst). Unlimited with 64 slots peaks at 592 MiB/s. | results.md, repair cap |
| Resume | The bytes that reach the nodes are the same with or without resume: a restarted Put claims every chunk and a stored chunk is not sent again (ADR-0024). What resume saves is the request body, and so the time to read, hash and send the file: 32 MiB without it, 4 MiB after a cut at 90%. | results.md, resume |
| Quota | Measured before ADR-0028: a Begin cost 0.7 us without a quota and 4, 41, 825 and 29,400 us with one over 100, 1,000, 10,000 and 100,000 files, because the state machine scanned every file in every namespace inside the Raft apply loop. With the per-namespace counter a limited Begin costs 0.9 to 1.8 us at every size. | results.md, quota |
| EC against replication | 1.50x against 3.00x stored; repair reads 4x what it rebuilds. | [ec.md](ec.md) |

The quota scan was fixed by ADR-0028: a byte counter per namespace in the state machine, rebuilt on restore and checked against a recount by `Reconcile`; S3-compatible stores keep a usage counter per bucket for the same reason. The first row shows the cost it removed.
