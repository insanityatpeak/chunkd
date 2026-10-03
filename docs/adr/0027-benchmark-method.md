# 0027. Benchmark method: a sim tier and a real tier

Status: accepted
Date: 2026-10-03

## Context

Benchmarks need numbers a reader can reproduce. The system has two run modes (ADR-0002) that answer different questions. The sim runs the shipped core code in simulated time, so it is exact, but it has no shared capacity: every message gets its own transfer time and nothing contends for a link, disk or CPU. The real mode has real contention, but one machine and one client give noisy numbers that say little about a cluster.

## Options considered

| Option | For | Against |
|---|---|---|
| Real only, on compose | Real disks and processes | Noisy; Docker overhead; one host's NIC and disk; slow to run; cannot be exact |
| Sim only | Exact, fast, replayable | Cannot show throughput, disk or contention effects; says nothing about this implementation's speed |
| **Two tiers, each used for what it can answer** | Policy questions get exact answers; implementation questions get honest, labelled ones | Two code paths; the reader must know which table is which |

## Decision

`go run ./tools/task bench` runs three stages and a report:

- **sim**: `TestBench*` in `internal/sim/cluster`. They write CSVs only when `CHUNKD_BENCH_OUT` is set, so `go test` skips them. Repair time by volume and rate cap, latency with a gray node and with loss (hedging on and off, a new client per operation), the peak repair rate under each cap, and the cost of resuming against restarting. Same seed, same CSV, on any machine.
- **meta**: `BenchmarkBegin` in `internal/core/meta`, the state machine's cost of a Begin with and without a quota.
- **real**: `cmd/chunkd-bench` starts 1 metadata server and 5 nodes in one process (`internal/real/local`), over loopback gRPC and files on disk. Throughput by file size (1 MiB to 1 GiB) and 1,000-sample latency. Wall clock; the hardware line is written next to the results.
- **report**: CSVs to SVG charts (`internal/bench/svg.go`) and a generated `results.md`.

A question goes to the tier that can answer it. The effect of repair on foreground reads is a contention question; the sim is flat by construction, so the table says so and the effect is listed as not measured. A GitHub Actions `workflow_dispatch` job runs the same command on the standard runner for a second machine's numbers; hosted runners are shared, so those are read for shape.

## Consequences

- The committed sim tables re-run to identical CSVs. The real tables re-run within noise: about 10% on large files, more on small ones and at the tail.
- The benchmarks found two things the tests had not: writes are not hedged (a gray node moves put p50 from 188 ms to 4.1 s), and a limited Begin scans every file in every namespace (29 ms at 100,000 files, inside the Raft apply loop). The second is in Known limitations.
- Hedging only shows with a new client per operation: a long-lived client learns the slow node from its first reads and avoids it.
- SIMPLIFIED: one client, sequential operations. S3 and MinIO benchmarks (warp, COSBench) drive many clients and report aggregate throughput.
- SIMPLIFIED: real numbers come from one process on loopback. A multi-host run needs machines this repo does not assume.

## At 100× scale

Aggregate throughput needs many clients and several hosts: warp-style workloads against a compose or Kubernetes cluster, with the sim tier kept for policy.
