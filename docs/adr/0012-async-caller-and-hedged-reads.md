# 0012. AsyncCaller for in-loop RPC, and Caller.Hedge

Status: accepted
Date: 2026-09-29

## Context

ADR-0009 gave two RPC shapes: `Serve` for handlers and a blocking `Caller.Do` for code outside the event loops. Phase 2 needs two more:

1. **A node pulling a chunk from a peer during repair** (ADR-0011). The node's logic runs on its event loop, where `Caller.Do` is forbidden: in the sim it would advance the clock from inside a timer. Chunk pulls must not block the loop in real mode either, or heartbeats stop and the detector marks a busy node suspect.
2. **Hedged reads.** A replica that heartbeats but serves slowly (a failing disk, a saturated NIC) is alive to the detector. Reading it costs the client its full latency unless a second request goes to another replica after a delay. `Do` sends a whole batch at once and waits for all of it, which is the opposite of what a hedge needs.

Both must stay deterministic in the sim.

## Options considered

| Need | Option | Against |
|---|---|---|
| In-loop RPC | Spawn a goroutine calling `Do` | Non-deterministic in the sim; callback races with the loop in real mode |
| | Send a message and correlate the reply by hand in each caller | Every caller reimplements timeouts and request IDs |
| | **`AsyncCaller.Go(call, cb)`, cb on the owner's loop** | One more seam implementation per mode |
| Hedging | Client-side goroutines and cancellation over `Do` | Advances the sim clock from several goroutines: breaks replay |
| | **`Caller.Hedge(calls, after, accept)`** | A second method on the seam every implementation must pass the conformance suite for |

## Decision

```go
type Caller interface {
    Do(ctx, []Call) []Result
    // calls[0] at once, calls[i] after another `after` with no accepted answer
    // (at once if every call so far failed); returns at the first accepted result.
    Hedge(ctx, calls []Call, after time.Duration, accept func(i int, r Result) bool) HedgeResult
}

type AsyncCaller interface {
    Go(c Call, cb func(Result)) // cb runs exactly once, on the owner's loop
}
```

- Sim: `Hedge` steps the fake clock and launches the next call when `after` passes; `AsyncCaller` sends a request message and schedules the callback on the owner's loop when the reply or the timeout arrives.
- Real: `Hedge` runs calls in goroutines and returns at the first accepted result; `Result.Pending` marks losers still in flight, and their latency is a lower bound. `AsyncCaller` runs the call in a goroutine and posts the result back to the loop.
- **Client health (`client/health.go`):** per-node EWMA of read latency (α = 0.2), failures penalised. Replicas are ordered alive first, then suspect, each by score. Hedge delay = p95 of recent successful reads, clamped to 20–500 ms, with a fixed 100 ms delay until 16 samples exist. Errors add to a decaying per-node error count (×0.8 per read) that raises the score. The score is local to one client and advisory: it never feeds placement or repair.
- `accept` verifies the chunk's SHA-256 before a result wins, so a fast corrupt replica cannot beat a slow correct one.

## Consequences

- `TestSlowNodeHedgedRead`: with one replica slowed to 4–10 s, hedged p99 is 176–194 ms against 4–10 s unhedged (bound 750 ms). `TestGrayNodeStaysAlive`: the slow node stays alive to the detector, as it should.
- Repair pulls run in the node's loop with no blocking and replay from the seed.
- A hedge costs at most one extra read per chunk and only past the p95, so about 5% extra read load in steady state.
- `ifacetest.RPC` covers `Hedge` ordering, the fail-fast launch, and `AsyncCaller` callbacks on both implementations.

## At 100× scale

Per-client scores see only that client's reads; a fleet would share them (a gossip or a metadata-side outlier detector, as HDFS does with slow-node reports) and feed them into placement, so slow nodes stop receiving new replicas. Hedging would gain a per-client budget (hedge at most N% of requests) to avoid amplifying load during a cluster-wide slowdown.
