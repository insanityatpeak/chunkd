# 0004. Event-driven transport and deterministic executor

Status: accepted
Date: 2026-09-29

## Context

ADR-0002 requires that a failing sim run replays identically from its seed. The shape of the `Transport` interface decides whether that is possible.

## Options considered

| Option | Sim execution | Replays from seed | Cost |
|---|---|---|---|
| A. Blocking `Call(ctx, to, req) (resp, error)`, goroutines in sim | Go scheduler interleaves goroutines | No: goroutine order differs between runs | Simplest core code |
| **B. Async messages + event loop** | One goroutine drains a queue ordered by (sim time, sequence) | Yes | Core is written as handlers; request/response correlates by `ReqID` and times out via `Clock` |
| C. Blocking calls + a controlled goroutine scheduler | Custom scheduler parks and resumes goroutines | Mostly; fragile around runtime internals | Building a scheduler |

## Decision

Option B.

```
Transport.Send(to, Message)      fire-and-forget; may drop, delay, duplicate, reorder
Transport.Listen(id, Handler)    handler runs on the owner's event loop
Clock.AfterFunc(d, f)            f runs on the same loop
```

- Sim: `sim.Clock` is the event queue. `sim.Net` schedules each delivery as a clock event with a delay drawn from the seeded RNG. `Advance(d)` fires events in (deadline, insertion order). One goroutine runs the whole cluster.
- Real: each process has one `runtime.Loop` goroutine. gRPC `Deliver` handlers and wall-clock timers post into it; the RPC returns as soon as the message is queued.

This is the model used by FoundationDB's simulator and TigerBeetle's VOPR.

## Consequences

- Core components never take locks: their state is only touched on their loop. HTTP handlers read it with `Loop.Do`.
- Core code must treat every send as possibly lost. That is true of real networks anyway; the interface stops us pretending otherwise.
- Blocking request/response code is not available in core. A small helper that tracks outstanding `ReqID`s with a timeout will be added when the first RPC-style exchange needs it.
- The loop queue is bounded (4096). A full queue blocks `Post`, which applies backpressure to gRPC handlers instead of growing memory without limit.

## At 100× scale

A single loop per metadata server becomes the throughput ceiling. The standard answer is to partition the state machine (by path prefix or inode range) with one loop per partition, which keeps the determinism argument per partition.
