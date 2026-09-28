# 0009. Request/response on top of the event-driven transport

Status: accepted
Date: 2026-09-29

## Context

ADR-0004 made the transport one-way messages plus a single event loop per process, so sim runs replay from a seed. Phase 1 needs request/response: clients call the metadata server, and chunk data moves between clients and nodes. Clients (CLI, gateway, tests, the WASM demo) are naturally written as blocking code, and an upload sends each chunk to three nodes at once.

## Options considered

| Option | Sim behaviour | Against |
|---|---|---|
| Blocking `Call` per request, goroutines for parallelism | Each goroutine advances the shared clock; order depends on the Go scheduler | Breaks replay |
| Futures on the event loop | Deterministic | Client code becomes callbacks everywhere |
| **Batch `Caller.Do(calls)` + `Serve(kind, handler, respond)`** | Do sends every call, then advances the clock until each is answered or times out, all on one goroutine | Parallelism is per batch, not arbitrary |

## Decision

```go
Transport.Serve(id, kind, h RPCHandler, ServeOpts{Concurrent})   // h(m, respond) may respond later
Caller.Do(ctx, []Call) []Result                                  // concurrent within a batch, results in order
```

- Sim: `sim.Caller` sends each call as a message with a request ID and steps the clock until every reply arrives or the per-call timeout passes. Replies cross the same faulty network as requests.
- Real: one goroutine per call. Control calls use unary gRPC (`Call`, up to 16 MiB). Kinds starting with `chunk.` use `CallStream` with 1 MiB frames in both directions, so no single gRPC message exceeds the 4 MiB default.
- Errors cross the wire as an `iface.Code` (not_found, conflict, invalid, unavailable, retry, internal); transport failures and timeouts are `unavailable`.
- Metadata handlers run on the metadata server's loop (they touch the state machine and the WAL). Chunk handlers are `Concurrent`: they touch only the thread-safe `BlockStore`, so disk I/O never stalls heartbeats.
- `Caller.Do` must never be called from inside a handler or timer; in the sim it would re-enter the clock.

## Consequences

- One client library runs in tests, the CLI, the gateway and the browser, deterministically in the sim.
- `ifacetest.RPC` runs the same suite against both implementations: round trip, error codes, unknown kinds, 4 MiB + 1 bodies in both directions, batch ordering, deferred responses, timeouts, unreachable nodes.
- Every response can be lost or duplicated, which surfaced the ambiguous-commit bug (`docs/bugs-found.md` #2) and forced idempotent commit and delete.

## At 100× scale

A unary call per control message and a fresh stream per chunk is fine at this scale. At high request rates: keep long-lived streams per peer, batch small messages, and add flow control on chunk streams (gRPC's per-stream window already bounds memory per transfer).
