# Design

This document grows with each phase. It describes what exists, not what is planned.

## Components

```
            clients (HTTP)
                 │
            ┌────▼────┐
            │ gateway │  /healthz = gRPC Ping to meta-1
            └────┬────┘
                 │ gRPC
   ┌─────────────▼──────────────┐
   │ meta-1   (meta-2, meta-3)  │  heartbeat tracker; Raft later
   └─────────────▲──────────────┘
                 │ heartbeat every 1 s (TransportService.Deliver)
   ┌──────┬──────┼──────┬──────┐
 node-1 node-2 node-3 node-4 node-5
```

## Run modes

| | sim | real |
|---|---|---|
| Processes | one | one per component |
| Transport | `sim.Net`: in-memory, seeded drop/dup/delay, partitions | `grpcnet`: one unary `Deliver` RPC per message |
| Clock | `sim.Clock`: manual, event queue ordered by (deadline, seq) | wall clock; timers post to `runtime.Loop` |
| Randomness | PCG seeded from the run seed | PCG seeded from the OS |
| Where | tests, `cmd/chunkd-wasm` in the browser | `docker compose up` |

Both modes run the same `internal/core/heartbeat` code.

## Heartbeats (phase 0)

- Each node sends `PingRequest{from, seq}` every 1 s, first send at a random offset in [0, 1 s) so nodes do not synchronise.
- The tracker records pings, the highest `seq` seen (pings can arrive out of order) and the local time of the last ping, then replies `PingResponse`.
- A node is alive if heard from within `dead-after` (3 s). This is a fixed timeout; see the `SIMPLIFIED:` note in `heartbeat.go`.
