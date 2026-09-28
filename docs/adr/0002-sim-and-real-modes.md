# 0002. Sim and real modes from one codebase

Status: accepted
Date: 2026-09-29

## Context

The system's claims are about behaviour under failure: node loss, partitions, message loss, slow disks. Testing those against real processes is slow (seconds per timeout), flaky (the OS schedules differently each run) and cannot replay a failing run. The project also needs a demo that runs with no install, which means the browser.

## Options considered

| Option | For | Against |
|---|---|---|
| Real processes only, fault injection with iptables/toxiproxy | Tests the exact production binary | Slow, needs root or containers, runs never replay |
| Separate simulator (model of the system) | Fast | Tests the model, not the code; the two drift |
| **Core behind interfaces, two wirings (sim, real)** | Same core code in both; sim runs thousands of seconds of cluster time per wall second and replays from a seed | Core must never touch the environment directly; needs a lint to enforce it |

## Decision

Core packages (`internal/core/...`) depend only on `internal/iface`: `Transport`, `Clock`, `BlockStore`, `MetaStore`, `Rand`. `internal/sim` implements them in memory with a fake clock and a seeded RNG; `internal/real` implements them with gRPC, disk, bbolt and the wall clock. Components receive dependencies through constructors (`heartbeat.Deps`), never globals.

`go run ./tools/task lint-imports` fails if any file under `internal/core` imports `net`, `os`, `syscall`, `math/rand`, `crypto/rand`, or uses anything from `time` other than `Duration` and its unit constants.

The sim build must stay browser-sized. Protobuf services are generated into a separate package (`chunkd.rpc.v1`) from message types (`chunkd.v1`): with both in one package, core's import of the message types pulled gRPC and `net/http` into `cluster.wasm` and took it from 7.6 MiB to 20.0 MiB. `task wasm` now fails if either package appears in the WASM dependency graph.

## Consequences

- A sim run is a pure function of its seed and step sequence. `TestSameSeedSameStateSequence` checks 400 consecutive states for equality across two runs; chaos tests in later phases will print the seed on failure.
- Anything nondeterministic must enter through an interface. Map iteration is a hidden source: sim stores and trackers sort before returning.
- Real mode needs its own tests for what the sim cannot model: gRPC framing, address learning, process restarts. `grpcnet.TestHeartbeatOverGRPC` runs the same heartbeat core over real sockets.
- If a dependency cannot build for `js/wasm` (hashicorp/raft's TCP transport is the expected first case), it stays behind its interface in `internal/real`, the sim gets its own implementation, and the choice is recorded in an ADR.

## At 100× scale

The sim runs one goroutine for the whole cluster. At 100× nodes a run is CPU-bound in that loop; FoundationDB's simulator has the same property and handles it by running many seeds in parallel rather than one large run. The same approach applies here.
