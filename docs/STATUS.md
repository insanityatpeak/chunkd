# Status

Current phase: **0 — bootstrap, CI and live WASM spike**

## Phase 0 checklist

- [x] Repository, ignore rules, license, README
- [x] `internal/iface`: Transport, Clock, BlockStore, MetaStore, Rand
- [x] Sim: fake clock, seeded RNG, fault-injecting network, in-memory stores
- [x] `tools/task` runner
- [x] Protobuf (`chunkd.v1` messages, `chunkd.rpc.v1` services), slog JSON logging, metrics registry
- [x] Hello-world sim cluster (1 meta + 3 nodes) compiled to WASM
- [x] Dashboard running the sim in a Web Worker
- [x] Real mode: gRPC transport, event loop, meta/node/gateway binaries running the same heartbeat core
- [x] Docker images and compose (full 9 containers, `small` profile 5)
- [x] CI and Pages workflows
- [x] ADRs 0001-0004, CONTRIBUTING, this file
- [ ] CI green on GitHub and Pages live (verified after first push)

## Next

- [ ] Phase 1: core data path (`02-phase1-core`)

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it); the approved design said 1.24. No effect on tooling; revisit only if a consumer needs an older Go.

## Known bugs

None open. See `docs/bugs-found.md` for fixed ones.
