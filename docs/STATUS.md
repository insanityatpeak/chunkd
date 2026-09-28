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
- [x] CI green on GitHub and Pages live; `?seed=42` replays identically on the live site

## Exit criteria (verified 2026-09-29)

| Criterion | Evidence |
|---|---|
| `task test` and `lint-imports` pass locally and in CI | CI run 36470366187: go, web, compose jobs green |
| Live page shows banner and advancing counters; `?seed=42` replays | Two browser loads of the live site: 237 shared sim times, 0 mismatches |
| `cluster.wasm` size printed in CI, under budget | 7.63 MiB (target 15, limit 20); 2.0 MB gzipped on Pages |
| `docker compose up` = 9 containers; `--profile small` = 5; gateway `/healthz` 200 | Local and CI `compose` job |
| `trace-check` passes; single author | 12 conventional commits by insanityatpeak |

## Next

- [ ] Phase 1: core data path (`02-phase1-core`)
- [ ] Request/response helper over `Transport` (ReqID + Clock timeout) when the first RPC-style exchange needs it
- [ ] `HttpClusterAPI`: gateway state endpoint for the dashboard's real-mode view

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it); the approved design said 1.24. No effect on tooling; revisit only if a consumer needs an older Go.

## Known bugs

None open. See `docs/bugs-found.md` for fixed ones.
