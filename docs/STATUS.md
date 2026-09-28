# Status

Current phase: **1: chunked, replicated upload and download** (complete)

## Phase 1 checklist

- [x] `core/chunk`: streaming splitter, content addressing, file hash
- [x] `core/placement`: rack spread, then least loaded, deterministic from the seed
- [x] `core/meta`: versioned state machine, CAS, pending uploads, tombstones, refcounts, location tracking
- [x] `real/metastore`: CRC-framed WAL with torn-tail recovery, bbolt snapshots, truncation
- [x] `real/blockstore` and `sim.BlockStore` with a shared conformance suite
- [x] RPC layer (`Serve` / `Caller.Do`) over sim and gRPC with a shared conformance suite
- [x] Storage node and metadata server processes
- [x] Client library, CLI (`put get ls stat rm cluster`), HTTP gateway
- [x] Sim harness (`UploadRandom`, `Download`, `AssertInvariants`); round trips in both modes
- [x] Dashboard: upload, chunk grid, crash/restart nodes, download verified with WebCrypto; same UI against the gateway
- [x] `task e2e` (20 MiB through the compose gateway) in CI
- [x] ADRs 0005–0009

## Exit criteria

| Criterion | Evidence |
|---|---|
| `TestRoundTrip` (0 B, 1 B, 4 MiB − 1, 4 MiB, 4 MiB + 1, 37 MiB) passes in sim and real | `internal/e2e` |
| `TestCommitRequiresMinReplicas` | `internal/e2e`, plus server-level version in `core/meta` |
| `TestUncommittedInvisible` | `internal/e2e`, plus state-level version in `core/meta` |
| Compose: 20 MiB put and get through the gateway, hashes match | `go run ./tools/task e2e`, CI `compose` job |
| Live demo: upload → chunk placement → download with verified hash | Checked in a headless browser, sim and against the compose gateway |

## Next

- [ ] Phase 2: failure detection and re-replication (`03-phase2-failure`)
- [ ] Acknowledged incremental block reports (removes the up-to-30 s commit delay after a lost report)
- [ ] Overlap chunk uploads (window of chunks in flight)

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.

## Known bugs

None open. See `docs/bugs-found.md`.
