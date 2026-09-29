# Status

Current phase: **3: integrity, scrubbing and corruption repair** (complete)

## Phase 3 checklist

- [x] Nodes verify every chunk before serving it; failures are quarantined (`quarantine/`) and reported through the ordered block-report path
- [x] Metadata server drops a corrupt copy's location and re-replicates at once (no repair delay); clients send hints and the node's re-check decides
- [x] `core/scrub`: paced background scrubber (8 MiB/s, a pass every 10 min), with metrics
- [x] Bit rot as a chaos fault: sim (`RotNode`, safety rule) and real mode (`chunkd debug corrupt` via `docker compose exec`)
- [x] Invariants: no read ever returns bytes never written to its path; no rotten chunk survives two scrub passes
- [x] Dashboard: corrupt copies per node, scrub progress, corrupt events in the timeline, scripted "rot 3 chunks on node-2"
- [x] ADR 0013; bug 8 in `docs/bugs-found.md`; README Known limitations

## Exit criteria

| Criterion | Evidence |
|---|---|
| `TestCorruptChunkDetectedOnRead` (sim + real): read succeeds from another copy, the bad copy is quarantined, RF restored within the bound | Sim, seeds 1–3: RF 3 after 0.25–10.25 s against 15.1 s; real: `corrupt-replicas` (4 rotted copies found, quarantined and replaced) |
| `TestScrubberFindsCorruption`: never-read corruption found and repaired within one pass interval | Found at 10 min with a 10 min interval; RF restored in the same tick; real mode with 30 s passes |
| `TestTwoReplicasCorrupt`: RF restored from the single good copy | Sim |
| `TestAllReplicasCorrupt`: the read fails loudly with a clear error | `corrupt: … every replica … failed verification; the data is lost`; counted as lost; zero copies |
| Chaos with random corruption, 500 seeds green | CI `go` job; 500 seeds rot about 540 copies, 1000 seeds about 1090 |

## Phase 2 (complete)

Failure detection and re-replication; see ADRs 0010–0012. `TestKillNodeRestoresRF`, `TestTransientBlipNoRepair`, `TestRepairThrottle`, `TestSlowNodeHedgedRead`, and the real-mode kill, blip and freeze scenarios stay in CI.

## Phase 1 (complete)

Chunked, replicated upload and download; see ADRs 0005–0009. `TestRoundTrip`, `TestCommitRequiresMinReplicas`, `TestUncommittedInvisible`, and the 20 MiB compose round trip stay in CI.

## Next

- [ ] Ship v1 (`05-ship-v1`)
- [ ] Acknowledged incremental block reports (removes the up-to-30 s commit delay after a lost report)
- [ ] Overlap chunk uploads (window of chunks in flight)
- [ ] Per-step safety check in the chaos harness (real copies never below RF − 1, meta never counts a copy the store lacks); bugs #6 and #7 slipped past the end-state checker

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.
- Detector and repair timings (dead 10 s, delay 20 s) are demo-scale so a failure plays out in under a minute. HDFS waits 10.5 min and Ceph 10 min before re-replicating; the knobs are `detector.Config` and `repair.Config`.

## Known bugs

None open. See `docs/bugs-found.md`.
