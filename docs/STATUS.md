# Status

Current phase: **Phase 4 complete** (dedup, versioning, delete, GC), on top of v0.1.0. Next: Phase 5, Raft.

## Phase 4 (complete)

Dedup through logged chunk claims, compare-and-swap versioned commits with opt-in last-writer-wins, soft delete with retention in logical GC epochs and undelete, upload leases, mark-and-sweep GC with a grace period and fenced node-side deletes, and per-epoch refcount reconciliation. ADRs 0014–0016. Dashboard: dedup savings, GC counters, per-file versions with restore, deleted files with undelete, gc and write events, and a shareable `gc` scenario.

| Criterion | Evidence |
|---|---|
| `TestDedupSavings`: known overlap stored at the expected size, ratio recorded | 220 MiB logical, 60 MiB distinct, ratio 3.67 (in-place edits); a 1-byte insert dedups nothing, as fixed-size chunking predicts. `docs/benchmarks/dedup.md` |
| `TestCASConflict`: two commits on the same expected version, exactly one wins | Passes; `TestChaosConcurrentWriters` repeats it over 25 lossy-network seeds, 4 rounds each |
| `TestGCNoOrphanLeak`: after deletes, retention and 2 GC cycles, stored chunks == referenced chunks | Passes over 5 seeds with edits, a copy sharing every chunk of a deleted file, and deletes; fails at once with the sweep disabled |
| `TestGCSparesInflightUpload`: an upload stalled past a GC cycle still commits with every chunk | Passes; `TestChaosGCDuringSlowUpload` stalls past the grace plus two sweeps on 25 seeds |
| Chaos over 1,000 sim seeds, green, with GC invariants | `task chaos --seeds=1000`: 0 failed in 36 s. Every seed ends with `GCSettle` then `AssertCollected` (no orphan, no refcount or claim drift). CI runs 1,000 per push |
| Scenario links still replay; the new `gc` scenario replays | Playwright (Edge locally, Chromium in CI): `kill-node`, `corrupt-chunk` and `gc` match `TestScenarioGolden` event for event; the older goldens are unchanged |
| Dashboard works in sim and LIVE | Headless Edge: the sim `gc` scenario shows dedup, versions, undelete and collection; against `docker compose up`, delete and undelete round-trip through the gateway (bugs-found #11 fixed on the way) |

Bugs found: #10 (upload lease shorter than GC grace plus two sweeps), #11 (undelete not routed when the gateway serves the dashboard).

## Ship v0.1.0 checklist

- [x] Dashboard:
  - fault buttons per node: kill, freeze, slow, partition from the metadata server;
  - click a replica to corrupt it;
  - disabled placeholders for add node (Phase 6) and kill metadata leader (Phase 5);
  - 1×–50× speed, pause and step, replication-health bar, filtered timeline;
  - guided tour, SIMULATION / LIVE badge, read-only at phone width.
- [x] Shareable scenarios `kill-node`, `corrupt-chunk`, `rack-loss`, `slow-node`, and "Copy link to this run"
- [x] Gateway serves the dashboard at `/` (LIVE mode) and `/mode.json`
- [x] `docker compose run --rm demo` and `go run ./tools/task demo`
- [x] `docs/assets/demo.gif` (vhs, 50 s, 0.3 MiB) and `go run ./tools/task gif`
- [x] README rewritten: GIF, pitch, D2 architecture SVG (`task diagram`), quickstart, proof table, limitations, layout, roadmap
- [x] Social preview image, CHANGELOG
- [x] `trace-check --all`: every commit in history and every tracked file clean
- [x] Repo description, topics and website set; tag `v0.1.0` and [GitHub release](https://github.com/insanityatpeak/chunkd/releases/tag/v0.1.0) published (social preview image: uploaded by hand in repo settings)

## Exit criteria

| Criterion | Evidence |
|---|---|
| Fresh clone, only Docker: `docker compose up` → dashboard at `localhost:8080` against the real cluster; `docker compose run --rm demo` passes | Cloned `714a67e` into a clean directory: all containers healthy, `/mode.json` is live, the demo verified the download and trimmed 5 copies in 35 s; the CI compose job runs the same demo |
| Every scenario link replays identically | Playwright in the CI `web` job loads `kill-node` and `corrupt-chunk` (seed 7) and compares the page's timeline with `TestScenarioGolden`'s, event for event |
| README renders on GitHub and the GIF plays | Checked on github.com after the push |
| Lighthouse on the Pages URL: performance ≥ 80, accessibility ≥ 90 | Performance 97, accessibility 100, best practices 96 (LCP 1.3 s, TBT 170 ms, CLS 0.048), run locally against the live URL |
| `trace-check` passes; release `v0.1.0` published | `trace-check --all`: every file and commit clean; [v0.1.0](https://github.com/insanityatpeak/chunkd/releases/tag/v0.1.0) published |

## Phase 3 (complete)

Integrity: verify on read, scrubbing, quarantine, corruption repair; ADR 0013. `TestCorruptChunkDetectedOnRead`, `TestScrubberFindsCorruption`, `TestTwoReplicasCorrupt`, `TestAllReplicasCorrupt` and the real-mode `corrupt-replicas` scenario stay in CI.

## Phase 2 (complete)

Failure detection and re-replication; see ADRs 0010–0012. `TestKillNodeRestoresRF`, `TestTransientBlipNoRepair`, `TestRepairThrottle`, `TestSlowNodeHedgedRead`, and the real-mode kill, blip and freeze scenarios stay in CI.

## Phase 1 (complete)

Chunked, replicated upload and download; see ADRs 0005–0009. `TestRoundTrip`, `TestCommitRequiresMinReplicas`, `TestUncommittedInvisible`, and the 20 MiB compose round trip stay in CI.

## Next

- [ ] Phase 5: Raft for the metadata server (`07-phase5-raft`). `core/meta` State is the FSM; epochs, claims and leases are already logged ops. GC soft state (`orphanSince`, pending deletes) stays leader-local and resets on leader change. The dashboard's "Kill metadata leader" button becomes real.
- [ ] Skip trims of chunks a pending upload has claimed (closes the trim-vs-dedup race in Known limitations)
- [ ] Acknowledged incremental block reports (removes the up-to-30 s commit delay after a lost report)
- [ ] Overlap chunk uploads (window of chunks in flight)
- [ ] Per-step safety check in the chaos harness (real copies never below RF − 1, meta never counts a copy the store lacks); bugs #6 and #7 slipped past the end-state checker

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.
- Detector and repair timings (dead 10 s, delay 20 s) are demo-scale so a failure plays out in under a minute. HDFS waits 10.5 min and Ceph 10 min before re-replicating; the knobs are `detector.Config` and `repair.Config`.

## Known bugs

None open. See `docs/bugs-found.md`.
