# Status

Current phase: **Phase 5 complete** (Raft metadata group), released as [v0.2.0](https://github.com/insanityatpeak/chunkd/releases/tag/v0.2.0). Next: Phase 6, rebalancing.

## Phase 5 (complete)

The metadata service is a group of three Raft peers on etcd raft's `RawNode`, the same code in the sim, the browser and the real processes. Election timers are seeded and driven by the event loop; followers are never ticked, so the vote lease and the leader's quorum check are done in `core/consensus`. Writes commit through the log, reads are read-index reads, commands to nodes are fenced by the Raft term (kept on each node's disk), and GC deletes are logged intents sent only after they commit. ADRs 0017–0019. Dashboard: the group with roles, terms and log indexes, peer and leader faults, elections on the timeline, and a shareable `kill-leader` scenario; the browser runs three peers.

| Criterion | Evidence |
|---|---|
| `TestKillLeaderMidUpload` (real, 3 meta): kill the leader between chunk writes and commit; every acknowledged upload reads back | Sim over 12 seeds; `e2e.TestKillLeaderMidUploadReal` against in-process real peers; `kill-meta-leader` in the compose short suite, with a failover required |
| `TestStaleLeaderCannotCommit` | Sim (4 seeds) and consensus: the woken leader's write fails and leaves nothing, and a node refuses its old-term command (`Fenced` counter) |
| `TestMinorityPartitionRejectsWrites` | Passes; the cut-off side refuses, a write sent before it noticed never commits |
| Porcupine: 500 sim histories plus a real short suite, all linearizable; CI fails and uploads the visualization | Green in CI at `dde2a2c` (500 meta seeds in 44 s, 0 failed; the compose short suite with `kill-meta-leader`). CI runs `chaos --seeds=500 --metas=3` next to the 1,000-seed step; 3,000 meta seeds green locally. Every real-mode scenario records and checks its history |
| Log bounded under 10k ops; a restarted follower catches up from a snapshot | `TestLogBoundedUnder10kOps`, `consensus.TestSnapshotBoundsLogAndCatchesUp`, `Status.SnapshotsInstalled` |
| Dashboard: group view, kill and partition the leader, re-election on the timeline | Playwright replays `kill-node`, `corrupt-chunk`, `gc` and `kill-leader` against `TestScenarioGolden` (goldens regenerated for 3 peers); `TestDashboardLeaderCut`; checked by hand in LIVE mode against compose, killing the leader twice |
| WASM under 15 MiB; Lighthouse on Pages ≥ 80 performance, ≥ 90 accessibility | 10.09 MiB. Performance 100, accessibility 100, best practices 96 (LCP 1.3 s, TBT 20 ms, CLS 0.047), Lighthouse 12.8 in headless Edge against the live URL at `dde2a2c` |

Bugs found: #13 (a granted pre-vote renewed the voter's lease and dropped the real vote), #14 (a snapshot from before a membership change refused by a new voter), #15 (sim peers started before all existed), #16 (the agreement check compared peers at one instant), #17 (real-mode repair counts underflowed across a failover).

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
| Lighthouse on the Pages URL still ≥ 80 performance, ≥ 90 accessibility | Performance 100, accessibility 100, best practices 96 (LCP 1.2 s, TBT 30 ms, CLS 0), Lighthouse 12.8 in headless Edge against the live URL at `9523e65` |
| Real-mode suite green in CI | The compose job passes at `9523e65`, first time since before Phase 4; `transient-blip-no-repair` had failed about half the runs (bugs-found #12) |

Bugs found: #10 (upload lease shorter than GC grace plus two sweeps), #11 (undelete not routed when the gateway serves the dashboard), #12 (gRPC dial backoff kept a returning node unreachable from the gateway, so the real-mode blip scenario committed chunks a copy short and repaired them).

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

- [ ] Phase 6: rebalancing when nodes join or leave; ADRs start at 0020. The dashboard's "Add node" button becomes real.
- [ ] A shorter per-attempt timeout (or a hedged call) for metadata RPCs, so a silently dead leader costs less than the 10 s call timeout
- [ ] Membership change from the CLI (the consensus layer supports single-server changes), to replace a peer whose WAL is corrupt
- [ ] Skip trims of chunks a pending upload has claimed (closes the trim-vs-dedup race in Known limitations)
- [ ] Acknowledged incremental block reports (removes the up-to-30 s commit delay after a lost report)
- [ ] Overlap chunk uploads (window of chunks in flight)
- [ ] Per-step safety check in the chaos harness (real copies never below RF − 1, meta never counts a copy the store lacks); bugs #6 and #7 slipped past the end-state checker

## Open decisions

- `go.mod` declares `go 1.26` (etcd raft v3.7.0 requires it; grpc-go 1.84 needs 1.25).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.
- Detector and repair timings (dead 10 s, delay 20 s) are demo-scale so a failure plays out in under a minute. HDFS waits 10.5 min and Ceph 10 min before re-replicating; the knobs are `detector.Config` and `repair.Config`.

## Known bugs

None open. See `docs/bugs-found.md`.
