# Status

Current phase: **none open; v1.4.0 is the latest release** ([release](https://github.com/insanityatpeak/chunkd/releases/tag/v1.4.0)) (Phase 9 fixed the known limitations and audited the codebase; v1.1.0, v1.2.0, v1.2.1, v1.3.0 and v1.4.0 are tagged). **v1.0.0 shipped** ([release](https://github.com/insanityatpeak/chunkd/releases/tag/v1.0.0)). Phase 7 was released as [v0.4.0](https://github.com/insanityatpeak/chunkd/releases/tag/v0.4.0), Phase 6 as [v0.3.0](https://github.com/insanityatpeak/chunkd/releases/tag/v0.3.0), Phase 5 as [v0.2.0](https://github.com/insanityatpeak/chunkd/releases/tag/v0.2.0).

## Phase 9 (v1.1.0, with v1.2.0, v1.2.1, v1.3.0 and v1.4.0 follow-ups on `main`)

Fixes for the limitations Phase 8 listed, then an audit of the whole codebase. One additive log op and snapshot field (ADR-0030); data directories from v1.0.0 replay unchanged.

| Item | Evidence |
|---|---|
| Quota check no longer scans every file | Per-namespace counter, reconciled each epoch; a limited Begin cost 825 us at 10,000 files and 29 ms at 100,000. ADR-0028 |
| A put returns at the replica quorum | Put p50 with one gray node 4,111 ms to 214 ms (`docs/benchmarks/results.md`); `TestPutNotHeldBySlowReplica`. ADR-0029 |
| No trim of a chunk a pending upload has claimed | `trimSafe` waits for the claim; `resendTrims` retries |
| Hints rate-limited per node | `TestSuspectHintsAreRateLimited` (1 a second, burst 10) |
| Repair copy's store write off the event loop | `AsyncCaller.GoOff`; `TestGoOffLeavesLoopFree`. ADR-0012 amended |
| Repair targets use the surviving copies' racks | `placement.Extend`, `TestRepairKeepsRackSpread`; bugs-found #31. ADR-0011 amended |
| Quarantine directory capped | `TestQuarantineCapEvictsOldest` (1 GiB, `-quarantine-cap`) |
| A silently dead metadata leader costs 3 s, not 10 s | `Call.Timeout`, `TestSilentLeaderCostsOneShortAttempt`; the `kill-leader` golden's write goes from 10.5 s to 3.5 s. ADR-0009 amended |
| A new leader accepts drain for a node it has not heard from | `NodeJoinOp`, `TestDrainBeforeFirstHeartbeatAfterRestart`, `TestNodeJoinIsLoggedAndSnapshotted`. ADR-0030 |
| An EC put returns at 5 of 6 shards (v1.3.0) | EC put p50 with one gray node on 7 nodes 4,157 ms to 238 ms (`put-latency.csv`); `TestECPutNotHeldBySlowShard`, `TestECPutShardNeverLands`. ADR-0031 |
| A lost incremental block report is resent within a heartbeat or two (v1.4.0) | `TestLostIncrementalReportIsResent`: commit 1.2 s after the writes with every report lost (20 s and waiting without the resend); real-mode short suite 7 of 7 against compose with it, `transient-blip-no-repair` at 0 repair copies. ADR-0033 |
| A metadata peer that lost its log is replaced (v1.3.0) | `chunkd meta replace`, `-join`; `TestReplacePeer`, `TestReplaceMetaPeerReal` (a corrupt WAL on disk), chaos: 307 replacements in 1,500 `--metas=3` seeds. ADR-0032 |
| A chunk repeated within one upload wave is stored once (v1.4.0) | `storeWave` reuses the wave's first put for repeats; `TestWindowDedupsRepeatsInWave`; bugs-found #36 |
| A node back from a long outage is reachable within seconds (v1.2.1) | `pool.get` dials `passthrough:///`; real-mode short suite 7 of 7 locally and in CI; bugs-found #35 |

Audit (Part 3):

| Area | Finding | Severity | State |
|---|---|---|---|
| Chaos | `--seeds=3000 --from=2001` failed 5 seeds, `--metas=3` 2 of 1500, `--ec` 4 of 1500: a second balance move of a chunk in flight leaked a repair slot, and rack-aware repair then waited on that node forever (bugs-found #33; a Phase 6 bug) | High | Fixed, `TestSecondMoveOfAChunkInFlightIsDropped`, `TestChaosRepairSlotRegressions` |
| Metadata | `Begin` sized a placement table from the declared size: a size near the int64 limit panicked the leader, a huge one asked for terabytes (bugs-found #32) | High | Fixed, `TestBeginRefusesAbsurdSize` |
| Client | `Append` checked `offset+n`, which wraps near the int64 limit; the claim check and the length check refuse the request later, so nothing is stored | Low | Hardened, no test can tell it from the old code |
| Tests | Four sim tests assumed the old retry timing (a surplus chunk before GC, a mid-drain view after a leader change, a balancer counter, a repair bound with one lost-copy timeout) | Test only | Fixed; the bound now budgets two copy timeouts (ADR-0011) |
| Docs | Three test names in ADR-0002, 0018, 0019 no longer existed | Low | Fixed |
| Static analysis | `go vet` (native and wasm), staticcheck, lint-imports, trace-check: clean. Flake runs `-count=3` of `grpcnet`, `gateway`, `e2e`, `cmd/chunkd` under `-race`: clean | | |
| Read-through | Panics, ignored errors, wall-clock use outside core, growth of `requests`, `committed` and lease maps, gateway path handling (meta rejects any path that is not clean), `WithUI` route list: nothing else found | | |
| Dependencies | `x/net` v0.57.0 had five HTTP/2 advisories (fixed in v0.60.0): bumped to v0.61.0, `go.mod` now says `go 1.26.0`. `npm audit` high severity in `source-map-js` (dev build): fixed with `npm audit fix`. The Go toolchain on this machine, 1.27.0, has 12 standard-library advisories fixed in 1.27.2 | | Modules fixed; the local toolchain was upgraded to 1.27.2 on 2026-10-19 |

Chaos after the fixes: `--seeds=3000 --from=2001`, `--metas=3 --seeds=1500`, `--ec --seeds=1500`, `--metas=3 --ec --seeds=1000`: 0 failed in all four (slowest RF restore 29 to 42 s).

Gates: lint, lint-imports, wasm 11.11 MiB, `go test -race -p 1 ./...` (`internal/sim/cluster` about 610 s: use `-timeout=25m` as `tools/task` does), trace-check.

Scenario goldens: regenerated three times (quorum puts; rack-aware targets; the metadata timeout and the join entries), each with the owner's OK. The golden tests, `ec.spec.ts` and the README dashboard links now use seed 1.

Added after the merge, on `main`: a compose scenario for `put -resume` (`go run ./tools/task resume-e2e`: the gateway is killed mid-upload, the put resumes, hashes match), a 7-node EC compose profile with two real-mode scenarios (`chaos --mode=real --ec`: a node down past the repair delay, then two nodes down at once; both pass, 33 s longest under-replication against a 90 s bound), the dashboard diff view (mark a placement, see what was placed and lost; `web/e2e/diff.spec.ts`), and the repair-against-reads measurement (`chunkd-bench -stages=compose`; results.md: read p99 241.6 ms while copying against 212.6 ms repaired, p50 148.1 against 145.0 ms, one run). The first real EC run found bug #34 (EC downloads through the gateway failed verification). Since then (2026-10-19): CI's compose job also runs `resume-e2e` (on the fresh cluster) and `chaos --mode=real --ec`, next to the `--short` suite; the local Go toolchain is 1.27.2. Not done: repeating the measurement on separate hosts. Content-defined chunking was out of scope.

Bugs found: #31 (a repair copy could target a node that already held the chunk), #32 (a declared upload size could crash the metadata leader), #33 (a second balance move of a chunk in flight leaked a repair slot), #34 (an erasure-coded download through the gateway failed verification).

## Phase 8 (complete)

No protocol, log-format or on-disk change. Benchmarks, a rewritten design document, an ADR audit, design questions, a write-up and a README pass.

| Item | Evidence |
|---|---|
| Benchmarks re-run with one command and produce the committed tables | `go run ./tools/task bench`: `docs/benchmarks/{data,charts,results.md}`; sim tables are exact for a seed, real tables are within noise. ADR-0027 |
| Design document complete | `docs/design.md`: every phase's mechanism, consistency per operation, CAP per plane, decisions table, prior art, what the sim cannot capture |
| ADR audit | 0001 to 0027; none superseded; 0001, 0002 and 0006 amended by 0017 to 0019, with an index in `docs/adr/README.md`; stale phase references corrected |
| Design questions and write-up | `docs/interview.md` (15), `docs/writeup-chaos-harness.md` (about 1,180 words) |
| README | Benchmark table, dashboard capture, new "What's proven" rows, updated Known limitations and roadmap |
| Gates | lint, lint-imports, wasm 11.07 MiB, `go test -race -p 1 ./...` green (sim/cluster 684 s), trace-check clean over the tree and all 146 commits |

Findings from the benchmarks, now in Known limitations: a limited Begin scans every file (29 ms at 100,000 files); writes are not hedged; the effect of repair on foreground reads is not measured because the sim shares no capacity.

Checked from a fresh clone: `docker compose up -d --build --wait` healthy, `docker compose run --rm demo` passes (SHA-256 verified with a node down, extras trimmed on return), and `demo.gif` re-rendered against it.

Possible future work at the end of Phase 8: a per-namespace usage counter for quotas (done, ADR-0028), hedged or latency-aware writes (quorum puts done, ADR-0029; latency-aware placement open), content-defined chunking (open), background tiering from copies to erasure coding (open), a multi-host benchmark run (open), a compose scenario for `put -resume` and for EC (done, v1.2.0), and a diff view in the dashboard (done, v1.2.0).

## Phase 7 (complete)

Erasure coding (item A) is built. An upload can store each chunk as an RS(4,2) stripe on 6 distinct nodes; each shard is a block of its own with a slot-and-stripe header, so GC, scrub, drain and trims treat it as any block. Reads gather 4 shards with hedging and decode from parity; repair rebuilds a lost shard from 4 others on a node outside the stripe. ADRs 0022–0023. Dashboard: a "Store as" choice, shard counts, a shard-level chunk grid, rebuild sources, and a shareable `ec` scenario. The other four items follow.

Resumable uploads (B), API keys with quotas (D), and versions and CLI polish (C/E) are built on top of it. ADRs 0024–0026.

| Item | Evidence |
|---|---|
| B: resume after a client crash sends no stored chunk again; a wrong offset is refused; resume survives a leader failover | `sim/cluster` `TestResumeAfterClientCrash`, `TestResumeRefusals`, `TestResumeAcrossLeaderFailover`; `TestPutResumeAfterCutOff` (CLI over real disks); gateway tus tests |
| D: a refused upload reads no bytes; concurrent uploads cannot pass a quota; keys see only their namespace and uploads | `TestQuotaReservesAtBegin` (state), `TestQuotaRefusalReadsNoBytes`, `TestQuotaRefusesBeforeBytesAreSent`, `TestAuthScopesPathsToNamespace`, `TestAuthUploadsBelongToTheirKey` |
| C: per-path retention, chunk-level diff, restore | `TestRetentionPerPath`, `TestRetentionSurvivesSnapshot`, `TestRetentionIsPerNamespace`, `TestDiffManifests`, `TestDiffAgainstACluster`. The dashboard already lists retained versions with restore; v1.2.0 added a diff view |
| E: exit codes, completion, bench, progress | `TestExitCodes`; `chunkd completion`, `chunkd bench` |
| Existing goldens, chaos | The 7 goldens unchanged; `chaos --seeds=1000` and `--seeds=500 --metas=3`: 0 failed |

Both items later done in v1.2.0: `put -resume` against compose, and the dashboard diff view.

Erasure coding:

| Criterion | Evidence |
|---|---|
| Any 2 shards lost reads; 3 lost fails loudly | Codec tables; `TestECReadSurvivesTwoLostNodesNotThree`, `TestECReadDecodesAroundARottedShard`; `e2e.TestRoundTripEC` over real gRPC and disk, 0 B to 37 MiB |
| Lost shards rebuilt within the repair bound, stripes on distinct nodes | `TestECRepairRebuildsLostShards`; scheduler and node rebuild tests |
| Storage and repair cost measured | `docs/benchmarks/ec.md`: 1.50× against 3.00× stored; repair read 80.1 MiB for 20.0 MiB rebuilt (4×) |
| Chaos with EC puts | `chaos --seeds=500 --ec` per push (CI); locally 3,000 EC seeds and 1,500 with the metadata group, 0 failed |
| Goldens and replay | The 7 existing goldens unchanged; `ec` added; Playwright replays all 7 plus an EC UI check (Edge) |

Bugs found: #25 (a stripe read gave up on a shard after one lost message), #26 (a rebuild could take two source slots on one node), #27 (the trim watcher blamed a trim for a later failure), #28 (the harness counted a copy GC was deleting as surplus), #29 (one lost message stalled a rebuild past its copy timeout), #30 (a stripe read waited a call timeout on a node that had just died).

## Phase 6 (complete)

A placement planner in `core/rebalance` gives every node a rack-feasible byte target. A rack holds at most ceil(RF/racks) copies of a chunk, so its nodes split that share. The planner moves chunks only while some node is outside its band, max(10% of target, 2 chunks). A move must strictly lower the total distance from target and never lose a distinct rack. Moves run in the repair scheduler as a third class (repair before drain before balance; drain and balance share at most 4 of 8 slots), and only once membership has settled. Every trim, whether a repair surplus or a move's source, is a logged TrimIntent sent only after it commits, with at most one pending per chunk. TrimDone clears it on every peer, and the leader rechecks RF elsewhere before each send. Drain state is a logged NodeAdmin op (active, draining, decommissioned). Decommission is refused until every chunk has RF copies on other nodes; the gateway and CLI expose all three. ADRs 0020–0021. Dashboard: an Add node button, per-node Drain/Undrain, amber outlines and labels for leaving nodes, a usage bar with the balance target and band, tagged drain and balance copies, and shareable `add-node` and `drain` scenarios.

| Criterion | Evidence |
|---|---|
| `TestAddNodeConverges`: 5 → 6 nodes, every node within its band, bytes moved ≤ the stated bound | Moved 96.0 MiB against ½·L1 100.9 MiB and a bound of 124.9 MiB; never below RF during the moves; the cluster view's targets agree. `docs/benchmarks/rebalance.md` (`TestRebalanceBenchmark`): demo set on r3 12.0 MiB moved (½·L1 25.0, band-limited), loaded set on r3 94.7 MiB of ½·L1 101.3 MiB in 2.6 s |
| `TestDrainNeverDropsRF`: drain under load, a node killed mid-drain, RF below target only for chunks the failure hit | Passes: 41 drain copies, 43 repair copies, decommission allowed as soon as RF was back. Real mode: `drain-node` in the compose short suite |
| `TestDecommissionOnlyWhenSafe` | Refused while active and while copies are short; allowed after the drain (99 MiB in 13 s); the node then dies with no repair copy |
| Chaos over 1,000 sim seeds green, porcupine green | `chaos --seeds=1000`: 0 failed, 1,370 membership faults (add-node, drain, undrain, kill-drain-target). `--seeds=500 --metas=3`: 0 failed, 799 leader faults, 37 of them a leader kill with a balance move in flight. Wider sweeps locally with membership faults: 3,000 seeds and 1,500 meta seeds green. Trim safety is checked at every trim delete in every run |
| Real-mode short suite | 7 scenarios green against compose, `drain-node` and `add-node` included; node-6 and its volume are removed afterwards |
| Goldens and replay | The 4 existing goldens are unchanged; `add-node` and `drain` added; Playwright replays all 6 (Edge) |
| CI and Lighthouse on Pages | CI green at `693e87b` ([run 37027145171](https://github.com/insanityatpeak/chunkd/actions/runs/37027145171): go, web, compose). Performance 100, accessibility 100, best practices 96 (LCP 1.3 s, TBT 20 ms, CLS 0) against the live URL |

Bugs found: #18 (a deposed leader could trim from soft state), #19 (a follower kept a phantom location after TrimDone), #20 (a stale trim was resent after another holder died), #21 (real-mode no-repair check counted balance moves), #22 (a retried drain overtook the undrain after it, in the sim's chaos client), #23 (the demo waited for a removed node to come back), #24 (a recreated container's old IP routed one node's commands to another).


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
| Every scenario link replays identically | Playwright in the CI `web` job loads `kill-node` and `corrupt-chunk` (seed 1) and compares the page's timeline with `TestScenarioGolden`'s, event for event |
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

- [x] Phase 6: rebalancing when nodes join or leave; ADRs start at 0020. The dashboard's "Add node" button becomes real.
- [x] A shorter first attempt for metadata RPCs (3 s, doubling); a hedged call to the next peer is still open
- [x] Membership change from the CLI, to replace a peer whose WAL is corrupt: `chunkd meta replace` and `chunkd-meta -join` (ADR-0032)
- [x] Skip trims of chunks a pending upload has claimed (`trimSafe`, `TestTrimSkipsChunkWithPendingClaim`)
- [x] Confirmed incremental block reports: heartbeats list unconfirmed reports and the missing ones are resent (ADR-0033)
- [x] Overlap chunk uploads: waves of 4 chunks, one claim per wave and every chunk's puts in one `Caller.Quorums` batch (ADR-0034); a sliding window is still open
- [x] (done in v1.2.0, 7-node `ec` profile) A real-mode EC chaos scenario: compose ran 5 nodes and a stripe needs 6 (real processes are covered by `e2e.TestRoundTripEC` on 6 in-process nodes)
- [x] Per-step safety check in the chaos harness: the trim check (`checkTrim`, since `2fc4980`) and `cluster.WatchSafety` (copy floor, lasting phantom copies) every 100 ms of every chaos seed. Sweeps of 3,000 seeds, 1,500 with `--metas=3` and 1,500 with `--ec` found no violation; a planted GC bug that deletes two copies of a few referenced chunks, which repair then restores, fails 111 of 200 seeds with the watcher and 3 without

## Open decisions

- `go.mod` declares `go 1.26.0` (etcd raft v3.7.0 requires 1.26; `golang.org/x/net` v0.61.0 requires 1.26.0).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.
- Detector and repair timings (dead 10 s, delay 20 s) are demo-scale so a failure plays out in under a minute. HDFS waits 10.5 min and Ceph 10 min before re-replicating; the knobs are `detector.Config` and `repair.Config`.

## Known bugs

None open. See `docs/bugs-found.md`.
