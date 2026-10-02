# Bugs found

Real defects caught by tests, gates or the chaos harness.

## 1. gRPC linked into the browser build

| | |
|---|---|
| Symptom | `task wasm` failed the size gate: `cluster.wasm` 20.01 MiB against a 20 MiB limit. |
| Repro | Generate services and messages into one Go package, import the messages from core, run `go run ./tools/task wasm`. |
| Root cause | `protoc-gen-go-grpc` writes service stubs into the same package as the message types. Core imported `chunkdv1` for `PingRequest`, which also contained the gRPC client and server stubs, so `google.golang.org/grpc`, `net/http` and `golang.org/x/net` were linked into the WASM binary (325 packages). |
| Fix | Services moved to proto package `chunkd.rpc.v1` (Go package `rpcv1`); messages stay in `chunkd.v1`. `cluster.wasm` dropped to 7.62 MiB. Commit `194d04a`. |
| Regression test | `task wasm` runs `go list -deps ./cmd/chunkd-wasm` for `js/wasm` and fails if `google.golang.org/grpc` or `net/http` appears. |

## 2. Ambiguous commit reported as failure

| | |
|---|---|
| Symptom | Under message duplication, `Put` returned `not_found: upload 1` for an upload whose version was committed and visible. Under loss, a single dropped metadata message failed the whole operation. |
| Repro | `TestUploadsUnderMessageLoss`, seed 1 (5% drop, 5% duplication). |
| Root cause | The network delivered `CommitUpload` twice. The first copy committed and removed the pending upload; the second found no upload and answered `not_found`. Response delays are random, so the error could reach the client first. A lost commit response had the same effect. The client could not tell "not committed" from "committed, answer lost". |
| Fix | Each version records the `upload_id` that created it, and a commit for an upload that already committed returns that version. Delete is conditional (the client resolves the live version first) and a repeated delete of the same version returns the existing tombstone. The client retries metadata calls on `unavailable` and failed replica puts once. Commit `c56233f`. |
| Regression test | `meta.TestCommitAndDeleteAreIdempotent` (duplicate commit and delete, and commit retry after a metadata restart); `e2e.TestUploadsUnderMessageLoss` over 20 seeds. |

## 3. A copy finishing inside the repair delay dropped its chunk until the next scan

| | |
|---|---|
| Symptom | A chunk down to its last copy was repaired to 2 copies at once (the last-copy rule skips the delay), then sat at 2 of 3 for up to 30 s after the delay ended. |
| Repro | `repair.TestRepairDelay`, subtest "last copy is repaired without waiting": two of three holders dead, the first copy finishes inside the 20 s delay. |
| Root cause | `Scheduler.finish` re-assessed the chunk, saw the remaining copy was still inside the delay, and returned. Nothing scheduled a wake-up for the delay's end, so only the periodic 30 s scan picked the chunk up again. |
| Fix | `finish` calls `wakeAtLeast(readyAt)` when the rest of the chunk is waiting out the delay. Commit `17a2616`. |
| Regression test | `repair.TestRepairDelay` ("last copy is repaired without waiting": all third copies complete by the end of the delay, not at the next scan). |

## 4. A restarted node's chunks looked missing for one round trip

| | |
|---|---|
| Symptom | A node that restarted inside the repair delay still cost 3 copies per blip, all trimmed again seconds later. |
| Repro | `TestTransientBlipNoRepair` (node down 2 s, 15 s and 25 s). |
| Root cause | On a new incarnation the metadata server dropped the node's locations, expecting its full block report. The report arrives one heartbeat round trip later; in that window a scan triggered by the detector's transition saw the node's chunks with too few holders, and chunks already past the delay were copied. |
| Fix | A dead or restarted node keeps its locations until its next full report replaces them. Suspect and dead holders still count toward RF during the delay, so nothing is copied for a node that comes back. Commit `17a2616`. |
| Regression test | `TestTransientBlipNoRepair` (zero copies), `meta.TestClusterRestartKeepsLocationsUntilReport`; real mode `transient-blip-no-repair`. |

## 5. Over-replication lingered for a full scan interval

| | |
|---|---|
| Symptom | Chaos seed 63 ended with chunks at 4 copies after the settle window. |
| Repro | `go run ./tools/task chaos --seed=63` before the fix. |
| Root cause | Two paths created extra copies that only the 30 s periodic scan removed: a client write that dedups against an existing chunk places it on nodes that may already hold enough copies, and a trim whose command or confirmation was lost was forgotten. |
| Fix | Every block report runs `checkExcess` on the chunks it adds, and a trim timeout re-checks the chunk at once. Commit `e292f7d`. |
| Regression test | Chaos seed 63 and the 500-seed CI run (settled with zero over-replicated chunks); `repair.TestTrimSafety`. |

Related: with 1% message loss a copy command or its completion report can be lost, and the copy then holds its slots until the copy timeout. The timeout was cut from 30 s to 10 s and the RF restore bound includes one copy timeout.

## 6. A retried trim could remove the wrong replica

| | |
|---|---|
| Symptom | In the dashboard's kill-node-3 script, chunk `4bbb1e8e…` was trimmed on node-5 after its first trim on node-2 timed out, then re-replicated 18 s later: it had been at 2 real copies in between. |
| Repro | `TestDashboardTimeline` (seed 5, 1% loss) before the fix; `repair.TestTrimRetrySameVictim`. |
| Root cause | node-2 deleted its copy but the confirmation was lost. On the trim timeout the scheduler chose a victim again from the holders it believed confirmed, still counting node-2. node-2's heartbeat now reported less used space, so the victim rule picked node-5, whose copy was real. The chunk was one below RF until node-2's next full report exposed the gap. Chaos did not catch it: it checks the settled end state, and the gap healed itself. |
| Fix | A timed-out trim is re-sent to the same node, which confirms absent chunks as removed. A new victim is chosen only once the old one no longer counts as a confirmed holder. Commit `3c20807`. |
| Regression test | `repair.TestTrimRetrySameVictim` (victim's used space drops after the lost confirmation; the retry must go to the same node), `TestDashboardTimeline`. |

## 7. Reordered block reports dropped new chunks and resurrected deleted ones

| | |
|---|---|
| Symptom | CI's real-mode `transient-blip-no-repair` failed with 6 repair copies. All 6 started one second after the scenario's uploads, before the blip, one per freshly uploaded chunk, each from node-1 to node-4, with no error logged anywhere. |
| Repro | CI run on `9770903`, compose job; deterministically `meta.TestClusterReportOrdering`. |
| Root cause | A full block report replaced the node's locations wholesale. On a node, chunk puts run on concurrent handlers while the full report is listed on the event loop, and nothing ordered the two: a report listed just before a put finished, delivered after that put's incremental report, dropped the new chunk, and repair copied it again. The sim runs one goroutine, but its network reorders messages by random delay, so the same drop was possible there, and so was the dangerous mirror case: a trim's delete report overtaken by an older full report re-added a copy that no longer existed, so repair could count, or trim against, a phantom replica. |
| Fix | Reports carry the node's incarnation and a sequence number. A put or delete takes its number after changing the store while holding a shared lock; a full report takes its number and lists the store under the exclusive lock, so a full report numbered S reflects exactly the changes numbered below S. The metadata server keeps the latest change number per chunk since the last full report: a full report cannot drop a newer add or re-add a newer delete, reports older than the last full report are ignored, and reports from a previous incarnation are dropped. Only changes a report actually made complete copies and trims. Commit `4a02c76`. The real-mode runner also now waits for every node to be alive before calling a scenario settled, so scenarios cannot overlap (`33dcce0`). |
| Regression test | `meta.TestClusterReportOrdering` (seven arrival orders), `meta.TestClusterRestartKeepsLocationsUntilReport` (old incarnation ignored, seq restarting at 1 accepted); real mode `transient-blip-no-repair` in CI. |

## 8. A chunk with every copy corrupt was reported as unavailable

| | |
|---|---|
| Symptom | With all 3 copies of a chunk rotted, `Get` failed with `unavailable: no intact replica`, which invites retries, instead of saying the data is lost. |
| Repro | `TestAllReplicasCorrupt` (seed 5) against the first version of the check. |
| Root cause | The client declared a chunk lost only if every replica answered `CodeCorrupt` or sent bad bytes. But the first corrupt report made repair start a copy whose source read hit the second rotten replica, which quarantined itself before the client got there, so the client saw `not_found` from it and concluded nothing. |
| Fix | A chunk is reported corrupt when at least one replica failed verification and every other replica either failed it too or no longer has the chunk. Commit `da02133`. |
| Regression test | `TestAllReplicasCorrupt` (`CodeCorrupt`, counted as lost, zero copies). |

## 9. Repair copied chunks whose third replica was still reporting

| | |
|---|---|
| Symptom | CI's real-mode `transient-blip-no-repair` failed with 1 repair copy, made 2 s into the scenario, before the blip, for a chunk uploaded a moment earlier. |
| Repro | CI run on `74d158e`, compose job; `repair.TestUploadGrace`. |
| Root cause | A commit needs 2 of 3 replicas reported (ADR-0007). The third replica's incremental report travels on its own and can land after the commit. A scan in that gap saw 2 of 3 with no dead holder, so no delay applied, and it copied the chunk at once; the late report then made it over-replicated and a trim followed. The repair delay only excused copies on dead nodes, not copies still being written. |
| Fix | A freshly committed chunk gets an upload grace (one copy timeout, 10 s) before a missing copy counts; a chunk on its last copy is still repaired at once. HDFS likewise leaves blocks under construction to pending-replication timeouts. Commit `405d1b2`. |
| Regression test | `repair.TestUploadGrace` (wait inside the grace, copy after it, last copy at once); real mode `transient-blip-no-repair` in CI. |

## 10. An upload stalled past the GC grace could lose its lease first

| | |
|---|---|
| Symptom | `TestChaosGCDuringSlowUpload` (seed 1) and `TestChaosDeleteWhileUploadingSameChunk` (seed 11) failed at commit with `not_found: upload N` after stalls of 120 s and 100 s. No chunk had been deleted; the upload itself was gone. |
| Repro | `go test ./internal/sim/cluster -run TestChaosGCDuringSlowUpload` with `LeaseEpochs: 4`. |
| Root cause | A lease expires when `touched + LeaseEpochs <= epoch`. An upload touched just before an epoch tick loses almost a whole epoch, so 4 epochs of 30 s guarantee only 90 s of idle time, not 120 s. That is shorter than the GC grace plus two sweeps (120 s): the very stall the GC design promises to survive expired the upload through its lease instead. |
| Fix | `LeaseEpochs: 6` (at least 150 s idle), and a sizing rule in `meta.Config`: the minimum lease, `(LeaseEpochs-1) × EpochEvery`, must exceed `GCGrace + 2 × EpochEvery`. The slow-upload scenario checks the rule against the config it runs. Found before the GC work was committed. |
| Regression test | `TestChaosGCDuringSlowUpload`, `TestChaosDeleteWhileUploadingSameChunk`, `meta.TestUploadLeaseExpires`. |

## 11. Undelete through the compose gateway hit the file server

| | |
|---|---|
| Symptom | In `docker compose up`, `POST http://localhost:8080/undelete/<path>` answered `404 page not found` in plain text, for the dashboard and for `chunkd undelete` against the gateway alike. The same call through `gateway.Handler` alone worked. |
| Repro | `docker compose up -d --build --wait`, upload and delete a file, then `curl -X POST localhost:8080/undelete/<path>`. |
| Root cause | `gateway.WithUI` puts the API and the dashboard's static files on one mux and forwards an explicit list of API prefixes. The undelete route was added to `Handler` but not to that list, so it fell through to `http.FileServer`. The gateway tests used `Handler`; the one `WithUI` test only covered `GET` routes. |
| Fix | `/undelete/` added to the forwarded prefixes, with a comment that every `Handler` route must be listed. Found by checking the dashboard's new undelete button in live mode. |
| Regression test | `gateway.TestGatewayServesUI` posts to `/undelete/` and requires the API's JSON `not_found`. |

## 12. A node back from a long outage was unreachable from clients for seconds

| | |
|---|---|
| Symptom | The real-mode suite's `transient-blip-no-repair` failed in CI about half the time with 1 to 6 repair copies, although the blipped node came back well inside the repair delay. The copies started exactly 10 s (the upload grace) after the scenario's first uploads, not after the blip. |
| Repro | `docker compose up -d --wait`, then `go run ./tools/task chaos --mode=real --short`; failed 2 of 3 local runs. Polling the blip's files showed the first uploads committing on 2 nodes without node-3, the only node in rack r3. |
| Root cause | The previous scenario kept node-3 down for 85 s while reads kept dialing it, so the gateway's gRPC channel backed off exponentially (gRPC's default grows to 120 s). node-3's heartbeats reach the metadata server on a different connection, so it was marked alive within 2 s of returning and placed on at once. The gateway's channel only redialed when its backoff timer fired, about 10 s after node-3 was back (gRPC channel log: READY at 12:42:36 for a node started at 12:42:26). Until then, calls failed immediately with `unavailable`, both write rounds missed node-3, the chunks committed with 2 copies, and repair topped them up after the grace. |
| Fix | The connection pool caps the dial backoff at 2 s (base 250 ms). |
| Regression test | `grpcnet.TestCallerReconnectsSoonAfterLongOutage`: 20 s of failed calls, then the peer returns and must answer within 4 s (10+ s before the fix, 0.7 s after). The real-mode suite passes on repeated runs. |

## 13. A granted pre-vote refreshed the voter's lease, so the real vote was dropped

| | |
|---|---|
| Symptom | After the metadata leader was killed, a follower won its pre-vote but never the election that followed: its real vote requests were dropped, and the group stayed leaderless across election rounds. |
| Repro | `go test ./internal/core/consensus -run TestLeaderKilledAndRestarted` with a pre-vote grant counted as leader contact. |
| Root cause | The vote lease (ADR-0017) drops vote requests while a peer has heard from a leader within the election timeout. Granting a vote counted as contact, so the candidate gets time to win. A granted *pre*-vote counted too, which renewed the voter's own lease at the exact moment the candidate's real vote request arrived, and the lease dropped it. |
| Fix | Only a granted real vote (`MsgVoteResp` without reject) refreshes `lastHeard`; a pre-vote response does not. Found before the consensus code was committed, in `9fd99eb`. |
| Regression test | `consensus.TestLeaderKilledAndRestarted`, `TestIsolatedFollowerDoesNotDisruptTheLeader`; 500 chaos seeds with leader faults per push. |

## 14. A snapshot from before a membership change could not catch up a new voter

| | |
|---|---|
| Symptom | A voter added after the log had been compacted never caught up: the leader kept sending it the stored snapshot, and the new peer kept refusing it. |
| Repro | `go test ./internal/core/consensus -run TestAddAndRemoveVoter` (120 entries with a snapshot every 50, then a fourth voter) with the stored snapshot sent as is. |
| Root cause | etcd raft refuses a snapshot whose membership does not include the receiver. The stored snapshot was cut before the change that added the peer, so its membership listed only the old voters. |
| Fix | The storage the library reads snapshots from compares the stored snapshot's membership with the current one, and cuts a fresh snapshot of the applied state when they differ. Found before the consensus code was committed, in `9fd99eb`. |
| Regression test | `consensus.TestAddAndRemoveVoter`. |

## 15. The sim started metadata peers before all of them existed

| | |
|---|---|
| Symptom | With `Metas: 3`, the first peer started knowing only itself: it bootstrapped a group of one and led it, apart from the other two. |
| Repro | `newMetaGroup` in `internal/sim/cluster/metagroup_test.go` with each peer started as it is created. |
| Root cause | `cluster.New` created and started each peer in one loop, and the peer list a peer bootstraps from is read from the peers created so far. |
| Fix | All peers are created first, then started; each one bootstraps from the full list. Found before the sim group was committed, in `ea8d5c6`. |
| Regression test | `TestMetaGroupRoundTripAndAgreement` (one leader, `AssertMetaAgree`); every 3-meta chaos seed checks the same. |

## 16. The metadata agreement check compared peers at one instant (harness)

| | |
|---|---|
| Symptom | A `chunkd-chaos --metas=3` run failed seed 260 with a metadata divergence between peers, though no operation had been applied differently. |
| Repro | `go run ./cmd/chunkd-chaos --seed=260 --metas=3` before `5c6f005`. |
| Root cause | `AssertMetaAgree` compared every live peer's state at the same moment. Seed 260 ends with the 30 s epoch proposal committing at the very instant of the check: one follower had not yet heard the new commit index, so it was one entry behind. That is normal replication lag, not divergence. |
| Fix | Compare at one applied index: advance the clock (up to 2 s) until every live peer has applied the same index, then compare. Different state at equal indexes is still reported. Commit `5c6f005`. |
| Regression test | `chaos.TestChaosMetaAgreeAcrossEpochTick` (seed 260). |

## 17. Real-mode repair counts underflowed across a leader change (harness)

| | |
|---|---|
| Symptom | After the new `kill-meta-leader` real-mode scenario, the run reported 18446744073709551556 repair copies. A no-repair scenario with a leader change would have failed on that number. |
| Repro | `go run ./tools/task chaos --mode=real --short` before `f4e9e58`. |
| Root cause | `RunTarget` computed repair copies as the last poll's counter minus the first. The counters are the answering leader's, since its process started; after the failover the new leader's count started below the old one's, and the unsigned subtraction wrapped to 2^64 − 60. |
| Fix | The health poll adds up deltas within one leader and starts again at each leader change (`leaderDeltas`). Copies made in the second before a new leader's first poll are not counted. Commit `f4e9e58`. |
| Regression test | `chaos.TestLeaderDeltasAcrossFailover`; the real-mode short suite runs `kill-meta-leader` in CI. |

## 18. A deposed leader could trim from soft state

| | |
|---|---|
| Symptom | Found in design review of rebalancing, then reproduced. A metadata leader cut off from its peers keeps acting for up to an election timeout. A storage node can still reach it and reports a surplus copy. The old leader trims one copy and the new leader trims a different one, leaving the chunk at RF − 1. |
| Repro | `TestDeposedLeaderCannotTrim` against the code before `47eecaf`: it failed for 4 of 4 seeds. |
| Root cause | A trim is a delete, but it was decided and sent from the leader's in-memory location map, which needs no quorum. Two leaders overlap for up to an election timeout, so two trims of one chunk could be in flight from different terms. |
| Fix | Every trim is logged as a TrimIntent and sent only after it commits; at most one trim per chunk is pending in the log; TrimDone clears it. A cut-off leader cannot commit, so it cannot trim (ADR-0020). Commit `47eecaf`. |
| Regression test | `TestDeposedLeaderCannotTrim` (4 seeds): no trim while cut off, one trim by the new leader after the election. |

## 19. A follower kept a phantom location after TrimDone

| | |
|---|---|
| Symptom | `chaos --seed=395 --metas=3`: the trim-safety watch caught a trim that left a chunk with 2 intact copies on running nodes. |
| Repro | `go run ./tools/task chaos --seed=395 --metas=3` before `2fc4980`. |
| Root cause | Locations are soft state: each peer learns them from block reports. A follower missed the report of the trimmed node's delete, so it still counted that copy after TrimDone committed. Once that follower became leader, it saw RF + 1 copies and trimmed a real one. |
| Fix | Applying TrimDone drops its targets from every peer's location map: the log, not a report, says those copies are gone. Commit `2fc4980`. |
| Regression test | `TestNewLeaderTrustsLoggedTrimDone`: node→follower links are blocked while the leader trims, then the leader is killed. Also the per-delete trim-safety invariant (`trimwatch.go`) in every chaos run. |

## 20. A stale trim was resent after another holder died

| | |
|---|---|
| Symptom | `chaos --seed=1153` (also 1932, 2037, 2352, 2853, found by a sweep from seed 1001): the trim watch saw a trim leave a chunk at RF − 1. |
| Repro | `go run ./tools/task chaos --seed=1153` before `8ba54f4`. |
| Root cause | A trim is authorized when it is logged, but its delete can go out much later. The leader resends a pending trim when its victim returns. Here the victim was down; while it was away another holder died, inside the repair delay, so nothing had replaced that copy yet. On the victim's return the resend deleted one of only RF copies left. The bug dates from `47eecaf`; the new trim watch exposed it. |
| Fix | The leader rechecks before the first send and before every resend: the delete goes out only while RF other alive, reported, non-leaving copies remain with no GC or trim pending on them. Otherwise the trim stays logged until repair has caught up. Commit `8ba54f4`. |
| Regression test | `TestStaleTrimWaitsForRepair` (seed 4): it reproduces the sequence and asserts the trim completes only after repair. |

## 21. Real-mode no-repair check counted balance moves (harness)

| | |
|---|---|
| Symptom | `transient-blip-no-repair` failed with 12 repair copies for a 15 s blip, in the first real-mode run after balancing landed. |
| Repro | `go run ./tools/task chaos --mode=real --short` before `fc4c43e`. |
| Root cause | The scheduler's completed-copies counter covers every class: repair, drain and balance. After `kill-node-restores-rf`, the balancer was still evening out the restored node; those moves landed in the next scenario and were blamed on the blip. |
| Fix | Cluster health reports drain copies and balance moves separately (`repair_evacuated`, `repair_moved`), and the runner counts only repair copies. Commit `fc4c43e`. |
| Regression test | `chaos.TestLeaderDeltasSkipMoves`; the short suite runs in CI. |

## 22. A retried drain overtook the undrain after it (harness)

| | |
|---|---|
| Symptom | While the sim chaos faults for drain and undrain were being written (`302512f`), seeds 84, 136, 323 and 777 ended with a node still draining and chunks over-replicated for good. |
| Repro | Those seeds with admin commands sent independently, each retrying on its own timer. |
| Root cause | A drain sent during an election waits and retries. The undrain that followed found a leader at once and committed first, then the retried drain committed over it. The command log was right; the client reordered the commands. |
| Fix | The sim sends one node's admin commands one at a time, in call order (`Cluster.AdminAsync`). The compose target and the CLI were never affected: they wait for each call. Never committed. |
| Regression test | Every chaos run with membership faults: the node must end active and every chunk at exactly RF. |
