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
