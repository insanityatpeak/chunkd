# Design questions and answers

Fifteen questions about chunkd, each answered from the code, the tests and the measurements in this repo. Paths are relative to the repo root; `bugs-found #N` is an entry in [bugs-found.md](bugs-found.md); the design behind each is in [design.md](design.md) and the ADRs.

1. [Why are chunk locations not stored durably?](#1-why-are-chunk-locations-not-stored-durably)
2. [Walk through a write when things fail](#2-walk-through-a-write-when-things-fail)
3. [How do you know no acknowledged data is lost?](#3-how-do-you-know-no-acknowledged-data-is-lost)
4. [What does linearizability mean here, and how did you check it?](#4-what-does-linearizability-mean-here-and-how-did-you-check-it)
5. [Explain the GC race](#5-explain-the-gc-race)
6. [Why delay and throttle repair?](#6-why-delay-and-throttle-repair)
7. [Replication or erasure coding?](#7-replication-or-erasure-coding)
8. [What breaks at 100× scale?](#8-what-breaks-at-100-scale)
9. [What was the hardest bug?](#9-what-was-the-hardest-bug)
10. [What would you do differently?](#10-what-would-you-do-differently)
11. [The leader froze for 30 seconds and woke up. What stops it corrupting things?](#11-the-leader-froze-for-30-seconds-and-woke-up-what-stops-it-corrupting-things)
12. [How does a quota hold under concurrent uploads?](#12-how-does-a-quota-hold-under-concurrent-uploads)
13. [What happens to a resumable upload when the leader fails over?](#13-what-happens-to-a-resumable-upload-when-the-leader-fails-over)
14. [How do you test consensus deterministically, and what does the simulator miss?](#14-how-do-you-test-consensus-deterministically-and-what-does-the-simulator-miss)
15. [Why fixed-size chunks?](#15-why-fixed-size-chunks)

## 1. Why are chunk locations not stored durably?

A durable location map is a second copy of facts the nodes already hold, and it goes stale the moment a disk dies, is wiped, or a node restarts. The node is the only party that knows what is on its disk, so the node is the source of truth. Nodes report what they hold: a full block report every 30 s and an incremental one after each chunk stored. The leader keeps the result as soft state.

Durable would cost a log entry for every chunk on every report, through Raft, to record something wrong a second later. Soft state costs one thing: after a leader change or a metadata restart the new leader knows no locations until reports arrive. The heartbeat ack asks for a full report (`NeedFullReport`, `internal/core/meta/server.go`), which takes under a second at 1 s heartbeats. Meanwhile a read finds no replicas and the client retries (ADR-0006, ADR-0019). GFS and HDFS make the same choice.

The cost shows up as bugs, and I found them. A full report replaces locations wholesale, so one overtaken by an incremental report drops a new chunk, and one that overtakes a delete report resurrects a deleted copy (bugs-found #7, `meta.TestClusterReportOrdering`). The fix is sequence numbers taken under a lock on the node. A follower that missed a delete report kept a phantom location and, as the new leader, trimmed a real copy (bugs-found #19, chaos seed 395 with 3 metas); applying a logged `TrimDone` now drops the location on every peer.

## 2. Walk through a write when things fail

`Put` of a 12 MiB file (3 chunks), one storage node dies after the second chunk.

1. `BeginUpload(path, expected version, size, request ID)`. The leader checks the version, places every chunk on three nodes in different racks, and writes the placement into one log entry. A retry after a lost answer carries the same request ID and gets the same upload back (`TestBeginRequestIDIsIdempotent`).
2. For each chunk the client hashes it and sends `ClaimChunks`; this is logged before any byte is written. A chunk already stored with 2 reported copies answers `present` and is skipped.
3. The client sends the chunk to all three replicas in parallel. A replica that fails is retried once. Fewer than 2 acks aborts the upload and nothing becomes visible. One dead node out of three is a failed replica, not a failed write.
4. `CommitUpload(ids, file hash)`. The leader commits only if every chunk is claimed by this upload and has at least 2 live copies that nodes themselves reported. If a report is late it answers retry and the client backs off up to 45 s. The commit is one log entry: the version becomes visible and refcounts rise together.
5. If the commit response is lost, the client repeats it and gets the same version, because the version records the upload that created it (bugs-found #2; `meta.TestCommitAndDeleteAreIdempotent`, `e2e.TestUploadsUnderMessageLoss` over 20 seeds with 5% loss and 5% duplication).
6. A chunk committed at 2 of 3 is topped up by repair after a 10 s upload grace (bugs-found #9 is the bug where it was topped up at once).

If the client dies at any step, the upload is invisible. Its lease expires after 6 epochs and GC collects its chunks. `TestCommitRequiresMinReplicas` checks that an upload with one reachable node fails loudly and stays invisible.

## 3. How do you know no acknowledged data is lost?

Three layers, none of them a statement in prose.

- **The sim harness records what was acknowledged.** `internal/sim/cluster/harness.go` keeps, per path, the set of hashes the path may legitimately hold given every acknowledged and ambiguous operation. A failed put or delete may still have applied, so it widens the set; a success narrows it. `AssertInvariants` reads every path back and compares SHA-256 against that set, and checks that every chunk of every committed file has at least `min_replicas` live reported copies.
- **Chaos.** `go run ./tools/task chaos` runs seeded fault schedules (kills, freezes, slow nodes, partitions, bit rot, node adds and drains, leader faults, EC puts) and checks the invariants after each: acknowledged data reads back, RF is restored within the bound, no read ever returns bytes never written to that path, GC settles with no orphans. CI runs 1,000 seeds per push, 500 more with `--metas=3`, and 500 with `--ec`. Locally I ran 3,000 membership-fault seeds and 1,500 with the metadata group, all green. Any failure prints its seed and replays exactly.
- **Real mode.** The same claims against containers: kill, pause, wipe, and kill the metadata leader (`go run ./tools/task chaos --mode=real --short`, 7 scenarios).

What it does not cover: three simultaneous independent failures within one repair window lose data by design (RF 3), and correlated corruption is in Known limitations. Durability past a crash also depends on fsync being honest, which the sim cannot test (see question 14).

## 4. What does linearizability mean here, and how did you check it?

Every metadata operation (put, delete, stat, list) appears to take effect atomically at one instant between its call and its return, in an order every client agrees on. A stat after an acknowledged put must show that version or a later one; two puts racing on one expected version cannot both win.

It holds because writes go through the Raft log and reads use read-index: the leader confirms with a quorum that it is still leader and waits to apply the commit index it was told (ADR-0019, `internal/core/consensus`). Chunk locations inside a stat are soft and can be stale. That is not a violation of the file's contents, because chunks are immutable and hash-checked: a stale location costs a hedged read, never wrong bytes.

I check it with porcupine. `internal/history` records every client call with its call and return times. `internal/history/check/check.go` models a path as `{live, ver, hash}` where `ver` counts every version ever created and never repeats, so a successful put or delete must return exactly `ver+1`. A call that failed ambiguously (timeout, lost answer) is treated as taking effect at any time after its call, which is where the interesting bugs live. 500 sim histories per push run under leader kills, freezes and partitions, and every real-mode scenario records and checks its own. When a history fails, CI uploads the visualization.

Two limits, both in Known limitations: sim clients block, so their concurrency comes only from ambiguous operations and a pinned client on the minority side; and a list is checked per path, not as one atomic snapshot.

## 5. Explain the GC race

Chunks are shared (dedup), so "unreferenced" is not "garbage": an upload may be writing a chunk that no committed version references yet.

```
t0  upload U claims X (logged)        X marked
t1  U writes X to N1, N2
t2  sweep: X marked, skipped
t3  U commits: refcount(X) = 1
```

Without the claim, the sweep at t2 sees X unreferenced and deletes it, and t3 publishes a version with a missing chunk. So the order is fixed: the claim is logged before any write, and a chunk is marked while a version references it or a pending upload claims it. A chunk must also sit unmarked for a 60 s grace before it is a candidate, and an upload's lease (150 s minimum) outlives the grace plus two sweeps. I got the lease wrong first: with 4 epochs the guaranteed idle time was 90 s, shorter than the 120 s grace-plus-sweeps, and the stalled upload expired through its lease (bugs-found #10, seed 1 of `TestChaosGCDuringSlowUpload`).

The second race is a delete overtaken by a rewrite: a delete authorized at report sequence 40 is lost; a new upload claims X and writes it at sequence 44; the delete is resent. The node refuses a delete if the chunk was written after the delete's fence, under the same per-chunk lock writes take. `TestChaosNodeReturnsWithDeletedChunks` builds this and fails if the fence is removed. With Raft, deletes are also decided in the log first (`GCIntent`), so a deposed leader cannot create one (ADR-0016, ADR-0019).

## 6. Why delay and throttle repair?

**Delay.** Most node outages are restarts. If a node holding terabytes is declared dead and immediately rebuilt, the cluster copies everything for a machine that returns in a minute, then trims it all again. The detector marks a node suspect at 3 s and dead at 10 s, and repair waits another 20 s. The price is 30 s at reduced redundancy. A chunk down to its last copy skips the delay, because losing it is data loss. `TestTransientBlipNoRepair` makes zero copies for 2, 15 and 25 s outages; the same scenario runs against containers.

**Throttle.** Repair reads and writes the same disks and NICs as clients, at the moment the cluster has fewer replicas to spread reads over. Unbounded repair turns one dead node into a cluster-wide slowdown, and slow nodes start to look dead. The scheduler runs at most 8 copies at once, 2 per source and 2 per target, within a 40 MiB/s token bucket (`internal/core/repair`).

The cost is repair time, and I measured it ([benchmarks/results.md](benchmarks/results.md)). 240 MiB lost at a 5 MiB/s cap took 47.5 s past the 30 s floor, 12 s at 20 MiB/s. At an 80 MiB/s cap it still took 12.75 s, because the 8 copy slots, not the cap, bound it. What I did not measure is the effect on foreground reads: the sim shares no capacity between messages, so repair cannot slow a read there, and I have not run the real-hardware comparison. I would not claim the throttle protects p99 until I had.

## 7. Replication or erasure coding?

It is a per-upload choice (`put -redundancy=ec-4+2`). Numbers from [benchmarks/ec.md](benchmarks/ec.md), the same 20 files on 7 nodes:

| | `replicate:3` | `ec:4+2` |
|---|---|---|
| Stored per logical byte | 3.00× | 1.50× |
| On the dead node | 39.0 MiB | 20.0 MiB |
| Repair read | 39.0 MiB | 80.1 MiB |
| Read per byte lost | 1× | 4× |

EC halves the bytes and survives any two losses like three copies, but a lost byte costs four to rebuild (the target reads four shards and decodes), a normal read touches four nodes, and a write needs six distinct nodes and commits at 5. Replication is better for small files (a 1-byte file still takes 6 shards of 34 bytes), hot data and small clusters; EC is better for large cold data. I do not move data between the two in the background; that is on the roadmap.

Why per-chunk striping and not stripes across chunks: dedup. A chunk shared by two files inside a cross-chunk stripe would stop being protected when one file is collected. Inside a chunk, one logical ID owns one stripe, so claims, refcounts and GC work unchanged, and a shard is an ordinary block with a copy target of 1 (ADR-0022, ADR-0023). Why commit at 5 of 6 shards: 4 is enough to decode but leaves no margin for a loss before repair; 6 lets one slow node block every write; 5 is the same margin as 2 of 3 copies (`TestECCommitNeedsFiveShards`).

## 8. What breaks at 100× scale?

In the order I would expect to hit them, with the measurement where I have one.

1. **The quota check.** A limited Begin scans every file in every namespace, inside the apply loop: 0.7 us without a quota, 825 us at 10,000 files and 29 ms at 100,000 ([benchmarks/results.md](benchmarks/results.md)). At 29 ms every other metadata write waits. A running counter per namespace fixes it.
2. **One metadata group.** The whole namespace is in RAM and every commit goes through one log. ADR-0006 and ADR-0014 say the answer is to shard the namespace by path prefix; CAS stays per path, so it stays inside one group.
3. **Chunk metadata and block reports.** At 4 MiB, 100 TB is about 4 GB of chunk records and full reports of hundreds of MB. Larger chunks for large files, split and rate-limited reports (ADR-0005).
4. **The repair cap.** 40 MiB/s cluster-wide would take 29 hours for one 4 TB node. The limit has to be per node, with every survivor sourcing and sinking a share, so repair time falls as the cluster grows (ADR-0011).
5. **The full scans.** The 30 s repair scan and the GC sweep are O(chunks) on one server (about 1 s per 10 M chunks for GC); they become incremental indexes.
6. **Placement and balance.** One failure-domain level and equal disks; a real cluster needs a tree of domains and capacity weights, or a CRUSH-like function.
7. **Client egress.** The client sends 3× the file. A pipeline or a first-replica fan-out inside the cluster.

## 9. What was the hardest bug?

Reordered block reports (bugs-found #7). CI's real-mode `transient-blip-no-repair` failed with 6 repair copies, all started one second after the scenario's uploads, one per fresh chunk, with no error logged anywhere. The sim never showed it.

Root cause: a full report replaced the node's locations wholesale. On a node, chunk puts run on concurrent handlers while the full report is listed on the event loop, and nothing ordered them. A report listed just before a put finished, delivered after that put's incremental report, dropped the new chunk, and repair copied it again. The mirror case was the dangerous one: a trim's delete report overtaken by an older full report re-added a copy that no longer existed, so repair could count, or trim against, a phantom replica.

The fix is ordering by sequence, not by arrival. A put or delete takes its number after changing the store, under a shared lock; a full report takes its number and lists the store under the exclusive lock, so a full report numbered S reflects exactly the changes below S. The leader keeps the latest change number per chunk since the last full report, so a full report cannot drop a newer add or re-add a newer delete. Regression: `meta.TestClusterReportOrdering` tries seven arrival orders. It is the HDFS incremental-versus-full block report race.

The lesson I took from the same area: an end-state checker misses transient safety violations. A retried trim removed the wrong replica and the chunk was at 2 real copies for 18 s, but chaos checks the settled state and the dip healed (bugs-found #6). That is why every trim delete in every chaos run is now checked against RF at the moment it is sent (`internal/sim/cluster/trimwatch.go`), and that check found #19 and #20.

## 10. What would you do differently?

- **Quota accounting.** I chose a scan inside the log entry because it is exact and short, then measured 29 ms at 100,000 files. A per-namespace counter in the state machine, rebuilt from the snapshot, is the same exactness at O(1).
- **Writes are not hedged.** A gray node adds 2 s to every message and moves put p50 from 188 ms to 4,112 ms, while hedging holds get p99 to 235 ms. A write waits for the slowest of its replicas. Placement ignores slow nodes. Both are listed; I would feed client latency back into placement.
- **Content-defined chunking.** Fixed 4 MiB chunks give nothing after an insert (a 1-byte insert at offset 0 stores 20 MiB again). I picked them for simple placement and repair, and I would switch to FastCDC with a bounded size range if the workload were edits to large files.
- **One chunk at a time on upload.** A window of chunks in flight, claims batched, would take the claim round trip off the critical path (ADR-0015).
- **Measure contention on real hardware earlier.** The sim made me confident about policy and says nothing about disks and NICs. I would have set up a multi-host run before writing the claim about foreground impact.
- **Single-writer sim clients.** Concurrency in the linearizability histories comes only from ambiguous operations. Several concurrent simulated clients per seed would test more.

## 11. The leader froze for 30 seconds and woke up. What stops it corrupting things?

Everything the old leader can do needs a quorum or a term.

- **Writes:** a proposal must be appended by a majority. The group has elected a new leader meanwhile; the old leader's uncommitted entries are overwritten. The proposal fails with `unavailable`, and the client retries an idempotent request.
- **Reads:** a read-index read needs a quorum to confirm leadership; it fails with `NotLeader`. A leader that heard from no majority within the election timeout reports itself not ready, even before it learns of the higher term (`core/consensus`).
- **Commands to nodes:** every replicate, delete and verify carries the leader's term. A node keeps the highest term it has seen on its disk, learns it from the new leader's heartbeat acks, and refuses lower ones (the `Fenced` counter). `node.TestStaleTermCommandsAreRefused`, `TestFenceSurvivesRestart`.
- **GC and trims:** deletes are decided in the log first (`GCIntent`, `TrimIntent`) and sent only after the intent commits, which a deposed leader cannot do. `TestDeposedLeaderSendsNoGCDeletes`, `TestDeposedLeaderCannotTrim` (bugs-found #18: before the fix, a cut-off leader and the new one each trimmed a different copy and left RF − 1).

I did not rely on a lease or on clocks: a frozen process breaks lease reads exactly when it matters, which is why reads use read-index instead. The sim reproduces a pause as a partition, and the real-mode suite can kill the leader but not freeze it. `TestStaleLeaderCannotCommit` runs on 4 seeds in the sim and in the consensus package.

## 12. How does a quota hold under concurrent uploads?

The check and the reservation are one log entry. `BeginUploadOp.quota` carries the limit; `State.Validate` computes the namespace's live bytes plus every pending upload's size plus the new size and refuses with `CodeQuota` if it passes the limit. `Apply` runs `Validate` again on every peer, so all agree. A pending upload therefore reserves its whole size from Begin: two uploads that each fit alone cannot together pass the quota, because the second Begin sees the first's reservation. Commit swaps the reservation for a live version; abort and lease expiry release it; delete frees the bytes; undelete is checked as a new write. A repeated Begin with the same request ID returns the pending upload and is not charged twice.

Tests: `TestQuotaReservesAtBegin` (`internal/core/meta`), `TestQuotaFreedByDeleteAndChargedByUndelete`, `TestQuotaSurvivesSnapshot`, `gateway.TestQuotaRefusesBeforeBytesAreSent`, `TestQuotaRefusalReadsNoBytes` (a refused upload reads zero bytes of the source).

What breaks without it: a counter kept outside the log, or a check at commit, lets N uploads that each fit pass the limit together. A check outside the log is also racy across two gateways. What it costs: the scan, 29 ms at 100,000 files (question 10). What it does not count: retired versions kept for undelete, stored bytes (EC against replication), and cross-tenant dedup. Those are in ADR-0025 and Known limitations.

## 13. What happens to a resumable upload when the leader fails over?

Nothing is lost, because progress is not held by any process. It is the upload's claims in the log, replicated to every peer. A client that asks `UploadStatus` (a read-index read) gets the leading run of chunks that are claimed and stored; the new leader has the same log, so it reports the same offset (ADR-0024).

`TestResumeAcrossLeaderFailover`: a 1 MiB upload is appended to 512 KiB, the metadata leader is killed, a second client asks the new leader and gets offset 512 KiB, appends the rest and commits version 1; the peers then agree byte for byte. `TestResumeAfterClientCrash` does the same after a client crash, for replicated and EC uploads, and checks the second process sends only the 6 MiB left. `TestPutResumeAfterCutOff` covers the CLI over real disks.

What is sent after a cut-off? I measured it ([benchmarks/results.md](benchmarks/results.md)): the bytes that reach the nodes are the same with or without resume, because a restarted `Put` claims every chunk and stored chunks are skipped. Resume saves the request body, the local re-read and the re-hash: after a cut at 90% of a 32 MiB file the client sends 4 MiB instead of 32. Limits: appends are whole chunks (a cut mid-chunk resends that chunk), the upload must resume within its lease (3 minutes at the demo setting), and a split upload cannot be checked against its declared hash before commit; a wrong hash commits a version every read refuses.

## 14. How do you test consensus deterministically, and what does the simulator miss?

Every source of time, randomness and I/O is injected: `iface.Clock`, `Rand`, `Transport`, `MetaStore`. The sim runs one goroutine with a fake clock and a seeded RNG, so a seed is a complete replay (`TestSameSeedSameTrace`, `TestSameSeedSameTraceMetaGroup`), in Go and in the browser (`TestScenarioGolden` against Playwright). The one leak was etcd raft's election timeout, drawn from `crypto/rand`; I removed it by never ticking followers and campaigning from a seeded timer in `core/consensus` (ADR-0017). It is the same package in the sim, in WASM and in the real processes (ADR-0018), so a bug found on a seed is a bug in what ships. Building it that way turned up four bugs, three in the consensus layer and the sim and one in the checker (bugs-found #13 to #16).

What it misses (full list in [design.md](design.md#what-sim-mode-cannot-capture)):

- **Shared capacity.** Messages do not contend. Repair cannot slow a read in the sim.
- **fsync.** Torn or lost fsyncs at the consensus layer; the WAL's torn-tail recovery is tested on its own.
- **Disks, kernel, TCP.** Latency distributions, half-open connections. Bugs-found #24 (a recreated container's old IP routed one node's commands to another) only showed in compose.
- **Clocks.** All peers share one clock, so a design that needed synchronized clocks would pass. Nothing here does, but the sim could not tell.
- **A paused process.** I reproduce it with a partition, not a pause.
- **Scale.** One goroutine.

Real mode covers the transport, disks and process faults, and is rerunnable but not replayable.

## 15. Why fixed-size chunks?

Fixed 4 MiB chunks keep placement, repair, verification and the block report simple: a chunk is a unit with a size you can compute. Content addressing gives dedup and end-to-end verification for free. The cost is that an insert shifts every later boundary: in [benchmarks/dedup.md](benchmarks/dedup.md), ten in-place 64-byte edits to a 20 MiB base store 220 MiB of logical data as 60 MiB (3.67×), while a single byte inserted at offset 0 stores the whole 20 MiB again (1.00×).

Content-defined chunking (Rabin fingerprints, FastCDC as restic and Borg use) cuts at content-derived boundaries so an insert changes one or two chunks. It makes chunk sizes variable, so placement, balance bands and the EC shard size (which is `ceil(size/4)`) all have to handle a range, and it needs a bounded min and max size. I decided against it for this workload: whole-file uploads of mostly-unchanged or in-place-edited files dedup well. It is marked `SIMPLIFIED:` in ADR-0015 and listed in Known limitations, and `chunkd diff` has the same limit: it compares chunks by index, so an insertion reads as every later chunk changed (ADR-0026).
