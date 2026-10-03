# chunkd

A fault-tolerant distributed file store in Go, in the style of GFS and HDFS.

[![ci](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml/badge.svg)](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![live demo](https://img.shields.io/badge/live%20demo-in%20your%20browser-0f766e)](https://insanityatpeak.github.io/chunkd/)

![A storage node is killed; the cluster detects it, re-replicates its chunks, verifies a download, and trims the extra copies when the node returns](docs/assets/demo.gif)

The same story in the browser dashboard, running the real core code compiled to WebAssembly (seed 7, 10×; [MP4](docs/assets/dashboard.mp4)):

![The dashboard replays a node kill: the card turns red, repair copies appear in the events list, and the replication bar returns to three copies](docs/assets/dashboard.gif)

**[Try it in your browser →](https://insanityatpeak.github.io/chunkd/)** A simulated cluster running the same core code, compiled to WebAssembly. Kill nodes, rot a disk, partition the network, and watch it recover. Nothing to install.

chunkd splits files into 4 MiB chunks named by their SHA-256 and keeps three copies of each, on nodes in different racks. A metadata group of three Raft peers tracks versions and where every copy lives; killing, freezing or cutting off its leader loses no acknowledged write. Nodes can join, or be drained and retired, while the cluster serves: a balancer moves copies toward rack-feasible targets, and no chunk drops below three copies on the way. An upload can instead store each chunk as a Reed-Solomon stripe, 4 data and 2 parity shards on six nodes: half the bytes of three copies, still readable with any two shards gone, and a lost shard is rebuilt from four others. Identical chunks are stored once, every file keeps its recent versions, a delete can be undone for a retention window, and garbage collection reclaims what nothing references. It is built to stay correct while things break. A node can crash, freeze, turn slow or come back with an empty disk, and a disk can silently rot bits. Throughout, acknowledged data stays readable, every read is verified end to end, and lost copies are rebuilt within a stated time bound, at a capped rate, without copying anything for a node that only rebooted. Every claim below is a test that runs in CI: 1,000 seeded chaos schedules on every push in a deterministic simulator that replays any failure from its seed, 500 more over the metadata group with leader faults and every client history checked for linearizability, plus a real-mode suite that kills, freezes and corrupts Docker containers, the metadata leader included.

## Architecture

![Clients talk HTTP to the gateway; the gateway asks the metadata group (three Raft peers, one leader) where chunks go and moves the bytes to and from storage nodes directly; nodes heartbeat and report to every metadata peer, and the leader sends repair and trim commands](docs/assets/architecture.svg)

The metadata group is never on the data path. Chunk locations are not stored: nodes report what they hold, as in GFS and HDFS. Design decisions are in [docs/adr](docs/adr), the protocol with sequence diagrams in [docs/design.md](docs/design.md).

## Quickstart

```
git clone https://github.com/insanityatpeak/chunkd && cd chunkd
docker compose up -d --build --wait
```

Open **http://localhost:8080**: the same dashboard, connected to the real cluster (3 metadata peers, 5 storage nodes on 3 racks, a gateway). On a small machine, `docker compose --profile small up -d --build --wait` runs 1 metadata server and 3 nodes.

From a Go toolchain:

```
go run ./cmd/chunkd put ./photo.jpg /photos/photo.jpg
go run ./cmd/chunkd put -redundancy=ec-4+2 ./scan.tif /scans/scan.tif   # 1.5x: 4 data + 2 parity shards on 6 nodes
go run ./cmd/chunkd put -resume ./big.iso /isos/big.iso   # after a cut-off: continues from the chunks the log holds
go run ./cmd/chunkd log /photos/photo.jpg         # retained versions and the epoch each expires
go run ./cmd/chunkd diff /photos/photo.jpg 1 2    # chunks that differ, and the bytes a rewrite sends
go run ./cmd/chunkd restore /photos/photo.jpg 1   # make version 1 the live one again
go run ./cmd/chunkd retain /photos/photo.jpg 40   # keep its retired versions 40 GC epochs
go run ./cmd/chunkd keygen -name alice -quota 1073741824   # an API key and its entry for the gateway's -keys file
go run ./cmd/chunkd stat /photos/photo.jpg        # version, SHA-256, replicas per chunk
go run ./cmd/chunkd get /photos/photo.jpg ./copy.jpg
go run ./cmd/chunkd cluster status                # detector state, replication, repair
go run ./cmd/chunkd node drain node-4             # move its copies away; it takes no new chunks
go run ./cmd/chunkd node decommission -wait 5m node-4   # once every chunk has 3 copies elsewhere
go run ./cmd/chunkd node undrain node-4           # back in service; the balancer evens out the bytes
docker compose --profile full --profile extra up -d node-6   # a sixth node, empty, on rack r3
```

## Chaos demo

```
docker compose run --rm demo
```

Uploads 20 MiB, kills the node holding the most copies, and prints the failure detector's and the repair scheduler's timeline. Then it downloads with the node still down, checks the SHA-256, restarts the node, and watches the extra copies get trimmed. The demo container kills and starts containers through the Docker socket, which gives it root-equivalent access to Docker on your machine: it is opt-in and local. `go run ./tools/task demo` does the same from a clone.

More ways to break it:

```
go run ./tools/task chaos --seeds=1000                 # 1,000 random fault schedules in the simulator
go run ./tools/task chaos --seed=63 -v                 # replay one, with its schedule and logs
go run ./tools/task chaos --seeds=500 --metas=3        # leader kills, freezes, partitions; histories checked
go run ./tools/task chaos --seeds=500 --ec             # 7 nodes, about half the puts erasure-coded
go run ./tools/task chaos --mode=real --short          # kill, blip, rot and freeze real containers, and the metadata leader
docker compose exec node-2 chunkd debug corrupt -n 3   # rot 3 chunk files; the scrubber finds them
```

Shared dashboard links replay exactly: [kill a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=kill-node&speed=10), [silent bit rot](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=corrupt-chunk&speed=10), [lose a rack](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=rack-loss&speed=10), [slow node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=slow-node&speed=10), [kill the metadata leader](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=kill-leader&speed=10), [add a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=add-node&speed=10), [drain a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=drain&speed=10), [erasure coding](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=ec&speed=10).

## What's proven

| Claim | Test | CI job |
|---|---|---|
| Round trips from 0 B to 37 MiB, with chunk and file hashes checked | `e2e.TestRoundTrip` (sim and real processes) | go |
| An upload is invisible until it commits, and commits only with 2 reported copies | `TestUncommittedInvisible`, `TestCommitRequiresMinReplicas` | go |
| Commit and delete are safe to repeat when responses are lost or duplicated | `TestCommitAndDeleteAreIdempotent`, `TestUploadsUnderMessageLoss` | go |
| A crash mid-write never loses an acknowledged metadata op | `TestTornTailRecovers`, `TestCorruptionBeforeTailIsAnError` | go |
| A dead node's chunks are back at 3 copies within the stated bound | `TestKillNodeRestoresRF` (sim), `kill-node-restores-rf` (containers) | go, compose |
| A node that only reboots costs zero copies | `TestTransientBlipNoRepair`, `transient-blip-no-repair` | go, compose |
| Repair never exceeds its concurrency limits and byte rate | `TestRepairThrottle` | go |
| A slow replica does not slow reads | `TestSlowNodeHedgedRead` (hedged p99 under 200 ms against 4–10 s) | go |
| A corrupt copy is caught on read, quarantined and replaced | `TestCorruptChunkDetectedOnRead`, `corrupt-replicas` | go, compose |
| Rot that nobody reads is found within one scrub pass | `TestScrubberFindsCorruption` | go |
| When every copy is bad, the read fails loudly instead of returning bad bytes | `TestAllReplicasCorrupt` | go |
| Block reports reordered by the network never drop or resurrect a copy | `TestClusterReportOrdering` | go |
| Under random faults, node additions and drains, acknowledged data stays readable, RF returns within the bound, rot is found, and no read returns bytes never written | 1,000 chaos seeds per push (20,000 on demand) | go |
| Identical chunks are stored once: the same base with small edits ×10 stores 220 MiB as 60 MiB | `TestDedupSavings` ([docs/benchmarks/dedup.md](docs/benchmarks/dedup.md)) | go |
| Two commits against the same version: exactly one wins, the other gets a conflict | `TestCASConflict`, `TestChaosConcurrentWriters` | go |
| GC never collects a chunk an in-flight upload needs, even one stalled past a GC cycle | `TestGCSparesInflightUpload`, `TestChaosGCDuringSlowUpload`, `TestChaosDeleteWhileUploadingSameChunk` | go |
| After deletes, retention and two sweeps, the chunks on disk are exactly the referenced ones | `TestGCNoOrphanLeak`; every chaos seed ends with GC settled and `AssertCollected` | go |
| A GC delete never removes a copy written after the decision | `node.TestGCDeleteFence`, `TestChaosNodeReturnsWithDeletedChunks` | go |
| Refcounts and claims always equal a recount from the versions | `TestReconcile`, checked after every chaos seed | go |
| Killing the metadata leader between chunk writes and commit loses no acknowledged upload: the client finishes it on the new leader | `TestKillLeaderMidUpload` (sim), `TestKillLeaderMidUploadReal` (processes), `kill-meta-leader` (containers) | go, compose |
| A paused leader that wakes up deposed cannot commit, and nodes refuse its lower-term commands | `TestStaleLeaderCannotCommit` (sim and consensus) | go |
| A leader cut off with a minority rejects writes; the majority elects a new one | `TestMinorityPartitionRejectsWrites` | go |
| Client histories are linearizable under leader faults | 500 chaos seeds with `--metas=3` per push, plus every real-mode scenario, checked with porcupine | go, compose |
| The metadata log stays bounded, and a lagging follower catches up from a snapshot | `TestLogBoundedUnder10kOps`, `TestSnapshotBoundsLogAndCatchesUp` | go |
| A node joining moves at most ½·L1 plus one chunk per node (½·L1 is the least any plan can move), and every chunk stays at RF throughout | `TestAddNodeConverges`, `TestRebalanceBenchmark` ([docs/benchmarks/rebalance.md](docs/benchmarks/rebalance.md)), `add-node` (containers) | go, compose |
| Draining a node never takes a chunk below RF, even when another node dies mid-drain; decommission is refused until every chunk has RF copies elsewhere | `TestDrainNeverDropsRF`, `drain-node` (containers) | go, compose |
| Only a committed trim deletes a copy: a deposed leader cannot trim, a new leader trusts the logged TrimDone, and a trim waits while its chunk is short elsewhere | `TestDeposedLeaderCannotTrim`, `TestNewLeaderTrustsLoggedTrimDone`, `TestStaleTrimWaitsForRepair`; every chaos run checks RF at each trim delete | go |
| An erasure-coded file reads with any 2 of a stripe's 6 shards gone, decoding from parity, and fails loudly with 3 gone | `TestECReadSurvivesTwoLostNodesNotThree`, `TestECReadDecodesAroundARottedShard`, `ec.TestJoinSurvivesAnyTwoLosses`; every size round-trips as stripes over real gRPC and disk (`e2e.TestRoundTripEC`) | go |
| A dead node's shards are rebuilt, each from 4 others, onto nodes outside the stripe within the repair bound; EC stores 1.50× against 3.00×, and repair reads 4 bytes per byte rebuilt | `TestECRepairRebuildsLostShards`, `TestECBenchmark` ([docs/benchmarks/ec.md](docs/benchmarks/ec.md)) | go |
| Under random faults with about half the puts erasure-coded, the same invariants hold, counted per shard | 500 chaos seeds with `--ec` per push | go |
| A resumed upload sends no stored chunk again, survives a client crash and a metadata leader failover, and refuses a wrong offset | `TestResumeAfterClientCrash`, `TestResumeAcrossLeaderFailover`, `TestResumeRefusals`, `TestPutResumeAfterCutOff` (CLI over real disks), gateway tus tests | go |
| A namespace quota holds under concurrent uploads, and a refused upload reads no bytes of the source | `TestQuotaReservesAtBegin`, `TestQuotaRefusalReadsNoBytes`, `TestQuotaRefusesBeforeBytesAreSent` | go |
| Keys see only their namespace and their own uploads | `TestAuthScopesPathsToNamespace`, `TestAuthUploadsBelongToTheirKey` | go |
| Retention is per path and survives a snapshot; diff compares two versions chunk by chunk | `TestRetentionPerPath`, `TestRetentionSurvivesSnapshot`, `TestDiffManifests`, `TestDiffAgainstACluster` | go |
| Hedged reads cut a fresh client's get p99 from 4,141 ms to 235 ms with one gray node, and cost nothing without a fault | `go run ./tools/task bench` ([docs/benchmarks/results.md](docs/benchmarks/results.md)); `TestSlowNodeHedgedRead` asserts the bound in CI | go |
| A seed replays the same run, in Go and in the browser | `TestSameSeedSameTrace`, `TestScenarioGolden` + Playwright replay | go, web |
| The demo runs with only Docker installed | `docker compose run --rm demo` | compose |

Bugs these tests caught, with root causes and fixes: [docs/bugs-found.md](docs/bugs-found.md).

## Benchmarks

`go run ./tools/task bench` re-runs everything and rewrites [docs/benchmarks](docs/benchmarks/README.md): CSVs, SVG charts and a generated summary. Sim numbers are exact for a seed. Real numbers are one client, sequential, a 5-node cluster in one process on loopback with real disks (AMD Ryzen 7 7840HS, 16 threads, 15 GiB, Go 1.27, Windows 11, otherwise idle); expect about 10% on large files and more on small ones.

| Measurement | Result | Tier |
|---|---|---|
| Put, RF 3, 16 / 128 / 1024 MiB | 52 / 60 / 58 MiB/s (every byte is written three times) | real |
| Get, 16 / 128 / 1024 MiB | 182 / 187 / 157 MiB/s, every chunk and the file hash verified | real |
| 256 KiB files, p50 / p99 | put 42 / 328 ms, get 2.6 / 11.8 ms, stat 0.5 / 13 ms | real |
| Get p99, one gray node (2 s added), fresh client | 4,141 ms unhedged, 235 ms hedged; identical with no fault | sim |
| Time to RF 3 after a node kill, 240 MiB lost | 77.5 s at a 5 MiB/s cap, 42 s at 20 MiB/s: a 30 s floor (detector and delay) plus copies | sim |
| Storage, replicate:3 against ec:4+2 | 3.00x against 1.50x; repair reads 4 bytes per byte rebuilt ([ec.md](docs/benchmarks/ec.md)) | sim |
| Dedup, base plus 10 small edits | 220 MiB logical in 60 MiB ([dedup.md](docs/benchmarks/dedup.md)) | sim |
| Cut-off 32 MiB upload at 90% | resume re-sends a 4 MiB body; a restart re-sends 32 MiB (the same bytes reach the nodes, since stored chunks are skipped) | sim |
| Quota check per Begin, 100 / 10,000 / 100,000 files | 1.3 / 1.8 / 0.9 us, against 0.7 to 1.2 us without a quota (a scan cost 4 / 825 / 29,379 us before ADR-0028) | state machine |

Charts and every table: [docs/benchmarks/results.md](docs/benchmarks/results.md). What the numbers do and do not show: [docs/benchmarks/README.md](docs/benchmarks/README.md).

## Known limitations

| Limitation | Why it is acceptable now | Plan |
|---|---|---|
| Authentication is optional static bearer keys at the gateway (`-keys`); with none the gateway is open. gRPC between processes, and HTTP, are plaintext, and node chunk calls are unauthenticated | Runs on a private Docker network; put TLS in front of a keyed gateway (ADR-0025) | mTLS in the transport, signed requests, a logged key table with rotation |
| A key owns one path segment and a byte quota counted in logical bytes of live versions and pending uploads; retired versions are not counted, an overwrite counts old and new until it commits, and a limited begin reads a per-namespace counter, about 1 us at any size (ADR-0028) | The check is one log entry, so concurrent uploads cannot overshoot (ADR-0025); the cost is measured in [docs/benchmarks](docs/benchmarks/results.md) | Stored-byte accounting, S3-style IAM policies |
| An upload ID is a sequence number, and an interrupted upload lives one lease (3 minutes at the demo setting) | Dedup makes a restarted upload cost claims, not bytes (ADR-0024) | A longer per-upload lease charged to its owner |
| A split resumable upload cannot be checked against its declared hash before commit | A wrong hash commits a version every read refuses, never one that serves wrong bytes (ADR-0024) | A composite checksum over chunk IDs, as S3 multipart does |
| Upload size must be known up front | Placement is computed per chunk at `BeginUpload` | HDFS-style `addBlock` per chunk |
| A chunk the cluster already holds is skipped only once it has 2 reported copies; identical chunks written close together can each get 3 | Correct, just extra copies, trimmed on the next report | Count claimed in-flight writes as pending copies |
| With unbalanced racks, the smallest rack holds a replica of every chunk | Rack spread deliberately outranks load (ADR-0008) | Balanced racks, or capacity-weighted placement |
| A lost incremental block report delays a commit until the next full report (up to 30 s) | The client retries commit for 45 s | Acknowledged incremental reports |
| Every copy of a chunk rotting within one scrub window loses it; the read fails with `corrupt` | Needs three independent failures inside one pass interval plus repair time (about q³; ADR-0013) | Shorter passes or erasure coding with parity checks |
| Correlated corruption (a firmware or driver bug on several disks at once) defeats the independence the math assumes | Rack spread puts copies on different hardware; nothing more | Mixed disk models and firmware per replica set, as large operators do |
| Corruption in the client's memory before it hashes the data is stored as valid | The hash is computed from what the client holds | End-to-end checksums from the data's source (the application) |
| Quarantined files are never removed; GC sweeps only the copies nodes report | Kept for forensics; small next to live data | Nodes age quarantine out after a set number of GC epochs |
| The scrubber runs on the node's event loop, one chunk per step | A 4 MiB hash takes a few ms, far below the 3 s suspect timeout | A scanner thread per volume, as HDFS does |
| Client re-check hints are not rate-limited | Each costs the node one re-read, and only the node's check removes a copy | Per-client hint budget |
| A metadata peer whose WAL has a corrupt entry refuses to start, and nothing rebuilds it automatically | The other two peers keep a quorum and serve; the entry is intact on them | Remove the peer and add a fresh one with a membership change, which catches up from a snapshot (single-server changes exist in the consensus layer, with no CLI yet) |
| Directory fsync is a no-op on Windows | Production target is Linux; NTFS journals renames | None |
| Real-mode chaos has no message-level faults (loss, duplication, delay) | The sim covers them in 1,000 seeds per push; real mode covers process faults (kill, pause, wipe) | `tc netem` in each container, which needs `NET_ADMIN` |
| Repair targets ignore the surviving replicas' racks; a repaired chunk can end with two copies in one rack | Upload placement still spreads racks, and trims keep spread when a node returns | Rack-aware target choice, as HDFS's `BlockPlacementPolicy` does with existing replicas as input |
| A repair copy's store write runs on the node's event loop | One chunk is at most 4 MiB, a few ms of disk | Move the write to the concurrent chunk path, as client writes already are |
| Placement does not avoid slow nodes; only reads route around them | Hedged reads bound the read cost; writes need 2 of 3 | Feed client latency reports into placement (HDFS slow-node detection) |
| A trim can race a client write that dedups against the trimmed chunk: the commit may count a copy that is deleted a moment later | The chunk keeps at least RF confirmed copies before the trim, and repair tops it up on the next report or scan | Skip trims of chunks a pending upload has claimed |
| Dedup is cluster-wide: an uploader can learn whether content exists by watching which chunks are skipped | Single tenant; the demo holds no one else's data | Per-tenant salt in chunk IDs, as Dropbox moved to after 2011 (ADR-0015) |
| Fixed-size chunks: an insert shifts every later boundary, so dedup finds nothing after it | In-place edits dedup well (3.67× in the benchmark) | Content-defined chunking (FastCDC), as restic and Borg use |
| Retention is demo-scale by default: retired versions are restorable for 60–90 s; `chunkd retain <path> <epochs>` sets it per exact path | Keeps the demo and the tests fast; no prefix rules or version-count limit (ADR-0026) | Lifecycle rules by prefix, age and count, as S3 has |
| `chunkd diff` compares chunks by index, and the dashboard shows no diff | Needs only two manifests; an insertion reads as every later chunk changed (ADR-0026) | Content-defined chunking, and a diff view on the chunk grid |
| A GC delete delayed by more than one epoch, arriving after a newer send was answered, could remove an uncounted copy | gRPC streams deliver in order, so it needs a reconnect in between, and repair restores the copy | Per-send delete IDs the node remembers, so a stale send is refused |
| In the sim, a frozen metadata peer's timers keep firing; only its messages are held | It still wakes with a stale view and a lower term, which is the case the stale-leader tests need | Stop the peer's clock as well, as SIGSTOP does in real mode |
| Compose can kill and restart the metadata leader, but not freeze or partition it | Docker's network model cuts a container from every peer at once; the sim covers both faults on 500 seeds per push | Per-link rules (`iptables` in each container, which needs `NET_ADMIN`) |
| Sim clients block, so a history's concurrency comes only from ambiguous operations (timeouts) and the pinned client | Ambiguous operations are where linearizability bugs live: a write that may or may not have committed | Several concurrent simulated clients per seed |
| The history checker accepts a put conflict in any state: it does not check that the expected version really was stale, so a wrongly refused put would pass | A refused put leaves no trace, so it cannot corrupt what later reads see; `TestCASConflict` checks that exactly one of two racing commits wins | Record the expected version and model a conflict as a read showing a different one |
| Minority-side clients exist only in the sim, and only while a leader is cut off | They are the case that matters: a client next to a deposed leader must never see a stale read or a commit | Pinned clients in real mode, once compose can partition a peer |
| A client call to a metadata leader that died silently waits out the 10 s call timeout before trying the next peer | A killed process on a live host refuses connections at once; only a silent host costs the timeout | A shorter per-attempt timeout for metadata calls, or a hedged call to the next peer |
| A leader refuses drain, undrain and decommission for a node it has not heard from since it started | Node state is soft and rebuilt from reports; the node's heartbeat arrives within a second of it being up; the chaos targets retry, and from the CLI it is a second try | Log node registration, so a new leader knows every node the cluster ever admitted |
| Balance targets are bytes, not capacity: every node is assumed to have the same disk | The compose and sim nodes are identical | Report capacity in heartbeats and weight targets by it, as HDFS's balancer does with utilization percent |
| The band is at least two of the largest chunk, so a small cluster stops far from its target (node-6 at 12 of 20 MiB on the demo set) | It is what stops a chunk bouncing between two nodes near their targets; at 10% of 100 MiB the floor no longer matters | None at this scale |
| No balancing while membership is unsettled: a suspect node, or a dead one inside the repair delay, pauses all moves | A node that returns with its data would otherwise have copies moved only to be moved back | Plan around a node that is likely gone, as Ceph does after `mon_osd_down_out_interval` |
| Erasure coding is chosen per upload; nothing moves cold data from copies to stripes later | The choice is the uploader's, and both forms read the same | Background tiering by age, as Facebook's f4 and HDFS storage policies do |
| The same bytes uploaded replicated and erasure-coded are stored twice: dedup works within a policy | A stripe's record is keyed by its own ID, so one record never mixes copies and shards (ADR-0022) | Convert in place when a second policy references a chunk |
| An erasure-coded upload needs 6 placeable nodes, and the begin is refused with fewer | Two shards on one node would turn one failure into two | Wider clusters, or a narrower code for small ones |
| A small file still takes 6 shards (a 1-byte file stores 6 × 34 bytes) | Negligible at 4 MiB chunks | Keep small objects replicated or inline, as S3 and MinIO do |
| The balancer leaves shards where they are; a joining node gets shards only from rebuilds | A shard is a quarter of a chunk, and upload placement already spreads stripes | A planner that keeps a stripe on distinct nodes, as Ceph's balancer moves EC placement groups |
| Writes are not hedged: with one gray node a put waits for the slowest of its replicas (put p50 188 ms to 4,112 ms with 2 s added to one node) | Reads are hedged and a client orders replicas by its own latency score; writes need 2 of 3 acks (ADR-0007) | Feed client latency into placement, or hedge the third replica's write |
| The effect of repair on foreground reads is not measured | The sim shares no capacity between messages, so it is flat by construction; the repair cap itself is measured (peak rate against each limit) | A multi-host run with real disks and NICs |
| Benchmarks use one client, sequential operations, and a 5-node cluster in one process on loopback | They show what the code costs on one machine, not what a cluster delivers ([docs/benchmarks](docs/benchmarks/README.md)) | Many clients across hosts, warp-style |
| No real-mode (compose) run of `put -resume`; the dashboard shows version history and restore but no diff | The sim covers resume across a leader failover, and `TestPutResumeAfterCutOff` covers the CLI over real disks | A compose scenario for resume; a diff view on the chunk grid |
| No real-mode chaos scenario uses erasure coding | Compose runs 5 nodes and a stripe needs 6; the sim runs 500 EC seeds per push, and `e2e.TestRoundTripEC` covers the real transport and disks | A compose profile with 7 nodes and an EC scenario in the short suite |
| The client encodes stripes, and the cluster trusts the shard IDs it claims | A wrong claim fails its read-time check against the stripe ID, as a wrong chunk ID already does | Encode on a server, as Ceph's primary OSD does |
| A decommissioned node stays in the metadata log; starting one again with the same ID needs an undrain | It takes nothing while decommissioned, which is the safe default; the compose add-node path undrains it | Remove a decommissioned node from the log once its last copy is trimmed |

## Project layout

| Path | Contents |
|---|---|
| `internal/core/` | Deterministic logic: chunking, placement, metadata state machine, failure detector, repair scheduler, scrubber, storage node. Imports no network, disk, clock or randomness. |
| `internal/iface/` | The seams: transport, clock, block store, metadata log, randomness, with conformance suites every implementation must pass |
| `internal/sim/` | Fake clock, seeded network with loss, duplication, delay, partitions and crashes, in-memory stores, the cluster harness |
| `internal/real/` | gRPC transport, disk block store, WAL + bbolt metadata log, HTTP gateway, process runtime |
| `internal/chaos/` | Seeded fault schedules with invariant checks, in the sim and against docker compose |
| `internal/client/` | The client library the CLI, gateway, tests and browser all use |
| `cmd/` | `chunkd` (CLI), `chunkd-meta`, `chunkd-node`, `chunkd-gateway`, `chunkd-demo`, `chunkd-chaos`, `chunkd-bench`, `chunkd-wasm` |
| `internal/bench/` | CSV files, hardware probe and SVG charts shared by the benchmarks |
| `web/` | Preact + TypeScript dashboard, one UI for the simulation and the real cluster |
| `deploy/` | Dockerfile, compose file, the GIF's `vhs` tape |
| `docs/` | [Design](docs/design.md), [ADRs](docs/adr/README.md), [benchmarks](docs/benchmarks/README.md), [design questions](docs/interview.md), [what the chaos harness found](docs/writeup-chaos-harness.md), [bugs found](docs/bugs-found.md), [status](docs/STATUS.md) |

Every command runs through `go run ./tools/task <name>`, the same on Windows, Linux and CI; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Roadmap

- [x] Chunked, replicated upload and download with end-to-end verification
- [x] Failure detection, throttled re-replication, hedged reads
- [x] Integrity: verify on read, scrubbing, quarantine, corruption repair
- [x] High-availability metadata with Raft (replicated log, leader election, term fencing, linearizable histories)
- [x] Deduplication, versioned files with compare-and-swap, undelete, garbage collection
- [x] Rebalancing when nodes join, drain and decommission
- [x] Erasure coding, chosen per upload (Reed-Solomon 4+2)
- [x] Resumable uploads, API keys with byte quotas, version diff and per-path retention, CLI polish
- [x] Reproducible benchmarks, a design document, interview notes and a write-up
- [ ] Moving cold data from copies to erasure coding in the background
- [ ] A per-namespace usage counter for quotas, content-defined chunking, hedged writes

## License

MIT
