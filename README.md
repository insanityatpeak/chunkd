# chunkd

A fault-tolerant distributed file store in Go, in the style of GFS and HDFS.

[![ci](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml/badge.svg)](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![live demo](https://img.shields.io/badge/live%20demo-in%20your%20browser-0f766e)](https://insanityatpeak.github.io/chunkd/)

![A storage node is killed; the cluster detects it, re-replicates its chunks, verifies a download, and trims the extra copies when the node returns](docs/assets/demo.gif)

**[Try it in your browser →](https://insanityatpeak.github.io/chunkd/)** A simulated cluster running the same core code, compiled to WebAssembly. Kill nodes, rot a disk, partition the network, and watch it recover. Nothing to install.

chunkd splits files into 4 MiB chunks named by their SHA-256 and keeps three copies of each, on nodes in different racks. A metadata group of three Raft peers tracks versions and where every copy lives; killing, freezing or cutting off its leader loses no acknowledged write. Nodes can join, or be drained and retired, while the cluster serves: a balancer moves copies toward rack-feasible targets, and no chunk drops below three copies on the way. Identical chunks are stored once, every file keeps its recent versions, a delete can be undone for a retention window, and garbage collection reclaims what nothing references. It is built to stay correct while things break. A node can crash, freeze, turn slow or come back with an empty disk, and a disk can silently rot bits. Throughout, acknowledged data stays readable, every read is verified end to end, and lost copies are rebuilt within a stated time bound, at a capped rate, without copying anything for a node that only rebooted. Every claim below is a test that runs in CI: 1,000 seeded chaos schedules on every push in a deterministic simulator that replays any failure from its seed, 500 more over the metadata group with leader faults and every client history checked for linearizability, plus a real-mode suite that kills, freezes and corrupts Docker containers, the metadata leader included.

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
go run ./tools/task chaos --mode=real --short          # kill, blip, rot and freeze real containers, and the metadata leader
docker compose exec node-2 chunkd debug corrupt -n 3   # rot 3 chunk files; the scrubber finds them
```

Shared dashboard links replay exactly: [kill a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=kill-node&speed=10), [silent bit rot](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=corrupt-chunk&speed=10), [lose a rack](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=rack-loss&speed=10), [slow node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=slow-node&speed=10), [kill the metadata leader](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=kill-leader&speed=10), [add a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=add-node&speed=10), [drain a node](https://insanityatpeak.github.io/chunkd/?seed=7&scenario=drain&speed=10).

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
| A seed replays the same run, in Go and in the browser | `TestSameSeedSameTrace`, `TestScenarioGolden` + Playwright replay | go, web |
| The demo runs with only Docker installed | `docker compose run --rm demo` | compose |

Bugs these tests caught, with root causes and fixes: [docs/bugs-found.md](docs/bugs-found.md).

## Known limitations

| Limitation | Why it is acceptable now | Plan |
|---|---|---|
| No authentication; plaintext gRPC and HTTP | Runs on a private Docker network | mTLS in the transport |
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
| Retention is demo-scale: overwritten and deleted versions are restorable for 60–90 s | Keeps the demo and the tests fast; the window is two config values | Days, as S3 lifecycle rules keep noncurrent versions |
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
| `cmd/` | `chunkd` (CLI), `chunkd-meta`, `chunkd-node`, `chunkd-gateway`, `chunkd-demo`, `chunkd-chaos`, `chunkd-wasm` |
| `web/` | Preact + TypeScript dashboard, one UI for the simulation and the real cluster |
| `deploy/` | Dockerfile, compose file, the GIF's `vhs` tape |
| `docs/` | ADRs, design, status, bugs found |

Every command runs through `go run ./tools/task <name>`, the same on Windows, Linux and CI; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Roadmap

- [x] Chunked, replicated upload and download with end-to-end verification
- [x] Failure detection, throttled re-replication, hedged reads
- [x] Integrity: verify on read, scrubbing, quarantine, corruption repair
- [x] High-availability metadata with Raft (replicated log, leader election, term fencing, linearizable histories)
- [x] Deduplication, versioned files with compare-and-swap, undelete, garbage collection
- [x] Rebalancing when nodes join, drain and decommission
- [ ] Erasure coding for cold data

## License

MIT
