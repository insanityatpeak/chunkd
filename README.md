# chunkd

[![ci](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml/badge.svg)](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml)
[![pages](https://github.com/insanityatpeak/chunkd/actions/workflows/pages.yml/badge.svg)](https://insanityatpeak.github.io/chunkd/)

A fault-tolerant distributed file store in Go, in the style of GFS and HDFS. Files are split into 4 MiB content-addressed chunks, each stored on 3 nodes in different racks, and verified end to end by the client.

The same core code runs in two modes:

- `real`: separate processes over gRPC, chunks on disk, metadata in a WAL with bbolt snapshots, wall clock.
- `sim`: one process, in-memory network with loss, duplication, delay and partitions, fake clock, seeded RNG. Every run replays from its seed. It compiles to WebAssembly and runs in the browser.

**Live demo:** https://insanityatpeak.github.io/chunkd/ (upload a file, see which node holds each chunk, crash a node, download and verify the SHA-256 in your browser). "Run scenario: kill node-3" plays a failure at up to 50× speed: suspect, dead, re-replication back to 3 copies, then trimming when the node returns.

## Run it

```
docker compose up -d --build --wait          # 3 metadata servers, 5 storage nodes over 3 racks, gateway on :8080
go run ./cmd/chunkd put ./photo.jpg /photos/photo.jpg
go run ./cmd/chunkd stat /photos/photo.jpg   # version, SHA-256, replicas per chunk
go run ./cmd/chunkd get /photos/photo.jpg ./copy.jpg -expect-sha256 <hash>
go run ./cmd/chunkd cluster
go run ./tools/task e2e                      # 20 MiB round trip through the gateway
go run ./tools/task chaos --seeds=500        # 500 seeded fault schedules in the sim
go run ./tools/task chaos --seed=63 -v       # replay one, with its schedule and logs
go run ./tools/task chaos --mode=real --short   # kill, blip and freeze containers of the compose cluster
```

The dashboard works against this cluster too: open the live demo with `?gateway=http://localhost:8080`.

`docker compose --profile small up` runs 1 metadata server and 3 nodes.

## How it works

| | |
|---|---|
| Upload | `BeginUpload` (compare-and-swap on the version, placement per chunk) → client sends each chunk to 3 nodes in parallel → `CommitUpload` once at least 2 replicas per chunk are reported by the nodes. The version is invisible until that single commit. |
| Download | Metadata returns chunk IDs and live replicas; the client fetches each chunk, checks its SHA-256 against the ID, and falls through to the next replica on mismatch or failure; then checks the file's SHA-256. |
| Metadata | Ops appended to a CRC-framed WAL with fsync, snapshotted to bbolt every 1000 ops. Chunk locations are not stored: nodes report them. |
| Storage | One file per chunk at `ab/cd/<sha256>`; temp file, fsync, rename, fsync directory. |
| Failure detection | Heartbeats each second. Suspect after 3 s of silence (still readable, no new replicas), dead after 10 s, back to alive after 3 on-time beats. A stalled metadata server kills nobody ([ADR-0010](docs/adr/0010-failure-detector.md)). |
| Repair | 20 s after a death, chunks below 3 copies are copied, fewest copies first, 8 at a time, at most 40 MiB/s. The target pulls from a surviving replica and verifies the hash. Extra copies from a returning node are trimmed ([ADR-0011](docs/adr/0011-re-replication.md)). |
| Slow replicas | The client scores nodes by read latency and sends a second read after the p95 (20–500 ms) ([ADR-0012](docs/adr/0012-async-caller-and-hedged-reads.md)). |

Design decisions are in [docs/adr](docs/adr); bugs found by the tests in [docs/bugs-found.md](docs/bugs-found.md); current state in [docs/STATUS.md](docs/STATUS.md); the protocol with sequence diagrams in [docs/design.md](docs/design.md).

## Known limitations

| Limitation | Why it is acceptable now | Plan |
|---|---|---|
| One metadata server; if it is down, nothing is readable or writable | Data on nodes is untouched; the server recovers from its WAL and asks nodes for locations | Raft replication (Phase 5) |
| Abandoned pending uploads and deleted files' chunks are never reclaimed | Refcounts are recorded; nothing reads them yet | GC (Phase 4) |
| No authentication; plaintext gRPC and HTTP | Runs on a private Docker network | mTLS in the transport |
| Upload size must be known up front | Placement is computed per chunk at `BeginUpload` | HDFS-style `addBlock` per chunk |
| Placement ignores existing copies of a chunk; identical chunks in one upload can get extra replicas | Correct, just wasteful | Dedup-aware placement (Phase 4) |
| With unbalanced racks, the smallest rack holds a replica of every chunk | Rack spread deliberately outranks load (ADR-0008) | Balanced racks, or capacity-weighted placement |
| A lost incremental block report delays a commit until the next full report (up to 30 s) | The client retries commit for 45 s | Acknowledged incremental reports |
| Directory fsync is a no-op on Windows | Production target is Linux; NTFS journals renames | None |
| Real-mode chaos has no message-level faults (loss, duplication, delay) | The sim covers them in 500 seeds per push; real mode covers process faults (kill, pause, wipe) | `tc netem` in each container, which needs `NET_ADMIN` |
| Repair targets ignore the surviving replicas' racks; a repaired chunk can end with two copies in one rack | Upload placement still spreads racks, and trims keep spread when a node returns | Rack-aware target choice, as HDFS's `BlockPlacementPolicy` does with existing replicas as input |
| A repair copy's store write runs on the node's event loop | One chunk is at most 4 MiB, a few ms of disk | Move the write to the concurrent chunk path, as client writes already are |
| Placement does not avoid slow nodes; only reads route around them | Hedged reads bound the read cost; writes need 2 of 3 | Feed client latency reports into placement (HDFS slow-node detection) |
| A trim can race a client write that dedups against the trimmed chunk: the commit may count a copy that is deleted a moment later | The chunk keeps at least RF confirmed copies before the trim, and repair tops it up on the next report or scan | Pin chunks of pending uploads against trims (Phase 4 GC) |
| Dedup'd writes can leave chunks over-replicated until the next block report | Correct, just extra copies, trimmed on the next report | Dedup-aware placement (Phase 4) |

## License

MIT
