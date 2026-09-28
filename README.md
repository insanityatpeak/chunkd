# chunkd

[![ci](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml/badge.svg)](https://github.com/insanityatpeak/chunkd/actions/workflows/ci.yml)
[![pages](https://github.com/insanityatpeak/chunkd/actions/workflows/pages.yml/badge.svg)](https://insanityatpeak.github.io/chunkd/)

A fault-tolerant distributed file store in Go, in the style of GFS and HDFS. Files are split into 4 MiB content-addressed chunks, each stored on 3 nodes in different racks, and verified end to end by the client.

The same core code runs in two modes:

- `real`: separate processes over gRPC, chunks on disk, metadata in a WAL with bbolt snapshots, wall clock.
- `sim`: one process, in-memory network with loss, duplication, delay and partitions, fake clock, seeded RNG. Every run replays from its seed. It compiles to WebAssembly and runs in the browser.

**Live demo:** https://insanityatpeak.github.io/chunkd/ (upload a file, see which node holds each chunk, crash a node, download and verify the SHA-256 in your browser).

## Run it

```
docker compose up -d --build --wait          # 3 metadata servers, 5 storage nodes over 3 racks, gateway on :8080
go run ./cmd/chunkd put ./photo.jpg /photos/photo.jpg
go run ./cmd/chunkd stat /photos/photo.jpg   # version, SHA-256, replicas per chunk
go run ./cmd/chunkd get /photos/photo.jpg ./copy.jpg -expect-sha256 <hash>
go run ./cmd/chunkd cluster
go run ./tools/task e2e                      # 20 MiB round trip through the gateway
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

Design decisions are in [docs/adr](docs/adr); bugs found by the tests in [docs/bugs-found.md](docs/bugs-found.md); current state in [docs/STATUS.md](docs/STATUS.md); the protocol with sequence diagrams in [docs/design.md](docs/design.md).

## Known limitations

| Limitation | Why it is acceptable now | Plan |
|---|---|---|
| One metadata server; if it is down, nothing is readable or writable | Data on nodes is untouched; the server recovers from its WAL and asks nodes for locations | Raft replication (Phase 5) |
| No re-replication: a chunk that loses a replica stays at 2 copies | Commit requires 2 of 3; reads fall through to any intact replica | Failure detection and repair (Phase 2) |
| Abandoned pending uploads and deleted files' chunks are never reclaimed | Refcounts are recorded; nothing reads them yet | GC (Phase 4) |
| No authentication; plaintext gRPC and HTTP | Runs on a private Docker network | mTLS in the transport |
| Upload size must be known up front | Placement is computed per chunk at `BeginUpload` | HDFS-style `addBlock` per chunk |
| Placement ignores existing copies of a chunk; identical chunks in one upload can get extra replicas | Correct, just wasteful | Dedup-aware placement (Phase 4) |
| With unbalanced racks, the smallest rack holds a replica of every chunk | Rack spread deliberately outranks load (ADR-0008) | Balanced racks, or capacity-weighted placement |
| A lost incremental block report delays a commit until the next full report (up to 30 s) | The client retries commit for 45 s | Acknowledged incremental reports |
| Directory fsync is a no-op on Windows | Production target is Linux; NTFS journals renames | None |

## License

MIT
