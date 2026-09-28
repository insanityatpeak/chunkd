# 0005. Chunk size: 4 MiB

Status: accepted
Date: 2026-09-29

## Context

Files are split into fixed-size, content-addressed chunks (SHA-256 of the bytes). The chunk is the unit of placement, replication, repair, verification and, from Phase 4, deduplication. The target is a small cluster (3 to 10 nodes, terabytes), plus a browser demo that uploads files up to 50 MiB.

## Options considered

Per 1 TB stored, about 150 bytes of metadata per chunk (ID, size, refcount, three locations):

| | 1 MiB | **4 MiB** | 64 MiB (GFS) |
|---|---|---|---|
| Chunks per TB | 1,048,576 | 262,144 | 16,384 |
| Metadata RAM per TB | ~157 MB | ~39 MB | ~2.4 MB |
| Parallelism, 37 MiB file | 37 chunks | 10 chunks | 1 chunk on one replica set |
| Repair after losing a 1 TB node | 1M small copies from many sources | 262k copies from many sources | 16k large copies; a few sources hot |
| One chunk at 1 Gbit/s | ~8 ms | ~34 ms | ~540 ms |
| Tail latency from one slow replica | Small | Small | A slow replica stalls a whole 64 MiB unit |
| Small-file overhead | Last chunk is short (no padding); cost is per-file metadata | Same | Same |
| RPC and hashing overhead per byte | Highest | Low | Lowest |
| Demo, 50 MiB upload | 50 chunks | 13 chunks, readable in the grid | 1 chunk |

## Decision

4 MiB default, configurable per metadata server (`-chunk-size`). The size is recorded in each file version, so changing the default never breaks existing files. The last chunk is short, never padded.

GFS chose 64 MiB so a single master could keep the metadata for petabytes in RAM. At this scale 39 MB per TB is negligible, and smaller chunks win on parallelism, repair spread and tail latency.

## Consequences

- A 4 MiB chunk plus framing fits gRPC's 4 MiB default message limit only if split. Chunk transfers use `CallStream` with 1 MiB frames (ADR-0009).
- Whole-chunk SHA-256 is cheap enough at 4 MiB that nodes and clients verify every transfer; no per-block checksum sidecars (HDFS keeps a CRC per 512 bytes).
- More chunks means larger block reports: 262k chunk IDs per TB is 8 MB per full report.

## At 100× scale

At 100 TB, chunk metadata is about 4 GB of RAM on one server and full block reports reach hundreds of MB. Either raise the chunk size for large files (GFS/HDFS use 64-128 MiB) or shard metadata. Block reports would need splitting per volume and rate limiting, as HDFS does.
