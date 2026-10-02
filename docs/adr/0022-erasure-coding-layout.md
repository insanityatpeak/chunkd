# 0022. Erasure coding: RS(4,2) stripes inside each chunk

Status: accepted
Date: 2026-10-02

## Context

Three full copies cost 3× the logical bytes and survive the loss of any 2 copies. A Reed-Solomon code with 4 data and 2 parity shards survives the same 2 losses at 1.5×. The price is paid on the read and repair paths: a missing data shard must be decoded from 4 others, and rebuilding one shard reads 4 shards. The store already rests on content-addressed blocks, chunk-level dedup, logged claims and refcounts for GC, logged trims, and per-block repair. An EC design that bypasses any of these reopens problems earlier phases solved with tests.

`klauspost/reedsolomon` v1.14.2 builds for `GOOS=js GOARCH=wasm` (pure-Go path there, SIMD natively), so the browser simulation runs the same codec. Native encode runs at about 6 GB/s, so CPU is not the constraint.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Stripe layout | Across 4 consecutive chunks (HDFS-RAID, Facebook f4); **inside each chunk, 4 MiB → 4 × 1 MiB cells + 2 parity** (MinIO, HDFS-EC striped cells) | Inside each chunk. A stripe across chunks breaks under dedup: a chunk shared with another file sits in one stripe, and once the file that owns the stripe is collected, its parity no longer protects the shared chunk. Within a chunk, dedup, claims and refcounts stay keyed by one ID |
| Shard identity | Content hash of the payload; a derived name (chunk ID + index) with a separate checksum; **content hash of a 33-byte header (index, stripe ID) plus the payload** | Header + payload. Two zero quarters of one chunk, or a quarter two chunks share, would collide under a plain payload hash. The header makes every slot's block unique, and the block store's put-time and scrub-time hash checks work unchanged |
| Policy unit | Per bucket (no buckets exist); per path prefix; **per upload** | Per upload, recorded in the Begin op and on the version. A path's next version may choose differently |
| Dedup across policies | Adopt whichever form is stored first; **keep the two forms apart** | Apart. A stripe's record is keyed by its logical ID, `sha256("chunkd-ec-4+2:" ‖ chunk ID)`, never by the chunk ID, so one record never mixes copies and shards. The same bytes uploaded under both policies are stored twice |
| Placement | Best effort, doubling up on a node when short; **6 distinct nodes or refuse** | Refuse. Two shards on one node turn a single failure into a double one. Begin answers `unavailable` with fewer than 6 placeable nodes |
| Commit threshold | All 6 shards; 4; **5 (k + 1)** | 5. Same margin as replication's 2 of 3: one more loss before repair still reads. The 6th is rebuilt by repair |

## Decision

An EC upload splits each chunk into 4 data and 2 parity payloads of `ceil(size / 4)` bytes, wraps each in a header (slot index, stripe ID) and stores each block on its own node, shard i on placement node i. Placement fills racks round-robin, so with 3 racks each holds 2 shards and losing a rack loses at most 2.

```
chunk C (4 MiB) ─ stripe ID L = sha256("chunkd-ec-4+2:" ‖ sha256(C))
        │ RS(4,2)
        ▼
  [0|L|d0] [1|L|d1] [2|L|d2] [3|L|d3] [4|L|p0] [5|L|p1]   block = 33-byte header + 1 MiB
     n1       n2       n3       n4       n5       n6       one block per node, ≤ 2 per rack
```

The client claims `L` with the 6 shard IDs before writing (ADR-0015 unchanged: the claim marks every shard for GC). Commit needs 5 shards reported on alive nodes. The chunk record keeps the 6 shard IDs, and the state derives a shard → (stripe, slot) index from them. Each shard is a block with a copy target of 1: GC marking, fenced deletes, trims, scrub and drain see an ordinary block, and repair rebuilds it instead of copying (ADR-0023).

A read fetches the 4 data shards in one batch. If any is missing, fails or does not verify, the client fetches parity and decodes from any 4; once no untried shard is left, a shard that timed out is asked once more, since a lost message is not a lost shard (bugs-found #25). The decoded chunk is checked against `L`, and the file against its SHA-256. With 3 or more shards gone the read fails with the stripe and its readable count.

## Consequences

| | `replicate:3` | `ec:4+2` |
|---|---|---|
| Stored bytes per logical byte | 3.0× | 1.5× (+33 B per MiB of headers) |
| Losses survived | any 2 copies | any 2 shards |
| Nodes needed | 3 (2 to commit) | 6 (5 to commit) |
| Normal read | 1 copy, hedged | 4 shards in parallel |
| Degraded read | another copy | 4 shards + decode |
| Repair one lost unit | read 1 × 4 MiB | read 4 × 1 MiB, decode |
| Dedup | across replicated uploads | across EC uploads only |

- Repair bandwidth per lost byte is 4× replication's. The 2 parity shards cost 0.5× the data in storage. `docs/benchmarks/ec.md` measures both.
- A small last chunk makes small shards; a 1-byte chunk stores 6 × 34 bytes. Fine at 4 MiB chunks, wasteful for tiny files, which object stores keep replicated or inline (S3 and MinIO inline small objects).
- SIMPLIFIED: the client trusts its own encoding. A client that claims a stripe ID with the wrong shards stores a stripe that fails its read-time check. The same trust already applies to chunk IDs; Ceph's primary OSD encodes on the server side for this reason.
- SIMPLIFIED: shards of one stripe are written in one parallel batch, without HDFS-EC's streamer pipeline.
- Tests: codec tables (any 2 losses decode, 3 fail, every slot rebuilds, equal payloads get distinct IDs), state replay and snapshot with stripes, sim round trips with 2 nodes killed and with 3 killed (fails loudly).

## At 100× scale

Wider codes cut overhead further: RS(10,4) at 1.4× is common (HDFS's RS-10-4, Facebook f4), with repair reading 10 shards per lost one. Azure's LRC(12,2,2) adds local parities so the common single-failure repair reads 6 shards, not 12. Ceph lets each pool choose its plugin and k/m, and places shards with CRUSH across failure domains. At that size, cold data would also move from replicated to EC in the background, as Facebook's f4 and HDFS's storage policies do, rather than being chosen per upload.
