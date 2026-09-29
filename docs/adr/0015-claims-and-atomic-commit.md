# 0015. Chunk claims: dedup, the GC mark for uploads, atomic commit

Status: accepted
Date: 2026-09-29

## Context

Three needs meet in the upload path:

1. **Dedup.** Chunk IDs are content hashes, so identical chunks already share a record and a refcount. The client should not send a chunk the cluster holds.
2. **GC safety.** A chunk written by an upload that has not committed is referenced by nothing durable. A sweep in that window would delete it, and the commit would then publish a version with a missing chunk.
3. **Atomic commit.** A version must become visible all at once, and only when every chunk is durable.

Placement is decided at Begin, before the client has hashed anything, so the chunk IDs are not known then.

## Options considered

| Option | For | Against |
|---|---|---|
| Chunk IDs in Begin | One round trip | The client must read the input twice (hash, then send). The gateway would have to spool every HTTP body to disk |
| **Claim each chunk before writing it** (`meta.claim`, a logged op) | Streaming. The claim is durable before any copy exists, so it marks the chunk for GC for the upload's whole life | One extra round trip per chunk (about 1 ms against about 34 ms to send 4 MiB at 1 Gbit/s) |
| Write first, register at commit | No extra round trip | The window in need 2 stays open for the whole upload |

## Decision

- `ClaimChunks(upload, [(index, id)])` is logged, then answered with `present[i]`: the chunk has at least MinReplicas copies on alive nodes with no GC delete in flight. A present chunk is not sent.
- A claim renews the upload's lease (ADR-0016).
- Commit is one logged op. It checks that the upload still exists (its lease has not run out), that every chunk is claimed by this upload with the same ID, and that the live version is still the expected one (ADR-0014). Before logging it, the server checks that every chunk has MinReplicas live copies, counting only reported copies on alive nodes with no GC delete pending. Applying the op publishes the version and increments refcounts in one step. Until then Stat and List do not see it.
- `BeginUploadOp.claims` is set by servers that require claims, so logs written before claims existed replay unchanged.

`SIMPLIFIED:` dedup is cluster-wide. An uploader can learn whether content exists by watching which chunks are skipped (the upload-to-probe side channel). Dropbox moved to per-user dedup after this was shown in 2011; a multi-tenant chunkd would key chunk IDs with a per-tenant salt, giving up cross-tenant savings.

`SIMPLIFIED:` fixed-size chunks. An insert shifts every later boundary (see `docs/benchmarks/dedup.md`). restic and Borg use content-defined chunking.

## Consequences

- `TestDedupSavings`: 220 MiB logical stored as 60 MiB distinct, measured on the nodes' disks.
- `TestCommitWithoutClaimRejected`, `TestGCSparesInflightUpload`, `TestChaosGCDuringSlowUpload`.
- One more RPC to keep idempotent: re-claiming the same chunk is a no-op, and claiming a different ID at the same index is rejected.

## At 100× scale

Claim traffic grows with chunks, not bytes. Batching a window of claims into one RPC (the request already takes a list) and overlapping it with the previous chunk's transfer takes the round trip off the critical path. Claim state lives only on pending uploads and is released at commit or abort, so it stays proportional to uploads in flight.
