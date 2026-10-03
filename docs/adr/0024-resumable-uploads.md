# 0024. Resumable uploads

Status: accepted
Date: 2026-10-03

## Context

An upload is a pending record in the log (ADR-0015): placement fixed at Begin, chunks claimed one by one, a commit that publishes the version. If the client dies, the record stays until its lease runs out (6 epochs, 3 minutes at the demo setting), then expires and GC collects its chunks. Most of resuming already exists:

- A Begin repeated with its request ID returns the same upload.
- A claim of a stored chunk answers `present`, so the chunk is not sent again.
- Every claim renews the lease.

What is missing:

1. **A way to ask what a pending upload holds.** A new client process cannot know which chunks the old one stored.
2. **A resume path in the client and the CLI.** `chunkd put` should continue a broken upload, not start over.
3. **A resumable protocol at the gateway.** An HTTP body cut off at 3 GiB is lost, and curl and browsers have no way to continue it.
4. **A file hash the gateway can commit with.** Commit carries the whole file's SHA-256, and reads verify it. A client that streams one body computes it as the bytes pass. A gateway that receives the file in pieces, possibly across its own restarts, never sees the whole file.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Where progress lives | In the gateway (a spool directory, as tusd keeps partial uploads); **in the log, as the upload's claims** | The log. The claims already record which chunk IDs belong to the upload, and every peer has them, so any gateway or client can resume after any restart, a leader failover included |
| Gateway protocol | S3 multipart (parts numbered by the client, completed with a list); **tus 1.0 core** (create with a length, append at an offset, ask for the offset) | tus core. One upload URL, an offset the server reports, and existing clients (curl in a loop, tus-js-client). S3 multipart needs the client to track parts and ETags |
| Append granularity | Any byte range (tus allows it); **whole chunks, the last one short** | Whole chunks. An offset is then derived from the log alone: the leading run of chunks that are claimed and stored. A partial chunk would need a spool, as in tusd |
| File hash | Computed by the gateway (impossible across pieces); hash of the chunk IDs (changes what a version's SHA-256 means); **declared at creation and logged with the upload** | Declared. The CLI hashes the local file; a browser hashes with WebCrypto; curl users run `sha256sum`. Commit refuses a different hash, and every read checks the bytes against it |
| Resume after the lease runs out | Longer lease for resumable uploads; **start a new upload** | New upload. Dedup still skips every chunk the cluster holds with 2 copies, so the cost is the claims, not the bytes |

## Decision

```
client                          gateway / client library              metadata leader
  │ POST /uploads/f  Upload-Length: N,                                     │
  │      Upload-SHA256: h ─────────▶ Begin{size N, sha256 h} ─────────────▶│ logged; placement
  │◀── 201  Location: /uploads/42 ──┤                                      │
  │ PATCH /uploads/42  Upload-Offset: 0, chunks 0-2 ──▶ claim, put ×3 ───▶ │
  │ ✗ connection lost                                                      │
  │ HEAD /uploads/42 ──────────────▶ UploadStatus{42} ────────────────────▶│ read-index
  │◀── 200  Upload-Offset: 3·4 MiB ─┤  (leading chunks claimed and stored) │
  │ PATCH /uploads/42  Upload-Offset: 12 MiB, the rest ──▶ claim, put, ... │
  │                                   offset = N: Commit{ids, h} ─────────▶│ checks h, publishes
  │◀── 204  Upload-Offset: N, Chunkd-Version: 1                            │
```

- `meta.upload_status` returns a pending upload's path, size, chunk size, redundancy, declared hash, placement and claims, with `present` for each claimed chunk. It also returns the epoch its lease runs out. For an upload that already committed, it returns the version it created, so a client whose commit response was lost learns the outcome. It is a linearizable read (ADR-0019).
- `BeginUploadOp.sha256`: optional. When set, Commit must carry the same hash. Logs without it replay unchanged.
- The client library: `BeginResumable`, `UploadStatus`, `Append(id, offset, data)`. The reported offset is the leading run of chunks claimed and stored. Stored-ness comes from block reports, which can trail a write by a moment, so Append accepts any chunk boundary up to the end of the claimed run, not only the reported offset. A lower offset resends chunks whose claims already name them, and a stored one is skipped. A chunk whose bytes differ from its claim is refused, as is an offset past the claimed run (`conflict`, as tus answers 409). Append claims and stores each whole chunk (`putChunk` or `putStripe`, the same code as `Put`), and commits once the offset reaches the length.
- `chunkd put` hashes the file, begins with the hash and keeps `{upload id, local path, size, hash}` in the user cache directory until the commit. `chunkd put --resume` re-hashes the file and refuses if it changed. It then continues from the reported offset, or starts over if the upload expired.
- Gateway: `POST /uploads/{path}`, `HEAD` and `PATCH /uploads/{id}` (`Tus-Resumable: 1.0.0`, `Upload-Length`, `Upload-Offset`, `Upload-SHA256`, the same `?redundancy`, `?expected` and `?lww` as `PUT /files`). `PUT /files/{path}` is unchanged.

## Consequences

- Progress survives the client, the gateway and the metadata leader, because it is the log. A resumed upload sends only chunks the cluster does not hold.
- The client must know the file's hash before it starts: one extra read of a local file, or WebCrypto in a browser. An HTTP client streaming data it cannot re-read uses `PUT /files`, as before.
- SIMPLIFIED: nothing checks the declared hash against the bytes when an upload is split over appends, because no process sees the whole file. A single-pass append checks it before committing, and `chunkd put --resume` re-hashes the local file. A client that declares a wrong hash and appends in parts commits a version that every read refuses (`file SHA-256 does not match`), not one that serves wrong bytes. S3 multipart checksums each part and derives a composite whole-object checksum from them; a composite over chunk IDs would close this, at the cost of a second hash per version.
- SIMPLIFIED: appends are whole chunks; tusd accepts any range and spools the partial chunk. A client cut off mid-chunk resends that chunk from its start.
- SIMPLIFIED: an interrupted upload is resumable only within its lease (3 minutes here). S3 keeps multipart parts until a lifecycle rule aborts them; the lease is one config value.
- SIMPLIFIED: an upload ID is a sequence number, so whoever holds it can append. Authorization of uploads comes with item D.
- Tests: state replay with a declared hash, commit refused with another hash, status of pending, committed and expired uploads, resume after a client crash sending no stored chunk again, a wrong offset refused, resume across a leader failover, the gateway protocol over HTTP.

## At 100× scale

Uploads of terabytes would want appends in parallel and out of order, as S3 multipart allows: offsets give way to a set of completed chunk indexes, which the claims already are. The lease would become per upload and long, with GC charging abandoned uploads to their owner's quota (item D). The status read is O(chunks) per call, so a 1 M-chunk upload would page it.
