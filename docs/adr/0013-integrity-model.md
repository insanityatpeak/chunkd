# 0013. Integrity: verify on every read, scrub, quarantine

Status: accepted
Date: 2026-09-29

## Context

Disks return wrong bytes without an error: latent sector errors, misdirected or torn writes, firmware bugs, bit flips in RAM or on the bus. Field studies measure checksum mismatches on a small percentage of disks per year, often several blocks at once. Before this phase:

- Stores rejected a `Put` whose bytes did not hash to the chunk ID, so corruption in transit was never acknowledged.
- Nodes served stored bytes unverified. Clients checked each chunk and fell through to another replica, but told nobody. The bad copy stayed counted as a replica, so the cluster believed it had 3 copies while it had 2 good ones.
- A copy nobody reads was never checked at all. Rot accumulates there until the other copies fail too.

The goal: detect corruption on read and in the background, never hand a client bad bytes, and restore RF from a good copy.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Who verifies a read | Client only; node only; **both** | Both. The node is the only place that can quarantine and report; the client is the only place that sees what arrived (network, gateway) |
| Who may condemn a replica | Any client that saw bad bytes; **only the node's own check** | A client sends a hint and the node re-checks. A buggy client or gateway cannot remove good copies |
| Bad copy on disk | Delete; overwrite in place; **move to `quarantine/`** | Quarantine: kept for forensics, out of reads, lists and usage |
| Background detection | None (reads only); **rate-capped scrubber per node** (HDFS DataBlockScanner, Ceph deep scrub) | Paced to 8 MiB/s, a pass started every 10 minutes |
| Checksum granularity | Per 512 B block with a sidecar (HDFS); **one SHA-256 per chunk** | The chunk ID is its hash, so verification needs no extra metadata; chunks are read whole |

## Decision

- **Read path.** `node.getChunk` hashes before serving. On a mismatch it quarantines, sends a block report with `corrupt_ids`, and answers `CodeCorrupt`. The report goes through the sequence-numbered path (ADR-0011, bugs-found #7), so it is ordered against full reports like any delete. The client falls through to the next replica, as before.
- **Client hint.** If bytes fail the *client's* check after the node's check passed, the path is suspect, not the disk. The client sends one `meta.suspect` hint without retries. The metadata server asks the node to re-check (`node.verify_chunk`), and only the node's check removes a copy.
- **Metadata server.** A corrupt report removes the location and emits a timeline event. `repair.Recheck` then queues the chunk at once. The 20 s repair delay only excuses holders that are dead and may return, and a corrupt copy will not come back. The queue is already ordered by fewest live copies, so a chunk down to one good copy goes first.
- **Repair source.** A copy's target verifies on `Put`, and its source verifies on `getChunk`. A corrupt source quarantines itself and the copy fails and is retried from another holder, so rot is never propagated.
- **Scrubber** (`core/scrub`). Each pass snapshots the node's chunk list, then verifies one chunk per step on the node's clock. Steps are paced so the average rate stays under the cap: after n bytes the next step waits n ÷ rate. It exposes `chunkd_scrub_bytes_total`, `chunkd_scrub_corrupt_total`, `chunkd_scrub_last_pass_seconds` and `chunkd_scrub_pass_progress`. Progress rides on heartbeats to the dashboard.
- **All copies bad.** The read fails with `CodeCorrupt` ("every replica failed verification; the data is lost"), not `unavailable`: retrying cannot help.

### Scrub rate

| | Rate | Full pass |
|---|---|---|
| HDD streaming about 150 MB/s, 5% budget | 7.5 MB/s | 4 TB: about 6 days |
| HDFS DataBlockScanner (default) | 1 MiB/s per volume | 3-week scan period |
| Ceph deep scrub | load-gated | weekly per placement group |
| **chunkd** | **8 MiB/s per node** | **4 TB: about 6 days; a 200 MiB demo node: 25 s** |

The 10-minute pass interval suits a small demo cluster. With 4 TB per node, the rate, not the interval, decides the pass time.

### Probability of losing all copies to rot

A chunk is lost only if all 3 copies rot within one *window*: the time until the scrubber reaches the first rotten copy, plus the repair time. With independent rot and probability q per copy per window, P(loss) ≈ q³ per chunk per window.

| q per copy per window | P(chunk lost) per window | 10⁸ chunks, per window |
|---|---|---|
| 10⁻⁴ | 10⁻¹² | 10⁻⁴ |
| 10⁻⁶ | 10⁻¹⁸ | 10⁻¹⁰ |

q scales with the window, so halving the scrub interval divides the risk by 8. Rack-spread placement (ADR-0008) keeps the three copies on different disks, controllers and power, which is what independence needs. What remains unprotected is listed in README Known limitations: correlated rot (a firmware bug across disks), all copies rotting within one window, and corruption in client memory before hashing.

## Consequences

- `TestCorruptChunkDetectedOnRead`: RF 3 restored within 250 ms to 10.25 s of the read (bound 15.1 s; the 10 s case is one lost message and one copy timeout). `TestTwoReplicasCorrupt`: restored from the one good copy. `TestAllReplicasCorrupt`: loud `CodeCorrupt`, counted as lost, nothing copied. `TestScrubberFindsCorruption`: found within one pass interval with nobody reading. `TestSuspectHintMakesNodeRecheck`: an intact copy survives a hint.
- Chaos: bit rot is a fault kind (at most 3 chunks per event, never the last 2 intact copies). After replication settles, the runner waits for two full scrub passes, then requires no rotten chunk on any running node's disk. 500 seeds per push rot about 540 copies. Every read, during a run and after, must return bytes that were written to that path.
- Real mode: `corrupt-replicas` flips bytes in node-2's volume with `chunkd debug corrupt`; all 4 found, quarantined and replaced.
- Each read costs one extra SHA-256 on the node: about 2 ms for 4 MiB with SHA extensions, 10 ms without.

## At 100× scale

At 400 TB per node, 8 MiB/s is 1.6 years per pass. The rate has to scale with spindles: one scanner per volume, as HDFS does. Scrub I/O should also be scheduled below client reads, as Ceph's deep scrub uses load-gated time windows. Per-chunk SHA-256 would give way to per-block CRCs, so partial reads verify without reading the whole chunk. Erasure-coded data needs parity checks, not only per-fragment hashes, to find a silently wrong fragment.
