# Dedup

Source: `TestDedupSavings` (`internal/sim/cluster/dedup_test.go`), sim, seed 1, 4 MiB chunks, RF 3. Runs in CI.

## Dataset

A 20 MiB random base, then 10 copies, each with one 64-byte in-place edit inside one chunk. Then one copy with a single byte inserted at offset 0.

## Results

| Case | Logical | Distinct stored | Ratio | Client bytes skipped |
|---|---|---|---|---|
| Base + 10 in-place edits | 220 MiB | 60 MiB (5 + 10 chunks) | 3.67 | 160 MiB (4 of 5 chunks per copy) |
| 1-byte insert at offset 0 | 20 MiB | +20 MiB | 1.00 | 0 |

Nodes store exactly RF × distinct bytes (180 MiB), checked against the block stores, not only against metadata.

## Why the insert gets nothing

Chunks are fixed-size (ADR-0005). One inserted byte moves every later boundary, so every chunk hash changes. Content-defined chunking (Rabin fingerprints, FastCDC as used by restic and Borg) cuts at content-derived boundaries, so an insert changes one or two chunks. `SIMPLIFIED:` fixed-size only.

## Cost

One claim round trip per chunk before it is written (ADR-0015). For a 4 MiB chunk on a 1 Gbit/s link that is about 1 ms of about 34 ms; a window of chunks in flight would hide it.
