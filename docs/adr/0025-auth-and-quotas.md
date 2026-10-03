# 0025. API keys, namespaces and byte quotas

Status: accepted (the usage scan was replaced by a counter in ADR-0028)
Date: 2026-10-03

## Context

The gateway is open: anyone who reaches it can read, write, delete and drain nodes. Item D adds identity and a limit on how much one tenant stores. Two requirements decide the design:

- A quota must hold under concurrent uploads and across a leader failover or a second gateway. An upload that fits alone but not together with another in flight must be refused.
- A refused upload must cost nothing: no bytes read, no chunks written.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Authentication | S3 SigV4 or HMAC-signed requests; mutual TLS; **static bearer keys** | Static bearer keys. A signature covers the body hash, which a streaming `POST /files` cannot know before it sends. TLS is the deployment's job (the keys are secrets on the wire without it). SIMPLIFIED: S3 uses SigV4 with IAM users and policies |
| Key storage | Plain keys in a file; **SHA-256 of the key** | The hash. The keys file leaks nothing usable; `chunkd keygen` prints the key once and the entry to add |
| Tenant scope | S3-style buckets in metadata; **the first path segment** | The first segment (`/alice/...`). No change to naming, listing or the sim. A key owns `/<namespace>/` as a whole segment, so `/alicex/` is not alice's. An admin key reaches every path and the node controls |
| Where quota state lives | Gateway memory; gateway disk; **the metadata log** | The log. Begin carries the limit; every peer applies the same check in log order, so two gateways, a restart or a failover cannot overshoot |
| What counts | A running counter per namespace; **a scan of live versions plus pending uploads** | The scan, first. ADR-0028 replaced it with a derived counter once the scan measured 29 ms at 100,000 files; a snapshot restores the uploads, so reservations survive it |
| Upload ownership | A token per upload; **the upload's path** | The upload ID names no path, so the gateway asks the log for the upload's path and compares it with the key's namespace. Another key's ID answers `not_found`, as an unknown one does |

## Decision

```
client ── Authorization: Bearer k ──▶ gateway ── sha256(k) in keys file? ──▶ no: 401
                                         │ path under /<namespace>/ ?   ──▶ no: 403
                                         │ ctx = WithQuota(limit)
                                         ▼
                                  client library ── BeginUpload{path, size, quota} ──▶ log
                                         live bytes(ns) + pending(ns) + size > quota ?
                                         yes: quota_exceeded (507), nothing applied
```

- `BeginUploadOp.quota` and `UndeleteOp.quota` (0: unlimited). `State.Validate` refuses the op with `CodeQuota` when `NamespaceBytes(ns) + size` passes it; Apply runs Validate again, so every peer agrees.
- `NamespaceBytes` sums the newest version of each path in the namespace unless it is a tombstone, plus the size of every pending upload there. A pending upload therefore reserves its whole size from Begin. Commit swaps the reservation for a live version; abort and lease expiry release it; delete frees the bytes; undelete is checked as a new write.
- A repeat of a Begin (same request ID) is answered with the pending upload and is not charged twice.
- The gateway turns the key's quota into `client.WithQuota(ctx, n)`. The metadata server trusts the limit as it trusts any client; the gateway is the trusted party. Open mode (no keys file) is unchanged.
- Routes: `/files`, `/undelete`, `/uploads` are scoped to the key's namespace; a list without a prefix is narrowed to it; `/nodes` needs an admin key. `/cluster` and the dashboard's static files stay open: they hold counts and node health, no file names.
- Status codes: 401 no or unknown key, 403 outside the namespace or not admin, 507 over quota.

## Consequences

- Quota holds under concurrency because the check and the reservation are one log entry. `TestQuotaReservesAtBegin` (state), `TestQuotaRefusesBeforeBytesAreSent` (gateway) and `TestQuotaRefusalReadsNoBytes` (library, zero bytes read).
- SIMPLIFIED (fixed by ADR-0028): each quota-limited Begin scanned every file and upload, O(files) on every peer; it now reads a per-namespace counter, as S3-compatible stores do per bucket.
- SIMPLIFIED: an overwrite counts the old and the new version until the commit, so a tenant at 60% of its quota cannot overwrite a file larger than 40% of it.
- SIMPLIFIED: only live versions count. Retired versions kept for undelete (and the chunks they pin) cost the cluster bytes the quota does not see; Ceph and S3 count noncurrent versions.
- SIMPLIFIED: the quota counts logical bytes, not stored bytes, so an erasure-coded tenant (1.5x) and a replicated one (3x) cost the cluster different amounts for the same quota, and dedup across tenants is invisible to it.
- Keys are bearer secrets: anyone who sees one acts as its owner. Run the gateway behind TLS. There is no rotation beyond editing the file and restarting the gateway, and no per-key rate limit.
- Chunk reads and writes between the gateway and the nodes are not authenticated; the nodes trust the network, as before.

## At 100x scale

The limit moves into a logged namespace record instead of travelling on every request. Keys move from a file to a logged table with rotation and expiry, and signed requests replace bearer tokens so a captured request cannot be replayed with another body. Quotas would count stored bytes per redundancy class, charge abandoned uploads to their owner at expiry, and report usage per namespace in `/cluster`.
