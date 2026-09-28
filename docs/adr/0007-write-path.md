# 0007. Write path: client fan-out, commit at 2 of 3

Status: accepted
Date: 2026-09-29

## Context

Each chunk must reach N=3 storage nodes on distinct racks. The write path decides latency, client bandwidth, and what happens when a node fails mid-write.

## Options considered

| | **Client fan-out, commit at W** | Pipeline chain (GFS, HDFS) | Primary-backup |
|---|---|---|---|
| Latency | Slowest of the W fastest replicas | One transfer plus a hop per replica; streamed, so close to one transfer | Two hops |
| Client egress | N× the data | 1× | 1× |
| Failure mid-write | Independent replicas; a failure costs that ack only | Chain breaks; pipeline recovery rebuilds it (HDFS's most complex code) | Primary failover, leases |
| Retry | Re-put is idempotent: content-addressed chunks | Rebuild the chain | Through the primary |
| Write ordering | Not needed: chunks are immutable | Needed for GFS record append | Its main purpose |

## Decision

The client sends each chunk to all N placed replicas in parallel and retries failed replicas once (puts are idempotent). If fewer than `min_replicas` (default 2) acknowledge, the upload aborts. Commit succeeds only when the metadata server has at least `min_replicas` locations reported by the nodes themselves in block reports; the client's word is not enough. Commit answers `retry` while reports are in flight, and the client backs off for up to 45 s, longer than the 30 s full-report interval, so a lost incremental report is covered by the next full one.

Ordering and chains exist to serialise mutations. Chunks here are immutable and named by their hash, so neither is needed.

W=2 of 3 keeps writes available with one node down. The cost is a window where a chunk has two copies; Phase 2 repair closes it.

## Consequences

- Client egress is 3× the file size. Fine for a gateway on the cluster network; costly for a laptop on a slow uplink.
- Commit is idempotent (the version records its upload ID) because the client cannot distinguish a lost commit response from a failed commit.
- `TestCommitRequiresMinReplicas` checks that an upload with one reachable node fails loudly and stays invisible; `TestUploadsUnderMessageLoss` runs 20 seeds at 5% loss and 5% duplication.

## At 100× scale

Client bandwidth becomes the limit. Switch to a pipeline ordered by network distance (GFS pushes data along the chain nearest-first and decouples data flow from control flow), or fan out from the first replica inside the cluster. Chunk uploads should also overlap (a window of chunks in flight, as HDFS does with packets) instead of running one chunk at a time.
