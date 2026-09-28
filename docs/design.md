# Design

This document describes what exists, not what is planned.

## Components

```
   CLI / browser ──HTTP──► gateway ─┐   (client library: verifies every chunk and the file)
   CLI (-meta) ──────────────────────┤
                                     │ control: begin, commit, stat, list, delete, cluster (unary gRPC)
                                     ▼
                              ┌─────────────┐   WAL + bbolt snapshots
                              │   meta-1    │   namespace, versions, refcounts, pending uploads
                              └──────▲──────┘   node view + chunk locations (rebuilt, not stored)
               heartbeat 1 s,        │
               block reports ────────┘
      ┌──────────┬──────────┬──────────┬──────────┐
    node-1     node-2     node-3     node-4     node-5        chunks at ab/cd/<sha256>
     r1         r2         r3         r1         r2
      ▲  data: chunk.put / chunk.get (gRPC streams, 1 MiB frames), client ↔ nodes directly
```

The metadata server is never on the data path.

## Run modes

| | sim | real |
|---|---|---|
| Processes | one | one per component |
| Transport | `sim.Net`: in-memory, seeded loss, duplication, delay, bandwidth, partitions, crashes | `grpcnet`: Deliver (one-way), Call (unary), CallStream (framed) |
| RPC client | `sim.Caller`: advances the clock until answered | `grpcnet.Caller`: goroutine per call |
| Clock | `sim.Clock`: event queue ordered by (deadline, seq) | wall clock; timers post to `runtime.Loop` |
| Chunk store | `sim.BlockStore` | `blockstore`: files, atomic rename |
| Metadata log | `sim.MetaStore` | `metastore`: WAL + bbolt |
| Where | tests, `cmd/chunkd-wasm` in the browser | `docker compose up`, `internal/real/local` in tests |

Both modes run the same `internal/core/{meta,node,placement,chunk}` code and the same `internal/client`.

## Upload

```
Client               Meta                         node-A      node-B      node-C
  │─BeginUpload(path, expected v, size)─►│
  │                                      │ CAS on live version; place each chunk
  │                                      │ (rack spread, least loaded); WAL append
  │◄──upload_id, placement, min_replicas─│
  │ read chunk i, SHA-256 → id_i
  │─chunk.put(id_i, bytes)────────────────────────────►│──────────►│──────────►│  in parallel
  │                                      │◄─block report "received id_i"──────┤  each node, before acking
  │◄─ack──────────────────────────────────────────────┤  (need ≥ min_replicas acks, else abort)
  │─CommitUpload(upload_id, [ids], file sha256)─►│
  │                                      │ each id has ≥ 2 live reported locations?
  │                                      │   no  → retry (client backs off up to 45 s)
  │                                      │   yes → CAS again, WAL append: version visible
  │◄──version─────────────────────────────│   repeated commit of the same upload → same version
```

## Download

```
Client               Meta                  node-A      node-B
  │─Stat(path)─────────►│
  │◄─version, sha256, [id_i → live replicas]
  │─chunk.get(id_i)──────────────────────────►│
  │◄─bytes: SHA-256 ≠ id_i ✗ ─────────────────┤
  │─chunk.get(id_i)──────────────────────────────────────►│   next replica
  │◄─bytes: SHA-256 = id_i ✓ ─────────────────────────────┤
  │ … every chunk, then file SHA-256 must equal the committed one
```

## Heartbeats and block reports

- Every node heartbeats each second: rack, address, bytes and chunks stored, draining flag.
- The heartbeat ack asks for a full block report when the metadata server has none for the node (first contact, or after a metadata restart).
- Nodes send an incremental report after storing each chunk, and a full report every 30 s, which repairs any lost incremental report.
- A node is alive if heard from within 5 s. Placement and reads only use live nodes.

## Invariants checked by the sim harness

After each scenario, `cluster.AssertInvariants` checks:

1. Every upload that returned success (and was not deleted) reads back with the SHA-256 it was written with.
2. Every committed file downloads and matches its recorded file hash.
3. Every chunk of every committed file has at least `min_replicas` live reported locations.
4. Every committed chunk has a durable record with refcount ≥ 1.
