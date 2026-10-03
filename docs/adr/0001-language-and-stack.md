# 0001. Language and stack

Status: accepted (the Raft library was changed by ADR-0017: etcd raft, not hashicorp/raft)
Date: 2026-09-29

## Context

chunkd is a metadata service, storage nodes and a gateway that talk over RPC, stream chunk data, replicate metadata with consensus, and must be testable under injected failure. The same core code must also compile to WebAssembly for the browser demo. One engineer builds and maintains it.

## Options considered

| | Go | Rust | Java |
|---|---|---|---|
| Concurrency model | Goroutines and channels; blocking code that scales | async/await with Tokio; explicit, strict about sharing | Virtual threads (21+); mature but heavy runtime |
| gRPC | grpc-go, first-party | tonic, solid | grpc-java, first-party |
| Raft | hashicorp/raft (Consul, Nomad), etcd/raft | openraft, raft-rs (TiKV) | Apache Ratis, JRaft |
| Linearizability checking | porcupine | stateright, custom | Jepsen/Knossos via Clojure |
| WASM | `GOOS=js GOARCH=wasm` in the standard toolchain; 7-8 MiB binary here | Best-in-class size (hundreds of KiB) | TeaVM/CheerpJ; not practical for this code |
| Debuggability | pprof, race detector, execution tracer; Delve debugger | Good; async stacks harder to read | Excellent (JFR, JMX) |
| Performance | GC pauses in the sub-ms range; fine for an I/O-bound store | No GC; highest ceiling | JIT; high throughput, larger memory |
| Build speed | Seconds | Minutes for a clean build | Tens of seconds |

## Decision

Go. The deciding factors are the race detector for concurrent code, etcd raft (first hashicorp/raft; see ADR-0017) plus porcupine for the hard parts, and a WASM target in the standard toolchain. The module declares `go 1.26` (grpc-go 1.84 needs a recent toolchain); CI and the Docker build use Go 1.27.

Rest of the stack (locked): gRPC + protobuf with buf, bbolt for metadata, plain files for chunks, `log/slog` JSON logs, Prometheus text metrics, Docker compose, Preact + TypeScript + Vite for the dashboard.

## Consequences

- WASM binary is 7-8 MiB instead of Rust's sub-MiB. Acceptable against a 20 MiB budget; `task wasm` fails the build above it.
- GC pauses are a latency source for the data path. Chunk buffers will be pooled once the data path exists.
- Error handling is explicit and verbose; `errors.Is` sentinel values in `internal/iface` keep it uniform.

## At 100× scale

Go remains a fit: etcd, CockroachDB, SeaweedFS and MinIO run Go at far larger scale. The first limits would be GC pressure from chunk buffers (mitigated by pooling and `GOMEMLIMIT`) and single-goroutine event loops per process (mitigated by sharding the metadata state machine by path prefix).
