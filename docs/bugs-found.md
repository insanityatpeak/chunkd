# Bugs found

Real defects caught by tests, gates or the chaos harness.

## 1. gRPC linked into the browser build

| | |
|---|---|
| Symptom | `task wasm` failed the size gate: `cluster.wasm` 20.01 MiB against a 20 MiB limit. |
| Repro | Generate services and messages into one Go package, import the messages from core, run `go run ./tools/task wasm`. |
| Root cause | `protoc-gen-go-grpc` writes service stubs into the same package as the message types. Core imported `chunkdv1` for `PingRequest`, which also contained the gRPC client and server stubs, so `google.golang.org/grpc`, `net/http` and `golang.org/x/net` were linked into the WASM binary (325 packages). |
| Fix | Services moved to proto package `chunkd.rpc.v1` (Go package `rpcv1`); messages stay in `chunkd.v1`. `cluster.wasm` dropped to 7.62 MiB. Commit `194d04a`. |
| Regression test | `task wasm` runs `go list -deps ./cmd/chunkd-wasm` for `js/wasm` and fails if `google.golang.org/grpc` or `net/http` appears. |
