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

## 2. Ambiguous commit reported as failure

| | |
|---|---|
| Symptom | Under message duplication, `Put` returned `not_found: upload 1` for an upload whose version was committed and visible. Under loss, a single dropped metadata message failed the whole operation. |
| Repro | `TestUploadsUnderMessageLoss`, seed 1 (5% drop, 5% duplication). |
| Root cause | The network delivered `CommitUpload` twice. The first copy committed and removed the pending upload; the second found no upload and answered `not_found`. Response delays are random, so the error could reach the client first. A lost commit response had the same effect. The client could not tell "not committed" from "committed, answer lost". |
| Fix | Each version records the `upload_id` that created it, and a commit for an upload that already committed returns that version. Delete is conditional (the client resolves the live version first) and a repeated delete of the same version returns the existing tombstone. The client retries metadata calls on `unavailable` and failed replica puts once. Commit `COMMIT_PLACEHOLDER`. |
| Regression test | `meta.TestCommitAndDeleteAreIdempotent` (duplicate commit and delete, and commit retry after a metadata restart); `e2e.TestUploadsUnderMessageLoss` over 20 seeds. |
