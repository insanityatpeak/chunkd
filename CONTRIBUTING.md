# Contributing

## Layout

| Path | Contents | May import |
|---|---|---|
| `internal/core/` | Chunking, placement, metadata state machine, failure detection, repair, scrub, GC | `internal/iface`, `internal/obs`, generated message types, stdlib minus the environment (see below) |
| `internal/iface/` | `Transport`, `Clock`, `BlockStore`, `MetaStore`, `Rand` | stdlib types only |
| `internal/sim/` | Fake clock, seeded RNG, fault-injecting in-memory network and stores, cluster harness | anything that builds for `js/wasm` |
| `internal/real/` | gRPC transport and caller, event loop, wall clock, disk block store, WAL + bbolt metastore, HTTP gateway, process bootstrap, in-process cluster (`local`) | anything |
| `internal/client/` | Client library (`Direct` over any `iface.Caller`) and the HTTP client for the gateway | no environment packages in `Direct`; `httpclient` may use `net/http` |
| `internal/e2e/` | Scenarios run against both the sim cluster and `real/local` | anything |
| `internal/obs/` | `slog` JSON logging with request IDs, metrics registry | no network or OS packages |
| `cmd/` | `chunkd-meta`, `chunkd-node`, `chunkd-gateway`, `chunkd` (CLI), `chunkd-wasm` | everything; the only place wiring happens |
| `proto/` | `.proto` sources; generated Go in `proto/gen` (committed) | |
| `web/` | Preact + TypeScript dashboard; `ClusterAPI` with WASM and HTTP implementations | |
| `deploy/` | Dockerfile (per-role targets) and `compose.yaml` | |
| `docs/` | `STATUS.md`, ADRs, design notes, `bugs-found.md` | |

## The interface rule

Core code never touches the environment directly. It gets time, randomness, network and storage through `internal/iface`, passed in through constructors. `go run ./tools/task lint-imports` enforces it: files under `internal/core` must not import `net`, `os`, `syscall`, `math/rand`, `crypto/rand`, and may use only `time.Duration` and its unit constants from `time`.

Wall-clock time never decides correctness. Use logical versions and fencing tokens.

## Tasks

Everything runs through `go run ./tools/task <command>` so it behaves the same on Windows, Linux and CI.

| Command | Does |
|---|---|
| `build` | `go build ./...` |
| `test` | `go test -race ./...` (falls back to no `-race` locally if no C compiler is installed) |
| `lint` | `go vet` (native and `js/wasm`) and staticcheck |
| `lint-imports` | The interface rule above |
| `proto` | `buf generate` in Docker; nothing to install |
| `wasm` | Builds `web/public/cluster.wasm`, copies `wasm_exec.js` from the same toolchain, fails above 20 MiB or if gRPC/`net/http` reach the WASM build |
| `web` | `npm ci` and `npm run build` into `web/dist` |
| `up` / `down` | Compose cluster; `up --small` runs 1 meta + 3 nodes; `down -v` deletes the volumes |
| `bench` | Benchmarks into `docs/benchmarks` (`-stages=sim,meta,real,report`); run it with the machine otherwise idle. See `docs/benchmarks/README.md` |
| `e2e` | Builds the CLI, puts and gets 20 MiB through the compose gateway, compares hashes (`--up`, `--down`) |
| `trace-check` | Fails on attribution trailers in tracked files or unpushed commit messages; run before every push |
| `ci` | build, lint, lint-imports, test, wasm, web in CI order |

The dashboard capture in `docs/assets/dashboard.{mp4,gif}` comes from `web/scripts/capture.mjs` (needs a built `web/dist` served on :4173 and ffmpeg).

Local dashboard: `go run ./tools/task wasm && cd web && npm run dev`. Add `?gateway=http://localhost:8080` to point it at the compose cluster.

Conformance suites in `internal/iface/ifacetest` run against every implementation of a seam (`BlockStore`, RPC). A new implementation must pass them.

## Commits

- Conventional commits: `feat(meta): …`, `fix(node): …`, `test(chaos): …`, `docs(adr): …`, `ci: …`, `build: …`, `chore: …`.
- One commit per completed step; `go test -race ./...` green before each.
- The message describes the change and nothing else: no trailers.

## Style

- Comments are one line and state what the code cannot: an invariant, why an ordering matters, a failure case.
- Every shortcut carries a `SIMPLIFIED:` note naming how a production system (GFS, HDFS, Ceph, S3, MinIO, SeaweedFS) does it.
- Docs: short sentences, concrete numbers, tables for trade-offs.
- ADRs live in `docs/adr/NNNN-title.md` with Context, Options considered, Decision, Consequences, At 100× scale. Copy `0000-template.md`.

## Tests

- New core logic ships with table-driven unit tests; failure logic also ships with a sim scenario.
- Sim failures print a seed and must replay identically from it.
- Bugs found by tests or the harness go in `docs/bugs-found.md`: symptom, repro, root cause, fix commit, regression test.
