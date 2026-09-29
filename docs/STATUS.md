# Status

Current phase: **2: failure detection and re-replication** (complete)

## Phase 2 checklist

- [x] `core/detector`: alive, suspect, dead with hysteresis, incarnations, stall guard (ADR-0010)
- [x] Hedged reads: `Caller.Hedge`, client latency scoring, suspect replicas last (ADR-0012)
- [x] `iface.AsyncCaller` (sim and gRPC) for in-loop RPC; repair pulls use it (ADR-0012)
- [x] `core/repair`: priority queue, 20 s delay, per-node and byte-rate throttle, copy timeout, periodic scan (ADR-0011)
- [x] Trim of over-replicated chunks, reconcile of returning and wiped nodes
- [x] Metrics and `chunkd cluster status`: replication histogram, repair counters, detector stalls
- [x] `internal/chaos`: seeded fault schedules with invariant checks and replay; 500 seeds on every push, 20k on demand
- [x] Real-mode chaos over docker compose: kill past the delay, blip, freeze
- [x] Dashboard: detector state and heartbeat age, per-file replication, repair queue and copies in flight, event timeline, 1×–50× speed, scripted kill-node-3 scenario; same UI against the gateway
- [x] ADRs 0010–0012; bugs 3–6 in `docs/bugs-found.md`

## Exit criteria

| Criterion | Evidence |
|---|---|
| `TestKillNodeRestoresRF`, sim: 50 files, one node killed, RF 3 within `dead 10 s + delay 20 s + bytes ÷ 40 MiB/s + copy timeout 10 s + 5 s` | 196 MiB on the node, restored in 42.8–44.8 s against a 49.9 s bound, seeds 1–3 (`internal/sim/cluster`) |
| `TestKillNodeRestoresRF`, real: 50 files, bound 90 s | `kill-node-restores-rf`: longest under-replication 32 s, 97 copies (`go run ./tools/task chaos --mode=real --short`, CI `compose` job) |
| `TestTransientBlipNoRepair` | 2 s, 15 s, 25 s down: zero copies (sim); 15 s blip: zero copies (real) |
| `TestRepairThrottle`: two nodes killed | Peaks 4 in flight, 2 per source, 2 per target (limits 8/2/2); all restored |
| `TestSlowNodeHedgedRead` | Hedged p99 176–194 ms against 4–10 s unhedged, bound 750 ms |
| Chaos: 500 seeds green in CI, failures print a replay command | CI `go` job; 5000 seeds locally in 50 s |
| Dashboard shows suspect → dead and the repair queue draining | "Run scenario: kill node-3"; checked in headless Edge, sim and against the compose gateway; `TestDashboardTimeline` runs the same script |

## Phase 1 (complete)

Chunked, replicated upload and download; see ADRs 0005–0009. `TestRoundTrip`, `TestCommitRequiresMinReplicas`, `TestUncommittedInvisible`, and the 20 MiB compose round trip stay in CI.

## Next

- [ ] Phase 3: integrity (`04-phase3-integrity`)
- [ ] Acknowledged incremental block reports (removes the up-to-30 s commit delay after a lost report)
- [ ] Overlap chunk uploads (window of chunks in flight)
- [ ] Per-step safety check in the chaos harness (real copies never below RF − 1); bug #6 slipped past the end-state checker

## Open decisions

- `go.mod` declares `go 1.25` (grpc-go 1.84 requires it).
- Compose racks are unbalanced (r1 ×2, r2 ×2, r3 ×1), so node-3 holds a replica of every chunk. Kept to show the rule; a sixth node would balance it.
- Detector and repair timings (dead 10 s, delay 20 s) are demo-scale so a failure plays out in under a minute. HDFS waits 10.5 min and Ceph 10 min before re-replicating; the knobs are `detector.Config` and `repair.Config`.

## Known bugs

None open. See `docs/bugs-found.md`.
