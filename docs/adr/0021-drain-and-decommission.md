# 0021. Drain and decommission: logged node states

Status: accepted
Date: 2026-10-02

## Context

An operator needs to retire a node without ever dropping a chunk below RF on purpose. Until now "draining" was a static node-side flag sent in heartbeats and copied into the leader's soft node table on every beat. Nothing set it, nothing moved data off a draining node, and node membership itself is not logged: a node joins on its first heartbeat, separately at every metadata peer. A drain held only in memory or in heartbeats would be lost or undone by a leader change, a restart, or the next heartbeat.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Where the state lives | Node-side flag in heartbeats; leader memory; **logged ops in the metadata state** | Logged. A new leader re-derives the evacuation from the log and the location map; nothing about the plan exists only in memory |
| The heartbeat flag | Keep it alongside; **remove it** | Removed. Two sources of truth let a heartbeat undo an operator's drain |
| Finishing a drain | Automatic once safe; **an explicit decommission, refused unless safe** | Explicit. The operator decides when the node may be powered off; `--wait` polls until it is allowed |
| Counting a draining node's copies | Not at all; **as holders and read sources, not toward RF** | It still serves reads and is a copy source, so evacuation is a copy from it, not a repair from fewer copies |

## Decision

`NodeAdminOp{node, state}` is a log op; the state lives in `State` and in its snapshot. The states:

```
          drain                    decommission (only when safe)
ACTIVE ───────────▶ DRAINING ───────────────────────────────▶ DECOMMISSIONED
  ▲                    │                                            │
  └───── undrain ──────┘◀────────────── undrain (recommission) ─────┘
```

- **ACTIVE**: the default for any node not in the table.
- **DRAINING**: excluded from placement and as a copy target. Its copies still count as holders and serve reads, but a chunk needs RF copies on non-leaving nodes, so each of its chunks gets an evacuation copy (class 1 in ADR-0020's queue, after repair). A draining node's copy is never trimmed.
- **DECOMMISSIONED**: excluded from placement, holders and sources. Its heartbeats are still accepted, so it stays visible; a heartbeat cannot bring it back, because eligibility reads the logged state.

`Validate` enforces the transitions in log order. Decommission is safe when every chunk the node holds has at least RF copies elsewhere on nodes that are alive, confirmed and not leaving. The leader checks that when the command arrives and refuses with the count of chunks still short. Undrain from DECOMMISSIONED returns the node to service; its copies are content-addressed and verified on read, so any that are still referenced count again, and the rest are garbage-collected.

Commands: `chunkd node drain|undrain|decommission <id> [--wait]`, through the gateway (`POST /nodes/{id}/{action}`) or straight to the metadata group (`-meta`), served by the `meta.node_admin` RPC on the leader.

## Consequences

- A node killed mid-drain is a failure like any other: its chunks go to class 0 (repair) first. A drain itself never lowers a chunk's live copies, because a draining copy is never trimmed and the decommission check counts only copies elsewhere.
- Draining the only node in a rack lowers rack spread for its chunks to the remaining racks; the command warns.
- Decommission is refused if fewer than RF eligible nodes remain.
- Tests: `TestDrainNeverDropsRF` (drain under load, a node killed mid-drain), `TestDecommissionOnlyWhenSafe`, state transition tests, chaos kinds `drain`, `undrain` and `kill-drain-target`, and the real-mode `drain-node` scenario.
- SIMPLIFIED: the safety check reads the leader's soft location map, not a logged record. HDFS's decommission check reads the NameNode's block map the same way.
- SIMPLIFIED: no automatic decommission of nodes dead for a long time. HDFS marks such nodes dead and re-replicates, but leaves removal to the operator, as here.

## At 100× scale

Decommissioning a full node at 40 MiB/s takes hours per terabyte. HDFS lets operators raise per-node replication streams for decommissioning and spreads the work across many sources; here the per-node limits and the drain class's slot share would be the knobs. Draining a whole rack becomes the common operation, and a rack-level admin state saves one command per node.
