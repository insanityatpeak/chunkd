# 0030. Log node admission

Status: accepted
Date: 2026-10-18

## Context

ADR-0021 keeps a node's drain state in the log, but the leader refused drain, undrain and decommission for any node it had not heard from since it started: node membership was soft state, rebuilt from heartbeats. After a failover or a restart, a command for a node whose heartbeat had not yet reached the new leader failed with "no storage node has reported", and the operator had to retry.

## Options considered

| Option | For | Against |
|---|---|---|
| Leave it: retry for a second | No log change | The error is wrong (the node is a member), and a caller must know to retry |
| Accept any node name | No log change | A typo drains nothing and still logs an admin state for a node that never existed |
| **Log a join the first time the leader hears from a node** | A later leader knows every node the cluster ever admitted; a typo is still refused | One log entry per node, a snapshot field |
| Log the whole node table (rack, address, capacity) | A new leader could place before any heartbeat | Addresses and racks are claims the node re-makes every second; logging them makes stale values durable |

## Decision

`NodeJoinOp{node}` is a log op. The leader proposes it from the heartbeat handler when the log does not know the node (`State.Admitted`), once at a time per node; a failed proposal is retried by the next heartbeat. Applying it records the node in an `admitted` set, which is in the snapshot (`Snapshot.admitted`, field 9). A repeat applies nothing.

`State.Admitted(n)` is true for a joined node and for any node with an admin state, so logs and snapshots from before this ADR replay unchanged: they have no join entries, and each node is admitted again on its next heartbeat to the leader.

Drain and undrain are accepted for an admitted node the leader has not heard from. Decommission still requires that the leader has: the safety check counts a node's chunks from the location map, which a restarted leader has not rebuilt, so it would count nothing short and approve a decommission that is not safe. The refusal now says the leader has not heard from the node, which is true and retryable. The rack warning on a drain is skipped for an unheard node, since its rack is unknown.

## Consequences

- `TestNodeJoinIsLoggedAndSnapshotted`: a join applies once, survives a snapshot round trip byte for byte, and a snapshot with only an admin state still counts the node as admitted.
- `TestDrainBeforeFirstHeartbeatAfterRestart`: a restarted leader with no node view accepts a drain, refuses decommission with a conflict, and refuses a node that was never admitted.
- One extra log entry per node ever admitted. Each entry changes the number of messages in a sim run, which moves the 1% message-drop draws: the scenario goldens were regenerated, and the golden and scenario tests and the web e2e links moved from seed 7 to seed 1, because seed 7 now loses a claim during the demo load and the gc scenario's delete falls after the first epoch.
- A node removed for good stays in the admitted set. There is no op to forget it; the set is as large as the number of node names ever used.

## At 100x scale

At thousands of nodes the admitted set is a table of names in every snapshot, and joins are a burst at cluster start. HDFS's namenode keeps datanode registrations in memory and has them re-register after a restart; Ceph keeps an OSD map in its monitors' replicated store. A real membership table with explicit removal and epochs, as in the Ceph OSD map, would replace this set.
