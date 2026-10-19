# 0032. Replace a metadata peer

Status: accepted
Date: 2026-10-20

## Context

A metadata peer whose WAL holds a record that fails its checksum before the tail refuses to start (`metastore.ErrCorrupt`): truncating there would drop acknowledged entries. The other two peers keep a quorum and serve, but the group can no longer survive a second failure, and nothing rebuilt the broken peer.

The obvious repair is the dangerous one. Wiping the peer's directory and starting it again made it bootstrap as the same Raft ID with an empty log and no hard state: it forgot its vote, so it could vote twice in one term, and forgot entries it had acknowledged, so it could help elect a leader that lacks committed entries. Raft's safety argument assumes a voter never loses its log; a voter that does must come back as a new member.

The consensus layer already supports single-server membership changes (ADR-0017, `TestAddAndRemoveVoter`). What was missing: IDs a replacement can take that every peer can name, a safe order of changes, a way for the new process to learn its ID, and an operator command.

## Options considered

| Option | For | Against |
|---|---|---|
| Wipe and restart under the same ID | No code | Unsafe: the peer forgets its votes and acknowledged entries |
| Random member IDs, member list (name, address) in the log, as etcd does | Grows, shrinks, moves a member to a new address | A replicated address book every process must follow; nodes, gateways and compose files name metadata peers by a static list today |
| **Slot and generation IDs: `id = slot + N·gen`, names from the static `-peers` list** | Every peer names any ID from its own flag; no address book; existing groups are generation 0 unchanged | Cannot grow, shrink or move a member |
| Joint consensus (remove and add in one change) | One step | Not supported by the consensus layer; single-server steps suffice for one peer |

Order of the two changes:

| Option | For | Against |
|---|---|---|
| Add the new ID, then remove the old | The slot never lacks a voter | With the old peer down, four voters with two down have no quorum: the removal never commits. Found by `TestReplacePeer` |
| **Remove the old ID, then add the new** | Two voters, both up, then three with one down: a quorum throughout | The slot is empty between the steps, so a retry must know which ID was last |

## Decision

**IDs.** A peer's slot is its 1-based position in the sorted `-peers` list. Raft ID `slot + N·g` is generation `g` of that slot (`meta.RaftSlot`), so every peer maps any ID to a name and address from configuration it already has. Generation 0 is the ID every peer had before this ADR: existing data directories start unchanged.

**Replace** (`meta.replace_peer`, `chunkd meta replace <peer>`). The request names the peer and the Raft ID the caller believes it has. The leader:

1. refuses if the peer is the leader, or answered within the election timeout (unless `force`);
2. logs `RetirePeerOp{raft_id}`, kept in a `retired` set in the snapshot (field 10);
3. removes the old ID, then adds `old + N`, one single-server change at a time.

A repeat is safe at every point: if the slot's voter is already `old + N` the leader answers without a change; if the slot is empty and its highest retired ID is `old`, it only adds; any other state is a conflict. An ID is only added after its predecessor is retired, so none is ever added twice.

**Join** (`chunkd-meta -join`, `CHUNKD_JOIN=1`). Over an empty directory, the process asks the other peers (`meta.join`) for the voter in its slot, records the answer in the snapshot database (`metastore.SetRaftID`) and only then starts consensus, with no bootstrap voters, catching up from a snapshot. A restart reads the recorded ID; any ID past generation 0 never bootstraps. `-join` over a directory that holds a log is ignored with a warning: that peer still has its votes and entries, so starting every peer with the flag, as `CHUNKD_JOIN=1 docker compose up` would, is harmless. The leader refuses a join once that voter has acknowledged an entry: a peer asking with an empty store after that has lost a log the group relied on, and must be replaced again.

SIMPLIFIED: the acknowledged-entry check reads the leader's match index, and a new voter can grant a vote before its first append. etcd avoids the question by giving every member a random ID and treating any restart with an empty store as a new member.

## Consequences

- `TestReplacePeer` (three peers, one dies, replace, join, catch up, a second peer dies and the group still commits), `TestReplacePeerRefusals`, `TestRaftIDMustMatchSlot`, `TestRetirePeerIsLoggedAndSnapshotted`.
- `TestReplaceMetaWithCorruptLog` (sim) and `TestReplaceMetaPeerReal` (real gRPC and disk: a byte flipped in the WAL gives `ErrCorrupt`, then replace, `-join`, catch-up, and a second peer killed).
- Chaos: in about a third of `--metas=3` seeds a follower loses its log and is replaced mid-run (`lose-peer-log`, `replace-peer`), drawn from a stream of its own so other faults keep their schedule; 1,500 seeds pass with 279 replacements, every peer byte-identical at the end.
- `chunkd meta peers` shows each peer's Raft ID, role, match index and last contact.
- The group's names and addresses stay fixed by `-peers`: no growing, shrinking or moving a peer. A replacement must come back at the same address.
- One log op and one snapshot field, both additive: logs and snapshots from before this ADR replay unchanged.

## At 100x scale

Many metadata groups (one per namespace range) would each need replacements, and a fixed name list per group stops working once peers move between hosts. The member list then belongs in each group's log, with random member IDs and joint consensus for moving a whole failure domain, as etcd and CockroachDB do, and replacement becomes automatic: a placement service notices a dead replica and adds a new one elsewhere before removing it, as CockroachDB's replicate queue does.
