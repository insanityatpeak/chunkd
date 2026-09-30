# 0019. Metadata reads, term fencing and failover

Status: accepted
Date: 2026-09-30

## Context

With three metadata peers, four things must hold: a read must not return state older than an acknowledged write; a deposed leader must not be able to change the data plane (delete or copy chunks) after a new leader exists; a client must be able to retry any request across a failover without applying it twice or losing it; and a new leader must not act on a view older than what its predecessor committed. The metadata plane is CP: the majority side serves, the minority refuses.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Linearizable reads | Read from the leader's memory; lease-based reads; **read-index** | Read-index. A lease trusts that clocks drift less than the lease margin; a leader that stalls (GC pause, SIGSTOP) then serves a stale read after a new leader has committed. Read-index costs one heartbeat round and consults no clock |
| Fencing token for the data plane | Wall-clock leases; a separate epoch counter; **the Raft term** | The term. It is already monotonic, already agreed by a majority, and every command sent by a leader can carry it |
| Where a node keeps the highest term | Memory; **on its disk with the chunks** | Disk. A node that restarts must not readmit a deposed leader; its incarnation changes on restart, the term must not reset |
| Stale-leader GC deletes | Term fencing on the node only; **decide in the log, send after commit, and fence** | Log the decision. A term check alone leaves a window: a deposed leader's delete can reach a node before that node has heard the new term. A committed intent cannot come from a deposed leader |
| Retry after failover | Client tracks sequence numbers per session; **idempotent operations plus a request ID for Begin** | Idempotent operations. Commit is keyed by upload ID, delete and undelete by expected version, claim by chunk index; Begin, the one operation that created something new on every attempt, gets a client request ID |
| A new leader's view of chunk locations | Rebuild from block reports after election; **nodes report to every peer** | Report to every peer, as HDFS's DataNodes do to an active and a standby NameNode. A new leader starts with a warm location map |

## Decision

**Serving.** Only the leader answers. A follower replies `CodeNotLeader` with the leader's node ID as the message; the client goes there at once. A leader serves nothing until it has applied an entry of its own term (the no-op every new leader appends), so it holds everything committed before it. A leader that has not heard from a quorum within the election timeout reports itself not ready, even though the library still calls it leader until it sees a higher term.

**Reads.** `Stat`, `List` and `Log` run after `ReadIndex`: the leader confirms with a quorum that it is still leader, then waits until it has applied the commit index it was told. The dashboard's `Cluster` view skips the round: it shows soft state (liveness, locations, this peer's timeline) that is not linearizable anyway. Chunk locations inside `Stat` are soft state and can be stale; that is safe, because a chunk is immutable and hash-verified, so a stale location costs one extra hedged read and never returns wrong bytes.

**Writes.** A mutation is validated against the applied state (fails fast), proposed, and answered when the log has applied it. Two operations proposed together can invalidate each other (two commits on one expected version); the log orders them, `State.Apply` rejects the loser with the same error on every peer, and the loser's caller gets that error. A proposal that outlives its leader gets `CodeUnavailable`: the entry may still commit, so the client retries an idempotent request. A retried commit, delete or undelete whose first attempt applied finds its own result.

**Fencing.** Every command a leader sends a node (`Replicate`, `DeleteReplica` for trims and GC, `VerifyChunk`) carries its term. A node records the highest term it has seen, on its disk, and refuses any command below it. It learns the term from the leader's heartbeat acks (which carry `leader` and `term`) as well as from commands, so a deposed leader is refused from the first heartbeat after an election, not only after the new leader's first command. Only a leader's term counts: a follower or candidate can carry any term. A node refuses a command with no term once it has seen one.

**GC deletes are decided in the log.** The sweep proposes a `GCIntent` listing the copies to delete, each with its fence (the leader's view of the node's report sequence). Applying it skips chunks that are marked at that point of the log and copies already pending, and records the rest as pending state, replicated like any other. Only after the intent commits does the leader send the deletes. Answers go back through block reports; the leader batches them into a `GCDone`. A new leader resends every pending delete with its original fence, and a node answers an absent copy as deleted, so a lost `GCDone` costs a repeat, not a mistake. The node-side fence (a chunk written after `fence_seq` is kept) still guards the race with a claim logged after the intent.

**New-leader soft state.** The repair queue and in-flight copies, the GC's first-seen and sent times, and the acks awaiting a `GCDone` belong to one leader's term and start empty. Repair treats every chunk as freshly committed for its upload grace after taking over, since the new leader cannot know which commits have reports in flight.

**Client.** The client keeps a list of peers and the one that led last. `CodeNotLeader` with a hint moves it at once; with no hint (an election), or an unreachable peer, it tries the next after a pause, for about eleven seconds with three peers. `Begin` carries a request ID chosen once per `Put`; a Begin with a known request ID returns the upload it already opened, with the placement chosen then.

**CAP per plane.**

| Plane | Choice | Majority side | Minority side |
|---|---|---|---|
| Metadata | CP | serves reads and writes | refuses both (`CodeNotLeader`) |
| Data reads | Available | any reachable valid replica, given a location from the majority or one the client already holds | the same, from locations already held |
| Data writes | CP | need `MinReplicas` reported and a metadata majority to commit | fail loudly: nothing becomes visible |

## Consequences

- A stale leader can neither commit metadata (no quorum), nor authorize a GC delete (its intent cannot commit), nor have a trim or copy carried out once any node has heard a later term; and it stops sending on its own within one election timeout of losing its quorum.
- Tests: `TestStaleTermCommandsAreRefused` and `TestFenceSurvivesRestart` (node), `TestDeposedLeaderSendsNoGCDeletes` (three metas and three nodes), `TestGCIntents` and `TestBeginRequestIDIsIdempotent` (state), `TestPutSurvivesTheLeaderDying` (client, 49 kill points), `TestKillLeaderMidUpload`, `TestMinorityLeaderRejectsAndStaleWriteNeverCommits` (sim cluster), `TestKillLeaderMidUploadReal` (gRPC and disk).
- Every request costs the client a leader lookup after a failover; a read costs one heartbeat round.
- The fence lives on the node's disk: a wiped disk forgets it. It relearns from the next leader's ack within a heartbeat.
- SIMPLIFIED: read-index per request, not batched. etcd batches concurrent reads into one round.
- SIMPLIFIED: the dashboard's cluster view is served by the leader alone. A follower has a warm location map and could serve it.
- Known limitation: a node partitioned from every metadata peer but reachable by clients keeps serving reads of chunks it holds; no client can learn that it should not, and none needs to (chunks are immutable and verified).

## At 100× scale

The read-index round and the single leader bound metadata throughput. etcd's answer is the same design plus batching; CockroachDB adds leader leases for reads, accepting a clock-drift bound, in exchange for serving reads without a round trip. A term-fenced data plane scales as is, since the token is one integer per command. The GC intents would be batched per epoch across many chunks (they already are) and split by range, one intent stream per range group. Reports to every peer multiply with the group size, which is why HDFS limits it to two NameNodes; a larger group would report to the leader and a small set of observers.
