# What the chaos harness found

I built chunkd, a distributed file store in Go, so that the interesting question was never "does it work" but "what does it do when things break". The core code never reads a clock, opens a socket or calls a random number generator directly. It receives a transport, a clock, a block store and a randomness source through interfaces. In tests those are a simulated network with a seeded fault schedule and a fake clock that only moves when the test says so. One goroutine runs the whole cluster, so a run is a pure function of its seed.

That buys three things I could not have got from containers. A failing schedule prints a seed that replays it exactly. Time is free, so CI runs 1,000 random schedules on every push. And the same code runs in a browser, so anyone can reproduce a scenario from a link.

Each schedule kills, freezes, slows or wipes nodes, drops and duplicates messages, rots bits on disk, adds and drains nodes, and with three metadata peers kills or cuts off the leader. After the faults stop, the harness checks the invariants: every acknowledged write reads back with the hash it was written with, replication is restored within a stated bound, no read ever returned bytes that were never written, nothing is left to garbage-collect. A porcupine pass checks every client history for linearizability.

Thirty defects are written up in `docs/bugs-found.md`. Four of them show what the approach is good for, and one shows its edge.

## A commit that succeeded and reported failure (seed 1)

`TestUploadsUnderMessageLoss` runs uploads at 5% message loss and 5% duplication. With seed 1, `Put` returned `not_found: upload 1` for a file whose version was committed and visible.

The network had delivered `CommitUpload` twice. The first copy committed and removed the pending upload. The second found no upload and answered `not_found`. Response delays are random, so that answer could reach the client first. A lost commit response looked the same: the client could not tell "not committed" from "committed, answer lost".

The fix was to make the version remember the upload that created it, so a repeated commit returns the same version, and to make deletes conditional on a version. Seed 1 was enough to show it.

## A follower that remembered a copy that was gone (seed 395)

Chunk locations are not stored in the metadata log. Nodes report what they hold, and each peer builds its own map. When a node's copy is trimmed, a logged `TrimDone` says so.

`chaos --seed=395 --metas=3` caught a trim that left a chunk with two intact copies. One follower had missed the node's delete report, so it still counted the trimmed copy. When it later became leader, it saw four copies where there were three, and trimmed a real one. The fix was to make applying `TrimDone` drop the location on every peer: the log, not a report, is the authority that the copy is gone.

It needs a missed report, a follower promoted at a particular moment, and a trim in flight. Reading the code would not have shown it; the seed made it reproducible.

## A trim that waited too long (seed 1153)

A trim is authorized when it is logged, but the delete can go out much later. Seed 1153, and four others from a sweep starting at 1001, left a chunk at two copies. The leader resends a pending trim when its victim returns. Here the victim had been down, and while it was away another holder died, inside the repair delay, so nothing had replaced that copy. When the victim came back, the resend deleted one of only two remaining copies.

The bug dated from the commit that introduced logged trims. It stayed hidden until I added a watch that checks, at the instant of every trim delete, that the chunk keeps its replication factor. The old end-of-run check only looked at the settled state, and a dip below the factor that heals itself passes it. The fix is a recheck before the first send and before every resend.

## Three lost messages among six (seed 946)

I added erasure coding: each 4 MiB chunk becomes four data shards and two parity shards on six nodes, readable from any four. The first sweep with it, `chaos --seeds=1000 --ec`, left nine seeds with an acknowledged file unreadable: `3 of 6 shards readable, 4 needed`. Every missing shard was a request that timed out against a node that was alive.

The simulated network drops 1% of messages, so one request or its answer is lost about 2% of the time. The replicated read makes two passes over three replicas. The stripe read asked each shard once, so three lost messages among six requests failed it: about 1.6 in 10,000 reads, which is a handful across a sweep.

The fix is a second ask for a shard that only timed out. A shard that answered `not_found` or `corrupt` is final.

## What it missed

The harness is not a proof. In CI, the real-mode suite against Docker containers failed with six repair copies that nothing had justified (bug 7). Block reports can be reordered on a real node, where chunk writes run on concurrent handlers while the full report is listed on the event loop. A report listed before a write finished, delivered after that write's incremental report, dropped a new chunk, and repair copied it again. The dangerous mirror case let an old report resurrect a deleted copy.

The single-goroutine simulator hid the first case, and its random message delay could produce both but chaos never landed on it. Reports now carry the node's incarnation and a sequence number taken under a lock, and a regression test tries seven arrival orders.

Another found only in compose: a recreated container's old IP routed one node's commands to a different node (bug 24). Nothing in the simulator models addresses.

The simulator also shares no capacity between messages. A repair copy cannot slow a read in it. I measured the cost of throttling repair, but not its effect on readers, and I say so in the benchmark notes.

## What deterministic simulation made possible

- Replaying a failure from a seed, which turns "it failed once in CI" into a debugging session.
- Running far more schedules than any container test could.
- Checking claims that need controlled time: a repair delay of exactly 20 seconds, a lease that must outlive a GC grace period. I got the second wrong first: with four epochs of 30 seconds, the guaranteed idle time was 90 seconds, shorter than the 120 seconds the GC design promises to survive. A slow-upload scenario caught it before the code was committed.

The cost is discipline. Core code may not touch the environment, and a lint step in CI fails the build if it does. Everything nondeterministic comes in through an interface, down to map iteration order.

The repo is github.com/insanityatpeak/chunkd. Every scenario in the README replays from its seed in the browser.
