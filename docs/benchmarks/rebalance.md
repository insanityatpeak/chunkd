# Rebalance: adding a sixth node

Source: `TestRebalanceBenchmark` (`internal/sim/cluster/rebalance_test.go`), sim, RF 3, 4 MiB chunks, 5 nodes on 3 racks (r1: node-1, node-4; r2: node-2, node-5; r3: node-3). Repair is idle when node-6 joins. Runs in CI.

## Datasets

- **demo**: the dashboard's demo files, 8 × 5 MiB, 40 MiB distinct (seed 1).
- **loaded**: the repair tests' cluster, 50 files of 1 B to 8 MiB, about 200 MiB distinct (seed 6).

## The minimum

Rack-aware placement keeps at most one copy of a chunk per rack (ceil(RF/racks) = 1), so each rack stores every chunk once: D bytes, with D the distinct bytes. The rack's nodes share that, so a node's rack-feasible target is D divided by the number of nodes on its rack (ADR-0020).

- **node-6 on r3**: node-3 holds all of r3's D. Two nodes share it, so node-6's target is D/2: 20 MiB for the demo set. That is the least any plan can move.
- **node-6 on r1**: three nodes share r1's D, so node-6's target is D/3, about 13.3 MiB for the demo set. r3 keeps all of D on node-3, because nothing else on r3 could take it.

½·L1 (half the total distance of every node from its target) is that minimum for the actual placement. It is higher than D/2 when the nodes in a rack start unevenly loaded.

## Results

| Dataset | node-6 on | ½·L1 | Moved | Moves | Bound | node-6 holds | Converged after alive |
|---|---|---|---|---|---|---|---|
| demo | r3 | 25.0 MiB | 12.0 MiB | 3 | 49.0 MiB | 12.0 MiB | 0.6 s |
| demo | r1 | 13.3 MiB | 8.0 MiB | 2 | 37.3 MiB | 8.0 MiB | 0.4 s |
| loaded | r3 | 101.3 MiB | 94.7 MiB | 26 | 125.3 MiB | 92.0 MiB | 2.6 s |
| loaded | r1 | 68.1 MiB | 62.5 MiB | 18 | 92.1 MiB | 60.0 MiB | 1.8 s |

In every case the detector confirms node-6 alive 1 s after it starts, and planning begins only after that ("settled membership").

Bound = ½·L1 + nodes × largest chunk; the test fails above it. A move is a whole chunk, so the planner can overshoot a target by up to one chunk per node.

## Why it moves less than ½·L1

The planner acts only while some node is outside its band, max(10% of target, 2 × largest chunk), and stops once every node is inside. On the demo set, two 4 MiB chunks (8 MiB) is 40% of a 20 MiB target, so node-6 stops at 12 MiB. On the loaded set the 10% term dominates: node-6 reaches 92 MiB of a target of about 100 MiB. The band is what stops one-chunk oscillation between two nodes near their targets.

## Time to converge

The loaded set moves at about 36 MiB/s, close to the scheduler's 40 MiB/s limit for all copy traffic. Every move here lands on node-6, and a node receives at most 2 copies at once. Balance and drain together take at most 4 of the scheduler's 8 copy slots, and repair always goes first. Each move copies and confirms the new copy before it trims the source (a logged TrimIntent/TrimDone), so no chunk drops below RF during the move. `TestAddNodeConverges` asserts that every 500 ms.

Times are simulated: 1 Gbit/s links (125 MiB/s), 1 to 40 ms message delay, 1% loss. On real disks and networks the 40 MiB/s limit is what decides the time, not the links.
