package rebalance

import (
	"fmt"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

const mib = 1 << 20

func chunkID(i int) iface.ChunkID {
	var id iface.ChunkID
	copy(id[:], fmt.Sprintf("chunk-%06d", i))
	return id
}

// usedOf recomputes every node's used bytes from the chunks' holders.
func usedOf(nodes []Node, chunks []Chunk) []Node {
	out := slices.Clone(nodes)
	for i := range out {
		out[i].Used = 0
		for _, c := range chunks {
			if slices.Contains(c.Holders, out[i].ID) {
				out[i].Used += c.Size
			}
		}
	}
	return out
}

func racksOf(c Chunk, rack map[iface.NodeID]string) int {
	seen := map[string]bool{}
	for _, h := range c.Holders {
		seen[rack[h]] = true
	}
	return len(seen)
}

// apply checks one move against the rules and performs it.
func apply(t *testing.T, chunks []Chunk, m Move, rack map[iface.NodeID]string) {
	t.Helper()
	for i := range chunks {
		if chunks[i].ID != m.Chunk {
			continue
		}
		c := &chunks[i]
		if !slices.Contains(c.Holders, m.From) || slices.Contains(c.Holders, m.To) {
			t.Fatalf("move %v: from must hold, to must not: holders %v", m, c.Holders)
		}
		before := racksOf(*c, rack)
		c.Holders[slices.Index(c.Holders, m.From)] = m.To
		if after := racksOf(*c, rack); after < before {
			t.Fatalf("move %v lowered distinct racks %d -> %d", m, before, after)
		}
		return
	}
	t.Fatalf("move of unknown chunk %v", m)
}

// layout returns nodes named node-1.. on racks r1..rN by (i-1)%racks, and
// chunks placed one copy per rack (RF = racks, at most 3), balanced within
// each rack, skipping nodes in empty.
func layout(nodes, racks, chunks int, size int64, empty ...iface.NodeID) ([]Node, []Chunk) {
	var ns []Node
	byRack := map[string][]iface.NodeID{}
	for i := 1; i <= nodes; i++ {
		n := Node{ID: iface.NodeID(fmt.Sprintf("node-%d", i)), Rack: fmt.Sprintf("r%d", (i-1)%racks+1)}
		ns = append(ns, n)
		if !slices.Contains(empty, n.ID) {
			byRack[n.Rack] = append(byRack[n.Rack], n.ID)
		}
	}
	var cs []Chunk
	for i := range chunks {
		c := Chunk{ID: chunkID(i), Size: size}
		for r := 1; r <= min(racks, 3); r++ {
			in := byRack[fmt.Sprintf("r%d", r)]
			c.Holders = append(c.Holders, in[i%len(in)])
		}
		cs = append(cs, c)
	}
	return usedOf(ns, cs), cs
}

func rackMap(nodes []Node) map[iface.NodeID]string {
	m := map[iface.NodeID]string{}
	for _, n := range nodes {
		m[n.ID] = n.Rack
	}
	return m
}

func moved(ms []Move) (b int64) {
	for _, m := range ms {
		b += m.Size
	}
	return b
}

// The dashboard layout: 40 MiB distinct over r1 ×2, r2 ×2, r3 ×1. A sixth
// node on r3 evens the racks: node-3 gives it exactly half, total/6, which is
// the minimum any algorithm must move.
func TestAddNodeToSmallRackMovesTheMinimum(t *testing.T) {
	nodes, chunks := layout(6, 3, 40, mib, "node-6")
	cfg := DefaultConfig()
	target := Targets(nodes, 40*mib, 3)
	for _, n := range nodes {
		if target[n.ID] != 20*mib {
			t.Fatalf("target %s = %d MiB, want 20", n.ID, target[n.ID]/mib)
		}
	}
	ms := Plan(nodes, chunks, cfg, 0)
	if got, want := moved(ms), L1(nodes, target)/2; got != want || got != 20*mib {
		t.Fatalf("moved %d MiB, want %d MiB (½·L1)", got/mib, want/mib)
	}
	for _, m := range ms {
		if m.From != "node-3" || m.To != "node-6" {
			t.Fatalf("move %v: only node-3 -> node-6 keeps rack spread at minimum cost", m)
		}
	}
	rack := rackMap(nodes)
	for _, m := range ms {
		apply(t, chunks, m, rack)
	}
	if after := usedOf(nodes, chunks); !Balanced(after, chunks, cfg) {
		t.Fatalf("not balanced after plan: %+v", after)
	}
}

// A sixth node on r1: r1 shares its rack's 40 MiB three ways; node-3 stays at
// 40 MiB, the rack-feasible target, though that is twice the mean.
func TestAddNodeToLargeRack(t *testing.T) {
	nodes, chunks := layout(5, 3, 40, mib)
	nodes = append(nodes, Node{ID: "node-6", Rack: "r1"})
	target := Targets(nodes, 40*mib, 3)
	if target["node-3"] != 40*mib || target["node-6"] != 40*mib/3 || target["node-2"] != 20*mib {
		t.Fatalf("targets %v", target)
	}
	ms := Plan(nodes, chunks, DefaultConfig(), 0)
	got, bound := moved(ms), L1(nodes, target)/2+int64(len(nodes))*mib
	if got > bound || got < L1(nodes, target)/2-mib {
		t.Fatalf("moved %d bytes, want about ½·L1 = %d", got, L1(nodes, target)/2)
	}
	for _, m := range ms {
		if m.To != "node-6" || (m.From != "node-1" && m.From != "node-4") {
			t.Fatalf("move %v: should stay within r1", m)
		}
	}
}

func TestBalancedClusterPlansNothing(t *testing.T) {
	nodes, chunks := layout(5, 3, 40, mib)
	if ms := Plan(nodes, chunks, DefaultConfig(), 0); len(ms) != 0 {
		t.Fatalf("balanced cluster planned %d moves", len(ms))
	}
}

func TestLimit(t *testing.T) {
	nodes, chunks := layout(6, 3, 40, mib, "node-6")
	if ms := Plan(nodes, chunks, DefaultConfig(), 3); len(ms) != 3 {
		t.Fatalf("limit 3 gave %d moves", len(ms))
	}
}

// Property, over random clusters: every move keeps rack spread and strictly
// lowers L1; the plan is a function of its input, not of its order; bytes
// moved stay within ½·L1 plus one chunk per node.
func TestPlanProperties(t *testing.T) {
	for seed := uint64(0); seed < 500; seed++ {
		rng := sim.NewRand(seed)
		racks := 1 + rng.IntN(4)
		n := racks + rng.IntN(6)
		var empty []iface.NodeID
		// node-1..node-racks keep every rack populated; later ones may join empty.
		for i := racks + 1; i <= n; i++ {
			if rng.IntN(3) == 0 {
				empty = append(empty, iface.NodeID(fmt.Sprintf("node-%d", i)))
			}
		}
		nodes, chunks := layout(n, racks, 10+rng.IntN(80), 0, empty...)
		var largest int64
		for i := range chunks {
			chunks[i].Size = int64(1+rng.IntN(4)) * mib
			largest = max(largest, chunks[i].Size)
		}
		nodes = usedOf(nodes, chunks)
		// Some skew: move random copies onto random nodes, keeping holders distinct.
		for range rng.IntN(30) {
			c := &chunks[rng.IntN(len(chunks))]
			to := nodes[rng.IntN(len(nodes))].ID
			if !slices.Contains(c.Holders, to) {
				c.Holders[rng.IntN(len(c.Holders))] = to
			}
		}
		nodes = usedOf(nodes, chunks)
		cfg := DefaultConfig()
		var distinct int64
		for _, c := range chunks {
			distinct += c.Size
		}
		target := Targets(nodes, distinct, cfg.Replicas)
		start := L1(nodes, target)

		ms := Plan(nodes, chunks, cfg, 0)

		shuffledNodes, shuffledChunks := slices.Clone(nodes), slices.Clone(chunks)
		rng.Shuffle(len(shuffledNodes), func(i, j int) { shuffledNodes[i], shuffledNodes[j] = shuffledNodes[j], shuffledNodes[i] })
		rng.Shuffle(len(shuffledChunks), func(i, j int) {
			shuffledChunks[i], shuffledChunks[j] = shuffledChunks[j], shuffledChunks[i]
		})
		if again := Plan(shuffledNodes, shuffledChunks, cfg, 0); !slices.Equal(again, ms) {
			t.Fatalf("seed %d: plan depends on input order", seed)
		}

		rack := rackMap(nodes)
		work := make([]Chunk, len(chunks))
		for i, c := range chunks {
			work[i] = Chunk{ID: c.ID, Size: c.Size, Holders: slices.Clone(c.Holders)}
		}
		prev := start
		for _, m := range ms {
			apply(t, work, m, rack)
			cur := L1(usedOf(nodes, work), target)
			if cur >= prev {
				t.Fatalf("seed %d: move %v did not lower L1 (%d -> %d)", seed, m, prev, cur)
			}
			prev = cur
		}
		if got, bound := moved(ms), start/2+int64(len(nodes))*largest; got > bound {
			t.Fatalf("seed %d: moved %d bytes, bound ½·L1 + nodes·chunk = %d", seed, got, bound)
		}
	}
}
