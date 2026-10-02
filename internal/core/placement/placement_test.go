package placement

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

func cluster(racks, perRack int) []Node {
	var out []Node
	for r := 1; r <= racks; r++ {
		for i := 1; i <= perRack; i++ {
			out = append(out, Node{ID: iface.NodeID(fmt.Sprintf("r%d-n%d", r, i)), Rack: fmt.Sprintf("r%d", r), Alive: true})
		}
	}
	return out
}

func rackOf(nodes []Node) map[iface.NodeID]string {
	m := map[iface.NodeID]string{}
	for _, n := range nodes {
		m[n.ID] = n.Rack
	}
	return m
}

func sizes(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = 4 << 20
	}
	return s
}

// Property: over many random clusters, replicas are on distinct nodes, on
// distinct racks whenever there are at least n racks, and never on dead or
// draining nodes.
func TestPlacementProperties(t *testing.T) {
	for seed := uint64(0); seed < 300; seed++ {
		rng := sim.NewRand(seed)
		racks := 1 + rng.IntN(5)
		nodes := cluster(racks, 1+rng.IntN(4))
		for i := range nodes {
			nodes[i].Used = int64(rng.IntN(1000)) << 20
			nodes[i].Alive = rng.IntN(10) > 0
			nodes[i].Draining = rng.IntN(10) == 0
		}
		rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })

		eligible := map[iface.NodeID]bool{}
		eligibleRacks := map[string]bool{}
		for _, n := range nodes {
			if n.Alive && !n.Draining {
				eligible[n.ID] = true
				eligibleRacks[n.Rack] = true
			}
		}
		got, err := Place(nodes, sizes(8), 3, 1, rng)
		if len(eligible) == 0 {
			if !errors.Is(err, ErrNotEnoughNodes) {
				t.Fatalf("seed %d: no eligible nodes but err = %v", seed, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		racksOf := rackOf(nodes)
		for c, reps := range got {
			if want := min(3, len(eligible)); len(reps) != want {
				t.Fatalf("seed %d chunk %d: %d replicas, want %d", seed, c, len(reps), want)
			}
			seenNode := map[iface.NodeID]bool{}
			seenRack := map[string]bool{}
			for _, id := range reps {
				if !eligible[id] {
					t.Fatalf("seed %d: placed on ineligible node %s", seed, id)
				}
				if seenNode[id] {
					t.Fatalf("seed %d: node %s used twice for one chunk", seed, id)
				}
				seenNode[id] = true
				if seenRack[racksOf[id]] && len(eligibleRacks) >= len(reps) {
					t.Fatalf("seed %d: rack %s used twice with %d racks available: %v", seed, racksOf[id], len(eligibleRacks), reps)
				}
				seenRack[racksOf[id]] = true
			}
		}
	}
}

func TestPlacementIsDeterministicFromSeed(t *testing.T) {
	nodes := cluster(3, 3)
	a, _ := Place(nodes, sizes(20), 3, 2, sim.NewRand(9))
	slices.Reverse(nodes) // input order must not matter
	b, _ := Place(nodes, sizes(20), 3, 2, sim.NewRand(9))
	for i := range a {
		if !slices.Equal(a[i], b[i]) {
			t.Fatalf("chunk %d: %v vs %v", i, a[i], b[i])
		}
	}
}

func TestPlacementSpreadsLoad(t *testing.T) {
	nodes := cluster(3, 2)
	nodes[0].Used = 1 << 30 // r1-n1 is full-ish; it should get nothing
	got, err := Place(nodes, sizes(30), 3, 2, sim.NewRand(1))
	if err != nil {
		t.Fatal(err)
	}
	count := map[iface.NodeID]int{}
	for _, reps := range got {
		for _, id := range reps {
			count[id]++
		}
	}
	if count["r1-n1"] != 0 {
		t.Fatalf("heavily loaded node got %d replicas", count["r1-n1"])
	}
	// Rack spread outranks load: every chunk needs rack r1, and r1-n2 is its
	// only light node, so it takes all 30. Within r2 and r3, pending-bytes
	// accounting splits evenly.
	want := map[iface.NodeID]int{"r1-n2": 30, "r2-n1": 15, "r2-n2": 15, "r3-n1": 15, "r3-n2": 15}
	for id, c := range want {
		if count[id] != c {
			t.Fatalf("spread %v, want %v", count, want)
		}
	}
}

func TestPlacementMinReplicas(t *testing.T) {
	tests := []struct {
		name    string
		alive   int
		min     int
		wantErr bool
		wantLen int
	}{
		{"all alive", 3, 2, false, 3},
		{"two alive meets min", 2, 2, false, 2},
		{"one alive below min", 1, 2, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := cluster(3, 1)
			for i := tt.alive; i < len(nodes); i++ {
				nodes[i].Alive = false
			}
			got, err := Place(nodes, sizes(1), 3, tt.min, sim.NewRand(1))
			if tt.wantErr {
				if !errors.Is(err, ErrNotEnoughNodes) {
					t.Fatalf("err = %v, want ErrNotEnoughNodes", err)
				}
				return
			}
			if err != nil || len(got[0]) != tt.wantLen {
				t.Fatalf("got %v, %v; want %d replicas", got, err, tt.wantLen)
			}
		})
	}
}

// Six shards on 3 uneven racks still fill round-robin: losing any rack
// loses at most 2.
func TestPlacementFillsRacksRoundRobin(t *testing.T) {
	nodes := append(cluster(1, 5), cluster(3, 1)[1:]...) // r1: 5 nodes, r2 and r3: 1 each
	nodes = append(nodes, Node{ID: "r2-n2", Rack: "r2", Alive: true}, Node{ID: "r3-n2", Rack: "r3", Alive: true})
	racks := rackOf(nodes)
	for seed := range uint64(50) {
		pl, err := Place(nodes, sizes(4), 6, 6, sim.NewRand(seed))
		if err != nil {
			t.Fatal(err)
		}
		for i, ids := range pl {
			per := map[string]int{}
			for _, id := range ids {
				per[racks[id]]++
			}
			if per["r1"] != 2 || per["r2"] != 2 || per["r3"] != 2 {
				t.Fatalf("seed %d chunk %d: racks %v", seed, i, per)
			}
		}
	}
}
