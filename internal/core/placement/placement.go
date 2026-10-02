// Package placement chooses which storage nodes hold each chunk's replicas.
package placement

import (
	"cmp"
	"errors"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// ErrNotEnoughNodes means fewer than the minimum replicas could be placed.
var ErrNotEnoughNodes = errors.New("placement: not enough eligible nodes")

// Node is the placement view of a storage node.
type Node struct {
	ID       iface.NodeID
	Rack     string
	Used     int64 // bytes stored, from heartbeats
	Alive    bool
	Draining bool
}

// Place returns replica nodes for each chunk, in preference order.
//
// Per chunk: the first replica is the least-loaded eligible node; each next
// replica is the least-loaded node on the racks this chunk uses least, so
// racks fill round-robin (6 EC shards on 3 racks: 2 each), and nodes never
// repeat. Load is bytes used plus bytes placed earlier in this call, so one
// large upload spreads out instead of piling onto the emptiest nodes. Ties
// break through rng.
//
// It returns ErrNotEnoughNodes if any chunk gets fewer than min replicas.
// SIMPLIFIED: one level of failure domain (rack). HDFS and Ceph CRUSH model
// a tree (datacenter, row, rack, host) and spread across each level.
func Place(nodes []Node, sizes []int64, n, min int, rng iface.Rand) ([][]iface.NodeID, error) {
	var eligible []Node
	for _, nd := range nodes {
		if nd.Alive && !nd.Draining {
			eligible = append(eligible, nd)
		}
	}
	// Callers often build nodes from a map; sort so rng sees the same order.
	slices.SortFunc(eligible, func(a, b Node) int { return cmp.Compare(a.ID, b.ID) })

	pending := map[iface.NodeID]int64{}
	load := func(nd Node) int64 { return nd.Used + pending[nd.ID] }
	out := make([][]iface.NodeID, len(sizes))
	for i, size := range sizes {
		chosen := map[iface.NodeID]bool{}
		racks := map[string]int{}
		for len(out[i]) < n {
			nd, ok := pick(eligible, chosen, racks, load, rng)
			if !ok {
				break
			}
			chosen[nd.ID] = true
			racks[nd.Rack]++
			pending[nd.ID] += size
			out[i] = append(out[i], nd.ID)
		}
		if len(out[i]) < min {
			return nil, ErrNotEnoughNodes
		}
	}
	return out, nil
}

// pick returns the least-loaded unchosen node on the least-used racks.
func pick(nodes []Node, chosen map[iface.NodeID]bool, racks map[string]int, load func(Node) int64, rng iface.Rand) (Node, bool) {
	var pool []Node
	least := -1
	for _, nd := range nodes {
		if chosen[nd.ID] {
			continue
		}
		switch used := racks[nd.Rack]; {
		case least < 0 || used < least:
			pool, least = []Node{nd}, used
		case used == least:
			pool = append(pool, nd)
		}
	}
	if len(pool) == 0 {
		return Node{}, false
	}
	best := slices.MinFunc(pool, func(a, b Node) int { return cmp.Compare(load(a), load(b)) })
	var ties []Node
	for _, nd := range pool {
		if load(nd) == load(best) {
			ties = append(ties, nd)
		}
	}
	return ties[rng.IntN(len(ties))], true
}
