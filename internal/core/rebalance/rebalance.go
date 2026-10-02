// Package rebalance plans replica moves that even out bytes stored across
// storage nodes without weakening rack spread (ADR-0020). It is pure: the
// caller passes the cluster as it sees it and gets back a list of moves.
package rebalance

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Node is a storage node that may take part in balancing: alive, confirmed
// and not leaving. Leaving nodes are drained, not balanced.
type Node struct {
	ID   iface.NodeID
	Rack string
	Used int64 // bytes of the chunks the planner is given that this node holds
}

// Chunk is a replicated chunk and the participating nodes that hold it.
type Chunk struct {
	ID      iface.ChunkID
	Size    int64
	Holders []iface.NodeID
	// Pinned chunks count toward the targets but never move: a copy, a trim
	// or a repair is under way, or the chunk is not at exactly RF.
	Pinned bool
}

// Move copies Chunk from From to To; the copy at From is then trimmed.
type Move struct {
	Chunk    iface.ChunkID
	Size     int64
	From, To iface.NodeID
}

// Config sets the replication factor and the band.
type Config struct {
	Replicas int
	// BandPercent of a node's target is the tolerated distance from it;
	// never less than two of the largest chunk.
	BandPercent int64
}

// DefaultConfig: RF 3, ±10%.
func DefaultConfig() Config { return Config{Replicas: 3, BandPercent: 10} }

// Targets returns each node's rack-feasible share of the bytes stored.
//
// A rack can hold at most ceil(RF/racks) copies of a chunk, so its share is
// capped at distinct × ceil(RF/racks). Racks whose proportional share
// (by node count) exceeds the cap take the cap; the rest is shared by the
// others, repeated until no rack is over its cap (water-filling). A node's
// target is its rack's share over the rack's node count.
func Targets(nodes []Node, distinct int64, replicas int) map[iface.NodeID]int64 {
	perRack := map[string][]iface.NodeID{}
	var total int64
	for _, n := range nodes {
		perRack[n.Rack] = append(perRack[n.Rack], n.ID)
		total += n.Used
	}
	racks := slices.Sorted(func(yield func(string) bool) {
		for r := range perRack {
			if !yield(r) {
				return
			}
		}
	})
	out := map[iface.NodeID]int64{}
	if len(racks) == 0 {
		return out
	}
	rackCap := distinct * int64((replicas+len(racks)-1)/len(racks))
	share := map[string]int64{}
	open := racks
	remaining := total
	for len(open) > 0 {
		var count int64
		for _, r := range open {
			count += int64(len(perRack[r]))
		}
		var capped, rest []string
		for _, r := range open {
			if remaining*int64(len(perRack[r]))/count > rackCap {
				capped = append(capped, r)
			} else {
				rest = append(rest, r)
			}
		}
		if len(capped) == 0 {
			for _, r := range open {
				share[r] = remaining * int64(len(perRack[r])) / count
			}
			break
		}
		for _, r := range capped {
			share[r] = rackCap
			remaining -= rackCap
		}
		open = rest
	}
	for _, r := range racks {
		for _, id := range perRack[r] {
			out[id] = share[r] / int64(len(perRack[r]))
		}
	}
	return out
}

// L1 is the total distance of used bytes from target: twice the minimum any
// sequence of moves must copy to reach the targets.
func L1(nodes []Node, target map[iface.NodeID]int64) int64 {
	var d int64
	for _, n := range nodes {
		d += abs(n.Used - target[n.ID])
	}
	return d
}

// Balanced reports whether every node is within its band.
func Balanced(nodes []Node, chunks []Chunk, cfg Config) bool {
	target, band := Frame(nodes, chunks, cfg)
	for _, n := range nodes {
		if abs(n.Used-target[n.ID]) > band(n.ID) {
			return false
		}
	}
	return true
}

// Frame returns each node's target and its band: the distance from target the
// planner tolerates, BandPercent of the target but at least two of the
// largest chunk.
func Frame(nodes []Node, chunks []Chunk, cfg Config) (map[iface.NodeID]int64, func(iface.NodeID) int64) {
	var distinct, largest int64
	for _, c := range chunks {
		distinct += c.Size
		largest = max(largest, c.Size)
	}
	target := Targets(nodes, distinct, cfg.Replicas)
	return target, func(id iface.NodeID) int64 { return max(target[id]*cfg.BandPercent/100, 2*largest) }
}

// Plan returns up to limit moves (all if limit <= 0) that bring the nodes
// toward their targets.
//
// It acts only while some node is outside its band. A move of chunk c from
// src to dst needs src above its target and dst below, dst not holding c,
// the excess at src plus the deficit at dst larger than c (so L1 strictly
// drops and no copy ever moves back), and no loss of a distinct rack for c.
// Sources are taken most-over first, destinations most-under first; for a
// pair, the largest chunk that fits both gaps, else the smallest allowed.
// Ties break by ID, so the plan is a function of its input.
func Plan(nodes []Node, chunks []Chunk, cfg Config, limit int) []Move {
	if Balanced(nodes, chunks, cfg) {
		return nil
	}
	target, _ := Frame(nodes, chunks, cfg)
	used := map[iface.NodeID]int64{}
	rack := map[iface.NodeID]string{}
	ids := make([]iface.NodeID, 0, len(nodes))
	for _, n := range nodes {
		used[n.ID], rack[n.ID] = n.Used, n.Rack
		ids = append(ids, n.ID)
	}
	slices.Sort(ids)
	// on[n]: chunks n holds, as indexes into cs; holders are mutated as moves
	// are planned so later moves see earlier ones.
	cs := make([]Chunk, len(chunks))
	on := map[iface.NodeID][]int{}
	for i, c := range chunks {
		cs[i] = Chunk{ID: c.ID, Size: c.Size, Holders: slices.Clone(c.Holders)}
		if c.Pinned {
			continue
		}
		for _, h := range c.Holders {
			if _, ok := used[h]; ok {
				on[h] = append(on[h], i)
			}
		}
	}
	diff := func(id iface.NodeID) int64 { return used[id] - target[id] }

	var moves []Move
	for limit <= 0 || len(moves) < limit {
		srcs := slices.DeleteFunc(slices.Clone(ids), func(id iface.NodeID) bool { return diff(id) <= 0 })
		dsts := slices.DeleteFunc(slices.Clone(ids), func(id iface.NodeID) bool { return diff(id) >= 0 })
		slices.SortStableFunc(srcs, func(a, b iface.NodeID) int { return cmp.Compare(diff(b), diff(a)) })
		slices.SortStableFunc(dsts, func(a, b iface.NodeID) int { return cmp.Compare(diff(a), diff(b)) })
		m, ok := pick(srcs, dsts, cs, on, rack, diff)
		if !ok {
			break
		}
		moves = append(moves, m)
		used[m.From] -= m.Size
		used[m.To] += m.Size
		for _, i := range on[m.From] {
			if cs[i].ID == m.Chunk {
				h := cs[i].Holders
				h[slices.Index(h, m.From)] = m.To
				on[m.To] = append(on[m.To], i)
				on[m.From] = slices.DeleteFunc(on[m.From], func(j int) bool { return j == i })
				break
			}
		}
	}
	return moves
}

func pick(srcs, dsts []iface.NodeID, cs []Chunk, on map[iface.NodeID][]int, rack map[iface.NodeID]string, diff func(iface.NodeID) int64) (Move, bool) {
	for _, s := range srcs {
		for _, d := range dsts {
			excess, deficit := diff(s), -diff(d)
			var fit, small *Chunk
			for _, i := range on[s] {
				c := &cs[i]
				if c.Size >= excess+deficit || !movable(c, s, d, rack) {
					continue
				}
				if c.Size <= min(excess, deficit) && (fit == nil || before(c, fit, true)) {
					fit = c
				}
				if small == nil || before(c, small, false) {
					small = c
				}
			}
			if c := cmp.Or(fit, small); c != nil {
				return Move{Chunk: c.ID, Size: c.Size, From: s, To: d}, true
			}
		}
	}
	return Move{}, false
}

// before orders candidate chunks: by size (descending if large), then ID.
func before(a, b *Chunk, large bool) bool {
	if a.Size != b.Size {
		return (a.Size > b.Size) == large
	}
	return bytes.Compare(a.ID[:], b.ID[:]) < 0
}

// movable: dst holds no copy, and moving src's copy keeps the number of
// distinct racks holding the chunk.
func movable(c *Chunk, src, dst iface.NodeID, rack map[iface.NodeID]string) bool {
	if slices.Contains(c.Holders, dst) {
		return false
	}
	if rack[src] == rack[dst] {
		return true
	}
	srcRackOthers, dstRackHas := false, false
	for _, h := range c.Holders {
		if h == src {
			continue
		}
		srcRackOthers = srcRackOthers || rack[h] == rack[src]
		dstRackHas = dstRackHas || rack[h] == rack[dst]
	}
	// Losing src's rack is fine only if dst's rack is new to the chunk.
	return srcRackOthers || !dstRackHas
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
