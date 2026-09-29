package meta

import (
	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Health summarizes replication across the cluster.
type Health struct {
	// Nodes counts storage nodes by detector state.
	Nodes map[string]int `json:"nodes"`
	// Replicas[i] is how many wanted chunks have i copies on alive nodes;
	// the last bucket also counts every chunk with more.
	Replicas        []int        `json:"replicas"`
	Chunks          int          `json:"chunks"`
	UnderReplicated int          `json:"underReplicated"`
	OverReplicated  int          `json:"overReplicated"`
	Lost            int          `json:"lost"` // no copy on any alive or suspect node
	Repair          repair.Stats `json:"repair"`
	DetectorStalls  uint64       `json:"detectorStalls"`
}

// Health walks every wanted chunk. O(chunks); called about once a second.
func (s *Server) Health() Health {
	h := Health{Nodes: map[string]int{}, Replicas: make([]int, s.cfg.Replicas+2)}
	for _, n := range s.cluster.Nodes() {
		h.Nodes[n.State.String()]++
	}
	s.state.Chunks(func(id iface.ChunkID, ci ChunkInfo) {
		if ci.Refcount == 0 {
			return
		}
		h.Chunks++
		alive, readable := 0, 0
		for _, n := range s.cluster.Locations(id) {
			switch st, _ := s.cluster.Node(n); st.State {
			case detector.Alive:
				alive++
				readable++
			case detector.Suspect:
				readable++
			}
		}
		h.Replicas[min(alive, len(h.Replicas)-1)]++
		switch {
		case readable == 0:
			h.Lost++
			h.UnderReplicated++
		case alive < s.cfg.Replicas:
			h.UnderReplicated++
		case alive > s.cfg.Replicas:
			h.OverReplicated++
		}
	})
	h.Repair = s.repair.Stats()
	h.DetectorStalls = s.cluster.Detector().Stalls()
	return h
}
