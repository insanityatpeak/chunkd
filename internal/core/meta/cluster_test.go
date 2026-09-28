package meta

import (
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func TestClusterLocations(t *testing.T) {
	c := NewCluster()
	a, b := iface.ChunkID{1}, iface.ChunkID{2}

	if !c.Heartbeat(NodeState{ID: "n1", Rack: "r1"}, 0) {
		t.Fatal("new node not asked for a full report")
	}
	c.FullReport("n1", []iface.ChunkID{a})
	if c.Heartbeat(NodeState{ID: "n1", Rack: "r1"}, 1) {
		t.Fatal("full report requested again after one arrived")
	}
	c.Heartbeat(NodeState{ID: "n2", Rack: "r2"}, 1)
	c.Received("n2", []iface.ChunkID{a, b})

	if got := c.Locations(a); !slices.Equal(got, []iface.NodeID{"n1", "n2"}) {
		t.Fatalf("Locations(a) = %v", got)
	}
	// A full report replaces: n2 lost chunk a (disk replaced, say).
	c.FullReport("n2", []iface.ChunkID{b})
	if got := c.Locations(a); !slices.Equal(got, []iface.NodeID{"n1"}) {
		t.Fatalf("after full report Locations(a) = %v, want [n1]", got)
	}
	if got := c.Locations(iface.ChunkID{9}); len(got) != 0 {
		t.Fatalf("unknown chunk has locations %v", got)
	}
}

func TestClusterAlive(t *testing.T) {
	c := NewCluster()
	c.Heartbeat(NodeState{ID: "n1"}, 0)
	dead := 3 * time.Second
	if !c.Alive("n1", iface.Instant(dead), dead) {
		t.Fatal("dead exactly at the boundary")
	}
	if c.Alive("n1", iface.Instant(dead+1), dead) {
		t.Fatal("alive past dead-after")
	}
	if c.Alive("ghost", 0, dead) {
		t.Fatal("unknown node alive")
	}
	view := c.PlacementView(iface.Instant(dead+1), dead)
	if len(view) != 1 || view[0].Alive {
		t.Fatalf("placement view %+v", view)
	}
}
