package meta

import (
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

func beat(c *Cluster, id iface.NodeID, seq uint64, now iface.Instant) (needFull bool) {
	_, _, needFull = c.Heartbeat(NodeState{ID: id, Rack: "r1"}, detector.Beat{Incarnation: 1, Seq: seq}, now)
	return needFull
}

func TestClusterLocations(t *testing.T) {
	c := NewCluster(detector.DefaultConfig())
	a, b := iface.ChunkID{1}, iface.ChunkID{2}

	if !beat(c, "n1", 1, 0) {
		t.Fatal("new node not asked for a full report")
	}
	c.FullReport("n1", []iface.ChunkID{a})
	if beat(c, "n1", 2, 1) {
		t.Fatal("full report requested again after one arrived")
	}
	beat(c, "n2", 1, 1)
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

func TestClusterLiveness(t *testing.T) {
	c := NewCluster(detector.DefaultConfig())
	a := iface.ChunkID{1}
	beat(c, "n1", 1, 0)
	c.FullReport("n1", []iface.ChunkID{a})
	sec := func(s float64) iface.Instant { return iface.Instant(s * float64(time.Second)) }
	// Tick at the owner's cadence: a jump would read as a stall of the owner.
	next := sec(0)
	tickTo := func(end iface.Instant) {
		for ; next <= end; next = next.Add(500 * time.Millisecond) {
			c.Tick(next)
		}
	}

	tickTo(sec(3.5))
	if c.Alive("n1") || !c.Readable("n1") {
		t.Fatal("suspect node should be readable but not alive")
	}
	if v := c.PlacementView(); len(v) != 1 || v[0].Alive {
		t.Fatalf("suspect node eligible for placement: %+v", v)
	}
	if got := c.Locations(a); len(got) != 1 {
		t.Fatal("suspect node lost its locations")
	}

	tickTo(sec(10.5))
	if c.Readable("n1") {
		t.Fatal("dead node readable")
	}
	if got := c.Locations(a); len(got) != 0 {
		t.Fatalf("dead node still listed for chunk: %v", got)
	}
	// Back: it must send a full report before its chunks count again.
	if !beat(c, "n1", 2, sec(20)) {
		t.Fatal("returning node not asked for a full report")
	}
	if c.Alive("n1") {
		t.Fatal("returning node alive after one beat")
	}
	beat(c, "n1", 3, sec(21))
	beat(c, "n1", 4, sec(22))
	if !c.Alive("n1") {
		t.Fatal("not alive after 3 on-time beats")
	}
	if c.Alive("ghost") || c.Readable("ghost") {
		t.Fatal("unknown node alive")
	}
}

func TestClusterRestartForgetsLocations(t *testing.T) {
	c := NewCluster(detector.DefaultConfig())
	beat(c, "n1", 1, 0)
	c.FullReport("n1", []iface.ChunkID{{1}})
	// Restarted within the suspect window: new incarnation, seq back at 1.
	_, _, need := c.Heartbeat(NodeState{ID: "n1"}, detector.Beat{Incarnation: 2, Seq: 1}, iface.Instant(time.Second))
	if !need || len(c.Locations(iface.ChunkID{1})) != 0 {
		t.Fatal("restarted node kept stale locations")
	}
}
