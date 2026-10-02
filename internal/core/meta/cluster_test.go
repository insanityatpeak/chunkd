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

// rfull, radd and rdel send incarnation-1 reports with the given seq.
func rfull(c *Cluster, id iface.NodeID, seq uint64, chunks ...iface.ChunkID) {
	c.Report(id, Report{Incarnation: 1, Seq: seq, Full: true, Added: chunks})
}

func radd(c *Cluster, id iface.NodeID, seq uint64, chunks ...iface.ChunkID) {
	c.Report(id, Report{Incarnation: 1, Seq: seq, Added: chunks})
}

func rdel(c *Cluster, id iface.NodeID, seq uint64, chunks ...iface.ChunkID) {
	c.Report(id, Report{Incarnation: 1, Seq: seq, Deleted: chunks})
}

func TestClusterLocations(t *testing.T) {
	c := NewCluster(detector.DefaultConfig())
	a, b := iface.ChunkID{1}, iface.ChunkID{2}

	if !beat(c, "n1", 1, 0) {
		t.Fatal("new node not asked for a full report")
	}
	rfull(c, "n1", 1, a)
	if beat(c, "n1", 2, 1) {
		t.Fatal("full report requested again after one arrived")
	}
	beat(c, "n2", 1, 1)
	radd(c, "n2", 1, a, b)

	if got := c.Locations(a); !slices.Equal(got, []iface.NodeID{"n1", "n2"}) {
		t.Fatalf("Locations(a) = %v", got)
	}
	// A full report replaces: n2 lost chunk a (disk replaced, say).
	rfull(c, "n2", 2, b)
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
	rfull(c, "n1", 1, a)
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
	if v := c.PlacementView(func(iface.NodeID) bool { return false }); len(v) != 1 || v[0].Alive {
		t.Fatalf("suspect node eligible for placement: %+v", v)
	}
	if got := c.Locations(a); len(got) != 1 {
		t.Fatal("suspect node lost its locations")
	}

	tickTo(sec(10.5))
	if c.Readable("n1") {
		t.Fatal("dead node readable")
	}
	if n, _ := c.Node("n1"); n.DeadSince != sec(10.5) {
		t.Fatalf("DeadSince = %v", n.DeadSince)
	}
	// Kept, so repair can tell a recent death from an old one.
	if got := c.Locations(a); len(got) != 1 {
		t.Fatalf("dead node's locations dropped: %v", got)
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

func TestClusterRestartKeepsLocationsUntilReport(t *testing.T) {
	c := NewCluster(detector.DefaultConfig())
	beat(c, "n1", 1, 0)
	rfull(c, "n1", 7, iface.ChunkID{1}, iface.ChunkID{2})
	// Restarted within the suspect window: new incarnation, seq back at 1.
	_, _, need := c.Heartbeat(NodeState{ID: "n1"}, detector.Beat{Incarnation: 2, Seq: 1}, iface.Instant(time.Second))
	if !need {
		t.Fatal("restarted node not asked for a full report")
	}
	if c.Alive("n1") || len(c.Locations(iface.ChunkID{1})) != 1 {
		t.Fatal("restarted node should be suspect with its locations kept")
	}
	// A late report from the killed process is ignored.
	if _, _, ok := c.Report("n1", Report{Incarnation: 1, Seq: 8, Deleted: []iface.ChunkID{{1}}}); ok || len(c.Locations(iface.ChunkID{1})) != 1 {
		t.Fatal("report from the old incarnation applied")
	}
	// Its disk lost chunk 2: the report, not the restart, removes it. The
	// new incarnation's seq starts again at 1, below the old 7.
	c.Report("n1", Report{Incarnation: 2, Seq: 1, Full: true, Added: []iface.ChunkID{{1}}})
	if len(c.Locations(iface.ChunkID{2})) != 0 || len(c.Locations(iface.ChunkID{1})) != 1 {
		t.Fatal("full report did not replace locations")
	}
}

// TestClusterReportOrdering: the network reorders a node's reports. A full
// report with seq S reflects exactly the changes numbered below S; newer
// incremental changes win over it, and older ones are already in it.
func TestClusterReportOrdering(t *testing.T) {
	x, y := iface.ChunkID{1}, iface.ChunkID{2}
	for _, tc := range []struct {
		name    string
		reports func(c *Cluster) // in arrival order
		want    bool             // n1 holds x afterwards
	}{
		{"put finished after the listing; its report overtakes the full report",
			func(c *Cluster) { radd(c, "n1", 5, x); rfull(c, "n1", 4, y) }, true},
		{"trim deleted after the listing; its report overtakes the full report",
			func(c *Cluster) { rfull(c, "n1", 3, x); rdel(c, "n1", 5, x); rfull(c, "n1", 4, x, y) }, false},
		{"in order: a newer full report drops a chunk deleted before it",
			func(c *Cluster) { radd(c, "n1", 5, x); rfull(c, "n1", 6, y) }, false},
		{"a late add older than the full report is ignored",
			func(c *Cluster) { rfull(c, "n1", 6, y); radd(c, "n1", 5, x) }, false},
		{"a late delete older than the full report is ignored",
			func(c *Cluster) { rfull(c, "n1", 6, x); rdel(c, "n1", 5, x) }, true},
		{"an older full report after a newer one is ignored",
			func(c *Cluster) { rfull(c, "n1", 6, x); rfull(c, "n1", 4, y) }, true},
		{"re-added after a delete, both newer than the full report",
			func(c *Cluster) { rdel(c, "n1", 5, x); radd(c, "n1", 7, x); rfull(c, "n1", 6, y) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCluster(detector.DefaultConfig())
			beat(c, "n1", 1, 0)
			tc.reports(c)
			if got := slices.Contains(c.Locations(x), "n1"); got != tc.want {
				t.Fatalf("n1 holds x: %v, want %v", got, tc.want)
			}
		})
	}
}
