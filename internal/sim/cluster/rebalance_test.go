package cluster

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/rebalance"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// unbalanced returns the nodes outside their band of the rack-feasible
// target, computed as the balancer computes it from the located bytes of
// every live file's chunks.
func (c *Cluster) unbalanced(band int64) []string {
	var nodes []rebalance.Node
	for _, n := range c.Meta().Cluster().Nodes() {
		if c.Meta().Cluster().Alive(n.ID) && !c.Meta().State().Leaving(n.ID) {
			nodes = append(nodes, rebalance.Node{ID: n.ID, Rack: n.Rack, Used: c.BytesOn(n.ID)})
		}
	}
	var distinct, largest int64
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, ch := range e.Chunks {
			if !seen[ch] {
				seen[ch] = true
				ci, _ := c.Meta().State().Chunk(ch)
				distinct += ci.Size
				largest = max(largest, ci.Size)
			}
		}
	}
	target := rebalance.Targets(nodes, distinct, c.cfg.Meta.Replicas)
	var out []string
	for _, n := range nodes {
		b := max(target[n.ID]*band/100, 2*largest)
		if d := n.Used - target[n.ID]; d > b || d < -b {
			out = append(out, fmt.Sprintf("%s used %d MiB, target %d MiB", n.ID, n.Used>>20, target[n.ID]>>20))
		}
	}
	return out
}

// A node that lost its disk comes back empty after repair restored RF on
// the others. The balancer refills it with moves, under the repair throttle,
// until every node is within the band, and no chunk drops below RF.
func TestWipedNodeIsRefilled(t *testing.T) {
	c := loaded(t, 3)
	c.KillNode("node-1")
	c.Tick(c.cfg.Meta.Detector.DeadAfter + c.cfg.Meta.Repair.Delay + time.Second)
	if _, ok := c.Settle(2 * c.RepairBound(c.BytesOn("node-2"))); !ok {
		t.Fatalf("RF not restored after node-1 died: %d short", c.UnderReplicated())
	}
	c.WipeNode("node-1")
	c.RestartNode("node-1")
	moved0 := c.Meta().Repair().Stats().MovedBytes
	for range 600 {
		c.Tick(500 * time.Millisecond)
		if n := c.UnderReplicated(); n != 0 {
			t.Fatalf("at %v: %d chunks below RF while balancing", c.Now(), n)
		}
		if c.BytesOn("node-1") > 0 && len(c.unbalanced(10)) == 0 && c.OverReplicated() == 0 {
			break
		}
	}
	if off := c.unbalanced(10); len(off) != 0 {
		t.Fatalf("not balanced after 5 min: %v", off)
	}
	moved := c.Meta().Repair().Stats().MovedBytes - moved0
	t.Logf("refilled node-1 with %d MiB of moves by %v", moved>>20, c.Now())
	if moved == 0 || c.BytesOn("node-1") == 0 {
		t.Fatal("nothing moved onto the returning node")
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

// intactElsewhere is the fewest intact on-disk copies, on running nodes
// other than n and not leaving, of any live file's chunk that n holds.
func (c *Cluster) intactElsewhere(n iface.NodeID) int {
	fewest := c.cfg.Meta.Replicas + 1
	for _, e := range c.Meta().State().List("/") {
		for _, ch := range e.Chunks {
			if !c.Intact(n, ch) {
				continue
			}
			k := 0
			for _, o := range c.Nodes() {
				if o.ID() != n && !c.net.Crashed(o.ID()) && !c.Meta().State().Leaving(o.ID()) && c.Intact(o.ID(), ch) {
					k++
				}
			}
			fewest = min(fewest, k)
		}
	}
	return fewest
}

// decommissionWhenSafe retries decommission every second until the leader
// allows it, failing the test on any other error or after limit. It returns
// how long the drain took.
func (c *Cluster) decommissionWhenSafe(t *testing.T, n iface.NodeID, limit time.Duration) time.Duration {
	t.Helper()
	start := c.Now()
	for {
		_, err := c.Client().NodeAdmin(t.Context(), string(n), "decommissioned")
		if err == nil {
			return c.Now().Sub(start)
		}
		if iface.CodeOf(err) != iface.CodeConflict && iface.CodeOf(err) != iface.CodeNotLeader && iface.CodeOf(err) != iface.CodeUnavailable {
			t.Fatalf("decommission %s: %v", n, err)
		}
		if c.Now().Sub(start) > limit {
			t.Fatalf("decommission %s still refused after %v: %v", n, limit, err)
		}
		if n := c.UnderReplicated(); n != 0 {
			t.Fatalf("at %v: %d chunks below RF while draining", c.Now(), n)
		}
		c.Tick(time.Second)
	}
}

// TestDecommissionOnlyWhenSafe: decommission is refused while the drained
// node's chunks lack RF copies elsewhere, allowed once they have them, and
// the node can then die without a single repair copy.
func TestDecommissionOnlyWhenSafe(t *testing.T) {
	c := loaded(t, 4)
	const n = "node-4"
	held := c.BytesOn(n)
	if _, err := c.Client().NodeAdmin(t.Context(), n, "decommissioned"); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("decommission of an active node: %v, want conflict", err)
	}
	res, err := c.Client().NodeAdmin(t.Context(), n, "draining")
	if err != nil {
		t.Fatal(err)
	}
	if res.Warning != "" {
		t.Fatalf("unexpected warning draining one of two r1 nodes: %s", res.Warning)
	}
	if _, err := c.Client().NodeAdmin(t.Context(), n, "decommissioned"); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("decommission right after drain: %v, want conflict (nothing copied yet)", err)
	}
	took := c.decommissionWhenSafe(t, n, 5*time.Minute)
	if k := c.intactElsewhere(n); k < c.cfg.Meta.Replicas {
		t.Fatalf("decommissioned with a chunk at %d intact copies elsewhere", k)
	}
	t.Logf("drained %d MiB in %v", held>>20, took.Round(time.Second))

	before := c.Meta().Repair().Stats().Repairs
	c.KillNode(n)
	c.Tick(c.cfg.Meta.Detector.DeadAfter + c.cfg.Meta.Repair.Delay + 10*time.Second)
	if got := c.Meta().Repair().Stats().Repairs - before; got != 0 {
		t.Fatalf("killing a decommissioned node cost %d repair copies", got)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}

// The drain is logged state, not the leader's memory: the leader dies
// mid-evacuation and the next one finishes it.
func TestDrainSurvivesLeaderChange(t *testing.T) {
	c := newMetaGroup(t, 2)
	for i := range 12 {
		if _, _, err := c.UploadRandom(fmt.Sprintf("/d/%02d", i), 2<<20); err != nil {
			t.Fatal(err)
		}
	}
	c.Tick(15 * time.Second)
	if _, err := c.Client().NodeAdmin(t.Context(), "node-2", "draining"); err != nil {
		t.Fatal(err)
	}
	c.Tick(time.Second)
	old := c.MetaLeader()
	c.KillMeta(old)
	c.Tick(5 * time.Second)
	if nl := c.MetaLeader(); nl == "" || nl == old {
		t.Fatalf("no new leader after killing %s", old)
	}
	if !c.Meta().State().Leaving("node-2") {
		t.Fatal("the new leader lost the drain")
	}
	c.decommissionWhenSafe(t, "node-2", 5*time.Minute)
	if k := c.intactElsewhere("node-2"); k < c.cfg.Meta.Replicas {
		t.Fatalf("decommissioned with a chunk at %d intact copies elsewhere", k)
	}
	mustAgree(t, c)
}

// The trim watch fails the run when a trim takes a chunk below RF: a
// delete sent past the log, as a buggy leader would, to a chunk at RF.
func TestTrimWatchCatchesUnsafeTrim(t *testing.T) {
	c := New(1, DefaultConfig(), io.Discard)
	c.Tick(3 * time.Second)
	_, _, err := c.UploadRandom("/f", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatal("not at RF after upload")
	}
	ch := c.Meta().State().List("/")[0].Chunks[0]
	holder := c.Meta().Cluster().Locations(ch)[0]
	c.net.Send(holder, iface.Message{From: MetaID, Kind: wire.KindDeleteReplica,
		Body: wire.Marshal(&chunkdv1.DeleteReplica{ChunkId: ch[:], Term: 1 << 40})})
	c.Tick(time.Second)
	if err := c.AssertInvariants(); err == nil || !strings.Contains(err.Error(), "a trim on "+string(holder)) {
		t.Fatalf("AssertInvariants = %v, want the unsafe trim on %s", err, holder)
	}
}

// A follower that missed a node's report of a trimmed copy must not count
// that copy once it leads: the committed TrimDone says the copy is gone.
// Node-1 returns after repair, its surplus copies are trimmed while the
// followers hear no reports, and the leader dies before the next full report
// would have corrected them. The new leader must not trim a real copy.
func TestNewLeaderTrustsLoggedTrimDone(t *testing.T) {
	c := newMetaGroup(t, 3)
	for i := range 6 {
		if _, _, err := c.UploadRandom(fmt.Sprintf("/t/%d", i), 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.Settle(time.Minute); !ok {
		t.Fatal("not at RF after uploads")
	}
	c.KillNode("node-1")
	c.Tick(c.cfg.Meta.Detector.DeadAfter + c.cfg.Meta.Repair.Delay + time.Second)
	if _, ok := c.Settle(2 * c.RepairBound(c.BytesOn("node-2"))); !ok {
		t.Fatal("RF not restored after node-1 died")
	}
	old := c.MetaLeader()
	var followers []iface.NodeID
	for _, p := range c.MetaIDs() {
		if p != old {
			followers = append(followers, p)
		}
	}
	reports := func(on bool) {
		for _, n := range c.Nodes() {
			for _, f := range followers {
				if on {
					c.net.Unblock(n.ID(), f)
				} else {
					c.net.Block(n.ID(), f)
				}
			}
		}
	}
	c.RestartNode("node-1")
	blocked := false
	for range 600 {
		c.Tick(20 * time.Millisecond)
		pending := len(c.MetaPeer(old).State().TrimPendingAll())
		if !blocked && pending > 0 {
			reports(false)
			blocked = true
		}
		if blocked && pending == 0 {
			break
		}
	}
	reports(true)
	if !blocked || c.MetaPeer(old).Trims().Done == 0 {
		t.Fatalf("no trim ran while the followers were cut off (blocked %v)", blocked)
	}
	c.KillMeta(old)
	c.Tick(20 * time.Second)
	if nl := c.MetaLeader(); nl == "" || nl == old {
		t.Fatalf("no new leader after killing %s", old)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}
