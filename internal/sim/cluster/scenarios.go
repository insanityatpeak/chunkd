package cluster

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Scenario is a scripted failure the dashboard can start and share as a
// link: ?seed=N&scenario=Name replays it exactly, because the script runs
// on the simulation clock from the moment the cluster starts.
type Scenario struct {
	Name  string `json:"name"`
	Title string `json:"title"`
	What  string `json:"what"`
}

// Scenarios lists the scripts, in the order the dashboard shows them.
var Scenarios = []Scenario{
	{"kill-node", "Kill a node", "node-3 dies with a replica of every chunk. Suspect at 3 s, dead at 10 s, repair after a 20 s delay, then it returns at 90 s and its extra copies are trimmed."},
	{"corrupt-chunk", "Silent bit rot", "Bytes flip in 3 chunks on node-2's disk and nobody is told. The scrubber (or the first reader) finds them, quarantines the copies and repair replaces them."},
	{"rack-loss", "Lose a rack", "Rack r1 (node-1 and node-4) goes down at once. Rack-aware placement kept at most one copy per rack, so every chunk survives with 2 copies; repair rebuilds the third."},
	{"slow-node", "Slow node", "node-4 answers 2 s late for a minute but keeps heartbeating, so the detector keeps it alive. Reads every 5 s show hedging: the client asks a second replica and learns to avoid node-4."},
}

// RunScenario starts a named script.
func (c *Cluster) RunScenario(name string) error {
	switch name {
	case "kill-node":
		return c.ScriptKillNode("node-3", 90*time.Second)
	case "corrupt-chunk":
		_, err := c.ScriptRot("node-2", 3)
		return err
	case "rack-loss":
		if err := c.demoFiles(); err != nil {
			return err
		}
		for _, n := range c.nodes {
			if n.Rack == "r1" {
				c.KillNode(n.ID())
			}
		}
		return nil
	case "slow-node":
		if err := c.demoFiles(); err != nil {
			return err
		}
		c.net.SetSlow("node-4", 2*time.Second)
		c.clock.AfterFunc(60*time.Second, func() { c.net.SetSlow("node-4", 0) })
		files := c.meta.State().List("/demo/")
		for i := range 14 {
			c.reads = append(c.reads, scriptedRead{at: c.clock.Now().Add(time.Duration(i+1) * 5 * time.Second), path: files[i%len(files)].Path})
		}
		return nil
	}
	return iface.Errorf(iface.CodeInvalid, "unknown scenario %q", name)
}

type scriptedRead struct {
	at   iface.Instant
	path string
}

// scriptRead downloads path and records how it went as a "read" event.
func (c *Cluster) scriptRead(path string) {
	start := c.clock.Now()
	data, m, err := c.Download(path)
	took := c.clock.Now().Sub(start).Round(time.Millisecond)
	c.readSeq++
	e := client.Event{Seq: c.readSeq, AtMs: int64(c.clock.Now()) / int64(time.Millisecond), Kind: "read", Node: "client"}
	switch {
	case err != nil:
		e.Text = fmt.Sprintf("read %s failed after %v: %v", path, took, err)
	default:
		var hedged, served []string
		for _, ch := range m.Chunk {
			served = append(served, ch.ServedBy)
			if ch.Hedged {
				hedged = append(hedged, fmt.Sprint(ch.Index))
			}
		}
		slices.Sort(served)
		e.Text = fmt.Sprintf("read %s (%d KiB) in %v from %s", path, len(data)>>10, took, strings.Join(slices.Compact(served), ", "))
		if len(hedged) > 0 {
			e.Text += fmt.Sprintf("; hedged chunk %s", strings.Join(hedged, ", "))
		}
	}
	c.readLog = append(c.readLog, e)
	if len(c.readLog) > 100 {
		c.readLog = c.readLog[1:]
	}
}

// Freeze pauses a node (messages held until Thaw); Thaw releases it.
func (c *Cluster) Freeze(id iface.NodeID) { c.net.Freeze(id) }
func (c *Cluster) Thaw(id iface.NodeID)   { c.net.Thaw(id) }

// SetSlow adds d to every message to or from id; 0 clears it.
func (c *Cluster) SetSlow(id iface.NodeID, d time.Duration) { c.net.SetSlow(id, d) }

// Partition cuts id off from the metadata server in both directions, or
// heals that cut. Clients can still reach the node: it looks dead to the
// metadata server but keeps serving reads.
func (c *Cluster) Partition(id iface.NodeID, on bool) {
	if on {
		c.net.Partition([]iface.NodeID{id}, []iface.NodeID{MetaID})
		return
	}
	c.net.Unblock(id, MetaID)
	c.net.Unblock(MetaID, id)
}

// CorruptReplica flips a byte in node's copy of chunk (hex ID).
func (c *Cluster) CorruptReplica(id iface.NodeID, chunkHex string) error {
	ch, err := parseChunkID(chunkHex)
	if err != nil {
		return err
	}
	if !c.node(id).Store.Corrupt(ch) {
		return iface.Errorf(iface.CodeNotFound, "%s holds no copy of %s", id, chunkHex[:min(12, len(chunkHex))])
	}
	return nil
}

func parseChunkID(s string) (iface.ChunkID, error) {
	var id iface.ChunkID
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(id) {
		return id, iface.Errorf(iface.CodeInvalid, "bad chunk id %q", s)
	}
	copy(id[:], raw)
	return id, nil
}
