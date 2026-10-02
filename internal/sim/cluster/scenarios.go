package cluster

import (
	"context"
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
	{"gc", "Delete and collect", "5 s after the demo files load, one is overwritten with its last chunk changed: the first chunk is already stored, so only the new one is sent. 5 s later another file is deleted. Both old versions stay restorable for 3 epochs of 30 s; then their chunks lose the last reference, and after a 60 s grace the sweep deletes the copies."},
	{"kill-leader", "Kill the metadata leader", "10 s after the demo files load, the metadata leader dies. About a second later a follower notices the silence, wins a pre-vote and then the election, and leads the next term. A write sent just after the kill goes to the dead leader first: it waits out the client's 10 s call timeout, then retries and commits on the new leader. Reads go on throughout. At 60 s the old leader returns as a follower and catches up from the new leader's log."},
	{"add-node", "Add a node", "5 s after the demo files load, node-6 joins empty on rack r3. Once the detector has confirmed it, the balancer gives every node a rack-feasible target and moves copies onto node-6, a few at a time beside repair. Each move copies first and trims the source only after the new copy is confirmed, so no chunk drops below 3 copies. It stops once every node is within its band."},
	{"drain", "Drain a node", "5 s after the demo files load, the operator drains node-4: it takes no new chunks and its copies move to other nodes, rack spread kept. 25 s later every chunk has 3 copies elsewhere and node-4 is decommissioned: it could be switched off now (with a copy still missing, the leader refuses and the timeline says so). 30 s after that it is undrained: its copies count again, the surplus is trimmed, and the balancer evens out the bytes."},
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
		files := c.Meta().State().List("/demo/")
		for i := range 14 {
			p := files[i%len(files)].Path
			c.after(time.Duration(i+1)*5*time.Second, func() { c.scriptRead(p) })
		}
		return nil
	case "gc":
		if err := c.demoFiles(); err != nil {
			return err
		}
		c.after(5*time.Second, func() { c.scriptEdit("/demo/file-0.bin") })
		c.after(10*time.Second, func() { c.scriptDelete("/demo/file-7.bin") })
		return nil
	case "kill-leader":
		return c.scriptKillLeader()
	case "add-node":
		if err := c.demoFiles(); err != nil {
			return err
		}
		c.after(5*time.Second, func() {
			if id, err := c.AddNode(); err != nil {
				c.logClient("admin", fmt.Sprintf("add node failed: %v", err))
			} else {
				c.logClient("admin", fmt.Sprintf("%s started, empty", id))
			}
		})
		return nil
	case "drain":
		if err := c.demoFiles(); err != nil {
			return err
		}
		c.after(5*time.Second, func() { c.scriptAdmin("node-4", "draining") })
		c.after(30*time.Second, func() { c.scriptAdmin("node-4", "decommissioned") })
		c.after(60*time.Second, func() { c.scriptAdmin("node-4", "active") })
		return nil
	}
	return iface.Errorf(iface.CodeInvalid, "unknown scenario %q", name)
}

type scriptedStep struct {
	at  iface.Instant
	run func()
}

// after schedules a client call d from now. It runs from Tick, never from a
// timer: a client call drives the simulation and must not nest in it.
func (c *Cluster) after(d time.Duration, run func()) {
	c.script = append(c.script, scriptedStep{at: c.clock.Now().Add(d), run: run})
}

// logClient records a scripted client call's result in the reads log.
func (c *Cluster) logClient(kind, text string) {
	c.readSeq++
	c.readLog = append(c.readLog, client.Event{Seq: c.readSeq, AtMs: int64(c.clock.Now()) / int64(time.Millisecond), Kind: kind, Node: "client", Text: text})
	if len(c.readLog) > 100 {
		c.readLog = c.readLog[1:]
	}
}

// scriptEdit rewrites path with its last byte changed: every chunk but the
// last is already stored, so the client sends only that one.
func (c *Cluster) scriptEdit(path string) {
	data, _, err := c.Download(path)
	if err == nil && len(data) == 0 {
		err = iface.Errorf(iface.CodeInvalid, "%s is empty", path)
	}
	if err != nil {
		c.logClient("write", fmt.Sprintf("edit %s failed: %v", path, err))
		return
	}
	data[len(data)-1] ^= 0xff
	m, _, err := c.Upload(path, data)
	if err != nil {
		c.logClient("write", fmt.Sprintf("write %s failed: %v", path, err))
		return
	}
	deduped := 0
	for _, ch := range m.Chunk {
		if ch.Deduped {
			deduped++
		}
	}
	c.logClient("write", fmt.Sprintf("write %s v%d, last byte changed: %d of %d chunks already stored, %d sent", path, m.Version, deduped, len(m.Chunk), len(m.Chunk)-deduped))
}

// scriptDelete deletes path; its last version stays restorable.
func (c *Cluster) scriptDelete(path string) {
	if err := c.Delete(path); err != nil {
		c.logClient("write", fmt.Sprintf("delete %s failed: %v", path, err))
		return
	}
	c.logClient("write", fmt.Sprintf("delete %s: a delete marker; the old version can be undeleted until it expires", path))
}

// scriptRead downloads path and records how it went as a "read" event.
func (c *Cluster) scriptRead(path string) {
	start := c.clock.Now()
	data, m, err := c.Download(path)
	took := c.clock.Now().Sub(start).Round(time.Millisecond)
	var text string
	switch {
	case err != nil:
		text = fmt.Sprintf("read %s failed after %v: %v", path, took, err)
	default:
		var hedged, served []string
		for _, ch := range m.Chunk {
			served = append(served, ch.ServedBy)
			if ch.Hedged {
				hedged = append(hedged, fmt.Sprint(ch.Index))
			}
		}
		slices.Sort(served)
		text = fmt.Sprintf("read %s (%d KiB) in %v from %s", path, len(data)>>10, took, strings.Join(slices.Compact(served), ", "))
		if len(hedged) > 0 {
			text += fmt.Sprintf("; hedged chunk %s", strings.Join(hedged, ", "))
		}
	}
	c.logClient("read", text)
}

// Freeze pauses a node (messages held until Thaw); Thaw releases it.
func (c *Cluster) Freeze(id iface.NodeID) { c.net.Freeze(id) }
func (c *Cluster) Thaw(id iface.NodeID)   { c.net.Thaw(id) }

// SetSlow adds d to every message to or from id; 0 clears it.
func (c *Cluster) SetSlow(id iface.NodeID, d time.Duration) { c.net.SetSlow(id, d) }

// Partition cuts id off from the metadata servers in both directions, or
// heals that cut. Clients can still reach the node: it looks dead to the
// metadata servers but keeps serving reads.
func (c *Cluster) Partition(id iface.NodeID, on bool) {
	if on {
		c.net.Partition([]iface.NodeID{id}, c.MetaIDs())
		return
	}
	for _, m := range c.MetaIDs() {
		c.net.Unblock(id, m)
		c.net.Unblock(m, id)
	}
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

// scriptKillLeader kills whichever peer leads 10 s after the demo files
// load and revives it at 60 s. Scripted reads and writes span the failover.
func (c *Cluster) scriptKillLeader() error {
	if len(c.metas) < 3 {
		return iface.Errorf(iface.CodeInvalid, "kill-leader needs a group of 3 metadata peers, have %d", len(c.metas))
	}
	if err := c.demoFiles(); err != nil {
		return err
	}
	var victim iface.NodeID
	c.clock.AfterFunc(10*time.Second, func() {
		if victim = c.MetaLeader(); victim != "" {
			c.KillMeta(victim)
		}
	})
	c.clock.AfterFunc(60*time.Second, func() {
		if victim != "" {
			_ = c.ReviveMeta(victim) // an in-memory log cannot fail to recover
		}
	})
	// In time order: Tick runs scripted calls first in, first out.
	c.after(5*time.Second, func() { c.scriptRead("/demo/file-0.bin") })
	c.after(10500*time.Millisecond, func() { c.scriptPut("/demo/during-election.bin", 1<<20) })
	c.after(25*time.Second, func() { c.scriptRead("/demo/file-1.bin") })
	c.after(30*time.Second, func() { c.scriptPut("/demo/new-leader.bin", 1<<20) })
	c.after(40*time.Second, func() { c.scriptRead("/demo/file-2.bin") })
	c.after(70*time.Second, func() { c.scriptRead("/demo/file-3.bin") })
	c.after(75*time.Second, func() { c.scriptPut("/demo/after-return.bin", 1<<20) })
	return nil
}

// scriptPut writes size random bytes to path and records how long it took.
func (c *Cluster) scriptPut(path string, size int64) {
	start := c.clock.Now()
	m, _, err := c.UploadRandom(path, size)
	took := c.clock.Now().Sub(start).Round(time.Millisecond)
	if err != nil {
		c.logClient("write", fmt.Sprintf("write %s failed after %v: %v", path, took, err))
		return
	}
	c.logClient("write", fmt.Sprintf("write %s (%d KiB) v%d in %v", path, size>>10, m.Version, took))
}

// scriptAdmin sets node's admin state through the client, as the CLI does,
// and logs the outcome.
func (c *Cluster) scriptAdmin(node, state string) {
	res, err := c.Client().NodeAdmin(context.Background(), node, state)
	switch {
	case err != nil:
		c.logClient("admin", fmt.Sprintf("%s → %s refused: %v", node, state, err))
	case res.Warning != "":
		c.logClient("admin", fmt.Sprintf("%s → %s (%s)", node, state, res.Warning))
	default:
		c.logClient("admin", fmt.Sprintf("%s → %s", node, state))
	}
}
