// Package cluster assembles an in-process simulated cluster from the same
// core components the real binaries run, wired to sim implementations. The
// WASM build, the chaos tests and the round-trip tests all drive it.
package cluster

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/core/node"
	"github.com/insanityatpeak/chunkd/internal/core/scrub"
	"github.com/insanityatpeak/chunkd/internal/core/wire"
	"github.com/insanityatpeak/chunkd/internal/history"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/obs"
	"github.com/insanityatpeak/chunkd/internal/sim"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// MetaID is the metadata server's node ID.
const MetaID iface.NodeID = "meta-1"

// Config sizes the cluster and its network.
type Config struct {
	Nodes int
	Racks int
	// Metas is the size of the metadata group; 0 or 1 is a single server.
	Metas  int
	Meta   meta.Config
	Faults sim.Faults
	// CallTimeout bounds each client RPC in simulated time.
	CallTimeout time.Duration
	// Scrub sets every node's scrubber.
	Scrub scrub.Config
}

// DefaultConfig is 1 metadata server and 5 storage nodes over 3 racks, on a
// LAN with 1-40 ms latency, 1 Gbit/s links and 1% loss.
func DefaultConfig() Config {
	return Config{
		Nodes:       5,
		Racks:       3,
		Meta:        meta.DefaultConfig(MetaID),
		Faults:      sim.Faults{DropRate: 0.01, DupRate: 0.005, MinDelay: time.Millisecond, MaxDelay: 40 * time.Millisecond, BytesPerSec: 125 << 20},
		CallTimeout: 10 * time.Second,
		Scrub:       scrub.DefaultConfig(),
	}
}

// DashboardConfig is DefaultConfig with a group of 3 metadata peers, as the
// browser runs it: a leader can be killed or cut off and the group re-elects.
func DashboardConfig() Config {
	cfg := DefaultConfig()
	cfg.Metas = 3
	return cfg
}

// Cluster is a running simulation. Like the sim, it is single-threaded.
type Cluster struct {
	seed   uint64
	cfg    Config
	clock  *sim.Clock
	net    *sim.Net
	rng    iface.Rand
	log    io.Writer
	metas  []*metaPeer
	nodes  []*Node
	client *client.Direct
	rec    *history.Recorder
	pinned map[iface.NodeID]*Session
	acked  map[string]*acked
	// written holds every content ever sent to a path, acknowledged or
	// not; badReads are successful reads that returned anything else.
	written  map[string][][32]byte
	badReads []error
	// badTrims are trims that broke the trim-safety invariant (checkTrim).
	badTrims []error
	// killedAt and wipedAt are each node's latest kill and wipe.
	killedAt map[iface.NodeID]iface.Instant
	wipedAt  map[iface.NodeID]iface.Instant
	// admin sends AdminAsync commands; adminQ holds each node's, in order.
	admin    iface.AsyncCaller
	adminQ   map[iface.NodeID][]func()
	rotPick  uint64
	timeline timeline
	script   []scriptedStep // due scripted client calls, in time order
	readLog  []client.Event // results of scripted client calls, newest last
	readSeq  uint64
}

// Node is one simulated storage node.
type Node struct {
	*node.Node
	Rack  string
	Store *sim.BlockStore
}

// New builds and starts a cluster whose every choice derives from seed. Logs
// go to w; pass io.Discard to silence them.
func New(seed uint64, cfg Config, w io.Writer) *Cluster {
	c := &Cluster{seed: seed, cfg: cfg, clock: sim.NewClock(), rng: sim.NewRand(seed), log: w, acked: map[string]*acked{},
		written: map[string][][32]byte{}, killedAt: map[iface.NodeID]iface.Instant{}, wipedAt: map[iface.NodeID]iface.Instant{}}
	c.net = sim.NewNet(c.clock, c.rng, cfg.Faults)
	for i := range max(cfg.Metas, 1) { // all of them exist before any starts: each needs the full peer list
		c.metas = append(c.metas, &metaPeer{id: metaName(i), store: sim.NewMetaStore()})
	}
	for _, p := range c.metas {
		if err := c.startMeta(p); err != nil {
			panic(err) // an empty in-memory log cannot fail to recover
		}
	}
	for i := 1; i <= cfg.Nodes; i++ {
		id := iface.NodeID(fmt.Sprintf("node-%d", i))
		nd := &Node{Rack: fmt.Sprintf("r%d", (i-1)%max(cfg.Racks, 1)+1), Store: sim.NewBlockStore()}
		c.nodes = append(c.nodes, nd)
		c.startNode(id, nd)
	}
	return c
}

// startNode runs a fresh node process (new incarnation) over nd's store.
func (c *Cluster) startNode(id iface.NodeID, nd *Node) {
	cfg := node.DefaultConfig(id, MetaID, nd.Rack)
	cfg.Metas = c.MetaIDs()
	cfg.Scrub = c.cfg.Scrub
	n, err := node.New(node.Deps{Clock: c.clock, Net: nodeNet{c.net, c}, Async: c.net.AsyncCaller(id, c.cfg.CallTimeout), Store: nd.Store, Rand: c.rng, Log: c.logger(id)}, cfg)
	if err != nil {
		panic(err) // an in-memory disk cannot fail to read its term
	}
	nd.Node = n
	nd.Node.Start()
}

func (c *Cluster) node(id iface.NodeID) *Node {
	for _, n := range c.nodes {
		if n.ID() == id {
			return n
		}
	}
	panic(fmt.Sprintf("no node %s", id))
}

// KillNode stops a node's process and cuts it off. Its disk survives.
func (c *Cluster) KillNode(id iface.NodeID) {
	c.killedAt[id] = c.clock.Now()
	c.node(id).Stop()
	c.net.Crash(id)
}

// RestartNode starts a new process for a killed node over the same disk.
func (c *Cluster) RestartNode(id iface.NodeID) {
	nd := c.node(id)
	nd.Stop()
	c.net.Restart(id)
	c.startNode(id, nd)
}

// MaxNodes caps AddNode: the dashboard lays out at most this many cards.
const MaxNodes = 8

// AddNode starts node-(N+1) with an empty disk on rack (N mod Racks)+1, so
// the 6th node of the 5-node layout joins r3, the rack with one node. It
// draws from the RNG only here, so a run that never adds keeps its stream.
func (c *Cluster) AddNode() (iface.NodeID, error) {
	return c.AddNodeOn(fmt.Sprintf("r%d", len(c.nodes)%max(c.cfg.Racks, 1)+1))
}

// AddNodeOn starts node-(N+1) with an empty disk on rack.
func (c *Cluster) AddNodeOn(rack string) (iface.NodeID, error) {
	if len(c.nodes) >= MaxNodes {
		return "", iface.Errorf(iface.CodeInvalid, "the simulation runs at most %d storage nodes", MaxNodes)
	}
	id := iface.NodeID(fmt.Sprintf("node-%d", len(c.nodes)+1))
	nd := &Node{Rack: rack, Store: sim.NewBlockStore()}
	c.nodes = append(c.nodes, nd)
	c.cfg.Nodes = len(c.nodes)
	c.startNode(id, nd)
	return id, nil
}

// Drain asks the metadata group to drain a node (ADR-0021) and returns its
// warning, if any. The call advances simulated time.
func (c *Cluster) Drain(id iface.NodeID) (string, error) {
	res, err := c.Client().NodeAdmin(context.Background(), string(id), "draining")
	return res.Warning, err
}

// Undrain returns a draining or decommissioned node to service.
func (c *Cluster) Undrain(id iface.NodeID) error {
	_, err := c.Client().NodeAdmin(context.Background(), string(id), "active")
	return err
}

// AdminAsync sends a drain, undrain or decommission to the metadata leader
// without blocking, for code that runs on the simulation clock (chaos
// faults): a client call would advance the clock from inside a timer. While
// there is no leader, the command finds none, or the leader has not heard
// from the node (it started while the node was down), it retries every
// second for up to two minutes. done, if set, gets the outcome.
//
// Commands for one node run one at a time, in call order: a drain whose
// answer was lost and is retried must not land after a later undrain.
func (c *Cluster) AdminAsync(id iface.NodeID, to chunkdv1.NodeAdmin, done func(error)) {
	if c.admin == nil {
		c.admin = c.net.AsyncCaller("admin-1", c.cfg.CallTimeout)
		c.adminQ = map[iface.NodeID][]func(){}
	}
	c.adminQ[id] = append(c.adminQ[id], func() { c.adminSend(id, to, done) })
	if len(c.adminQ[id]) == 1 {
		c.adminQ[id][0]()
	}
}

// adminSend runs the head of id's admin queue and, once it has an outcome,
// the next command.
func (c *Cluster) adminSend(id iface.NodeID, to chunkdv1.NodeAdmin, done func(error)) {
	finish := func(err error) {
		if done != nil {
			done(err)
		}
		c.adminQ[id] = c.adminQ[id][1:]
		if len(c.adminQ[id]) > 0 {
			c.adminQ[id][0]()
		}
	}
	start := c.clock.Now()
	body := wire.Marshal(&chunkdv1.NodeAdminRequest{Node: string(id), State: to})
	var try func()
	retry := func(err error) {
		if c.clock.Now().Sub(start) >= 2*time.Minute {
			finish(err)
			return
		}
		c.clock.AfterFunc(time.Second, try)
	}
	try = func() {
		leader := c.MetaLeader()
		if leader == "" {
			retry(iface.Errorf(iface.CodeUnavailable, "no metadata leader"))
			return
		}
		c.admin.Go(iface.Call{To: leader, Kind: wire.KindNodeAdmin, Body: body}, func(r iface.Result) {
			if code := iface.CodeOf(r.Err); code == iface.CodeNotLeader || code == iface.CodeUnavailable || code == iface.CodeNotFound {
				retry(r.Err)
				return
			}
			finish(r.Err)
		})
	}
	try()
}

// demoFiles uploads 8 files of 5 MiB if the cluster holds none.
func (c *Cluster) demoFiles() error {
	if len(c.Meta().State().List("/")) > 0 {
		return nil
	}
	for i := range 8 {
		if _, _, err := c.UploadRandom(fmt.Sprintf("/demo/file-%d.bin", i), 5<<20); err != nil {
			return err
		}
	}
	return nil
}

// ScriptRot is the dashboard's scripted bit rot: load the demo files if the
// cluster holds none, then flip bytes in n chunks on id's disk, only where
// two intact copies remain elsewhere. Nobody is told: the scrubber or a
// reader must find them. Returns the rotted chunks.
func (c *Cluster) ScriptRot(id iface.NodeID, n int) ([]iface.ChunkID, error) {
	if err := c.demoFiles(); err != nil {
		return nil, err
	}
	c.rotPick += 7919 // a different window of chunks each time, same for a seed
	return c.RotNode(id, n, c.rotPick, func(ch iface.ChunkID) bool {
		intact := 0
		for _, o := range c.nodes {
			if o.ID() != id && c.Intact(o.ID(), ch) {
				intact++
			}
		}
		return intact >= 2
	}), nil
}

// RotNode flips a byte in up to n chunks on a node's disk, as bit rot
// would. Chunks are visited in ID order starting at index pick % count, and
// only those ok accepts are rotted. It returns the rotted chunks. The node
// notices only when it next reads one (a client, a repair copy, the
// scrubber).
func (c *Cluster) RotNode(id iface.NodeID, n int, pick uint64, ok func(iface.ChunkID) bool) []iface.ChunkID {
	store := c.node(id).Store
	var ids []iface.ChunkID
	_ = store.List(context.Background(), func(ch iface.ChunkID) error { ids = append(ids, ch); return nil })
	var rotted []iface.ChunkID
	for i := range ids {
		if len(rotted) == n {
			break
		}
		ch := ids[(pick+uint64(i))%uint64(len(ids))]
		if ok(ch) && store.Corrupt(ch) {
			rotted = append(rotted, ch)
		}
	}
	return rotted
}

// Intact reports whether node's disk holds chunk with bytes that match its
// hash, whether or not the node process is running.
func (c *Cluster) Intact(node iface.NodeID, chunk iface.ChunkID) bool {
	b, err := c.node(node).Store.Get(context.Background(), chunk)
	return err == nil && sha256.Sum256(b) == chunk
}

// WipeNode replaces a killed node's disk with an empty one, as after a disk
// failure; RestartNode brings it back empty.
func (c *Cluster) WipeNode(id iface.NodeID) {
	c.wipedAt[id] = c.clock.Now()
	c.node(id).Store = sim.NewBlockStore()
}

func (c *Cluster) logger(id iface.NodeID) *slog.Logger {
	return obs.NewLogger(c.log, string(id), slog.LevelInfo)
}

// metaPeer is one metadata server process and the disk it recovers from.
type metaPeer struct {
	id    iface.NodeID
	store *sim.MetaStore
	srv   *meta.Server
}

// metaName names peer i (from 0): meta-1, meta-2, …
func metaName(i int) iface.NodeID { return iface.NodeID(fmt.Sprintf("meta-%d", i+1)) }

// MetaIDs are the metadata peers' node IDs.
func (c *Cluster) MetaIDs() []iface.NodeID {
	ids := make([]iface.NodeID, len(c.metas))
	for i, p := range c.metas {
		ids[i] = p.id
	}
	return ids
}

func (c *Cluster) metaPeer(id iface.NodeID) *metaPeer {
	for _, p := range c.metas {
		if p.id == id {
			return p
		}
	}
	panic(fmt.Sprintf("no metadata peer %s", id))
}

// startMeta runs a fresh process for p over its own log.
func (c *Cluster) startMeta(p *metaPeer) error {
	// The old process is gone: its timers and consensus node must not go on
	// writing to the store the new one recovers from.
	if p.srv != nil {
		p.srv.Stop()
	}
	cfg := c.cfg.Meta
	cfg.ID = p.id
	if len(c.metas) > 1 {
		cfg.Peers = c.MetaIDs()
	}
	srv, err := meta.NewServer(context.Background(), meta.Deps{Clock: c.clock, Net: c.net, Store: p.store, Rand: c.rng, Log: c.logger(p.id)}, cfg)
	if err != nil {
		return err
	}
	srv.Start()
	p.srv = srv
	return nil
}

// RestartMeta replaces the first metadata server with a fresh one recovered
// from its log, as after a process crash. Locations come back through block
// reports.
func (c *Cluster) RestartMeta() error { return c.startMeta(c.metas[0]) }

// KillMeta stops a metadata peer's process and cuts it off. Its log survives.
func (c *Cluster) KillMeta(id iface.NodeID) {
	c.pullEvents() // its events since the last Tick die with it
	p := c.metaPeer(id)
	p.srv.Stop()
	c.net.Crash(id)
}

// ReviveMeta starts a new process for a killed peer over the same log.
func (c *Cluster) ReviveMeta(id iface.NodeID) error {
	c.net.Restart(id)
	return c.startMeta(c.metaPeer(id))
}

// CutMeta partitions metadata peer id from the other peers, both ways.
// Nodes and clients still reach it, so a cut leader goes on answering until
// its quorum check fails, and can no longer commit.
func (c *Cluster) CutMeta(id iface.NodeID) { c.net.Partition([]iface.NodeID{id}, c.otherMetas(id)) }

// HealMeta undoes CutMeta.
func (c *Cluster) HealMeta(id iface.NodeID) {
	for _, p := range c.otherMetas(id) {
		c.net.Unblock(id, p)
		c.net.Unblock(p, id)
	}
}

// MetaCut reports whether peer id is cut off from every other peer.
func (c *Cluster) MetaCut(id iface.NodeID) bool {
	rest := c.otherMetas(id)
	for _, p := range rest {
		if !c.net.Blocked(id, p) {
			return false
		}
	}
	return len(rest) > 0
}

func (c *Cluster) otherMetas(id iface.NodeID) []iface.NodeID {
	var rest []iface.NodeID
	for _, p := range c.MetaIDs() {
		if p != id {
			rest = append(rest, p)
		}
	}
	return rest
}

// MetaPeer returns peer id's server, for tests that inspect one peer.
func (c *Cluster) MetaPeer(id iface.NodeID) *meta.Server { return c.metaPeer(id).srv }

// AfterFunc runs f at Now()+d on the simulation's clock, including while
// a client call is advancing it. Chaos schedules faults this way so they
// fire on time even in the middle of a blocking operation.
func (c *Cluster) AfterFunc(d time.Duration, f func()) { c.clock.AfterFunc(d, f) }

// Tick advances simulated time by d, running every event due in that window.
// Scripted reads due in the window run between clock advances, never from
// a timer: a client call advances the clock itself.
func (c *Cluster) Tick(d time.Duration) {
	end := c.clock.Now().Add(d)
	for len(c.script) > 0 && c.script[0].at <= end {
		r := c.script[0]
		c.script = c.script[1:]
		if now := c.clock.Now(); r.at > now {
			c.clock.Advance(r.at.Sub(now))
		}
		c.pullEvents()
		r.run()
	}
	if now := c.clock.Now(); end > now {
		c.clock.Advance(end.Sub(now))
	}
	c.pullEvents()
}

// Now returns simulated time.
func (c *Cluster) Now() iface.Instant { return c.clock.Now() }

// Net exposes the network for fault injection.
func (c *Cluster) Net() *sim.Net { return c.net }

// Meta exposes the metadata leader, or while an election runs the live peer
// that has applied the most. With a single server it is that server. Only
// touch it between Ticks.
func (c *Cluster) Meta() *meta.Server {
	best := c.metas[0].srv
	for _, p := range c.metas {
		if c.net.Crashed(p.id) {
			continue
		}
		if p.srv.Raft().Ready {
			return p.srv
		}
		if c.net.Crashed(c.metaID(best)) || p.srv.Applied() > best.Applied() {
			best = p.srv
		}
	}
	return best
}

func (c *Cluster) metaID(srv *meta.Server) iface.NodeID {
	for _, p := range c.metas {
		if p.srv == srv {
			return p.id
		}
	}
	return ""
}

// MetaLeader returns the ready leader's ID, or "" during an election.
func (c *Cluster) MetaLeader() iface.NodeID {
	for _, p := range c.metas {
		if !c.net.Crashed(p.id) && p.srv.Raft().Ready {
			return p.id
		}
	}
	return ""
}

// Nodes returns the storage nodes in ID order.
func (c *Cluster) Nodes() []*Node { return c.nodes }

// Seed returns the seed the cluster was built from.
func (c *Cluster) Seed() uint64 { return c.seed }

// Config returns the configuration.
func (c *Cluster) Config() Config { return c.cfg }

// NewCaller returns a client endpoint on the sim network.
func (c *Cluster) NewCaller(id iface.NodeID) *sim.Caller {
	return c.net.NewCaller(id, c.cfg.CallTimeout)
}

// ScriptKillNode is the dashboard's scripted failure: load 8 files of 5 MiB
// if the cluster holds none, kill id now and restart it after down, all on
// the simulation clock so a seed replays it exactly. With down above dead
// (10 s) plus the repair delay (20 s), the timeline shows suspect, dead,
// re-replication to RF 3, the node's return and the trims.
func (c *Cluster) ScriptKillNode(id iface.NodeID, down time.Duration) error {
	if err := c.demoFiles(); err != nil {
		return err
	}
	c.KillNode(id)
	c.clock.AfterFunc(down, func() {
		if c.net.Crashed(id) {
			c.RestartNode(id)
		}
	})
	return nil
}
