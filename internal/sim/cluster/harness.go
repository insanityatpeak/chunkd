package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/ec"
	"github.com/insanityatpeak/chunkd/internal/history"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

// Client returns the cluster's test client, creating it on first use. Its
// retries advance simulated time.
func (c *Cluster) Client() *client.Direct {
	if c.client == nil {
		caller := c.NewCaller("client-1")
		opts := client.Options{Meta: MetaID, Sleep: caller.Sleep}
		if len(c.metas) > 1 {
			for _, id := range c.MetaIDs() {
				opts.Peers = append(opts.Peers, client.MetaPeer{ID: id})
			}
			// Request IDs come from a stream of their own: a single-server
			// run, which sets none, must draw exactly what it always drew.
			opts.Rand = sim.NewRand(c.seed ^ 0x5eed_c11e)
		}
		c.client = client.New(caller, opts)
	}
	return c.client
}

// RandomData returns size bytes determined by the cluster seed and path, so
// every file in a run differs and the run still replays.
func (c *Cluster) RandomData(path string, size int64) []byte {
	h := fnv.New64a()
	h.Write([]byte(path))
	r := rand.New(rand.NewPCG(c.seed, h.Sum64()))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// acked is what a path may legitimately hold, given every acknowledged and
// ambiguous operation on it. A failed put or delete may still have been
// applied (its response was lost), so it widens the set instead of being
// ignored; a success narrows it.
type acked struct {
	hashes [][32]byte
	// gone: the path may legitimately not exist (an ambiguous delete).
	gone bool
}

// Session is one client of the cluster. When history is recording, its calls
// are recorded under its client number. Like the cluster it is single-threaded.
type Session struct {
	c  *Cluster
	cl *client.Direct
	id int
}

// Session returns the default client's session (client 1).
func (c *Cluster) Session() *Session { return &Session{c: c, cl: c.Client(), id: 1} }

// RecordHistory turns on recording of every session's calls and returns the
// recorder. It draws nothing from the RNG, so a recorded run replays a
// recorded-off one exactly.
func (c *Cluster) RecordHistory() *history.Recorder {
	if c.rec == nil {
		c.rec = &history.Recorder{}
	}
	return c.rec
}

// Pinned returns a client that talks to metadata peer id only: it follows no
// hint to another peer, so a deposed leader that still answered a read would
// be seen. Its calls are recorded as client 100 plus the peer's index.
func (c *Cluster) Pinned(id iface.NodeID) *Session {
	p := c.metaPeer(id)
	if s := c.pinned[id]; s != nil {
		return s
	}
	caller := c.NewCaller("client-" + id)
	h := fnv.New64a()
	h.Write([]byte(id))
	cl := client.New(caller, client.Options{Peers: []client.MetaPeer{{ID: id}}, Sleep: caller.Sleep, Rand: sim.NewRand(c.seed ^ h.Sum64())})
	s := &Session{c: c, cl: cl, id: 100 + slices.Index(c.MetaIDs(), p.id)}
	if c.pinned == nil {
		c.pinned = map[iface.NodeID]*Session{}
	}
	c.pinned[id] = s
	return s
}

// UploadRandom writes size random bytes to path (overwriting). On success
// the harness remembers the content: AssertInvariants checks it stays
// readable.
func (c *Cluster) UploadRandom(path string, size int64) (client.Manifest, []byte, error) {
	return c.Session().UploadRandom(path, size)
}

// Upload writes data to path, overwriting, and records it as UploadRandom does.
func (c *Cluster) Upload(path string, data []byte) (client.Manifest, []byte, error) {
	return c.Session().Upload(path, data)
}

// Download reads path through the client (which verifies every chunk). A
// successful read is also checked here, independently of the client: its
// bytes must be something written to that path (AssertInvariants, 5).
func (c *Cluster) Download(path string) ([]byte, client.Manifest, error) {
	return c.Session().Download(path)
}

// Delete removes path and forgets it in the acknowledged set.
func (c *Cluster) Delete(path string) error { return c.Session().Delete(path) }

// List returns the live files under prefix, recorded when history is on.
func (c *Cluster) List(prefix string) ([]client.FileInfo, error) { return c.Session().List(prefix) }

// UploadRandom is Cluster.UploadRandom through this client.
func (s *Session) UploadRandom(path string, size int64) (client.Manifest, []byte, error) {
	return s.Upload(path, s.c.RandomData(path, size))
}

// Upload is Cluster.Upload through this client.
func (s *Session) Upload(path string, data []byte) (client.Manifest, []byte, error) {
	return s.UploadAs(path, data, client.Replicated)
}

// UploadAs is Upload with a redundancy policy.
func (s *Session) UploadAs(path string, data []byte, r client.Redundancy) (client.Manifest, []byte, error) {
	c := s.c
	call := int64(c.clock.Now())
	m, err := s.cl.Put(context.Background(), path, bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true, Redundancy: r})
	sum := sha256.Sum256(data)
	if c.rec != nil {
		c.rec.Put(s.id, path, sum, call, int64(c.clock.Now()), m.Version, err)
	}
	c.written[path] = append(c.written[path], sum)
	switch a := c.acked[path]; {
	case err == nil:
		c.acked[path] = &acked{hashes: [][32]byte{sum}}
	case a != nil:
		a.hashes = append(a.hashes, sum)
	}
	return m, data, err
}

// Download is Cluster.Download through this client.
func (s *Session) Download(path string) ([]byte, client.Manifest, error) {
	c := s.c
	var buf bytes.Buffer
	call := int64(c.clock.Now())
	m, err := s.cl.Get(context.Background(), path, &buf)
	if c.rec != nil {
		c.rec.Read(s.id, path, call, int64(c.clock.Now()), m.Version, sha256.Sum256(buf.Bytes()), err)
	}
	if err == nil {
		c.checkRead(path, buf.Bytes())
	}
	return buf.Bytes(), m, err
}

// Delete is Cluster.Delete through this client.
func (s *Session) Delete(path string) error {
	c := s.c
	call := int64(c.clock.Now())
	v, err := s.cl.Delete(context.Background(), path, 0)
	if c.rec != nil {
		c.rec.Delete(s.id, path, call, int64(c.clock.Now()), v, err)
	}
	switch a := c.acked[path]; {
	case err == nil:
		delete(c.acked, path)
	case a != nil:
		a.gone = true
	}
	return err
}

// List is Cluster.List through this client.
func (s *Session) List(prefix string) ([]client.FileInfo, error) {
	c := s.c
	call := int64(c.clock.Now())
	fs, err := s.cl.List(context.Background(), prefix)
	if c.rec != nil {
		var es []history.Entry
		for _, f := range fs {
			e := history.Entry{Path: f.Path, Version: f.Version}
			if b, herr := hex.DecodeString(f.SHA256); herr == nil {
				copy(e.Hash[:], b)
			}
			es = append(es, e)
		}
		c.rec.List(s.id, prefix, call, int64(c.clock.Now()), es, err)
	}
	return fs, err
}

// checkRead records a read that returned bytes nobody wrote to path. Only
// paths written through the harness are checked; a test that uploads
// through its own client checks its own reads.
func (c *Cluster) checkRead(path string, data []byte) {
	written, tracked := c.written[path]
	if sum := sha256.Sum256(data); tracked && !slices.Contains(written, sum) {
		c.badReads = append(c.badReads, fmt.Errorf("a read of %s at t=%v returned %d bytes (sha256 %x) that were never written there",
			path, c.clock.Now(), len(data), sum[:8]))
	}
}

// AssertInvariants checks:
//
//  1. No acknowledged data lost: every path with an acknowledged upload
//     reads back with one of the hashes it may legitimately hold, or is
//     absent only if an ambiguous delete may have removed it.
//  2. Every committed file downloads, and its content matches the recorded
//     file hash (the client checks this on every Get).
//  3. Every chunk of every committed file has at least MinReplicas reported
//     locations on alive nodes.
//  4. Every committed chunk has a durable record with refcount >= 1.
//  5. No successful read, during the run or here, ever returned bytes that
//     were not written to that path.
//  6. No trim, when it ran, left a chunk below RF intact copies on running
//     nodes, unless a holder had just failed (checkTrim).
func (c *Cluster) AssertInvariants() error {
	errs := append(slices.Clone(c.badReads), c.badTrims...)
	// Sorted: each download advances the clock, so map order would make the
	// run nondeterministic.
	for _, path := range slices.Sorted(maps.Keys(c.acked)) {
		a := c.acked[path]
		data, _, err := c.Download(path)
		if err != nil {
			if a.gone && iface.CodeOf(err) == iface.CodeNotFound {
				continue
			}
			errs = append(errs, fmt.Errorf("acknowledged %s unreadable: %w", path, err))
			continue
		}
		if got := sha256.Sum256(data); !slices.Contains(a.hashes, got) {
			errs = append(errs, fmt.Errorf("acknowledged %s reads back %x, not any of %d acknowledged or ambiguous versions", path, got[:8], len(a.hashes)))
		}
	}
	st := c.Meta().State()
	now := c.clock.Now()
	for _, e := range st.List("/") {
		if _, _, err := c.Download(e.Path); err != nil {
			errs = append(errs, fmt.Errorf("committed %s v%d unreadable: %w", e.Path, e.V, err))
		}
		for i, id := range e.Chunks {
			live, need := c.aliveCopies(id), c.cfg.Meta.MinReplicas
			if shards, ok := st.Stripe(id); ok {
				live, need = c.aliveShards(shards), ec.CommitShards
			}
			if live < need {
				errs = append(errs, fmt.Errorf("%s chunk %d (%s) has %d live replicas, want >= %d", e.Path, i, hex.EncodeToString(id[:6]), live, need))
			}
			if ci, ok := st.Chunk(id); !ok || ci.Refcount == 0 {
				errs = append(errs, fmt.Errorf("%s chunk %d has no durable record", e.Path, i))
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("seed %d at t=%v: %w", c.seed, iface.Instant(now), err)
	}
	return nil
}

// UnderReplicated counts chunks of live files with fewer than Replicas
// reported copies on alive nodes, or for a stripe, fewer than 6 shards with one.
func (c *Cluster) UnderReplicated() int {
	n := 0
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, id := range e.Chunks {
			if seen[id] {
				continue
			}
			seen[id] = true
			if shards, ok := c.Meta().State().Stripe(id); ok {
				if c.aliveShards(shards) < ec.TotalShards {
					n++
				}
				continue
			}
			if c.aliveCopies(id) < c.cfg.Meta.Replicas {
				n++
			}
		}
	}
	return n
}

// OverReplicated counts chunks of live files with more than Replicas kept
// copies (keptCopies), or for a stripe, a shard with more than one.
func (c *Cluster) OverReplicated() int {
	n := 0
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, id := range e.Chunks {
			if seen[id] {
				continue
			}
			seen[id] = true
			if shards, ok := c.Meta().State().Stripe(id); ok {
				if slices.ContainsFunc(shards, func(sh iface.ChunkID) bool { return c.keptCopies(sh) > 1 }) {
					n++
				}
				continue
			}
			if c.keptCopies(id) > c.cfg.Meta.Replicas {
				n++
			}
		}
	}
	return n
}

// BytesOn sums the sizes of live files' chunks (and shards) reported on node id.
func (c *Cluster) BytesOn(id iface.NodeID) int64 {
	var total int64
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, ch := range e.Chunks {
			if shards, ok := c.Meta().State().Stripe(ch); ok && !seen[ch] {
				seen[ch] = true
				ci, _ := c.Meta().State().Chunk(ch)
				for _, sh := range shards {
					if slices.Contains(c.Meta().Cluster().Locations(sh), id) {
						total += ec.BlockSize(ci.Size)
					}
				}
			}
			if seen[ch] || !slices.Contains(c.Meta().Cluster().Locations(ch), id) {
				continue
			}
			seen[ch] = true
			ci, _ := c.Meta().State().Chunk(ch)
			total += ci.Size
		}
	}
	return total
}

// RepairBound is the documented time to restore RF after a node holding
// bytes dies (ADR-0011):
//
//	dead timeout + repair delay + bytes / throttle rate
//	  + one copy timeout (a lost command or completion report)
//	  + 5 s (heartbeat phase, detector tick, copy latency)
func (c *Cluster) RepairBound(bytes int64) time.Duration {
	r := c.cfg.Meta.Repair
	return c.cfg.Meta.Detector.DeadAfter + r.Delay + time.Duration(bytes*int64(time.Second)/r.BytesPerSec) + r.CopyTimeout + 5*time.Second
}

// Settle ticks until no chunk is under-replicated or limit passes, and
// reports how long it took.
func (c *Cluster) Settle(limit time.Duration) (time.Duration, bool) {
	start := c.clock.Now()
	for c.clock.Now().Sub(start) <= limit {
		if c.UnderReplicated() == 0 {
			return c.clock.Now().Sub(start), true
		}
		c.Tick(250 * time.Millisecond)
	}
	return c.clock.Now().Sub(start), false
}

// metaApplySame reports whether every live metadata peer has applied the same index.
func (c *Cluster) metaApplySame() bool {
	var at uint64
	first := true
	for _, p := range c.metas {
		if c.net.Crashed(p.id) {
			continue
		}
		if a := p.srv.Raft().Applied; first {
			at, first = a, false
		} else if a != at {
			return false
		}
	}
	return true
}

// AssertMetaAgree checks that every live metadata peer holds byte-identical
// state: the same log applied gives the same namespace, refcounts, claims,
// epoch and pending GC deletes. Call it after the group has been quiet for a
// few seconds. Periodic proposals (the epoch tick) mean the group is never
// fully quiet, so it first advances the clock, up to 2 s, until every live
// peer has applied the same index: state is compared at one index, not at
// one instant, when a follower may not yet have heard the latest commit.
func (c *Cluster) AssertMetaAgree() error {
	for waited := time.Duration(0); waited < 2*time.Second && !c.metaApplySame(); waited += 10 * time.Millisecond {
		c.Tick(10 * time.Millisecond)
	}
	var base []byte
	var baseID iface.NodeID
	var errs []error
	for _, p := range c.metas {
		if c.net.Crashed(p.id) {
			continue
		}
		snap := p.srv.State().Snapshot()
		switch {
		case base == nil:
			base, baseID = snap, p.id
		case !bytes.Equal(base, snap):
			errs = append(errs, fmt.Errorf("%s (applied %d) and %s (applied %d) hold different state", baseID, c.metaPeer(baseID).srv.Applied(), p.id, p.srv.Applied()))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("seed %d at t=%v: %w", c.seed, c.clock.Now(), err)
	}
	return nil
}

// aliveCopies counts id's reported copies on alive nodes.
func (c *Cluster) aliveCopies(id iface.ChunkID) int {
	cl := c.Meta().Cluster()
	n := 0
	for _, nd := range cl.Locations(id) {
		if cl.Alive(nd) {
			n++
		}
	}
	return n
}

// aliveShards counts the shards with a copy on an alive node.
func (c *Cluster) aliveShards(shards []iface.ChunkID) int {
	n := 0
	for _, sh := range shards {
		if c.aliveCopies(sh) > 0 {
			n++
		}
	}
	return n
}

// copyTarget is the copies a referenced block must keep: RF for a chunk, 1
// for a shard of a referenced stripe.
func (c *Cluster) copyTarget(id iface.ChunkID) (int, bool) {
	st := c.Meta().State()
	b, ok := st.Block(id)
	if !ok {
		return 0, false
	}
	if !b.Shard {
		ci, _ := st.Chunk(id)
		return c.cfg.Meta.Replicas, ci.Refcount > 0
	}
	ci, ok := st.Chunk(b.Ref.Stripe)
	return 1, ok && ci.Refcount > 0
}

// keptCopies counts id's copies on alive nodes that GC has not authorized
// deleting. A copy under a pending GC delete is gone to repair (bugs-found
// #28): counting it as surplus would wait on GC, not on a trim.
func (c *Cluster) keptCopies(id iface.ChunkID) int {
	cl, st := c.Meta().Cluster(), c.Meta().State()
	n := 0
	for _, nd := range cl.Locations(id) {
		if _, gc := st.GCPending(id, nd); cl.Alive(nd) && !gc {
			n++
		}
	}
	return n
}
