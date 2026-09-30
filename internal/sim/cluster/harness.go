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

// UploadRandom writes size random bytes to path (overwriting). On success
// the harness remembers the content: AssertInvariants checks it stays
// readable.
func (c *Cluster) UploadRandom(path string, size int64) (client.Manifest, []byte, error) {
	return c.Upload(path, c.RandomData(path, size))
}

// Upload writes data to path, overwriting, and records it as UploadRandom does.
func (c *Cluster) Upload(path string, data []byte) (client.Manifest, []byte, error) {
	m, err := c.Client().Put(context.Background(), path, bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true})
	sum := sha256.Sum256(data)
	c.written[path] = append(c.written[path], sum)
	switch a := c.acked[path]; {
	case err == nil:
		c.acked[path] = &acked{hashes: [][32]byte{sum}}
	case a != nil:
		a.hashes = append(a.hashes, sum)
	}
	return m, data, err
}

// Download reads path through the client (which verifies every chunk). A
// successful read is also checked here, independently of the client: its
// bytes must be something written to that path (AssertInvariants, 5).
func (c *Cluster) Download(path string) ([]byte, client.Manifest, error) {
	var buf bytes.Buffer
	m, err := c.Client().Get(context.Background(), path, &buf)
	if err == nil {
		c.checkRead(path, buf.Bytes())
	}
	return buf.Bytes(), m, err
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

// Delete removes path and forgets it in the acknowledged set.
func (c *Cluster) Delete(path string) error {
	_, err := c.Client().Delete(context.Background(), path, 0)
	switch a := c.acked[path]; {
	case err == nil:
		delete(c.acked, path)
	case a != nil:
		a.gone = true
	}
	return err
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
func (c *Cluster) AssertInvariants() error {
	errs := slices.Clone(c.badReads)
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
	cl := c.Meta().Cluster()
	for _, e := range st.List("/") {
		if _, _, err := c.Download(e.Path); err != nil {
			errs = append(errs, fmt.Errorf("committed %s v%d unreadable: %w", e.Path, e.V, err))
		}
		for i, id := range e.Chunks {
			live := 0
			for _, n := range cl.Locations(id) {
				if cl.Alive(n) {
					live++
				}
			}
			if live < c.cfg.Meta.MinReplicas {
				errs = append(errs, fmt.Errorf("%s chunk %d (%s) has %d live replicas, want >= %d", e.Path, i, hex.EncodeToString(id[:6]), live, c.cfg.Meta.MinReplicas))
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
// reported copies on alive nodes.
func (c *Cluster) UnderReplicated() int {
	n := 0
	cl := c.Meta().Cluster()
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, id := range e.Chunks {
			if seen[id] {
				continue
			}
			seen[id] = true
			live := 0
			for _, nd := range cl.Locations(id) {
				if cl.Alive(nd) {
					live++
				}
			}
			if live < c.cfg.Meta.Replicas {
				n++
			}
		}
	}
	return n
}

// OverReplicated counts chunks of live files with more than Replicas
// reported locations on alive nodes.
func (c *Cluster) OverReplicated() int {
	n := 0
	cl := c.Meta().Cluster()
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, id := range e.Chunks {
			if seen[id] {
				continue
			}
			seen[id] = true
			live := 0
			for _, nd := range cl.Locations(id) {
				if cl.Alive(nd) {
					live++
				}
			}
			if live > c.cfg.Meta.Replicas {
				n++
			}
		}
	}
	return n
}

// BytesOn sums the sizes of live files' chunks reported on node id.
func (c *Cluster) BytesOn(id iface.NodeID) int64 {
	var total int64
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.Meta().State().List("/") {
		for _, ch := range e.Chunks {
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

// AssertMetaAgree checks that every live metadata peer holds byte-identical
// state: the same log applied gives the same namespace, refcounts, claims,
// epoch and pending GC deletes. Call it after the group has been quiet for a
// few seconds, so followers have applied what the leader committed.
func (c *Cluster) AssertMetaAgree() error {
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
