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
)

// Client returns the cluster's test client, creating it on first use. Its
// retries advance simulated time.
func (c *Cluster) Client() *client.Direct {
	if c.client == nil {
		caller := c.NewCaller("client-1")
		c.client = client.New(caller, client.Options{Meta: MetaID, Sleep: caller.Sleep})
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

// UploadRandom writes size random bytes to path (overwriting). On success
// the harness remembers the content: AssertInvariants checks it stays
// readable.
func (c *Cluster) UploadRandom(path string, size int64) (client.Manifest, []byte, error) {
	data := c.RandomData(path, size)
	m, err := c.Client().Put(context.Background(), path, bytes.NewReader(data), size, client.PutOptions{Overwrite: true})
	if err == nil {
		c.acked[path] = sha256.Sum256(data)
	}
	return m, data, err
}

// Download reads path through the client (which verifies every chunk).
func (c *Cluster) Download(path string) ([]byte, client.Manifest, error) {
	var buf bytes.Buffer
	m, err := c.Client().Get(context.Background(), path, &buf)
	return buf.Bytes(), m, err
}

// Delete removes path and forgets it in the acknowledged set.
func (c *Cluster) Delete(path string) error {
	_, err := c.Client().Delete(context.Background(), path, 0)
	if err == nil {
		delete(c.acked, path)
	}
	return err
}

// AssertInvariants checks the Phase 1 invariants:
//
//  1. No acknowledged data lost: every upload that returned success (and was
//     not deleted since) reads back with the SHA-256 it was written with.
//  2. Every committed file downloads, and its content matches the recorded
//     file hash (the client checks this on every Get).
//  3. Every chunk of every committed file has at least MinReplicas reported
//     locations on live nodes.
//  4. Every committed chunk has a durable record with refcount >= 1.
func (c *Cluster) AssertInvariants() error {
	var errs []error
	// Sorted: each download advances the clock, so map order would make the
	// run nondeterministic.
	for _, path := range slices.Sorted(maps.Keys(c.acked)) {
		want := c.acked[path]
		data, _, err := c.Download(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("acknowledged %s unreadable: %w", path, err))
			continue
		}
		if got := sha256.Sum256(data); got != want {
			errs = append(errs, fmt.Errorf("acknowledged %s reads back %x, wrote %x", path, got[:8], want[:8]))
		}
	}
	st := c.meta.State()
	now := c.clock.Now()
	cl := c.meta.Cluster()
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
	cl := c.meta.Cluster()
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.meta.State().List("/") {
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
	cl := c.meta.Cluster()
	seen := map[iface.ChunkID]bool{}
	for _, e := range c.meta.State().List("/") {
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
	for _, e := range c.meta.State().List("/") {
		for _, ch := range e.Chunks {
			if seen[ch] || !slices.Contains(c.meta.Cluster().Locations(ch), id) {
				continue
			}
			seen[ch] = true
			ci, _ := c.meta.State().Chunk(ch)
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
