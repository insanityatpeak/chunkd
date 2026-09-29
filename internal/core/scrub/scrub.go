// Package scrub finds silent corruption on a storage node before a reader
// does: it re-reads and re-hashes every stored chunk, one at a time, at a
// capped rate, and starts a new pass every Pass interval.
package scrub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Config sets the scrub rate and cadence.
type Config struct {
	// BytesPerSec caps the average read rate. Steps are paced, not
	// bursted: after a chunk of n bytes the next step waits n/BytesPerSec.
	BytesPerSec int64
	// Pass is the interval between pass starts. A pass that takes longer
	// than Pass is followed at once by the next one.
	Pass time.Duration
}

// DefaultConfig: 8 MiB/s (about 5% of a disk's streaming bandwidth; a 4 TB
// node takes about 6 days per pass), a pass started every 10 minutes.
// HDFS scans at 1 MiB/s per volume with a 3-week period; Ceph deep-scrubs
// each placement group weekly. The short Pass suits a small demo cluster.
func DefaultConfig() Config {
	return Config{BytesPerSec: 8 << 20, Pass: 10 * time.Minute}
}

// Stats are cumulative counters and the current pass's progress.
type Stats struct {
	Bytes   uint64 `json:"bytes"`   // bytes verified, all passes
	Chunks  uint64 `json:"chunks"`  // chunks verified, all passes
	Corrupt uint64 `json:"corrupt"` // chunks that failed verification
	Passes  uint64 `json:"passes"`  // completed passes
	// LastPass is how long the last completed pass took.
	LastPass time.Duration `json:"lastPassNs"`
	// Done and Total are the current pass's progress, in chunks.
	Done  int `json:"done"`
	Total int `json:"total"`
}

// Scrubber walks a store. It runs on its owner's event loop through Clock.
type Scrubber struct {
	cfg     Config
	clock   iface.Clock
	store   iface.BlockStore
	corrupt func(iface.ChunkID)

	todo      []iface.ChunkID
	passStart iface.Instant
	stats     Stats
	stopped   bool
}

// New returns a stopped scrubber. corrupt is called for every chunk whose
// bytes do not hash to its ID; the owner quarantines and reports it.
func New(cfg Config, clock iface.Clock, store iface.BlockStore, corrupt func(iface.ChunkID)) *Scrubber {
	if cfg.BytesPerSec <= 0 || cfg.Pass <= 0 {
		panic("scrub: rate and pass interval must be positive")
	}
	return &Scrubber{cfg: cfg, clock: clock, store: store, corrupt: corrupt}
}

// Start begins the first pass after delay (jitter, so nodes started
// together do not scrub in lockstep).
func (s *Scrubber) Start(delay time.Duration) { s.clock.AfterFunc(delay, s.startPass) }

// Stop halts the scrubber at its next step.
func (s *Scrubber) Stop() { s.stopped = true }

// Stats returns counters and progress. Loop-owned.
func (s *Scrubber) Stats() Stats { return s.stats }

// startPass snapshots the chunk list. Chunks stored during the pass wait
// for the next one; chunks deleted during it are skipped when reached.
func (s *Scrubber) startPass() {
	if s.stopped {
		return
	}
	s.todo = s.todo[:0]
	err := s.store.List(context.Background(), func(id iface.ChunkID) error {
		s.todo = append(s.todo, id)
		return nil
	})
	if err != nil {
		// Try again at the next pass.
		s.clock.AfterFunc(s.cfg.Pass, s.startPass)
		return
	}
	// List order is unspecified; sort so a sim run replays.
	slices.SortFunc(s.todo, func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
	s.passStart = s.clock.Now()
	s.stats.Done, s.stats.Total = 0, len(s.todo)
	s.step()
}

// step verifies one chunk and schedules the next, paced to the rate.
// SIMPLIFIED: reads and hashes on the owner's event loop, one chunk (at
// most 4 MiB, a few ms) per step. HDFS runs one scanner thread per volume.
func (s *Scrubber) step() {
	if s.stopped {
		return
	}
	if s.stats.Done == len(s.todo) {
		s.stats.Passes++
		s.stats.LastPass = s.clock.Now().Sub(s.passStart)
		next := max(s.passStart.Add(s.cfg.Pass).Sub(s.clock.Now()), 0)
		s.clock.AfterFunc(next, s.startPass)
		return
	}
	id := s.todo[s.stats.Done]
	s.stats.Done++
	data, err := s.store.Get(context.Background(), id)
	var n int64
	switch {
	case errors.Is(err, iface.ErrNotFound): // deleted or quarantined since the snapshot
	case err != nil:
		// An unreadable chunk is as bad as a corrupt one to a reader.
		s.stats.Corrupt++
		s.corrupt(id)
	default:
		n = int64(len(data))
		s.stats.Bytes += uint64(n)
		s.stats.Chunks++
		if sha256.Sum256(data) != id {
			s.stats.Corrupt++
			s.corrupt(id)
		}
	}
	s.clock.AfterFunc(time.Duration(n)*time.Second/time.Duration(s.cfg.BytesPerSec), s.step)
}
