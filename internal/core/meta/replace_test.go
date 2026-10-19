package meta_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim"
)

func TestRaftSlot(t *testing.T) {
	for _, tc := range []struct {
		id   uint64
		n    int
		slot int
	}{{1, 3, 1}, {3, 3, 3}, {4, 3, 1}, {5, 3, 2}, {8, 3, 2}, {1, 1, 1}, {7, 1, 1}} {
		if got := meta.RaftSlot(tc.id, tc.n); got != tc.slot {
			t.Errorf("RaftSlot(%d, %d) = %d, want %d", tc.id, tc.n, got, tc.slot)
		}
	}
}

// A peer may start under any generation of its own slot and no other's.
func TestRaftIDMustMatchSlot(t *testing.T) {
	peers := []iface.NodeID{"meta-1", "meta-2", "meta-3"}
	start := func(id iface.NodeID, raftID uint64) error {
		clock := sim.NewClock()
		rng := sim.NewRand(1)
		cfg := meta.DefaultConfig(id)
		cfg.Peers, cfg.RaftID, cfg.Join = peers, raftID, raftID > 3
		_, err := meta.NewServer(context.Background(), meta.Deps{Clock: clock, Net: sim.NewNet(clock, rng, sim.Faults{MaxDelay: time.Millisecond}),
			Store: sim.NewMetaStore(), Rand: rng, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, cfg)
		return err
	}
	for _, ok := range []struct {
		id  iface.NodeID
		raw uint64
	}{{"meta-2", 0}, {"meta-2", 2}, {"meta-2", 5}, {"meta-3", 9}} {
		if err := start(ok.id, ok.raw); err != nil {
			t.Errorf("%s as raft ID %d: %v", ok.id, ok.raw, err)
		}
	}
	if err := start("meta-2", 4); err == nil || !strings.Contains(err.Error(), "belongs to slot 1") {
		t.Errorf("meta-2 as raft ID 4: err = %v, want a slot mismatch", err)
	}
}
