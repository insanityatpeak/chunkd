package heartbeat

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/sim"
)

func setup(f sim.Faults) (*sim.Clock, *sim.Net, Deps) {
	c := sim.NewClock()
	r := sim.NewRand(1)
	n := sim.NewNet(c, r, f)
	return c, n, Deps{Clock: c, Net: n, Rand: r, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestSenderAndTracker(t *testing.T) {
	tests := []struct {
		name      string
		faults    sim.Faults
		run       time.Duration
		wantSent  uint64
		wantPings uint64
		wantAcked uint64
	}{
		// First ping lands within [0, 1s), then one per second.
		{"reliable", sim.Faults{}, 10 * time.Second, 10, 10, 10},
		{"all dropped", sim.Faults{DropRate: 1}, 10 * time.Second, 10, 0, 0},
		{"duplicated", sim.Faults{DupRate: 1}, 10 * time.Second, 10, 20, 40},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _, d := setup(tt.faults)
			tr := NewTracker(d, "meta")
			tr.Start()
			s := NewSender(d, "n1", "meta", time.Second)
			s.Start()
			c.Advance(tt.run)

			if got := s.Stats(); got.Sent != tt.wantSent || got.Acked != tt.wantAcked {
				t.Errorf("sender = %+v, want sent %d acked %d", got, tt.wantSent, tt.wantAcked)
			}
			var pings uint64
			if ps := tr.Peers(); len(ps) == 1 {
				pings = ps[0].Pings
			}
			if pings != tt.wantPings {
				t.Errorf("tracker pings = %d, want %d", pings, tt.wantPings)
			}
		})
	}
}

func TestAliveTimeout(t *testing.T) {
	c, n, d := setup(sim.Faults{})
	tr := NewTracker(d, "meta")
	tr.Start()
	NewSender(d, "n1", "meta", time.Second).Start()

	c.Advance(2 * time.Second)
	if !tr.Alive("n1", 3*time.Second) {
		t.Fatal("n1 not alive while pinging")
	}
	n.Block("n1", "meta")
	c.Advance(3*time.Second + time.Nanosecond)
	if tr.Alive("n1", 3*time.Second) {
		t.Fatal("n1 alive 3s after its last ping")
	}
	if tr.Alive("unknown", time.Hour) {
		t.Fatal("unknown node reported alive")
	}
}

func TestLastSeqIgnoresReordering(t *testing.T) {
	c, n, d := setup(sim.Faults{MinDelay: 0, MaxDelay: 3 * time.Second})
	tr := NewTracker(d, "meta")
	tr.Start()
	s := NewSender(d, "n1", "meta", 100*time.Millisecond)
	s.Start()
	c.Advance(time.Second) // ~10 pings in flight with delays up to 3 s
	n.Block("n1", "meta")
	c.Advance(10 * time.Second)
	ps := tr.Peers()
	if len(ps) != 1 {
		t.Fatalf("peers = %v", ps)
	}
	if ps[0].LastSeq == 0 || ps[0].LastSeq > s.Stats().Sent {
		t.Fatalf("lastSeq = %d, sent = %d", ps[0].LastSeq, s.Stats().Sent)
	}
}

func TestMissingDependencyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewSender with empty Deps did not panic")
		}
	}()
	NewSender(Deps{}, "a", "b", time.Second)
}
