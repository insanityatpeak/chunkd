package sim

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

var lossy = Faults{DropRate: 0.2, DupRate: 0.1, MinDelay: time.Millisecond, MaxDelay: 50 * time.Millisecond}

// trace runs three nodes that each send 50 messages to both peers and
// returns every delivery as "at from->to kind".
func trace(seed uint64, f Faults) []string {
	c := NewClock()
	n := NewNet(c, NewRand(seed), f)
	ids := []iface.NodeID{"a", "b", "c"}
	var out []string
	for _, id := range ids {
		n.Listen(id, func(m iface.Message) {
			out = append(out, fmt.Sprintf("%d %s->%s %s", c.Now(), m.From, m.To, m.Kind))
		})
	}
	for i := range 50 {
		for _, from := range ids {
			for _, to := range ids {
				if from != to {
					n.Send(to, iface.Message{From: from, Kind: fmt.Sprint(i)})
				}
			}
		}
		c.Advance(5 * time.Millisecond)
	}
	c.Advance(time.Second)
	return out
}

func TestNetReplaysFromSeed(t *testing.T) {
	a, b := trace(42, lossy), trace(42, lossy)
	if !slices.Equal(a, b) {
		t.Fatal("same seed produced different delivery traces")
	}
	if slices.Equal(a, trace(43, lossy)) {
		t.Fatal("different seeds produced identical traces; faults are not randomized")
	}
}

func TestNetFaultCounts(t *testing.T) {
	tests := []struct {
		name   string
		faults Faults
		check  func(NetStats) error
	}{
		{"reliable", Faults{}, func(s NetStats) error {
			if s.Dropped != 0 || s.Duplicated != 0 || s.Delivered != s.Sent {
				return fmt.Errorf("stats %+v, want all delivered once", s)
			}
			return nil
		}},
		{"drop all", Faults{DropRate: 1}, func(s NetStats) error {
			if s.Delivered != 0 || s.Dropped != s.Sent {
				return fmt.Errorf("stats %+v, want all dropped", s)
			}
			return nil
		}},
		{"dup all", Faults{DupRate: 1}, func(s NetStats) error {
			if s.Delivered != 2*s.Sent {
				return fmt.Errorf("stats %+v, want every message delivered twice", s)
			}
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClock()
			n := NewNet(c, NewRand(1), tt.faults)
			n.Listen("b", func(iface.Message) {})
			for range 100 {
				n.Send("b", iface.Message{From: "a"})
			}
			c.Advance(time.Second)
			if err := tt.check(n.Stats()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNetDelayBounds(t *testing.T) {
	c := NewClock()
	n := NewNet(c, NewRand(7), Faults{MinDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond})
	var lat []time.Duration
	n.Listen("b", func(iface.Message) { lat = append(lat, time.Duration(c.Now())) })
	for range 200 {
		n.Send("b", iface.Message{From: "a"})
	}
	c.Advance(time.Second)
	if len(lat) != 200 {
		t.Fatalf("delivered %d, want 200", len(lat))
	}
	if lo, hi := slices.Min(lat), slices.Max(lat); lo < 10*time.Millisecond || hi > 20*time.Millisecond || lo == hi {
		t.Fatalf("latency range [%v, %v], want spread within [10ms, 20ms]", lo, hi)
	}
}

func TestNetPartitionDropsInFlight(t *testing.T) {
	c := NewClock()
	n := NewNet(c, NewRand(1), Faults{MinDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond})
	got := 0
	n.Listen("b", func(iface.Message) { got++ })

	n.Send("b", iface.Message{From: "a"}) // in flight when the partition starts
	c.Advance(5 * time.Millisecond)
	n.Partition([]iface.NodeID{"a"}, []iface.NodeID{"b"})
	n.Send("b", iface.Message{From: "a"})
	c.Advance(time.Second)
	if got != 0 {
		t.Fatalf("delivered %d across partition, want 0", got)
	}

	n.Heal()
	n.Send("b", iface.Message{From: "a"})
	c.Advance(time.Second)
	if got != 1 {
		t.Fatalf("delivered %d after heal, want 1", got)
	}
}

func TestNetBlockIsOneWay(t *testing.T) {
	c := NewClock()
	n := NewNet(c, NewRand(1), Faults{})
	var got []iface.NodeID
	for _, id := range []iface.NodeID{"a", "b"} {
		n.Listen(id, func(m iface.Message) { got = append(got, m.To) })
	}
	n.Block("a", "b")
	n.Send("b", iface.Message{From: "a"})
	n.Send("a", iface.Message{From: "b"})
	c.Advance(time.Second)
	if !slices.Equal(got, []iface.NodeID{"a"}) {
		t.Fatalf("deliveries to %v, want only a", got)
	}
}
