package sim

import (
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Faults configures message-level fault injection. Rates are probabilities in
// [0, 1]; delay is drawn uniformly from [MinDelay, MaxDelay].
type Faults struct {
	DropRate float64
	DupRate  float64
	MinDelay time.Duration
	MaxDelay time.Duration
}

// NetStats counts message outcomes since the network was created.
type NetStats struct {
	Sent, Delivered, Dropped, Duplicated uint64
}

// Net is an in-memory Transport. Every delivery is an event on the shared
// Clock, and every random choice comes from the seeded Rand, so message
// order is reproducible from the seed.
type Net struct {
	clock    *Clock
	rng      iface.Rand
	faults   Faults
	handlers map[iface.NodeID]iface.Handler
	blocked  map[link]bool
	stats    NetStats
}

type link struct{ from, to iface.NodeID }

var _ iface.Transport = (*Net)(nil)

// NewNet returns a network that schedules deliveries on c and draws faults from r.
func NewNet(c *Clock, r iface.Rand, f Faults) *Net {
	return &Net{
		clock:    c,
		rng:      r,
		faults:   f,
		handlers: map[iface.NodeID]iface.Handler{},
		blocked:  map[link]bool{},
	}
}

// Listen registers h as the receiver for messages addressed to id.
func (n *Net) Listen(id iface.NodeID, h iface.Handler) { n.handlers[id] = h }

// SetFaults replaces the fault configuration for messages sent from now on.
func (n *Net) SetFaults(f Faults) { n.faults = f }

// Stats returns message counters.
func (n *Net) Stats() NetStats { return n.stats }

// Send schedules delivery of m to `to`. Blocked links drop at send time and
// again at delivery time, so a partition also kills messages in flight.
func (n *Net) Send(to iface.NodeID, m iface.Message) {
	m.To = to
	n.stats.Sent++
	if n.blocked[link{m.From, to}] || chance(n.rng, n.faults.DropRate) {
		n.stats.Dropped++
		return
	}
	n.schedule(m)
	if chance(n.rng, n.faults.DupRate) {
		n.stats.Duplicated++
		n.schedule(m)
	}
}

func (n *Net) schedule(m iface.Message) {
	d := n.faults.MinDelay
	if span := n.faults.MaxDelay - n.faults.MinDelay; span > 0 {
		d += time.Duration(n.rng.IntN(int(span) + 1))
	}
	n.clock.AfterFunc(d, func() {
		h, ok := n.handlers[m.To]
		if !ok || n.blocked[link{m.From, m.To}] {
			n.stats.Dropped++
			return
		}
		n.stats.Delivered++
		h(m)
	})
}

// Block drops all messages from `from` to `to` until Heal.
func (n *Net) Block(from, to iface.NodeID) { n.blocked[link{from, to}] = true }

// Partition blocks traffic in both directions between every node in a and
// every node in b.
func (n *Net) Partition(a, b []iface.NodeID) {
	for _, x := range a {
		for _, y := range b {
			n.Block(x, y)
			n.Block(y, x)
		}
	}
}

// Heal removes all blocks.
func (n *Net) Heal() { clear(n.blocked) }
