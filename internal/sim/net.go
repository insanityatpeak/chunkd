package sim

import (
	"context"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Faults configures message-level fault injection. Rates are probabilities in
// [0, 1]; delay is drawn uniformly from [MinDelay, MaxDelay]. BytesPerSec, if
// set, adds transfer time proportional to the body size.
type Faults struct {
	DropRate    float64
	DupRate     float64
	MinDelay    time.Duration
	MaxDelay    time.Duration
	BytesPerSec int64
}

// NetStats counts message outcomes since the network was created.
type NetStats struct {
	Sent       uint64 `json:"sent"`
	Delivered  uint64 `json:"delivered"`
	Dropped    uint64 `json:"dropped"`
	Duplicated uint64 `json:"duplicated"`
	Bytes      uint64 `json:"bytes"`
}

// KindResponse marks RPC responses on the wire.
const KindResponse = "rpc.response"

// Net is an in-memory Transport. Every delivery is an event on the shared
// Clock, and every random choice comes from the seeded Rand, so message
// order is reproducible from the seed.
type Net struct {
	clock    *Clock
	rng      iface.Rand
	faults   Faults
	handlers map[iface.NodeID]iface.Handler
	rpcs     map[rpcKey]iface.RPCHandler
	blocked  map[link]bool
	down     map[iface.NodeID]bool
	stats    NetStats
}

type link struct{ from, to iface.NodeID }
type rpcKey struct {
	id   iface.NodeID
	kind string
}

var _ iface.Transport = (*Net)(nil)

// NewNet returns a network that schedules deliveries on c and draws faults from r.
func NewNet(c *Clock, r iface.Rand, f Faults) *Net {
	return &Net{
		clock:    c,
		rng:      r,
		faults:   f,
		handlers: map[iface.NodeID]iface.Handler{},
		rpcs:     map[rpcKey]iface.RPCHandler{},
		blocked:  map[link]bool{},
		down:     map[iface.NodeID]bool{},
	}
}

// Listen registers h as the receiver for one-way messages addressed to id.
func (n *Net) Listen(id iface.NodeID, h iface.Handler) { n.handlers[id] = h }

// Serve registers an RPC handler. Everything runs on the sim goroutine, so
// Concurrent has no effect here.
func (n *Net) Serve(id iface.NodeID, kind string, h iface.RPCHandler, _ iface.ServeOpts) {
	n.rpcs[rpcKey{id, kind}] = h
}

// SetFaults replaces the fault configuration for messages sent from now on.
func (n *Net) SetFaults(f Faults) { n.faults = f }

// Faults returns the current fault configuration.
func (n *Net) Faults() Faults { return n.faults }

// Stats returns message counters.
func (n *Net) Stats() NetStats { return n.stats }

// Send schedules delivery of m to `to`. Blocked links and crashed nodes drop
// at send time and again at delivery time, so a partition also kills
// messages in flight.
func (n *Net) Send(to iface.NodeID, m iface.Message) {
	m.To = to
	n.stats.Sent++
	if n.cut(m.From, to) || chance(n.rng, n.faults.DropRate) {
		n.stats.Dropped++
		return
	}
	n.schedule(m)
	if chance(n.rng, n.faults.DupRate) {
		n.stats.Duplicated++
		n.schedule(m)
	}
}

func (n *Net) cut(from, to iface.NodeID) bool {
	return n.blocked[link{from, to}] || n.down[from] || n.down[to]
}

func (n *Net) schedule(m iface.Message) {
	d := n.faults.MinDelay
	if span := n.faults.MaxDelay - n.faults.MinDelay; span > 0 {
		d += time.Duration(n.rng.IntN(int(span) + 1))
	}
	if bps := n.faults.BytesPerSec; bps > 0 {
		d += time.Duration(int64(len(m.Body)) * int64(time.Second) / bps)
	}
	n.clock.AfterFunc(d, func() { n.deliver(m) })
}

func (n *Net) deliver(m iface.Message) {
	if n.cut(m.From, m.To) {
		n.stats.Dropped++
		return
	}
	if h, ok := n.rpcs[rpcKey{m.To, m.Kind}]; ok {
		n.stats.Delivered++
		n.stats.Bytes += uint64(len(m.Body))
		h(m, n.responder(m))
		return
	}
	h, ok := n.handlers[m.To]
	if !ok {
		n.stats.Dropped++
		return
	}
	n.stats.Delivered++
	n.stats.Bytes += uint64(len(m.Body))
	h(m)
}

// responder sends the response back over the (faulty) network.
func (n *Net) responder(req iface.Message) iface.Responder {
	done := false
	return func(body []byte, err error) {
		if done {
			return
		}
		done = true
		resp := iface.Message{From: req.To, Kind: KindResponse, ReqID: req.ReqID, Body: body}
		if err != nil {
			resp.Body = nil
			resp.Err = iface.AsError(err, iface.CodeInternal)
		}
		n.Send(req.From, resp)
	}
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

// Heal removes all blocks (not crashes).
func (n *Net) Heal() { clear(n.blocked) }

// Crash makes a node unreachable in both directions until Restart.
func (n *Net) Crash(id iface.NodeID) { n.down[id] = true }

// Restart makes a crashed node reachable again.
func (n *Net) Restart(id iface.NodeID) { delete(n.down, id) }

// Caller is an iface.Caller on the sim network. Do advances the shared clock
// until every call is answered or times out, so a client written as
// blocking code still runs deterministically on the sim goroutine.
type Caller struct {
	net     *Net
	id      iface.NodeID
	timeout time.Duration
	nextReq uint64
	pending map[uint64]*iface.Result
}

var _ iface.Caller = (*Caller)(nil)

// NewCaller registers a client endpoint id on the network. Each call times
// out after timeout of simulated time.
func (n *Net) NewCaller(id iface.NodeID, timeout time.Duration) *Caller {
	c := &Caller{net: n, id: id, timeout: timeout, pending: map[uint64]*iface.Result{}}
	n.Listen(id, c.handle)
	return c
}

func (c *Caller) handle(m iface.Message) {
	r, ok := c.pending[m.ReqID]
	if !ok || m.Kind != KindResponse {
		return // late or duplicate response
	}
	if m.Err != nil {
		r.Err = m.Err
	} else {
		r.Body = m.Body
	}
	delete(c.pending, m.ReqID)
}

// Do sends every call, then steps the clock until all are answered, the
// timeout passes, or ctx is cancelled.
func (c *Caller) Do(ctx context.Context, calls []iface.Call) []iface.Result {
	results := make([]iface.Result, len(calls))
	ids := make([]uint64, len(calls))
	for i, call := range calls {
		c.nextReq++
		ids[i] = c.nextReq
		c.pending[ids[i]] = &results[i]
		c.net.Send(call.To, iface.Message{From: c.id, Kind: call.Kind, ReqID: ids[i], Body: call.Body})
	}
	deadline := c.net.clock.Now().Add(c.timeout)
	open := func() bool {
		for _, id := range ids {
			if _, ok := c.pending[id]; ok {
				return true
			}
		}
		return false
	}
	for open() && ctx.Err() == nil {
		next, ok := c.net.clock.Next()
		if !ok || next > deadline {
			c.net.clock.Advance(deadline.Sub(c.net.clock.Now()))
			break
		}
		c.net.clock.Step()
	}
	for i, id := range ids {
		if _, ok := c.pending[id]; ok {
			delete(c.pending, id)
			if ctx.Err() != nil {
				results[i].Err = iface.Errorf(iface.CodeUnavailable, "%s to %s: %v", calls[i].Kind, calls[i].To, ctx.Err())
			} else {
				results[i].Err = iface.Errorf(iface.CodeUnavailable, "%s to %s: timed out after %v", calls[i].Kind, calls[i].To, c.timeout)
			}
		}
	}
	return results
}
