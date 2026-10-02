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
	frozen   map[iface.NodeID][]iface.Message // held until Thaw
	slow     map[iface.NodeID]time.Duration
	async    map[asyncKey]*asyncCall
	stats    NetStats
}

type asyncKey struct {
	id  iface.NodeID
	req uint64
}

type asyncCall struct {
	cb    func(iface.Result)
	timer iface.Timer
	sent  iface.Instant
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
		frozen:   map[iface.NodeID][]iface.Message{},
		slow:     map[iface.NodeID]time.Duration{},
		async:    map[asyncKey]*asyncCall{},
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
	if _, ok := n.frozen[m.From]; ok {
		n.frozen[m.From] = append(n.frozen[m.From], m)
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
	d += n.slow[m.From] + n.slow[m.To]
	n.clock.AfterFunc(d, func() { n.deliver(m) })
}

func (n *Net) deliver(m iface.Message) {
	if n.cut(m.From, m.To) {
		n.stats.Dropped++
		return
	}
	if _, ok := n.frozen[m.To]; ok {
		n.frozen[m.To] = append(n.frozen[m.To], m)
		return
	}
	if m.Kind == KindResponse {
		if a, ok := n.async[asyncKey{m.To, m.ReqID}]; ok {
			delete(n.async, asyncKey{m.To, m.ReqID})
			a.timer.Stop()
			n.stats.Delivered++
			n.stats.Bytes += uint64(len(m.Body))
			r := iface.Result{Body: m.Body, Latency: n.clock.Now().Sub(a.sent)}
			if m.Err != nil {
				r = iface.Result{Err: m.Err, Latency: r.Latency}
			}
			a.cb(r)
			return
		}
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

// Unblock removes one direction of a block.
func (n *Net) Unblock(from, to iface.NodeID) { delete(n.blocked, link{from, to}) }

// Blocked reports whether messages from `from` to `to` are dropped.
func (n *Net) Blocked(from, to iface.NodeID) bool { return n.blocked[link{from, to}] }

// Heal removes all blocks (not crashes).
func (n *Net) Heal() { clear(n.blocked) }

// Crash makes a node unreachable in both directions until Restart.
func (n *Net) Crash(id iface.NodeID) { n.down[id] = true }

// Restart makes a crashed node reachable again.
func (n *Net) Restart(id iface.NodeID) { delete(n.down, id) }

// Caller is an iface.Caller on the sim network. Do and Hedge advance the
// shared clock until their calls are answered or time out, so a client
// written as blocking code still runs deterministically on the sim goroutine.
type Caller struct {
	net     *Net
	id      iface.NodeID
	timeout time.Duration
	nextReq uint64
	pending map[uint64]*call
}

type call struct {
	res      *iface.Result
	sent     iface.Instant
	deadline iface.Instant
}

var _ iface.Caller = (*Caller)(nil)

// NewCaller registers a client endpoint id on the network. Each call times
// out after timeout of simulated time.
func (n *Net) NewCaller(id iface.NodeID, timeout time.Duration) *Caller {
	c := &Caller{net: n, id: id, timeout: timeout, pending: map[uint64]*call{}}
	n.Listen(id, c.handle)
	return c
}

func (c *Caller) handle(m iface.Message) {
	p, ok := c.pending[m.ReqID]
	if !ok || m.Kind != KindResponse {
		return // late or duplicate response
	}
	if m.Err != nil {
		p.res.Err = m.Err
	} else {
		p.res.Body = m.Body
	}
	p.res.Latency = c.net.clock.Now().Sub(p.sent)
	delete(c.pending, m.ReqID)
}

// Sleep advances simulated time; clients use it to back off between retries.
func (c *Caller) Sleep(d time.Duration) { c.net.clock.Advance(d) }

func (c *Caller) send(ic iface.Call, res *iface.Result) uint64 {
	c.nextReq++
	now := c.net.clock.Now()
	c.pending[c.nextReq] = &call{res: res, sent: now, deadline: now.Add(c.timeout)}
	c.net.Send(ic.To, iface.Message{From: c.id, Kind: ic.Kind, ReqID: c.nextReq, Body: ic.Body})
	return c.nextReq
}

// expire fails a pending call as unavailable.
func (c *Caller) expire(req uint64, ic iface.Call, ctx context.Context) {
	p := c.pending[req]
	delete(c.pending, req)
	p.res.Latency = c.net.clock.Now().Sub(p.sent)
	if ctx.Err() != nil {
		p.res.Err = iface.Errorf(iface.CodeUnavailable, "%s to %s: %v", ic.Kind, ic.To, ctx.Err())
	} else {
		p.res.Err = iface.Errorf(iface.CodeUnavailable, "%s to %s: timed out after %v", ic.Kind, ic.To, c.timeout)
	}
}

// Do sends every call, then steps the clock until all are answered, the
// timeout passes, or ctx is cancelled.
func (c *Caller) Do(ctx context.Context, calls []iface.Call) []iface.Result {
	results := make([]iface.Result, len(calls))
	ids := make([]uint64, len(calls))
	for i, ic := range calls {
		ids[i] = c.send(ic, &results[i])
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
			c.expire(id, calls[i], ctx)
		}
	}
	return results
}

// Hedge implements iface.Caller. Launches, timeouts and deliveries all
// happen at clock instants, so the winner is a function of the seed.
func (c *Caller) Hedge(ctx context.Context, calls []iface.Call, after time.Duration, accept func(int, iface.Result) bool) iface.HedgeResult {
	out := iface.HedgeResult{Winner: -1, Results: make([]iface.Result, len(calls))}
	if len(calls) == 0 {
		return out
	}
	ids := make([]uint64, len(calls))
	settled := make([]bool, len(calls))
	var lastLaunch iface.Instant
	launch := func() {
		i := out.Launched
		ids[i] = c.send(calls[i], &out.Results[i])
		out.Launched++
		lastLaunch = c.net.clock.Now()
	}
	launch()
	for ctx.Err() == nil {
		now := c.net.clock.Now()
		allFailed := true
		for i := range out.Launched {
			if settled[i] {
				continue
			}
			p, open := c.pending[ids[i]]
			if open && now >= p.deadline {
				c.expire(ids[i], calls[i], ctx)
				open = false
			}
			if open {
				allFailed = false
				continue
			}
			settled[i] = true
			if out.Results[i].Err == nil && accept(i, out.Results[i]) {
				out.Winner = i
				break
			}
		}
		if out.Winner >= 0 {
			break
		}
		more := out.Launched < len(calls)
		if more && (allFailed || now >= lastLaunch.Add(after)) {
			launch()
			continue
		}
		if allFailed && !more {
			break
		}
		// Wake at the next launch, the next deadline, or the next event.
		wake := iface.Instant(-1)
		if more {
			wake = lastLaunch.Add(after)
		}
		for i := range out.Launched {
			if p, open := c.pending[ids[i]]; open && (wake < 0 || p.deadline < wake) {
				wake = p.deadline
			}
		}
		if next, ok := c.net.clock.Next(); ok && next <= wake {
			c.net.clock.Step()
		} else {
			c.net.clock.Advance(wake.Sub(now))
		}
	}
	now := c.net.clock.Now()
	for i := range out.Launched {
		if p, open := c.pending[ids[i]]; open {
			delete(c.pending, ids[i])
			out.Results[i] = iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "%s to %s: abandoned", calls[i].Kind, calls[i].To),
				Latency: now.Sub(p.sent), Pending: true}
		}
	}
	return out
}

// Gather implements iface.Caller on the sim clock, like Hedge.
func (c *Caller) Gather(ctx context.Context, calls []iface.Call, first, need int, after time.Duration, accept func(int, iface.Result) bool) iface.GatherResult {
	out := iface.GatherResult{Results: make([]iface.Result, len(calls))}
	ids := make([]uint64, len(calls))
	settled := make([]bool, len(calls))
	var lastNews iface.Instant // the last launch or accepted answer
	launch := func() {
		i := out.Launched
		ids[i] = c.send(calls[i], &out.Results[i])
		out.Launched++
		lastNews = c.net.clock.Now()
	}
	for out.Launched < min(max(first, 1), len(calls)) {
		launch()
	}
	for ctx.Err() == nil && len(out.Accepted) < need {
		now := c.net.clock.Now()
		outstanding, replace := 0, 0
		for i := range out.Launched {
			if settled[i] {
				continue
			}
			p, open := c.pending[ids[i]]
			if open && now >= p.deadline {
				c.expire(ids[i], calls[i], ctx)
				open = false
			}
			if open {
				outstanding++
				continue
			}
			settled[i] = true
			if out.Results[i].Err == nil && accept(i, out.Results[i]) {
				out.Accepted = append(out.Accepted, i)
				lastNews = now
			} else {
				replace++
			}
		}
		if len(out.Accepted) >= need {
			break
		}
		for ; replace > 0 && out.Launched < len(calls); replace-- {
			launch()
			outstanding++
		}
		more := out.Launched < len(calls)
		if more && (outstanding == 0 || now >= lastNews.Add(after)) {
			launch()
			continue
		}
		if outstanding == 0 {
			break
		}
		// Wake at the next launch, the next deadline, or the next event.
		wake := iface.Instant(-1)
		if more {
			wake = lastNews.Add(after)
		}
		for i := range out.Launched {
			if p, open := c.pending[ids[i]]; open && (wake < 0 || p.deadline < wake) {
				wake = p.deadline
			}
		}
		if next, ok := c.net.clock.Next(); ok && next <= wake {
			c.net.clock.Step()
		} else {
			c.net.clock.Advance(wake.Sub(now))
		}
	}
	now := c.net.clock.Now()
	for i := range out.Launched {
		if p, open := c.pending[ids[i]]; open {
			delete(c.pending, ids[i])
			out.Results[i] = iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "%s to %s: abandoned", calls[i].Kind, calls[i].To),
				Latency: now.Sub(p.sent), Pending: true}
		}
	}
	return out
}

// AsyncCaller returns an iface.AsyncCaller for code running as node id.
// Responses are matched by request ID before id's Listen handler sees them.
func (n *Net) AsyncCaller(id iface.NodeID, timeout time.Duration) iface.AsyncCaller {
	return &asyncCaller{net: n, id: id, timeout: timeout}
}

type asyncCaller struct {
	net     *Net
	id      iface.NodeID
	timeout time.Duration
	next    uint64
}

func (a *asyncCaller) Go(c iface.Call, cb func(iface.Result)) {
	a.next++
	key := asyncKey{a.id, a.next}
	sent := a.net.clock.Now()
	timer := a.net.clock.AfterFunc(a.timeout, func() {
		if _, ok := a.net.async[key]; !ok {
			return
		}
		delete(a.net.async, key)
		cb(iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "%s to %s: timed out after %v", c.Kind, c.To, a.timeout), Latency: a.timeout})
	})
	a.net.async[key] = &asyncCall{cb: cb, timer: timer, sent: sent}
	a.net.Send(c.To, iface.Message{From: a.id, Kind: c.Kind, ReqID: a.next, Body: c.Body})
}

// Freeze stops a node without killing it, like a long GC pause or a
// SIGSTOP: messages to and from it are held, then released by Thaw.
// SIMPLIFIED: the node's timers keep firing, so its heartbeats queue up and
// arrive as a burst on thaw. A real paused process fires them late instead.
func (n *Net) Freeze(id iface.NodeID) {
	if _, ok := n.frozen[id]; !ok {
		n.frozen[id] = []iface.Message{}
	}
}

// Thaw releases a frozen node's held messages, each with a fresh delay.
func (n *Net) Thaw(id iface.NodeID) {
	held, ok := n.frozen[id]
	if !ok {
		return
	}
	delete(n.frozen, id)
	for _, m := range held {
		n.schedule(m)
	}
}

// Frozen reports whether a node is frozen.
func (n *Net) Frozen(id iface.NodeID) bool {
	_, ok := n.frozen[id]
	return ok
}

// SetSlow adds d to every message to or from id: a gray failure, alive to
// the detector but slow to serve. Zero clears it.
func (n *Net) SetSlow(id iface.NodeID, d time.Duration) {
	if d <= 0 {
		delete(n.slow, id)
		return
	}
	n.slow[id] = d
}

// Slow returns the delay SetSlow added for id, or 0.
func (n *Net) Slow(id iface.NodeID) time.Duration { return n.slow[id] }

// Crashed reports whether a node is crashed.
func (n *Net) Crashed(id iface.NodeID) bool { return n.down[id] }
