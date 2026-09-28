// Package heartbeat sends periodic pings from storage nodes to the metadata
// server and tracks when each node was last heard from. It is the seed of the
// failure detector: later phases replace Ping with heartbeats carrying chunk
// reports and add suspicion before declaring a node dead.
package heartbeat

import (
	"cmp"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/iface"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Message kinds on the transport.
const (
	KindPing = "chunkd.v1.PingRequest"
	KindPong = "chunkd.v1.PingResponse"
)

// Deps are the environment a heartbeat component runs in.
type Deps struct {
	Clock iface.Clock
	Net   iface.Transport
	Rand  iface.Rand
	Log   *slog.Logger
}

func (d Deps) check() {
	if d.Clock == nil || d.Net == nil || d.Rand == nil || d.Log == nil {
		panic("heartbeat: missing dependency")
	}
}

// SenderStats counts one node's pings.
type SenderStats struct {
	Sent       uint64 `json:"sent"`
	Acked      uint64 `json:"acked"`
	LastAckSeq uint64 `json:"lastAckSeq"`
}

// Sender pings a target every interval and counts acknowledgements.
type Sender struct {
	d        Deps
	id       iface.NodeID
	target   iface.NodeID
	interval time.Duration
	stats    SenderStats
}

// NewSender returns a stopped sender; call Start to begin pinging.
func NewSender(d Deps, id, target iface.NodeID, interval time.Duration) *Sender {
	d.check()
	return &Sender{d: d, id: id, target: target, interval: interval}
}

// Start registers the sender on the transport and schedules the first ping
// after a random fraction of the interval, so nodes started together do not
// ping in lockstep.
func (s *Sender) Start() {
	s.d.Net.Listen(s.id, s.handle)
	s.d.Clock.AfterFunc(time.Duration(s.d.Rand.IntN(int(s.interval))), s.tick)
}

// Stats returns the sender's counters.
func (s *Sender) Stats() SenderStats { return s.stats }

func (s *Sender) tick() {
	s.stats.Sent++
	body, err := proto.Marshal(&chunkdv1.PingRequest{From: string(s.id), Seq: s.stats.Sent})
	if err != nil {
		panic(err) // a fixed schema that fails to marshal is a programming error
	}
	s.d.Net.Send(s.target, iface.Message{From: s.id, Kind: KindPing, ReqID: s.stats.Sent, Body: body})
	s.d.Clock.AfterFunc(s.interval, s.tick)
}

func (s *Sender) handle(m iface.Message) {
	if m.Kind != KindPong {
		return
	}
	var resp chunkdv1.PingResponse
	if err := proto.Unmarshal(m.Body, &resp); err != nil {
		s.d.Log.Warn("bad pong", "from", m.From, "err", err)
		return
	}
	s.stats.Acked++ // duplicates count; acked can exceed sent under duplication
	s.stats.LastAckSeq = max(s.stats.LastAckSeq, resp.Seq)
}

// PeerState is what the tracker knows about one node.
type PeerState struct {
	ID       iface.NodeID  `json:"id"`
	Pings    uint64        `json:"pings"`
	LastSeq  uint64        `json:"lastSeq"`
	LastSeen iface.Instant `json:"lastSeen"`
}

// Tracker answers pings and records when each peer was last heard from.
type Tracker struct {
	d     Deps
	id    iface.NodeID
	peers map[iface.NodeID]*PeerState
}

// NewTracker returns a tracker; call Start to begin answering pings.
func NewTracker(d Deps, id iface.NodeID) *Tracker {
	d.check()
	return &Tracker{d: d, id: id, peers: map[iface.NodeID]*PeerState{}}
}

// Start registers the tracker on the transport.
func (t *Tracker) Start() { t.d.Net.Listen(t.id, t.handle) }

func (t *Tracker) handle(m iface.Message) {
	if m.Kind != KindPing {
		return
	}
	var req chunkdv1.PingRequest
	if err := proto.Unmarshal(m.Body, &req); err != nil {
		t.d.Log.Warn("bad ping", "from", m.From, "err", err)
		return
	}
	p := t.peers[m.From]
	if p == nil {
		p = &PeerState{ID: m.From}
		t.peers[m.From] = p
		t.d.Log.Info("node joined", "node", m.From)
	}
	p.Pings++
	p.LastSeq = max(p.LastSeq, req.Seq) // delayed pings arrive out of order
	p.LastSeen = t.d.Clock.Now()

	body, err := proto.Marshal(&chunkdv1.PingResponse{From: string(t.id), Seq: req.Seq})
	if err != nil {
		panic(err)
	}
	t.d.Net.Send(m.From, iface.Message{From: t.id, Kind: KindPong, ReqID: m.ReqID, Body: body})
}

// Peers returns every known peer sorted by ID.
func (t *Tracker) Peers() []PeerState {
	out := make([]PeerState, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b PeerState) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Alive reports whether id was heard from within timeout of now.
// SIMPLIFIED: a fixed timeout. HDFS marks a DataNode dead after
// 2*recheck + 10*heartbeat (about 10.5 min); Cassandra uses a phi-accrual
// detector that adapts to observed inter-arrival times.
func (t *Tracker) Alive(id iface.NodeID, timeout time.Duration) bool {
	p := t.peers[id]
	return p != nil && t.d.Clock.Now().Sub(p.LastSeen) <= timeout
}
