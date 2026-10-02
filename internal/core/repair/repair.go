// Package repair restores the replication factor of chunks that lost
// replicas. It decides what to copy, from where, to where and when; the
// metadata server turns each decision into a command to the target node.
package repair

import (
	"bytes"
	"cmp"
	"container/heap"
	"slices"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/core/rebalance"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Config sets the replication target, the delay and the throttle.
type Config struct {
	Replicas int
	// Delay is how long after a node is declared dead its replicas are
	// still expected back. A restart inside it costs zero copies.
	Delay time.Duration
	// MaxInFlight bounds concurrent copies cluster-wide; PerSource and
	// PerTarget bound them per node, so one node's disk and NIC are never
	// the whole repair.
	MaxInFlight int
	PerSource   int
	PerTarget   int
	// BytesPerSec caps repair traffic; Burst is the bucket size.
	BytesPerSec int64
	Burst       int64
	// CopyTimeout releases a copy's slots if the target never reports it:
	// the command or the completion report was lost. One copy is one
	// chunk (<= 4 MiB), well under a second even throttled.
	CopyTimeout time.Duration
	// ScanEvery rescans every chunk, catching anything the event-driven
	// path missed (a lost block report, a failed command).
	ScanEvery time.Duration
	// UploadGrace is how long a freshly committed chunk may sit below RF
	// before repair copies it: commit needs 2 of 3 reports, and the third
	// is usually still in flight.
	UploadGrace time.Duration
	// Background bounds the copy slots drain and balance copies may hold
	// together, so a failure always finds slots for repair (ADR-0020).
	Background int
	// BandPercent is the balancer's tolerated distance from a node's target.
	BandPercent int64
}

// DefaultConfig: RF 3, 20 s delay, 8 copies in flight (2 per node as source
// and as target, at most 4 for drain and balance), 40 MiB/s, a ±10% band.
func DefaultConfig() Config {
	return Config{
		Replicas:    3,
		Delay:       20 * time.Second,
		MaxInFlight: 8,
		PerSource:   2,
		PerTarget:   2,
		BytesPerSec: 40 << 20,
		Burst:       4 << 20,
		CopyTimeout: 10 * time.Second,
		ScanEvery:   30 * time.Second,
		UploadGrace: 10 * time.Second,
		Background:  4,
		BandPercent: 10,
	}
}

// Holder is a node reported to hold a chunk.
type Holder struct {
	Node      iface.NodeID
	State     detector.State
	DeadSince iface.Instant
	// Confirmed is false between a node's return (restart or death) and its
	// next full block report: the location may be stale.
	Confirmed bool
	Rack      string
	Used      int64
	// Leaving: the node is draining (ADR-0021). Its copy serves reads and can
	// be a source, but does not count toward RF and is never trimmed.
	Leaving bool
}

// View is the scheduler's read-only window onto the metadata server.
type View interface {
	// Want returns a chunk's size, and false if nothing references it.
	Want(id iface.ChunkID) (size int64, ok bool)
	// Chunks calls fn for every wanted chunk, in a deterministic order.
	Chunks(fn func(id iface.ChunkID, size int64))
	// Holders returns every node reported to hold id, dead ones included.
	Holders(id iface.ChunkID) []Holder
	// Target picks a node for a new replica by the placement rules, never
	// one of exclude.
	Target(size int64, exclude []iface.NodeID) (iface.NodeID, bool)
	// Nodes returns every known node that is not leaving, ordered by ID, a
	// node holding nothing included: the candidates for balancing.
	Nodes() []Member
}

// Member is a node that may take part in balancing.
type Member struct {
	ID        iface.NodeID
	Rack      string
	State     detector.State
	DeadSince iface.Instant
	Confirmed bool
}

// eligible: alive and confirmed by a full report since it last returned.
func (m Member) eligible() bool { return m.State == detector.Alive && m.Confirmed }

// Class is a copy's priority: repair before drain before balance.
type Class int

const (
	Repair  Class = iota // a chunk below RF
	Drain                // a chunk at RF only by counting leaving copies
	Balance              // a move from an over-full node to an under-full one
)

func (c Class) String() string {
	switch c {
	case Drain:
		return "drain"
	case Balance:
		return "rebalance"
	}
	return "repair"
}

// Copy is one dispatched replication: Target pulls Chunk from Source.
type Copy struct {
	ID      uint64
	Chunk   iface.ChunkID
	Source  iface.NodeID
	Target  iface.NodeID
	Size    int64
	Started iface.Instant
	Class   Class
}

// Trim is one over-replication removal: Node deletes its copy of Chunk.
type Trim struct {
	ID    uint64
	Chunk iface.ChunkID
	Node  iface.NodeID
}

// Outcome is how a copy ended.
type Outcome int

const (
	Completed Outcome = iota + 1 // the target reported the chunk
	TimedOut
	Failed // the target reported an error
)

func (o Outcome) String() string {
	switch o {
	case Completed:
		return "completed"
	case TimedOut:
		return "timed out"
	case Failed:
		return "failed"
	}
	return "unknown"
}

// Sender delivers the scheduler's decisions to storage nodes. Done and
// Trimmed are optional observers of how copies and trims end.
type Sender struct {
	Copy    func(Copy)
	Trim    func(Trim)
	Done    func(Copy, Outcome)
	Trimmed func(Trim)
}

// Stats are cumulative counters plus current gauges.
type Stats struct {
	Queued     int    `json:"queued"`
	InFlight   int    `json:"inFlight"`
	Waiting    int    `json:"waiting"` // inside the delay, as of the last scan
	Lost       int    `json:"lost"`    // no live replica to copy from
	Dispatched uint64 `json:"dispatched"`
	Completed  uint64 `json:"completed"`
	TimedOut   uint64 `json:"timedOut"`
	Failed     uint64 `json:"failed"`
	Cancelled  uint64 `json:"cancelled"`
	Bytes      uint64 `json:"bytes"`
	Trimmed    uint64 `json:"trimmed"`
	// Evacuated and Moved count completed drain and balance copies; MovedBytes
	// is the balance share of Bytes.
	Evacuated  uint64 `json:"evacuated"`
	Moved      uint64 `json:"moved"`
	MovedBytes uint64 `json:"movedBytes"`
	// Peaks are high-water marks of concurrent copies: cluster-wide, and
	// the most any one node sourced or received at once.
	PeakInFlight  int `json:"peakInFlight"`
	PeakPerSource int `json:"peakPerSource"`
	PeakPerTarget int `json:"peakPerTarget"`
}

// Scheduler owns the repair queue. It runs on the metadata server's loop.
type Scheduler struct {
	cfg    Config
	clock  iface.Clock
	view   View
	send   Sender
	bucket *Bucket

	q        queue
	queued   map[iface.ChunkID]*item
	inflight map[iface.ChunkID]*Copy
	trimming map[iface.ChunkID]Trim
	src, dst map[iface.NodeID]int
	// fresh holds commit times of chunks still inside UploadGrace; entries
	// are dropped when an assessment finds the grace over.
	fresh  map[iface.ChunkID]iface.Instant
	nextID uint64
	seq    uint64
	stats  Stats

	wake   iface.Timer
	wakeAt iface.Instant

	// active: this scheduler may send commands. Only the metadata leader's
	// is; a deposed leader must stop at once, since a command from it would
	// race the new leader's (SetActive).
	active bool
	// graceUntil: after SetActive(true), a chunk with more than one copy left
	// waits until then; zero for a scheduler never deactivated.
	graceUntil iface.Instant

	// load and target are each member's located bytes and balance target as
	// of the last plan; the trim victim rule prefers the most over-target.
	load, target map[iface.NodeID]int64
}

// New returns a scheduler that acts through send.
func New(cfg Config, clock iface.Clock, view View, send Sender) *Scheduler {
	return &Scheduler{
		cfg: cfg, clock: clock, view: view, send: send,
		bucket:   NewBucket(cfg.BytesPerSec, cfg.Burst),
		queued:   map[iface.ChunkID]*item{},
		inflight: map[iface.ChunkID]*Copy{},
		trimming: map[iface.ChunkID]Trim{},
		fresh:    map[iface.ChunkID]iface.Instant{},
		src:      map[iface.NodeID]int{},
		dst:      map[iface.NodeID]int{},
		active:   true,
	}
}

// SetActive turns command sending on or off. Turning it off drops all queue
// and in-flight bookkeeping: it is soft state that belongs to one leader's
// term, and copies already on their way finish harmlessly (the next leader
// trims any surplus). Turning it on scans, and treats every chunk as freshly
// committed for UploadGrace, since the new leader cannot know which commits
// have reports still in flight.
func (s *Scheduler) SetActive(on bool) {
	if s.active == on {
		return
	}
	s.active = on
	if !on {
		s.q, s.queued = nil, map[iface.ChunkID]*item{}
		s.inflight, s.trimming = map[iface.ChunkID]*Copy{}, map[iface.ChunkID]Trim{}
		s.src, s.dst = map[iface.NodeID]int{}, map[iface.NodeID]int{}
		s.fresh = map[iface.ChunkID]iface.Instant{}
		if s.wake != nil {
			s.wake.Stop()
			s.wake = nil
		}
		return
	}
	s.graceUntil = s.clock.Now().Add(s.cfg.UploadGrace)
	s.Scan()
}

// Start begins periodic scans.
func (s *Scheduler) Start() {
	var periodic func()
	periodic = func() {
		s.Scan()
		s.clock.AfterFunc(s.cfg.ScanEvery, periodic)
	}
	s.clock.AfterFunc(s.cfg.ScanEvery, periodic)
}

// assessment is one chunk's replication state at an instant.
type assessment struct {
	holders []Holder
	live    int // alive or suspect and not leaving: counted toward RF
	leaving int // alive or suspect and leaving: a source, not counted
	missing int // Replicas - live, or 0
	// sure are alive, non-leaving holders whose location is confirmed by a
	// full report since they last returned. Only these may justify a trim.
	sure []Holder
	lost bool // missing, and nothing to copy from
	// class is Drain when the leaving copies still make up RF, else Repair.
	class Class
	// readyAt is when repair may start: now, or the end of the delay for
	// the latest-dead holder whose absence is still excused.
	readyAt iface.Instant
}

func (s *Scheduler) assess(id iface.ChunkID, now iface.Instant) assessment {
	a := assessment{holders: s.view.Holders(id), readyAt: now}
	var excused []iface.Instant
	for _, h := range a.holders {
		switch {
		case (h.State == detector.Alive || h.State == detector.Suspect) && h.Leaving:
			a.leaving++
		case h.State == detector.Alive || h.State == detector.Suspect:
			a.live++
			if h.State == detector.Alive && h.Confirmed {
				a.sure = append(a.sure, h)
			}
		case h.State == detector.Dead && !h.Leaving && now < h.DeadSince.Add(s.cfg.Delay):
			excused = append(excused, h.DeadSince.Add(s.cfg.Delay))
		}
	}
	a.missing = max(s.cfg.Replicas-a.live, 0)
	if a.missing == 0 {
		return a
	}
	copies := a.live + a.leaving
	if copies == 0 {
		a.lost = true
		return a
	}
	if copies >= s.cfg.Replicas {
		a.class = Drain
	}
	// A single remaining copy is not worth the gamble: repair at once.
	// Otherwise wait while recently dead holders would still cover the gap.
	if copies > 1 && a.live+len(excused) >= s.cfg.Replicas {
		slices.Sort(excused)
		// Ready once enough excuses expire that the gap is real.
		a.readyAt = excused[a.live+len(excused)-s.cfg.Replicas]
	}
	if copies > 1 && now < s.graceUntil {
		a.readyAt = max(a.readyAt, s.graceUntil)
	}
	// A just-committed chunk's missing report is usually in flight, not
	// lost: wait out the grace (bugs-found #9).
	if t, ok := s.fresh[id]; ok {
		switch end := t.Add(s.cfg.UploadGrace); {
		case now >= end:
			delete(s.fresh, id)
		case copies > 1:
			a.readyAt = max(a.readyAt, end)
		}
	}
	return a
}

// Fresh records that chunks were just committed.
func (s *Scheduler) Fresh(ids []iface.ChunkID) {
	now := s.clock.Now()
	for _, id := range ids {
		s.fresh[id] = now
	}
}

// Scan re-evaluates every wanted chunk, queues the ones that need a copy
// now and schedules a wake-up for the ones still inside the delay.
func (s *Scheduler) Scan() {
	if !s.active {
		return
	}
	now := s.clock.Now()
	waiting, lost := 0, 0
	next := iface.Instant(-1)
	s.view.Chunks(func(id iface.ChunkID, _ int64) {
		if s.inflight[id] != nil {
			return
		}
		a := s.assess(id, now)
		switch {
		case a.missing == 0:
			if it := s.queued[id]; it != nil {
				heap.Remove(&s.q, it.idx)
				delete(s.queued, id)
				s.stats.Cancelled++
			}
			if len(a.sure) > s.cfg.Replicas {
				s.trim(id, a.sure)
			}
		case a.lost:
			lost++
		case a.readyAt > now:
			waiting++
			if next < 0 || a.readyAt < next {
				next = a.readyAt
			}
		default:
			s.enqueue(id, a.live, a.class)
		}
	})
	s.stats.Waiting, s.stats.Lost = waiting, lost
	if next >= 0 {
		s.wakeAtLeast(next)
	}
	// Balance only once repair and drain have nothing waiting, queued or in
	// flight: their copies change the loads the plan is made from.
	if waiting == 0 && !s.urgent() {
		s.planBalance()
	}
	s.dispatch()
}

// Recheck assesses chunks that lost a copy without a death (a replica
// failed verification and was quarantined) and queues the short ones at
// once: the repair delay only excuses holders that are dead and may return.
func (s *Scheduler) Recheck(ids []iface.ChunkID) {
	if !s.active {
		return
	}
	now := s.clock.Now()
	for _, id := range ids {
		if _, ok := s.view.Want(id); !ok || s.inflight[id] != nil {
			continue
		}
		switch a := s.assess(id, now); {
		case a.missing == 0, a.lost:
		case a.readyAt > now:
			s.wakeAtLeast(a.readyAt)
		default:
			s.enqueue(id, a.live, a.class)
		}
	}
	s.dispatch()
}

func (s *Scheduler) enqueue(id iface.ChunkID, live int, class Class) {
	if it := s.queued[id]; it != nil {
		if it.live != live || it.class != class {
			// A planned move gives way: the chunk needs a copy for RF now.
			it.live, it.class, it.move = live, class, nil
			heap.Fix(&s.q, it.idx)
		}
		return
	}
	s.seq++
	it := &item{id: id, live: live, class: class, seq: s.seq}
	heap.Push(&s.q, it)
	s.queued[id] = it
}

// urgent reports repair or drain work queued or in flight.
func (s *Scheduler) urgent() bool {
	for _, it := range s.queued {
		if it.class < Balance {
			return true
		}
	}
	for _, c := range s.inflight {
		if c.Class < Balance {
			return true
		}
	}
	return false
}

// wakeAtLeast schedules a Scan no later than at.
func (s *Scheduler) wakeAtLeast(at iface.Instant) {
	if s.wake != nil && s.wakeAt <= at {
		return
	}
	if s.wake != nil {
		s.wake.Stop()
	}
	s.wakeAt = at
	s.wake = s.clock.AfterFunc(at.Sub(s.clock.Now()), func() {
		s.wake = nil
		s.Scan()
	})
}

// dispatch starts copies from the head of the queue while slots and tokens
// allow. Items blocked on a per-node limit are skipped, not dropped, so one
// busy node does not stall the rest of the queue.
func (s *Scheduler) dispatch() {
	if !s.active {
		return
	}
	now := s.clock.Now()
	var blocked []*item
	defer func() {
		for _, it := range blocked {
			heap.Push(&s.q, it)
			s.queued[it.id] = it
		}
	}()
	background := 0
	for _, c := range s.inflight {
		if c.Class > Repair {
			background++
		}
	}
	// Copies cannot be preempted, so priority is enforced at dispatch: while
	// a repair waits on a per-node limit, nothing of a lower class starts,
	// and drain and balance never hold more than Background slots.
	repairBlocked := false
	for s.q.Len() > 0 && len(s.inflight) < s.cfg.MaxInFlight {
		it := heap.Pop(&s.q).(*item)
		delete(s.queued, it.id)
		if it.class > Repair && (repairBlocked || background >= s.cfg.Background) {
			blocked = append(blocked, it)
			continue
		}
		size, ok := s.view.Want(it.id)
		if !ok {
			s.stats.Cancelled++
			continue
		}
		if it.move != nil {
			switch s.startMove(it, size, now) {
			case moveBlocked:
				blocked = append(blocked, it)
			case moveStarted:
				background++
			case moveNoTokens:
				blocked = append(blocked, it)
				s.wakeAtLeast(s.bucket.ReadyAt(size, now))
				return
			}
			continue
		}
		// Re-check: the node may have come back, or a report arrived, since
		// the chunk was queued. This is how a node returning inside the
		// delay cancels its repairs.
		a := s.assess(it.id, now)
		if a.missing == 0 {
			s.stats.Cancelled++
			continue
		}
		if a.lost {
			continue
		}
		if a.readyAt > now {
			s.wakeAtLeast(a.readyAt)
			continue
		}
		it.class = a.class
		if a.class > Repair && (repairBlocked || background >= s.cfg.Background) {
			blocked = append(blocked, it)
			continue
		}
		source, ok := s.pickSource(a.holders)
		if !ok {
			blocked = append(blocked, it)
			repairBlocked = repairBlocked || a.class == Repair
			continue
		}
		exclude := make([]iface.NodeID, 0, len(a.holders)+len(s.dst))
		for _, h := range a.holders {
			exclude = append(exclude, h.Node)
		}
		for n, c := range s.dst {
			if c >= s.cfg.PerTarget {
				exclude = append(exclude, n)
			}
		}
		target, ok := s.view.Target(size, exclude)
		if !ok {
			blocked = append(blocked, it)
			repairBlocked = repairBlocked || a.class == Repair
			continue
		}
		if !s.bucket.Take(size, now) {
			blocked = append(blocked, it)
			s.wakeAtLeast(s.bucket.ReadyAt(size, now))
			return
		}
		s.launch(it.id, source, target, size, a.class, now)
		if a.class > Repair {
			background++
		}
	}
}

func (s *Scheduler) launch(id iface.ChunkID, source, target iface.NodeID, size int64, class Class, now iface.Instant) {
	s.nextID++
	c := &Copy{ID: s.nextID, Chunk: id, Source: source, Target: target, Size: size, Started: now, Class: class}
	s.inflight[id] = c
	s.src[source]++
	s.dst[target]++
	s.stats.Dispatched++
	s.stats.PeakInFlight = max(s.stats.PeakInFlight, len(s.inflight))
	s.stats.PeakPerSource = max(s.stats.PeakPerSource, s.src[source])
	s.stats.PeakPerTarget = max(s.stats.PeakPerTarget, s.dst[target])
	s.clock.AfterFunc(s.cfg.CopyTimeout, func() {
		if cur := s.inflight[c.Chunk]; cur != nil && cur.ID == c.ID {
			s.stats.TimedOut++
			s.finish(c, TimedOut)
		}
	})
	s.send.Copy(*c)
}

type moveResult int

const (
	moveDropped moveResult = iota
	moveBlocked
	moveNoTokens
	moveStarted
)

// startMove dispatches a planned balance move if it still holds: the chunk
// is at exactly RF on confirmed, non-leaving holders, the source is one of
// them and the destination is an eligible node without a copy. A stale move
// is dropped; the next plan sees the cluster as it is.
func (s *Scheduler) startMove(it *item, size int64, now iface.Instant) moveResult {
	m := it.move
	a := s.assess(it.id, now)
	if _, busy := s.trimming[it.id]; busy || a.missing != 0 || len(a.sure) != s.cfg.Replicas || len(a.holders) != s.cfg.Replicas ||
		!slices.ContainsFunc(a.sure, func(h Holder) bool { return h.Node == m.from }) ||
		slices.ContainsFunc(a.holders, func(h Holder) bool { return h.Node == m.to }) ||
		!slices.ContainsFunc(s.view.Nodes(), func(n Member) bool { return n.ID == m.to && n.eligible() }) {
		s.stats.Cancelled++
		return moveDropped
	}
	if s.src[m.from] >= s.cfg.PerSource || s.dst[m.to] >= s.cfg.PerTarget {
		return moveBlocked
	}
	if !s.bucket.Take(size, now) {
		return moveNoTokens
	}
	s.launch(it.id, m.from, m.to, size, Balance, now)
	return moveStarted
}

// planBalance asks the planner for moves while none are queued, from the
// members' located bytes (plus bytes on their way to them). Chunks that are
// busy or not at exactly RF on members count but stay put.
func (s *Scheduler) planBalance() {
	for _, it := range s.queued {
		if it.class == Balance {
			return
		}
	}
	// Only with a settled membership: a node that is suspect, back but not yet
	// confirmed, or dead inside the repair delay may return with its data, and
	// targets computed without it would move copies only to move them back.
	var members []Member
	now := s.clock.Now()
	for _, m := range s.view.Nodes() {
		switch {
		case m.eligible():
			members = append(members, m)
		case m.State == detector.Dead && now >= m.DeadSince.Add(s.cfg.Delay):
			// Gone: its chunks were repaired elsewhere.
		default:
			return
		}
	}
	if len(members) == 0 {
		return
	}
	load := make(map[iface.NodeID]int64, len(members))
	for _, m := range members {
		load[m.ID] = 0
	}
	var chunks []rebalance.Chunk
	s.view.Chunks(func(id iface.ChunkID, size int64) {
		c := rebalance.Chunk{ID: id, Size: size}
		all := s.view.Holders(id)
		tr, trimming := s.trimming[id]
		for _, h := range all {
			// A copy being trimmed is as good as gone: counting it would make
			// its node look fuller than it is about to be.
			if trimming && h.Node == tr.Node {
				continue
			}
			if _, ok := load[h.Node]; ok {
				c.Holders = append(c.Holders, h.Node)
				load[h.Node] += size
			}
		}
		c.Pinned = s.inflight[id] != nil || trimming || len(c.Holders) != s.cfg.Replicas || len(all) != s.cfg.Replicas
		chunks = append(chunks, c)
	})
	for _, c := range s.inflight {
		if _, ok := load[c.Target]; ok {
			load[c.Target] += c.Size
		}
	}
	nodes := make([]rebalance.Node, 0, len(members))
	var distinct int64
	for _, c := range chunks {
		distinct += c.Size
	}
	for _, m := range members {
		nodes = append(nodes, rebalance.Node{ID: m.ID, Rack: m.Rack, Used: load[m.ID]})
	}
	s.load, s.target = load, rebalance.Targets(nodes, distinct, s.cfg.Replicas)
	for _, m := range rebalance.Plan(nodes, chunks, rebalance.Config{Replicas: s.cfg.Replicas, BandPercent: s.cfg.BandPercent}, 2*s.cfg.Background) {
		s.seq++
		it := &item{id: m.Chunk, class: Balance, live: s.cfg.Replicas, seq: s.seq, move: &move{from: m.From, to: m.To}}
		heap.Push(&s.q, it)
		s.queued[m.Chunk] = it
	}
}

// trim removes copies beyond Replicas. Only confirmed alive copies count,
// so at least Replicas of them remain; nothing is trimmed while a copy or
// another trim of the chunk is in flight.
func (s *Scheduler) trim(id iface.ChunkID, sure []Holder) {
	if s.inflight[id] != nil {
		return
	}
	if _, busy := s.trimming[id]; busy {
		return
	}
	s.sendTrim(id, s.victim(sure))
}

// victim applies Victim with each member's distance above its balance target
// in place of bytes used, once a plan has computed targets.
func (s *Scheduler) victim(sure []Holder) iface.NodeID {
	hs := slices.Clone(sure)
	if s.target != nil {
		for i := range hs {
			if t, ok := s.target[hs[i].Node]; ok {
				hs[i].Used = s.load[hs[i].Node] - t
			}
		}
	}
	return Victim(hs)
}

func (s *Scheduler) sendTrim(id iface.ChunkID, victim iface.NodeID) {
	if !s.active {
		return
	}
	s.nextID++
	t := Trim{ID: s.nextID, Chunk: id, Node: victim}
	s.trimming[id] = t
	s.clock.AfterFunc(s.cfg.CopyTimeout, func() {
		if cur, ok := s.trimming[id]; ok && cur.ID == t.ID {
			// Command or confirmation lost: look again now, not at the
			// next periodic scan.
			delete(s.trimming, id)
			s.retryTrim(t)
		}
	})
	s.send.Trim(t)
}

// retryTrim re-sends a trim whose confirmation never came to the same node
// (nodes confirm absent chunks too). The victim may have deleted already, so
// trimming a different holder could leave Replicas-1 copies until the
// victim's next full report. Only once the victim stops counting as a
// confirmed holder is a new victim chosen, from the holders that remain.
func (s *Scheduler) retryTrim(t Trim) {
	if _, ok := s.view.Want(t.Chunk); !ok || s.inflight[t.Chunk] != nil {
		return
	}
	a := s.assess(t.Chunk, s.clock.Now())
	if a.missing != 0 || len(a.sure) <= s.cfg.Replicas {
		return
	}
	if slices.ContainsFunc(a.sure, func(h Holder) bool { return h.Node == t.Node }) {
		s.sendTrim(t.Chunk, t.Node)
		return
	}
	s.trim(t.Chunk, a.sure)
}

// Victim picks the replica to drop: one on the rack holding the most
// copies (keeps the spread), then the highest Used (bytes, or distance above
// target), then the highest ID. Callers pass only non-leaving holders.
func Victim(holders []Holder) iface.NodeID {
	perRack := map[string]int{}
	for _, h := range holders {
		perRack[h.Rack]++
	}
	best := holders[0]
	for _, h := range holders[1:] {
		if cmp.Or(cmp.Compare(perRack[h.Rack], perRack[best.Rack]), cmp.Compare(h.Used, best.Used), cmp.Compare(h.Node, best.Node)) > 0 {
			best = h
		}
	}
	return best.Node
}

// checkExcess trims id if it has more confirmed copies than Replicas.
func (s *Scheduler) checkExcess(id iface.ChunkID) {
	if _, ok := s.view.Want(id); !ok {
		return
	}
	if a := s.assess(id, s.clock.Now()); a.missing == 0 && len(a.sure) > s.cfg.Replicas {
		s.trim(id, a.sure)
	}
}

// Removed tells the scheduler node deleted chunks, completing their trims.
func (s *Scheduler) Removed(node iface.NodeID, chunks []iface.ChunkID) {
	for _, id := range chunks {
		if t, ok := s.trimming[id]; ok && t.Node == node {
			delete(s.trimming, id)
			s.stats.Trimmed++
			if s.send.Trimmed != nil {
				s.send.Trimmed(t)
			}
		}
	}
}

// pickSource prefers alive over suspect replicas, then the least busy.
func (s *Scheduler) pickSource(holders []Holder) (iface.NodeID, bool) {
	var best *Holder
	for i := range holders {
		h := &holders[i]
		if (h.State != detector.Alive && h.State != detector.Suspect) || s.src[h.Node] >= s.cfg.PerSource {
			continue
		}
		if best == nil || cmp.Or(cmp.Compare(h.State, best.State), cmp.Compare(s.src[h.Node], s.src[best.Node]), cmp.Compare(h.Node, best.Node)) < 0 {
			best = h
		}
	}
	if best == nil {
		return "", false
	}
	return best.Node, true
}

// movable: src is among sure and dropping it keeps the racks the others hold.
func movable(sure []Holder, src iface.NodeID) bool {
	racks := map[string]int{}
	var rack string
	found := false
	for _, h := range sure {
		racks[h.Rack]++
		if h.Node == src {
			rack, found = h.Rack, true
		}
	}
	return found && (racks[rack] > 1 || len(racks) > len(sure)-1)
}

// finish releases a copy's slots and re-queues its chunk if it still needs
// more replicas.
func (s *Scheduler) finish(c *Copy, o Outcome) {
	delete(s.inflight, c.Chunk)
	if s.send.Done != nil {
		s.send.Done(*c, o)
	}
	s.src[c.Source]--
	s.dst[c.Target]--
	if s.src[c.Source] == 0 {
		delete(s.src, c.Source)
	}
	if s.dst[c.Target] == 0 {
		delete(s.dst, c.Target)
	}
	now := s.clock.Now()
	if _, ok := s.view.Want(c.Chunk); ok {
		switch a := s.assess(c.Chunk, now); {
		case a.missing == 0 && c.Class == Balance && o == Completed && len(a.sure) > s.cfg.Replicas && movable(a.sure, c.Source):
			// The second half of a move: the source's copy goes.
			if _, busy := s.trimming[c.Chunk]; !busy {
				s.sendTrim(c.Chunk, c.Source)
			}
		case a.missing == 0:
			// The copy may have landed after a holder came back.
			if len(a.sure) > s.cfg.Replicas {
				s.trim(c.Chunk, a.sure)
			}
		case a.lost:
		case a.readyAt > now:
			// Back above one copy, the rest waits out the delay again.
			s.wakeAtLeast(a.readyAt)
		default:
			s.enqueue(c.Chunk, a.live, a.class)
		}
	}
	// The last planned move is done: plan the next ones now, not at the next
	// periodic scan.
	if c.Class == Balance && !slices.ContainsFunc(s.q, func(it *item) bool { return it.class == Balance }) {
		s.wakeAtLeast(now)
	}
	s.dispatch()
}

// Reported tells the scheduler node now holds chunks (a block report). A
// report from a copy's target completes it.
func (s *Scheduler) Reported(node iface.NodeID, chunks []iface.ChunkID) {
	if !s.active {
		return
	}
	for _, id := range chunks {
		if c := s.inflight[id]; c != nil && c.Target == node {
			s.stats.Completed++
			s.stats.Bytes += uint64(c.Size)
			switch c.Class {
			case Drain:
				s.stats.Evacuated++
			case Balance:
				s.stats.Moved++
				s.stats.MovedBytes += uint64(c.Size)
			}
			s.finish(c, Completed)
			continue
		}
		// A client write that dedups against an existing chunk, or a late
		// report, can push a chunk over RF: trim now rather than at the
		// next scan.
		s.checkExcess(id)
	}
}

// Failed reports that a copy's target could not complete it.
func (s *Scheduler) Failed(id uint64, chunk iface.ChunkID) {
	if !s.active {
		return
	}
	if c := s.inflight[chunk]; c != nil && c.ID == id {
		s.stats.Failed++
		s.finish(c, Failed)
	}
}

// Stats returns counters and gauges.
func (s *Scheduler) Stats() Stats {
	st := s.stats
	st.Queued, st.InFlight = s.q.Len(), len(s.inflight)
	return st
}

// InFlight returns the copies in progress, ordered by ID.
func (s *Scheduler) InFlight() []Copy {
	out := make([]Copy, 0, len(s.inflight))
	for _, c := range s.inflight {
		out = append(out, *c)
	}
	slices.SortFunc(out, func(a, b Copy) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Busy reports whether a copy of chunk is in flight.
func (s *Scheduler) Busy(chunk iface.ChunkID) bool { return s.inflight[chunk] != nil }

// item is a queued chunk. Class first (repair, drain, balance); then fewest
// live replicas; then FIFO; the chunk ID breaks the (impossible) remaining
// tie so order never depends on maps.
type item struct {
	id    iface.ChunkID
	class Class
	live  int
	seq   uint64
	idx   int
	move  *move // set for a planned balance move
}

type move struct{ from, to iface.NodeID }

type queue []*item

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	return cmp.Or(cmp.Compare(q[i].class, q[j].class), cmp.Compare(q[i].live, q[j].live), cmp.Compare(q[i].seq, q[j].seq), bytes.Compare(q[i].id[:], q[j].id[:])) < 0
}
func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].idx, q[j].idx = i, j
}
func (q *queue) Push(x any) {
	it := x.(*item)
	it.idx = len(*q)
	*q = append(*q, it)
}
func (q *queue) Pop() any {
	old := *q
	it := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return it
}
