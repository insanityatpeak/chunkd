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
	"github.com/insanityatpeak/chunkd/internal/core/ec"
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

// Block is a wanted block: a replicated chunk, or one shard of an
// erasure-coded stripe (ADR-0023).
type Block struct {
	Size int64
	// Target is the copies wanted: RF for a chunk, 1 for a shard.
	Target int
	// Stripe is set for a shard.
	Stripe *Stripe
}

// Stripe places a shard within its stripe.
type Stripe struct {
	ID        iface.ChunkID // the stripe's logical ID
	Index     int
	ChunkSize int64           // size of the chunk the stripe encodes
	Shards    []iface.ChunkID // every shard, data first; Shards[Index] is this one
}

// ShardSource is a sibling shard a rebuild reads, and the node it reads from.
type ShardSource struct {
	Index int
	Shard iface.ChunkID
	Node  iface.NodeID
}

// Rebuild is how a Copy recomputes a shard instead of copying it: the
// target reads the first ec.DataShards sources (each holds a source slot),
// falling back to the rest, and decodes.
type Rebuild struct {
	Stripe  Stripe
	Sources []ShardSource
}

// View is the scheduler's read-only window onto the metadata server.
type View interface {
	// Want describes a block, and false if nothing references it.
	Want(id iface.ChunkID) (Block, bool)
	// Chunks calls fn for every wanted block, in a deterministic order.
	Chunks(fn func(id iface.ChunkID, b Block))
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

// Copy is one dispatched replication: Target pulls Chunk from Source, or
// for a rebuild recomputes it from the sources in Rebuild (Source empty).
type Copy struct {
	ID      uint64
	Chunk   iface.ChunkID
	Source  iface.NodeID
	Target  iface.NodeID
	Size    int64 // the block written
	Started iface.Instant
	Class   Class
	Rebuild *Rebuild
}

// sources are the nodes holding a source slot for c.
func (c *Copy) sources() []iface.NodeID {
	if c.Rebuild == nil {
		return []iface.NodeID{c.Source}
	}
	var out []iface.NodeID
	for _, s := range c.Rebuild.Sources[:ec.DataShards] {
		out = append(out, s.Node)
	}
	return out
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
	// Repairs counts dispatched repair-class copies: those caused by a chunk
	// below RF, not by drain or balance.
	Repairs uint64 `json:"repairs"`
	// Rebuilds counts dispatched shard rebuilds and RebuildRead the bytes
	// they read: ec.DataShards shards each (ADR-0023).
	Rebuilds    uint64 `json:"rebuilds"`
	RebuildRead uint64 `json:"rebuildRead"`
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
	target  int // the block's wanted copies
	missing int // target - live, or 0
	// sure are alive, non-leaving holders whose location is confirmed by a
	// full report since they last returned. Only these may justify a trim.
	sure []Holder
	lost bool // missing, and nothing to copy or rebuild from
	// rebuild: a shard with no copy left, recomputed from siblings, the
	// shards of its stripe with a usable holder.
	rebuild  bool
	siblings []sibling
	// class is Drain when the leaving copies still make up RF, else Repair.
	class Class
	// readyAt is when repair may start: now, or the end of the delay for
	// the latest-dead holder whose absence is still excused.
	readyAt iface.Instant
}

func (s *Scheduler) assess(id iface.ChunkID, b Block, now iface.Instant) assessment {
	a := assessment{holders: s.view.Holders(id), readyAt: now, target: b.Target}
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
	a.missing = max(b.Target-a.live, 0)
	if a.missing == 0 {
		return a
	}
	copies := a.live + a.leaving
	// spare: one more loss still leaves the data readable, so waiting for a
	// holder to come back is affordable. A single remaining copy is not
	// worth the gamble.
	spare := copies > 1
	if copies == 0 {
		if b.Stripe == nil {
			a.lost = true
			return a
		}
		a.siblings = s.siblings(b.Stripe)
		if len(a.siblings) < ec.DataShards {
			a.lost = true
			return a
		}
		// A stripe down to 4 shards is one loss from unreadable, like a
		// chunk down to its last copy: rebuild at once.
		a.rebuild, spare = true, len(a.siblings) > ec.DataShards
	}
	if copies >= b.Target {
		a.class = Drain
	}
	// Wait while recently dead holders would still cover the gap.
	if spare && a.live+len(excused) >= b.Target {
		slices.Sort(excused)
		// Ready once enough excuses expire that the gap is real.
		a.readyAt = excused[a.live+len(excused)-b.Target]
	}
	if spare && now < s.graceUntil {
		a.readyAt = max(a.readyAt, s.graceUntil)
	}
	// A just-committed chunk's missing report is usually in flight, not
	// lost: wait out the grace (bugs-found #9). So is a stripe's sixth
	// shard: commit needs 5.
	if t, ok := s.fresh[id]; ok {
		switch end := t.Add(s.cfg.UploadGrace); {
		case now >= end:
			delete(s.fresh, id)
		case spare:
			a.readyAt = max(a.readyAt, end)
		}
	}
	return a
}

// rank orders the queue, lowest first: live copies, or for a rebuild the
// copies a chunk with the same margin would have (4 shards left ranks with
// a last copy).
func (a assessment) rank() int {
	if a.rebuild {
		return len(a.siblings) - ec.DataShards + 1
	}
	return a.live
}

// sibling is another shard of a stripe and the holders it can be read from.
type sibling struct {
	index   int
	shard   iface.ChunkID
	holders []Holder // alive or suspect, alive first
}

// siblings lists the stripe's other shards that have a usable holder,
// leaving ones included: a draining node still serves reads.
func (s *Scheduler) siblings(st *Stripe) []sibling {
	var out []sibling
	for j, sh := range st.Shards {
		if j == st.Index {
			continue
		}
		var hs []Holder
		for _, h := range s.view.Holders(sh) {
			if h.State == detector.Alive || h.State == detector.Suspect {
				hs = append(hs, h)
			}
		}
		if len(hs) > 0 {
			slices.SortStableFunc(hs, func(a, b Holder) int { return cmp.Compare(a.State, b.State) })
			out = append(out, sibling{index: j, shard: sh, holders: hs})
		}
	}
	return out
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
	s.view.Chunks(func(id iface.ChunkID, b Block) {
		if s.inflight[id] != nil {
			return
		}
		a := s.assess(id, b, now)
		switch {
		case a.missing == 0:
			if it := s.queued[id]; it != nil {
				heap.Remove(&s.q, it.idx)
				delete(s.queued, id)
				s.stats.Cancelled++
			}
			if len(a.sure) > a.target {
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
			s.enqueue(id, a.rank(), a.class)
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
		b, ok := s.view.Want(id)
		if !ok || s.inflight[id] != nil {
			continue
		}
		switch a := s.assess(id, b, now); {
		case a.missing == 0, a.lost:
		case a.readyAt > now:
			s.wakeAtLeast(a.readyAt)
		default:
			s.enqueue(id, a.rank(), a.class)
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
		b, ok := s.view.Want(it.id)
		if !ok {
			s.stats.Cancelled++
			continue
		}
		size := b.Size
		if it.move != nil {
			switch s.startMove(it, b, now) {
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
		a := s.assess(it.id, b, now)
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
		c := &Copy{Chunk: it.id, Size: size, Class: a.class}
		charge := size
		if a.rebuild {
			sources, ok := s.pickSources(a.siblings)
			if !ok {
				blocked = append(blocked, it)
				repairBlocked = true
				continue
			}
			c.Rebuild = &Rebuild{Stripe: *b.Stripe, Sources: sources}
			// The cost is the shards read, not the one written (ADR-0023).
			charge = ec.DataShards * size
		} else if c.Source, ok = s.pickSource(a.holders); !ok {
			blocked = append(blocked, it)
			repairBlocked = repairBlocked || a.class == Repair
			continue
		}
		exclude := make([]iface.NodeID, 0, len(a.holders)+len(s.dst))
		for _, h := range a.holders {
			exclude = append(exclude, h.Node)
		}
		if b.Stripe != nil {
			// Every shard of a stripe on its own node: two on one would turn
			// one failure into two.
			for _, sh := range b.Stripe.Shards {
				for _, h := range s.view.Holders(sh) {
					exclude = append(exclude, h.Node)
				}
			}
		}
		for n, c := range s.dst {
			if c >= s.cfg.PerTarget {
				exclude = append(exclude, n)
			}
		}
		if c.Target, ok = s.view.Target(size, exclude); !ok {
			blocked = append(blocked, it)
			repairBlocked = repairBlocked || a.class == Repair
			continue
		}
		if !s.bucket.Take(charge, now) {
			blocked = append(blocked, it)
			s.wakeAtLeast(s.bucket.ReadyAt(charge, now))
			return
		}
		s.launch(c, now)
		if a.class > Repair {
			background++
		}
	}
}

// pickSources chooses a holder for each sibling, preferring alive ones and
// nodes with a free source slot: the first ec.DataShards, which must all
// have a free slot, are read first; the rest are fallbacks and hold none.
func (s *Scheduler) pickSources(sibs []sibling) ([]ShardSource, bool) {
	type choice struct {
		src   ShardSource
		state detector.State
		free  bool
	}
	var cs []choice
	for _, sb := range sibs {
		best := choice{src: ShardSource{Index: sb.index, Shard: sb.shard, Node: sb.holders[0].Node}, state: sb.holders[0].State}
		for _, h := range sb.holders {
			if s.src[h.Node] < s.cfg.PerSource {
				best = choice{src: ShardSource{Index: sb.index, Shard: sb.shard, Node: h.Node}, state: h.State, free: true}
				break
			}
		}
		cs = append(cs, best)
	}
	slices.SortStableFunc(cs, func(a, b choice) int {
		if a.free != b.free {
			if a.free {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.state, b.state)
	})
	if len(cs) < ec.DataShards || !cs[ec.DataShards-1].free {
		return nil, false
	}
	out := make([]ShardSource, len(cs))
	for i, c := range cs {
		out[i] = c.src
	}
	return out, true
}

func (s *Scheduler) launch(c *Copy, now iface.Instant) {
	s.nextID++
	c.ID, c.Started = s.nextID, now
	id, class := c.Chunk, c.Class
	s.inflight[id] = c
	for _, n := range c.sources() {
		s.src[n]++
		s.stats.PeakPerSource = max(s.stats.PeakPerSource, s.src[n])
	}
	s.dst[c.Target]++
	s.stats.Dispatched++
	if class == Repair {
		s.stats.Repairs++
	}
	if c.Rebuild != nil {
		s.stats.Rebuilds++
		s.stats.RebuildRead += uint64(ec.DataShards * c.Size)
	}
	s.stats.PeakInFlight = max(s.stats.PeakInFlight, len(s.inflight))
	s.stats.PeakPerTarget = max(s.stats.PeakPerTarget, s.dst[c.Target])
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
func (s *Scheduler) startMove(it *item, b Block, now iface.Instant) moveResult {
	m := it.move
	a := s.assess(it.id, b, now)
	if _, busy := s.trimming[it.id]; busy || b.Stripe != nil || a.missing != 0 || len(a.sure) != b.Target || len(a.holders) != b.Target ||
		!slices.ContainsFunc(a.sure, func(h Holder) bool { return h.Node == m.from }) ||
		slices.ContainsFunc(a.holders, func(h Holder) bool { return h.Node == m.to }) ||
		!slices.ContainsFunc(s.view.Nodes(), func(n Member) bool { return n.ID == m.to && n.eligible() }) {
		s.stats.Cancelled++
		return moveDropped
	}
	if s.src[m.from] >= s.cfg.PerSource || s.dst[m.to] >= s.cfg.PerTarget {
		return moveBlocked
	}
	if !s.bucket.Take(b.Size, now) {
		return moveNoTokens
	}
	s.launch(&Copy{Chunk: it.id, Source: m.from, Target: m.to, Size: b.Size, Class: Balance}, now)
	return moveStarted
}

// planBalance asks the planner for moves while none are queued.
func (s *Scheduler) planBalance() {
	for _, it := range s.queued {
		if it.class == Balance {
			return
		}
	}
	nodes, chunks, distinct, ok := s.balanceInput()
	if !ok {
		return
	}
	s.load, s.target = map[iface.NodeID]int64{}, rebalance.Targets(nodes, distinct, s.cfg.Replicas)
	for _, n := range nodes {
		s.load[n.ID] = n.Used
	}
	for _, m := range rebalance.Plan(nodes, chunks, rebalance.Config{Replicas: s.cfg.Replicas, BandPercent: s.cfg.BandPercent}, 2*s.cfg.Background) {
		s.seq++
		it := &item{id: m.Chunk, class: Balance, live: s.cfg.Replicas, seq: s.seq, move: &move{from: m.From, to: m.To}}
		heap.Push(&s.q, it)
		s.queued[m.Chunk] = it
	}
}

// balanceInput is the planner's input: each member's located bytes (plus
// bytes on their way to it), every chunk, and the distinct bytes stored.
// Chunks that are busy or not at exactly RF on members count but stay put.
// ok is false while the membership is unsettled: a node that is suspect,
// back but not yet confirmed, or dead inside the repair delay may return
// with its data, and targets computed without it would move copies only to
// move them back.
func (s *Scheduler) balanceInput() (nodes []rebalance.Node, chunks []rebalance.Chunk, distinct int64, ok bool) {
	var members []Member
	now := s.clock.Now()
	for _, m := range s.view.Nodes() {
		switch {
		case m.eligible():
			members = append(members, m)
		case m.State == detector.Dead && now >= m.DeadSince.Add(s.cfg.Delay):
			// Gone: its chunks were repaired elsewhere.
		default:
			return nil, nil, 0, false
		}
	}
	if len(members) == 0 {
		return nil, nil, 0, false
	}
	load := make(map[iface.NodeID]int64, len(members))
	for _, m := range members {
		load[m.ID] = 0
	}
	s.view.Chunks(func(id iface.ChunkID, b Block) {
		// SIMPLIFIED: shards are neither moved nor counted: one copy each,
		// placed 6 to a stripe on distinct nodes, they would need a planner
		// that keeps the stripe spread (ADR-0023). Ceph's balancer moves
		// erasure-coded placement groups like any other.
		if b.Stripe != nil {
			return
		}
		size := b.Size
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
		c.Pinned = s.inflight[id] != nil || trimming || len(c.Holders) != b.Target || len(all) != b.Target
		chunks = append(chunks, c)
	})
	for _, c := range s.inflight {
		if _, ok := load[c.Target]; ok {
			load[c.Target] += c.Size
		}
	}
	for _, c := range chunks {
		distinct += c.Size
	}
	for _, m := range members {
		nodes = append(nodes, rebalance.Node{ID: m.ID, Rack: m.Rack, Used: load[m.ID]})
	}
	return nodes, chunks, distinct, true
}

// NodeBalance is one member's located bytes against its balance target; the
// planner leaves it alone while Used is within Band of Target.
type NodeBalance struct {
	Node               iface.NodeID
	Used, Target, Band int64
}

// Balance is the planner's view of the members now, nil while the membership
// is unsettled. O(chunks).
func (s *Scheduler) Balance() []NodeBalance {
	nodes, chunks, _, ok := s.balanceInput()
	if !ok {
		return nil
	}
	cfg := rebalance.Config{Replicas: s.cfg.Replicas, BandPercent: s.cfg.BandPercent}
	target, band := rebalance.Frame(nodes, chunks, cfg)
	out := make([]NodeBalance, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, NodeBalance{Node: n.ID, Used: n.Used, Target: target[n.ID], Band: band(n.ID)})
	}
	return out
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
	b, ok := s.view.Want(t.Chunk)
	if !ok || s.inflight[t.Chunk] != nil {
		return
	}
	a := s.assess(t.Chunk, b, s.clock.Now())
	if a.missing != 0 || len(a.sure) <= a.target {
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

// checkExcess trims id if it has more confirmed copies than its target.
func (s *Scheduler) checkExcess(id iface.ChunkID) {
	b, ok := s.view.Want(id)
	if !ok {
		return
	}
	if a := s.assess(id, b, s.clock.Now()); a.missing == 0 && len(a.sure) > a.target {
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
	for _, n := range c.sources() {
		if s.src[n]--; s.src[n] == 0 {
			delete(s.src, n)
		}
	}
	if s.dst[c.Target]--; s.dst[c.Target] == 0 {
		delete(s.dst, c.Target)
	}
	now := s.clock.Now()
	if b, ok := s.view.Want(c.Chunk); ok {
		switch a := s.assess(c.Chunk, b, now); {
		case a.missing == 0 && c.Class == Balance && o == Completed && len(a.sure) > a.target && movable(a.sure, c.Source):
			// The second half of a move: the source's copy goes.
			if _, busy := s.trimming[c.Chunk]; !busy {
				s.sendTrim(c.Chunk, c.Source)
			}
		case a.missing == 0:
			// The copy may have landed after a holder came back.
			if len(a.sure) > a.target {
				s.trim(c.Chunk, a.sure)
			}
		case a.lost:
		case a.readyAt > now:
			// Back above one copy, the rest waits out the delay again.
			s.wakeAtLeast(a.readyAt)
		default:
			s.enqueue(c.Chunk, a.rank(), a.class)
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
