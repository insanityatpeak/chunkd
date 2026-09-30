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
}

// DefaultConfig: RF 3, 20 s delay, 8 copies in flight (2 per node as source
// and as target), 40 MiB/s.
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
}

// Copy is one dispatched replication: Target pulls Chunk from Source.
type Copy struct {
	ID      uint64
	Chunk   iface.ChunkID
	Source  iface.NodeID
	Target  iface.NodeID
	Size    int64
	Started iface.Instant
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
	live    int // alive or suspect: still counted toward RF
	missing int // Replicas - live, or 0
	// sure are alive holders whose location is confirmed by a full report
	// since they last returned. Only these may justify a trim.
	sure []Holder
	lost bool // missing, and nothing to copy from
	// readyAt is when repair may start: now, or the end of the delay for
	// the latest-dead holder whose absence is still excused.
	readyAt iface.Instant
}

func (s *Scheduler) assess(id iface.ChunkID, now iface.Instant) assessment {
	a := assessment{holders: s.view.Holders(id), readyAt: now}
	var excused []iface.Instant
	for _, h := range a.holders {
		switch {
		case h.State == detector.Alive || h.State == detector.Suspect:
			a.live++
			if h.State == detector.Alive && h.Confirmed {
				a.sure = append(a.sure, h)
			}
		case h.State == detector.Dead && now < h.DeadSince.Add(s.cfg.Delay):
			excused = append(excused, h.DeadSince.Add(s.cfg.Delay))
		}
	}
	a.missing = max(s.cfg.Replicas-a.live, 0)
	if a.missing == 0 {
		return a
	}
	if a.live == 0 {
		a.lost = true
		return a
	}
	// A single remaining copy is not worth the gamble: repair at once.
	// Otherwise wait while recently dead holders would still cover the gap.
	if a.live > 1 && a.live+len(excused) >= s.cfg.Replicas {
		slices.Sort(excused)
		// Ready once enough excuses expire that the gap is real.
		a.readyAt = excused[a.live+len(excused)-s.cfg.Replicas]
	}
	if a.live > 1 && now < s.graceUntil {
		a.readyAt = max(a.readyAt, s.graceUntil)
	}
	// A just-committed chunk's missing report is usually in flight, not
	// lost: wait out the grace (bugs-found #9).
	if t, ok := s.fresh[id]; ok {
		switch end := t.Add(s.cfg.UploadGrace); {
		case now >= end:
			delete(s.fresh, id)
		case a.live > 1:
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
			s.enqueue(id, a.live)
		}
	})
	s.stats.Waiting, s.stats.Lost = waiting, lost
	if next >= 0 {
		s.wakeAtLeast(next)
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
			s.enqueue(id, a.live)
		}
	}
	s.dispatch()
}

func (s *Scheduler) enqueue(id iface.ChunkID, live int) {
	if it := s.queued[id]; it != nil {
		if it.live != live {
			it.live = live
			heap.Fix(&s.q, it.idx)
		}
		return
	}
	s.seq++
	it := &item{id: id, live: live, seq: s.seq}
	heap.Push(&s.q, it)
	s.queued[id] = it
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
	for s.q.Len() > 0 && len(s.inflight) < s.cfg.MaxInFlight {
		it := heap.Pop(&s.q).(*item)
		delete(s.queued, it.id)
		size, ok := s.view.Want(it.id)
		if !ok {
			s.stats.Cancelled++
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
		source, ok := s.pickSource(a.holders)
		if !ok {
			blocked = append(blocked, it)
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
			continue
		}
		if !s.bucket.Take(size, now) {
			blocked = append(blocked, it)
			s.wakeAtLeast(s.bucket.ReadyAt(size, now))
			return
		}
		s.nextID++
		c := &Copy{ID: s.nextID, Chunk: it.id, Source: source, Target: target, Size: size, Started: now}
		s.inflight[it.id] = c
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
	s.sendTrim(id, Victim(sure))
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
// copies (keeps the spread), then the most used node, then the highest ID.
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
			s.enqueue(c.Chunk, a.live)
		}
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

// item is a queued chunk. Fewest live replicas first; then FIFO; the chunk
// ID breaks the (impossible) remaining tie so order never depends on maps.
type item struct {
	id   iface.ChunkID
	live int
	seq  uint64
	idx  int
}

type queue []*item

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	return cmp.Or(cmp.Compare(q[i].live, q[j].live), cmp.Compare(q[i].seq, q[j].seq), bytes.Compare(q[i].id[:], q[j].id[:])) < 0
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
