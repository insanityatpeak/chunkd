package meta

import (
	"fmt"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/detector"
	"github.com/insanityatpeak/chunkd/internal/core/repair"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// EventRing is how many recent events the metadata server keeps.
const EventRing = 500

// Event is one entry of the dashboard timeline. Events are not durable: a
// restarted metadata server starts a new sequence at 1.
type Event struct {
	Seq  uint64
	At   iface.Instant
	Kind string // "node", "copy", "trim" or "corrupt"
	Node iface.NodeID
	Text string
}

// events is a fixed ring of the most recent EventRing events.
type events struct {
	buf  [EventRing]Event
	next uint64 // Seq of the next event; buf[(seq-1)%EventRing] holds seq
}

func (e *events) add(ev Event) {
	e.next++
	ev.Seq = e.next
	e.buf[(ev.Seq-1)%EventRing] = ev
}

// since returns the retained events with Seq > after, oldest first.
func (e *events) since(after uint64) []Event {
	first := max(after+1, e.next+1-min(e.next, EventRing))
	var out []Event
	for seq := first; seq <= e.next; seq++ {
		out = append(out, e.buf[(seq-1)%EventRing])
	}
	return out
}

// EventSeq is the latest event's seq.
func (s *Server) EventSeq() uint64 { return s.events.next }

// Events returns the retained events after seq, and the latest seq. A
// latest below after means the server restarted and the sequence reset.
func (s *Server) Events(after uint64) ([]Event, uint64) {
	return s.events.since(after), s.events.next
}

func (s *Server) event(kind string, node iface.NodeID, format string, args ...any) {
	s.events.add(Event{At: s.d.Clock.Now(), Kind: kind, Node: node, Text: fmt.Sprintf(format, args...)})
}

func (s *Server) nodeEvent(tr detector.Transition) {
	switch {
	case tr.From == 0:
		s.event("node", tr.Node, "joined as %s", tr.To)
	case tr.Restarted:
		s.event("node", tr.Node, "%s → %s (restarted)", tr.From, tr.To)
	default:
		s.event("node", tr.Node, "%s → %s", tr.From, tr.To)
	}
}

func (s *Server) copyStarted(c repair.Copy) {
	tag := ""
	if c.Class != repair.Repair {
		tag = " (" + c.Class.String() + ")"
	}
	s.event("copy", c.Target, "copy %d started%s: chunk %s from %s, %d bytes", c.ID, tag, c.Chunk.String()[:12], c.Source, c.Size)
}

func (s *Server) copyDone(c repair.Copy, o repair.Outcome) {
	s.event("copy", c.Target, "copy %d %s after %v", c.ID, o, s.d.Clock.Now().Sub(c.Started).Round(time.Millisecond))
}

func (s *Server) trimSent(t repair.Trim) {
	s.event("trim", t.Node, "trim %d sent: chunk %s", t.ID, t.Chunk.String()[:12])
}

func (s *Server) trimmed(id iface.ChunkID, n iface.NodeID, trimID uint64) {
	s.event("trim", n, "trim %d done: chunk %s removed", trimID, id.String()[:12])
}
