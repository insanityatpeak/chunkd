package detector

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// step is one event in a scripted run: a beat from n at ms, or a tick at ms.
type step struct {
	ms   int64
	beat bool
	inc  uint64 // new incarnation; 0 keeps the current one
	seq  uint64 // 0 means "next"
}

func beat(ms int64) step                { return step{ms: ms, beat: true} }
func beatSeq(ms int64, seq uint64) step { return step{ms: ms, beat: true, seq: seq} }
func restart(ms int64, inc uint64) step { return step{ms: ms, beat: true, inc: inc, seq: 1} }
func tick(ms int64) step                { return step{ms: ms} }
func at(ms int64) iface.Instant         { return iface.Instant(ms * int64(time.Millisecond)) }
func beatsEvery(from, to, every int64) []step {
	var out []step
	for ms := from; ms <= to; ms += every {
		out = append(out, beat(ms), tick(ms))
	}
	return out
}

// run drives a detector for node "n" and returns "from>to@ms" for each
// transition after the first join.
func run(steps []step) (*Detector, []string) {
	d := New(DefaultConfig())
	var seq uint64
	inc := uint64(1)
	var got []string
	record := func(tr Transition) {
		if tr.From == 0 {
			return
		}
		s := fmt.Sprintf("%v>%v@%d", tr.From, tr.To, int64(tr.At)/int64(time.Millisecond))
		if tr.Restarted {
			s += "!"
		}
		got = append(got, s)
	}
	for _, s := range steps {
		if !s.beat {
			for _, tr := range d.Tick(at(s.ms)) {
				record(tr)
			}
			continue
		}
		if s.inc != 0 {
			inc = s.inc
		}
		if s.seq != 0 {
			seq = s.seq
		} else {
			seq++
		}
		if tr, ok := d.Observe("n", Beat{Incarnation: inc, Seq: seq}, at(s.ms)); ok {
			record(tr)
		}
	}
	return d, got
}

func TestDetector(t *testing.T) {
	ticks := func(from, to int64) []step {
		var out []step
		for ms := from; ms <= to; ms += 500 {
			out = append(out, tick(ms))
		}
		return out
	}
	cat := func(parts ...[]step) []step { return slices.Concat(parts...) }

	tests := []struct {
		name  string
		steps []step
		want  []string
		final State
	}{
		{
			name:  "steady beats stay alive",
			steps: beatsEvery(0, 30_000, 1000),
			final: Alive,
		},
		{
			name:  "jittered beats within 3 s never suspect",
			steps: cat([]step{beat(0)}, ticks(0, 2900), []step{beat(2900)}, ticks(2900, 5800), []step{beat(5800), tick(6000)}),
			final: Alive,
		},
		{
			name:  "silence: suspect after 3 s, dead after 10 s",
			steps: cat([]step{beat(0)}, ticks(0, 12_000)),
			want:  []string{"alive>suspect@3500", "suspect>dead@10500"},
			final: Dead,
		},
		{
			name: "blip recovers only after 3 on-time beats",
			steps: cat([]step{beat(0)}, ticks(0, 4000),
				[]step{beat(4200), tick(4500), beat(5200), tick(5500), beat(6200), tick(6500)}),
			want:  []string{"alive>suspect@3500", "suspect>alive@6200"},
			final: Alive,
		},
		{
			name: "flapping: a late beat resets the streak",
			steps: cat([]step{beat(0)}, ticks(0, 3500),
				// Two on-time beats, a 2.5 s gap (> 1.5 s), then three more.
				[]step{beat(3600), beat(4600)}, ticks(4600, 7000),
				[]step{beat(7100), beat(8100), beat(9100), tick(9500)}),
			want:  []string{"alive>suspect@3500", "suspect>alive@9100"},
			final: Alive,
		},
		{
			name: "flapping below the dead timeout never reaches dead",
			steps: func() []step {
				var out []step
				// Beat twice, pause 4 s, forever: suspect but never recovered, never dead.
				for base := int64(0); base < 60_000; base += 5000 {
					out = append(out, beat(base), beat(base+1000))
					out = append(out, ticks(base+1000, base+4500)...)
				}
				return out
			}(),
			want:  []string{"alive>suspect@4500"},
			final: Suspect,
		},
		{
			name: "dead node that returns goes through suspect",
			steps: cat([]step{beat(0)}, ticks(0, 11_000),
				[]step{beat(20_000), beat(21_000), beat(22_000)}),
			want:  []string{"alive>suspect@3500", "suspect>dead@10500", "dead>suspect@20000", "suspect>alive@22000"},
			final: Alive,
		},
		{
			name: "duplicated beats do not count toward recovery",
			steps: cat([]step{beat(0)}, ticks(0, 4000),
				[]step{beatSeq(4200, 10), beatSeq(4210, 10), beatSeq(4220, 10), beatSeq(4230, 9)}),
			want:  []string{"alive>suspect@3500"},
			final: Suspect,
		},
		{
			name:  "restart unnoticed by timeouts still loses alive",
			steps: []step{beat(0), beat(1000), restart(1500, 2), beat(2500), beat(3500)},
			want:  []string{"alive>suspect@1500!", "suspect>alive@3500"},
			final: Alive,
		},
		{
			name:  "restarted node whose seq restarts at 1 is not ignored",
			steps: cat([]step{beat(0)}, []step{beatSeq(1000, 50)}, ticks(1000, 11_600), []step{restart(12_000, 7)}),
			want:  []string{"alive>suspect@4500", "suspect>dead@11500", "dead>suspect@12000!"},
			final: Suspect,
		},
		{
			name: "clock jump: a 15 s stall of the owner kills nobody",
			// Beats every second, the detector's loop freezes from 2 s to
			// 17 s, and beats resume right after.
			steps: cat(beatsEvery(0, 2000, 1000), []step{tick(17_000), beat(17_000), tick(17_500), beat(18_000), tick(18_000)}),
			final: Alive,
		},
		{
			name:  "clock jump does not resurrect a node dead before the stall",
			steps: cat([]step{beat(0)}, ticks(0, 11_000), []step{tick(40_000), tick(40_500)}),
			want:  []string{"alive>suspect@3500", "suspect>dead@10500"},
			final: Dead,
		},
		{
			name:  "silence continuing after a stall still ends in dead",
			steps: cat([]step{beat(0)}, ticks(0, 2000), []step{tick(20_000)}, ticks(20_500, 32_000)),
			// 500 ms of the stall counts (one normal tick); the rest does not.
			want:  []string{"alive>suspect@21000", "suspect>dead@28000"},
			final: Dead,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, got := run(tc.steps)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("transitions\n got  %s\n want %s", strings.Join(got, " "), strings.Join(tc.want, " "))
			}
			if s := d.State("n"); s != tc.final {
				t.Fatalf("final state %v, want %v", s, tc.final)
			}
		})
	}
}

func TestStallCounted(t *testing.T) {
	d := New(DefaultConfig())
	d.Observe("n", Beat{1, 1}, 0)
	d.Tick(0)
	d.Tick(at(500))
	d.Tick(at(5000))
	if d.Stalls() != 1 {
		t.Fatalf("stalls = %d, want 1", d.Stalls())
	}
}

func TestTickOrderIsSorted(t *testing.T) {
	d := New(DefaultConfig())
	for _, id := range []iface.NodeID{"c", "a", "b"} {
		d.Observe(id, Beat{1, 1}, 0)
	}
	d.Tick(0)
	var got []iface.NodeID
	for ms := int64(500); ms <= 4000; ms += 500 {
		for _, tr := range d.Tick(at(ms)) {
			got = append(got, tr.Node)
		}
	}
	if !slices.Equal(got, []iface.NodeID{"a", "b", "c"}) {
		t.Fatalf("order %v", got)
	}
}
