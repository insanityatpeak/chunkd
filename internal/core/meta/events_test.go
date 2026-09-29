package meta

import (
	"testing"
)

func TestEventRing(t *testing.T) {
	seqs := func(evs []Event) (first, last uint64, n int) {
		if len(evs) == 0 {
			return 0, 0, 0
		}
		return evs[0].Seq, evs[len(evs)-1].Seq, len(evs)
	}
	for _, tc := range []struct {
		name              string
		added, after      uint64
		first, last, want uint64
	}{
		{"empty", 0, 0, 0, 0, 0},
		{"all", 10, 0, 1, 10, 10},
		{"after some", 10, 7, 8, 10, 3},
		{"caught up", 10, 10, 0, 0, 0},
		{"full ring", EventRing, 0, 1, EventRing, EventRing},
		{"wrapped keeps the newest", EventRing + 120, 0, 121, EventRing + 120, EventRing},
		{"wrapped, after inside", EventRing + 120, 400, 401, EventRing + 120, EventRing - 280},
		{"after beyond latest (server restarted)", 10, 50, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var e events
			for i := range tc.added {
				e.add(Event{Kind: "node", Text: string(rune('a' + i%26))})
			}
			first, last, n := seqs(e.since(tc.after))
			if first != tc.first || last != tc.last || uint64(n) != tc.want {
				t.Fatalf("since(%d) = seqs %d..%d (%d), want %d..%d (%d)", tc.after, first, last, n, tc.first, tc.last, tc.want)
			}
			if e.next != tc.added {
				t.Fatalf("latest %d, want %d", e.next, tc.added)
			}
		})
	}
}
