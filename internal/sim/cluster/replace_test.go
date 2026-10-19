package cluster

import (
	"fmt"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// A follower whose log is corrupt cannot restart; replacing it brings the
// group back to three voters: the new member catches up over an empty store,
// every peer agrees, and losing another peer afterwards still leaves a
// quorum that serves reads and writes (ADR-0032).
func TestReplaceMetaWithCorruptLog(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		c := newMetaGroup(t, seed)
		for i := range 4 {
			if _, _, err := c.UploadRandom(fmt.Sprintf("/a%d", i), 1<<20); err != nil {
				t.Fatalf("seed %d: upload: %v", seed, err)
			}
		}
		var victim, other iface.NodeID
		for _, id := range c.MetaIDs() {
			switch {
			case id == c.MetaLeader():
			case victim == "":
				victim = id
			default:
				other = id
			}
		}
		c.KillMeta(victim)
		c.CorruptMetaLog(victim)
		if err := c.ReviveMeta(victim); err == nil {
			t.Fatalf("seed %d: %s started over a corrupt log", seed, victim)
		}
		c.Net().Crash(victim)
		if _, _, err := c.UploadRandom("/while-down", 1<<20); err != nil {
			t.Fatalf("seed %d: upload with %s down: %v", seed, victim, err)
		}

		if err := c.ReplaceMeta(victim); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if id := c.metaPeer(victim).raftID; id <= 3 {
			t.Fatalf("seed %d: %s joined as raft ID %d, want a later generation", seed, victim, id)
		}
		mustAgree(t, c)

		c.KillMeta(other)
		for i := range 3 {
			if _, _, err := c.UploadRandom(fmt.Sprintf("/b%d", i), 1<<20); err != nil {
				t.Fatalf("seed %d: upload with %s down after the replace: %v", seed, other, err)
			}
		}
		if err := c.ReviveMeta(other); err != nil {
			t.Fatal(err)
		}
		mustAgree(t, c)
	}
}
