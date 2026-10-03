package meta

import (
	"bytes"
	"maps"
	"slices"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Drift is a disagreement between the incremental counts and a recount
// from first principles. Any drift is a bug: nothing corrects it silently.
type Drift struct {
	// Refcounts lists chunks whose stored refcount differs from the number
	// of retained versions referencing them (a missing record counts as 0).
	Refcounts []RefDrift
	// Claims lists chunks whose claim count differs from the pending
	// uploads' claims.
	Claims []iface.ChunkID
	// Stripes lists stripes whose hold count differs from their record plus
	// the pending uploads claiming them: their shards' GC mark is wrong.
	Stripes []iface.ChunkID
	// Namespaces lists namespaces whose quota counter differs from a recount.
	Namespaces []string
}

// Count is the number of drifted entries.
func (d Drift) Count() int {
	return len(d.Refcounts) + len(d.Claims) + len(d.Stripes) + len(d.Namespaces)
}

// RefDrift is one chunk's stored and recounted refcount.
type RefDrift struct {
	Chunk          iface.ChunkID
	Stored, Actual uint64
}

// Empty reports whether the counts agree.
func (d Drift) Empty() bool { return d.Count() == 0 }

// Reconcile recounts chunk references from the retained versions and claims
// from the pending uploads, and reports where the stored counts differ.
// O(versions × chunks); run once per epoch.
func (s *State) Reconcile() Drift {
	refs := map[iface.ChunkID]uint64{}
	for _, f := range s.files {
		for _, v := range f.Versions {
			for _, id := range v.Chunks {
				refs[id]++
			}
		}
	}
	var d Drift
	ids := map[iface.ChunkID]struct{}{}
	for id := range refs {
		ids[id] = struct{}{}
	}
	for id := range s.chunks {
		ids[id] = struct{}{}
	}
	for _, id := range sortedIDs(ids) {
		var stored uint64
		if ci := s.chunks[id]; ci != nil {
			stored = ci.Refcount
		}
		if stored != refs[id] {
			d.Refcounts = append(d.Refcounts, RefDrift{Chunk: id, Stored: stored, Actual: refs[id]})
		}
	}
	claims := map[iface.ChunkID]int{}
	for _, u := range s.uploads {
		for _, id := range u.Claimed {
			claims[id]++
		}
	}
	cids := map[iface.ChunkID]struct{}{}
	for id := range claims {
		cids[id] = struct{}{}
	}
	for id := range s.claimed {
		cids[id] = struct{}{}
	}
	for _, id := range sortedIDs(cids) {
		if claims[id] != s.claimed[id] {
			d.Claims = append(d.Claims, id)
		}
	}
	holds := map[iface.ChunkID]int{}
	for id, ci := range s.chunks {
		if ci.Shards != nil {
			holds[id]++
		}
	}
	for _, u := range s.uploads {
		for i, id := range u.Claimed {
			if u.Shards[i] != nil {
				holds[id]++
			}
		}
	}
	sids := map[iface.ChunkID]struct{}{}
	for id := range holds {
		sids[id] = struct{}{}
	}
	for id := range s.stripes {
		sids[id] = struct{}{}
	}
	for _, id := range sortedIDs(sids) {
		if st := s.stripes[id]; st == nil || st.holds != holds[id] {
			d.Stripes = append(d.Stripes, id)
		}
	}
	want := s.recountNamespaces()
	for ns := range s.nsBytes {
		if _, ok := want[ns]; !ok {
			want[ns] = 0
		}
	}
	for _, ns := range slices.Sorted(maps.Keys(want)) {
		if s.nsBytes[ns] != want[ns] {
			d.Namespaces = append(d.Namespaces, ns)
		}
	}
	return d
}

func sortedIDs(m map[iface.ChunkID]struct{}) []iface.ChunkID {
	return slices.SortedFunc(maps.Keys(m), func(a, b iface.ChunkID) int { return bytes.Compare(a[:], b[:]) })
}
