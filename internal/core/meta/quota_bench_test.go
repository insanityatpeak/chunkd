package meta

import (
	"fmt"
	"testing"
)

// BenchmarkBegin is docs/benchmarks/quota.md: the state machine's cost of a
// Begin with and without a namespace quota, by number of files in the
// namespace. Ten other namespaces hold n files between them: the quota scan
// walks every file, not only the namespace's.
func BenchmarkBegin(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000, 100_000} {
		s := New()
		for i := range n {
			for _, ns := range []string{"a", "x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"} {
				if i >= n/10 && ns != "a" {
					continue
				}
				r, err := s.Apply(begin(fmt.Sprintf("/%s/f%d", ns, i), 0, 8))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := s.Apply(commit(r.UploadID, 2, byte(i))); err != nil {
					b.Fatal(err)
				}
			}
		}
		for _, limit := range []int64{0, 1 << 40} {
			name := "no-quota"
			if limit > 0 {
				name = "quota"
			}
			b.Run(fmt.Sprintf("%s/files=%d", name, n), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					op := withQuota(begin("/a/new", 0, 8), limit)
					r, err := s.Apply(op)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := s.Apply(abort(r.UploadID)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
