package ec

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func pattern(n int) []byte {
	b := make([]byte, n)
	x := uint32(2463534242)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

var sizes = []int{1, 3, 4, 5, 1<<20 + 3, 4 << 20}

func TestJoinSurvivesAnyTwoLosses(t *testing.T) {
	c := New()
	for _, size := range sizes {
		data := pattern(size)
		for a := -1; a < TotalShards; a++ {
			for b := a + 1; b < TotalShards; b++ {
				shards, err := c.Split(data)
				if err != nil {
					t.Fatal(err)
				}
				if a >= 0 {
					shards[a] = nil
				}
				shards[b] = nil // a == -1: b is the only loss
				got, err := c.Join(shards, size)
				if err != nil {
					t.Fatalf("size %d lose %d,%d: %v", size, a, b, err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("size %d lose %d,%d: wrong bytes", size, a, b)
				}
			}
		}
	}
}

func TestJoinFailsLoudlyOnThreeLosses(t *testing.T) {
	c := New()
	for _, lost := range [][3]int{{0, 1, 2}, {0, 4, 5}, {3, 4, 5}, {1, 2, 5}} {
		shards, _ := c.Split(pattern(1000))
		for _, i := range lost {
			shards[i] = nil
		}
		if _, err := c.Join(shards, 1000); !errors.Is(err, ErrUnrecoverable) {
			t.Fatalf("lose %v: err %v, want ErrUnrecoverable", lost, err)
		}
		if _, err := c.Rebuild(shards, 1000, lost[0]); !errors.Is(err, ErrUnrecoverable) {
			t.Fatalf("rebuild after losing %v: err %v, want ErrUnrecoverable", lost, err)
		}
	}
}

func TestRebuildEachShard(t *testing.T) {
	c := New()
	for _, size := range sizes {
		want, _ := c.Split(pattern(size))
		for i := range TotalShards {
			for other := range TotalShards {
				shards, _ := c.Split(pattern(size))
				shards[i] = nil
				if other != i {
					shards[other] = nil // a second shard lost too: still 4 left
				}
				got, err := c.Rebuild(shards, size, i)
				if err != nil {
					t.Fatalf("size %d shard %d (also lost %d): %v", size, i, other, err)
				}
				if !bytes.Equal(got, want[i]) {
					t.Fatalf("size %d shard %d: rebuilt bytes differ", size, i)
				}
			}
		}
	}
}

func TestSplitShapes(t *testing.T) {
	c := New()
	tests := []struct{ size, per int }{{1, 1}, {4, 1}, {5, 2}, {4 << 20, 1 << 20}, {4<<20 - 1, 1 << 20}}
	for _, tt := range tests {
		data := pattern(tt.size)
		shards, err := c.Split(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(shards) != TotalShards {
			t.Fatalf("size %d: %d shards", tt.size, len(shards))
		}
		for i, s := range shards {
			if len(s) != tt.per {
				t.Fatalf("size %d shard %d: %d bytes, want %d", tt.size, i, len(s), tt.per)
			}
		}
		data[0] ^= 0xff
		if shards[0][0] == data[0] {
			t.Fatalf("size %d: shard aliases the input", tt.size)
		}
	}
	if _, err := c.Split(nil); err == nil {
		t.Fatal("empty chunk split without error")
	}
}

func TestJoinRejectsBadShapes(t *testing.T) {
	c := New()
	shards, _ := c.Split(pattern(100))
	if _, err := c.Join(shards[:5], 100); err == nil {
		t.Fatal("5 shards accepted")
	}
	shards[2] = shards[2][:3]
	if _, err := c.Join(shards, 100); err == nil {
		t.Fatal("short shard accepted")
	}
	if _, err := c.Rebuild(shards, 100, 6); err == nil {
		t.Fatal("index 6 accepted")
	}
}

func TestEncodeGivesEverySlotItsOwnBlock(t *testing.T) {
	c := New()
	zeros := make([]byte, 4<<20) // four equal quarters, equal parity
	other := make([]byte, 4<<20)
	other[len(other)-1] = 1 // shares d0-d2 payloads with zeros
	seen := map[iface.ChunkID]string{}
	for name, data := range map[string][]byte{"zeros": zeros, "other": other} {
		shards, err := c.Encode(data)
		if err != nil {
			t.Fatal(err)
		}
		logical := LogicalID(sha256.Sum256(data))
		if logical == sha256.Sum256(data) {
			t.Fatal("logical ID equals the replicated chunk ID")
		}
		for i, s := range shards {
			if s.ID != sha256.Sum256(s.Block) || int64(len(s.Block)) != BlockSize(int64(len(data))) {
				t.Fatalf("%s shard %d: bad ID or size", name, i)
			}
			if prev, dup := seen[s.ID]; dup {
				t.Fatalf("%s shard %d has the same block ID as %s", name, i, prev)
			}
			seen[s.ID] = name
			p, err := Payload(s.Block, logical, i)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Payload(s.Block, logical, (i+1)%TotalShards); err == nil {
				t.Fatalf("%s shard %d accepted as slot %d", name, i, (i+1)%TotalShards)
			}
			if !bytes.Equal(Block(logical, i, p), s.Block) {
				t.Fatalf("%s shard %d: Block(Payload) differs", name, i)
			}
		}
	}
}

func BenchmarkSplit4MiB(b *testing.B) {
	c := New()
	data := pattern(4 << 20)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := c.Split(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinDegraded4MiB(b *testing.B) {
	c := New()
	data := pattern(4 << 20)
	full, _ := c.Split(data)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		shards := append([][]byte(nil), full...)
		shards[0], shards[1] = nil, nil
		if _, err := c.Join(shards, len(data)); err != nil {
			b.Fatal(err)
		}
	}
}
