package chunk

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func data(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func TestSplitter(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		chunkSize int
		wantLens  []int
	}{
		{"empty", 0, 4, nil},
		{"exact multiple", 12, 4, []int{4, 4, 4}},
		{"short last chunk", 10, 4, []int{4, 4, 2}},
		{"smaller than one chunk", 3, 4, []int{3}},
		{"one-byte chunks", 5, 1, []int{1, 1, 1, 1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := data(tt.size)
			// OneByteReader forces ReadFull to assemble chunks from short reads.
			s := NewSplitter(iotest.OneByteReader(bytes.NewReader(in)), tt.chunkSize)
			var got []int
			var joined []byte
			for {
				c, err := s.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if c.Index != len(got) {
					t.Fatalf("index %d, want %d", c.Index, len(got))
				}
				if c.ID != sha256.Sum256(c.Data) {
					t.Fatalf("chunk %d: ID does not match its data", c.Index)
				}
				got = append(got, len(c.Data))
				joined = append(joined, c.Data...)
			}
			if len(got) != len(tt.wantLens) {
				t.Fatalf("chunk lengths %v, want %v", got, tt.wantLens)
			}
			for i := range got {
				if got[i] != tt.wantLens[i] {
					t.Fatalf("chunk lengths %v, want %v", got, tt.wantLens)
				}
			}
			if !bytes.Equal(joined, in) {
				t.Fatal("chunks do not reassemble to the input")
			}
			if s.Sum() != sha256.Sum256(in) || s.Total() != int64(tt.size) {
				t.Fatal("file hash or total does not match the input")
			}
			if Count(int64(tt.size), tt.chunkSize) != len(tt.wantLens) {
				t.Fatalf("Count = %d, want %d", Count(int64(tt.size), tt.chunkSize), len(tt.wantLens))
			}
			for i, l := range tt.wantLens {
				if SizeOf(int64(tt.size), tt.chunkSize, i) != int64(l) {
					t.Fatalf("SizeOf(%d) = %d, want %d", i, SizeOf(int64(tt.size), tt.chunkSize, i), l)
				}
			}
			if _, err := s.Next(); !errors.Is(err, io.EOF) {
				t.Fatal("Next after EOF did not return EOF")
			}
		})
	}
}

func TestSplitterPropagatesReadErrors(t *testing.T) {
	boom := errors.New("disk on fire")
	s := NewSplitter(iotest.ErrReader(boom), 4)
	if _, err := s.Next(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestChunksDoNotAlias(t *testing.T) {
	s := NewSplitter(bytes.NewReader(data(8)), 4)
	a, _ := s.Next()
	before := append([]byte(nil), a.Data...)
	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Data, before) {
		t.Fatal("reading the next chunk overwrote the previous chunk's data")
	}
}
