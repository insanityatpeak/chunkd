package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/real/local"
)

// flaky fails every Append after the first n, as a dying connection would.
type flaky struct {
	client.API
	left    int
	appends []int64 // offsets of the appends that went through
}

func (f *flaky) Append(ctx context.Context, id uint64, offset int64, r io.Reader, n int64) (client.UploadInfo, error) {
	if f.left == 0 {
		return client.UploadInfo{}, errors.New("connection reset")
	}
	f.left--
	f.appends = append(f.appends, offset)
	return f.API.Append(ctx, id, offset, r, n)
}

func TestPutResumeAfterCutOff(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("LocalAppData", cache)
	t.Setenv("HOME", cache)

	const size = 70<<20 + 12345 // 18 chunks, so 3 appends of 8 chunks
	data := make([]byte, size)
	r := rand.New(rand.NewPCG(7, 7))
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	local1 := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(local1, data, 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := local.Start(t.TempDir(), 3, meta.DefaultConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	out := printer{json: true}
	opts := client.PutOptions{Overwrite: true}

	cut := &flaky{API: c.Client, left: 1}
	err = putResumable(ctx, cut, local1, "/r/big", opts, false, out)
	if err == nil {
		t.Fatal("put survived the cut-off")
	}
	if _, serr := c.Client.Stat(ctx, "/r/big"); serr == nil {
		t.Fatal("a cut-off upload is visible")
	}

	// A changed file is refused, the original continues.
	if err := os.WriteFile(local1, append([]byte{^data[0]}, data[1:]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := putResumable(ctx, c.Client, local1, "/r/big", opts, true, out); err == nil {
		t.Fatal("resume accepted a different file")
	}
	if err := os.WriteFile(local1, data, 0o600); err != nil {
		t.Fatal(err)
	}

	more := &flaky{API: c.Client, left: 100}
	if err := putResumable(ctx, more, local1, "/r/big", opts, true, out); err != nil {
		t.Fatal(err)
	}
	if want := int64(8 * (4 << 20)); len(more.appends) != 2 || more.appends[0] < want {
		t.Fatalf("resumed appends at %v, want 2 starting at or past %d", more.appends, want)
	}
	var buf bytes.Buffer
	if _, err := c.Client.Get(ctx, "/r/big", &buf); err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(buf.Bytes()) != sha256.Sum256(data) {
		t.Fatal("read back differs from the file")
	}
	if file, _ := pendingFile(local1, "/r/big"); fileExists(file) {
		t.Fatal("pending upload record outlived the commit")
	}
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }
