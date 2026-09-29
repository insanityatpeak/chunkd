package gateway_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/gateway"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

// locked serialises calls: the sim is single-threaded, HTTP handlers are not.
type locked struct {
	mu  sync.Mutex
	api client.API
}

func (l *locked) Put(ctx context.Context, p string, r io.Reader, n int64, o client.PutOptions) (client.Manifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Put(ctx, p, r, n, o)
}
func (l *locked) Get(ctx context.Context, p string, w io.Writer) (client.Manifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Get(ctx, p, w)
}
func (l *locked) Stat(ctx context.Context, p string) (client.Manifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Stat(ctx, p)
}
func (l *locked) List(ctx context.Context, p string) ([]client.FileInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.List(ctx, p)
}
func (l *locked) Delete(ctx context.Context, p string, v uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Delete(ctx, p, v)
}
func (l *locked) Cluster(ctx context.Context, after uint64) (client.Cluster, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Cluster(ctx, after)
}
func (l *locked) Log(ctx context.Context, p string) ([]client.VersionInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.Log(ctx, p)
}
func (l *locked) StatVersion(ctx context.Context, p string, v uint64) (client.Manifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.StatVersion(ctx, p, v)
}
func (l *locked) GetVersion(ctx context.Context, p string, v uint64, w io.Writer) (client.Manifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.api.GetVersion(ctx, p, v, w)
}

func setup(t *testing.T) (*httptest.Server, *cluster.Cluster) {
	t.Helper()
	cfg := cluster.DefaultConfig()
	cfg.Meta.ChunkSize = 64 << 10
	c := cluster.New(7, cfg, io.Discard)
	c.Tick(3 * time.Second)
	caller := c.NewCaller("gateway")
	api := &locked{api: client.New(caller, client.Options{Meta: cluster.MetaID, Sleep: caller.Sleep})}
	srv := httptest.NewServer(gateway.Handler(api, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return srv, c
}

// payload is pseudo-random so no two chunks share content (and address).
func payload(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 1))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func TestGatewayRoundTrip(t *testing.T) {
	srv, _ := setup(t)
	api := httpclient.New(srv.URL)
	ctx := context.Background()
	data := payload(300 << 10) // 5 chunks at 64 KiB

	m, err := api.Put(ctx, "/docs/a.bin", bytes.NewReader(data), int64(len(data)), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if m.Version != 1 || m.Chunks != 5 || m.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("put manifest %+v", m.FileInfo)
	}
	var got bytes.Buffer
	if _, err := api.Get(ctx, "/docs/a.bin", &got); err != nil || !bytes.Equal(got.Bytes(), data) {
		t.Fatalf("get: %d bytes, %v", got.Len(), err)
	}
	st, err := api.Stat(ctx, "/docs/a.bin")
	if err != nil || len(st.Chunk) != 5 {
		t.Fatalf("stat %+v %v", st, err)
	}
	// Commit guarantees 2 reported copies (ADR-0007); the third may still
	// be in flight.
	for _, ch := range st.Chunk {
		if len(ch.Replicas) < 2 {
			t.Fatalf("chunk %d has replicas %v, want at least 2", ch.Index, ch.Replicas)
		}
	}
	files, err := api.List(ctx, "/docs")
	if err != nil || len(files) != 1 || files[0].Path != "/docs/a.bin" {
		t.Fatalf("list %+v %v", files, err)
	}
	c, err := api.Cluster(ctx, 0)
	if err != nil || len(c.Nodes) != 5 || c.Files != 1 {
		t.Fatalf("cluster %+v %v", c, err)
	}
	if len(c.FileHealth) != 1 || c.FileHealth[0].Chunks != 5 || c.FileHealth[0].MinLive < 2 {
		t.Fatalf("file health %+v", c.FileHealth)
	}
	// Five joins on the timeline; asking after the latest returns none.
	if len(c.Events) < 5 || c.Events[len(c.Events)-1].Seq != c.EventSeq {
		t.Fatalf("events %+v, latest %d", c.Events, c.EventSeq)
	}
	if again, err := api.Cluster(ctx, c.EventSeq); err != nil || len(again.Events) != 0 || again.EventSeq < c.EventSeq {
		t.Fatalf("events after %d: %+v %v", c.EventSeq, again.Events, err)
	}

	// Compare-and-swap through HTTP: stale expected version is a conflict.
	if _, err := api.Put(ctx, "/docs/a.bin", bytes.NewReader(data), int64(len(data)), client.PutOptions{ExpectedVersion: 0}); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("create over existing: %v, want conflict", err)
	}
	if v, err := api.Delete(ctx, "/docs/a.bin", 1); err != nil || v != 2 {
		t.Fatalf("delete = v%d, %v", v, err)
	}
	if _, err := api.Get(ctx, "/docs/a.bin", io.Discard); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("get after delete: %v, want not found", err)
	}
}

func TestGatewayStatusCodes(t *testing.T) {
	srv, _ := setup(t)
	tests := []struct {
		method, path string
		body         io.Reader
		chunked      bool
		want         int
	}{
		{"GET", "/files/missing", nil, false, http.StatusNotFound},
		{"GET", "/files/missing?manifest=1", nil, false, http.StatusNotFound},
		{"POST", "/files/x", strings.NewReader("abc"), true, http.StatusLengthRequired},
		{"POST", "/files/x?expected=zz", strings.NewReader("abc"), false, http.StatusBadRequest},
		{"DELETE", "/files/missing", nil, false, http.StatusNotFound},
		{"GET", "/cluster?events_after=-1", nil, false, http.StatusBadRequest},
		{"OPTIONS", "/files/x", nil, false, http.StatusNoContent},
	}
	for _, tt := range tests {
		req, _ := http.NewRequest(tt.method, srv.URL+tt.path, tt.body)
		if tt.chunked {
			req.ContentLength = -1
			req.TransferEncoding = []string{"chunked"}
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, resp.StatusCode, tt.want)
		}
	}
}

// A gateway that flips one byte of the data it serves. The HTTP client must
// notice: it checks every chunk against the manifest hash.
func TestClientDetectsLyingGateway(t *testing.T) {
	srv, _ := setup(t)
	data := payload(200 << 10)
	if _, err := httpclient.New(srv.URL).Put(context.Background(), "/f", bytes.NewReader(data), int64(len(data)), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := http.Get(srv.URL + r.URL.String())
		if err != nil {
			t.Error(err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		if !r.URL.Query().Has("manifest") && len(body) > 1000 {
			body[1000] ^= 1
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
	}))
	defer liar.Close()
	_, err := httpclient.New(liar.URL).Get(context.Background(), "/f", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("tampered download accepted: %v", err)
	}
}

func TestGatewayServesUI(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.html"), []byte("<title>chunkd</title>"), 0o644)
	cfg := cluster.DefaultConfig()
	c := cluster.New(7, cfg, io.Discard)
	c.Tick(3 * time.Second)
	caller := c.NewCaller("gateway")
	api := &locked{api: client.New(caller, client.Options{Meta: cluster.MetaID, Sleep: caller.Sleep})}
	srv := httptest.NewServer(gateway.WithUI(gateway.Handler(api, slog.New(slog.NewTextHandler(io.Discard, nil))), dir))
	defer srv.Close()
	for path, want := range map[string]string{"/": "<title>chunkd</title>", "/mode.json": `"live"`, "/cluster": `"nodes"`} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), want) {
			t.Errorf("GET %s = %d %q, want %q", path, resp.StatusCode, b, want)
		}
	}
}
