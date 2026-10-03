package gateway_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/gateway"
)

func keyed(t *testing.T) (url string, alice, bob, root *httpclient.Client) {
	t.Helper()
	entries := []gateway.Key{
		{Name: "alice", SHA256: gateway.HashKey("alice-key"), Namespace: "alice", Quota: 200 << 10},
		{Name: "bob", SHA256: gateway.HashKey("bob-key"), Namespace: "bob"},
		{Name: "root", SHA256: gateway.HashKey("root-key"), Admin: true},
	}
	raw, _ := json.Marshal(entries)
	keys, err := gateway.ParseKeys(raw)
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := setupWith(t, gateway.WithKeys(keys))
	mk := func(key string) *httpclient.Client {
		c := httpclient.New(srv.URL)
		c.Key = key
		return c
	}
	return srv.URL, mk("alice-key"), mk("bob-key"), mk("root-key")
}

func status(t *testing.T, method, url, key string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestKeysRejectedAtLoad(t *testing.T) {
	h := gateway.HashKey("k")
	for name, entries := range map[string][]gateway.Key{
		"bad hash":         {{Name: "a", SHA256: "zz", Namespace: "a"}},
		"no namespace":     {{Name: "a", SHA256: h}},
		"nested namespace": {{Name: "a", SHA256: h, Namespace: "a/b"}},
		"admin namespace":  {{Name: "a", SHA256: h, Namespace: "a", Admin: true}},
		"shared namespace": {{Name: "a", SHA256: h, Namespace: "n"}, {Name: "b", SHA256: gateway.HashKey("k2"), Namespace: "n"}},
		"repeated key":     {{Name: "a", SHA256: h, Namespace: "a"}, {Name: "b", SHA256: h, Namespace: "b"}},
	} {
		raw, _ := json.Marshal(entries)
		if _, err := gateway.ParseKeys(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAuthScopesPathsToNamespace(t *testing.T) {
	url, alice, bob, root := keyed(t)
	ctx := context.Background()
	data := payload(100 << 10)
	put := func(c *httpclient.Client, p string) error {
		_, err := c.Put(ctx, p, bytes.NewReader(data), int64(len(data)), client.PutOptions{})
		return err
	}

	if got := status(t, "GET", url+"/files", ""); got != http.StatusUnauthorized {
		t.Fatalf("list without a key: %d", got)
	}
	if got := status(t, "GET", url+"/files", "wrong"); got != http.StatusUnauthorized {
		t.Fatalf("list with an unknown key: %d", got)
	}
	if got := status(t, "GET", url+"/cluster", ""); got != http.StatusOK {
		t.Fatalf("cluster is open: %d", got)
	}
	if err := put(alice, "/alice/a"); err != nil {
		t.Fatal(err)
	}
	if err := put(alice, "/bob/a"); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("alice into bob's namespace: %v", err)
	}
	if err := put(alice, "/alicex/a"); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("a namespace is a whole segment: %v", err)
	}
	if err := put(bob, "/bob/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := bob.Stat(ctx, "/alice/a"); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("bob reads alice: %v", err)
	}
	if _, err := bob.Delete(ctx, "/alice/a", 0); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("bob deletes alice: %v", err)
	}
	// A list without a prefix is narrowed to the key's namespace.
	files, err := alice.List(ctx, "/")
	if err != nil || len(files) != 1 || files[0].Path != "/alice/a" {
		t.Fatalf("alice lists %v, %v", files, err)
	}
	if _, err := alice.List(ctx, "/bob"); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("alice lists bob: %v", err)
	}
	if files, err = root.List(ctx, "/"); err != nil || len(files) != 2 {
		t.Fatalf("admin lists %v, %v", files, err)
	}
	if _, err := alice.NodeAdmin(ctx, "node-1", "draining"); iface.CodeOf(err) != iface.CodeDenied {
		t.Fatalf("alice drains a node: %v", err)
	}
}

// An upload ID names no path, so another key's ID answers not found.
func TestAuthUploadsBelongToTheirKey(t *testing.T) {
	_, alice, bob, _ := keyed(t)
	ctx := context.Background()
	data := payload(100 << 10)
	sum := sha256.Sum256(data)
	info, err := alice.BeginResumable(ctx, "/alice/r", int64(len(data)), sum, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bob.UploadStatus(ctx, info.ID); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("bob reads alice's upload: %v", err)
	}
	if _, err := bob.Append(ctx, info.ID, 0, bytes.NewReader(data), int64(len(data))); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("bob appends to alice's upload: %v", err)
	}
	if info, err = alice.Append(ctx, info.ID, 0, bytes.NewReader(data), int64(len(data))); err != nil || info.Version != 1 {
		t.Fatalf("alice finishes: %+v, %v", info, err)
	}
}

// Alice may hold 200 KiB. Reservations count at Begin, so a second upload
// is refused before it sends a byte, whether the first has committed or not.
func TestQuotaRefusesBeforeBytesAreSent(t *testing.T) {
	_, alice, _, _ := keyed(t)
	ctx := context.Background()
	data := payload(150 << 10)
	sum := sha256.Sum256(data)
	open, err := alice.BeginResumable(ctx, "/alice/one", int64(len(data)), sum, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other := payload(100 << 10)
	if _, err := alice.BeginResumable(ctx, "/alice/two", int64(len(other)), sha256.Sum256(other), client.PutOptions{}); iface.CodeOf(err) != iface.CodeQuota {
		t.Fatalf("second begin: %v", err)
	}
	if _, err := alice.Put(ctx, "/alice/two", bytes.NewReader(other), int64(len(other)), client.PutOptions{}); iface.CodeOf(err) != iface.CodeQuota {
		t.Fatalf("put over the reservation: %v", err)
	}
	if _, err := alice.Append(ctx, open.ID, 0, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Put(ctx, "/alice/two", bytes.NewReader(other), int64(len(other)), client.PutOptions{}); iface.CodeOf(err) != iface.CodeQuota {
		t.Fatalf("put over the committed bytes: %v", err)
	}
	if _, err := alice.Delete(ctx, "/alice/one", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Put(ctx, "/alice/two", bytes.NewReader(other), int64(len(other)), client.PutOptions{}); err != nil {
		t.Fatalf("after the delete freed the bytes: %v", err)
	}
}

type countingReader struct {
	r interface{ Read([]byte) (int, error) }
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// The library refuses at Begin, before it reads the body or writes a chunk.
func TestQuotaRefusalReadsNoBytes(t *testing.T) {
	_, c := setup(t)
	api := c.Client()
	ctx := client.WithQuota(context.Background(), 100<<10)
	data := payload(150 << 10)
	body := &countingReader{r: bytes.NewReader(data)}
	if _, err := api.Put(ctx, "/q/f", body, int64(len(data)), client.PutOptions{}); iface.CodeOf(err) != iface.CodeQuota {
		t.Fatalf("put: %v", err)
	}
	if body.n != 0 {
		t.Fatalf("%d bytes read before the refusal", body.n)
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatal(err)
	}
}
