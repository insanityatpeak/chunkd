package gateway_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

func TestGatewayVersions(t *testing.T) {
	srv, _ := setup(t)
	api := httpclient.New(srv.URL)
	ctx := context.Background()
	v1, v2 := payload(100<<10), payload(150<<10)
	if _, err := api.Put(ctx, "/v.bin", bytes.NewReader(v1), int64(len(v1)), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.Put(ctx, "/v.bin", bytes.NewReader(v2), int64(len(v2)), client.PutOptions{ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	// A stale expected version conflicts; last-writer-wins does not.
	if _, err := api.Put(ctx, "/v.bin", bytes.NewReader(v1), int64(len(v1)), client.PutOptions{ExpectedVersion: 1}); iface.CodeOf(err) != iface.CodeConflict {
		t.Fatalf("stale CAS put: %v, want conflict", err)
	}
	if m, err := api.Put(ctx, "/v.bin", bytes.NewReader(v1), int64(len(v1)), client.PutOptions{LastWriterWins: true}); err != nil || m.Version != 3 {
		t.Fatalf("LWW put: v%d, %v; want v3", m.Version, err)
	}
	if _, err := api.Delete(ctx, "/v.bin", 3); err != nil {
		t.Fatal(err)
	}
	log, err := api.Log(ctx, "/v.bin")
	if err != nil || len(log) != 4 || log[1].Size != int64(len(v2)) || !log[3].Deleted {
		t.Fatalf("log = %+v, %v", log, err)
	}
	var got bytes.Buffer
	if _, err := api.Get(ctx, "/v.bin", &got); iface.CodeOf(err) != iface.CodeNotFound {
		t.Fatalf("get of a deleted file: %v", err)
	}
	m, err := api.GetVersion(ctx, "/v.bin", 2, &got)
	if err != nil || m.Version != 2 || !bytes.Equal(got.Bytes(), v2) {
		t.Fatalf("get v2: v%d, %d bytes, %v", m.Version, got.Len(), err)
	}
	// v3 re-uploaded v1's bytes: every chunk was already there.
	st, err := api.StatVersion(ctx, "/v.bin", 3)
	if err != nil || st.SHA256 != log[0].SHA256 {
		t.Fatalf("stat v3: %+v, %v", st.FileInfo, err)
	}
}
