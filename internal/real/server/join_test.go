package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/metastore"
)

func TestMetaIdentityWithoutAsking(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	peers := []iface.NodeID{"meta-1", "meta-2", "meta-3"}
	open := func(t *testing.T) *metastore.Store {
		s, err := metastore.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	cfg := func() *meta.Config {
		c := meta.DefaultConfig("meta-2")
		c.Peers = peers
		return &c
	}

	t.Run("no record, no join: generation 0", func(t *testing.T) {
		c := cfg()
		if err := MetaIdentity(context.Background(), open(t), c, nil, false, log); err != nil || c.RaftID != 0 || c.Join {
			t.Fatalf("raft ID %d join %v, %v", c.RaftID, c.Join, err)
		}
	})
	t.Run("a recorded later generation joins without the flag", func(t *testing.T) {
		s, c := open(t), cfg()
		if err := s.SetRaftID(5); err != nil {
			t.Fatal(err)
		}
		if err := MetaIdentity(context.Background(), s, c, nil, false, log); err != nil || c.RaftID != 5 || !c.Join {
			t.Fatalf("raft ID %d join %v, %v", c.RaftID, c.Join, err)
		}
	})
	t.Run("join over a log is refused", func(t *testing.T) {
		s, c := open(t), cfg()
		if err := s.Save(context.Background(), 1, [][]byte{[]byte("e")}, nil); err != nil {
			t.Fatal(err)
		}
		if err := MetaIdentity(context.Background(), s, c, nil, true, log); err == nil || !strings.Contains(err.Error(), "holds a log") {
			t.Fatalf("err = %v, want a refusal", err)
		}
	})
}
