package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/insanityatpeak/chunkd/internal/core/meta"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/metastore"
)

// MetaIdentity sets cfg.RaftID and cfg.Join for a metadata peer about to
// start over store (ADR-0032). A recorded ID wins. Otherwise, with join set
// and an empty store, the peer asks the group for the ID it added for this
// slot, retrying while no leader answers, and records it before consensus
// starts. join over a store that holds a log is ignored: that peer still has
// its votes and entries and keeps its identity, so starting every peer with
// the flag is harmless. Without a record or a join, the peer is generation 0
// of its slot, as every peer was before ADR-0032.
func MetaIdentity(ctx context.Context, store *metastore.Store, cfg *meta.Config, addrs map[iface.NodeID]string, join bool, log *slog.Logger) error {
	id, err := store.RaftID()
	if err != nil {
		return err
	}
	if id == 0 && join {
		empty, err := store.Empty()
		if err != nil {
			return err
		}
		if !empty {
			log.Warn("join ignored: the data directory holds a log")
			join = false
		} else {
			if id, err = askJoin(ctx, cfg, addrs, log); err != nil {
				return err
			}
			if err := store.SetRaftID(id); err != nil {
				return err
			}
			log.Info("joined the metadata group", "raft_id", id)
		}
	}
	cfg.RaftID = id
	// Any generation past 0 joined a running group: it never bootstraps one.
	cfg.Join = join || id > uint64(len(cfg.Peers))
	return nil
}

func askJoin(ctx context.Context, cfg *meta.Config, addrs map[iface.NodeID]string, log *slog.Logger) (uint64, error) {
	caller := grpcnet.NewCaller(addrs, 5*time.Second)
	defer caller.Close()
	for {
		id, err := meta.AskJoin(ctx, caller, cfg.ID, cfg.Peers)
		if err == nil {
			return id, nil
		}
		switch iface.CodeOf(err) {
		case iface.CodeConflict, iface.CodeNotFound, iface.CodeInvalid:
			return 0, fmt.Errorf("join: %w", err)
		}
		log.Warn("join: no leader answered; retrying", "err", err)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
