// Package compose is the real-mode chaos target: the docker compose
// cluster, faulted through the docker CLI and driven through the gateway.
package compose

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/chaos"
	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Target drives the compose project in the current directory.
type Target struct {
	api     *httpclient.Client
	project string
	wiped   map[string]bool
	added   []string // extra nodes AddNode started, for Cleanup
	killed  string   // metadata peer KillLeader stopped, until ReviveLeader
}

var _ chaos.Target = (*Target)(nil)

// New returns a target for the compose project whose gateway is at gateway.
func New(gateway, project string) *Target {
	return &Target{api: httpclient.New(gateway), project: project, wiped: map[string]bool{}}
}

func docker(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}

// Apply injects process-level faults. Message-level faults (loss,
// duplication, per-node delay) need tc netem and NET_ADMIN in every
// container; they run in the sim only, as do the leader freeze and the
// leader partition, which need per-link rules between metadata peers.
func (t *Target) Apply(f chaos.Fault) error {
	svc := string(f.Node)
	switch f.Kind {
	case chaos.KillLeader:
		if t.killed != "" {
			return fmt.Errorf("%s is still down", t.killed)
		}
		leader, err := t.leader(15 * time.Second)
		if err != nil {
			return err
		}
		if err := docker("compose", "kill", leader); err != nil {
			return err
		}
		t.killed = leader
		return nil
	case chaos.ReviveLeader:
		if t.killed == "" {
			return fmt.Errorf("no killed leader to revive")
		}
		if err := docker("compose", "start", t.killed); err != nil {
			return err
		}
		t.killed = ""
		return nil
	case chaos.Kill:
		return docker("compose", "kill", svc)
	case chaos.Restart:
		if t.wiped[svc] {
			delete(t.wiped, svc)
			return docker("compose", "up", "-d", "--no-deps", svc)
		}
		return docker("compose", "start", svc)
	case chaos.Wipe:
		// A new container on a new, empty volume: the disk is gone.
		if err := docker("compose", "rm", "-f", "-s", svc); err != nil {
			return err
		}
		t.wiped[svc] = true
		return docker("volume", "rm", t.project+"_"+svc)
	case chaos.Freeze:
		return docker("compose", "pause", svc)
	case chaos.Thaw:
		return docker("compose", "unpause", svc)
	case chaos.Corrupt:
		// Flips bytes in the container's own volume; the node finds out on
		// its next read of those chunks, as with real bit rot.
		return docker("compose", "exec", "-T", svc, "chunkd", "debug", "corrupt",
			"-root", "/data", "-n", strconv.Itoa(f.Count), "-pick", strconv.FormatUint(f.Pick, 10))
	case chaos.AddNode:
		return t.addNode()
	case chaos.Drain:
		return t.admin(svc, "draining", time.Minute)
	case chaos.Undrain:
		return t.admin(svc, "active", time.Minute)
	case chaos.Decommission:
		return t.admin(svc, "decommissioned", 3*time.Minute)
	}
	return chaos.ErrUnsupported
}

// leader returns the metadata leader the cluster view names (the service
// has the peer's name), waiting out an election for up to limit.
func (t *Target) leader(limit time.Duration) (string, error) {
	deadline := time.Now().Add(limit)
	for {
		cl, err := t.Cluster()
		if err == nil && cl.MetaLeader != "" {
			return cl.MetaLeader, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("no metadata leader in the cluster view after %v (last error: %v)", limit, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

func (t *Target) Put(path string, data []byte) (uint64, error) {
	c, cancel := ctx()
	defer cancel()
	m, err := t.api.Put(c, path, bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true})
	return m.Version, err
}

func (t *Target) Get(path string) ([]byte, uint64, error) {
	c, cancel := ctx()
	defer cancel()
	var buf bytes.Buffer
	m, err := t.api.Get(c, path, &buf)
	return buf.Bytes(), m.Version, err
}

func (t *Target) Delete(path string) (uint64, error) {
	c, cancel := ctx()
	defer cancel()
	return t.api.Delete(c, path, 0)
}

func (t *Target) Cluster() (client.Cluster, error) {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := t.api.Cluster(c, 0)
	return cl, err
}

// extra is the compose service AddNode starts: node-6, on rack r3, under the
// extra profile. Commands name full too: --profile replaces COMPOSE_PROFILES.
const extra = "node-6"

// addNode starts node-6 on a fresh volume. A run that removed it leaves it
// decommissioned in the metadata log, so it is returned to service too.
func (t *Target) addNode() error {
	if err := docker("compose", "--profile", "full", "--profile", "extra", "up", "-d", "--no-deps", "--wait", extra); err != nil {
		return err
	}
	t.added = append(t.added, extra)
	cl, err := t.Cluster()
	if err == nil && slices.ContainsFunc(cl.Nodes, func(n client.NodeInfo) bool { return n.ID == extra && n.Admin == "active" }) {
		return nil
	}
	return t.admin(extra, "active", time.Minute)
}

// admin sets node's admin state through the gateway, retrying for up to
// limit while the leader is unknown or has not heard from the node since it
// started, and, for a decommission, while the node's chunks are still short
// elsewhere.
func (t *Target) admin(node, state string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := t.api.NodeAdmin(c, node, state)
		cancel()
		code := iface.CodeOf(err)
		retry := code == iface.CodeUnavailable || code == iface.CodeNotLeader || code == iface.CodeNotFound ||
			(code == iface.CodeConflict && state == "decommissioned")
		if err == nil || !retry || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

// Cleanup drains, decommissions and removes every node AddNode started,
// with its volume, so the cluster is back to its compose shape. The
// metadata log keeps the node, decommissioned.
func (t *Target) Cleanup() error {
	for _, n := range t.added {
		if err := t.admin(n, "draining", time.Minute); err != nil {
			return fmt.Errorf("drain %s: %w", n, err)
		}
		if err := t.admin(n, "decommissioned", 5*time.Minute); err != nil {
			return fmt.Errorf("decommission %s: %w", n, err)
		}
		if err := docker("compose", "--profile", "full", "--profile", "extra", "rm", "-f", "-s", n); err != nil {
			return err
		}
		if err := docker("volume", "rm", t.project+"_"+n); err != nil {
			return err
		}
	}
	t.added = nil
	return nil
}
