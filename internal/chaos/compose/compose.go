// Package compose is the real-mode chaos target: the docker compose
// cluster, faulted through the docker CLI and driven through the gateway.
package compose

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/insanityatpeak/chunkd/internal/chaos"
	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/client/httpclient"
)

// Target drives the compose project in the current directory.
type Target struct {
	api     *httpclient.Client
	project string
	wiped   map[string]bool
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
// container; they run in the sim only.
func (t *Target) Apply(f chaos.Fault) error {
	svc := string(f.Node)
	switch f.Kind {
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
	}
	return chaos.ErrUnsupported
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

func (t *Target) Put(path string, data []byte) error {
	c, cancel := ctx()
	defer cancel()
	_, err := t.api.Put(c, path, bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true})
	return err
}

func (t *Target) Get(path string) ([]byte, error) {
	c, cancel := ctx()
	defer cancel()
	var buf bytes.Buffer
	_, err := t.api.Get(c, path, &buf)
	return buf.Bytes(), err
}

func (t *Target) Delete(path string) error {
	c, cancel := ctx()
	defer cancel()
	_, err := t.api.Delete(c, path, 0)
	return err
}

func (t *Target) Health() (client.Health, error) {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := t.api.Cluster(c)
	return cl.Health, err
}
