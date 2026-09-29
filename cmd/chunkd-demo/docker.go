package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

// docker kills and starts compose services. Inside a container it talks to
// the Docker Engine API over the mounted socket, so the image needs no CLI;
// on a host without that socket (Windows) it runs the docker CLI.
type docker interface {
	kill(service string) error
	start(service string) error
}

func newDocker(socket, project string) docker {
	if _, err := os.Stat(socket); err == nil {
		return &engineAPI{project: project, http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			}},
		}}
	}
	return dockerCLI{project: project}
}

// engineAPI is the Docker Engine API, just the three calls needed.
type engineAPI struct {
	project string
	http    *http.Client
}

func (d *engineAPI) container(service string) (string, error) {
	filters, _ := json.Marshal(map[string][]string{"label": {
		"com.docker.compose.project=" + d.project, "com.docker.compose.service=" + service}})
	resp, err := d.http.Get("http://docker/containers/json?all=1&filters=" + url.QueryEscape(string(filters)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var cs []struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cs); err != nil {
		return "", err
	}
	if len(cs) != 1 {
		return "", fmt.Errorf("%d containers for service %s in project %s", len(cs), service, d.project)
	}
	return cs[0].ID, nil
}

func (d *engineAPI) post(service, action string) error {
	id, err := d.container(service)
	if err != nil {
		return err
	}
	resp, err := d.http.Post("http://docker/containers/"+id+"/"+action, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker %s %s: %s %s", action, service, resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

func (d *engineAPI) kill(service string) error  { return d.post(service, "kill") }
func (d *engineAPI) start(service string) error { return d.post(service, "start") }

// dockerCLI finds the container by its compose labels, so it works from
// any directory.
type dockerCLI struct{ project string }

func (d dockerCLI) container(service string) (string, error) {
	out, err := exec.Command("docker", "ps", "-a", "--format", "{{.Names}}",
		"--filter", "label=com.docker.compose.project="+d.project, "--filter", "label=com.docker.compose.service="+service).Output()
	if err != nil {
		return "", fmt.Errorf("docker ps: %w", err)
	}
	names := strings.Fields(string(out))
	if len(names) != 1 {
		return "", fmt.Errorf("%d containers for service %s in project %s", len(names), service, d.project)
	}
	return names[0], nil
}

func (d dockerCLI) run(action, service string) error {
	name, err := d.container(service)
	if err != nil {
		return err
	}
	if out, err := exec.Command("docker", action, name).CombinedOutput(); err != nil {
		return fmt.Errorf("docker %s %s: %w: %s", action, name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (d dockerCLI) kill(service string) error  { return d.run("kill", service) }
func (d dockerCLI) start(service string) error { return d.run("start", service) }
