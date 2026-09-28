package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// e2e runs against the compose cluster: build the CLI, put a 20 MiB random
// file through the gateway, get it back with its SHA-256 pinned, and compare
// bytes. --up starts the cluster first; --down removes it (and its volumes)
// afterwards.
func e2e(args []string) error {
	up, down := false, false
	for _, a := range args {
		switch a {
		case "--up":
			up = true
		case "--down":
			down = true
		default:
			return fmt.Errorf("e2e: unknown flag %q", a)
		}
	}
	if up {
		if err := run(nil, "", "docker", "compose", "up", "-d", "--build", "--wait", "--wait-timeout", "180"); err != nil {
			return err
		}
	}
	if down {
		defer func() { _ = run(nil, "", "docker", "compose", "down", "-v", "--remove-orphans") }()
	}

	dir, err := os.MkdirTemp("", "chunkd-e2e-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	cli := filepath.Join(dir, "chunkd")
	if runtime.GOOS == "windows" {
		cli += ".exe"
	}
	if err := goCmd(nil, "build", "-o", cli, "./cmd/chunkd"); err != nil {
		return err
	}

	data := make([]byte, 20<<20)
	if _, err := rand.Read(data); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	in := filepath.Join(dir, "in.bin")
	if err := os.WriteFile(in, data, 0o644); err != nil {
		return err
	}
	remote := "/e2e/random-20mib-" + want[:8] + ".bin"

	out, err := exec.Command(cli, "-json", "put", in, remote).Output()
	if err != nil {
		return fmt.Errorf("chunkd put: %w %s", err, stderr(err))
	}
	var m struct {
		Version uint64 `json:"version"`
		SHA256  string `json:"sha256"`
		Chunks  int    `json:"chunks"`
	}
	if err := json.Unmarshal(out, &m); err != nil {
		return fmt.Errorf("parse put output %q: %w", out, err)
	}
	if m.SHA256 != want {
		return fmt.Errorf("cluster recorded sha256 %s, local file is %s", m.SHA256, want)
	}
	fmt.Printf("put %s v%d: %d chunks, sha256 %s\n", remote, m.Version, m.Chunks, m.SHA256)

	back := filepath.Join(dir, "out.bin")
	if out, err := exec.Command(cli, "-expect-sha256", want, "get", remote, back).CombinedOutput(); err != nil {
		return fmt.Errorf("chunkd get: %w\n%s", err, out)
	}
	got, err := os.ReadFile(back)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("downloaded %d bytes differ from the uploaded %d", len(got), len(data))
	}
	fmt.Printf("get %s: 20 MiB, sha256 matches\n", remote)

	if out, err := exec.Command(cli, "stat", remote).CombinedOutput(); err == nil {
		fmt.Print(string(out))
	}
	if out, err := exec.Command(cli, "cluster").CombinedOutput(); err == nil {
		fmt.Print(string(out))
	}
	fmt.Println("e2e: ok")
	return nil
}

func stderr(err error) string {
	if ee, ok := err.(*exec.ExitError); ok {
		return strings.TrimSpace(string(ee.Stderr))
	}
	return ""
}
