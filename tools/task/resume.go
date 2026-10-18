package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// resumeSize is large enough that the upload is still running when the
// gateway is killed: the CLI reports progress every 8 chunks (32 MiB).
const resumeSize = 256 << 20

// resumeE2E runs against the compose cluster: start put -resume of a random
// file, kill the gateway once the upload is under way, let the first put
// fail, start the gateway again, run put -resume, and compare the downloaded
// bytes with the original. --up starts the cluster first; --down removes it
// (and its volumes) afterwards.
func resumeE2E(args []string) error {
	up, down := false, false
	for _, a := range args {
		switch a {
		case "--up":
			up = true
		case "--down":
			down = true
		default:
			return fmt.Errorf("resume-e2e: unknown flag %q", a)
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

	dir, err := os.MkdirTemp("", "chunkd-resume-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	// The CLI keeps the upload id in the user cache directory; point it at
	// the temp directory so a run never finds another run's upload.
	// 127.0.0.1, not localhost: after a container restart Docker Desktop can leave
	// the [::1] forward stale while IPv4 works.
	env := append(os.Environ(), "CHUNKD_GATEWAY=http://127.0.0.1:8080", "XDG_CACHE_HOME="+dir, "LocalAppData="+dir, "HOME="+dir)
	cli := filepath.Join(dir, "chunkd")
	if runtime.GOOS == "windows" {
		cli += ".exe"
	}
	if err := goCmd(nil, "build", "-o", cli, "./cmd/chunkd"); err != nil {
		return err
	}

	in := filepath.Join(dir, "in.bin")
	sum, err := randomFile(in, resumeSize)
	if err != nil {
		return err
	}
	remote := "/resume/random-" + sum[:8] + ".bin"

	// First put: killed mid-upload.
	first := exec.Command(cli, "put", "-resume", in, remote)
	first.Env = env
	stderrPipe, err := first.StderrPipe()
	if err != nil {
		return err
	}
	if err := first.Start(); err != nil {
		return err
	}
	progressed := make(chan struct{})
	var firstErr bytes.Buffer
	go func() {
		// Progress is written with \r, so split on it as well as on \n.
		sc := bufio.NewScanner(io.TeeReader(stderrPipe, &firstErr))
		sc.Split(splitCR)
		lines := 0
		for sc.Scan() {
			if strings.Contains(sc.Text(), " / ") {
				if lines++; lines == 2 {
					close(progressed)
				}
			}
		}
	}()
	select {
	case <-progressed:
	case <-time.After(2 * time.Minute):
		_ = first.Process.Kill()
		return fmt.Errorf("the first put reported no progress in 2 minutes")
	}
	fmt.Println("upload under way: killing the gateway")
	if err := run(nil, "", "docker", "compose", "kill", "gateway"); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- first.Wait() }()
	// The CLI has no request deadline, and the Docker port proxy can keep the
	// connection open after the gateway dies, so a put may hang: give it 10 s
	// to fail, then interrupt it as a user would.
	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("the first put finished although the gateway was killed; raise resumeSize")
		}
		fmt.Println("first put failed as expected:", err)
	case <-time.After(10 * time.Second):
		fmt.Println("first put still waiting on the dead gateway: interrupting it")
		_ = first.Process.Kill()
		<-done
	}

	if err := run(nil, "", "docker", "compose", "start", "gateway"); err != nil {
		return err
	}
	if err := waitHealthy("http://127.0.0.1:8080/healthz", 90*time.Second); err != nil {
		return err
	}

	// Second put: continues the kept upload.
	second := exec.Command(cli, "put", "-resume", in, remote)
	second.Env = env
	var secondErr bytes.Buffer
	second.Stdout, second.Stderr = os.Stdout, io.MultiWriter(os.Stderr, &secondErr)
	if err := second.Run(); err != nil {
		return fmt.Errorf("put -resume: %w", err)
	}
	if strings.Contains(secondErr.String(), "starting over") {
		return fmt.Errorf("put -resume restarted the upload instead of continuing it")
	}

	back := filepath.Join(dir, "out.bin")
	get := exec.Command(cli, "-expect-sha256", sum, "get", remote, back)
	get.Env = env
	if out, err := get.CombinedOutput(); err != nil {
		return fmt.Errorf("chunkd get: %w\n%s", err, out)
	}
	got, err := sha256File(back)
	if err != nil {
		return err
	}
	if got != sum {
		return fmt.Errorf("downloaded sha256 %s, uploaded %s", got, sum)
	}
	fmt.Printf("resume-e2e: ok (%d MiB, sha256 %s)\n", resumeSize>>20, sum)
	return nil
}

func splitCR(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func randomFile(path string, n int64) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, h), rand.Reader, n); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func waitHealthy(url string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy after %v", url, limit)
		}
		time.Sleep(time.Second)
	}
}
