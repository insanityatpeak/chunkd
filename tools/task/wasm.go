package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	wasmOut      = "web/public/cluster.wasm"
	wasmExecOut  = "web/public/wasm_exec.js"
	wasmWarnSize = 15 << 20
	wasmMaxSize  = 20 << 20
)

func wasm() error {
	if err := os.MkdirAll(filepath.Dir(wasmOut), 0o755); err != nil {
		return err
	}
	env := []string{"GOOS=js", "GOARCH=wasm", "CGO_ENABLED=0"}
	if err := checkWasmDeps(env); err != nil {
		return err
	}
	if err := goCmd(env, "build", "-trimpath", "-ldflags=-s -w", "-o", wasmOut, "./cmd/chunkd-wasm"); err != nil {
		return err
	}
	if err := copyWasmExec(); err != nil {
		return err
	}
	fi, err := os.Stat(wasmOut)
	if err != nil {
		return err
	}
	size := fi.Size()
	msg := fmt.Sprintf("cluster.wasm: %.2f MiB (target < %d MiB, limit %d MiB)",
		float64(size)/(1<<20), wasmWarnSize>>20, wasmMaxSize>>20)
	fmt.Println(msg)
	if p := os.Getenv("GITHUB_STEP_SUMMARY"); p != "" {
		if f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			fmt.Fprintf(f, "**%s**\n", msg)
			f.Close()
		}
	}
	switch {
	case size > wasmMaxSize:
		return fmt.Errorf("cluster.wasm exceeds the %d MiB budget", wasmMaxSize>>20)
	case size > wasmWarnSize:
		fmt.Println("warning: cluster.wasm is above the target size")
	}
	return nil
}

// wasmBannedDeps are server-side packages that must never reach the browser
// build; gRPC alone added over 12 MiB before services moved to chunkd.rpc.v1.
var wasmBannedDeps = []string{"google.golang.org/grpc", "net/http"}

func checkWasmDeps(env []string) error {
	cmd := exec.Command("go", "list", "-deps", "./cmd/chunkd-wasm")
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("go list -deps: %w", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, b := range wasmBannedDeps {
			if dep == b || strings.HasPrefix(dep, b+"/") {
				return fmt.Errorf("cmd/chunkd-wasm depends on %s; keep server-only code out of the sim build", dep)
			}
		}
	}
	return nil
}

// copyWasmExec copies the JS glue from the toolchain that built the binary;
// a mismatched wasm_exec.js fails at runtime with obscure import errors.
func copyWasmExec() error {
	out, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		return err
	}
	root := strings.TrimSpace(string(out))
	var src string
	for _, p := range []string{"lib/wasm/wasm_exec.js", "misc/wasm/wasm_exec.js"} {
		if _, err := os.Stat(filepath.Join(root, p)); err == nil {
			src = filepath.Join(root, p)
			break
		}
	}
	if src == "" {
		return fmt.Errorf("wasm_exec.js not found under %s", root)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	dst, err := os.Create(wasmExecOut)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, in); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}
