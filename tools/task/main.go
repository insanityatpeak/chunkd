// Command task is the project's cross-platform task runner:
//
//	go run ./tools/task <command> [flags]
//
// It replaces a Makefile so the same commands work on Windows, Linux and CI.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const staticcheck = "honnef.co/go/tools/cmd/staticcheck@v0.8.1"

type command struct {
	help string
	run  func(args []string) error
}

var commands = map[string]command{
	"build":        {"compile all packages", func([]string) error { return goCmd(nil, "build", "./...") }},
	"test":         {"run tests with the race detector", test},
	"lint":         {"go vet and staticcheck", lint},
	"lint-imports": {"check core packages import no environment packages", func([]string) error { return lintImports("internal/core") }},
	"proto":        {"regenerate protobuf code with buf in Docker", proto},
	"wasm":         {"build cluster.wasm, copy wasm_exec.js, enforce size budget", func([]string) error { return wasm() }},
	"web":          {"install and build the dashboard into web/dist", web},
	"up":           {"start the compose cluster (--small for 1 meta + 3 nodes)", up},
	"down":         {"stop the compose cluster (-v also deletes its volumes)", down},
	"demo":         {"start the compose cluster, kill a node, narrate the repair, verify the download", demo},
	"diagram":      {"render docs/assets/architecture.svg from its D2 source, in Docker", diagram},
	"gif":          {"render docs/assets/demo.gif from deploy/demo.tape with vhs in Docker", gif},
	"chaos":        {"randomized fault scenarios with invariant checks (--seed=N replays one; --seeds=N; --metas=3 adds leader faults; --ec adds erasure-coded puts)", chaos},
	"bench": {"run the benchmarks and write docs/benchmarks (-stages=sim,meta,real,report; -dir=D for the real cluster's disks)", func(args []string) error {
		return goCmd(nil, append([]string{"run", "./cmd/chunkd-bench"}, args...)...)
	}},
	"e2e":         {"put and get 20 MiB via the compose gateway (--up to start, --down to clean up)", e2e},
	"trace-check": {"fail if tracked files or outgoing commits carry attribution text (--all: every commit in history)", func(args []string) error { return traceCheck(slices.Contains(args, "--all")) }},
	"ci":          {"run every check CI runs, in CI order", ci},
}

func main() {
	if err := rootDir(); err != nil {
		fatal(err)
	}
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	c, ok := commands[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err := c.run(os.Args[2:]); err != nil {
		fatal(err)
	}
}

// chaos forwards flags to cmd/chunkd-chaos. --mode=real runs the compose
// runner instead of the sim.
func chaos(args []string) error {
	for _, a := range args {
		if a == "--mode=real" || a == "-mode=real" {
			return chaosReal(args)
		}
	}
	var fwd []string
	for _, a := range args {
		if a != "--mode=sim" && a != "-mode=sim" {
			fwd = append(fwd, a)
		}
	}
	return goCmd(nil, append([]string{"run", "./cmd/chunkd-chaos"}, fwd...)...)
}

// demo starts the compose cluster and runs cmd/chunkd-demo against it.
func demo(args []string) error {
	if err := run(nil, "", "docker", "compose", "up", "-d", "--build", "--wait", "--wait-timeout", "300"); err != nil {
		return err
	}
	return goCmd(nil, append([]string{"run", "./cmd/chunkd-demo"}, args...)...)
}

// diagram renders the README's architecture SVG from its D2 source.
func diagram([]string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	return run(nil, "", "docker", "run", "--rm", "-v", wd+":/home/debian/src", "-w", "/home/debian/src",
		"terrastruct/d2:v0.7.1", "--theme=0", "--dark-theme=200", "--pad=24",
		"docs/assets/architecture.d2", "docs/assets/architecture.svg")
}

// gif renders the README GIF: the demo against a live compose cluster,
// recorded by vhs in Docker.
func gif([]string) error {
	if err := run(nil, "", "docker", "compose", "up", "-d", "--build", "--wait", "--wait-timeout", "300"); err != nil {
		return err
	}
	env := []string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}
	if err := goCmd(env, "build", "-trimpath", "-o", "bin/chunkd-demo", "./cmd/chunkd-demo"); err != nil {
		return err
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := run(nil, "", "docker", "run", "--rm", "-v", wd+":/vhs", "-v", "/var/run/docker.sock:/var/run/docker.sock",
		"--add-host", "host.docker.internal:host-gateway", "ghcr.io/charmbracelet/vhs:v0.10.0", "deploy/demo.tape"); err != nil {
		return err
	}
	fi, err := os.Stat("docs/assets/demo.gif")
	if err != nil {
		return err
	}
	fmt.Printf("docs/assets/demo.gif: %.1f MiB (limit 5 MiB)\n", float64(fi.Size())/(1<<20))
	if fi.Size() > 5<<20 {
		return fmt.Errorf("demo.gif is over 5 MiB")
	}
	return nil
}

// chaosReal builds and starts the compose cluster, runs the real-mode
// suite, and with --down removes the cluster afterwards.
func chaosReal(args []string) error {
	down := false
	fwd := []string{"run", "./cmd/chunkd-chaos", "-mode=real"}
	for _, a := range args {
		switch a {
		case "--mode=real", "-mode=real":
		case "--down":
			down = true
		default:
			fwd = append(fwd, a)
		}
	}
	if err := run(nil, "", "docker", "compose", "up", "-d", "--build", "--wait", "--wait-timeout", "180"); err != nil {
		return err
	}
	if down {
		defer func() { _ = run(nil, "", "docker", "compose", "down", "-v", "--remove-orphans") }()
	}
	return goCmd(nil, fwd...)
}

func usage() {
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintln(os.Stderr, "usage: go run ./tools/task <command>")
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-13s %s\n", n, commands[n].help)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "task:", err)
	os.Exit(1)
}

// rootDir moves to the module root so every command uses repo-relative paths.
func rootDir() error {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return fmt.Errorf("go env GOMOD: %w", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == os.DevNull {
		return fmt.Errorf("not inside the chunkd module")
	}
	return os.Chdir(filepath.Dir(mod))
}

// run executes name with args in dir, streaming output. env entries are added
// to the current environment.
func run(env []string, dir, name string, args ...string) error {
	fmt.Println(">", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func goCmd(env []string, args ...string) error { return run(env, "", "go", args...) }

// testTimeout is per package. internal/sim/cluster runs about 10 min under
// -race on a CI runner, past go test's 10 min default.
const testTimeout = "-timeout=25m"

func test(args []string) error {
	if _, err := exec.LookPath("gcc"); err != nil && os.Getenv("CI") == "" {
		fmt.Println("warning: no C compiler found; running tests without -race (CI always uses -race)")
		return goCmd(nil, append([]string{"test", testTimeout}, append(args, "./...")...)...)
	}
	return goCmd([]string{"CGO_ENABLED=1"}, append([]string{"test", "-race", testTimeout}, append(args, "./...")...)...)
}

func lint([]string) error {
	if err := goCmd(nil, "vet", "./..."); err != nil {
		return err
	}
	if err := goCmd([]string{"GOOS=js", "GOARCH=wasm"}, "vet", "./cmd/chunkd-wasm"); err != nil {
		return err
	}
	return goCmd(nil, "run", staticcheck, "./...")
}

func proto([]string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	return run(nil, "", "docker", "run", "--rm", "-v", wd+":/workspace", "-w", "/workspace",
		"bufbuild/buf:1.73.0", "generate")
}

func web([]string) error {
	if err := run(nil, "web", "npm", "ci", "--no-audit", "--no-fund"); err != nil {
		return err
	}
	return run(nil, "web", "npm", "run", "build")
}

func up(args []string) error {
	cmd := []string{"compose"}
	var env []string
	for _, a := range args {
		if a == "--small" {
			cmd = append(cmd, "--profile", "small")
			// One metadata server: the group is just meta-1.
			env = append(env, "META_PEERS=meta-1=meta-1:7000")
		}
	}
	return run(env, "", "docker", append(cmd, "up", "-d", "--build")...)
}

func down(args []string) error {
	cmd := []string{"compose", "down", "--remove-orphans"}
	for _, a := range args {
		if a == "-v" {
			cmd = append(cmd, "-v")
		}
	}
	return run(nil, "", "docker", cmd...)
}

func ci([]string) error {
	steps := []struct {
		name string
		f    func() error
	}{
		{"build", func() error { return goCmd(nil, "build", "./...") }},
		{"lint", func() error { return lint(nil) }},
		{"lint-imports", func() error { return lintImports("internal/core") }},
		{"test", func() error { return test(nil) }},
		{"wasm", wasm},
		{"web", func() error { return web(nil) }},
	}
	for _, s := range steps {
		fmt.Printf("\n=== %s\n", s.name)
		if err := s.f(); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}
	return nil
}
