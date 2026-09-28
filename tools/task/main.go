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
	"down":         {"stop the compose cluster", func([]string) error { return run(nil, "", "docker", "compose", "down", "--remove-orphans") }},
	"demo":         {"scripted failure demo (not implemented yet)", func([]string) error { fmt.Println("demo: not implemented yet"); return nil }},
	"trace-check":  {"fail if tracked files or outgoing commits carry attribution text", func([]string) error { return traceCheck() }},
	"ci":           {"run every check CI runs, in CI order", ci},
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

func test(args []string) error {
	if _, err := exec.LookPath("gcc"); err != nil && os.Getenv("CI") == "" {
		fmt.Println("warning: no C compiler found; running tests without -race (CI always uses -race)")
		return goCmd(nil, append([]string{"test"}, append(args, "./...")...)...)
	}
	return goCmd([]string{"CGO_ENABLED=1"}, append([]string{"test", "-race"}, append(args, "./...")...)...)
}

func lint([]string) error {
	if err := goCmd(nil, "vet", "./..."); err != nil {
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
	for _, a := range args {
		if a == "--small" {
			cmd = append(cmd, "--profile", "small")
		}
	}
	return run(nil, "", "docker", append(cmd, "up", "-d", "--build")...)
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
