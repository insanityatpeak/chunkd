package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// defaultPatterns catch generic attribution trailers. Tool-specific names are
// kept in the untracked patterns file so the repo never has to spell them.
var defaultPatterns = []string{"co-authored-by", "generated with", "\U0001F916"}

const localPatternsFile = ".trace-patterns.local.txt"

// traceExempt files legitimately contain pattern text: the checker's own
// pattern list, and ignore rules that name local-only files.
var traceExempt = map[string]bool{
	".gitignore":                    true,
	"tools/task/tracecheck.go":      true,
	"tools/task/tracecheck_test.go": true,
}

func traceCheck() error {
	patterns, err := loadPatterns()
	if err != nil {
		return err
	}
	var hits []string

	files, err := gitLines("ls-files", "-z")
	if err != nil {
		return err
	}
	for _, f := range files {
		if traceExempt[f] {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			if os.IsNotExist(err) {
				continue // deleted in the working tree
			}
			return err
		}
		if bytes.IndexByte(data[:min(len(data), 8000)], 0) >= 0 {
			continue // binary
		}
		hits = append(hits, scan(f, string(data), patterns)...)
	}

	email, _ := gitOutput("config", "user.email")
	commits, err := outgoingCommits()
	if err != nil {
		return err
	}
	for _, c := range commits {
		hits = append(hits, scan("commit "+c.sha[:7], c.msg, patterns)...)
		if email != "" && c.author != email {
			hits = append(hits, fmt.Sprintf("commit %s: author %s, want %s", c.sha[:7], c.author, email))
		}
	}

	for _, h := range hits {
		fmt.Println(h)
	}
	if len(hits) > 0 {
		return fmt.Errorf("trace-check: %d finding(s)", len(hits))
	}
	fmt.Printf("trace-check: %d files and %d outgoing commits clean (%d patterns)\n", len(files), len(commits), len(patterns))
	return nil
}

func loadPatterns() ([]string, error) {
	patterns := append([]string(nil), defaultPatterns...)
	f, err := os.Open(localPatternsFile)
	if os.IsNotExist(err) {
		return patterns, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if p := strings.TrimSpace(s.Text()); p != "" && !strings.HasPrefix(p, "#") {
			patterns = append(patterns, strings.ToLower(p))
		}
	}
	return patterns, s.Err()
}

// scan returns "name:line: pattern" for every case-insensitive match.
func scan(name, text string, patterns []string) []string {
	var out []string
	for i, line := range strings.Split(text, "\n") {
		l := strings.ToLower(line)
		for _, p := range patterns {
			if strings.Contains(l, p) {
				out = append(out, fmt.Sprintf("%s:%d: contains %q", name, i+1, p))
			}
		}
	}
	return out
}

type commit struct{ sha, author, msg string }

// outgoingCommits returns commits not yet on the upstream branch, or every
// commit when there is no upstream (first push).
func outgoingCommits() ([]commit, error) {
	if _, err := gitOutput("rev-parse", "--verify", "-q", "HEAD"); err != nil {
		return nil, nil // no commits yet
	}
	rng := "HEAD"
	if _, err := gitOutput("rev-parse", "--abbrev-ref", "@{u}"); err == nil {
		rng = "@{u}..HEAD"
	}
	out, err := gitOutput("log", "--format=%H%x1f%ae%x1f%B%x1e", rng)
	if err != nil {
		return nil, err
	}
	var cs []commit
	for _, rec := range strings.Split(out, "\x1e") {
		parts := strings.SplitN(strings.TrimLeft(rec, "\n"), "\x1f", 3)
		if len(parts) == 3 {
			cs = append(cs, commit{parts[0], parts[1], parts[2]})
		}
	}
	return cs, nil
}

func gitOutput(args ...string) (string, error) {
	out, err := exec.Command("git", args...).Output()
	return strings.TrimSpace(string(out)), err
}

func gitLines(args ...string) ([]string, error) {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	var lines []string
	for _, l := range strings.Split(string(out), "\x00") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines, nil
}
