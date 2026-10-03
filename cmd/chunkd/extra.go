package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Exit codes: 1 any other failure, 2 usage, then one per class of error a
// script wants to branch on.
const (
	exitUsage       = 2
	exitNotFound    = 3
	exitConflict    = 4
	exitQuota       = 5
	exitDenied      = 6
	exitUnavailable = 7
	exitCorrupt     = 8
)

func exitCode(err error) int {
	switch iface.CodeOf(err) {
	case iface.CodeNotFound:
		return exitNotFound
	case iface.CodeConflict:
		return exitConflict
	case iface.CodeQuota:
		return exitQuota
	case iface.CodeDenied:
		return exitDenied
	case iface.CodeUnavailable, iface.CodeRetry, iface.CodeNotLeader:
		return exitUnavailable
	case iface.CodeCorrupt:
		return exitCorrupt
	}
	return 1
}

// Diff is the chunk-level difference between two versions of a path. Chunks
// are compared by index and content ID, so an insertion shifts every later
// chunk: fixed-size chunking finds in-place edits, not moved data.
type Diff struct {
	Path        string `json:"path"`
	From        uint64 `json:"from"`
	To          uint64 `json:"to"`
	Same        int    `json:"same"`
	Changed     int    `json:"changed"`
	Added       int    `json:"added"`
	Removed     int    `json:"removed"`
	SharedBytes int64  `json:"sharedBytes"`
	// NewBytes is what a write of To would send on top of From: the bytes
	// of its changed and added chunks.
	NewBytes int64 `json:"newBytes"`
	Changes  []int `json:"changedIndexes,omitempty"`
}

func diffVersions(ctx context.Context, api client.API, path string, from, to uint64) (Diff, error) {
	a, err := api.StatVersion(ctx, path, from)
	if err != nil {
		return Diff{}, err
	}
	b, err := api.StatVersion(ctx, path, to)
	if err != nil {
		return Diff{}, err
	}
	return diffManifests(a, b), nil
}

func diffManifests(a, b client.Manifest) Diff {
	d := Diff{Path: b.Path, From: a.Version, To: b.Version}
	for i, c := range b.Chunk {
		switch {
		case i >= len(a.Chunk):
			d.Added++
			d.NewBytes += c.Size
			d.Changes = append(d.Changes, i)
		case a.Chunk[i].ID == c.ID:
			d.Same++
			d.SharedBytes += c.Size
		default:
			d.Changed++
			d.NewBytes += c.Size
			d.Changes = append(d.Changes, i)
		}
	}
	if n := len(a.Chunk) - len(b.Chunk); n > 0 {
		d.Removed = n
	}
	return d
}

func (p printer) diff(d Diff) {
	if p.json {
		p.print(d)
		return
	}
	fmt.Printf("%s v%d -> v%d: %d same, %d changed, %d added, %d removed\n", d.Path, d.From, d.To, d.Same, d.Changed, d.Added, d.Removed)
	fmt.Printf("shared %s; a write of v%d over v%d sends %s\n", human(d.SharedBytes), d.To, d.From, human(d.NewBytes))
	if len(d.Changes) > 0 {
		fmt.Printf("chunks to send: %v\n", d.Changes)
	}
}

// progress writes the bytes seen so far to stderr when it is a terminal.
type progress struct {
	n    int64
	last time.Time
	on   bool
}

func newProgress() *progress {
	fi, err := os.Stderr.Stat()
	return &progress{on: err == nil && fi.Mode()&os.ModeCharDevice != 0}
}

func (p *progress) Write(b []byte) (int, error) {
	p.n += int64(len(b))
	if p.on && time.Since(p.last) > 200*time.Millisecond {
		p.last = time.Now()
		fmt.Fprintf(os.Stderr, "\r%s", human(p.n))
	}
	return len(b), nil
}

func (p *progress) done() {
	if p.on {
		fmt.Fprintf(os.Stderr, "\r%s\n", human(p.n))
	}
}

// bench puts, gets and removes count random files of size bytes and reports
// the rates. A rough check of one deployment, not a benchmark: one client,
// one stream at a time.
func bench(ctx context.Context, api client.API, size int64, count int, opts client.PutOptions, out printer) error {
	type result struct {
		PutMiBs float64 `json:"putMiBs"`
		GetMiBs float64 `json:"getMiBs"`
	}
	var results []result
	prefix := "/bench-" + strconv.FormatInt(time.Now().Unix(), 36)
	for i := range count {
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			return err
		}
		path := fmt.Sprintf("%s/%d", prefix, i)
		t0 := time.Now()
		m, err := api.Put(ctx, path, bytes.NewReader(data), size, opts)
		if err != nil {
			return err
		}
		put := time.Since(t0)
		t0 = time.Now()
		if _, err := api.Get(ctx, path, io.Discard); err != nil {
			return err
		}
		get := time.Since(t0)
		if _, err := api.Delete(ctx, path, m.Version); err != nil {
			return err
		}
		mib := float64(size) / (1 << 20)
		results = append(results, result{mib / put.Seconds(), mib / get.Seconds()})
		if !out.json {
			fmt.Printf("run %d: put %.1f MiB/s, get %.1f MiB/s (%s)\n", i+1, mib/put.Seconds(), mib/get.Seconds(), human(size))
		}
	}
	if out.json {
		out.print(results)
	}
	return nil
}

const completionBash = `_chunkd() {
  local cur=${COMP_WORDS[COMP_CWORD]}
  if [ "$COMP_CWORD" -eq 1 ]; then
    COMPREPLY=($(compgen -W "put get log diff restore retain undelete ls stat rm cluster node keygen bench completion ping probe debug" -- "$cur"))
  else
    case ${COMP_WORDS[1]} in
      node) COMPREPLY=($(compgen -W "drain undrain decommission" -- "$cur")) ;;
      completion) COMPREPLY=($(compgen -W "bash zsh powershell" -- "$cur")) ;;
      *) COMPREPLY=($(compgen -f -- "$cur")) ;;
    esac
  fi
}
complete -F _chunkd chunkd
`

const completionZsh = `#compdef chunkd
_chunkd() {
  local -a cmds
  cmds=(put get log diff restore retain undelete ls stat rm cluster node keygen bench completion ping probe debug)
  if (( CURRENT == 2 )); then
    _describe command cmds
  elif [[ $words[2] == node ]]; then
    _values action drain undrain decommission
  elif [[ $words[2] == completion ]]; then
    _values shell bash zsh powershell
  else
    _files
  fi
}
compdef _chunkd chunkd
`

const completionPowerShell = `Register-ArgumentCompleter -Native -CommandName chunkd -ScriptBlock {
  param($word, $ast, $pos)
  $cmds = 'put','get','log','diff','restore','retain','undelete','ls','stat','rm','cluster','node','keygen','bench','completion','ping','probe','debug'
  $cmds | Where-Object { $_ -like "$word*" } | ForEach-Object { [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_) }
}
`

func completion(shell string) error {
	switch shell {
	case "bash":
		fmt.Print(completionBash)
	case "zsh":
		fmt.Print(completionZsh)
	case "powershell":
		fmt.Print(completionPowerShell)
	default:
		return fmt.Errorf("completion: want bash, zsh or powershell, got %q", shell)
	}
	return nil
}
