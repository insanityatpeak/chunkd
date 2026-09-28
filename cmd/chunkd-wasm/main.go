//go:build js && wasm

// Command chunkd-wasm runs the simulated cluster inside a browser Web Worker.
// It exposes a global `chunkd` object with start(seed), tick(ms) and state();
// state() returns JSON. The worker drives tick in fixed steps so a seed
// always yields the same state sequence.
package main

import (
	"encoding/json"
	"io"
	"syscall/js"
	"time"

	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

func main() {
	var c *cluster.Cluster

	api := map[string]any{
		"start": js.FuncOf(func(_ js.Value, args []js.Value) any {
			seed := uint64(1)
			if len(args) > 0 && args[0].Type() == js.TypeNumber {
				seed = uint64(args[0].Float())
			}
			c = cluster.New(seed, cluster.DefaultConfig(), io.Discard)
			return nil
		}),
		"tick": js.FuncOf(func(_ js.Value, args []js.Value) any {
			if c != nil && len(args) > 0 {
				c.Tick(time.Duration(args[0].Int()) * time.Millisecond)
			}
			return nil
		}),
		"state": js.FuncOf(func(js.Value, []js.Value) any {
			if c == nil {
				return js.Null()
			}
			b, err := json.Marshal(c.State())
			if err != nil {
				return js.Null()
			}
			return string(b)
		}),
	}
	js.Global().Set("chunkd", js.ValueOf(api))
	select {} // keep the Go runtime alive for callbacks
}
