//go:build js && wasm

// Command chunkd-wasm runs the simulated cluster inside a browser Web Worker.
// It exposes a global `chunkd` object:
//
//	start(seed)             build a cluster
//	tick(ms)                advance simulated time
//	state(afterSeq)         JSON snapshot with timeline events after afterSeq
//	upload(path, bytes)     JSON manifest; runs the real client, advancing sim time
//	download(path)          {manifest: JSON, data: Uint8Array}; verified by the client
//	stat(path)              JSON manifest with replica locations
//	remove(path)            JSON {version}
//	crash(node), restart(node)  kill a node's process (disk kept); start a new one
//	scenario(node, downMs)  load demo files if empty, kill node, restart it after downMs
//	rot(node, n)            load demo files if empty, flip bytes in n chunks on node's disk
//
// Errors come back as {"error": "..."} JSON. The worker only calls tick with
// a fixed step, so a seed always yields the same state sequence.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"syscall/js"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/sim/cluster"
)

var c *cluster.Cluster

func jsonValue(v any, err error) any {
	if err != nil {
		return errorValue(err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return errorValue(err)
	}
	return string(b)
}

func errorValue(err error) any {
	b, _ := json.Marshal(map[string]string{"error": err.Error(), "code": iface.CodeOf(err).String()})
	return string(b)
}

func needCluster(f func(args []js.Value) any) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if c == nil {
			return errorValue(iface.Errorf(iface.CodeUnavailable, "cluster not started"))
		}
		return f(args)
	})
}

func main() {
	ctx := context.Background()
	api := map[string]any{
		"start": js.FuncOf(func(_ js.Value, args []js.Value) any {
			seed := uint64(1)
			if len(args) > 0 && args[0].Type() == js.TypeNumber {
				seed = uint64(args[0].Float())
			}
			c = cluster.New(seed, cluster.DefaultConfig(), io.Discard)
			return nil
		}),
		"tick": needCluster(func(args []js.Value) any {
			c.Tick(time.Duration(args[0].Int()) * time.Millisecond)
			return nil
		}),
		"state": needCluster(func(args []js.Value) any {
			var after uint64
			if len(args) > 0 && args[0].Type() == js.TypeNumber {
				after = uint64(args[0].Float())
			}
			return jsonValue(c.StateSince(after), nil)
		}),
		"upload": needCluster(func(args []js.Value) any {
			data := make([]byte, args[1].Get("length").Int())
			js.CopyBytesToGo(data, args[1])
			m, err := c.Client().Put(ctx, args[0].String(), bytes.NewReader(data), int64(len(data)), client.PutOptions{Overwrite: true})
			return jsonValue(m, err)
		}),
		"download": needCluster(func(args []js.Value) any {
			var buf bytes.Buffer
			m, err := c.Client().Get(ctx, args[0].String(), &buf)
			out := js.Global().Get("Object").New()
			out.Set("manifest", jsonValue(m, err))
			if err == nil {
				arr := js.Global().Get("Uint8Array").New(buf.Len())
				js.CopyBytesToJS(arr, buf.Bytes())
				out.Set("data", arr)
			}
			return out
		}),
		"stat": needCluster(func(args []js.Value) any { return jsonValue(c.Client().Stat(ctx, args[0].String())) }),
		"remove": needCluster(func(args []js.Value) any {
			v, err := c.Client().Delete(ctx, args[0].String(), 0)
			return jsonValue(map[string]uint64{"version": v}, err)
		}),
		"crash": needCluster(func(args []js.Value) any {
			c.KillNode(iface.NodeID(args[0].String()))
			return nil
		}),
		"restart": needCluster(func(args []js.Value) any {
			c.RestartNode(iface.NodeID(args[0].String()))
			return nil
		}),
		"rot": needCluster(func(args []js.Value) any {
			ids, err := c.ScriptRot(iface.NodeID(args[0].String()), args[1].Int())
			return jsonValue(map[string]int{"rotted": len(ids)}, err)
		}),
		"scenario": needCluster(func(args []js.Value) any {
			err := c.ScriptKillNode(iface.NodeID(args[0].String()), time.Duration(args[1].Int())*time.Millisecond)
			return jsonValue(struct{}{}, err)
		}),
	}
	js.Global().Set("chunkd", js.ValueOf(api))
	select {} // keep the Go runtime alive for callbacks
}
