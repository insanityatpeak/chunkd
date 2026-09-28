package ifacetest

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// RPCEnv is one run mode's wiring for the RPC conformance suite.
type RPCEnv interface {
	// Server returns the transport for a server process with this id. Calls
	// to handlers registered on it must be reachable at Addr(id).
	Server(id iface.NodeID) iface.Transport
	Addr(id iface.NodeID) string
	// Clock is the clock handlers use to respond later.
	Clock(id iface.NodeID) iface.Clock
	// Caller must time out unanswered calls within about one second.
	Caller() iface.Caller
	// Async returns an AsyncCaller for handlers running on server id's
	// loop, with a timeout of about 300 ms (well inside Caller's).
	Async(id iface.NodeID) iface.AsyncCaller
}

// RPC runs the request/response conformance suite.
func RPC(t *testing.T, newEnv func(t *testing.T) RPCEnv) {
	ctx := context.Background()

	echo := func(m iface.Message, respond iface.Responder) { respond(m.Body, nil) }

	t.Run("round trip", func(t *testing.T) {
		env := newEnv(t)
		env.Server("s1").Serve("s1", "test.echo", echo, iface.ServeOpts{})
		r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.echo", Body: []byte("ping")}})
		if r[0].Err != nil || string(r[0].Body) != "ping" {
			t.Fatalf("got %q, %v", r[0].Body, r[0].Err)
		}
	})

	t.Run("error codes cross the wire", func(t *testing.T) {
		env := newEnv(t)
		srv := env.Server("s1")
		for _, c := range []iface.Code{iface.CodeNotFound, iface.CodeConflict, iface.CodeRetry, iface.CodeInvalid} {
			srv.Serve("s1", "test.fail."+c.String(), func(_ iface.Message, respond iface.Responder) {
				respond(nil, iface.Errorf(c, "because %s", c))
			}, iface.ServeOpts{})
		}
		srv.Serve("s1", "test.plain", func(_ iface.Message, respond iface.Responder) {
			respond(nil, fmt.Errorf("uncoded"))
		}, iface.ServeOpts{})
		for _, c := range []iface.Code{iface.CodeNotFound, iface.CodeConflict, iface.CodeRetry, iface.CodeInvalid} {
			r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.fail." + c.String()}})
			if iface.CodeOf(r[0].Err) != c {
				t.Fatalf("want %v, got %v", c, r[0].Err)
			}
		}
		r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.plain"}})
		if iface.CodeOf(r[0].Err) != iface.CodeInternal {
			t.Fatalf("uncoded error arrived as %v, want internal", r[0].Err)
		}
	})

	t.Run("unknown kind", func(t *testing.T) {
		env := newEnv(t)
		env.Server("s1").Serve("s1", "test.echo", echo, iface.ServeOpts{})
		r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.nope"}})
		if r[0].Err == nil {
			t.Fatal("call to an unregistered kind succeeded")
		}
	})

	t.Run("large bodies both ways", func(t *testing.T) {
		env := newEnv(t)
		env.Server("s1").Serve("s1", "chunk.echo", echo, iface.ServeOpts{Concurrent: true})
		for _, n := range []int{0, 1, 4<<20 - 1, 4<<20 + 1} {
			body := bytes.Repeat([]byte{byte(n)}, n)
			r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "chunk.echo", Body: body}})
			if r[0].Err != nil || !bytes.Equal(r[0].Body, body) {
				t.Fatalf("%d bytes: got %d bytes, %v", n, len(r[0].Body), r[0].Err)
			}
		}
	})

	t.Run("batch results in call order", func(t *testing.T) {
		env := newEnv(t)
		for _, id := range []iface.NodeID{"s1", "s2", "s3"} {
			env.Server(id).Serve(id, "test.whoami", func(m iface.Message, respond iface.Responder) {
				respond([]byte(string(m.To)+":"+string(m.Body)), nil)
			}, iface.ServeOpts{})
		}
		var calls []iface.Call
		for i := range 9 {
			id := iface.NodeID(fmt.Sprintf("s%d", 3-i%3))
			calls = append(calls, iface.Call{To: id, Addr: env.Addr(id), Kind: "test.whoami", Body: []byte(fmt.Sprint(i))})
		}
		for i, r := range env.Caller().Do(ctx, calls) {
			if want := fmt.Sprintf("%s:%d", calls[i].To, i); r.Err != nil || string(r.Body) != want {
				t.Fatalf("result %d = %q, %v; want %q", i, r.Body, r.Err, want)
			}
		}
	})

	t.Run("deferred response", func(t *testing.T) {
		env := newEnv(t)
		clk := env.Clock("s1")
		env.Server("s1").Serve("s1", "test.later", func(m iface.Message, respond iface.Responder) {
			clk.AfterFunc(20*time.Millisecond, func() { respond([]byte("later"), nil) })
		}, iface.ServeOpts{})
		r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.later"}})
		if r[0].Err != nil || string(r[0].Body) != "later" {
			t.Fatalf("got %q, %v", r[0].Body, r[0].Err)
		}
	})

	t.Run("no response times out as unavailable", func(t *testing.T) {
		env := newEnv(t)
		env.Server("s1").Serve("s1", "test.silent", func(iface.Message, iface.Responder) {}, iface.ServeOpts{})
		r := env.Caller().Do(ctx, []iface.Call{{To: "s1", Addr: env.Addr("s1"), Kind: "test.silent"}})
		if iface.CodeOf(r[0].Err) != iface.CodeUnavailable {
			t.Fatalf("err = %v, want unavailable", r[0].Err)
		}
	})

	t.Run("unreachable node is unavailable", func(t *testing.T) {
		env := newEnv(t)
		r := env.Caller().Do(ctx, []iface.Call{{To: "ghost", Addr: "127.0.0.1:1", Kind: "test.echo"}})
		if iface.CodeOf(r[0].Err) != iface.CodeUnavailable {
			t.Fatalf("err = %v, want unavailable", r[0].Err)
		}
	})

	t.Run("hedge", func(t *testing.T) {
		env := newEnv(t)
		for _, id := range []iface.NodeID{"fast", "slow", "bad", "down"} {
			clk := env.Clock(id)
			env.Server(id).Serve(id, "test.hedge", func(m iface.Message, respond iface.Responder) {
				switch m.To {
				case "slow":
					clk.AfterFunc(400*time.Millisecond, func() { respond([]byte("ok"), nil) })
				case "bad":
					respond([]byte("corrupt"), nil)
				case "down":
					respond(nil, iface.Errorf(iface.CodeInternal, "disk"))
				default:
					respond([]byte("ok"), nil)
				}
			}, iface.ServeOpts{})
		}
		mk := func(ids ...iface.NodeID) []iface.Call {
			var out []iface.Call
			for _, id := range ids {
				out = append(out, iface.Call{To: id, Addr: env.Addr(id), Kind: "test.hedge"})
			}
			return out
		}
		okBody := func(_ int, r iface.Result) bool { return string(r.Body) == "ok" }
		const long = time.Hour // a hedge that must never be what launched the next call

		tests := []struct {
			name         string
			calls        []iface.Call
			after        time.Duration
			winner       int
			launched     int
			firstPending bool
		}{
			{"fast first: no hedge sent", mk("fast", "slow"), 100 * time.Millisecond, 0, 1, false},
			{"slow first: hedge wins", mk("slow", "fast"), 50 * time.Millisecond, 1, 2, true},
			{"error: next sent at once", mk("down", "fast"), long, 1, 2, false},
			{"rejected answer: next sent at once", mk("bad", "fast"), long, 1, 2, false},
			{"nothing acceptable", mk("bad", "down"), long, -1, 2, false},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				h := env.Caller().Hedge(ctx, tc.calls, tc.after, okBody)
				if h.Winner != tc.winner || h.Launched != tc.launched {
					t.Fatalf("winner %d launched %d, want %d and %d (%+v)", h.Winner, h.Launched, tc.winner, tc.launched, h.Results)
				}
				if h.Results[0].Pending != tc.firstPending {
					t.Fatalf("first call pending = %v, want %v", h.Results[0].Pending, tc.firstPending)
				}
				if tc.winner >= 0 && h.Results[tc.winner].Latency >= 400*time.Millisecond {
					t.Fatalf("winner latency %v", h.Results[tc.winner].Latency)
				}
			})
		}
	})

	t.Run("async call from inside a handler", func(t *testing.T) {
		env := newEnv(t)
		env.Server("s1").Serve("s1", "test.echo", echo, iface.ServeOpts{})
		env.Server("s1").Serve("s1", "test.silent", func(iface.Message, iface.Responder) {}, iface.ServeOpts{})
		async := env.Async("s2")
		// s2 relays each request to s1 with the async caller and answers
		// from the callback, which must run on s2's loop.
		env.Server("s2").Serve("s2", "test.relay", func(m iface.Message, respond iface.Responder) {
			async.Go(iface.Call{To: "s1", Addr: env.Addr("s1"), Kind: string(m.Body), Body: []byte("via s2")}, func(r iface.Result) {
				if r.Err != nil {
					respond([]byte(iface.CodeOf(r.Err).String()), nil)
					return
				}
				respond(r.Body, nil)
			})
		}, iface.ServeOpts{})
		relay := func(kind string) string {
			r := env.Caller().Do(ctx, []iface.Call{{To: "s2", Addr: env.Addr("s2"), Kind: "test.relay", Body: []byte(kind)}})
			if r[0].Err != nil {
				t.Fatalf("relay %s: %v", kind, r[0].Err)
			}
			return string(r[0].Body)
		}
		if got := relay("test.echo"); got != "via s2" {
			t.Fatalf("echo through async = %q", got)
		}
		if got := relay("test.silent"); got != iface.CodeUnavailable.String() {
			t.Fatalf("silent peer through async = %q, want unavailable", got)
		}
	})
}
