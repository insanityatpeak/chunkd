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
}
