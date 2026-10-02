package grpcnet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// A connection that reaches a process not hosting the addressee is refused
// and dropped, so the next dial re-resolves the name. In compose, a node
// container recreated with a new IP left its old IP to another node; the
// metadata server's pooled connection still worked, so repair copies and
// heartbeat acks for one node went to another, which dropped them, and a
// repair copy timed out every 10 s for good.
func TestMisroutedConnectionIsDropped(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop := runtime.NewLoop()
	go loop.Run(ctx)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(addr, nil, loop, quiet)
	defer srv.Close()
	// This process is node-b only.
	srv.Listen("node-b", func(iface.Message) {})
	srv.Serve("node-b", "ping", func(_ iface.Message, respond iface.Responder) { respond([]byte("pong"), nil) }, iface.ServeOpts{Concurrent: true})
	s := grpc.NewServer()
	srv.Register(s)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	// The caller believes node-a lives at node-b's address.
	caller := NewCaller(map[iface.NodeID]string{"node-a": addr, "node-b": addr}, 2*time.Second)
	defer caller.Close()
	pooled := func(p *pool) *grpc.ClientConn {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.conns[addr]
	}

	if r := caller.Do(ctx, []iface.Call{{To: "node-b", Kind: "ping"}})[0]; r.Err != nil || string(r.Body) != "pong" {
		t.Fatalf("call to node-b = %q, %v", r.Body, r.Err)
	}
	first := pooled(caller.pool)
	// An unknown kind on a hosted node is the handler's error, not a misroute.
	if r := caller.Do(ctx, []iface.Call{{To: "node-b", Kind: "nope"}})[0]; iface.CodeOf(r.Err) != iface.CodeInvalid {
		t.Fatalf("unknown kind on node-b: %v, want invalid", r.Err)
	}
	if pooled(caller.pool) != first {
		t.Fatal("a handler error dropped the connection")
	}
	// Unary, streamed, and streamed with a body of several frames: the server
	// refuses after the first frame, so the client's next Send sees io.EOF and
	// must fetch the status to recognise the refusal.
	for _, c := range []struct {
		kind string
		body []byte
	}{{"ping", nil}, {"chunk.get", nil}, {"chunk.put", make([]byte, 4<<20)}} {
		kind := c.kind
		if r := caller.Do(ctx, []iface.Call{{To: "node-a", Kind: kind, Body: c.body}})[0]; iface.CodeOf(r.Err) != iface.CodeUnavailable || !strings.Contains(r.Err.Error(), "misrouted") {
			t.Fatalf("%s to node-a at node-b's address: %v, want unavailable, misrouted", kind, r.Err)
		}
		if c := pooled(caller.pool); c != nil {
			t.Fatalf("%s: the misrouted connection is still pooled", kind)
		}
		if r := caller.Do(ctx, []iface.Call{{To: "node-b", Kind: "ping"}})[0]; r.Err != nil {
			t.Fatalf("node-b after the eviction: %v", r.Err)
		}
	}

	// One-way messages: the sender's transport drops the connection too.
	sender := New("127.0.0.1:1", map[iface.NodeID]string{"node-a": addr}, loop, quiet)
	defer sender.Close()
	if _, err := sender.pool.get(addr); err != nil { // pooled before the send, so its removal is the evidence
		t.Fatal(err)
	}
	sender.Send("node-a", iface.Message{From: "meta-1", Kind: "node.heartbeat_ack"})
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		sender.pool.mu.Lock()
		_, held := sender.pool.conns[addr]
		sender.pool.mu.Unlock()
		if !held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a misrouted Deliver left the connection pooled")
		}
	}
}

type recvOnly struct{ err error }

func (r recvOnly) Recv() (*chunkdv1.CallStreamResponse, error) { return nil, r.err }

// A streamed call the server refuses mid-body fails its next Send with a
// bare io.EOF; the refusal is only in Recv. Over loopback the sends finish
// first, so this is tested directly: in compose, chunk puts to misrouted
// nodes failed with "EOF" and were never retried.
func TestStreamSendEOFYieldsServerStatus(t *testing.T) {
	refusal := misrouted("node-a")
	got := sendFailed(recvOnly{refusal}, io.EOF)
	if !isMisrouted(got) {
		t.Fatalf("sendFailed(EOF) = %v, want the misroute refusal", got)
	}
	other := errors.New("reset")
	if got := sendFailed(recvOnly{refusal}, other); got != other {
		t.Fatalf("a non-EOF send error became %v", got)
	}
}
