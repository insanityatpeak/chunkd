// Package grpcnet implements iface.Transport over gRPC for real mode.
package grpcnet

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

const sendTimeout = 2 * time.Second

// Transport delivers messages with one unary RPC each. Like the sim network it
// is fire-and-forget: send errors are logged and the message is dropped, and
// core code recovers through its own retries and timeouts.
// SIMPLIFIED: no batching or per-peer ordering. GFS and HDFS keep long-lived
// streams per peer; a unary call per message is enough for heartbeats.
type Transport struct {
	rpcv1.UnimplementedTransportServiceServer

	selfAddr string
	loop     *runtime.Loop
	log      *slog.Logger

	mu       sync.Mutex
	addrs    map[iface.NodeID]string
	conns    map[string]*grpc.ClientConn
	handlers map[iface.NodeID]iface.Handler
}

var _ iface.Transport = (*Transport)(nil)

// New returns a transport that advertises selfAddr to peers and knows the
// given static peer addresses. Addresses of other senders are learned from
// incoming envelopes.
func New(selfAddr string, peers map[iface.NodeID]string, loop *runtime.Loop, log *slog.Logger) *Transport {
	t := &Transport{
		selfAddr: selfAddr,
		loop:     loop,
		log:      log,
		addrs:    map[iface.NodeID]string{},
		conns:    map[string]*grpc.ClientConn{},
		handlers: map[iface.NodeID]iface.Handler{},
	}
	for id, a := range peers {
		t.addrs[id] = a
	}
	return t
}

// Register attaches the transport's Deliver handler to s.
func (t *Transport) Register(s *grpc.Server) { rpcv1.RegisterTransportServiceServer(s, t) }

// Listen registers h for messages addressed to id.
func (t *Transport) Listen(id iface.NodeID, h iface.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = h
}

// Send delivers m asynchronously; it never blocks the event loop on the network.
func (t *Transport) Send(to iface.NodeID, m iface.Message) {
	m.To = to
	t.mu.Lock()
	addr, ok := t.addrs[to]
	t.mu.Unlock()
	if !ok {
		t.log.Warn("send to unknown node", "to", to, "kind", m.Kind)
		return
	}
	go func() {
		conn, err := t.conn(addr)
		if err != nil {
			t.log.Debug("dial failed", "to", to, "addr", addr, "err", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()
		_, err = rpcv1.NewTransportServiceClient(conn).Deliver(ctx, &chunkdv1.DeliverRequest{Envelope: &chunkdv1.Envelope{
			From: string(m.From), To: string(m.To), Kind: m.Kind, ReqId: m.ReqID, Body: m.Body, FromAddr: t.selfAddr,
		}})
		if err != nil {
			t.log.Debug("deliver failed", "to", to, "kind", m.Kind, "err", err)
		}
	}()
}

// Deliver is the gRPC handler. It queues the message on the loop and returns
// immediately, so a slow handler never holds an RPC open.
func (t *Transport) Deliver(_ context.Context, req *chunkdv1.DeliverRequest) (*chunkdv1.DeliverResponse, error) {
	e := req.GetEnvelope()
	m := iface.Message{From: iface.NodeID(e.GetFrom()), To: iface.NodeID(e.GetTo()), Kind: e.GetKind(), ReqID: e.GetReqId(), Body: e.GetBody()}
	t.mu.Lock()
	if e.GetFromAddr() != "" {
		t.addrs[m.From] = e.GetFromAddr()
	}
	h := t.handlers[m.To]
	t.mu.Unlock()
	if h == nil {
		t.log.Warn("message for unknown local node", "to", m.To, "kind", m.Kind)
		return &chunkdv1.DeliverResponse{}, nil
	}
	t.loop.Post(func() { h(m) })
	return &chunkdv1.DeliverResponse{}, nil
}

func (t *Transport) conn(addr string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[addr]; ok {
		return c, nil
	}
	// SIMPLIFIED: plaintext. Production clusters use mTLS between nodes.
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[addr] = c
	return c, nil
}

// Close releases all client connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	clear(t.conns)
}
