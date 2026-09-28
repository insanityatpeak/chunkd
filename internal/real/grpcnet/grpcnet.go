// Package grpcnet implements iface.Transport and iface.Caller over gRPC for
// real mode.
package grpcnet

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

const (
	sendTimeout = 2 * time.Second
	// FrameSize is the body size per CallStream message.
	FrameSize = 1 << 20
	// maxUnary caps control messages; larger bodies must use CallStream.
	MaxUnary = 16 << 20
)

// Streamed reports whether a call of this kind and body size uses
// CallStream. Chunk transfers always stream because responses are large too.
func Streamed(kind string, bodyLen int) bool {
	return strings.HasPrefix(kind, "chunk.") || bodyLen > FrameSize
}

// Transport delivers one-way messages with one Deliver RPC each and serves
// request/response calls. Send is fire-and-forget like the sim network:
// errors are logged and the message is dropped.
// SIMPLIFIED: no batching or per-peer ordering for one-way messages. GFS and
// HDFS keep long-lived streams per peer; a unary call per message is enough
// for heartbeats and block reports.
type Transport struct {
	rpcv1.UnimplementedTransportServiceServer

	selfAddr string
	loop     *runtime.Loop
	log      *slog.Logger
	pool     *pool

	mu       sync.Mutex
	addrs    map[iface.NodeID]string
	handlers map[iface.NodeID]iface.Handler
	rpcs     map[rpcKey]served
}

type rpcKey struct {
	id   iface.NodeID
	kind string
}

type served struct {
	h    iface.RPCHandler
	opts iface.ServeOpts
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
		pool:     newPool(),
		addrs:    map[iface.NodeID]string{},
		handlers: map[iface.NodeID]iface.Handler{},
		rpcs:     map[rpcKey]served{},
	}
	for id, a := range peers {
		t.addrs[id] = a
	}
	return t
}

// Register attaches the transport's handlers to s.
func (t *Transport) Register(s *grpc.Server) { rpcv1.RegisterTransportServiceServer(s, t) }

// Listen registers h for one-way messages addressed to id.
func (t *Transport) Listen(id iface.NodeID, h iface.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = h
}

// Serve registers an RPC handler for (id, kind).
func (t *Transport) Serve(id iface.NodeID, kind string, h iface.RPCHandler, opts iface.ServeOpts) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rpcs[rpcKey{id, kind}] = served{h, opts}
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
		conn, err := t.pool.get(addr)
		if err != nil {
			t.log.Debug("dial failed", "to", to, "addr", addr, "err", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		defer cancel()
		env := toEnvelope(m)
		env.FromAddr = t.selfAddr
		if _, err := rpcv1.NewTransportServiceClient(conn).Deliver(ctx, &chunkdv1.DeliverRequest{Envelope: env}); err != nil {
			t.log.Debug("deliver failed", "to", to, "kind", m.Kind, "err", err)
		}
	}()
}

// Deliver is the gRPC handler for one-way messages. It queues the message on
// the loop and returns immediately.
func (t *Transport) Deliver(_ context.Context, req *chunkdv1.DeliverRequest) (*chunkdv1.DeliverResponse, error) {
	m := fromEnvelope(req.GetEnvelope())
	t.mu.Lock()
	if a := req.GetEnvelope().GetFromAddr(); a != "" {
		t.addrs[m.From] = a
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

// Call serves a unary request.
func (t *Transport) Call(ctx context.Context, req *chunkdv1.CallRequest) (*chunkdv1.CallResponse, error) {
	body, err := t.dispatch(ctx, fromEnvelope(req.GetEnvelope()))
	return &chunkdv1.CallResponse{Envelope: responseEnvelope(body, err)}, nil
}

// CallStream serves a framed request and streams the response back.
func (t *Transport) CallStream(stream rpcv1.TransportService_CallStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	m := fromEnvelope(first.GetHeader())
	body := append([]byte(nil), first.GetData()...)
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		body = append(body, f.GetData()...)
	}
	m.Body = body
	resp, callErr := t.dispatch(stream.Context(), m)
	if err := stream.Send(&chunkdv1.CallStreamResponse{Header: responseEnvelope(nil, callErr)}); err != nil {
		return err
	}
	for off := 0; off < len(resp); off += FrameSize {
		if err := stream.Send(&chunkdv1.CallStreamResponse{Data: resp[off:min(off+FrameSize, len(resp))]}); err != nil {
			return err
		}
	}
	return nil
}

// dispatch runs the handler for m and waits for its response.
func (t *Transport) dispatch(ctx context.Context, m iface.Message) ([]byte, error) {
	t.mu.Lock()
	s, ok := t.rpcs[rpcKey{m.To, m.Kind}]
	t.mu.Unlock()
	if !ok {
		return nil, iface.Errorf(iface.CodeInvalid, "no handler for %s on %s", m.Kind, m.To)
	}
	type reply struct {
		body []byte
		err  error
	}
	done := make(chan reply, 1)
	var once sync.Once
	respond := func(body []byte, err error) { once.Do(func() { done <- reply{body, err} }) }
	if s.opts.Concurrent {
		s.h(m, respond)
	} else {
		t.loop.Post(func() { s.h(m, respond) })
	}
	select {
	case r := <-done:
		return r.body, r.err
	case <-ctx.Done():
		return nil, iface.Errorf(iface.CodeUnavailable, "%s: %v", m.Kind, ctx.Err())
	}
}

// Close releases client connections.
func (t *Transport) Close() { t.pool.close() }

func toEnvelope(m iface.Message) *chunkdv1.Envelope {
	e := &chunkdv1.Envelope{From: string(m.From), To: string(m.To), Kind: m.Kind, ReqId: m.ReqID, Body: m.Body}
	if m.Err != nil {
		e.ErrCode, e.ErrMsg = uint32(m.Err.Code), m.Err.Msg
	}
	return e
}

func fromEnvelope(e *chunkdv1.Envelope) iface.Message {
	m := iface.Message{From: iface.NodeID(e.GetFrom()), To: iface.NodeID(e.GetTo()), Kind: e.GetKind(), ReqID: e.GetReqId(), Body: e.GetBody()}
	if e.GetErrCode() != 0 {
		m.Err = &iface.Error{Code: iface.Code(e.GetErrCode()), Msg: e.GetErrMsg()}
	}
	return m
}

func responseEnvelope(body []byte, err error) *chunkdv1.Envelope {
	e := &chunkdv1.Envelope{Body: body}
	if err != nil {
		ie := iface.AsError(err, iface.CodeInternal)
		e.Body, e.ErrCode, e.ErrMsg = nil, uint32(ie.Code), ie.Msg
	}
	return e
}

// pool caches one client connection per address.
type pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPool() *pool { return &pool{conns: map[string]*grpc.ClientConn{}} }

func (p *pool) get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	// SIMPLIFIED: plaintext. Production clusters use mTLS between nodes.
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxUnary), grpc.MaxCallSendMsgSize(MaxUnary)))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

// Caller is the real-mode iface.Caller: one goroutine per call in a batch.
type Caller struct {
	pool    *pool
	timeout time.Duration
	mu      sync.Mutex
	addrs   map[iface.NodeID]string
}

var _ iface.Caller = (*Caller)(nil)

// NewCaller returns a caller with static addresses for known nodes (the
// metadata server); addresses of storage nodes arrive in Call.Addr.
func NewCaller(peers map[iface.NodeID]string, timeout time.Duration) *Caller {
	c := &Caller{pool: newPool(), timeout: timeout, addrs: map[iface.NodeID]string{}}
	for id, a := range peers {
		c.addrs[id] = a
	}
	return c
}

// Close releases connections.
func (c *Caller) Close() { c.pool.close() }

// Do runs the calls concurrently and returns results in call order.
func (c *Caller) Do(ctx context.Context, calls []iface.Call) []iface.Result {
	out := make([]iface.Result, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			out[i] = c.one(ctx, call)
			out[i].Latency = time.Since(start)
		}()
	}
	wg.Wait()
	return out
}

// Async returns an iface.AsyncCaller whose callbacks run on loop. Each call
// runs on its own goroutine, so a slow peer never blocks the loop.
func (c *Caller) Async(loop *runtime.Loop) iface.AsyncCaller { return &async{c: c, loop: loop} }

type async struct {
	c    *Caller
	loop *runtime.Loop
}

func (a *async) Go(call iface.Call, cb func(iface.Result)) {
	go func() {
		r := a.c.Do(context.Background(), []iface.Call{call})[0]
		a.loop.Post(func() { cb(r) })
	}()
}

// Hedge implements iface.Caller. Losing calls are cancelled when it returns.
func (c *Caller) Hedge(ctx context.Context, calls []iface.Call, after time.Duration, accept func(int, iface.Result) bool) iface.HedgeResult {
	out := iface.HedgeResult{Winner: -1, Results: make([]iface.Result, len(calls))}
	if len(calls) == 0 {
		return out
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type done struct {
		i int
		r iface.Result
	}
	ch := make(chan done, len(calls)) // buffered: losers finish after we return
	starts := make([]time.Time, len(calls))
	settled := make([]bool, len(calls))
	finished := 0
	timer := time.NewTimer(after)
	defer timer.Stop()
	launch := func() {
		i := out.Launched
		out.Launched++
		starts[i] = time.Now()
		go func() {
			r := c.one(ctx, calls[i])
			r.Latency = time.Since(starts[i])
			ch <- done{i, r}
		}()
		timer.Reset(after)
	}
	launch()
loop:
	for {
		select {
		case d := <-ch:
			out.Results[d.i], settled[d.i] = d.r, true
			finished++
			if d.r.Err == nil && accept(d.i, d.r) {
				out.Winner = d.i
				break loop
			}
			if finished == out.Launched {
				if out.Launched == len(calls) {
					break loop
				}
				launch()
			}
		case <-timer.C:
			if out.Launched < len(calls) {
				launch()
			}
		case <-ctx.Done():
			break loop
		}
	}
	for i := range out.Launched {
		if !settled[i] {
			out.Results[i] = iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "%s to %s: abandoned", calls[i].Kind, calls[i].To),
				Latency: time.Since(starts[i]), Pending: true}
		}
	}
	return out
}

func (c *Caller) one(ctx context.Context, call iface.Call) iface.Result {
	addr := call.Addr
	c.mu.Lock()
	if addr == "" {
		addr = c.addrs[call.To]
	} else {
		c.addrs[call.To] = addr
	}
	c.mu.Unlock()
	if addr == "" {
		return iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "no address for %s", call.To)}
	}
	conn, err := c.pool.get(addr)
	if err != nil {
		return iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "dial %s: %v", addr, err)}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	client := rpcv1.NewTransportServiceClient(conn)
	env := &chunkdv1.Envelope{To: string(call.To), Kind: call.Kind}

	var resp *chunkdv1.Envelope
	var body []byte
	if Streamed(call.Kind, len(call.Body)) {
		resp, body, err = stream(ctx, client, env, call.Body)
	} else {
		env.Body = call.Body
		var r *chunkdv1.CallResponse
		r, err = client.Call(ctx, &chunkdv1.CallRequest{Envelope: env})
		resp, body = r.GetEnvelope(), r.GetEnvelope().GetBody()
	}
	if err != nil {
		return iface.Result{Err: iface.Errorf(iface.CodeUnavailable, "%s to %s: %v", call.Kind, call.To, err)}
	}
	if resp.GetErrCode() != 0 {
		return iface.Result{Err: &iface.Error{Code: iface.Code(resp.GetErrCode()), Msg: resp.GetErrMsg()}}
	}
	return iface.Result{Body: body}
}

func stream(ctx context.Context, client rpcv1.TransportServiceClient, env *chunkdv1.Envelope, body []byte) (*chunkdv1.Envelope, []byte, error) {
	s, err := client.CallStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := s.Send(&chunkdv1.CallStreamRequest{Header: env}); err != nil {
		return nil, nil, err
	}
	for off := 0; off < len(body); off += FrameSize {
		if err := s.Send(&chunkdv1.CallStreamRequest{Data: body[off:min(off+FrameSize, len(body))]}); err != nil {
			return nil, nil, err
		}
	}
	if err := s.CloseSend(); err != nil {
		return nil, nil, err
	}
	first, err := s.Recv()
	if err != nil {
		return nil, nil, err
	}
	var out []byte
	for {
		f, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return first.GetHeader(), out, nil
		}
		if err != nil {
			return nil, nil, err
		}
		out = append(out, f.GetData()...)
	}
}
