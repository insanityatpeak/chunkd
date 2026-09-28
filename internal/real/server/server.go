// Package server boots a real-mode chunkd process: logger, event loop, gRPC
// transport and ping service, admin HTTP endpoints, and graceful shutdown.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/obs"
	"github.com/insanityatpeak/chunkd/internal/real/admin"
	"github.com/insanityatpeak/chunkd/internal/real/grpcnet"
	"github.com/insanityatpeak/chunkd/internal/real/runtime"
	rpcv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/rpc/v1"
	chunkdv1 "github.com/insanityatpeak/chunkd/proto/gen/chunkd/v1"
)

// Config describes one process.
type Config struct {
	ID        iface.NodeID
	GRPCAddr  string // listen address; empty disables the gRPC server
	Advertise string // address peers use to reach this process
	AdminAddr string // /metrics and /healthz
	Peers     map[iface.NodeID]string
}

// Process is what a command's setup function wires core components into.
type Process struct {
	ID      iface.NodeID
	Log     *slog.Logger
	Loop    *runtime.Loop
	Clock   *runtime.Clock
	Rand    iface.Rand
	Net     *grpcnet.Transport
	Metrics *obs.Registry
	// Healthy backs /healthz; nil means always healthy.
	Healthy func() error
	// HTTP, if set, is mounted at / on the admin server next to /metrics and /healthz.
	HTTP http.Handler
}

// Run builds the process, calls setup before any server starts (so setup may
// touch core components directly), then serves until SIGINT or SIGTERM.
func Run(component string, cfg Config, setup func(*Process) error) error {
	log := obs.NewLogger(os.Stdout, component, slog.LevelInfo).With("self", cfg.ID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loop := runtime.NewLoop()
	p := &Process{
		ID:      cfg.ID,
		Log:     log,
		Loop:    loop,
		Clock:   runtime.NewClock(loop),
		Rand:    runtime.NewRand(),
		Net:     grpcnet.New(cfg.Advertise, cfg.Peers, loop, log),
		Metrics: obs.NewRegistry(),
	}
	defer p.Net.Close()
	p.Metrics.Gauge("chunkd_up", "1 while the process is running.").Set(1)
	if err := setup(p); err != nil {
		return err
	}
	go loop.Run(ctx)

	errc := make(chan error, 2)
	var gs *grpc.Server
	if cfg.GRPCAddr != "" {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			return err
		}
		gs = grpc.NewServer(grpc.UnaryInterceptor(requestIDInterceptor), grpc.MaxRecvMsgSize(grpcnet.MaxUnary), grpc.MaxSendMsgSize(grpcnet.MaxUnary))
		p.Net.Register(gs)
		rpcv1.RegisterPingServiceServer(gs, pingServer{id: cfg.ID})
		go func() { errc <- gs.Serve(lis) }()
		log.Info("grpc listening", "addr", cfg.GRPCAddr, "advertise", cfg.Advertise)
	}

	mux := http.NewServeMux()
	ops := admin.Handler(p.Metrics, p.Healthy)
	mux.Handle("/metrics", ops)
	mux.Handle("/healthz", ops)
	if p.HTTP != nil {
		mux.Handle("/", p.HTTP)
	}
	hs := &http.Server{Addr: cfg.AdminAddr, Handler: withRequestID(mux, log), ReadHeaderTimeout: 5 * time.Second}
	go func() { errc <- hs.ListenAndServe() }()
	log.Info("http listening", "addr", cfg.AdminAddr)

	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	if gs != nil {
		gs.GracefulStop()
	}
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
		err = nil
	}
	return err
}

// ParsePeers parses "id=host:port,id=host:port".
func ParsePeers(s string) (map[iface.NodeID]string, error) {
	out := map[iface.NodeID]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		out[iface.NodeID(id)] = addr
	}
	return out, nil
}

// Env returns the environment variable key, or def if unset. Flags default to
// these so compose files can configure processes without long command lines.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

type pingServer struct {
	rpcv1.UnimplementedPingServiceServer
	id iface.NodeID
}

func (s pingServer) Ping(_ context.Context, req *chunkdv1.PingRequest) (*chunkdv1.PingResponse, error) {
	return &chunkdv1.PingResponse{From: string(s.id), Seq: req.GetSeq()}, nil
}

const requestIDHeader = "x-request-id"

// NewRequestID returns a random 16-hex-digit ID.
func NewRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func withRequestID(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = NewRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		ctx := obs.WithRequestID(r.Context(), id)
		if r.URL.Path != "/metrics" && r.URL.Path != "/healthz" {
			log.InfoContext(ctx, "http request", "method", r.Method, "path", r.URL.Path)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(requestIDHeader); len(v) > 0 {
			ctx = obs.WithRequestID(ctx, v[0])
		}
	}
	return h(ctx, req)
}

// OutgoingContext copies ctx's request ID into gRPC metadata for a client call.
func OutgoingContext(ctx context.Context) context.Context {
	if id := obs.RequestID(ctx); id != "" {
		return metadata.AppendToOutgoingContext(ctx, requestIDHeader, id)
	}
	return ctx
}
