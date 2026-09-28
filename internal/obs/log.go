// Package obs holds logging and metrics setup shared by every run mode. It
// has no network or OS dependencies so core and sim code can use it.
package obs

import (
	"context"
	"io"
	"log/slog"
)

type ctxKey struct{}

// WithRequestID returns a context carrying id; loggers built by NewLogger add
// it to every record logged with that context.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// RequestID returns the request ID in ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// NewLogger returns a JSON logger tagged with the component name.
func NewLogger(w io.Writer, component string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(requestIDHandler{h}).With("component", component)
}

type requestIDHandler struct{ slog.Handler }

func (h requestIDHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h requestIDHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return requestIDHandler{h.Handler.WithAttrs(as)}
}

func (h requestIDHandler) WithGroup(name string) slog.Handler {
	return requestIDHandler{h.Handler.WithGroup(name)}
}
