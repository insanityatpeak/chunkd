// Package admin serves the operational HTTP endpoints every real-mode process
// exposes: /metrics (Prometheus text) and /healthz.
package admin

import (
	"net/http"

	"github.com/insanityatpeak/chunkd/internal/obs"
)

// Handler returns a mux serving /metrics from reg and /healthz from healthy.
// A nil healthy always reports 200.
func Handler(reg *obs.Registry, healthy func() error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = reg.WriteText(w)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if healthy != nil {
			if err := healthy(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
