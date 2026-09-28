package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/insanityatpeak/chunkd/internal/obs"
)

func TestHandler(t *testing.T) {
	reg := obs.NewRegistry()
	reg.Counter("chunkd_up", "Process is up.").Inc()

	tests := []struct {
		name    string
		path    string
		healthy func() error
		code    int
		body    string
	}{
		{"metrics", "/metrics", nil, 200, "chunkd_up 1"},
		{"healthy", "/healthz", nil, 200, "ok"},
		{"unhealthy", "/healthz", func() error { return errors.New("meta unreachable") }, 503, "meta unreachable"},
		{"unknown", "/nope", nil, 404, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Handler(reg, tt.healthy).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if rec.Code != tt.code || !strings.Contains(rec.Body.String(), tt.body) {
				t.Fatalf("%s: %d %q, want %d containing %q", tt.path, rec.Code, rec.Body, tt.code, tt.body)
			}
		})
	}
}
