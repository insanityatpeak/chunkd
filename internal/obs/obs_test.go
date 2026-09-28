package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerAddsRequestID(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"with id", WithRequestID(context.Background(), "req-7"), "req-7"},
		{"without id", context.Background(), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			NewLogger(&buf, "meta", slog.LevelInfo).With("k", 1).InfoContext(tt.ctx, "hello")
			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("not JSON: %q", buf.String())
			}
			if rec["component"] != "meta" {
				t.Errorf("component = %v, want meta", rec["component"])
			}
			got, _ := rec["request_id"].(string)
			if got != tt.want {
				t.Errorf("request_id = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRegistryWriteText(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("chunkd_pings_total", "Pings received.")
	g := r.Gauge("chunkd_nodes_alive", "Nodes currently alive.")
	c.Add(3)
	g.Set(2.5)

	var buf bytes.Buffer
	if err := r.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"# HELP chunkd_nodes_alive Nodes currently alive.",
		"# TYPE chunkd_nodes_alive gauge",
		"chunkd_nodes_alive 2.5",
		"# HELP chunkd_pings_total Pings received.",
		"# TYPE chunkd_pings_total counter",
		"chunkd_pings_total 3",
		"",
	}, "\n")
	if buf.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", buf.String(), want)
	}
}

func TestRegistryDuplicatePanics(t *testing.T) {
	r := NewRegistry()
	r.Counter("x", "")
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate registration did not panic")
		}
	}()
	r.Gauge("x", "")
}
