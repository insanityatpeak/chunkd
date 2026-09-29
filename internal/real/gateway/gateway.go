// Package gateway is the HTTP front end: it turns HTTP requests into client
// library calls. It is not trusted by clients; they verify hashes themselves.
package gateway

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Response headers on GET /files/{path}.
const (
	HeaderVersion = "X-Chunkd-Version"
	HeaderSHA256  = "X-Chunkd-Sha256"
)

// Handler serves:
//
//	POST   /files/{path}            upload (Content-Length required); ?expected=N for compare-and-swap, ?lww=1 for last writer wins, else overwrite
//	GET    /files/{path}            download, verified chunk by chunk; ?version=N for an older version
//	GET    /files/{path}?manifest=1 metadata and chunk layout; ?version=N
//	GET    /files/{path}?log=1      every retained version, oldest first
//	GET    /files?prefix=/p         list
//	DELETE /files/{path}            tombstone; ?expected=N
//	GET    /cluster                 nodes, health, copies; ?events_after=N
func Handler(api client.API, log *slog.Logger) http.Handler {
	h := &handler{api: api, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /files/{path...}", h.put)
	mux.HandleFunc("GET /files/{path...}", h.get)
	mux.HandleFunc("GET /files", h.list)
	mux.HandleFunc("DELETE /files/{path...}", h.delete)
	mux.HandleFunc("GET /cluster", h.cluster)
	return cors(mux)
}

type handler struct {
	api client.API
	log *slog.Logger
}

func filePath(r *http.Request) string { return "/" + r.PathValue("path") }

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength < 0 {
		writeError(w, iface.Errorf(iface.CodeInvalid, "Content-Length required: placement needs the size up front"), http.StatusLengthRequired)
		return
	}
	opts := client.PutOptions{Overwrite: true, LastWriterWins: r.URL.Query().Get("lww") == "1"}
	if e := r.URL.Query().Get("expected"); e != "" && !opts.LastWriterWins {
		v, err := strconv.ParseUint(e, 10, 64)
		if err != nil {
			writeError(w, iface.Errorf(iface.CodeInvalid, "bad expected version %q", e), 0)
			return
		}
		opts = client.PutOptions{ExpectedVersion: v}
	}
	m, err := h.api.Put(r.Context(), filePath(r), r.Body, r.ContentLength, opts)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

// streamWriter sets headers from the manifest before the first byte.
type streamWriter struct{ w http.ResponseWriter }

func (s streamWriter) SetManifest(m client.Manifest) {
	s.w.Header().Set("Content-Type", "application/octet-stream")
	s.w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	s.w.Header().Set(HeaderVersion, strconv.FormatUint(m.Version, 10))
	s.w.Header().Set(HeaderSHA256, m.SHA256)
	s.w.WriteHeader(http.StatusOK)
}

func (s streamWriter) Write(p []byte) (int, error) { return s.w.Write(p) }

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("log") {
		vs, err := h.api.Log(r.Context(), filePath(r))
		if err != nil {
			writeError(w, err, 0)
			return
		}
		writeJSON(w, http.StatusOK, vs)
		return
	}
	var version uint64
	if e := r.URL.Query().Get("version"); e != "" {
		v, err := strconv.ParseUint(e, 10, 64)
		if err != nil {
			writeError(w, iface.Errorf(iface.CodeInvalid, "bad version %q", e), 0)
			return
		}
		version = v
	}
	if r.URL.Query().Has("manifest") {
		m, err := h.api.StatVersion(r.Context(), filePath(r), version)
		if err != nil {
			writeError(w, err, 0)
			return
		}
		writeJSON(w, http.StatusOK, m)
		return
	}
	_, err := h.api.GetVersion(r.Context(), filePath(r), version, streamWriter{w})
	if err == nil {
		return
	}
	if w.Header().Get(HeaderVersion) == "" { // nothing sent yet
		writeError(w, err, 0)
		return
	}
	// Headers are out: the only honest signal left is a broken stream, which
	// the client sees as a short body.
	h.log.ErrorContext(r.Context(), "download failed mid-stream", "path", filePath(r), "err", err)
	panic(http.ErrAbortHandler)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	files, err := h.api.List(r.Context(), r.URL.Query().Get("prefix"))
	if err != nil {
		writeError(w, err, 0)
		return
	}
	if files == nil {
		files = []client.FileInfo{}
	}
	writeJSON(w, http.StatusOK, files)
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	var expected uint64
	if e := r.URL.Query().Get("expected"); e != "" {
		v, err := strconv.ParseUint(e, 10, 64)
		if err != nil {
			writeError(w, iface.Errorf(iface.CodeInvalid, "bad expected version %q", e), 0)
			return
		}
		expected = v
	}
	v, err := h.api.Delete(r.Context(), filePath(r), expected)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	writeJSON(w, http.StatusOK, map[string]uint64{"version": v})
}

func (h *handler) cluster(w http.ResponseWriter, r *http.Request) {
	var after uint64
	if e := r.URL.Query().Get("events_after"); e != "" {
		v, err := strconv.ParseUint(e, 10, 64)
		if err != nil {
			writeError(w, iface.Errorf(iface.CodeInvalid, "bad events_after %q", e), 0)
			return
		}
		after = v
	}
	c, err := h.api.Cluster(r.Context(), after)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

// ErrorBody is the JSON shape of every error response.
type ErrorBody struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

// StatusOf maps an error code to an HTTP status.
func StatusOf(c iface.Code) int {
	switch c {
	case iface.CodeNotFound:
		return http.StatusNotFound
	case iface.CodeConflict:
		return http.StatusConflict
	case iface.CodeInvalid:
		return http.StatusBadRequest
	case iface.CodeUnavailable, iface.CodeRetry:
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

func writeError(w http.ResponseWriter, err error, status int) {
	var e *iface.Error
	if !errors.As(err, &e) {
		e = iface.AsError(err, iface.CodeInternal)
	}
	if status == 0 {
		status = StatusOf(e.Code)
	}
	writeJSON(w, status, ErrorBody{Code: e.Code.String(), Error: e.Msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// cors lets the dashboard, served from another origin (GitHub Pages or the
// Vite dev server), call a gateway on localhost.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Expose-Headers", HeaderVersion+", "+HeaderSHA256+", Content-Length")
		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			// Chrome's Private Network Access: a public page calling localhost.
			h.Set("Access-Control-Allow-Private-Network", "true")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WithUI serves the dashboard's static build from dir next to the API, so
// `docker compose up` gives one URL for both. /mode.json tells the page it
// is served by a real cluster (on GitHub Pages it is a 404, and the page
// runs the simulation instead).
func WithUI(api http.Handler, dir string) http.Handler {
	mux := http.NewServeMux()
	for _, p := range []string{"/files", "/files/", "/cluster"} {
		mux.Handle(p, api)
	}
	mux.HandleFunc("GET /mode.json", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"mode": "live"})
	})
	mux.Handle("/", http.FileServer(http.Dir(dir)))
	return mux
}
