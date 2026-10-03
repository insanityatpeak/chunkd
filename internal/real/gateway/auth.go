package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// Key is one API key's entry in the keys file. The file holds the SHA-256 of
// the key, never the key. A key owns the paths under /<Namespace>/ and may
// grow that namespace to Quota bytes (0: unlimited). An Admin key reaches
// every path and the node controls.
type Key struct {
	Name      string `json:"name"`
	SHA256    string `json:"sha256"`
	Namespace string `json:"namespace,omitempty"`
	Quota     int64  `json:"quota,omitempty"`
	Admin     bool   `json:"admin,omitempty"`
}

// Allows reports whether the key may touch path p.
func (k Key) Allows(p string) bool {
	return k.Admin || strings.HasPrefix(p, "/"+k.Namespace+"/")
}

// Keys looks API keys up by the hash of the presented key.
type Keys struct{ byHash map[[32]byte]Key }

// HashKey returns the hex SHA-256 that identifies a key in the keys file.
func HashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// ParseKeys decodes a keys file: a JSON array of Key.
func ParseKeys(data []byte) (*Keys, error) {
	var list []Key
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	k := &Keys{byHash: map[[32]byte]Key{}}
	namespaces := map[string]string{}
	for _, e := range list {
		raw, err := hex.DecodeString(e.SHA256)
		if err != nil || len(raw) != 32 {
			return nil, fmt.Errorf("keys: %q: sha256 must be 64 hex digits", e.Name)
		}
		switch {
		case e.Admin && e.Namespace != "":
			return nil, fmt.Errorf("keys: %q: an admin key has no namespace", e.Name)
		case !e.Admin && (e.Namespace == "" || strings.ContainsAny(e.Namespace, "/\x00") || e.Namespace == "." || e.Namespace == ".."):
			return nil, fmt.Errorf("keys: %q: namespace %q is not a single path segment", e.Name, e.Namespace)
		case e.Quota < 0:
			return nil, fmt.Errorf("keys: %q: negative quota", e.Name)
		}
		// Two keys on one namespace would each be limited alone while
		// sharing the bytes the log counts.
		if other, dup := namespaces[e.Namespace]; dup && e.Namespace != "" {
			return nil, fmt.Errorf("keys: %q and %q share namespace %q", other, e.Name, e.Namespace)
		}
		namespaces[e.Namespace] = e.Name
		var h [32]byte
		copy(h[:], raw)
		if _, dup := k.byHash[h]; dup {
			return nil, fmt.Errorf("keys: %q repeats a key", e.Name)
		}
		k.byHash[h] = e
	}
	return k, nil
}

// LoadKeys reads a keys file.
func LoadKeys(path string) (*Keys, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseKeys(data)
}

// lookup finds the key in an "Authorization: Bearer <key>" header. The
// map is keyed by hash, so no comparison against a secret depends on how
// many leading bytes match.
func (k *Keys) lookup(r *http.Request) (Key, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || tok == "" {
		return Key{}, false
	}
	e, ok := k.byHash[sha256.Sum256([]byte(tok))]
	return e, ok
}

// Option configures Handler.
type Option func(*handler)

// WithKeys requires an API key on every route that reads or writes files or
// controls nodes (ADR-0025). Without it the gateway is open, as before.
// /cluster stays open: it carries counts and node health, no file names.
func WithKeys(k *Keys) Option { return func(h *handler) { h.keys = k } }

type scope int

const (
	scopeOpen   scope = iota // any caller, even without a key
	scopePath                // the file path in the URL
	scopeList                // ?prefix, narrowed to the key's namespace
	scopeUpload              // an upload ID: its path decides
	scopeAdmin               // admin keys only
)

// guard authenticates, authorizes by scope, and hands the key's quota to the
// client library through the request context.
func (h *handler) guard(sc scope, fn http.HandlerFunc) http.HandlerFunc {
	if h.keys == nil || sc == scopeOpen {
		return fn
	}
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := h.keys.lookup(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="chunkd"`)
			writeError(w, iface.Errorf(iface.CodeDenied, "missing or unknown API key"), http.StatusUnauthorized)
			return
		}
		denied := func() {
			writeError(w, iface.Errorf(iface.CodeDenied, "key %q may not use this path", key.Name), 0)
		}
		switch sc {
		case scopeAdmin:
			if !key.Admin {
				writeError(w, iface.Errorf(iface.CodeDenied, "key %q is not an admin key", key.Name), 0)
				return
			}
		case scopePath:
			if !key.Allows(filePath(r)) {
				denied()
				return
			}
		case scopeList:
			if !key.Admin {
				q := r.URL.Query()
				switch p := q.Get("prefix"); {
				case p == "" || p == "/":
					q.Set("prefix", "/"+key.Namespace+"/")
				case !key.Allows(p) && !key.Allows(p+"/"):
					denied()
					return
				}
				r.URL.RawQuery = q.Encode()
			}
		case scopeUpload:
			// The ID alone names no path, so the log is asked whose it is. A
			// foreign upload answers not found, as an unknown one does.
			id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
			if err != nil {
				writeError(w, iface.Errorf(iface.CodeInvalid, "bad upload id %q", r.PathValue("id")), 0)
				return
			}
			st, err := h.api.UploadStatus(r.Context(), id)
			if err != nil {
				writeError(w, err, 0)
				return
			}
			if !key.Allows(st.Path) {
				writeError(w, iface.Errorf(iface.CodeNotFound, "upload %d", id), 0)
				return
			}
		}
		fn(w, r.WithContext(client.WithQuota(r.Context(), key.Quota)))
	}
}

// Bearer sets the API key on an outgoing request.
func Bearer(req *http.Request, key string) {
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
}
