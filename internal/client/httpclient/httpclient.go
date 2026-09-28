// Package httpclient implements client.API against the HTTP gateway. It
// trusts the gateway for nothing it can check: every downloaded chunk is
// verified against the manifest, and the file against its SHA-256.
package httpclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
	"github.com/insanityatpeak/chunkd/internal/real/gateway"
)

// Client talks to a gateway at Base (e.g. http://localhost:8080).
type Client struct {
	Base string
	HTTP *http.Client
}

var _ client.API = (*Client)(nil)

// New returns a client for the gateway at base.
func New(base string) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), HTTP: http.DefaultClient}
}

func (c *Client) url(p string, q url.Values) string {
	u := c.Base + p
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

func filesURL(path string) string { return "/files" + path }

func (c *Client) do(ctx context.Context, method, u string, body io.Reader, size int64, out any) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = size
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, iface.Errorf(iface.CodeUnavailable, "%s %s: %v", method, u, err)
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var eb gateway.ErrorBody
		if json.NewDecoder(resp.Body).Decode(&eb) != nil || eb.Code == "" {
			return nil, iface.Errorf(iface.CodeInternal, "%s %s: %s", method, u, resp.Status)
		}
		return nil, &iface.Error{Code: iface.ParseCode(eb.Code), Msg: eb.Error}
	}
	if out != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return nil, iface.Errorf(iface.CodeInternal, "decode %s: %v", u, err)
		}
	}
	return resp, nil
}

// Put streams r to the gateway.
func (c *Client) Put(ctx context.Context, path string, r io.Reader, size int64, opts client.PutOptions) (client.Manifest, error) {
	q := url.Values{}
	if !opts.Overwrite {
		q.Set("expected", strconv.FormatUint(opts.ExpectedVersion, 10))
	}
	var m client.Manifest
	_, err := c.do(ctx, http.MethodPost, c.url(filesURL(path), q), r, size, &m)
	return m, err
}

// Stat fetches the manifest.
func (c *Client) Stat(ctx context.Context, path string) (client.Manifest, error) {
	var m client.Manifest
	_, err := c.do(ctx, http.MethodGet, c.url(filesURL(path), url.Values{"manifest": {"1"}}), nil, 0, &m)
	return m, err
}

// Get fetches the manifest, then the bytes, and verifies each chunk against
// the manifest's hash and the whole file against its SHA-256.
func (c *Client) Get(ctx context.Context, path string, w io.Writer) (client.Manifest, error) {
	m, err := c.Stat(ctx, path)
	if err != nil {
		return m, err
	}
	resp, err := c.do(ctx, http.MethodGet, c.url(filesURL(path), nil), nil, 0, nil)
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()
	if v := resp.Header.Get(gateway.HeaderVersion); v != strconv.FormatUint(m.Version, 10) {
		return m, iface.Errorf(iface.CodeConflict, "%s changed between manifest (v%d) and download (v%s); retry", path, m.Version, v)
	}
	file := sha256.New()
	for _, ch := range m.Chunk {
		buf := make([]byte, ch.Size)
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			return m, iface.Errorf(iface.CodeUnavailable, "chunk %d: stream ended early: %v", ch.Index, err)
		}
		if sum := sha256.Sum256(buf); hex.EncodeToString(sum[:]) != ch.ID {
			return m, iface.Errorf(iface.CodeInternal, "chunk %d: data does not match its hash", ch.Index)
		}
		file.Write(buf)
		if _, err := w.Write(buf); err != nil {
			return m, err
		}
	}
	if extra, _ := io.Copy(io.Discard, resp.Body); extra > 0 {
		return m, iface.Errorf(iface.CodeInternal, "%d bytes beyond the manifest", extra)
	}
	if got := hex.EncodeToString(file.Sum(nil)); got != m.SHA256 {
		return m, iface.Errorf(iface.CodeInternal, "file SHA-256 %s does not match manifest %s", got, m.SHA256)
	}
	return m, nil
}

// List returns files under prefix.
func (c *Client) List(ctx context.Context, prefix string) ([]client.FileInfo, error) {
	var out []client.FileInfo
	_, err := c.do(ctx, http.MethodGet, c.url("/files", url.Values{"prefix": {prefix}}), nil, 0, &out)
	return out, err
}

// Delete tombstones path.
func (c *Client) Delete(ctx context.Context, path string, expectedVersion uint64) (uint64, error) {
	q := url.Values{}
	if expectedVersion != 0 {
		q.Set("expected", strconv.FormatUint(expectedVersion, 10))
	}
	var out struct{ Version uint64 }
	_, err := c.do(ctx, http.MethodDelete, c.url(filesURL(path), q), nil, 0, &out)
	return out.Version, err
}

// Cluster returns the node view.
func (c *Client) Cluster(ctx context.Context) (client.Cluster, error) {
	var out client.Cluster
	_, err := c.do(ctx, http.MethodGet, c.url("/cluster", nil), nil, 0, &out)
	return out, err
}
