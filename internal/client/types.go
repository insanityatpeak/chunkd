// Package client is the chunkd client library shared by the CLI, the HTTP
// gateway, the WASM build and the tests. Metadata calls go to the metadata
// server; chunk data goes directly between the client and storage nodes.
// The client verifies every chunk and the whole file itself.
package client

import (
	"context"
	"io"
)

// FileInfo describes one committed file version.
type FileInfo struct {
	Path    string `json:"path"`
	Version uint64 `json:"version"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"` // hex
	Chunks  int    `json:"chunks"`
}

// ChunkRef is one chunk of a file and the nodes reported to hold it.
type ChunkRef struct {
	Index    int      `json:"index"`
	ID       string   `json:"id"` // hex SHA-256
	Size     int64    `json:"size"`
	Replicas []string `json:"replicas"`
	// ServedBy is the replica that returned intact data, set by Get.
	ServedBy string `json:"servedBy,omitempty"`
	// Rejected lists replicas whose data failed verification, set by Get.
	Rejected []string `json:"rejected,omitempty"`
	// Hedged is set when Get read more than one replica for this chunk.
	Hedged bool `json:"hedged,omitempty"`
}

// Manifest is a file's metadata including its chunk layout.
type Manifest struct {
	FileInfo
	ChunkSize int        `json:"chunkSize"`
	Chunk     []ChunkRef `json:"chunkList"`
}

// NodeInfo is one storage node in the cluster view.
type NodeInfo struct {
	ID        string `json:"id"`
	Rack      string `json:"rack"`
	Alive     bool   `json:"alive"`
	Draining  bool   `json:"draining"`
	UsedBytes int64  `json:"usedBytes"`
	Chunks    int64  `json:"chunks"`
}

// Cluster is the metadata server's view of the cluster.
type Cluster struct {
	Nodes        []NodeInfo `json:"nodes"`
	Files        int64      `json:"files"`
	LogicalBytes int64      `json:"logicalBytes"`
}

// PutOptions controls version checks on upload.
type PutOptions struct {
	// ExpectedVersion is the live version the upload replaces; 0 means the
	// path must not exist. Ignored when Overwrite is set.
	ExpectedVersion uint64
	// Overwrite replaces whatever version is live (read, then compare-and-swap).
	Overwrite bool
}

// API is implemented by the direct client and the HTTP gateway client.
type API interface {
	Put(ctx context.Context, path string, r io.Reader, size int64, opts PutOptions) (Manifest, error)
	// Get writes the verified file to w. It fails if any chunk has no intact
	// replica or the whole-file hash does not match.
	Get(ctx context.Context, path string, w io.Writer) (Manifest, error)
	Stat(ctx context.Context, path string) (Manifest, error)
	List(ctx context.Context, prefix string) ([]FileInfo, error)
	Delete(ctx context.Context, path string, expectedVersion uint64) (uint64, error)
	Cluster(ctx context.Context) (Cluster, error)
}

// ManifestWriter is an io.Writer that wants the manifest before the first
// byte, e.g. to set HTTP headers.
type ManifestWriter interface {
	io.Writer
	SetManifest(Manifest)
}
