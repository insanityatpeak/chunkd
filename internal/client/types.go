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
	// Deduped: the cluster already held this chunk, so Put did not send it.
	Deduped bool `json:"deduped,omitempty"`
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
	ID       string `json:"id"`
	Rack     string `json:"rack"`
	Alive    bool   `json:"alive"`
	State    string `json:"state"` // alive, suspect or dead
	Draining bool   `json:"draining"`
	// Admin is the operator-set state: active, draining or decommissioned.
	Admin     string `json:"admin"`
	UsedBytes int64  `json:"usedBytes"`
	Chunks    int64  `json:"chunks"`
	// HeartbeatAgeMs is the time since the node's last heartbeat.
	HeartbeatAgeMs int64 `json:"heartbeatAgeMs"`
	// Corrupt counts copies the node quarantined since it started;
	// ScrubDone of ScrubTotal is the current scrub pass's progress.
	Corrupt     uint64 `json:"corrupt"`
	ScrubDone   int64  `json:"scrubDone"`
	ScrubTotal  int64  `json:"scrubTotal"`
	ScrubPasses uint64 `json:"scrubPasses"`
	// The balancer's view: bytes located on the node, its target and the
	// band around it. BalanceTarget 0: no plan (unsettled membership).
	BalanceUsed   int64 `json:"balanceUsed"`
	BalanceTarget int64 `json:"balanceTarget"`
	BalanceBand   int64 `json:"balanceBand"`
}

// Health is replication state across all chunks.
type Health struct {
	Chunks          int64 `json:"chunks"`
	UnderReplicated int64 `json:"underReplicated"`
	OverReplicated  int64 `json:"overReplicated"`
	Lost            int64 `json:"lost"`
	// Replicas[i] counts chunks with i copies on alive nodes ("or more"
	// for the last entry).
	Replicas        []int64 `json:"replicas"`
	RepairQueued    int64   `json:"repairQueued"`
	RepairInFlight  int64   `json:"repairInFlight"`
	RepairWaiting   int64   `json:"repairWaiting"`
	RepairCompleted uint64  `json:"repairCompleted"`
	RepairBytes     uint64  `json:"repairBytes"`
	RepairTrimmed   uint64  `json:"repairTrimmed"`
	RepairTimedOut  uint64  `json:"repairTimedOut"`
	RepairFailed    uint64  `json:"repairFailed"`
	DetectorStalls  uint64  `json:"detectorStalls"`
	CorruptReplicas uint64  `json:"corruptReplicas"`
	// Of RepairCompleted: drain copies and balance moves, not repairs.
	RepairEvacuated uint64 `json:"repairEvacuated"`
	RepairMoved     uint64 `json:"repairMoved"`
}

// FileHealth is one committed file's replication state.
type FileHealth struct {
	Path            string `json:"path"`
	Chunks          int    `json:"chunks"`
	UnderReplicated int    `json:"underReplicated"`
	MinLive         int    `json:"minLive"` // fewest alive copies of any chunk
}

// RepairCopy is one re-replication in flight.
type RepairCopy struct {
	ID        uint64 `json:"id"`
	Chunk     string `json:"chunk"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Bytes     int64  `json:"bytes"`
	StartedMs int64  `json:"startedMs"`
}

// Event is one entry of the metadata server's recent-event timeline.
type Event struct {
	Seq  uint64 `json:"seq"`
	AtMs int64  `json:"atMs"`
	Kind string `json:"kind"` // node, copy, trim or corrupt
	Node string `json:"node"`
	Text string `json:"text"`
}

// Cluster is the metadata server's view of the cluster. Every *Ms instant
// is on the metadata server's clock, whose current reading is NowMs.
type Cluster struct {
	NowMs        int64        `json:"nowMs"`
	Nodes        []NodeInfo   `json:"nodes"`
	Files        int64        `json:"files"`
	LogicalBytes int64        `json:"logicalBytes"`
	Health       Health       `json:"health"`
	FileHealth   []FileHealth `json:"fileHealth"`
	Copies       []RepairCopy `json:"copies"`
	// Events after the requested seq; EventSeq below that seq means the
	// metadata server restarted and its timeline began again.
	Events   []Event `json:"events"`
	EventSeq uint64  `json:"eventSeq"`
	// Dedup saves ReferencedBytes-DistinctBytes per replica.
	ReferencedBytes int64         `json:"referencedBytes"`
	DistinctBytes   int64         `json:"distinctBytes"`
	Epoch           uint64        `json:"epoch"`
	GC              GCStats       `json:"gc"`
	Deleted         []DeletedFile `json:"deleted"`
	// MetaLeader is the metadata leader the answering peer knows, empty
	// during an election; MetaTerm is its Raft term.
	MetaLeader string `json:"metaLeader"`
	MetaTerm   uint64 `json:"metaTerm"`
	// MetaPeer is the answering peer, whose clock and event sequence the
	// view uses; MetaRole, MetaCommit and MetaApplied are its own.
	MetaPeer    string         `json:"metaPeer"`
	MetaRole    string         `json:"metaRole"`
	MetaCommit  uint64         `json:"metaCommit"`
	MetaApplied uint64         `json:"metaApplied"`
	MetaPeers   []MetaPeerInfo `json:"metaPeers"`
}

// GCStats are the sweep's counters and the retention settings.
type GCStats struct {
	Orphans      uint64 `json:"orphans"`
	Sent         uint64 `json:"sent"`
	Deleted      uint64 `json:"deleted"`
	Kept         uint64 `json:"kept"`
	Drift        uint64 `json:"drift"`
	RetainEpochs uint32 `json:"retainEpochs"`
	EpochEveryMs int64  `json:"epochEveryMs"`
}

// DeletedFile is a deleted path and the version undelete would restore.
type DeletedFile struct {
	Path         string `json:"path"`
	Version      uint64 `json:"version"`
	Size         int64  `json:"size"`
	ExpiresEpoch uint64 `json:"expiresEpoch"`
}

// PutOptions controls version checks on upload.
type PutOptions struct {
	// ExpectedVersion is the live version the upload replaces; 0 means the
	// path must not exist. Ignored when Overwrite is set.
	ExpectedVersion uint64
	// Overwrite replaces whatever version is live (read, then compare-and-swap).
	Overwrite bool
	// LastWriterWins commits over whatever is live at commit time, with no
	// version check. A concurrent update is silently lost (ADR-0014).
	LastWriterWins bool
}

// VersionInfo is one entry of a file's history.
type VersionInfo struct {
	Version uint64 `json:"version"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256,omitempty"`
	Chunks  int    `json:"chunks"`
	// Deleted marks a tombstone: the path reads as not found from here on.
	Deleted bool `json:"deleted,omitempty"`
	// Retired versions were superseded; undelete can restore them until
	// the metadata epoch reaches ExpiresEpoch.
	Retired      bool   `json:"retired,omitempty"`
	ExpiresEpoch uint64 `json:"expiresEpoch,omitempty"`
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
	// Undelete restores a retained version of path as its newest version;
	// version 0 picks the newest one that is not a delete marker.
	Undelete(ctx context.Context, path string, version uint64) (uint64, error)
	// Log returns every retained version of path, oldest first.
	Log(ctx context.Context, path string) ([]VersionInfo, error)
	// StatVersion is Stat for a given version; 0 is the live one.
	StatVersion(ctx context.Context, path string, version uint64) (Manifest, error)
	// GetVersion is Get for a given version; 0 is the live one.
	GetVersion(ctx context.Context, path string, version uint64, w io.Writer) (Manifest, error)
	// Cluster returns the cluster view with timeline events after eventsAfter.
	Cluster(ctx context.Context, eventsAfter uint64) (Cluster, error)
	// NodeAdmin sets a storage node's admin state: "draining", "active"
	// (undrain) or "decommissioned", refused while unsafe (ADR-0021).
	NodeAdmin(ctx context.Context, node, state string) (NodeAdminResult, error)
}

// ManifestWriter is an io.Writer that wants the manifest before the first
// byte, e.g. to set HTTP headers.
type ManifestWriter interface {
	io.Writer
	SetManifest(Manifest)
}

// MetaPeerInfo is one metadata peer as the answering peer sees it. Match is the
// last index known in its log; HeardAgoMs is -1 if never heard from.
type MetaPeerInfo struct {
	ID         string `json:"id"`
	Match      uint64 `json:"match"`
	HeardAgoMs int64  `json:"heardAgoMs"`
}

// NodeAdminResult is the outcome of a drain, undrain or decommission.
type NodeAdminResult struct {
	Node  string `json:"node"`
	Admin string `json:"admin"`
	// Warning says what the change gives up, e.g. rack spread.
	Warning string `json:"warning,omitempty"`
}
