// Package wire names the message kinds on the transport and converts between
// protobuf bodies and transport calls.
package wire

import (
	"google.golang.org/protobuf/proto"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// One-way messages between storage nodes and the metadata server.
const (
	KindHeartbeat    = "node.heartbeat"
	KindHeartbeatAck = "node.heartbeat_ack"
	KindBlockReport  = "node.block_report"

	// Repair: metadata server to target node, and failures back.
	KindReplicate       = "node.replicate"
	KindReplicateFailed = "node.replicate_failed"
	KindDeleteReplica   = "node.delete_replica"

	// Integrity: the metadata server asks a node to re-check one chunk.
	KindVerifyChunk = "node.verify_chunk"
)

// RPCs. Kinds starting with "chunk." carry chunk data and are streamed in
// real mode.
const (
	KindBegin   = "meta.begin"
	KindClaim   = "meta.claim"
	KindCommit  = "meta.commit"
	KindAbort   = "meta.abort"
	KindDelete  = "meta.delete"
	KindStat    = "meta.stat"
	KindList    = "meta.list"
	KindCluster = "meta.cluster"
	KindSuspect = "meta.suspect"

	KindPutChunk = "chunk.put"
	KindGetChunk = "chunk.get"
)

// Marshal encodes m; the schemas are fixed, so failure is a programming error.
func Marshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// Respond encodes resp, or forwards err, through respond.
func Respond(respond iface.Responder, resp proto.Message, err error) {
	if err != nil {
		respond(nil, err)
		return
	}
	respond(Marshal(resp), nil)
}

// Decode unmarshals a request body, returning a CodeInvalid error on failure.
func Decode(body []byte, m proto.Message) error {
	if err := proto.Unmarshal(body, m); err != nil {
		return iface.Errorf(iface.CodeInvalid, "decode %T: %v", m, err)
	}
	return nil
}

// ChunkID converts wire bytes to a ChunkID.
func ChunkID(b []byte) (iface.ChunkID, error) {
	var id iface.ChunkID
	if len(b) != len(id) {
		return id, iface.Errorf(iface.CodeInvalid, "chunk id of %d bytes", len(b))
	}
	copy(id[:], b)
	return id, nil
}
