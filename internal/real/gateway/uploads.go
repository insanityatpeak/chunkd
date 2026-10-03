package gateway

import (
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// The resumable upload protocol is the core of tus 1.0 (tus.io), with
// appends in whole chunks (ADR-0024).
const (
	HeaderTus          = "Tus-Resumable"
	TusVersion         = "1.0.0"
	HeaderUploadLength = "Upload-Length"
	HeaderUploadOffset = "Upload-Offset"
	// HeaderUploadSHA256 declares the whole file's SHA-256 (hex) at creation;
	// the commit carries it, so reads verify the file.
	HeaderUploadSHA256 = "Upload-Sha256"
	// HeaderUploadClaimed is the furthest offset a PATCH may start at.
	HeaderUploadClaimed = "Upload-Claimed"
	// ContentTypeOffset is what tus requires on a PATCH body.
	ContentTypeOffset = "application/offset+octet-stream"
)

// createUpload: POST /uploads/{path} with Upload-Length and Upload-Sha256,
// and the same ?expected, ?lww and ?redundancy as POST /files.
func (h *handler) createUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HeaderTus, TusVersion)
	size, err := strconv.ParseInt(r.Header.Get(HeaderUploadLength), 10, 64)
	if err != nil || size < 0 {
		writeError(w, iface.Errorf(iface.CodeInvalid, "%s required: the file size in bytes", HeaderUploadLength), 0)
		return
	}
	raw, err := hex.DecodeString(r.Header.Get(HeaderUploadSHA256))
	if err != nil || len(raw) != 32 {
		writeError(w, iface.Errorf(iface.CodeInvalid, "%s required: the whole file's SHA-256 in hex", HeaderUploadSHA256), 0)
		return
	}
	opts, err := putOptions(r)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	info, err := h.api.BeginResumable(r.Context(), filePath(r), size, [32]byte(raw), opts)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	w.Header().Set("Location", "/uploads/"+strconv.FormatUint(info.ID, 10))
	w.Header().Set(HeaderUploadOffset, "0")
	writeJSON(w, http.StatusCreated, info)
}

func uploadID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	w.Header().Set(HeaderTus, TusVersion)
	w.Header().Set("Cache-Control", "no-store")
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, iface.Errorf(iface.CodeInvalid, "bad upload id %q", r.PathValue("id")), 0)
		return 0, false
	}
	return id, true
}

// uploadStatus: HEAD /uploads/{id} answers with headers only, as tus does;
// GET /uploads/{id} with the UploadInfo as JSON too.
func (h *handler) uploadStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := uploadID(w, r)
	if !ok {
		return
	}
	info, err := h.api.UploadStatus(r.Context(), id)
	if err != nil {
		if r.Method == http.MethodHead {
			w.WriteHeader(StatusOf(iface.CodeOf(err)))
			return
		}
		writeError(w, err, 0)
		return
	}
	hd := w.Header()
	offset := info.Offset
	if info.Version != 0 {
		offset = info.Size
		hd.Set(HeaderVersion, strconv.FormatUint(info.Version, 10))
	} else {
		hd.Set(HeaderUploadLength, strconv.FormatInt(info.Size, 10))
		hd.Set(HeaderUploadClaimed, strconv.FormatInt(info.Claimed, 10))
	}
	hd.Set(HeaderUploadOffset, strconv.FormatInt(offset, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// appendUpload: PATCH /uploads/{id} with Upload-Offset, a body of whole
// chunks (or the rest of the file) and Content-Type
// application/offset+octet-stream. 204 with the new Upload-Offset, and
// X-Chunkd-Version once the append committed.
func (h *handler) appendUpload(w http.ResponseWriter, r *http.Request) {
	id, ok := uploadID(w, r)
	if !ok {
		return
	}
	if r.Header.Get("Content-Type") != ContentTypeOffset {
		writeError(w, iface.Errorf(iface.CodeInvalid, "Content-Type must be %s", ContentTypeOffset), http.StatusUnsupportedMediaType)
		return
	}
	offset, err := strconv.ParseInt(r.Header.Get(HeaderUploadOffset), 10, 64)
	if err != nil {
		writeError(w, iface.Errorf(iface.CodeInvalid, "%s required", HeaderUploadOffset), 0)
		return
	}
	if r.ContentLength < 0 {
		writeError(w, iface.Errorf(iface.CodeInvalid, "Content-Length required"), http.StatusLengthRequired)
		return
	}
	info, err := h.api.Append(r.Context(), id, offset, r.Body, r.ContentLength)
	if err != nil {
		writeError(w, err, 0)
		return
	}
	w.Header().Set(HeaderUploadOffset, strconv.FormatInt(info.Offset, 10))
	if info.Version != 0 {
		w.Header().Set(HeaderUploadOffset, strconv.FormatInt(info.Size, 10))
		w.Header().Set(HeaderVersion, strconv.FormatUint(info.Version, 10))
	}
	w.WriteHeader(http.StatusNoContent)
}
