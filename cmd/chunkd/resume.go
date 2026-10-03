package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/insanityatpeak/chunkd/internal/client"
	"github.com/insanityatpeak/chunkd/internal/iface"
)

// appendBatch is how many chunks one append carries: progress is reported and
// a cut-off connection costs at most this much resent.
const appendBatch = 8

// pending is what a put keeps between runs, so put -resume can find its upload.
type pending struct {
	ID         uint64            `json:"id"`
	Local      string            `json:"local"`
	Path       string            `json:"path"`
	Size       int64             `json:"size"`
	SHA256     string            `json:"sha256"`
	Redundancy client.Redundancy `json:"redundancy,omitempty"`
}

// pendingFile is keyed by the local file and the remote path, so two puts of
// different files never share an upload.
func pendingFile(local, path string) (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(local)
	if err != nil {
		return "", err
	}
	key := sha256.Sum256([]byte(abs + "\x00" + path))
	return filepath.Join(dir, "chunkd", "uploads", hex.EncodeToString(key[:8])+".json"), nil
}

func loadPending(file string) (pending, bool) {
	b, err := os.ReadFile(file)
	if err != nil {
		return pending{}, false
	}
	var p pending
	if json.Unmarshal(b, &p) != nil {
		return pending{}, false
	}
	return p, true
}

func savePending(file string, p pending) error {
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	b, _ := json.Marshal(p)
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, file)
}

func hashFile(f *os.File) ([32]byte, error) {
	var sum [32]byte
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return sum, err
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// putResumable uploads through a resumable upload (ADR-0024). The file is
// hashed first; the upload's id is kept in the user cache directory until the
// commit. With resume, a kept upload is continued from the claimed chunks it
// holds, or restarted if it expired. A file that changed since is refused.
func putResumable(ctx context.Context, api client.API, local, path string, opts client.PutOptions, resume bool, out printer) error {
	f, err := os.Open(local)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	sum, err := hashFile(f)
	if err != nil {
		return err
	}
	hexSum := hex.EncodeToString(sum[:])
	file, err := pendingFile(local, path)
	if err != nil {
		return err
	}
	start := time.Now()

	var info client.UploadInfo
	have := false
	if p, ok := loadPending(file); ok && resume {
		if p.Size != fi.Size() || p.SHA256 != hexSum {
			return fmt.Errorf("%s changed since upload %d began; remove %s to start over", local, p.ID, file)
		}
		switch info, err = api.UploadStatus(ctx, p.ID); {
		case err == nil:
			have = true
		case iface.CodeOf(err) == iface.CodeNotFound:
			fmt.Fprintf(os.Stderr, "upload %d expired; starting over\n", p.ID)
		default:
			return err
		}
	}
	if !have {
		if info, err = api.BeginResumable(ctx, path, fi.Size(), sum, opts); err != nil {
			return err
		}
	}
	if err := savePending(file, pending{ID: info.ID, Local: local, Path: path, Size: fi.Size(), SHA256: hexSum, Redundancy: info.Redundancy}); err != nil {
		return err
	}

	// A claimed chunk is accepted without being stored (its claim names it),
	// so resume from the stored offset: the stored chunks are skipped as
	// present, and the rest are written.
	offset := info.Offset
	step := int64(appendBatch) * int64(info.ChunkSize)
	for info.Version == 0 {
		n := min(step, fi.Size()-offset)
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		if info, err = api.Append(ctx, info.ID, offset, io.LimitReader(f, n), n); err != nil {
			return fmt.Errorf("%w (continue with: chunkd put -resume %s %s)", err, local, path)
		}
		offset += n
		if !out.json && fi.Size() > 0 {
			fmt.Fprintf(os.Stderr, "\r%s / %s", human(min(offset, fi.Size())), human(fi.Size()))
		}
	}
	if !out.json && fi.Size() > 0 {
		fmt.Fprintln(os.Stderr)
	}
	_ = os.Remove(file)

	m, err := api.Stat(ctx, path)
	if err != nil {
		return err
	}
	if out.json {
		out.print(m)
		return nil
	}
	policy := "replicated"
	if m.Redundancy != client.Replicated {
		policy = string(m.Redundancy)
	}
	fmt.Printf("put %s v%d  %s in %d chunks (%s)  %.1fs\nsha256 %s\n", m.Path, info.Version, human(m.Size), m.Chunks, policy, time.Since(start).Seconds(), m.SHA256)
	return nil
}
