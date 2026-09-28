//go:build windows

package fsutil

// SyncDir is a no-op on Windows: directories cannot be opened for fsync, and
// NTFS journals metadata changes such as renames. Production runs on Linux.
func SyncDir(string) error { return nil }
