//go:build !windows

package fsutil

import "os"

// SyncDir fsyncs a directory so a create or rename inside it is durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
