package disk

// Making a write durable, as opposed to merely issued.
//
// os.WriteFile returns when the bytes have reached the page cache, not when
// they have reached the platter, and os.Rename returns when the directory entry
// has been updated in the page cache for the same reason. Neither is a
// durability boundary on its own. Compaction depends on both being one: it
// writes a new image, then writes and *fsyncs* a WAL checkpoint marker saying
// "everything before this is in the image", then renames the image into place.
// Without the syncs here, a power loss after the checkpoint can leave a log
// that disclaims records whose image never reached the disk — the marker is
// durable and the data it vouches for is not.
//
// The ordering in compact.go was always right. These are the two syncs that
// ordering was assuming.

import (
	"fmt"
	"os"
)

// writeFileSync writes data to path and fsyncs it before returning, so the
// bytes are on the medium rather than in the page cache. It is os.WriteFile
// with the guarantee the caller thought os.WriteFile already gave.
//
// The file is truncated on open, as os.WriteFile does, and the sync happens
// before the close: a Close error after a successful Sync tells us nothing new,
// but a Sync error is the one that means the data is not there.
func writeFileSync(path string, data []byte, perm os.FileMode) error {
	return writeFileSyncHooked(path, data, perm, nil)
}

// writeFileSyncHooked is writeFileSync with a seam between the write and the
// sync, so a test can fail there.
//
// That instant is a distinct on-disk state and not a hypothetical one: the file
// exists at full length in the page cache and none of it is guaranteed to be on
// the medium. It is the state a compaction is in when it is about to write a
// checkpoint marker vouching for bytes that may not survive a power loss, which
// is the exact hazard the sync exists to close — so it has to be reachable.
//
// beforeSync is nil in production. An error from it aborts the write and the
// partial file is left for the caller to remove, which is what a real sync
// failure does too.
func writeFileSyncHooked(path string, data []byte, perm os.FileMode, beforeSync func() error) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if beforeSync != nil {
		if err := beforeSync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return f.Close()
}

// syncDir flushes a directory's own entries, which is what makes a rename or a
// create durable. A file can be fully synced and still vanish on power loss if
// the directory entry naming it never left the cache.
//
// Not every platform can express this; see fsync_dir.go and fsync_nodir.go for
// which do what. Where it cannot be expressed this is a no-op that reports
// success, because the alternative — failing a compaction on a platform whose
// filesystem does not offer the primitive — trades a working store for a
// guarantee that platform was never going to provide.
func syncDir(dir string) error {
	return fsyncDir(dir)
}
