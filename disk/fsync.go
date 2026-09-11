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
//
// For a payload the caller does not already hold, see writeStreamSync. The
// compacted image goes that way now; what is left here are the small ones — a
// backup manifest, a schema catalogue, a rebuilt log — where holding the bytes
// costs nothing.
func writeFileSync(path string, data []byte, perm os.FileMode) error {
	return writeStreamSync(path, perm, nil, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

// writeStreamSync is writeFileSync for a payload that is produced rather than
// held: write is handed the open file and streams into it.
//
// It exists because a compacted image is the size of the graph, and the caller
// that matters — compaction — had no use for the bytes except to pass them
// here. Building them in memory first made the largest allocation in the engine
// out of a value that was written once and dropped.
//
// The file is opened for reading as well as writing. SerialiseTo reads it back
// to compute the digest, because the digest covers a header field that is not
// final until the last section has been placed; see csr_write.go.
//
// Same durability contract as writeFileSync: truncated on open, synced before
// the close, and the sync error is the one that means the data is not there. A
// failure leaves the partial file for the caller to remove, exactly as a failed
// writeFileSync does.
func writeStreamSync(path string, perm os.FileMode, beforeSync func() error, write func(*os.File) error) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
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
