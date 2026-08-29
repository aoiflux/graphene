package disk

// Two file operations that behave differently when another process is reading
// the file, and the contract both platform halves have to meet.
//
// # Why this file exists
//
// A live reader (Options.LiveReader) holds the log open while it replays. On
// Unix that changes nothing: a directory entry and the inode behind it are
// separate things, so rename(2) and unlink(2) succeed with any number of
// readers holding the file, and each reader goes on reading the inode it
// opened. On Windows the name and the file are the same object, and an open
// handle blocks operations on the name unless the *opening* process said
// otherwise. Measured on Windows 11 with a foreign os.Open handle on the log:
//
//	operation                       plain os.Open   share-delete open
//	os.Rename away  (Rotate)        Access denied   OK
//	os.Rename over  (truncateFrom)  Access denied   Access denied
//	ReplaceFileW over               -               OK
//	os.Truncate                     OK              OK
//
// So making readers live costs two primitives, and neither may be a Windows
// feature that Linux and macOS do without:
//
//   - openSharedRead: open for reading in a way that does not block the writer
//     from renaming or deleting the file. Unix already works this way, which is
//     the contract; Windows needs FILE_SHARE_DELETE asked for explicitly.
//   - replaceFile: atomically replace a path, succeeding even when someone holds
//     the destination open for reading. rename(2) already does this; Windows
//     needs ReplaceFileW when the destination is held.
//
// # What is deliberately not traded away
//
// ReplaceFileW is not MoveFileEx. MoveFileEx with MOVEFILE_REPLACE_EXISTING --
// what os.Rename issues, and what Phase 2 chose for the log rebuild precisely
// because the carried tail is the only durable copy of the commits in it -- is a
// single metadata operation. ReplaceFileW removes the destination and moves the
// replacement into place, so there is a window in which the destination does not
// exist. That is a weaker guarantee, and it is not imposed on anyone who is not
// using it:
//
//   - replaceFile tries os.Rename first and reaches ReplaceFileW only when the
//     rename is refused because the destination is held open. A store with no
//     live reader attached takes exactly the path it always took.
//   - The window is covered rather than accepted. The replacement is written and
//     fsynced under a .tmp name before the destination is touched, so a crash
//     inside the window leaves a complete, durable copy of the log at that name
//     and nothing at the log's own. adoptOrphanedLog finds it on the next open.
//
// The result is one contract on all three platforms, with the platform-specific
// call as an implementation detail -- the same shape as lockFile and syncDir.

import (
	"fmt"
	"os"
)

// walTmpSuffix names the file truncateFrom builds the rebuilt log in.
const walTmpSuffix = ".tmp"

// adoptOrphanedLog recovers a log lost inside replaceFile's window.
//
// The window exists only on Windows and only when a live reader holds the log
// open, and it is bounded by construction: the replacement at path+".tmp" is
// complete and fsynced before the destination is touched, so the only states
// reachable are "destination present" (nothing to do, whichever file it is) and
// "destination absent, replacement present".
//
// A leftover .tmp beside an intact log is therefore not evidence of that window
// -- it is the residue of a rebuild that failed before it touched anything --
// and is removed rather than adopted. Adopting it would install a log that was
// never committed to.
func adoptOrphanedLog(path string) error {
	tmp := path + walTmpSuffix
	if _, err := os.Stat(tmp); err != nil {
		return nil // the ordinary case: no replacement is pending
	}
	if _, err := os.Stat(path); err == nil {
		// The log is there. Whether the replace ran or never started, the file
		// at the log's own name is the authority.
		return os.Remove(tmp)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("wal adopt: stat %s: %w", path, err)
	}

	// The log is gone and a replacement is sitting beside it. Only replaceFile
	// can produce that, and only after writing and syncing the replacement.
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("wal adopt: %w", err)
	}
	return syncDir(dirOf(path))
}
