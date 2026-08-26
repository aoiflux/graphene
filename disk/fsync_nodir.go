//go:build windows || plan9 || js || wasip1

package disk

// Platforms with no directory fsync.
//
// Windows has no handle you can FlushFileBuffers to that means "this
// directory's entries" — CreateFile on a directory needs
// FILE_FLAG_BACKUP_SEMANTICS and the resulting handle does not accept the
// flush. What Windows does instead is stronger in the case that matters here:
// MoveFileEx with MOVEFILE_WRITE_THROUGH, which Go's os.Rename passes for a
// replacing rename, makes the metadata change durable before it returns. plan9,
// js and wasip1 have no equivalent primitive at all.
//
// This is the same shape as lock_unsupported.go: the API is identical across
// platforms and the honesty lives in what the platform-specific file actually
// does, not in a pretence that every platform can do the same thing.

func fsyncDir(string) error { return nil }
