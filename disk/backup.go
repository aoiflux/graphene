package disk

// Online backup: a consistent copy of a store that is still being written.
//
// # What makes a copy consistent
//
// A store on disk is an image plus a log, and Phase 2 gave that pair an
// invariant worth stating plainly: the image holds every commit up to the
// compaction's pin, the log holds every commit after it, and neither holds
// both. A copy of the two is therefore a whole store exactly when both halves
// come from the same side of a compaction.
//
// That is the only hazard. Everything else in the directory is append-only —
// the log, the audit log, the redaction and grant ledgers, the anchor file — so
// a prefix of any of them is a valid shorter version of itself. Compaction is
// the sole operation that rewrites what is already there: it replaces the image
// and then truncates, rebuilds or retires the log. Copy across one of those and
// the result is a store that has lost the window commits (old log, new image) or
// replays them twice (new log, old image).
//
// So a backup does not freeze the store. It excludes compaction, notes where
// the log stands, and copies. Writers continue throughout: they only ever append
// past the noted offset, and the copy stops there.
//
// # Why the log is cut to an offset and nothing else is
//
// The reported commit sequence has to be exact, because it is what a restore's
// point-in-time cut is measured against. Copying whatever the log happened to
// hold when the read reached it would make that figure a lower bound rather than
// a number. The ledgers carry no such promise — they are history, and a few
// extra entries in a backup are extra history, not an inconsistency — so they
// are copied whole.
//
// # What is not copied
//
// The process lock file, which names the pid holding the *source* directory and
// would be a lie about the copy, and any .tmp file, which is a compaction or a
// log rebuild caught mid-flight and is meaningless without the operation that
// was writing it. The open path already ignores both.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aoiflux/graphene/store"
)

// backupManifestName holds the manifest inside a backup directory.
const backupManifestName = "graphene.backup.json"

// backupFormat is the manifest's own version, bumped if its shape changes.
const backupFormat = 1

// ErrBackupInProgress is returned by Compact while a backup is reading the
// directory, and by Backup when another one is already running.
//
// Compaction is the one operation a backup cannot tolerate running underneath
// it, so the two exclude each other. Refusing is right rather than queueing: the
// caller of a compaction is either a scheduler that will come round again or a
// background trigger that will fire on its next tick, and neither is improved by
// blocking for the length of a copy.
var ErrBackupInProgress = errors.New("graphene: a backup is reading this store")

// ErrBackupIncomplete reports a backup directory that does not match its
// manifest — a missing file, a wrong length, or a digest that does not verify.
var ErrBackupIncomplete = errors.New("graphene: the backup does not match its manifest")

// BackupFile is one file in a backup, with the length and digest the manifest
// commits to.
type BackupFile struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	Digest string `json:"digest"` // hex SHA-256 of the copied bytes
}

// BackupInfo describes a backup. It is also the manifest's on-disk shape.
type BackupInfo struct {
	// Format is the manifest version.
	Format int `json:"format"`

	// TakenAt is when the backup was pinned, not when the copy finished.
	TakenAt time.Time `json:"takenAt"`

	// Epoch is the store's visible epoch at the pin, and CommitSeq the commit
	// counter. CommitSeq is the figure a point-in-time restore is expressed in;
	// see RestoreOptions.AtCommitSeq.
	Epoch     uint64 `json:"epoch"`
	CommitSeq uint64 `json:"commitSeq"`

	// ImageCommitSeq is the commit high-water mark folded into the copied image.
	// It is the earliest point this backup can be restored to: everything at or
	// below it is inside the image and cannot be undone by replaying less log.
	ImageCommitSeq uint64 `json:"imageCommitSeq"`

	// WALBytes is where the log was cut. Records past it belong to commits that
	// landed after the pin and are deliberately not in the copy.
	WALBytes int64 `json:"walBytes"`

	Files []BackupFile `json:"files"`

	// Dir is where the backup was written. Not part of the manifest — a backup
	// that is moved is still the same backup.
	Dir string `json:"-"`
}

// Bytes reports the total size of the copied files.
func (b BackupInfo) Bytes() int64 {
	var n int64
	for _, f := range b.Files {
		n += f.Bytes
	}
	return n
}

// Backup writes a consistent copy of the store into dst, which is created if it
// does not exist and must not already hold a store or a backup.
//
// The store stays open and writable throughout. Compaction is refused for the
// duration — a background compactor's tick reports ErrBackupInProgress and tries
// again later — and every other operation is unaffected.
//
// The copy is verified as it is made: each file's digest is computed from the
// bytes actually written, and the image is additionally checked against the
// digest it carries in its own header. A backup that reports success is one
// whose contents have been read back through a hash.
func (s *Store) Backup(dst string) (BackupInfo, error) {
	return s.BackupCtx(context.Background(), dst)
}

// BackupCtx is Backup, abandoned if ctx is cancelled.
//
// Cancellation is checked between files and stops the copy where it is, leaving
// dst holding a partial backup with no manifest. That is deliberate: a backup
// directory without a manifest is not a backup, so a cancelled run cannot be
// mistaken for a complete one — not by an operator, and not by Restore.
func (s *Store) BackupCtx(ctx context.Context, dst string) (BackupInfo, error) {
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	if s.metricsOn() {
		// Wrapped rather than instrumented inline: a backup has a dozen return
		// paths and each one is a real outcome worth counting, so recording at
		// each would be a dozen chances to miss one.
		started := time.Now()
		info, err := s.backupCtx(ctx, dst)
		s.record(store.Metric{
			Kind:     store.MetricBackup,
			Duration: time.Since(started),
			Count:    int64(len(info.Files)),
			Bytes:    info.Bytes(),
			Err:      err,
		})
		return info, err
	}
	return s.backupCtx(ctx, dst)
}

func (s *Store) backupCtx(ctx context.Context, dst string) (BackupInfo, error) {

	src, err := filepath.Abs(s.dir)
	if err != nil {
		return BackupInfo{}, fmt.Errorf("backup: %w", err)
	}
	dest, err := filepath.Abs(dst)
	if err != nil {
		return BackupInfo{}, fmt.Errorf("backup: %w", err)
	}
	if src == dest {
		return BackupInfo{}, errors.New("backup: destination is the store's own directory")
	}

	info, err := s.backupPin()
	if err != nil {
		return BackupInfo{}, err
	}
	defer s.backupRelease()

	if s.afterBackupPinHook != nil {
		s.afterBackupPinHook()
	}

	if err := os.MkdirAll(dest, 0700); err != nil {
		return BackupInfo{}, fmt.Errorf("backup: mkdir %s: %w", dest, err)
	}
	if err := backupDestinationIsFree(dest); err != nil {
		return BackupInfo{}, err
	}

	info.Dir = dest
	for i, f := range info.Files {
		if err := ctx.Err(); err != nil {
			return BackupInfo{}, err
		}
		limit := int64(-1)
		if f.Name == walFileName {
			limit = info.WALBytes
		}
		n, digest, err := copyFileSync(filepath.Join(src, f.Name), filepath.Join(dest, f.Name), limit)
		if err != nil {
			return BackupInfo{}, fmt.Errorf("backup: %s: %w", f.Name, err)
		}
		info.Files[i].Bytes = n
		info.Files[i].Digest = hex.EncodeToString(digest)
	}

	// The image's own digest, checked against the copy rather than the original.
	// Reading it back here is what makes "the backup succeeded" a statement about
	// the bytes in dst and not about the bytes that were sent there.
	imgPath := filepath.Join(dest, csrFileName)
	if _, err := os.Stat(imgPath); err == nil {
		switch status, _, verr := VerifyCSRDigest(imgPath); {
		case verr != nil:
			return BackupInfo{}, fmt.Errorf("backup: verify copied image: %w", verr)
		case status == DigestMismatch:
			return BackupInfo{}, errors.New("backup: the copied image does not match its own digest")
		}
	}

	if err := writeBackupManifest(dest, info); err != nil {
		return BackupInfo{}, err
	}
	if err := syncDir(dest); err != nil {
		return BackupInfo{}, fmt.Errorf("backup: %w", err)
	}

	if aerr := s.recordAudit(AuditBackup, s.attestActorID,
		fmt.Sprintf("backup at commit %d to %s (%d files, %d bytes)",
			info.CommitSeq, dest, len(info.Files), info.Bytes())); aerr != nil {
		return BackupInfo{}, fmt.Errorf("backup: %w", aerr)
	}
	return info, nil
}

// backupPin notes where the store stands and lists what to copy, under the lock,
// and blocks compaction until backupRelease.
//
// The list is settled here rather than during the copy for the same reason the
// log offset is: a segment retired mid-copy would otherwise appear or vanish
// depending on when the directory happened to be read.
func (s *Store) backupPin() (BackupInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.compacting {
		return BackupInfo{}, ErrCompactionInProgress
	}
	if s.backups > 0 {
		return BackupInfo{}, ErrBackupInProgress
	}

	// Quiesce the log so the recorded offset is a record boundary and not
	// wherever a concurrent append had reached. A torn tail would replay
	// harmlessly, but the commit sequence this backup reports would then describe
	// more than the copy actually holds.
	walBytes, err := s.wal.stableSize()
	if err != nil {
		return BackupInfo{}, fmt.Errorf("backup: %w", err)
	}

	info := BackupInfo{
		Format:    backupFormat,
		TakenAt:   s.now(),
		Epoch:     s.visibleEpoch.Load(),
		CommitSeq: s.commitSeq.Load(),
		WALBytes:  walBytes,
	}
	if v := s.viewPtr.Load(); v != nil && v.csr != nil {
		info.ImageCommitSeq = v.csr.commitSeqHW
	}

	names, err := backupFileNames(s.dir)
	if err != nil {
		return BackupInfo{}, err
	}
	for _, n := range names {
		info.Files = append(info.Files, BackupFile{Name: n})
	}

	s.backups++
	return info, nil
}

// backupRelease lets compaction run again however the backup ended.
func (s *Store) backupRelease() {
	s.mu.Lock()
	if s.backups > 0 {
		s.backups--
	}
	s.mu.Unlock()
}

// backupFileNames lists the store files a backup carries, in a fixed order.
//
// Everything the engine writes under dir except the process lock, which
// describes the holder of the *source* directory, and any .tmp, which is an
// operation caught mid-flight rather than a file the store reads.
func backupFileNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("backup: read %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == lockFileName || filepath.Ext(name) == ".tmp" {
			continue
		}
		out = append(out, name)
	}
	// The image and the log first, so a copy that dies partway is missing history
	// rather than missing the graph, and deterministic afterwards so two backups
	// of the same store produce the same manifest order.
	sort.Slice(out, func(i, j int) bool {
		ri, rj := backupFileRank(out[i]), backupFileRank(out[j])
		if ri != rj {
			return ri < rj
		}
		return out[i] < out[j]
	})
	return out, nil
}

func backupFileRank(name string) int {
	switch name {
	case csrFileName:
		return 0
	case walFileName:
		return 1
	}
	return 2
}

// backupDestinationIsFree refuses a destination that already holds a store or a
// backup, so a copy never lands on top of one.
func backupDestinationIsFree(dir string) error {
	for _, name := range []string{csrFileName, walFileName, backupManifestName} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return fmt.Errorf("backup: %s already holds %s", dir, name)
		}
	}
	return nil
}

// copyFileSync copies src to dst, hashing what it writes, and fsyncs before
// returning. A negative limit copies the whole file.
//
// Streamed rather than read whole: an image is the size of the graph, and a
// backup that needs the store's memory footprint again to run is a backup that
// cannot run on the machine that most needs one.
func copyFileSync(src, dst string, limit int64) (int64, []byte, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, nil, err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return 0, nil, err
	}

	var r io.Reader = in
	if limit >= 0 {
		r = io.LimitReader(in, limit)
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), r)
	if err != nil {
		out.Close()
		return 0, nil, err
	}
	if limit >= 0 && n != limit {
		out.Close()
		return 0, nil, fmt.Errorf("copied %d bytes, expected %d — the file shrank while it was being read", n, limit)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return 0, nil, fmt.Errorf("sync %s: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		return 0, nil, err
	}
	return n, h.Sum(nil), nil
}

// writeBackupManifest writes the manifest last, which is what makes its presence
// the signal that a backup is complete.
func writeBackupManifest(dir string, info BackupInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("backup: manifest: %w", err)
	}
	data = append(data, '\n')
	if err := writeFileSync(filepath.Join(dir, backupManifestName), data, 0600); err != nil {
		return fmt.Errorf("backup: manifest: %w", err)
	}
	return nil
}

// ReadBackupManifest reads a backup's manifest without checking the files it
// describes. Use VerifyBackup to check them.
func ReadBackupManifest(dir string) (BackupInfo, error) {
	data, err := os.ReadFile(filepath.Join(dir, backupManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return BackupInfo{}, fmt.Errorf("%w: %s holds no manifest, so it is not a completed backup",
				ErrBackupIncomplete, dir)
		}
		return BackupInfo{}, fmt.Errorf("backup manifest: %w", err)
	}
	var info BackupInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return BackupInfo{}, fmt.Errorf("%w: manifest in %s is unreadable: %v", ErrBackupIncomplete, dir, err)
	}
	if info.Format != backupFormat {
		return BackupInfo{}, fmt.Errorf("%w: manifest format %d, this build writes and reads %d",
			ErrBackupIncomplete, info.Format, backupFormat)
	}
	info.Dir = dir
	return info, nil
}

// VerifyBackup checks a backup directory against its manifest: every file
// present, at the recorded length, hashing to the recorded digest.
//
// This is what a restore runs first, and what an operator can run on its own to
// find out whether an archived backup is still whole — a question that has no
// answer until someone asks it, and the worst moment to first ask is during a
// recovery.
func VerifyBackup(dir string) (BackupInfo, error) {
	info, err := ReadBackupManifest(dir)
	if err != nil {
		return BackupInfo{}, err
	}
	for _, f := range info.Files {
		path := filepath.Join(dir, f.Name)
		fi, err := os.Stat(path)
		if err != nil {
			return info, fmt.Errorf("%w: %s is missing", ErrBackupIncomplete, f.Name)
		}
		if fi.Size() != f.Bytes {
			return info, fmt.Errorf("%w: %s is %d bytes, the manifest says %d",
				ErrBackupIncomplete, f.Name, fi.Size(), f.Bytes)
		}
		got, err := fileDigest(path)
		if err != nil {
			return info, fmt.Errorf("backup verify: %s: %w", f.Name, err)
		}
		if hex.EncodeToString(got) != f.Digest {
			return info, fmt.Errorf("%w: %s hashes to %s…, the manifest says %s…",
				ErrBackupIncomplete, f.Name, hex.EncodeToString(got)[:16], shortHex(f.Digest))
		}
	}
	return info, nil
}

func shortHex(s string) string {
	if len(s) > 16 {
		return s[:16]
	}
	return s
}

func fileDigest(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
