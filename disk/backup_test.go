package disk

// Backup and restore.
//
// The interesting assertions here are not "the bytes copied" — io.Copy does
// that — but the two things a backup is actually promising. That the copy is a
// *store*: an image and a log that between them hold each commit exactly once,
// even though writers never stopped. And that the copy is *checked*: a backup
// that rotted in an archive is caught by VerifyBackup rather than by a restore
// that half works.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// fingerprint is every live node and edge in the store, in a stable order.
// Two stores with the same fingerprint hold the same graph.
func fingerprint(t *testing.T, s *Store) string {
	t.Helper()
	var lines []string
	for i := uint64(1); i <= s.nodeSeq.Load(); i++ {
		n, err := s.GetNode(store.NodeID(i))
		if err != nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("n%d labels=%v props=%q", i, n.Labels, n.Properties))
	}
	for i := uint64(1); i <= s.edgeSeq.Load(); i++ {
		e, err := s.GetEdge(store.EdgeID(i))
		if err != nil {
			continue
		}
		lines = append(lines, fmt.Sprintf("e%d %d->%d labels=%v", i, e.Src, e.Dst, e.Labels))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// txAddNode commits one node in its own transaction, which is what gives it a
// commit sequence number — the unit a recovery point is expressed in.
func txAddNode(t *testing.T, s *Store, label store.NodeType) store.NodeID {
	t.Helper()
	id := s.ReserveNodeID()
	err := s.ApplyTransaction([]store.TxOp{{
		Kind: store.TxOpAddNode,
		Node: &store.Node{ID: id, Labels: []store.NodeType{label}},
	}})
	if err != nil {
		t.Fatalf("ApplyTransaction: %v", err)
	}
	return id
}

func openRestored(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open restored store at %s: %v", dir, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBackup_RestoresTheSameGraph(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	// A store with history on both sides of a compaction: records the image
	// holds and records only the log holds. A backup that got either half wrong
	// would still restore *something*, which is why the fingerprint covers both.
	var ids []store.NodeID
	for i := 0; i < 20; i++ {
		ids = append(ids, txAddNode(t, s, store.NodeTypeEvidenceFile))
	}
	for i := 1; i < len(ids); i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for i := 0; i < 15; i++ {
		ids = append(ids, txAddNode(t, s, store.NodeTypeMicroArtefact))
	}
	if err := s.DeleteNode(ids[3]); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	want := fingerprint(t, s)

	backupDir := filepath.Join(t.TempDir(), "backup")
	info, err := s.Backup(backupDir)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if info.CommitSeq == 0 {
		t.Fatal("the backup reports no commit sequence")
	}
	if len(info.Files) < 2 {
		t.Fatalf("the backup carries %d files, expected at least the image and the log", len(info.Files))
	}

	if _, err := VerifyBackup(backupDir); err != nil {
		t.Fatalf("VerifyBackup on a backup just written: %v", err)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	out, err := Restore(backupDir, restoreDir, RestoreOptions{})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if out.CommitSeq != info.CommitSeq {
		t.Fatalf("restored to commit %d, the backup holds %d", out.CommitSeq, info.CommitSeq)
	}
	if out.DroppedRecords != 0 {
		t.Fatalf("a full restore dropped %d records", out.DroppedRecords)
	}

	r := openRestored(t, restoreDir)
	if got := fingerprint(t, r); got != want {
		t.Fatalf("the restored graph differs from the source:\nwant:\n%s\ngot:\n%s", want, got)
	}
	if err := r.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes on the restored store: %v", err)
	}
}

// TestBackup_IsConsistentWithWritesLandingDuringTheCopy is the headline.
//
// The store is never frozen, so commits land while files are being read. The
// copy has to hold every commit made before the pin and none made after it —
// not "roughly", exactly, because the log offset in the manifest is what a
// recovery point is later measured against.
func TestBackup_IsConsistentWithWritesLandingDuringTheCopy(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	var before []store.NodeID
	for i := 0; i < 30; i++ {
		before = append(before, txAddNode(t, s, store.NodeTypeEvidenceFile))
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for i := 0; i < 10; i++ {
		before = append(before, txAddNode(t, s, store.NodeTypeEvidenceFile))
	}

	// Commits that land inside the window the backup opens, on the same
	// goroutine so the ordering is a fact rather than a race the test hopes for.
	var during []store.NodeID
	s.afterBackupPinHook = func() {
		for i := 0; i < 12; i++ {
			during = append(during, txAddNode(t, s, store.NodeTypeMicroArtefact))
		}
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	s.afterBackupPinHook = nil

	restoreDir := filepath.Join(t.TempDir(), "restored")
	if _, err := Restore(backupDir, restoreDir, RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	r := openRestored(t, restoreDir)

	for _, id := range before {
		if _, err := r.GetNode(id); err != nil {
			t.Fatalf("node %d was committed before the pin and is not in the backup: %v", id, err)
		}
	}
	for _, id := range during {
		if _, err := r.GetNode(id); err == nil {
			t.Fatalf("node %d was committed after the pin and is in the backup", id)
		}
	}
	if err := r.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes on the restored store: %v", err)
	}
}

// TestBackup_AndCompactionRefuseEachOther: compaction is the only operation that
// rewrites bytes already on disk, so it is the only one a backup has to exclude.
func TestBackup_AndCompactionRefuseEachOther(t *testing.T) {
	t.Run("compaction refused during a backup", func(t *testing.T) {
		s, _ := openFresh(t)
		defer s.Close()
		for i := 0; i < 8; i++ {
			txAddNode(t, s, store.NodeTypeEvidenceFile)
		}

		var got error
		s.afterBackupPinHook = func() { got = s.Compact() }
		if _, err := s.Backup(filepath.Join(t.TempDir(), "backup")); err != nil {
			t.Fatalf("Backup: %v", err)
		}
		s.afterBackupPinHook = nil
		if !errors.Is(got, ErrBackupInProgress) {
			t.Fatalf("Compact during a backup: want ErrBackupInProgress, got %v", got)
		}

		// And it works again the moment the backup is done, so the refusal is a
		// window and not a latch.
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact after the backup: %v", err)
		}
	})

	t.Run("backup refused during a compaction", func(t *testing.T) {
		s, _ := openFresh(t)
		defer s.Close()
		for i := 0; i < 8; i++ {
			txAddNode(t, s, store.NodeTypeEvidenceFile)
		}

		var got error
		s.afterPinHook = func() { _, got = s.Backup(filepath.Join(t.TempDir(), "backup")) }
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		s.afterPinHook = nil
		if !errors.Is(got, ErrCompactionInProgress) {
			t.Fatalf("Backup during a compaction: want ErrCompactionInProgress, got %v", got)
		}
	})
}

func TestBackup_VerifyCatchesAnAlteredFile(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	for i := 0; i < 12; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for i := 0; i < 5; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}

	for _, target := range []string{csrFileName, walFileName} {
		t.Run(target, func(t *testing.T) {
			backupDir := filepath.Join(t.TempDir(), "backup")
			if _, err := s.Backup(backupDir); err != nil {
				t.Fatalf("Backup: %v", err)
			}

			path := filepath.Join(backupDir, target)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", target, err)
			}
			if len(data) == 0 {
				t.Skipf("%s is empty in this backup", target)
			}
			// A single flipped bit, in the middle where a length-only check would
			// never look.
			data[len(data)/2] ^= 0x01
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatalf("write %s: %v", target, err)
			}

			if _, err := VerifyBackup(backupDir); !errors.Is(err, ErrBackupIncomplete) {
				t.Fatalf("VerifyBackup on an altered %s: want ErrBackupIncomplete, got %v", target, err)
			}
			restoreDir := filepath.Join(t.TempDir(), "restored")
			if _, err := Restore(backupDir, restoreDir, RestoreOptions{}); !errors.Is(err, ErrBackupIncomplete) {
				t.Fatalf("Restore from an altered %s: want ErrBackupIncomplete, got %v", target, err)
			}
		})
	}
}

func TestBackup_VerifyCatchesAMissingFile(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	for i := 0; i < 6; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if err := os.Remove(filepath.Join(backupDir, csrFileName)); err != nil {
		t.Fatalf("remove image: %v", err)
	}
	if _, err := VerifyBackup(backupDir); !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("VerifyBackup with the image removed: want ErrBackupIncomplete, got %v", err)
	}
}

// TestBackup_ACancelledCopyIsNotABackup: the manifest is written last, so a
// directory without one cannot be restored from however complete it looks.
func TestBackup_ACancelledCopyIsNotABackup(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	for i := 0; i < 10; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	backupDir := filepath.Join(t.TempDir(), "backup")
	s.afterBackupPinHook = cancel
	_, err := s.BackupCtx(ctx, backupDir)
	s.afterBackupPinHook = nil
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BackupCtx with a cancelled context: want context.Canceled, got %v", err)
	}

	if _, err := VerifyBackup(backupDir); !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("VerifyBackup on a cancelled backup: want ErrBackupIncomplete, got %v", err)
	}
	// And it is refused at the *first* check, because the manifest is written
	// last and so is not there at all. The digests would catch a partial copy
	// anyway — a manifest commits to lengths that only exist once the copy is
	// done — but that is the second line, not the first.
	if _, err := os.Stat(filepath.Join(backupDir, backupManifestName)); !os.IsNotExist(err) {
		t.Fatalf("a cancelled backup left a manifest behind (stat: %v)", err)
	}
	// And the store is unharmed: the refusal was released.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after a cancelled backup: %v", err)
	}
}

func TestBackup_RefusesADestinationThatHoldsAStore(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	txAddNode(t, s, store.NodeTypeEvidenceFile)

	other, odir := openFresh(t)
	txAddNode(t, other, store.NodeTypeEvidenceFile)
	if err := other.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := s.Backup(odir); err == nil {
		t.Fatal("Backup wrote over a directory that already holds a store")
	}
	if _, err := s.Backup(s.dir); err == nil {
		t.Fatal("Backup wrote into the store's own directory")
	}
}

func TestRestore_RefusesANonEmptyDestination(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	txAddNode(t, s, store.NodeTypeEvidenceFile)

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst, "something"), []byte("x"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Restore(backupDir, dst, RestoreOptions{}); !errors.Is(err, ErrRestoreDestinationInUse) {
		t.Fatalf("Restore into a non-empty directory: want ErrRestoreDestinationInUse, got %v", err)
	}
}

// --- Point in time ---

func TestRestore_ToACommitSequence(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	// Twelve commits, each one node, so the graph's size names the point it was
	// restored to and an off-by-one cannot hide.
	ids := make([]store.NodeID, 0, 12)
	seqs := make([]uint64, 0, 12)
	for i := 0; i < 12; i++ {
		ids = append(ids, txAddNode(t, s, store.NodeTypeEvidenceFile))
		seqs = append(seqs, s.commitSeq.Load())
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	const keep = 7 // restore to the 7th commit
	restoreDir := filepath.Join(t.TempDir(), "restored")
	out, err := Restore(backupDir, restoreDir, RestoreOptions{AtCommitSeq: seqs[keep-1]})
	if err != nil {
		t.Fatalf("Restore at commit %d: %v", seqs[keep-1], err)
	}
	if out.CommitSeq != seqs[keep-1] {
		t.Fatalf("restored to commit %d, asked for %d", out.CommitSeq, seqs[keep-1])
	}
	if out.WALBytes >= out.From.WALBytes {
		t.Fatalf("the log was not cut: %d bytes, the backup holds %d", out.WALBytes, out.From.WALBytes)
	}

	r := openRestored(t, restoreDir)
	for i, id := range ids {
		_, err := r.GetNode(id)
		if i < keep && err != nil {
			t.Fatalf("node %d was committed at %d, at or below the recovery point, and is missing: %v",
				id, seqs[i], err)
		}
		if i >= keep && err == nil {
			t.Fatalf("node %d was committed at %d, past the recovery point, and is present", id, seqs[i])
		}
	}
	if err := r.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes on the restored store: %v", err)
	}

	// The restored store is an ordinary store: it writes, and its commit
	// numbering resumes past what it holds rather than colliding with it.
	next := txAddNode(t, r, store.NodeTypeMicroArtefact)
	if r.commitSeq.Load() <= out.CommitSeq {
		t.Fatalf("a write to the restored store reused commit %d", r.commitSeq.Load())
	}
	if _, err := r.GetNode(next); err != nil {
		t.Fatalf("the restored store did not accept a write: %v", err)
	}
}

func TestRestore_ToATime(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	// A pinned clock, so the test asserts the cut and not the machine's timing.
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	var tick int64
	s.nowUnixNano = func() int64 {
		tick++
		return base.Add(time.Duration(tick) * time.Minute).UnixNano()
	}

	ids := make([]store.NodeID, 0, 10)
	for i := 0; i < 10; i++ {
		ids = append(ids, txAddNode(t, s, store.NodeTypeEvidenceFile))
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Commits are stamped one minute apart from base+1m, so this keeps six.
	cutoff := base.Add(6*time.Minute + 30*time.Second)
	restoreDir := filepath.Join(t.TempDir(), "restored")
	out, err := Restore(backupDir, restoreDir, RestoreOptions{AtTime: cutoff})
	if err != nil {
		t.Fatalf("Restore at %s: %v", cutoff, err)
	}
	if out.CommitTime.After(cutoff) {
		t.Fatalf("restored to %s, past the requested %s", out.CommitTime, cutoff)
	}

	r := openRestored(t, restoreDir)
	for i, id := range ids {
		_, err := r.GetNode(id)
		if i < 6 && err != nil {
			t.Fatalf("node %d was committed at %s, before the cutoff, and is missing",
				id, base.Add(time.Duration(i+1)*time.Minute))
		}
		if i >= 6 && err == nil {
			t.Fatalf("node %d was committed at %s, after the cutoff, and is present",
				id, base.Add(time.Duration(i+1)*time.Minute))
		}
	}
}

// TestRestore_APointInsideTheImageIsRefused: compaction is not reversible, so a
// backup can only be rewound as far as the log it carries. Saying so is the
// point — the alternative is a restore that silently lands somewhere else.
func TestRestore_APointInsideTheImageIsRefused(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	for i := 0; i < 8; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	early := s.commitSeq.Load()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for i := 0; i < 4; i++ {
		txAddNode(t, s, store.NodeTypeMicroArtefact)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	info, err := s.Backup(backupDir)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if info.ImageCommitSeq < early {
		t.Fatalf("the image folded in commits up to %d but reports %d", early, info.ImageCommitSeq)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	_, err = Restore(backupDir, restoreDir, RestoreOptions{AtCommitSeq: early - 1})
	if !errors.Is(err, ErrRestorePointTooEarly) {
		t.Fatalf("Restore to a point inside the image: want ErrRestorePointTooEarly, got %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(info.ImageCommitSeq)) {
		t.Fatalf("the refusal does not name the earliest reachable commit %d: %v", info.ImageCommitSeq, err)
	}
}

// TestRestore_ReportsRecordsTheCutDropped: the single-record mutators carry no
// commit sequence, so a cut takes them with the commit that follows. That is a
// real limit and the figure that makes it visible is worth asserting.
func TestRestore_ReportsRecordsTheCutDropped(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	keepID := txAddNode(t, s, store.NodeTypeEvidenceFile)
	keepSeq := s.commitSeq.Load()

	// Written through AddNode, which is not a transaction: no sequence number,
	// no timestamp, nothing for a recovery point to name.
	var loose []store.NodeID
	for i := 0; i < 5; i++ {
		loose = append(loose, addNodeD(t, s, store.NodeTypeMicroArtefact))
	}
	txAddNode(t, s, store.NodeTypeEvidenceFile)

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	out, err := Restore(backupDir, restoreDir, RestoreOptions{AtCommitSeq: keepSeq})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if out.DroppedRecords != len(loose)+1 {
		t.Fatalf("the cut dropped %d records, expected %d unbatched plus the commit after them",
			out.DroppedRecords, len(loose)+1)
	}

	r := openRestored(t, restoreDir)
	if _, err := r.GetNode(keepID); err != nil {
		t.Fatalf("the node committed at the recovery point is missing: %v", err)
	}
	for _, id := range loose {
		if _, err := r.GetNode(id); err == nil {
			t.Fatalf("node %d was written after the recovery point and is present", id)
		}
	}
}

// TestBackup_FromAReadOnlyStore: a shared-lock store cannot be written to and
// cannot compact, so its directory is static and a copy of it is trivially
// consistent. Refusing would be refusing the easiest case.
func TestBackup_FromAReadOnlyStore(t *testing.T) {
	s, dir := openFresh(t)
	for i := 0; i < 10; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for i := 0; i < 4; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	want := fingerprint(t, s)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer ro.Close()

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := ro.Backup(backupDir); err != nil {
		t.Fatalf("Backup from a read-only store: %v", err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	if _, err := Restore(backupDir, restoreDir, RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	r := openRestored(t, restoreDir)
	if got := fingerprint(t, r); got != want {
		t.Fatalf("the restored graph differs:\nwant:\n%s\ngot:\n%s", want, got)
	}
}

// TestBackup_RecordsItselfInTheAuditLog: a backup is an operator action, and the
// restored store's chain says where it came from.
func TestBackup_RecordsItselfInTheAuditLog(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()
	for i := 0; i < 6; i++ {
		txAddNode(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.RecordAudit(AuditCustom, 7, "history written before the backup"); err != nil {
		t.Fatalf("RecordAudit: %v", err)
	}

	backupDir := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(backupDir); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	entries, err := s.AuditEntries()
	if err != nil {
		t.Fatalf("AuditEntries: %v", err)
	}
	if !hasAuditKind(entries, AuditBackup) {
		t.Fatal("the backup left no audit entry")
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	if _, err := Restore(backupDir, restoreDir, RestoreOptions{ActorID: 42}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restored, err := ReadAuditLog(restoreDir)
	if err != nil {
		t.Fatalf("ReadAuditLog on the restored store: %v", err)
	}
	if !hasAuditKind(restored, AuditRestore) {
		t.Fatal("the restored store's audit log does not say it is a restore")
	}
	// The chain continues rather than restarting: the history the backup copied
	// is still in front of the restore entry.
	//
	// Not the AuditBackup entry itself — that is recorded on the *source* once
	// the copy has succeeded, which is after the audit log was read, and
	// recording it earlier would mean a chain claiming a backup that then failed.
	if !hasAuditKind(restored, AuditCustom) {
		t.Fatal("the restored audit log lost the history it was copied with")
	}
	if err := VerifyAuditChain(restored); err != nil {
		t.Fatalf("the restored audit chain does not verify: %v", err)
	}
}

func hasAuditKind(entries []AuditEntry, kind AuditKind) bool {
	for _, e := range entries {
		if e.Kind == kind {
			return true
		}
	}
	return false
}
