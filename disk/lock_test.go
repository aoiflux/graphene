package disk

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The process lock, and the read-only mode it makes possible.
//
// These run in one process on purpose. Both primitives underneath — flock on
// Unix, LockFileEx on Windows — scope a lock to the open file description rather
// than to the process, so two Opens in one test conflict exactly as two
// processes would. That is not an accident of the implementation; it is why
// flock was chosen over fcntl, whose per-process ownership would make every test
// here pass while the guarantee was absent.
//
// The genuinely cross-process claim is in tests/graphene_multiprocess_test.go,
// which cannot be made from inside one process no matter how it is arranged.

// **The whole point.** A second writer is refused rather than admitted into a
// race whose loser silently serves a graph that no longer exists on disk.
func TestLock_SecondWriterIsRefused(t *testing.T) {
	dir := t.TempDir()

	first, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()

	second, err := Open(dir)
	if err == nil {
		second.Close()
		t.Fatal("a second writer opened a store already held exclusively")
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open: got %v, want ErrStoreLocked", err)
	}
}

// The refusal has to be diagnosable. An operator who cannot tell which process
// holds the store has been told only that they cannot have it.
func TestLock_RefusalNamesTheHolder(t *testing.T) {
	dir := t.TempDir()

	first, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()

	_, err = Open(dir)
	if err == nil {
		t.Fatal("second Open succeeded")
	}
	if o := peekLockOwner(dir); !o.Present {
		t.Fatal("the lock file records no owner")
	} else if o.PID != uint64(os.Getpid()) {
		t.Errorf("recorded pid %d, want this process %d", o.PID, os.Getpid())
	}
	// The pid has to reach the message, not merely the file: the error is what
	// the operator sees.
	if got := err.Error(); !contains(got, fmt.Sprintf("pid %d", os.Getpid())) {
		t.Errorf("error does not name the holder: %s", got)
	}
}

// A store held by readers must not be reported as belonging to whichever writer
// last touched it. Only exclusive holders write the owner record, so reading it
// out unconditionally names a process that has very likely been gone for hours —
// and an operator sent after a dead PID is worse off than one told nothing.
func TestLock_RefusalDoesNotNameADeadWriter(t *testing.T) {
	dir := t.TempDir()

	// A writer that has finished. Its PID stays in the record, marked clean.
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writerPID := os.Getpid()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer r.Close()

	_, err = Open(dir)
	if err == nil {
		t.Fatal("a writer opened a store a reader holds")
	}
	msg := err.Error()
	if contains(msg, fmt.Sprintf("pid %d", writerPID)) {
		t.Errorf("refusal names the departed writer: %s", msg)
	}
	if !contains(msg, "readers") {
		t.Errorf("refusal does not say readers hold the store: %s", msg)
	}
}

// Readers coexist. This is the half of the model that a plain exclusive lock
// would give up.
func TestLock_ReadersCoexist(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, 3)

	const readers = 4
	open := make([]*Store, 0, readers)
	defer func() {
		for _, s := range open {
			s.Close()
		}
	}()

	for i := 0; i < readers; i++ {
		s, err := OpenReadOnly(dir)
		if err != nil {
			t.Fatalf("reader %d refused: %v", i, err)
		}
		open = append(open, s)
		if _, err := s.NodeCount(); err != nil {
			t.Fatalf("reader %d cannot read: %v", i, err)
		}
	}
}

// A reader is refused while a writer holds the store, and a writer while readers
// do. Both directions, because a lock that excludes in only one of them excludes
// nothing.
func TestLock_ReadersAndWritersExcludeEachOther(t *testing.T) {
	t.Run("writer blocks reader", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer w.Close()

		if r, err := OpenReadOnly(dir); err == nil {
			r.Close()
			t.Fatal("a reader opened a store a writer holds")
		} else if !errors.Is(err, ErrStoreLocked) {
			t.Fatalf("got %v, want ErrStoreLocked", err)
		}
	})

	t.Run("reader blocks writer", func(t *testing.T) {
		dir := t.TempDir()
		seedStore(t, dir, 1)

		r, err := OpenReadOnly(dir)
		if err != nil {
			t.Fatalf("OpenReadOnly: %v", err)
		}
		defer r.Close()

		if w, err := Open(dir); err == nil {
			w.Close()
			t.Fatal("a writer opened a store a reader holds")
		} else if !errors.Is(err, ErrStoreLocked) {
			t.Fatalf("got %v, want ErrStoreLocked", err)
		}
	})
}

// Close must actually release. A lock that outlives its store turns one crash
// into a directory nothing can ever open again.
func TestLock_ClosingReleases(t *testing.T) {
	dir := t.TempDir()

	for i := 0; i < 3; i++ {
		s, err := Open(dir)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}
}

// Close is reached twice in real code — a defer plus an explicit call on the
// success path is the common shape — and releasing a lock twice must not turn
// that into an error.
func TestLock_DoubleCloseIsHarmless(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// The second close reports whatever the WAL makes of being closed twice; what
	// it must not do is panic or leave the lock in a state that blocks a reopen.
	_ = s.Close()

	again, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after a double Close: %v", err)
	}
	again.Close()
}

// A rejected Open must not leave the directory locked. The store that fails
// VerifyOnOpen is precisely the one an operator will immediately try to open
// again to find out what is wrong with it.
func TestLock_FailedOpenReleases(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, 4)

	// Corrupt the image so VerifyOnOpen rejects it.
	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read csr: %v", err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(csrPath, data, 0600); err != nil {
		t.Fatalf("write csr: %v", err)
	}

	if s, err := OpenWithOptions(dir, Options{VerifyOnOpen: true}); err == nil {
		s.Close()
		t.Fatal("a corrupted image passed VerifyOnOpen")
	}

	// The directory must be available again — to anyone, including the inspector.
	l, _, err := acquireStoreLock(dir, LockExclusive)
	if err != nil {
		t.Fatalf("the rejected Open kept the lock: %v", err)
	}
	l.release()
}

// --- read-only mode ---

// **The whole point of ReadOnly.** A read-only store must not touch the store's
// files. It used to: OpenWAL creates the log if missing and writes a container
// header into an empty one, and all three ledgers open for append.
func TestReadOnly_LeavesTheDirectoryUntouched(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, 6)

	before := snapshotDir(t, dir)

	s, err := OpenWithOptions(dir, Options{ReadOnly: true, Audit: true, Redaction: true, Roles: true})
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	// Exercise the read paths, which is when a lazily-created file would appear.
	if _, err := s.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if _, err := s.AuditEntries(); err != nil {
		t.Fatalf("AuditEntries: %v", err)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	after := snapshotDir(t, dir)
	if len(before) != len(after) {
		t.Fatalf("file set changed: %v -> %v", keysOf(before), keysOf(after))
	}
	for name, sum := range before {
		got, ok := after[name]
		if !ok {
			t.Errorf("%s disappeared", name)
			continue
		}
		if got != sum {
			t.Errorf("%s was modified by a read-only open (%s -> %s)", name, sum[:16], got[:16])
		}
	}
}

// A read-only store on a directory that has never been written to. There is no
// log to open and none may be created.
func TestReadOnly_StoreWithNoLog(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly on an empty directory: %v", err)
	}
	defer s.Close()

	n, err := s.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if n != 0 {
		t.Errorf("NodeCount = %d, want 0", n)
	}
	if _, err := os.Stat(filepath.Join(dir, walFileName)); !os.IsNotExist(err) {
		t.Error("a read-only open created the WAL")
	}
}

// A read-only open of a directory that does not exist is an error, not an empty
// store: creating it would be a write, and there is nothing there to read.
func TestReadOnly_MissingDirectoryIsAnError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-there")
	if s, err := OpenReadOnly(missing); err == nil {
		s.Close()
		t.Fatal("OpenReadOnly created a store directory")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Error("OpenReadOnly created the directory it was asked to read")
	}
}

// Every exported mutator refuses. Enumerated rather than sampled: a mutator
// added later without a guard is the failure this is here to catch, and a
// sampled test would not see it.
func TestReadOnly_EveryMutatorRefuses(t *testing.T) {
	dir := t.TempDir()
	ids := seedStore(t, dir, 3)

	s, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer s.Close()

	n := &store.Node{ID: ids[0], Labels: []store.NodeType{store.NodeTypeTag}}
	e := &store.Edge{ID: store.EdgeID(1), Labels: []store.EdgeType{store.EdgeTypeContains}}
	req := RedactionRequest{ActorID: 1, Reason: "test"}

	cases := []struct {
		name string
		call func() error
	}{
		{"AddNode", func() error { _, err := s.AddNode(n); return err }},
		{"AddNodesBatch", func() error { _, err := s.AddNodesBatch([]*store.Node{n}); return err }},
		{"AddEdge", func() error { _, err := s.AddEdge(e); return err }},
		{"AddEdgesBatch", func() error { _, err := s.AddEdgesBatch([]*store.Edge{e}); return err }},
		{"UpdateNode", func() error { return s.UpdateNode(n) }},
		{"UpdateEdge", func() error { return s.UpdateEdge(e) }},
		{"DeleteNode", func() error { return s.DeleteNode(ids[0]) }},
		{"DeleteEdge", func() error { return s.DeleteEdge(store.EdgeID(1)) }},
		{"IndexNodeProperty", func() error { return s.IndexNodeProperty(ids[0], "k", []byte("v")) }},
		{"IndexEdgeProperty", func() error { return s.IndexEdgeProperty(store.EdgeID(1), "k", []byte("v")) }},
		{"PurgeNodeIndex", func() error { return s.PurgeNodeIndex(ids[0]) }},
		{"PurgeEdgeIndex", func() error { return s.PurgeEdgeIndex(store.EdgeID(1)) }},
		{"DeclareOrderedNodeProperty", func() error { return s.DeclareOrderedNodeProperty("k") }},
		{"DeclareOrderedEdgeProperty", func() error { return s.DeclareOrderedEdgeProperty("k") }},
		{"RecordAudit", func() error { return s.RecordAudit(AuditCustom, 1, "x") }},
		{"Sync", func() error { return s.Sync() }},
		{"Compact", func() error { return s.Compact() }},
		{"RebuildIndexes", func() error { return s.RebuildIndexes() }},
		{"ApplyTransaction", func() error { return s.ApplyTransaction(nil) }},
		{"ApplyTransactionAs", func() error { return s.ApplyTransactionAs(nil, store.TxContext{}) }},
		{"RotateKey", func() error { return s.RotateKey(nil, nil) }},
		{"RedactNode", func() error { _, err := s.RedactNode(ids[0], req); return err }},
		{"RedactNodeProperties", func() error { _, err := s.RedactNodeProperties(ids[0], req); return err }},
		{"RedactEdge", func() error { _, err := s.RedactEdge(store.EdgeID(1), req); return err }},
		{"RedactEdgeProperties", func() error { _, err := s.RedactEdgeProperties(store.EdgeID(1), req); return err }},
		{"GrantRole", func() error { _, err := s.GrantRole(1, 1, 0, GrantRequest{}); return err }},
		{"RevokeRole", func() error { _, err := s.RevokeRole(1, 1, 0, GrantRequest{}); return err }},
		{"PublishCheckpoint", func() error { _, _, err := s.PublishCheckpoint(nil); return err }},
	}

	for _, c := range cases {
		if err := c.call(); !errors.Is(err, ErrReadOnly) {
			t.Errorf("%s: got %v, want ErrReadOnly", c.name, err)
		}
	}
}

// The accessors have to agree with what was actually asked for; they are how a
// caller reasons about a store handed to it by someone else.
func TestReadOnly_Accessors(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, 1)

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if w.ReadOnly() {
		t.Error("a writable store reports ReadOnly")
	}
	if w.LockMode() != LockExclusive {
		t.Errorf("writable LockMode = %v, want exclusive", w.LockMode())
	}
	w.Close()

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer r.Close()
	if !r.ReadOnly() {
		t.Error("a read-only store does not report ReadOnly")
	}
	if r.LockMode() != LockShared {
		t.Errorf("read-only LockMode = %v, want shared", r.LockMode())
	}
	if !r.LockEnforced() {
		t.Error("LockEnforced is false on a platform the build tags say is supported")
	}

	// The third mode, which LockMode could not report before v0.8.0 because it
	// re-derived the answer from Options.ReadOnly -- which LiveReader implies.
	// The two wrong answers are not equivalent: LockShared says every writer is
	// excluded, and running alongside the writer is the whole of what OpenLive
	// trades its fixed view for. A caller checking the mode to find out whether a
	// writer could be active was told the opposite of the truth.
	l, err := OpenLive(dir)
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer l.Close()
	if !l.ReadOnly() {
		t.Error("a live reader does not report ReadOnly")
	}
	if l.LockMode() != LockNone {
		t.Errorf("live-reader LockMode = %v, want none", l.LockMode())
	}
	if !l.LockEnforced() {
		t.Error("LockEnforced is false for a live reader: it reports whether the platform " +
			"locks at all, not whether this store took one")
	}
}

// --- unclean shutdown ---

// A store closed properly reports a clean previous run; one abandoned without a
// Close reports the opposite. The distinction is the whole content of the
// marker, so both halves are asserted together.
func TestUncleanShutdown_DetectedAndRecorded(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.RecoveredFromUncleanShutdown() {
		t.Error("a first open reports an unclean predecessor; there was none")
	}
	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clean, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if clean.RecoveredFromUncleanShutdown() {
		t.Error("an orderly Close was reported as an unclean shutdown")
	}
	clean.Close()

	// Simulate the process dying: the OS drops the lock, but nothing ever wrote
	// the clean marker. Releasing without markClean is exactly that.
	dying, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("open before the simulated crash: %v", err)
	}
	addNodeD(t, dying, store.NodeTypeTag)
	dying.wal.Close()
	dying.audit.Close()
	dying.lock.release()

	recovered, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("open after the simulated crash: %v", err)
	}
	defer recovered.Close()

	if !recovered.RecoveredFromUncleanShutdown() {
		t.Error("a store abandoned without Close did not report an unclean shutdown")
	}

	entries, err := recovered.AuditEntries()
	if err != nil {
		t.Fatalf("AuditEntries: %v", err)
	}
	var found int
	for _, e := range entries {
		if e.Kind == AuditUncleanRestart {
			found++
		}
	}
	if found != 1 {
		t.Errorf("recorded %d unclean-restart entries, want exactly 1", found)
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Errorf("the new audit kind broke the chain: %v", err)
	}
}

// A read-only store never claims the directory, so it must not overwrite the
// marker a crashed writer left behind — the next *writer* has to still find it.
func TestUncleanShutdown_ReaderDoesNotClearTheMarker(t *testing.T) {
	dir := t.TempDir()

	dying, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	addNodeD(t, dying, store.NodeTypeTag)
	dying.wal.Close()
	dying.lock.release()

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if r.RecoveredFromUncleanShutdown() {
		t.Error("a read-only store claimed to have recovered a directory it never held")
	}
	r.Close()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after the reader: %v", err)
	}
	defer w.Close()
	if !w.RecoveredFromUncleanShutdown() {
		t.Error("the reader consumed the marker the crashed writer left")
	}
}

// --- the owner record ---

func TestLockOwner_RoundTrips(t *testing.T) {
	for _, want := range []lockOwner{
		{Present: true, PID: 1, Clean: false},
		{Present: true, PID: 4294967296, Clean: true},
	} {
		got, ok := parseLockOwner(appendLockOwner(nil, want))
		if !ok {
			t.Fatalf("parse rejected a record it wrote: %+v", want)
		}
		if got != want {
			t.Errorf("round trip: got %+v, want %+v", got, want)
		}
	}
}

// A foreign, truncated or zeroed lock file must read as "no owner" rather than
// as a plausible one. A fabricated PID in an error message is worse than none.
func TestLockOwner_RejectsGarbage(t *testing.T) {
	cases := map[string][]byte{
		"empty":     nil,
		"short":     make([]byte, lockOwnerSize-1),
		"zeroed":    make([]byte, lockOwnerSize),
		"bad magic": append([]byte("XXXX"), make([]byte, lockOwnerSize-4)...),
	}
	for name, b := range cases {
		if o, ok := parseLockOwner(b); ok {
			t.Errorf("%s: parsed as owner %+v", name, o)
		}
	}
}

// --- helpers ---

// seedStore builds a compacted store and returns its node IDs, leaving the
// directory closed and unlocked.
func seedStore(t *testing.T, dir string, n int) []store.NodeID {
	t.Helper()
	s, err := OpenWithOptions(dir, Options{Audit: true, Redaction: true, Roles: true})
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	defer s.Close()

	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, addNodeD(t, s, store.NodeTypeMicroArtefact))
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("seed Compact: %v", err)
	}
	return ids
}

// snapshotDir hashes every file, skipping the lock file — which a read-only open
// is expected to create and write to, and which holds no store data.
//
// Contents rather than sizes: the failure this is guarding against is a write
// that lands in a file that already exists, and the WAL container header — the
// specific write that prompted all of this — is a fixed 50 bytes into a file
// whose size a size-only check would happily accept as unchanged.
func snapshotDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.IsDir() || e.Name() == lockFileName {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			t.Fatalf("read %s: %v", e.Name(), rerr)
		}
		out[e.Name()] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return out
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
