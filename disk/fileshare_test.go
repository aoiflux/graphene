package disk

// The two primitives a live reader needs from the filesystem, asserted against
// the behaviour they are named for rather than against the call they make.
//
// These are the tests that fail on Windows if openSharedRead drops
// FILE_SHARE_DELETE or replaceFile stops falling back to ReplaceFileW, and pass
// trivially on Unix — which is the point. The contract is one contract; only its
// implementation is per-platform.

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

// A file held by openSharedRead can still be renamed away — this is Rotate
// moving the active log out from under a reader.
func TestSharedReadDoesNotBlockARenameAway(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "held")
	dst := filepath.Join(dir, "moved")
	writeTemp(t, src, "contents")

	f, err := openSharedRead(src)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("rename away with a shared-read handle open: %v", err)
	}

	// And the handle still reads what it opened, which is the half of the
	// contract that makes the rename safe rather than merely permitted.
	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read through the handle after the rename: %v", err)
	}
	if string(buf) != "contents" {
		t.Errorf("read %q through the moved handle, want %q", buf, "contents")
	}
}

// A file held by openSharedRead can still be replaced — this is truncateFrom
// installing the rebuilt log while a reader is replaying the old one.
func TestReplaceFileSucceedsWithTheDestinationHeldOpen(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "log")
	src := filepath.Join(dir, "log.tmp")
	writeTemp(t, dst, "old")
	writeTemp(t, src, "new")

	f, err := openSharedRead(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := replaceFile(src, dst); err != nil {
		t.Fatalf("replaceFile with the destination held open: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("destination holds %q, want %q", got, "new")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("the replacement is still at its own name: %v", err)
	}
}

// With nobody holding it, replaceFile is a plain rename and behaves like one.
func TestReplaceFileWithNothingHoldingIt(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "log")
	src := filepath.Join(dir, "log.tmp")
	writeTemp(t, dst, "old")
	writeTemp(t, src, "new")

	if err := replaceFile(src, dst); err != nil {
		t.Fatalf("replaceFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("destination holds %q, want %q", got, "new")
	}
}

// The three states adoptOrphanedLog has to tell apart. Only one of them is a
// recovery; the other two must leave the log alone.
func TestAdoptOrphanedLog(t *testing.T) {
	t.Run("no replacement pending", func(t *testing.T) {
		dir := t.TempDir()
		log := filepath.Join(dir, walFileName)
		writeTemp(t, log, "live")

		if err := adoptOrphanedLog(log); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "live" {
			t.Errorf("log holds %q, want %q", got, "live")
		}
	})

	t.Run("replacement beside an intact log is discarded", func(t *testing.T) {
		dir := t.TempDir()
		log := filepath.Join(dir, walFileName)
		writeTemp(t, log, "live")
		writeTemp(t, log+walTmpSuffix, "never installed")

		if err := adoptOrphanedLog(log); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(log)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "live" {
			t.Errorf("an uncommitted replacement was installed: log holds %q", got)
		}
		if _, err := os.Stat(log + walTmpSuffix); !os.IsNotExist(err) {
			t.Errorf("the discarded replacement is still there: %v", err)
		}
	})

	t.Run("replacement with no log is adopted", func(t *testing.T) {
		dir := t.TempDir()
		log := filepath.Join(dir, walFileName)
		writeTemp(t, log+walTmpSuffix, "rebuilt")

		if err := adoptOrphanedLog(log); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(log)
		if err != nil {
			t.Fatalf("the rebuilt log was not adopted: %v", err)
		}
		if string(got) != "rebuilt" {
			t.Errorf("log holds %q, want %q", got, "rebuilt")
		}
	})
}

// The recovery, end to end: a store whose log was lost inside replaceFile's
// window comes back holding the commits the rebuild was carrying.
func TestAStoreRecoversALogLostMidReplace(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := addNodes(t, w, 4, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Exactly the state replaceFile can be interrupted in: the replacement is
	// complete and fsynced under its temporary name, and the destination is
	// gone.
	log := filepath.Join(dir, walFileName)
	body, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(log+walTmpSuffix, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(log); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after an interrupted replace: %v", err)
	}
	defer s.Close()
	mustHave(t, s, ids, "after recovery")
}
