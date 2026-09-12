package disk

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// What VerifyOnOpen costs to run.
//
// The three legs of the check — the digest matches, the roots describe the
// bytes, a named key vouched for the result — each used to open the image for
// itself, and the last two parsed it again from scratch. Four reads and three
// parses of a file that on this engine's target workload is most of the
// machine's memory. That is not a rounding error on an opt-in check; it is the
// reason the check is opt-in, and a check nobody can afford to leave on is one
// that protects nothing.
//
// The legs still run in the same order and still prove the same three things
// separately — the digest matching is no evidence the roots do, which is what
// catches an edit that repaired the digest. They just share one read and one
// parse now, and this file is the guard that says so.

// measureAlloc reports the bytes f allocates.
func measureAlloc(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// verifyImageAt reads the image at path and verifies it, which is what Open
// does in two statements instead of one now that the read is shared with the
// load. Only tests need the pair as a unit.
func verifyImageAt(path string, opts Options) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return verifyImage(&imageSource{data: data}, opts)
}

// verifyOnOpenAllocRatio reports what verifying an image on open allocates as a
// multiple of one read plus one parse of the same image.
//
// Measured against that baseline rather than against the file size, because the
// file size is not the unit the duplication was counted in: deserialiseCSR
// builds the decoded graph, which on a store of small records is many times the
// image, so a ratio against bytes-on-disk moves with the fixture's shape and
// says nothing about how many times the work was done. A multiple of one
// read-and-parse is exactly the quantity in question, and the fixture can then
// change freely.
func verifyOnOpenAllocRatio(t *testing.T, nodes int) (float64, int64) {
	t.Helper()

	dir := t.TempDir()
	key, ring := newAttestKey(t, 1)

	s, err := OpenWithOptions(dir, Options{Signer: key, AttestActorID: 7})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < nodes; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte("b")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	csrPath := filepath.Join(dir, csrFileName)
	fi, err := os.Stat(csrPath)
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	opts := Options{Verifier: ring}

	// Warm once: the first call through this path faults in whatever the
	// runtime lazily builds, and that is not per-image cost.
	if err := verifyImageAt(csrPath, opts); err != nil {
		t.Fatalf("verify image: %v", err)
	}

	baseline := measureAlloc(func() {
		data, err := os.ReadFile(csrPath)
		if err != nil {
			t.Errorf("read image: %v", err)
			return
		}
		csr, section, err := deserialiseCSR(data)
		if err != nil {
			t.Errorf("deserialiseCSR: %v", err)
			return
		}
		runtime.KeepAlive(csr)
		runtime.KeepAlive(section)
	})
	if baseline == 0 {
		t.Fatal("the baseline read-and-parse allocated nothing, which cannot be right")
	}

	got := measureAlloc(func() {
		if err := verifyImageAt(csrPath, opts); err != nil {
			t.Errorf("verify image: %v", err)
		}
	})

	t.Logf("image %d bytes: one read+parse allocates %d, one read+verify allocates %d",
		fi.Size(), baseline, got)
	return float64(got) / float64(baseline), fi.Size()
}

// TestVerifyOnOpen_ReadsAndParsesTheImageOnce separates one read-and-parse from
// the three reads and two parses this replaced.
//
// The ratio is above 1.0 either way and is meant to be: verifying is not only
// reading, and recomputing the snapshot roots allocates a leaf per record on top
// of the parse. Measured on the fixture below that unavoidable remainder puts
// the honest shape near 1.9 and the duplicating one just past 3.0, so the line
// goes between them. What it is really watching for is a shape change, not a
// number: a second os.ReadFile of the image, or a second deserialiseCSR, is
// worth about a whole extra unit here and cannot hide under it.
func TestVerifyOnOpen_ReadsAndParsesTheImageOnce(t *testing.T) {
	ratio, size := verifyOnOpenAllocRatio(t, 4000)
	t.Logf("verifying on open costs %.2f read-and-parses of a %d-byte image", ratio, size)
	if ratio > 2.5 {
		t.Errorf("verifying on open allocates %.2f× one read and parse of the image: it is "+
			"doing one of them more than once, which is what made the check too expensive "+
			"to leave on", ratio)
	}
}

// TestVerifyOnOpen_LegsStillFailSeparately pins the property the shared buffer
// must not quietly cost: the three checks prove different things, and folding
// them together must not let one stand in for another.
//
// A digest recomputed over a tampered body is the case that matters. It passes
// the first leg by construction — the digest is the body's — and only the roots
// can still object.
func TestVerifyOnOpen_LegsStillFailSeparately(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 20; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	if err := verifyImageAt(csrPath, Options{}); err != nil {
		t.Fatalf("the untouched image must verify: %v", err)
	}

	// Edit a record byte well past the header, then restamp the digest so the
	// file vouches for its own new contents.
	edited := append([]byte(nil), data...)
	at := csrV8HeaderSize + 24
	if at >= len(edited) {
		t.Fatalf("image is only %d bytes; too small to edit a record", len(edited))
	}
	edited[at] ^= 0xFF
	digest := computeCSRDigest(edited)
	copy(edited[csrDigestOffset:csrDigestOffset+csrDigestSize], digest[:])

	if status, _ := csrDigestStatus(edited); status != DigestMatch {
		t.Fatalf("the restamped digest should match its own body, got %v", status)
	}
	if err := os.WriteFile(csrPath, edited, 0o600); err != nil {
		t.Fatalf("write edited image: %v", err)
	}

	if err := verifyImageAt(csrPath, Options{}); err == nil {
		t.Error("an edit that repaired the digest was accepted: the roots leg is no longer " +
			"asked its own question, only the digest's")
	}
}

// TestVerifyCSRRootsOf_MatchesThePathTakingForm keeps the split honest. The
// public path-taking verifiers have callers outside Open — the CLI's inspect and
// debug commands and the backup verifier — and the refactor is only safe while
// the two forms cannot disagree.
func TestVerifyCSRRootsOf_MatchesThePathTakingForm(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 12; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "k", []byte("v")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}

	// Clean: both forms accept.
	viaPath := VerifyCSRRoots(csrPath)
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		t.Fatalf("deserialiseCSR: %v", err)
	}
	viaBytes := verifyCSRRootsOf(csr, section)
	if (viaPath == nil) != (viaBytes == nil) {
		t.Fatalf("clean image: path form says %v, parsed form says %v", viaPath, viaBytes)
	}

	// The digest status of the same bytes must agree too, since Open now asks
	// csrDigestStatus what VerifyCSRDigest used to be asked.
	wantStatus, wantDigest, err := VerifyCSRDigest(csrPath)
	if err != nil {
		t.Fatalf("VerifyCSRDigest: %v", err)
	}
	gotStatus, gotDigest := csrDigestStatus(data)
	if gotStatus != wantStatus || gotDigest != wantDigest {
		t.Errorf("csrDigestStatus returned (%v, %x) but VerifyCSRDigest returned (%v, %x)",
			gotStatus, gotDigest[:8], wantStatus, wantDigest[:8])
	}

	// And a directory path must still resolve, which is how the CLI calls it.
	if err := VerifyCSRRoots(dir); err != nil {
		t.Errorf("VerifyCSRRoots no longer accepts a store directory: %v", err)
	}
	if _, _, err := VerifyCSRDigest(dir); err != nil {
		t.Errorf("VerifyCSRDigest no longer accepts a store directory: %v", err)
	}
}
