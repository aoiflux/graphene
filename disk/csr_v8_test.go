package disk

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// CSR v8: the section directory, the body digest, and what survives compaction.

func v8Fixture(t *testing.T) (dir string, s *Store) {
	t.Helper()
	dir = t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.IndexNodeProperty(id, "sha256", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	return dir, s
}

// A written image carries a digest that describes it.
func TestCSRv8_DigestMatchesWhatWasWritten(t *testing.T) {
	dir, s := v8Fixture(t)
	defer s.Close()

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	status, _, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Fatalf("digest status %v, want match", status)
	}
}

// Any change to the body breaks the digest. This is what R-C3 was about: before
// v8, a silently corrupted body passed Open with nothing to detect it.
func TestCSRv8_DigestDetectsBodyTampering(t *testing.T) {
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit deep in the record region, well past the header.
	data[csrV8HeaderSize+40] ^= 0x01
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	status, _, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMismatch {
		t.Fatalf("digest status %v after flipping one bit in the body; want mismatch", status)
	}
}

// The compaction timestamp is outside the digest, so touching it does not break
// verification — and, more importantly, recompaction does not either.
func TestCSRv8_TimestampIsOutsideTheDigest(t *testing.T) {
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(data[csrLastCompactOffset:csrLastCompactOffset+8], 1234567890)
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	status, _, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Fatalf("changing the compaction time broke the digest (%v); it is not content "+
			"and must be excluded, or recompaction would change a store's identity", status)
	}
}

// A file predating v8 has no digest, and that is reported as absent rather than
// as a failure.
func TestCSRv8_PreV8FileReportsAbsentDigest(t *testing.T) {
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(data[4:6], 7) // claim v7
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	status, _, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestAbsent {
		t.Fatalf("digest status %v for a pre-v8 file, want absent", status)
	}
}

// The commit sequence and the last compaction time now survive the truncation
// that compaction performs. Before v8 both were lost: the counter restarted from
// whatever the surviving log replayed, and the compaction time was forgotten.
func TestCSRv8_CommitSequenceSurvivesCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Several commits, so the counter is meaningfully above zero.
	for i := 0; i < 4; i++ {
		if err := s.ApplyTransactionAs(
			[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
				ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
			store.TxContext{ActorID: 7},
		); err != nil {
			t.Fatal(err)
		}
	}
	before := s.commitSeq.Load()
	if before == 0 {
		t.Fatal("commit sequence did not advance")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Compaction truncated the log, so nothing in it carries the sequence now.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if got := s2.commitSeq.Load(); got < before {
		t.Fatalf("commit sequence restarted at %d after compaction and reopen; it was %d. "+
			"Sequence numbers must never be reissued", got, before)
	}
	st := s2.StorageStats()
	if st.LastCompact.IsZero() {
		t.Fatal("last compaction time did not survive the reopen")
	}
}

// An unknown OPTIONAL section is skipped: a reader that does not understand it
// still answers every query correctly.
func TestCSRv8_UnknownOptionalSectionIsSkipped(t *testing.T) {
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, csrFileName)
	data := addSection(t, p, "GZZZ", 0)

	if _, _, err := deserialiseCSR(data); err != nil {
		t.Fatalf("an unknown optional section should be skipped, got: %v", err)
	}
}

// An unknown CRITICAL section is refused. A build that cannot interpret a
// signature or attestation section must not present the file as though the
// section were absent — the same rule knownWALRecord applies to record types.
func TestCSRv8_UnknownCriticalSectionIsRefused(t *testing.T) {
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, csrFileName)
	data := addSection(t, p, "GZZZ", csrSectionCritial)

	_, _, err := deserialiseCSR(data)
	if err == nil {
		t.Fatal("a critical section this build does not understand was accepted")
	}
}

// addSection rewrites the directory of the image at path to include one more
// entry, and returns the new bytes.
func addSection(t *testing.T, path, magic string, flags uint32) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tableOff := binary.LittleEndian.Uint64(data[62:70])
	existing, err := readCSRSectionDirectory(data, tableOff)
	if err != nil {
		t.Fatal(err)
	}
	// Point the new section at a zero-length span inside the file so its bounds
	// check passes; the test is about the flags, not the payload.
	existing = append(existing, csrSection{Magic: magic, Flags: flags, Offset: tableOff, Length: 0})

	out := append([]byte(nil), data[:tableOff]...)
	out = appendSectionDirectory(out, existing)
	binary.LittleEndian.PutUint64(out[62:70], tableOff)

	digest := computeCSRDigest(out)
	copy(out[csrDigestOffset:csrDigestOffset+csrDigestSize], digest[:])
	return out
}

// The section directory's own count is bounded, like every other length prefix
// read from a file.
func TestCSRv8_SectionDirectoryCountIsBounded(t *testing.T) {
	// A directory claiming far more sections than the remaining bytes could hold.
	buf := make([]byte, 128)
	binary.LittleEndian.PutUint16(buf[64:66], 0xFFFF)
	if _, err := readCSRSectionDirectory(buf, 64); err == nil {
		t.Fatal("a directory claiming 65535 sections in 62 remaining bytes was accepted")
	}

	// A directory whose offset is outside the file.
	if _, err := readCSRSectionDirectory(buf, 1<<40); err == nil {
		t.Fatal("a directory offset past the end of the file was accepted")
	}

	// A section spanning past the end of the file.
	buf2 := make([]byte, 128)
	binary.LittleEndian.PutUint16(buf2[64:66], 1)
	copy(buf2[66:70], "GZZZ")
	binary.LittleEndian.PutUint64(buf2[74:82], 100)   // offset
	binary.LittleEndian.PutUint64(buf2[82:90], 1<<40) // length
	if _, err := readCSRSectionDirectory(buf2, 64); err == nil {
		t.Fatal("a section extending past the end of the file was accepted")
	}
}
