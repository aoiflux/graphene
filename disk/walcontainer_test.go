package disk

import (
	"os"
	"path/filepath"
	"testing"
)

// The WAL container: a header that versions the framing, and framing that
// checksums the record header it frames.

// A new log gets a header and adopts the stronger framing.
func TestWALContainer_NewLogIsV2(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "new.wal")

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	if w.Framing() != walFramingV2 {
		t.Fatalf("a new log uses framing %d, want %d", w.Framing(), walFramingV2)
	}
	if err := w.AppendNode([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(data[0:4]) != walMagic {
		t.Fatalf("a new log does not begin with the container magic: %q", data[0:4])
	}
}

// **R-F6, closed.** Flipping a record's type byte must now break its checksum.
//
// Under the original framing the CRC covered the payload alone, so changing the
// type left the checksum valid and silently changed what the record meant. The
// consequence BL-10 characterised was a batch-begin marker turning into an
// ordinary record, which made replay apply an uncommitted batch.
func TestWALContainer_V2DetectsATypeByteFlip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "typed.wal")

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendNode([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// The record begins right after the header. Flip its type byte only —
	// nothing else, and no checksum repair.
	data[walFileHeaderSize] ^= 0x01
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	applied := 0
	count := func([]byte) error { applied++; return nil }
	if err := w2.Replay(ReplayCallbacks{
		NodeFunc: count, EdgeFunc: count,
		NodePropFunc: count, EdgePropFunc: count,
		NodeDeleteFunc: count, EdgeDeleteFunc: count,
		NodePropPurgeFunc: count, EdgePropPurgeFunc: count,
	}); err != nil {
		t.Fatalf("replay should treat the record as a torn tail, not error: %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied %d records after a type byte was flipped; under v2 framing "+
			"the checksum covers the type, so the record must not verify", applied)
	}
}

// The same flip under the original framing still verifies, which is the gap
// being closed. Asserted so the difference between the two framings is stated
// rather than assumed.
func TestWALContainer_V1StillMissesATypeByteFlip(t *testing.T) {
	rec := appendRecordFramed(nil, walFramingV1, walRecordNode, []byte("payload"))
	flipped := append([]byte(nil), rec...)
	flipped[0] ^= 0x01 // node -> edge

	if recordCRC(walFramingV1, walRecordNode, rec[walHeaderSize:len(rec)-walFooterSize]) !=
		recordCRC(walFramingV1, walRecordEdge, rec[walHeaderSize:len(rec)-walFooterSize]) {
		t.Fatal("v1 framing should not distinguish record types; if it does, this test is obsolete")
	}
	if recordCRC(walFramingV2, walRecordNode, rec[walHeaderSize:len(rec)-walFooterSize]) ==
		recordCRC(walFramingV2, walRecordEdge, rec[walHeaderSize:len(rec)-walFooterSize]) {
		t.Fatal("v2 framing must distinguish record types")
	}
}

// A log written before the container existed must keep replaying, under its own
// framing, without being rewritten.
func TestWALContainer_HeaderlessLogStillReplays(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "legacy.wal")

	// Hand-build a headerless v1 log, as an older build would have written.
	var legacy []byte
	legacy = appendRecordFramed(legacy, walFramingV1, walRecordNode, []byte("old-1"))
	legacy = appendRecordFramed(legacy, walFramingV1, walRecordNode, []byte("old-2"))
	if err := os.WriteFile(p, legacy, 0600); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.Framing() != walFramingV1 {
		t.Fatalf("an existing headerless log was opened as framing %d, want %d — "+
			"records appended to it would not verify", w.Framing(), walFramingV1)
	}

	var got []string
	if err := w.Replay(ReplayCallbacks{
		NodeFunc: func(b []byte) error { got = append(got, string(b)); return nil },
	}); err != nil {
		t.Fatalf("a legacy log failed to replay: %v", err)
	}
	if len(got) != 2 || got[0] != "old-1" || got[1] != "old-2" {
		t.Fatalf("legacy log replayed as %v, want [old-1 old-2]", got)
	}
}

// Appending to a headerless log keeps its framing, so the file never holds two.
func TestWALContainer_AppendingToALegacyLogKeepsItsFraming(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "legacy.wal")

	legacy := appendRecordFramed(nil, walFramingV1, walRecordNode, []byte("old"))
	if err := os.WriteFile(p, legacy, 0600); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendNode([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var got []string
	if err := w2.Replay(ReplayCallbacks{
		NodeFunc: func(b []byte) error { got = append(got, string(b)); return nil },
	}); err != nil {
		t.Fatalf("replay after appending to a legacy log: %v", err)
	}
	if len(got) != 2 || got[0] != "old" || got[1] != "new" {
		t.Fatalf("got %v, want [old new] — a log must not hold two framings", got)
	}
}

// Compaction is the migration: it truncates the log, which rewrites the header
// and adopts the stronger framing without an explicit step.
func TestWALContainer_TruncateUpgradesALegacyLog(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, walFileName)

	legacy := appendRecordFramed(nil, walFramingV1, walRecordNode, []byte("old"))
	if err := os.WriteFile(p, legacy, 0600); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.Framing() != walFramingV1 {
		t.Fatal("fixture error: the legacy log should open as v1")
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}
	if w.Framing() != walFramingV2 {
		t.Fatalf("after Truncate the log uses framing %d, want %d", w.Framing(), walFramingV2)
	}
	if err := w.AppendNode([]byte("new")); err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := w.Replay(ReplayCallbacks{
		NodeFunc: func(b []byte) error { got = append(got, string(b)); return nil },
	}); err != nil {
		t.Fatalf("replay after the upgrade: %v", err)
	}
	if len(got) != 1 || got[0] != "new" {
		t.Fatalf("got %v, want [new]", got)
	}
}

// A corrupt container header fails the open rather than being read as a
// headerless log — otherwise damaging the header would silently downgrade the
// framing, which is the same shape as the attack v2 exists to stop.
func TestWALContainer_CorruptHeaderIsRejected(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bad.wal")

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendNode([]byte("x")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[10] ^= 0xFF // inside the header, past the magic
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	if w2, err := OpenWAL(p); err == nil {
		w2.Close()
		t.Fatal("a log with a corrupt container header opened successfully")
	}
}

// Inspection reports the framing, so an operator can tell which rule a log's
// checksums follow.
func TestWALContainer_InspectionReportsFraming(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addNodeD(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := InspectWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Framing != walFramingV2 {
		t.Fatalf("inspection reports framing %d, want %d", info.Framing, walFramingV2)
	}
	if len(info.Records) == 0 {
		t.Fatal("no records reported past the container header")
	}
}
