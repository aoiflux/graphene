package disk

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// Commit signing: what it detects, and what it does not.

func signedStore(t *testing.T) (dir string, key *signing.Key, ring *signing.Keyring) {
	t.Helper()
	dir = t.TempDir()

	key, pub, err := signing.GenerateKey(7)
	if err != nil {
		t.Fatal(err)
	}
	ring = signing.NewKeyring()
	if err := ring.Add(7, pub); err != nil {
		t.Fatal(err)
	}
	return dir, key, ring
}

// writeSigned writes n transactions under key and closes the store.
func writeSigned(t *testing.T, dir string, key *signing.Key, n int) {
	t.Helper()
	s, err := OpenWithOptions(dir, Options{Signer: key})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := s.ApplyTransactionAs(
			[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
				ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
			store.TxContext{ActorID: 42},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A signed log replays and verifies.
func TestSigning_SignedLogVerifies(t *testing.T) {
	dir, key, ring := signedStore(t)
	writeSigned(t, dir, key, 3)

	s, err := OpenWithOptions(dir, Options{
		Signer: key, Verifier: ring, RequireSignedCommits: true,
	})
	if err != nil {
		t.Fatalf("a correctly signed log failed to open under verification: %v", err)
	}
	defer s.Close()

	n, _ := s.NodeCount()
	if n != 3 {
		t.Fatalf("node count %d after replaying 3 signed transactions", n)
	}
}

// **The property signing exists for.** Editing a committed record must make the
// signature fail, because the signature covers a hash of the batch body.
func TestSigning_DetectsAnEditedRecord(t *testing.T) {
	dir, key, ring := signedStore(t)
	writeSigned(t, dir, key, 2)

	p := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Find a node record's payload and change it. Walk the framing rather than
	// guessing an offset, starting past the container header.
	off, patched := walFileHeaderSize, false
	for off+walHeaderSize <= len(data) && !patched {
		recType := data[off]
		length := int(data[off+1]) | int(data[off+2])<<8 | int(data[off+3])<<16 | int(data[off+4])<<24
		if recType == walRecordNode && length > 0 {
			payloadAt := off + walHeaderSize
			data[payloadAt] ^= 0x01
			// Repair the record's own CRC so replay reaches the batch check
			// rather than stopping at a torn record.
			crc := recordCRC(walFramingV2, recType, data[payloadAt:payloadAt+length])
			crcAt := payloadAt + length
			data[crcAt] = byte(crc)
			data[crcAt+1] = byte(crc >> 8)
			data[crcAt+2] = byte(crc >> 16)
			data[crcAt+3] = byte(crc >> 24)
			patched = true
		}
		off += walRecordOverhead + length
	}
	if !patched {
		t.Fatal("fixture error: no node record found to edit")
	}
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	// The batch CRC catches this one too, which is expected — it is the cheap
	// check. The point is that the store refuses to come up.
	s, err := OpenWithOptions(dir, Options{Verifier: ring, RequireSignedCommits: true})
	if err == nil {
		s.Close()
		t.Fatal("a store with an edited signed record opened successfully")
	}
}

// A forged signature must be rejected, as an error rather than as a torn tail.
func TestSigning_RejectsAForgedSignature(t *testing.T) {
	dir, key, ring := signedStore(t)
	writeSigned(t, dir, key, 1)

	p := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// The commit is the last record; corrupt a signature byte and repair the
	// record CRC so the forgery reaches the signature check.
	commitPayloadAt := len(data) - walFooterSize - walBatchCommitPayloadV3
	data[commitPayloadAt+50] ^= 0x01
	crc := recordCRC(walFramingV2, walRecordBatchCommit, data[commitPayloadAt:commitPayloadAt+walBatchCommitPayloadV3])
	crcAt := len(data) - walFooterSize
	data[crcAt] = byte(crc)
	data[crcAt+1] = byte(crc >> 8)
	data[crcAt+2] = byte(crc >> 16)
	data[crcAt+3] = byte(crc >> 24)
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	s, err := OpenWithOptions(dir, Options{Verifier: ring, RequireSignedCommits: true})
	if err == nil {
		s.Close()
		t.Fatal("a forged signature was accepted")
	}
	if !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("expected a signature failure, got: %v", err)
	}
}

// **The downgrade.** Stripping the signature turns a v3 commit into a v2 one,
// which verification would otherwise skip entirely. RequireSignedCommits is what
// makes signing worth anything against an attacker with file access.
func TestSigning_RequireSignedRejectsAStrippedSignature(t *testing.T) {
	dir, key, ring := signedStore(t)
	writeSigned(t, dir, key, 1)

	p := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}

	// Rewrite the commit record as an unsigned v2 one: same fields, shorter
	// payload, recomputed length and CRC. Exactly what an attacker would do.
	commitStart := len(data) - walRecordOverhead - walBatchCommitPayloadV3
	payloadAt := commitStart + walHeaderSize
	v2 := append([]byte(nil), data[payloadAt:payloadAt+walBatchCommitPayloadV2]...)
	stripped := append([]byte(nil), data[:commitStart]...)
	stripped = appendRecordFramed(stripped, walFramingV2, walRecordBatchCommit, v2)
	if err := os.WriteFile(p, stripped, 0600); err != nil {
		t.Fatal(err)
	}

	// Without the requirement the downgrade succeeds — which is the whole point
	// of the flag existing.
	lenient, err := OpenWithOptions(dir, Options{Verifier: ring})
	if err != nil {
		t.Fatalf("a stripped commit should replay when signatures are not required: %v", err)
	}
	lenient.Close()

	// With it, the log is refused.
	strict, err := OpenWithOptions(dir, Options{Verifier: ring, RequireSignedCommits: true})
	if err == nil {
		strict.Close()
		t.Fatal("a stripped signature was accepted under RequireSignedCommits — " +
			"an attacker could downgrade every commit and verification would pass")
	}
}

// A signature made by the wrong key is rejected, and the error distinguishes
// "unknown key" from "bad signature".
func TestSigning_UnknownKeyIsDistinctFromForgery(t *testing.T) {
	dir, key, _ := signedStore(t)
	writeSigned(t, dir, key, 1)

	// A ring holding a different key ID entirely.
	other := signing.NewKeyring()
	_, pub, err := signing.GenerateKey(99)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Add(99, pub); err != nil {
		t.Fatal(err)
	}

	s, err := OpenWithOptions(dir, Options{Verifier: other, RequireSignedCommits: true})
	if err == nil {
		s.Close()
		t.Fatal("a commit verified against a keyring that does not hold its key")
	}
	if !errors.Is(err, signing.ErrUnknownKey) {
		t.Fatalf("expected an unknown-key error, got: %v", err)
	}
}

// Signing is opt-in and costs nothing when unused: an unsigned store still
// opens under a verifier that does not require signatures.
func TestSigning_UnsignedStoreStillOpens(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ring := signing.NewKeyring()
	reopened, err := OpenWithOptions(dir, Options{Verifier: ring})
	if err != nil {
		t.Fatalf("an unsigned store failed to open under a lenient verifier: %v", err)
	}
	defer reopened.Close()
}

// A store configured to sign must fail the commit rather than silently write an
// unsigned one when signing breaks.
func TestSigning_SignerFailureAbortsTheCommit(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Signer: failingSigner{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.ApplyTransactionAs(
		[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
			ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
		store.TxContext{ActorID: 1},
	)
	if err == nil {
		t.Fatal("a commit succeeded despite the signer failing; it must not fall back to unsigned")
	}

	n, _ := s.NodeCount()
	if n != 0 {
		t.Fatalf("the failed commit applied %d nodes; nothing should have been applied", n)
	}
}

type failingSigner struct{}

func (failingSigner) KeyID() uint64 { return 1 }
func (failingSigner) Sign([]byte) ([]byte, error) {
	return nil, errors.New("key unavailable")
}

// A signature is bound to its commit's metadata, so it cannot be replayed
// against a different sequence number or actor.
func TestSigning_SignatureIsBoundToCommitMetadata(t *testing.T) {
	key, pub, err := signing.GenerateKey(3)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(3, pub); err != nil {
		t.Fatal(err)
	}

	b := newWALBatch(64)
	b.add(walRecordNode, []byte("payload"))
	framed, err := b.finish(batchMeta{CommitSeq: 5, UnixNano: 1000, ActorID: 9, Signer: key})
	if err != nil {
		t.Fatal(err)
	}

	// Rewrite the actor in the commit payload and repair the record CRC. The
	// signature covers the actor, so it must no longer verify.
	commitPayloadAt := len(framed) - walFooterSize - walBatchCommitPayloadV3
	framed[commitPayloadAt+24] ^= 0x01 // actorID
	// This fixture is hand-built under v1, so its CRCs are too.
	crc := recordCRC(walFramingV1, walRecordBatchCommit, framed[commitPayloadAt:commitPayloadAt+walBatchCommitPayloadV3])
	crcAt := len(framed) - walFooterSize
	framed[crcAt] = byte(crc)
	framed[crcAt+1] = byte(crc >> 8)
	framed[crcAt+2] = byte(crc >> 16)
	framed[crcAt+3] = byte(crc >> 24)

	err = replayRecords(bytesReader(framed), int64(len(framed)), walFramingV1, ReplayCallbacks{
		Verifier:             ring,
		RequireSignedCommits: true,
		NodeFunc:             func([]byte) error { return nil },
	})
	if err == nil {
		t.Fatal("a signature verified after its commit's actor was changed")
	}
}
