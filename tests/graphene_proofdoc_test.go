package graphene_test

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// SECURITY.md §4 "Handing a proof over", executed.
//
// Kept in its own file rather than appended to graphene_security_doc_test.go
// because it is the only flow in that document whose point is what the verifier
// does *not* have — no store, no image, no directory — and the separation makes
// the absence visible rather than something a reader has to notice.

// SECURITY.md §4: the producer/recipient split, end to end.
func TestSecurityDoc_ProofTravelsAndVerifiesWithoutTheStore(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	// Producer.
	blob, err := s.ExportNodeProof(id)
	if err != nil {
		t.Fatal(err)
	}

	// Recipient. Nothing below touches the store.
	proof, err := disk.UnmarshalProof(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.VerifyExportedProof(roots.Snapshot, proof); err != nil {
		t.Fatalf("§4 says a recipient with the bytes and a root can check it: %v", err)
	}

	// "A proof checked against a root carried inside it proves nothing" — so a
	// different root must fail, including a plausible-looking one.
	if err := disk.VerifyExportedProof(merkle.Hash{}, proof); err == nil {
		t.Fatal("§4's whole premise fails: the proof verified against an unrelated root")
	}
}

// SECURITY.md §4: "a single altered byte either fails to parse or fails to
// verify."
func TestSecurityDoc_EveryProofByteIsBound(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	blob, err := s.ExportNodeProof(id)
	if err != nil {
		t.Fatal(err)
	}

	// Past the four-byte magic and the two header bytes, which identify the file
	// rather than carry the claim.
	for i := 6; i < len(blob); i++ {
		tampered := append([]byte(nil), blob...)
		tampered[i] ^= 0x01
		p, uerr := disk.UnmarshalProof(tampered)
		if uerr != nil {
			continue // failed to parse, which §4 permits
		}
		if err := disk.VerifyExportedProof(roots.Snapshot, p); err == nil {
			t.Fatalf("§4 says every byte is bound; flipping byte %d changed nothing", i)
		}
	}
}

// SECURITY.md §4: "A redaction proof carries less still: a tombstone names an
// entity and a digest, never an actor or a reason."
func TestSecurityDoc_ExportedProofCarriesNoCircumstances(t *testing.T) {
	s, _ := redactableDocStore(t)

	const secret = "name=jane.doe"
	const reason = "subject access request 41"
	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte(secret),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNodeProperties(id, disk.RedactionRequest{ActorID: 7, Reason: reason}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	blob, err := s.ExportPropertyRedactionProof(id)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, []byte(secret)) {
		t.Fatal("the exported proof carries the redacted content")
	}
	if bytes.Contains(blob, []byte(reason)) {
		t.Fatal("§4 says a redaction proof carries no reason; it does")
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := disk.UnmarshalProof(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.VerifyExportedProof(roots.Snapshot, proof); err != nil {
		t.Fatalf("a content-free proof did not verify after travelling: %v", err)
	}
}
