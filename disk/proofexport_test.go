package disk

import (
	"errors"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// Exported proofs: evidence that can leave the process that holds it.

// **The whole point.** A recipient with the bytes and a root — no store, no
// image, no directory — can check the claim.
func TestExport_NodeInclusionVerifiesWithoutTheStore(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
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

	// Everything below is what the recipient does, and it touches no store.
	got, err := UnmarshalProof(blob)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != ProofKindNodeInclusion {
		t.Fatalf("decoded a %s", got.Kind)
	}
	if err := VerifyExportedProof(roots.Snapshot, got); err != nil {
		t.Fatalf("an exported proof did not verify: %v", err)
	}
	if got.Subject() != "node 1" && got.Node.NodeID != id {
		t.Fatalf("the proof is about %s", got.Subject())
	}

	// Against any other root it must fail.
	if err := VerifyExportedProof(merkle.Hash{}, got); err == nil {
		t.Fatal("an exported proof verified against a zero root")
	}
}

// A removal proof travels too, for both namespaces.
func TestExport_RemovalProofsTravel(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 2)
	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	edge := impact.CascadedEdges[0]
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactEdge(edge, RedactionRequest{ActorID: 1, Reason: "edge gone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(hub, RedactionRequest{ActorID: 1, Reason: "node gone"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	nodeBlob, err := s.ExportRedactionProof(hub)
	if err != nil {
		t.Fatal(err)
	}
	edgeBlob, err := s.ExportEdgeRedactionProof(edge)
	if err != nil {
		t.Fatal(err)
	}

	for name, blob := range map[string][]byte{"node": nodeBlob, "edge": edgeBlob} {
		got, uerr := UnmarshalProof(blob)
		if uerr != nil {
			t.Fatalf("%s removal proof did not decode: %v", name, uerr)
		}
		if err := VerifyExportedProof(roots.Snapshot, got); err != nil {
			t.Errorf("%s removal proof did not verify: %v", name, err)
		}
	}

	// The subjects name the right namespaces.
	nodeGot, _ := UnmarshalProof(nodeBlob)
	edgeGot, _ := UnmarshalProof(edgeBlob)
	if !strings.HasPrefix(nodeGot.Subject(), "node ") {
		t.Errorf("the node removal proof is about %q", nodeGot.Subject())
	}
	if !strings.HasPrefix(edgeGot.Subject(), "edge ") {
		t.Errorf("the edge removal proof is about %q", edgeGot.Subject())
	}
}

// **A property-redaction proof travels and still reveals nothing.** The
// encoding must not undo the content-free property the proof was built for.
func TestExport_PropertyRedactionTravelsWithoutContent(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	const secret = "name=jane.doe;nhs=4457718123"
	if err := s.UpdateNode(&store.Node{
		ID: id, Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte(secret),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNodeProperties(id, RedactionRequest{ActorID: 7, Reason: "erasure"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	blob, err := s.ExportPropertyRedactionProof(id)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "jane.doe") || strings.Contains(string(blob), "4457718123") {
		t.Fatal("the exported proof carries the redacted content")
	}
	// Nor the reason, which lives in the ledger and has no business travelling.
	if strings.Contains(string(blob), "erasure") {
		t.Fatal("the exported proof carries the redaction's reason")
	}

	got, err := UnmarshalProof(blob)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyExportedProof(roots.Snapshot, got); err != nil {
		t.Fatalf("an exported property-redaction proof did not verify: %v", err)
	}
}

// **A tampered proof must not verify.** Every field the verifier relies on has
// to be covered, so flipping any of them fails.
func TestExport_TamperingIsCaught(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
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

	// Every byte after the header. A change anywhere must either fail to decode
	// or fail to verify — never decode cleanly and verify.
	accepted := 0
	for i := 6; i < len(blob); i++ {
		tampered := append([]byte(nil), blob...)
		tampered[i] ^= 0x01
		got, uerr := UnmarshalProof(tampered)
		if uerr != nil {
			continue
		}
		if err := VerifyExportedProof(roots.Snapshot, got); err == nil {
			accepted++
			t.Errorf("flipping byte %d produced a proof that still verified", i)
		}
	}
	if accepted > 0 {
		t.Fatalf("%d single-byte edits went undetected", accepted)
	}
}

// A file that is not a proof is refused as malformed, distinctly from one that
// reads cleanly and disagrees with the root.
func TestExport_RefusesWhatIsNotAProof(t *testing.T) {
	for name, blob := range map[string][]byte{
		"empty":       {},
		"short":       []byte("GP"),
		"wrong magic": append([]byte("XXXX"), 1, 1),
		"bad version": append([]byte(proofMagic), 99, 1),
		"bad kind":    append([]byte(proofMagic), proofFormatVersion, 99),
	} {
		if _, err := UnmarshalProof(blob); !errors.Is(err, ErrProofMalformed) {
			t.Errorf("%s: got %v, want ErrProofMalformed", name, err)
		}
	}
}

// **Trailing bytes are refused.** Otherwise a proof could carry an unverified
// payload alongside a verified one.
func TestExport_RefusesTrailingBytes(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	blob, err := s.ExportNodeProof(id)
	if err != nil {
		t.Fatal(err)
	}

	padded := append(append([]byte(nil), blob...), []byte("and something else entirely")...)
	if _, err := UnmarshalProof(padded); !errors.Is(err, ErrProofMalformed) {
		t.Fatalf("a proof with a trailing payload was accepted (err=%v)", err)
	}
}

// A hostile length prefix must be refused rather than allocated for.
func TestExport_RefusesImpossibleLengths(t *testing.T) {
	// A well-formed header followed by a leaf-data length no file could satisfy.
	blob := append([]byte(proofMagic), proofFormatVersion, byte(ProofKindNodeInclusion))
	blob = append(blob, 1, 0, 0, 0, 0, 0, 0, 0) // node ID
	blob = append(blob, 0xFF, 0xFF, 0xFF, 0x7F) // leaf length ~2GB

	if _, err := UnmarshalProof(blob); !errors.Is(err, ErrProofMalformed) {
		t.Fatalf("a proof claiming a 2GB leaf was accepted (err=%v)", err)
	}
}

// A proof for one entity must not verify as a proof for another, even against
// the right root.
func TestExport_ProofsAreNotInterchangeable(t *testing.T) {
	_, s, _ := redactableStore(t)
	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	blobA, err := s.ExportNodeProof(a)
	if err != nil {
		t.Fatal(err)
	}
	gotA, err := UnmarshalProof(blobA)
	if err != nil {
		t.Fatal(err)
	}

	// **Relabelling must be rejected.** Nothing downstream reads NodeID — the
	// leaf binds the claim — so a proof carrying a's leaf under b's label would
	// verify while telling its reader the opposite.
	relabelled := gotA
	relabelled.Node.NodeID = b
	if err := VerifyExportedProof(roots.Snapshot, relabelled); err == nil {
		t.Fatal("a proof relabelled to name a different entity verified")
	}

	blobB, err := s.ExportNodeProof(b)
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := UnmarshalProof(blobB)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotA.Node.LeafData) == string(gotB.Node.LeafData) {
		t.Fatal("two different entities produced identical leaf data")
	}

	// Swapping the leaf for b's while keeping a's path must fail.
	forged := gotA
	forged.Node.LeafData = gotB.Node.LeafData
	if err := VerifyExportedProof(roots.Snapshot, forged); err == nil {
		t.Fatal("a proof with another entity's leaf verified")
	}
}

// Round-tripping is exact, so a stored proof reads back as what was written.
func TestExport_RoundTripIsExact(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	blob, err := s.ExportNodeProof(id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalProof(blob)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalProof(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(blob) != string(again) {
		t.Fatal("a proof did not round-trip to identical bytes")
	}
}
