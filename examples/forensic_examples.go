package main

// The forensic surface, end to end.
//
// Graphene's integrity machinery is opt-in and none of it is reachable from the
// `graphene.Graph` façade — it lives on `disk.Store`, so these examples open the
// store directly. SECURITY.md is the authority on what each mechanism does and,
// more importantly, what it does not; these show how to call them.
//
// The order below is the order a deployment adopts them in:
//
//	F1  sign the log and verify on open      StrictOptions
//	F2  prove an entity was in a snapshot    ProveNode / VerifyNodeInclusion
//	F3  hand that proof to someone else      ExportNodeProof / VerifyExportedProof
//	F4  redact lawfully, and prove you did   RedactNodeProperties / ProvePropertyRedaction
//	F5  account for an entity end to end     CustodyFor
//	F6  publish a checkpoint to an anchor    PublishCheckpoint / VerifyAgainstAnchor

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// exampleForensic0_FromTheFacade shows the entry point a consumer actually
// starts from.
//
// Everything below this opens disk.Store directly, which is shorter. But a
// caller who already holds a graphene.Graph does not need to: OpenWithOptions
// takes the same options, and Forensics returns the same store rather than a
// copy — so writes through either are visible to both.
func exampleForensic0_FromTheFacade() {
	fmt.Println("Forensic 0: reaching the machinery from graphene.Graph")

	dir, _ := os.MkdirTemp("", "graphene-forensic0")
	defer os.RemoveAll(dir)

	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		fmt.Println("  key:", err)
		return
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		fmt.Println("  ring:", err)
		return
	}

	g, err := graphene.OpenWithOptions(dir, disk.StrictOptions(key, ring, 42))
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		fmt.Println("  this Graph has no integrity machinery")
		return
	}

	// Written through the façade, proved through the store.
	id, err := g.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}
	if err := g.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		fmt.Println("  roots:", err)
		return
	}
	proof, err := s.ProveNode(id)
	if err != nil {
		fmt.Println("  prove:", err)
		return
	}
	if err := disk.VerifyNodeInclusion(roots.Snapshot, proof); err != nil {
		fmt.Println("  VERIFY FAILED:", err)
		return
	}
	fmt.Printf("  node %d added via Graph, proved via Forensics()\n", id)

	// The in-memory backend has none of it, and says so once.
	mem := graphene.NewInMemory()
	defer mem.Close()
	if _, ok := mem.Forensics(); !ok {
		fmt.Println("  in-memory backend: no integrity machinery, reported once")
	}
	fmt.Println()
}

// openForensicStore is the configuration every example below shares.
//
// StrictOptions is the cautious posture: every commit signed and required to be
// signed, the image verified before it is loaded, and the audit log on. Retention
// and redaction are separate opt-ins because they cost extra files.
func openForensicStore(dir string) (*disk.Store, *signing.Keyring, error) {
	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		return nil, nil, err
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		return nil, nil, err
	}

	opts := disk.StrictOptions(key, ring, 42) // 42 is the actor recorded in attestations
	opts.Retention = disk.RetentionPolicy{MaxSegments: 20}
	opts.Redaction = true
	opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 50}

	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		return nil, nil, err
	}
	return s, ring, nil
}

// exampleForensic1_SignedStoreAndAttestation shows the safe posture and the
// attestation a compaction produces.
func exampleForensic1_SignedStoreAndAttestation() {
	fmt.Println("Forensic 1: a signed store, and who vouched for the image")

	dir, _ := os.MkdirTemp("", "graphene-forensic1")
	defer os.RemoveAll(dir)

	s, ring, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer s.Close()

	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123;size=4096"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}

	// Compaction is what produces the snapshot roots and the attestation over
	// them. Before it, nothing is provable — the entity is live but unaccounted
	// for, which is a different thing from absent.
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		fmt.Println("  roots:", err)
		return
	}
	fmt.Printf("  node %d written; snapshot root %x…\n", id, roots.Snapshot[:8])

	att, err := s.SnapshotAttestation()
	if err != nil {
		fmt.Println("  attestation:", err)
		return
	}
	if err := disk.VerifyAttestation(ring, att); err != nil {
		fmt.Println("  attestation does not verify:", err)
		return
	}
	fmt.Printf("  attested by actor %d, key %d — signature verifies\n", att.ActorID, att.KeyID)

	// **Retain this value outside the system.** Every check the engine can run
	// compares the store against itself; only a root you kept elsewhere can tell
	// "internally consistent" from "not tampered with".
	fmt.Printf("  retain this root: %x\n", roots.Snapshot)
	fmt.Println()
}

// exampleForensic2_InclusionProof shows that one entity was in one snapshot,
// without handing over the file.
func exampleForensic2_InclusionProof() {
	fmt.Println("Forensic 2: proving one entity was in a snapshot")

	dir, _ := os.MkdirTemp("", "graphene-forensic2")
	defer os.RemoveAll(dir)

	s, _, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer s.Close()

	var target store.NodeID
	for i := 0; i < 5; i++ {
		id, aerr := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("sha256=file%d", i)),
		})
		if aerr != nil {
			fmt.Println("  add:", aerr)
			return
		}
		if i == 2 {
			target = id
		}
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}

	roots, _ := s.SnapshotRoots()
	retained := roots.Snapshot // the recipient's copy, obtained independently

	proof, err := s.ProveNode(target)
	if err != nil {
		fmt.Println("  prove:", err)
		return
	}
	fmt.Printf("  proof for node %d: %d sibling hashes over %d leaves\n",
		target, len(proof.Proof.Siblings), proof.Proof.Size)

	// The recipient's side. Note the root is supplied, not read from the proof:
	// checking a proof against a root carried inside it would be circular.
	if err := disk.VerifyNodeInclusion(retained, proof); err != nil {
		fmt.Println("  VERIFY FAILED:", err)
		return
	}
	fmt.Println("  verified against the retained root")

	// Against any other root it fails, which is the property that makes it worth
	// anything.
	if err := disk.VerifyNodeInclusion(merkle.Hash{}, proof); err == nil {
		fmt.Println("  BUG: verified against an unrelated root")
	} else {
		fmt.Println("  correctly refused against an unrelated root")
	}
	fmt.Println()
}

// exampleForensic3_HandingAProofOver shows a proof leaving the process as bytes
// and being checked with no store at all.
func exampleForensic3_HandingAProofOver() {
	fmt.Println("Forensic 3: handing a proof to someone who has no store")

	dir, _ := os.MkdirTemp("", "graphene-forensic3")
	defer os.RemoveAll(dir)

	s, _, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}

	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=evidence-1"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}
	roots, _ := s.SnapshotRoots()
	retained := roots.Snapshot

	// Producer: export and write to a file.
	blob, err := s.ExportNodeProof(id)
	if err != nil {
		fmt.Println("  export:", err)
		return
	}
	path := filepath.Join(dir, "claim.gprf")
	if err := os.WriteFile(path, blob, 0600); err != nil {
		fmt.Println("  write:", err)
		return
	}
	fmt.Printf("  exported %d bytes to %s\n", len(blob), filepath.Base(path))

	// The store is closed before the recipient does anything, to make the point
	// that verification needs none of it.
	s.Close()

	// Recipient: the file and a root, nothing else.
	received, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("  read:", err)
		return
	}
	proof, err := disk.UnmarshalProof(received)
	if err != nil {
		fmt.Println("  decode:", err)
		return
	}
	if err := disk.VerifyExportedProof(retained, proof); err != nil {
		fmt.Println("  VERIFY FAILED:", err)
		return
	}
	fmt.Printf("  verified: %s of %s, store closed\n", proof.Kind, proof.Subject())

	// A single altered byte is caught.
	received[len(received)-1] ^= 0x01
	if p, derr := disk.UnmarshalProof(received); derr == nil {
		if disk.VerifyExportedProof(retained, p) == nil {
			fmt.Println("  BUG: a tampered proof verified")
		} else {
			fmt.Println("  a tampered proof was correctly refused")
		}
	} else {
		fmt.Println("  a tampered proof was correctly refused at decode")
	}
	fmt.Println()
}

// exampleForensic4_LawfulRedaction shows erasing personal data without
// destroying the evidence around it, and proving afterwards that is all you did.
func exampleForensic4_LawfulRedaction() {
	fmt.Println("Forensic 4: erasing personal data without destroying the case")

	dir, _ := os.MkdirTemp("", "graphene-forensic4")
	defer os.RemoveAll(dir)

	s, _, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer s.Close()

	subject, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("name=jane.doe;dob=1984-02-11"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}
	artefact, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}
	if _, err := s.AddEdge(&store.Edge{
		Src: subject, Dst: artefact,
		Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
	}); err != nil {
		fmt.Println("  edge:", err)
		return
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}

	// Look before you leap: what would a whole-entity removal take with it?
	impact, err := s.RedactionImpactFor(subject)
	if err != nil {
		fmt.Println("  impact:", err)
		return
	}
	fmt.Printf("  RedactNode would also remove %d edge(s)\n", len(impact.CascadedEdges))

	// **The narrow form is almost always the right one.** An erasure request is
	// about personal data, not about the existence of the artefact that carried
	// it — so remove the properties and keep the entity, its labels and its edges.
	rec, err := s.RedactNodeProperties(subject, disk.RedactionRequest{
		ActorID: 7, RoleID: 3, Reason: "subject access request 41",
	})
	if err != nil {
		fmt.Println("  redact:", err)
		return
	}
	fmt.Printf("  redacted properties of node %d (ledger record %d)\n", subject, rec.Seq)

	n, _ := s.GetNode(subject)
	fmt.Printf("  entity survives: %d label(s), %d bytes of properties\n",
		len(n.Labels), len(n.Properties))

	// Compact so the image carries a tombstone; until then only the ledger knows.
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}
	roots, _ := s.SnapshotRoots()

	// **Provable without revealing what was removed.** The verifier learns the
	// properties went and that nothing else about the entity changed.
	proof, err := s.ProvePropertyRedaction(subject)
	if err != nil {
		fmt.Println("  prove:", err)
		return
	}
	if err := disk.VerifyPropertyRedaction(roots.Snapshot, proof); err != nil {
		fmt.Println("  VERIFY FAILED:", err)
		return
	}
	fmt.Println("  removal proven; the proof carries no plaintext")
	fmt.Println()
}

// exampleForensic5_ChainOfCustody shows the single report that walks every
// history for one entity.
func exampleForensic5_ChainOfCustody() {
	fmt.Println("Forensic 5: accounting for one entity across every history")

	dir, _ := os.MkdirTemp("", "graphene-forensic5")
	defer os.RemoveAll(dir)

	s, ring, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer s.Close()

	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123"),
	})
	if err != nil {
		fmt.Println("  add:", err)
		return
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}
	roots, _ := s.SnapshotRoots()

	// Unanchored: every check compares the store against itself, and the report
	// says so rather than letting a clean result be read as proof.
	report, err := s.CustodyFor(id, ring)
	if err != nil {
		fmt.Println("  custody:", err)
		return
	}
	fmt.Printf("  %s\n", report.Summary())
	for _, gap := range report.Gaps {
		fmt.Printf("    %s\n", gap)
	}

	// Anchored against a root retained outside the system, the same report can
	// be complete.
	anchored, err := s.CustodyForAnchored(id, ring, roots.Snapshot)
	if err != nil {
		fmt.Println("  custody:", err)
		return
	}
	fmt.Printf("  with a retained root: complete=%v broken=%v\n",
		anchored.Complete(), anchored.Broken())
	fmt.Println()
}

// exampleForensic6_Checkpoints shows publishing a digest that binds every
// history, and checking the store against what was published.
func exampleForensic6_Checkpoints() {
	fmt.Println("Forensic 6: publishing a checkpoint to an anchor")

	dir, _ := os.MkdirTemp("", "graphene-forensic6")
	defer os.RemoveAll(dir)
	anchorDir, _ := os.MkdirTemp("", "graphene-anchor6")
	defer os.RemoveAll(anchorDir)

	s, _, err := openForensicStore(dir)
	if err != nil {
		fmt.Println("  open:", err)
		return
	}
	defer s.Close()

	if _, err := s.AddNode(&store.Node{
		Labels: []store.NodeType{store.NodeTypeMicroArtefact},
	}); err != nil {
		fmt.Println("  add:", err)
		return
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}

	// **This is not an anchor.** A file on the same machine is reachable by
	// anyone who can rewrite the store. The engine ships no real transport by
	// design — implement disk.Anchor against a timestamp authority, a
	// transparency log, or another organisation's storage. The constructor
	// refuses a path inside the store directory, which is a tripwire rather than
	// a security property.
	anchor, err := disk.NewInsecureLocalAnchor(filepath.Join(anchorDir, "anchor.bin"), dir)
	if err != nil {
		fmt.Println("  anchor:", err)
		return
	}

	c, receipt, err := s.PublishCheckpoint(anchor)
	if err != nil {
		fmt.Println("  publish:", err)
		return
	}
	fmt.Printf("  %s\n", c)
	fmt.Printf("  anchor receipt: %q\n", receipt.Ref)

	audit, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		fmt.Println("  verify:", err)
		return
	}
	fmt.Printf("  %s\n", audit.Summary())
	fmt.Printf("  store unchanged since it was witnessed: %v\n", audit.CurrentMatchesLast)

	// Work done after publishing is outside the witnessed window, and the report
	// says so — the guarantee is bounded by how often you publish.
	if _, err := s.AddNode(&store.Node{
		Labels: []store.NodeType{store.NodeTypeTag},
	}); err != nil {
		fmt.Println("  add:", err)
		return
	}
	if err := s.Compact(); err != nil {
		fmt.Println("  compact:", err)
		return
	}
	moved, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		fmt.Println("  verify:", err)
		return
	}
	fmt.Printf("  after more work: broken=%v, findings=%d\n", moved.Broken(), len(moved.Gaps))
	for _, gap := range moved.Gaps {
		fmt.Printf("    %s\n", gap)
	}
	fmt.Println()
}
