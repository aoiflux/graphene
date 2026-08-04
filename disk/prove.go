package disk

// Inclusion proofs against a snapshot root.
//
// A proof is what makes a claim about one entity transferable. The recipient
// needs the entity's record, the proof, and a root they obtained independently
// — not the store, not the file, and not any other entity. Verification is a few
// hundred bytes of hashing and depends on nothing in this package, which is why
// merkle.VerifyProof lives in a package that imports none of it.
//
// The roots only exist for the compacted image. Anything written since the last
// compaction lives in the delta and is not covered — a proof describes what was
// in the snapshot, and the snapshot is what compaction wrote.

import (
	"errors"
	"fmt"
	"os"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// ErrNoSnapshotRoots is returned when a store has no compacted image carrying
// roots — either it has never been compacted, or its image predates them.
var ErrNoSnapshotRoots = errors.New("disk: store has no snapshot roots; compact first")

// ErrNotInSnapshot is returned for an entity that is not in the compacted image.
// An entity written since the last compaction is live but unproven, which is a
// different thing from absent.
var ErrNotInSnapshot = errors.New("disk: entity is not in the compacted snapshot")

// NodeInclusionProof is everything a third party needs to check that a node was
// in a snapshot.
type NodeInclusionProof struct {
	NodeID store.NodeID

	// LeafData is the canonical encoding that was hashed. Carried in full rather
	// than as a digest so the recipient can confirm it describes the entity they
	// were shown, instead of taking a hash on trust.
	LeafData []byte

	Proof merkle.Proof

	// NodeRoot is the root this proof resolves to. It is a component of the
	// snapshot root, not the snapshot root itself — see VerifyNodeInclusion.
	NodeRoot merkle.Hash

	// Roots lets a verifier confirm NodeRoot really belongs to the snapshot they
	// were given, rather than to some other tree.
	Roots SnapshotRoots
}

// SnapshotRoots returns the Merkle identity of the compacted image.
func (s *Store) SnapshotRoots() (SnapshotRoots, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil {
		return SnapshotRoots{}, ErrNoSnapshotRoots
	}
	r, ok := csr.Roots()
	if !ok {
		return SnapshotRoots{}, ErrNoSnapshotRoots
	}
	return r, nil
}

// ProveNode builds an inclusion proof for a node in the compacted image.
//
// Costs one pass over the node records, because the leaf hashes are recomputed
// rather than stored. Storing them would make this O(log n) at roughly 6% more
// disk; recomputing keeps the file smaller and proof generation is not a hot
// path. Revisit if it becomes one.
func (s *Store) ProveNode(id store.NodeID) (NodeInclusionProof, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil {
		return NodeInclusionProof{}, ErrNoSnapshotRoots
	}
	roots, ok := csr.Roots()
	if !ok {
		return NodeInclusionProof{}, ErrNoSnapshotRoots
	}

	index, present := csr.nodeLeafIndex(id)
	if !present {
		return NodeInclusionProof{}, fmt.Errorf("%w: node %d", ErrNotInSnapshot, id)
	}

	leaves := csr.NodeLeaves()
	proof, err := merkle.BuildProof(leaves, index)
	if err != nil {
		return NodeInclusionProof{}, err
	}

	return NodeInclusionProof{
		NodeID:   id,
		LeafData: nodeLeafData(csr.nodes[id]),
		Proof:    proof,
		NodeRoot: roots.NodeRoot,
		Roots:    roots,
	}, nil
}

// VerifyNodeInclusion checks a proof against a snapshot root the verifier
// obtained independently.
//
// snapshotRoot is the argument that matters. Checking the proof against the root
// carried inside the proof itself would be circular — an attacker supplying both
// can make them agree. The check is only worth anything when the root came from
// somewhere the attacker does not control: a published value, a co-signed
// attestation, a copy retained when the evidence was collected.
//
// It is a package-level function taking no store on purpose: a recipient
// verifying evidence has the proof and a root, and nothing else.
func VerifyNodeInclusion(snapshotRoot merkle.Hash, p NodeInclusionProof) error {
	// The component roots must actually produce the snapshot root being checked
	// against, or a valid proof into an unrelated tree would pass.
	if bindSnapshotRoot(p.Roots) != snapshotRoot {
		return errors.New("verify: the proof's component roots do not produce this snapshot root")
	}
	if p.Roots.NodeRoot != p.NodeRoot {
		return errors.New("verify: the proof's node root disagrees with its snapshot")
	}
	if !merkle.VerifyProof(p.NodeRoot, merkle.HashLeaf(p.LeafData), p.Proof) {
		return errors.New("verify: inclusion proof does not resolve to the node root")
	}
	return nil
}

// VerifyCSRRoots recomputes a file's Merkle roots from its records and compares
// them to the roots the file carries.
//
// Independent of the digest and worth running alongside it. The digest asks "do
// these bytes match what was written"; this asks "do the roots describe the
// records in the same file". A digest recomputed after an edit hides the first
// question, and this one still fails unless the roots were recomputed too — and
// if they were, the snapshot root changes, which is what an externally retained
// copy detects.
//
// path may be the store directory or the file.
func VerifyCSRRoots(path string) error {
	p, err := resolveFile(path, csrFileName)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("verify roots: %w", err)
	}
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		return fmt.Errorf("verify roots: %w", err)
	}
	stored, ok := csr.Roots()
	if !ok {
		return ErrNoSnapshotRoots
	}

	payload := csrPayload{}
	if section != nil {
		payload.NodeProps = section.NodeProps
		payload.EdgeProps = section.EdgeProps
	}
	// The tombstones the file carries are part of what its root describes.
	// deserialiseCSR has already checked they produce the stored TombstoneRoot,
	// so recomputing from them is not circular — it re-derives the same binding
	// this function is testing the rest of the image against.
	payload.Tombstones = csr.tombstones

	recomputed := computeSnapshotRoots(csr, payload, stored.PrevRoot)

	// Recomputation always produces the current body version. Comparing a v2
	// binding against a v1 image's stored root would fail on the version alone
	// and report it as a content mismatch, so the recomputed set adopts the
	// stored version and the components below are what actually get compared.
	recomputed.BodyVersion = stored.BodyVersion
	recomputed.Snapshot = bindSnapshotRoot(recomputed)

	switch {
	case recomputed.NodeRoot != stored.NodeRoot:
		return fmt.Errorf("verify roots: node root does not describe the node records (stored %x, computed %x)",
			stored.NodeRoot[:8], recomputed.NodeRoot[:8])
	case recomputed.EdgeRoot != stored.EdgeRoot:
		return fmt.Errorf("verify roots: edge root does not describe the edge records (stored %x, computed %x)",
			stored.EdgeRoot[:8], recomputed.EdgeRoot[:8])
	case recomputed.IndexRoot != stored.IndexRoot:
		return fmt.Errorf("verify roots: index root does not describe the property entries (stored %x, computed %x)",
			stored.IndexRoot[:8], recomputed.IndexRoot[:8])
	case stored.BodyVersion >= snapshotBodyV2 && recomputed.TombstoneRoot != stored.TombstoneRoot:
		return fmt.Errorf("verify roots: tombstone root does not describe the recorded removals (stored %x, computed %x)",
			stored.TombstoneRoot[:8], recomputed.TombstoneRoot[:8])
	case recomputed.Snapshot != stored.Snapshot:
		return fmt.Errorf("verify roots: snapshot root does not follow from its components")
	}
	return nil
}

// VerifyChain checks that later extends earlier — that the newer snapshot names
// the older as its predecessor.
//
// This is what distinguishes a store's history from a collection of internally
// consistent files. A substituted snapshot can be perfectly coherent on its own;
// what it cannot do is claim a predecessor it never had.
func VerifyChain(earlier, later SnapshotRoots) error {
	if earlier.Zero() || later.Zero() {
		return errors.New("verify chain: a snapshot without roots cannot be placed in a chain")
	}
	if later.PrevRoot != earlier.Snapshot {
		return fmt.Errorf("verify chain: the later snapshot names predecessor %x, but was given %x",
			later.PrevRoot[:8], earlier.Snapshot[:8])
	}
	if bindSnapshotRoot(later) != later.Snapshot {
		return errors.New("verify chain: the later snapshot root does not follow from its components")
	}
	return nil
}
