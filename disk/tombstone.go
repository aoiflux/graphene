package disk

// Tombstones: proving a redaction from the image alone.
//
// The redaction ledger (redact.go) records who destroyed what, when and why, and
// it survives compaction because it is its own file. That is enough for anyone
// holding the store. It is not enough for the party who matters most in an
// evidentiary exchange: **someone handed a single compacted image and nothing
// else.** To them a redacted entity is simply not there, indistinguishable from
// one that never existed — which is precisely the confusion §12.4's T-12 says
// must not persist.
//
// A tombstone is the image's own record that an entity was removed on purpose.
// It carries no content, only identity:
//
//	the scope — whole entity, its properties only, or one edge
//	the entity's ID
//	the version hash of what was destroyed
//	the ledger sequence and record hash it came from
//
// The tombstones are Merkle-rooted and that root is bound into the snapshot
// root, so a recipient can be given an **inclusion proof of a removal** in
// exactly the way `ProveNode` gives them an inclusion proof of a presence. A
// snapshot root retained outside the system therefore commits to what was taken
// out of the image as well as what is in it.
//
// # The ledger is the source of truth
//
// Tombstones are rebuilt from the ledger at every compaction rather than carried
// forward from the previous image. Carrying them forward would introduce a
// second copy that could drift, and a drifted tombstone set is worse than none —
// it would let the image and the ledger disagree about what was destroyed while
// each verified perfectly on its own.
//
// # What a tombstone does not contain
//
// No content, no reason, no actor. Those are in the ledger. The image gets the
// minimum that makes a removal provable, because the image is the artefact most
// likely to be handed to someone who should not receive an operator's name and a
// case reference along with it. A recipient who needs the circumstances asks for
// the ledger record, and the tombstone's RedactionHash tells them which one to
// ask for — and lets them check they were given the right one.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// leafTagTombstone separates a tombstone leaf from every other leaf kind, so a
// removal can never be presented as a presence.
const leafTagTombstone = 0x04

// tombstoneBodyVersion versions the GRDT section body.
const (
	tombstoneBodyV1 = 1 // node ID, version hash, ledger seq + hash
	tombstoneBodyV2 = 2 // adds scope and edge ID

	tombstoneBodyVersion = tombstoneBodyV2
)

// Tombstone is the image's record that an entity was deliberately removed.
type Tombstone struct {
	// Scope says what was removed. A property strip and a whole-entity removal
	// are both absences of *something*, and an image that could not tell them
	// apart would report a node still sitting in front of the reader as gone.
	Scope RedactionScope

	NodeID store.NodeID

	// EdgeID names the relationship for a ScopeEdge removal; zero otherwise.
	EdgeID store.EdgeID

	// VersionHash is the identity of the destroyed content, and is the same
	// value the entity's node leaf had in the image it was removed from. A
	// recipient holding that earlier image can match the two directly.
	VersionHash merkle.Hash

	// RedactionSeq and RedactionHash name the ledger record this came from.
	// Carrying the hash rather than just the sequence means a recipient given a
	// ledger record can confirm it is the one this tombstone refers to.
	RedactionSeq  uint64
	RedactionHash [sha256.Size]byte
}

// tombstoneLeafData is the canonical encoding hashed for one tombstone.
func tombstoneLeafData(t Tombstone) []byte {
	buf := make([]byte, 0, 1+1+8+8+merkle.Size+8+sha256.Size)
	buf = append(buf, leafTagTombstone)
	buf = append(buf, byte(t.Scope))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.NodeID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.EdgeID))
	buf = append(buf, t.VersionHash[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, t.RedactionSeq)
	return append(buf, t.RedactionHash[:]...)
}

// tombstonesFromLedger projects redaction records into the tombstones an image
// should carry.
//
// Ordered by ledger sequence, which is already the order they were written in,
// so the root is a deterministic function of the ledger — two compactions of the
// same store produce the same tombstone root. That is the same determinism
// requirement §10.4 places on every other part of the image.
func tombstonesFromLedger(records []RedactionRecord) []Tombstone {
	if len(records) == 0 {
		return nil
	}
	out := make([]Tombstone, 0, len(records))
	for _, r := range records {
		out = append(out, Tombstone{
			Scope:         r.Scope,
			NodeID:        r.NodeID,
			EdgeID:        r.EdgeID,
			VersionHash:   r.VersionHash,
			RedactionSeq:  r.Seq,
			RedactionHash: r.Hash,
		})

		// **One tombstone per cascaded edge, too.** An edge removed as collateral
		// is as absent from the image as one removed deliberately, and without its
		// own marker nobody auditing "why is there no edge between A and B" can be
		// shown an answer. The node's record *names* the cascade; only a leaf of
		// its own makes each removal provable.
		//
		// Records written before CascadedHashes existed name their edges without
		// identifying them, and get no per-edge tombstones rather than tombstones
		// with a zero version hash — an unidentifiable marker is worse than none,
		// because it looks like evidence.
		if len(r.CascadedHashes) != len(r.CascadedEdges) {
			continue
		}
		for i, eid := range r.CascadedEdges {
			out = append(out, Tombstone{
				Scope:         ScopeEdge,
				EdgeID:        eid,
				VersionHash:   r.CascadedHashes[i],
				RedactionSeq:  r.Seq,
				RedactionHash: r.Hash,
			})
		}
	}
	slices.SortFunc(out, func(a, b Tombstone) int {
		switch {
		case a.RedactionSeq < b.RedactionSeq:
			return -1
		case a.RedactionSeq > b.RedactionSeq:
			return 1
		default:
			return 0
		}
	})
	return out
}

// tombstoneLeaves returns the Merkle leaves for a tombstone set.
func tombstoneLeaves(ts []Tombstone) []merkle.Hash {
	if len(ts) == 0 {
		return nil
	}
	out := make([]merkle.Hash, len(ts))
	for i, t := range ts {
		out[i] = merkle.HashLeaf(tombstoneLeafData(t))
	}
	return out
}

// --- GRDT section encoding ---

const (
	tombstoneEntrySizeV1 = 8 + merkle.Size + 8 + sha256.Size
	tombstoneEntrySizeV2 = tombstoneEntrySizeV1 + 1 + 8
)

func appendTombstoneSection(buf []byte, ts []Tombstone) []byte {
	buf = append(buf, tombstoneBodyVersion)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ts)))
	for _, t := range ts {
		buf = append(buf, byte(t.Scope))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(t.NodeID))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(t.EdgeID))
		buf = append(buf, t.VersionHash[:]...)
		buf = binary.LittleEndian.AppendUint64(buf, t.RedactionSeq)
		buf = append(buf, t.RedactionHash[:]...)
	}
	return buf
}

func readTombstoneSection(data []byte) ([]Tombstone, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("truncated tombstone section: %d bytes", len(data))
	}
	entrySize := 0
	switch v := data[0]; v {
	case tombstoneBodyV1:
		entrySize = tombstoneEntrySizeV1
	case tombstoneBodyV2:
		entrySize = tombstoneEntrySizeV2
	default:
		return nil, fmt.Errorf("tombstone section version %d, this build understands %d and %d",
			v, tombstoneBodyV1, tombstoneBodyV2)
	}
	version := data[0]
	n := int(binary.LittleEndian.Uint32(data[1:5]))

	// Bounded against the section's own length before anything is allocated. A
	// five-byte section must never be able to ask for gigabytes, which is the
	// discipline every other length prefix read from a file here follows.
	if n < 0 || n > (len(data)-5)/entrySize {
		return nil, fmt.Errorf("tombstone section claims %d entries but holds %d bytes",
			n, len(data)-5)
	}

	out := make([]Tombstone, n)
	pos := 5
	for i := 0; i < n; i++ {
		if version >= tombstoneBodyV2 {
			out[i].Scope = RedactionScope(data[pos])
			pos++
		}
		out[i].NodeID = store.NodeID(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		if version >= tombstoneBodyV2 {
			out[i].EdgeID = store.EdgeID(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
		}
		copy(out[i].VersionHash[:], data[pos:pos+merkle.Size])
		pos += merkle.Size
		out[i].RedactionSeq = binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		copy(out[i].RedactionHash[:], data[pos:pos+sha256.Size])
		pos += sha256.Size
	}
	return out, nil
}

// --- proving a removal ---

// ErrNoTombstone is returned when an image carries no record of an entity having
// been redacted.
//
// Distinct from ErrNotInSnapshot on purpose: "this entity is not here" and "this
// entity was deliberately removed" are different answers, and an image that can
// only give the first is the state of affairs this file exists to end.
var ErrNoTombstone = errNoTombstone{}

type errNoTombstone struct{}

func (errNoTombstone) Error() string {
	return "disk: the image carries no tombstone for this entity"
}

// RedactionInclusionProof shows that an image records a specific entity as
// deliberately removed.
type RedactionInclusionProof struct {
	Tombstone Tombstone

	// LeafData is the canonical encoding that was hashed, carried in full so the
	// recipient can confirm it describes the entity they were asked about rather
	// than taking a digest on trust. Same reasoning as NodeInclusionProof.
	LeafData []byte

	Proof merkle.Proof

	// TombstoneRoot is the root this proof resolves to, and Roots lets a verifier
	// confirm it belongs to the snapshot they were given.
	TombstoneRoot merkle.Hash
	Roots         SnapshotRoots
}

// ProveRedaction produces an inclusion proof that this image records id as
// redacted.
func (s *Store) ProveRedaction(id store.NodeID) (RedactionInclusionProof, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil {
		return RedactionInclusionProof{}, ErrNoSnapshotRoots
	}
	roots, ok := csr.Roots()
	if !ok {
		return RedactionInclusionProof{}, ErrNoSnapshotRoots
	}

	// The most recent tombstone for this node, not the first. A node whose
	// properties were stripped and which was later removed outright has two, and
	// the one that describes its present state is the later one.
	// Matched on the node ID *and* on the tombstone actually being about a node:
	// an edge-scoped record leaves NodeID zero, and a scope alone no longer
	// separates the two now that a property redaction can apply to either.
	idx := -1
	for i := range csr.tombstones {
		t := csr.tombstones[i]
		if t.EdgeID == 0 && t.NodeID != 0 && t.NodeID == id {
			idx = i
		}
	}
	if idx < 0 {
		return RedactionInclusionProof{}, ErrNoTombstone
	}

	return buildTombstoneProof(csr, roots, idx)
}

// buildTombstoneProof assembles the proof for one tombstone index.
func buildTombstoneProof(csr *CSRGraph, roots SnapshotRoots, idx int) (RedactionInclusionProof, error) {
	proof, err := merkle.BuildProof(tombstoneLeaves(csr.tombstones), idx)
	if err != nil {
		return RedactionInclusionProof{}, fmt.Errorf("disk: building a tombstone proof: %w", err)
	}
	return RedactionInclusionProof{
		Tombstone:     csr.tombstones[idx],
		LeafData:      tombstoneLeafData(csr.tombstones[idx]),
		Proof:         proof,
		TombstoneRoot: roots.TombstoneRoot,
		Roots:         roots,
	}, nil
}

// VerifyRedactionInclusion checks a redaction proof against a snapshot root
// obtained independently.
//
// Package-level and taking no store, like VerifyNodeInclusion: a recipient
// checking that something was removed has the proof and a root, and nothing
// else. Checking against the root carried inside the proof would be circular.
func VerifyRedactionInclusion(snapshotRoot merkle.Hash, p RedactionInclusionProof) error {
	if bindSnapshotRoot(p.Roots) != snapshotRoot {
		return fmt.Errorf("verify redaction: the proof's component roots do not produce this snapshot root")
	}
	if p.Roots.BodyVersion < snapshotBodyV2 {
		// A v1 snapshot root does not commit to a tombstone root at all, so a
		// proof "under" one proves nothing about that image. Saying so beats
		// verifying against a value the root never covered.
		return fmt.Errorf("verify redaction: this snapshot predates tombstones and cannot attest a removal")
	}
	if p.Roots.TombstoneRoot != p.TombstoneRoot {
		return fmt.Errorf("verify redaction: the proof's tombstone root disagrees with its snapshot")
	}

	// The leaf must describe the tombstone the proof claims, or a valid proof for
	// some other removal would pass as this one.
	want := tombstoneLeafData(p.Tombstone)
	if len(want) != len(p.LeafData) {
		return fmt.Errorf("verify redaction: the leaf data does not describe the stated tombstone")
	}
	for i := range want {
		if want[i] != p.LeafData[i] {
			return fmt.Errorf("verify redaction: the leaf data does not describe the stated tombstone")
		}
	}

	if !merkle.VerifyProof(p.TombstoneRoot, merkle.HashLeaf(p.LeafData), p.Proof) {
		return fmt.Errorf("verify redaction: inclusion proof does not resolve to the tombstone root")
	}
	return nil
}

// ProveEdgeRedaction produces an inclusion proof that this image records an edge
// as deliberately removed.
//
// Separate from ProveRedaction because an edge ID and a node ID are different
// namespaces: answering "was 7 redacted?" without knowing which 7 was meant is
// how a proof about one thing gets read as a proof about another.
func (s *Store) ProveEdgeRedaction(id store.EdgeID) (RedactionInclusionProof, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil {
		return RedactionInclusionProof{}, ErrNoSnapshotRoots
	}
	roots, ok := csr.Roots()
	if !ok {
		return RedactionInclusionProof{}, ErrNoSnapshotRoots
	}

	// Any scope, so long as it is about this edge: a relationship can be removed
	// outright or have its properties stripped, and both are edge removals.
	idx := -1
	for i := range csr.tombstones {
		if csr.tombstones[i].EdgeID == id {
			idx = i
		}
	}
	if idx < 0 {
		return RedactionInclusionProof{}, ErrNoTombstone
	}
	return buildTombstoneProof(csr, roots, idx)
}

// Tombstones returns the removals this image records, oldest first.
func (s *Store) Tombstones() []Tombstone {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()
	if csr == nil {
		return nil
	}
	return slices.Clone(csr.tombstones)
}

// --- proving a property redaction without revealing what was removed ---

// PropertyRedactionProof shows that an entity's properties were removed and that
// nothing else about it changed.
//
// This is what §11.2's separated Properties Hash was for. The claim it carries
// is stronger than "this entity's version hash changed", and it is checkable by
// someone who never sees the removed content:
//
//	the entity is in the image, with these labels          (Surviving)
//	its properties are now empty                            (from the leaf)
//	it previously had properties hashing to P               (PriorPropertiesHash)
//	and was otherwise byte-identical                        (reconstructed leaf)
//	the removal is recorded under the snapshot root         (Removal)
//
// The last two are the load-bearing ones. Reconstructing the prior leaf from the
// *surviving* entity's ID and labels plus P, and finding that it hashes to the
// version the ledger recorded, is what rules out anything else having changed.
type PropertyRedactionProof struct {
	NodeID store.NodeID

	// Surviving proves the entity is still in the image, and carries the leaf
	// data a verifier reads its ID and labels from.
	Surviving NodeInclusionProof

	// Removal proves the image records this as a property redaction.
	Removal RedactionInclusionProof

	// PriorLeafData is the leaf the entity had before, reconstructed from its
	// surviving identity and the destroyed blob's hash. Content-free by
	// construction: the property blob appears only as a 32-byte digest.
	PriorLeafData []byte
}

// ErrNoPropertyRedaction is returned when an image records no property redaction
// for an entity.
var ErrNoPropertyRedaction = errors.New("disk: the image records no property redaction for this entity")

// ProvePropertyRedaction builds the pair of proofs showing that an entity's
// properties were removed and nothing else about it was.
func (s *Store) ProvePropertyRedaction(id store.NodeID) (PropertyRedactionProof, error) {
	removal, err := s.ProveRedaction(id)
	if err != nil {
		return PropertyRedactionProof{}, err
	}
	if removal.Tombstone.Scope != ScopeProperties {
		return PropertyRedactionProof{}, ErrNoPropertyRedaction
	}
	if removal.Roots.bodyVersion() < snapshotBodyV3 {
		return PropertyRedactionProof{}, fmt.Errorf(
			"disk: this image's leaves hash properties inline, so a property redaction " +
				"cannot be proven without revealing them; compact to adopt the separated hash")
	}

	surviving, err := s.ProveNode(id)
	if err != nil {
		return PropertyRedactionProof{}, err
	}

	// The ledger record holds the destroyed blob's hash; the image holds the
	// entity's surviving identity. Neither alone reconstructs the prior leaf.
	reds, err := ReadRedactions(s.dir)
	if err != nil {
		return PropertyRedactionProof{}, err
	}
	var prior merkle.Hash
	found := false
	for _, r := range reds {
		if r.Seq == removal.Tombstone.RedactionSeq {
			prior, found = r.PriorPropertiesHash, true
			break
		}
	}
	if !found {
		return PropertyRedactionProof{}, fmt.Errorf(
			"disk: the ledger has no record %d, which the image's tombstone names",
			removal.Tombstone.RedactionSeq)
	}
	if prior == (merkle.Hash{}) {
		return PropertyRedactionProof{}, fmt.Errorf(
			"disk: redaction %d predates the separated property hash and cannot be proven "+
				"without the removed content", removal.Tombstone.RedactionSeq)
	}

	priorLeaf, err := swapPropertiesHash(surviving.LeafData, prior)
	if err != nil {
		return PropertyRedactionProof{}, err
	}

	return PropertyRedactionProof{
		NodeID:        id,
		Surviving:     surviving,
		Removal:       removal,
		PriorLeafData: priorLeaf,
	}, nil
}

// swapPropertiesHash replaces the trailing property hash of a v2 node leaf.
//
// The property hash is the last 32 bytes by construction, and everything before
// it is the entity's identity — which is the whole reason the encoding puts it
// there.
func swapPropertiesHash(leaf []byte, h merkle.Hash) ([]byte, error) {
	if len(leaf) < 1+merkle.Size || leaf[0] != leafTagNodeV2 {
		return nil, errors.New("disk: not a v2 node leaf, so its property hash cannot be substituted")
	}
	out := make([]byte, len(leaf))
	copy(out, leaf)
	copy(out[len(out)-merkle.Size:], h[:])
	return out, nil
}

// VerifyPropertyRedaction checks a property-redaction proof against a snapshot
// root the verifier obtained independently.
//
// **The verifier never sees the removed properties.** What they see is that the
// prior leaf and the surviving leaf are byte-identical except in their final 32
// bytes, that the prior one hashes to the version the ledger recorded as
// destroyed, and that the surviving one is in the image.
func VerifyPropertyRedaction(snapshotRoot merkle.Hash, p PropertyRedactionProof) error {
	if err := VerifyNodeInclusion(snapshotRoot, p.Surviving); err != nil {
		return fmt.Errorf("verify property redaction: the entity is not in this image: %w", err)
	}
	if err := VerifyRedactionInclusion(snapshotRoot, p.Removal); err != nil {
		return fmt.Errorf("verify property redaction: the removal is not recorded: %w", err)
	}
	if p.Removal.Tombstone.Scope != ScopeProperties {
		return fmt.Errorf("verify property redaction: the tombstone describes a %s removal",
			p.Removal.Tombstone.Scope)
	}
	if p.Removal.Tombstone.NodeID != p.NodeID || p.Surviving.NodeID != p.NodeID {
		return errors.New("verify property redaction: the two proofs are about different entities")
	}

	// **Identical except in the property hash.** This is the claim: whatever the
	// properties were, everything else about the entity is unchanged.
	if len(p.PriorLeafData) != len(p.Surviving.LeafData) || len(p.PriorLeafData) < merkle.Size {
		return errors.New("verify property redaction: the prior leaf is not the same shape as the surviving one")
	}
	split := len(p.PriorLeafData) - merkle.Size
	for i := 0; i < split; i++ {
		if p.PriorLeafData[i] != p.Surviving.LeafData[i] {
			return errors.New("verify property redaction: the entity's identity changed too, " +
				"so this is not a property-only redaction")
		}
	}
	if p.PriorLeafData[0] != leafTagNodeV2 {
		return errors.New("verify property redaction: the leaves do not use the separated property hash")
	}

	// The surviving entity must actually have no properties left.
	if empty := propertiesHash(nil); !bytesEqual(p.Surviving.LeafData[split:], empty[:]) {
		return errors.New("verify property redaction: the entity still carries properties")
	}

	// And the prior leaf must be the version the ledger recorded as destroyed.
	if merkle.HashLeaf(p.PriorLeafData) != p.Removal.Tombstone.VersionHash {
		return errors.New("verify property redaction: the reconstructed prior leaf is not the " +
			"version the removal recorded")
	}
	return nil
}

// bytesEqual is bytes.Equal, kept local so this file's imports stay minimal.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
