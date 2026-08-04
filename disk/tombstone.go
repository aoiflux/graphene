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
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// leafTagTombstone separates a tombstone leaf from every other leaf kind, so a
// removal can never be presented as a presence.
const leafTagTombstone = 0x04

// tombstoneBodyVersion versions the GRDT section body.
const tombstoneBodyVersion = 1

// Tombstone is the image's record that an entity was deliberately removed.
type Tombstone struct {
	NodeID store.NodeID

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
	buf := make([]byte, 0, 1+8+merkle.Size+8+sha256.Size)
	buf = append(buf, leafTagTombstone)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.NodeID))
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
			NodeID:        r.NodeID,
			VersionHash:   r.VersionHash,
			RedactionSeq:  r.Seq,
			RedactionHash: r.Hash,
		})
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

const tombstoneEntrySize = 8 + merkle.Size + 8 + sha256.Size

func appendTombstoneSection(buf []byte, ts []Tombstone) []byte {
	buf = append(buf, tombstoneBodyVersion)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(ts)))
	for _, t := range ts {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(t.NodeID))
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
	if v := data[0]; v != tombstoneBodyVersion {
		return nil, fmt.Errorf("tombstone section version %d, this build understands %d",
			v, tombstoneBodyVersion)
	}
	n := int(binary.LittleEndian.Uint32(data[1:5]))

	// Bounded against the section's own length before anything is allocated. A
	// five-byte section must never be able to ask for gigabytes, which is the
	// discipline every other length prefix read from a file here follows.
	if n < 0 || n > (len(data)-5)/tombstoneEntrySize {
		return nil, fmt.Errorf("tombstone section claims %d entries but holds %d bytes",
			n, len(data)-5)
	}

	out := make([]Tombstone, n)
	pos := 5
	for i := 0; i < n; i++ {
		out[i].NodeID = store.NodeID(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
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

	idx := -1
	for i := range csr.tombstones {
		if csr.tombstones[i].NodeID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return RedactionInclusionProof{}, ErrNoTombstone
	}

	proof, err := merkle.BuildProof(tombstoneLeaves(csr.tombstones), idx)
	if err != nil {
		return RedactionInclusionProof{}, fmt.Errorf("disk: building a tombstone proof for node %d: %w", id, err)
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
