package disk

// Redaction: destroying content without destroying the record that it existed.
//
// Today `DeleteNode` cascades to incident edges, tombstones everything, and
// records nothing about who did it or why. After a compaction the entity is
// gone and so is any trace that it was ever there. §12.4's T-12 states the
// consequence plainly: **lawful redaction and evidence destruction are currently
// the same operation.** An engine holding evidence has to be able to comply with
// an erasure order without that compliance being indistinguishable from
// tampering.
//
// # What survives, and what does not
//
// A redaction destroys the plaintext and keeps everything else:
//
//	the fact      a record exists that a redaction happened
//	the actor     who did it, under which role, and when
//	the reason    free text the caller supplies, and must supply
//	the shape     which entity, and the full set cascaded with it
//	the identity  the version hash of what was removed
//
// The version hash is the load-bearing part. Keeping it means a party holding
// the redacted content can *prove* it was what was removed, and a party holding
// only the ledger can prove something specific was removed without learning what.
// That is §13.3's PO-4, and it is the difference between a redaction and a hole.
//
// # Why this is not in the WAL
//
// §12.4's T-10 proposes a WAL record for this. It cannot live there. Compaction
// truncates the WAL, and unless retention is configured — it is off by default —
// the deletion record would be destroyed by the very operation that makes the
// deletion permanent. A record that disappears exactly when it becomes the only
// evidence is not a record.
//
// The ledger is its own append-only, hash-chained file that compaction never
// touches, and its head is bound into the checkpoint (anchor.go) so that
// deleting the ledger wholesale is externally detectable.
//
// # What this does not do
//
// After a compaction the *graph* retains no trace of a redacted entity — no
// tombstone in the CSR image, no inclusion proof of absence. Only the ledger
// knows. Proving a redaction from a snapshot alone would need a tombstone record
// in the image itself, which is a format change this does not make.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// domainRedaction separates redaction hashes from every other hashed structure.
const domainRedaction = 0x2D

const redactionFileName = "graphene.redactions"

// ErrRedactionUnexplained rejects a redaction with no stated reason.
//
// Not a formality. An unexplained redaction is indistinguishable from evidence
// destruction, which is the entire distinction this file exists to draw, and a
// caller who cannot say why is not performing a lawful redaction.
var ErrRedactionUnexplained = errors.New("disk: a redaction must state a reason")

// ErrCascadeTooLarge reports a redaction that would remove more than the policy
// allows.
var ErrCascadeTooLarge = errors.New("disk: redaction cascade exceeds the configured limit")

// RedactionPolicy bounds what a single redaction may remove.
//
// §12.4's T-11: deleting a hub node silently removes a large evidentiary
// subgraph. Legitimate large deletions exist, so the engine has no default
// opinion — the zero value permits any cascade — but it will enforce a caller's.
type RedactionPolicy struct {
	// MaxCascade caps the number of incident edges a single node redaction may
	// take with it. Zero means unbounded.
	MaxCascade int
}

// RedactionRequest is what a caller must supply to redact something.
type RedactionRequest struct {
	// ActorID and RoleID attribute the decision. RoleID is recorded, never
	// checked — there is no role model; see SECURITY.md §3.
	ActorID uint64
	RoleID  uint32

	// Reason is why, in the caller's own words. Required.
	Reason string
}

// RedactionImpact is what a redaction would remove, computed before anything is
// destroyed.
//
// §12.4's T-11 asks for a pre-deletion impact report, and the point of it is to
// be available while refusing is still possible.
type RedactionImpact struct {
	NodeID store.NodeID

	// CascadedEdges are the incident edges that would be tombstoned with it.
	CascadedEdges []store.EdgeID

	// VersionHash identifies the node's current content.
	VersionHash merkle.Hash

	// ExceedsPolicy reports whether this cascade is larger than the store's
	// RedactionPolicy allows.
	ExceedsPolicy bool
}

// RedactionRecord is one entry in the ledger.
type RedactionRecord struct {
	Seq      uint64
	UnixNano int64
	ActorID  uint64
	RoleID   uint32

	// NodeID is what was redacted, and CascadedEdges what went with it.
	NodeID        store.NodeID
	CascadedEdges []store.EdgeID

	// VersionHash is the identity of the destroyed content. Holding it lets a
	// party with a copy prove it is what was removed, and a party without one
	// prove that *something specific* was removed.
	VersionHash merkle.Hash

	Reason string

	// PrevHash links to the record before this one; zero for the first.
	PrevHash [sha256.Size]byte

	// Hash covers everything above.
	Hash [sha256.Size]byte

	// Signature is over Hash, by the store's signer. Empty when unsigned, which
	// leaves the record tamper-evident but unattributed to a key.
	Signature []byte
	KeyID     uint64
}

// redactionSignedData is what a record's hash covers.
func redactionSignedData(r RedactionRecord) []byte {
	buf := make([]byte, 0, 1+8+8+8+4+8+4+len(r.CascadedEdges)*8+merkle.Size+4+len(r.Reason)+sha256.Size)
	buf = append(buf, domainRedaction)
	buf = binary.LittleEndian.AppendUint64(buf, r.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(r.UnixNano))
	buf = binary.LittleEndian.AppendUint64(buf, r.ActorID)
	buf = binary.LittleEndian.AppendUint32(buf, r.RoleID)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(r.NodeID))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.CascadedEdges)))
	for _, e := range r.CascadedEdges {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e))
	}
	buf = append(buf, r.VersionHash[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.Reason)))
	buf = append(buf, r.Reason...)
	return append(buf, r.PrevHash[:]...)
}

func computeRedactionHash(r RedactionRecord) [sha256.Size]byte {
	return sha256.Sum256(redactionSignedData(r))
}

func (r RedactionRecord) String() string {
	return fmt.Sprintf("redaction %d at %s: node %d (+%d edges) by actor %d role %d — %q",
		r.Seq, time.Unix(0, r.UnixNano).UTC().Format(time.RFC3339),
		r.NodeID, len(r.CascadedEdges), r.ActorID, r.RoleID, r.Reason)
}

// --- the ledger ---

// RedactionLedger is the append-only file of redaction records.
type RedactionLedger struct {
	mu       sync.Mutex
	file     *os.File
	lastSeq  uint64
	lastHash [sha256.Size]byte
}

// OpenRedactionLedger opens or creates dir's ledger, reading it to find the
// chain head.
func OpenRedactionLedger(dir string) (*RedactionLedger, error) {
	existing, err := ReadRedactions(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, redactionFileName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	l := &RedactionLedger{file: f}
	if n := len(existing); n > 0 {
		l.lastSeq = existing[n-1].Seq
		l.lastHash = existing[n-1].Hash
	}
	return l, nil
}

// Close releases the ledger file.
func (l *RedactionLedger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Head returns the hash of the last record, zero when the ledger is empty.
func (l *RedactionLedger) Head() [sha256.Size]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastHash
}

// append writes one record, filling in its sequence, chain link, hash and
// signature.
func (l *RedactionLedger) append(r RedactionRecord, signer store.Signer) (RedactionRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	r.Seq = l.lastSeq + 1
	r.PrevHash = l.lastHash
	r.Hash = computeRedactionHash(r)

	if signer != nil {
		sig, err := signer.Sign(r.Hash[:])
		if err != nil {
			return RedactionRecord{}, fmt.Errorf("disk: signing the redaction record: %w", err)
		}
		r.Signature = sig
		r.KeyID = signer.KeyID()
	}

	if _, err := l.file.Write(appendRedactionRecord(nil, r)); err != nil {
		return RedactionRecord{}, err
	}
	if err := l.file.Sync(); err != nil {
		return RedactionRecord{}, err
	}

	l.lastSeq = r.Seq
	l.lastHash = r.Hash
	return r, nil
}

// --- encoding ---
//
// Length-prefixed records, so a torn tail is detectable and the file reads
// forward without an index.

const redactionRecordFixed = 4 + 8 + 8 + 8 + 4 + 8 + merkle.Size +
	sha256.Size + sha256.Size + 8 + 4 + 4 + 4

func appendRedactionRecord(buf []byte, r RedactionRecord) []byte {
	body := make([]byte, 0, redactionRecordFixed+len(r.Reason)+len(r.CascadedEdges)*8+len(r.Signature))
	body = binary.LittleEndian.AppendUint64(body, r.Seq)
	body = binary.LittleEndian.AppendUint64(body, uint64(r.UnixNano))
	body = binary.LittleEndian.AppendUint64(body, r.ActorID)
	body = binary.LittleEndian.AppendUint32(body, r.RoleID)
	body = binary.LittleEndian.AppendUint64(body, uint64(r.NodeID))
	body = append(body, r.VersionHash[:]...)
	body = append(body, r.PrevHash[:]...)
	body = append(body, r.Hash[:]...)
	body = binary.LittleEndian.AppendUint64(body, r.KeyID)

	body = binary.LittleEndian.AppendUint32(body, uint32(len(r.CascadedEdges)))
	for _, e := range r.CascadedEdges {
		body = binary.LittleEndian.AppendUint64(body, uint64(e))
	}
	body = binary.LittleEndian.AppendUint32(body, uint32(len(r.Reason)))
	body = append(body, r.Reason...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(r.Signature)))
	body = append(body, r.Signature...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
	return append(buf, body...)
}

// ReadRedactions returns every record in dir's ledger, oldest first.
//
// A missing file is an empty ledger rather than an error: most stores have
// redacted nothing.
func ReadRedactions(dir string) ([]RedactionRecord, error) {
	data, err := os.ReadFile(filepath.Join(dir, redactionFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []RedactionRecord
	for off := 0; off+4 <= len(data); {
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if n < 0 || off+n > len(data) {
			break // torn tail: everything before it is whole
		}
		r, err := parseRedactionRecord(data[off : off+n])
		if err != nil {
			return out, err
		}
		out = append(out, r)
		off += n
	}
	return out, nil
}

func parseRedactionRecord(b []byte) (RedactionRecord, error) {
	const head = 8 + 8 + 8 + 4 + 8 + merkle.Size + sha256.Size + sha256.Size + 8
	if len(b) < head+4 {
		return RedactionRecord{}, errors.New("disk: redaction record is too short for its fixed fields")
	}
	var r RedactionRecord
	r.Seq = binary.LittleEndian.Uint64(b[0:8])
	r.UnixNano = int64(binary.LittleEndian.Uint64(b[8:16]))
	r.ActorID = binary.LittleEndian.Uint64(b[16:24])
	r.RoleID = binary.LittleEndian.Uint32(b[24:28])
	r.NodeID = store.NodeID(binary.LittleEndian.Uint64(b[28:36]))
	off := 36
	copy(r.VersionHash[:], b[off:off+merkle.Size])
	off += merkle.Size
	copy(r.PrevHash[:], b[off:off+sha256.Size])
	off += sha256.Size
	copy(r.Hash[:], b[off:off+sha256.Size])
	off += sha256.Size
	r.KeyID = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8

	// Counts are bounded by the record's own length before anything is
	// allocated: a 12-byte record must never be able to ask for a gigabyte.
	if off+4 > len(b) {
		return RedactionRecord{}, errors.New("disk: redaction record truncated before its cascade count")
	}
	n := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 || n > (len(b)-off)/8 {
		return RedactionRecord{}, fmt.Errorf("disk: redaction record claims %d cascaded edges "+
			"but only %d bytes remain", n, len(b)-off)
	}
	r.CascadedEdges = make([]store.EdgeID, n)
	for i := 0; i < n; i++ {
		r.CascadedEdges[i] = store.EdgeID(binary.LittleEndian.Uint64(b[off : off+8]))
		off += 8
	}

	var err error
	if r.Reason, off, err = readLengthPrefixedString(b, off, "reason"); err != nil {
		return RedactionRecord{}, err
	}
	var sig string
	if sig, _, err = readLengthPrefixedString(b, off, "signature"); err != nil {
		return RedactionRecord{}, err
	}
	if sig != "" {
		r.Signature = []byte(sig)
	}
	return r, nil
}

// readLengthPrefixedString reads a uint32-prefixed byte run, refusing a length
// the remaining buffer cannot satisfy.
func readLengthPrefixedString(b []byte, off int, what string) (string, int, error) {
	if off+4 > len(b) {
		return "", off, fmt.Errorf("disk: redaction record truncated before its %s", what)
	}
	n := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 || off+n > len(b) {
		return "", off, fmt.Errorf("disk: redaction record claims a %d-byte %s but only %d bytes remain",
			n, what, len(b)-off)
	}
	return string(b[off : off+n]), off + n, nil
}

// VerifyRedactionChain checks that each record's hash follows from its contents
// and links to the one before it.
//
// verifier may be nil, in which case signatures are not checked — the hash chain
// still holds, so a caller without the public key learns whether records were
// edited or removed.
func VerifyRedactionChain(records []RedactionRecord, verifier store.Verifier) error {
	var prev [sha256.Size]byte
	for i, r := range records {
		if want := uint64(i + 1); r.Seq != want {
			return fmt.Errorf("redaction chain: entry %d carries sequence %d, expected %d", i, r.Seq, want)
		}
		if r.PrevHash != prev {
			return fmt.Errorf("redaction chain: record %d does not link to its predecessor; "+
				"a record was removed or reordered", r.Seq)
		}
		if got := computeRedactionHash(r); got != r.Hash {
			return fmt.Errorf("redaction chain: record %d's hash does not follow from its contents; "+
				"it was edited after being written", r.Seq)
		}
		if verifier != nil && len(r.Signature) > 0 {
			if err := verifier.Verify(r.KeyID, r.Hash[:], r.Signature); err != nil {
				return fmt.Errorf("redaction chain: record %d's signature does not verify: %w", r.Seq, err)
			}
		}
		prev = r.Hash
	}
	return nil
}

// --- the operation ---

// RedactionImpactFor reports what redacting id would remove, without removing
// anything.
func (s *Store) RedactionImpactFor(id store.NodeID) (RedactionImpact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.redactionImpactLocked(id)
}

func (s *Store) redactionImpactLocked(id store.NodeID) (RedactionImpact, error) {
	if !s.nodeExistsLocked(id) {
		return RedactionImpact{}, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	impact := RedactionImpact{
		NodeID:        id,
		CascadedEdges: s.incidentEdgeIDsLocked(id),
	}
	h, err := s.nodeVersionHashLocked(id)
	if err != nil {
		return RedactionImpact{}, err
	}
	impact.VersionHash = h
	impact.ExceedsPolicy = s.redaction.MaxCascade > 0 && len(impact.CascadedEdges) > s.redaction.MaxCascade
	return impact, nil
}

// nodeVersionHashLocked computes the identity of a node's current content, using
// the same canonical bytes the snapshot's Merkle leaves use.
//
// Reusing that encoding is deliberate: a redaction record's version hash and the
// leaf hash of the same node in a snapshot are then the same value, so a party
// holding an old snapshot can match a redaction record against it directly.
func (s *Store) nodeVersionHashLocked(id store.NodeID) (merkle.Hash, error) {
	n, ok := s.getNodeLocked(id)
	if !ok {
		return merkle.Hash{}, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	return merkle.HashLeaf(nodeLeafData(nodeRecord{
		ID:         n.ID,
		Labels:     n.Labels,
		Properties: n.Properties,
	})), nil
}

// RedactNode destroys a node's content and records that it did.
//
// The order is: compute the impact, check the policy, write the ledger record,
// then delete. The record goes down **before** the deletion because the reverse
// order has a window in which the content is gone and nothing says why — and a
// crash in that window produces exactly the unattributed hole this file exists
// to prevent. A record with no matching deletion is recoverable; a deletion with
// no record is not.
func (s *Store) RedactNode(id store.NodeID, req RedactionRequest) (RedactionRecord, error) {
	if req.Reason == "" {
		return RedactionRecord{}, ErrRedactionUnexplained
	}
	if s.redactions == nil {
		return RedactionRecord{}, errors.New("disk: redaction requires Options.Redaction; " +
			"use DeleteNode for an unrecorded removal")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	impact, err := s.redactionImpactLocked(id)
	if err != nil {
		return RedactionRecord{}, err
	}
	if impact.ExceedsPolicy {
		return RedactionRecord{}, fmt.Errorf("%w: node %d would take %d edges with it, limit is %d",
			ErrCascadeTooLarge, id, len(impact.CascadedEdges), s.redaction.MaxCascade)
	}

	rec, err := s.redactions.append(RedactionRecord{
		UnixNano:      time.Now().UnixNano(),
		ActorID:       req.ActorID,
		RoleID:        req.RoleID,
		NodeID:        id,
		CascadedEdges: impact.CascadedEdges,
		VersionHash:   impact.VersionHash,
		Reason:        req.Reason,
	}, s.signer)
	if err != nil {
		return RedactionRecord{}, err
	}

	if err := s.deleteNodeLocked(id, impact.CascadedEdges); err != nil {
		// The ledger now names a redaction that did not happen. That is the safe
		// direction — an over-recorded redaction is visible and explicable, an
		// unrecorded one is not — but the caller must know.
		return rec, fmt.Errorf("disk: redaction %d was recorded but the deletion failed: %w", rec.Seq, err)
	}

	if err := s.recordAudit(AuditRedaction, req.ActorID,
		fmt.Sprintf("redacted node %d (+%d edges): %s", id, len(impact.CascadedEdges), req.Reason)); err != nil {
		return rec, err
	}
	return rec, nil
}

// Redactions returns this store's redaction ledger, oldest first.
func (s *Store) Redactions() ([]RedactionRecord, error) { return ReadRedactions(s.dir) }
