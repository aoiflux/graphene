package disk

// Roles and capabilities: attribution, not enforcement.
//
// §12.4's T-16 records that no role model exists anywhere in the tree and every
// caller has every capability. It also says what a role model can and cannot be
// here, and the limit is the important half:
//
//	The engine is in-process — RBAC here is an *audit and attribution*
//	mechanism, not a security boundary. Saying otherwise would be misleading.
//
// That is not a hedge, it is the design. Graphene is a library linked into your
// process; anything running there can call `AddNode` directly, bypass every
// check in this file, or use your signing key. **Nothing in the engine calls
// CheckCapability.** A caller who wants admission control calls it themselves,
// at their own boundary, and gets exactly as much enforcement as that boundary
// is worth.
//
// # What it is for, then
//
// §13.2's INV-3: *permissions granted within a session never increase without an
// attested grant record.* Formally, `caps(t₂) ⊆ caps(t₁) ∪ granted(t₁,t₂)`.
//
// That property is worth having on its own. It means a privilege change cannot
// happen quietly: an actor's capabilities are **derived only from the grant
// ledger**, which is hash-chained and signed like every other history here, so
// there is no path by which someone acquires a capability without a record of it
// existing. The invariant holds by construction rather than by checking, which
// is the only way it could hold in a process where the checks are advisory.
//
// The ledger's head is bound into the checkpoint (anchor.go), so deleting it
// wholesale is externally detectable rather than silent.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aoiflux/graphene/store"
)

// domainRoleGrant separates grant hashes from every other hashed structure.
const domainRoleGrant = 0x9B

const grantFileName = "graphene.grants"

// Capability is a bitmap of what a role may do.
//
// Deliberately coarse. A finer model would invite the belief that the engine is
// enforcing something subtle, and it is enforcing nothing at all — these are the
// distinctions an audit needs to be able to make after the fact, not a
// permission system.
type Capability uint32

const (
	// CapRead covers queries and traversal. Present for completeness of the
	// record: the engine does not gate reads, and an audit that wants to
	// distinguish a read-only role from a writing one needs to be able to say so.
	CapRead Capability = 1 << iota

	// CapWrite covers adding and updating entities.
	CapWrite

	// CapDelete covers unattributed removal — DeleteNode and DeleteEdge.
	CapDelete

	// CapRedact covers attributed removal, which is a different decision from
	// CapDelete and usually belongs to different people.
	CapRedact

	// CapCompact covers compaction, which produces roots and attestations.
	CapCompact

	// CapRotateKey covers signing-key rotation.
	CapRotateKey

	// CapPublish covers publishing a checkpoint to an anchor.
	CapPublish

	// CapGrant covers changing what other actors may do. Held separately because
	// an actor who can grant capabilities effectively holds all of them.
	CapGrant
)

// capabilityNames drives String, and is the order they are reported in.
var capabilityNames = []struct {
	cap  Capability
	name string
}{
	{CapRead, "read"},
	{CapWrite, "write"},
	{CapDelete, "delete"},
	{CapRedact, "redact"},
	{CapCompact, "compact"},
	{CapRotateKey, "rotate-key"},
	{CapPublish, "publish"},
	{CapGrant, "grant"},
}

func (c Capability) String() string {
	if c == 0 {
		return "none"
	}
	var parts []string
	for _, e := range capabilityNames {
		if c&e.cap != 0 {
			parts = append(parts, e.name)
		}
	}
	if rest := c &^ allCapabilities(); rest != 0 {
		parts = append(parts, fmt.Sprintf("unknown(%#x)", uint32(rest)))
	}
	return strings.Join(parts, "|")
}

// Has reports whether every capability in want is present.
func (c Capability) Has(want Capability) bool { return c&want == want }

func allCapabilities() Capability {
	var all Capability
	for _, e := range capabilityNames {
		all |= e.cap
	}
	return all
}

// GrantKind distinguishes a grant from its withdrawal.
type GrantKind uint8

const (
	// GrantAdd adds capabilities to an actor. Zero, so a record with no explicit
	// kind reads as the additive case.
	GrantAdd GrantKind = iota

	// GrantRevoke removes them.
	GrantRevoke
)

func (k GrantKind) String() string {
	if k == GrantRevoke {
		return "revoke"
	}
	return "grant"
}

// ErrGrantUnexplained rejects a privilege change with no stated reason.
//
// Same argument as a redaction: a privilege change nobody can account for is
// indistinguishable from an escalation, and that distinction is the only thing
// this ledger exists to preserve.
var ErrGrantUnexplained = errors.New("disk: a role grant must state a reason")

// ErrNoGrantLedger is returned when grants are not enabled.
var ErrNoGrantLedger = errors.New("disk: role grants require Options.Roles")

// ErrNotPermitted reports that an actor lacks a capability.
//
// **Advisory.** It is returned by CheckCapability, which nothing in the engine
// calls. A caller who ignores it is not defeating a control; there is no control.
var ErrNotPermitted = errors.New("disk: actor does not hold this capability")

// RoleGrant is one entry in the grant ledger.
type RoleGrant struct {
	Seq      uint64
	UnixNano int64

	// Kind says whether Capabilities were added or withdrawn.
	Kind GrantKind

	// Subject is the actor whose capabilities changed, and RoleID the role they
	// were granted under — recorded for reconstruction, never interpreted.
	Subject uint64
	RoleID  uint32

	// Capabilities is the set added or removed by this record.
	Capabilities Capability

	// GrantedBy is the actor making the change. An actor granting themselves a
	// capability is legal and recorded as such, which is exactly the shape an
	// audit wants to be able to see.
	GrantedBy uint64

	Reason string

	PrevHash [sha256.Size]byte
	Hash     [sha256.Size]byte

	Signature []byte
	KeyID     uint64
}

func grantSignedData(g RoleGrant) []byte {
	buf := make([]byte, 0, 1+8+8+1+8+4+4+8+4+len(g.Reason)+sha256.Size)
	buf = append(buf, domainRoleGrant)
	buf = binary.LittleEndian.AppendUint64(buf, g.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(g.UnixNano))
	buf = append(buf, byte(g.Kind))
	buf = binary.LittleEndian.AppendUint64(buf, g.Subject)
	buf = binary.LittleEndian.AppendUint32(buf, g.RoleID)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(g.Capabilities))
	buf = binary.LittleEndian.AppendUint64(buf, g.GrantedBy)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(g.Reason)))
	buf = append(buf, g.Reason...)
	return append(buf, g.PrevHash[:]...)
}

func computeGrantHash(g RoleGrant) [sha256.Size]byte {
	return sha256.Sum256(grantSignedData(g))
}

func (g RoleGrant) String() string {
	return fmt.Sprintf("%s %d at %s: actor %d role %d %s by actor %d — %q",
		g.Kind, g.Seq, time.Unix(0, g.UnixNano).UTC().Format(time.RFC3339),
		g.Subject, g.RoleID, g.Capabilities, g.GrantedBy, g.Reason)
}

// --- the ledger ---

// GrantLedger is the append-only file of privilege changes.
type GrantLedger struct {
	mu       sync.Mutex
	file     *os.File
	lastSeq  uint64
	lastHash [sha256.Size]byte
}

// OpenGrantLedger opens or creates dir's grant ledger, resuming its chain.
func OpenGrantLedger(dir string) (*GrantLedger, error) {
	existing, err := ReadGrants(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, grantFileName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	l := &GrantLedger{file: f}
	if n := len(existing); n > 0 {
		l.lastSeq = existing[n-1].Seq
		l.lastHash = existing[n-1].Hash
	}
	return l, nil
}

// Close releases the ledger file. Nil-safe, like the other ledgers here.
func (l *GrantLedger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Head returns the hash of the last record, zero when empty.
func (l *GrantLedger) Head() [sha256.Size]byte {
	if l == nil {
		return [sha256.Size]byte{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastHash
}

func (l *GrantLedger) append(g RoleGrant, signer store.Signer) (RoleGrant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	g.Seq = l.lastSeq + 1
	g.PrevHash = l.lastHash
	g.Hash = computeGrantHash(g)

	if signer != nil {
		sig, err := signer.Sign(g.Hash[:])
		if err != nil {
			return RoleGrant{}, fmt.Errorf("disk: signing the grant record: %w", err)
		}
		g.Signature = sig
		g.KeyID = signer.KeyID()
	}

	if _, err := l.file.Write(appendGrantRecord(nil, g)); err != nil {
		return RoleGrant{}, err
	}
	if err := l.file.Sync(); err != nil {
		return RoleGrant{}, err
	}

	l.lastSeq = g.Seq
	l.lastHash = g.Hash
	return g, nil
}

// --- encoding ---

const grantRecordFixed = 8 + 8 + 1 + 8 + 4 + 4 + 8 + sha256.Size + sha256.Size + 8 + 4 + 4

func appendGrantRecord(buf []byte, g RoleGrant) []byte {
	body := make([]byte, 0, grantRecordFixed+len(g.Reason)+len(g.Signature))
	body = binary.LittleEndian.AppendUint64(body, g.Seq)
	body = binary.LittleEndian.AppendUint64(body, uint64(g.UnixNano))
	body = append(body, byte(g.Kind))
	body = binary.LittleEndian.AppendUint64(body, g.Subject)
	body = binary.LittleEndian.AppendUint32(body, g.RoleID)
	body = binary.LittleEndian.AppendUint32(body, uint32(g.Capabilities))
	body = binary.LittleEndian.AppendUint64(body, g.GrantedBy)
	body = append(body, g.PrevHash[:]...)
	body = append(body, g.Hash[:]...)
	body = binary.LittleEndian.AppendUint64(body, g.KeyID)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(g.Reason)))
	body = append(body, g.Reason...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(g.Signature)))
	body = append(body, g.Signature...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
	return append(buf, body...)
}

// ReadGrants returns every record in dir's grant ledger, oldest first.
func ReadGrants(dir string) ([]RoleGrant, error) {
	data, err := os.ReadFile(filepath.Join(dir, grantFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []RoleGrant
	for off := 0; off+4 <= len(data); {
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if n < 0 || off+n > len(data) {
			break // torn tail: everything before it is whole
		}
		g, perr := parseGrantRecord(data[off : off+n])
		if perr != nil {
			return out, perr
		}
		out = append(out, g)
		off += n
	}
	return out, nil
}

func parseGrantRecord(b []byte) (RoleGrant, error) {
	const head = 8 + 8 + 1 + 8 + 4 + 4 + 8 + sha256.Size + sha256.Size + 8
	if len(b) < head+4 {
		return RoleGrant{}, errors.New("disk: grant record is too short for its fixed fields")
	}
	var g RoleGrant
	g.Seq = binary.LittleEndian.Uint64(b[0:8])
	g.UnixNano = int64(binary.LittleEndian.Uint64(b[8:16]))
	g.Kind = GrantKind(b[16])
	g.Subject = binary.LittleEndian.Uint64(b[17:25])
	g.RoleID = binary.LittleEndian.Uint32(b[25:29])
	g.Capabilities = Capability(binary.LittleEndian.Uint32(b[29:33]))
	g.GrantedBy = binary.LittleEndian.Uint64(b[33:41])
	off := 41
	copy(g.PrevHash[:], b[off:off+sha256.Size])
	off += sha256.Size
	copy(g.Hash[:], b[off:off+sha256.Size])
	off += sha256.Size
	g.KeyID = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8

	var err error
	if g.Reason, off, err = readLengthPrefixedString(b, off, "reason"); err != nil {
		return RoleGrant{}, err
	}
	sig, _, err := readLengthPrefixedString(b, off, "signature")
	if err != nil {
		return RoleGrant{}, err
	}
	if sig != "" {
		g.Signature = []byte(sig)
	}
	return g, nil
}

// VerifyGrantChain checks that each record's hash follows from its contents and
// links to the one before it.
//
// verifier may be nil, in which case signatures are not checked and the hash
// chain still holds.
func VerifyGrantChain(records []RoleGrant, verifier store.Verifier) error {
	var prev [sha256.Size]byte
	for i, g := range records {
		if want := uint64(i + 1); g.Seq != want {
			return fmt.Errorf("grant chain: entry %d carries sequence %d, expected %d", i, g.Seq, want)
		}
		if g.PrevHash != prev {
			return fmt.Errorf("grant chain: record %d does not link to its predecessor; "+
				"a record was removed or reordered", g.Seq)
		}
		if got := computeGrantHash(g); got != g.Hash {
			return fmt.Errorf("grant chain: record %d's hash does not follow from its contents; "+
				"it was edited after being written", g.Seq)
		}
		if verifier != nil && len(g.Signature) > 0 {
			if err := verifier.Verify(g.KeyID, g.Hash[:], g.Signature); err != nil {
				return fmt.Errorf("grant chain: record %d's signature does not verify: %w", g.Seq, err)
			}
		}
		prev = g.Hash
	}
	return nil
}

// CapabilitiesFrom replays a grant ledger into the capability set each actor
// holds at the end of it.
//
// **This is where INV-3 comes from.** Capabilities are a pure function of the
// ledger and of nothing else — there is no other path by which an actor acquires
// one — so `caps(t₂) ⊆ caps(t₁) ∪ granted(t₁,t₂)` holds by construction rather
// than by any check. A capability that appears without a record is not possible
// to express, which is a stronger thing than a capability that is checked for.
//
// Package-level and taking a slice, so a third party holding an exported ledger
// can compute the same answer the store would.
func CapabilitiesFrom(records []RoleGrant) map[uint64]Capability {
	out := make(map[uint64]Capability)
	for _, g := range records {
		switch g.Kind {
		case GrantRevoke:
			out[g.Subject] &^= g.Capabilities
		default:
			out[g.Subject] |= g.Capabilities
		}
	}
	// A actor reduced to nothing is dropped rather than left as an empty entry,
	// so "holds no capabilities" and "was never mentioned" read the same — which
	// they should, because they are the same.
	for actor, caps := range out {
		if caps == 0 {
			delete(out, actor)
		}
	}
	return out
}

// --- the operations ---

// GrantRole records that an actor was given capabilities.
func (s *Store) GrantRole(subject uint64, roleID uint32, caps Capability, req GrantRequest) (RoleGrant, error) {
	return s.recordGrant(GrantAdd, subject, roleID, caps, req)
}

// RevokeRole records that capabilities were withdrawn.
func (s *Store) RevokeRole(subject uint64, roleID uint32, caps Capability, req GrantRequest) (RoleGrant, error) {
	return s.recordGrant(GrantRevoke, subject, roleID, caps, req)
}

// GrantRequest is what a caller must supply to change what an actor may do.
type GrantRequest struct {
	// GrantedBy is the actor making the change. Recorded, never checked — see
	// this file's header for why.
	GrantedBy uint64

	// Reason is why, in the caller's own words. Required.
	Reason string
}

func (s *Store) recordGrant(kind GrantKind, subject uint64, roleID uint32, caps Capability, req GrantRequest) (RoleGrant, error) {
	if req.Reason == "" {
		return RoleGrant{}, ErrGrantUnexplained
	}
	if caps == 0 {
		return RoleGrant{}, errors.New("disk: a grant must name at least one capability")
	}
	if s.grants == nil {
		return RoleGrant{}, ErrNoGrantLedger
	}

	rec, err := s.grants.append(RoleGrant{
		UnixNano:     time.Now().UnixNano(),
		Kind:         kind,
		Subject:      subject,
		RoleID:       roleID,
		Capabilities: caps,
		GrantedBy:    req.GrantedBy,
		Reason:       req.Reason,
	}, s.signer)
	if err != nil {
		return RoleGrant{}, err
	}

	if err := s.recordAudit(AuditRoleGrant, req.GrantedBy,
		fmt.Sprintf("%s %s for actor %d (role %d): %s", kind, caps, subject, roleID, req.Reason)); err != nil {
		return rec, err
	}
	return rec, nil
}

// Grants returns this store's grant ledger, oldest first.
func (s *Store) Grants() ([]RoleGrant, error) { return ReadGrants(s.dir) }

// Capabilities reports what an actor currently holds, per the ledger.
func (s *Store) Capabilities(actor uint64) (Capability, error) {
	records, err := s.Grants()
	if err != nil {
		return 0, err
	}
	return CapabilitiesFrom(records)[actor], nil
}

// CheckCapability reports whether an actor holds a capability.
//
// **Advisory, and nothing in the engine calls it.** Graphene is a library in
// your process: a caller who wants this enforced calls it at their own boundary,
// before doing the thing. Wiring it into the engine's own write paths would
// produce a check that any code in the same process steps around by calling one
// layer down — which is worse than no check, because it looks like one.
func (s *Store) CheckCapability(actor uint64, want Capability) error {
	held, err := s.Capabilities(actor)
	if err != nil {
		return err
	}
	if !held.Has(want) {
		return fmt.Errorf("%w: actor %d holds %s, needs %s", ErrNotPermitted, actor, held, want)
	}
	return nil
}
