package disk

// The audit log: a hash-chained record of what was *done* to a store, as
// distinct from what it now contains.
//
// The WAL is a durability mechanism, not an audit trail. It records mutations
// so they survive a crash, and says nothing about anything that did not change
// the graph — a compaction, a key rotation, a verification run, an attestation
// handed to someone, a retention policy deleting history. Those are exactly the
// operator actions an investigation asks about, and until now the engine
// recorded none of them.
//
// # What is recorded, and what is not
//
// Operator actions and caller-declared events. **Not reads or queries.**
//
// That boundary is deliberate rather than lazy. Recording every read would put
// a synchronous append on the hot path of an engine whose entire design is
// ingest-once/query-many, and would produce a log dominated by entries nobody
// reads — which is how audit logs come to be switched off. The events here are
// low-volume and high-value: each one is something a person decided to do.
//
// A caller who *does* need query-level auditing has `Record`, and can decide
// what warrants an entry with knowledge the engine does not have.
//
// # Chaining
//
// Each entry carries the hash of the one before it, so removing an entry from
// the middle breaks every link after it. That does not stop deletion — nothing
// in-process can — but it makes a **selective** excision detectable, which is
// the realistic threat: an operator who wants one embarrassing entry gone, not
// the whole file. Deleting the whole file is still undetectable without an
// external anchor, and the plan is explicit about that (§12.4 T-13, T-14).

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const auditFileName = "graphene.audit"

// domainAuditEntry separates audit hashes from every other hashed structure.
const domainAuditEntry = 0xAD

// AuditKind classifies what happened.
type AuditKind uint16

const (
	AuditUnknown AuditKind = iota

	// Engine-recorded operator actions.
	AuditOpen
	AuditCompact
	AuditKeyRotation
	AuditRetentionDelete
	AuditVerify
	AuditAttestationExport

	// AuditCustom is the caller's, whose meaning the engine does not interpret.
	AuditCustom AuditKind = 1000
)

func (k AuditKind) String() string {
	switch k {
	case AuditOpen:
		return "open"
	case AuditCompact:
		return "compact"
	case AuditKeyRotation:
		return "key-rotation"
	case AuditRetentionDelete:
		return "retention-delete"
	case AuditVerify:
		return "verify"
	case AuditAttestationExport:
		return "attestation-export"
	case AuditCustom:
		return "custom"
	default:
		return fmt.Sprintf("unknown(%d)", uint16(k))
	}
}

// AuditEntry is one record in the chain.
type AuditEntry struct {
	// Seq is the entry's position, starting at 1.
	Seq uint64

	UnixNano int64
	ActorID  uint64
	Kind     AuditKind

	// Detail is free text describing the action. It is hashed, so it cannot be
	// edited without breaking the chain, but the engine never interprets it.
	Detail string

	// PrevHash links to the entry before this one; zero for the first.
	PrevHash [sha256.Size]byte

	// Hash covers everything above.
	Hash [sha256.Size]byte
}

// auditEntryData is what an entry's hash covers.
func auditEntryData(e AuditEntry) []byte {
	buf := make([]byte, 0, 1+8+8+8+2+len(e.Detail)+sha256.Size)
	buf = append(buf, domainAuditEntry)
	buf = binary.LittleEndian.AppendUint64(buf, e.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.UnixNano))
	buf = binary.LittleEndian.AppendUint64(buf, e.ActorID)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(e.Kind))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Detail)))
	buf = append(buf, e.Detail...)
	return append(buf, e.PrevHash[:]...)
}

func computeAuditHash(e AuditEntry) [sha256.Size]byte {
	return sha256.Sum256(auditEntryData(e))
}

// --- encoding ---
//
// One length-prefixed record per entry, so a torn tail is detectable and the
// file can be read forward without an index.

const auditRecordFixed = 8 + 8 + 8 + 2 + 4 + sha256.Size + sha256.Size

func appendAuditRecord(buf []byte, e AuditEntry) []byte {
	body := make([]byte, 0, auditRecordFixed+len(e.Detail))
	body = binary.LittleEndian.AppendUint64(body, e.Seq)
	body = binary.LittleEndian.AppendUint64(body, uint64(e.UnixNano))
	body = binary.LittleEndian.AppendUint64(body, e.ActorID)
	body = binary.LittleEndian.AppendUint16(body, uint16(e.Kind))
	body = binary.LittleEndian.AppendUint32(body, uint32(len(e.Detail)))
	body = append(body, e.Detail...)
	body = append(body, e.PrevHash[:]...)
	body = append(body, e.Hash[:]...)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
	return append(buf, body...)
}

// readAuditRecords parses every complete entry, stopping at the first torn one.
//
// A partial tail is the normal shape after a crash mid-append, so it stops
// rather than failing — the same disposition WAL replay gives a torn record.
func readAuditRecords(data []byte) ([]AuditEntry, error) {
	var out []AuditEntry
	pos := 0

	for pos+4 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		// A length larger than the file cannot be a record. Bounded before it is
		// used, like every other length prefix read from a file here.
		if size < auditRecordFixed || pos+size > len(data) {
			break
		}
		body := data[pos : pos+size]
		pos += size

		var e AuditEntry
		p := 0
		e.Seq = binary.LittleEndian.Uint64(body[p:])
		p += 8
		e.UnixNano = int64(binary.LittleEndian.Uint64(body[p:]))
		p += 8
		e.ActorID = binary.LittleEndian.Uint64(body[p:])
		p += 8
		e.Kind = AuditKind(binary.LittleEndian.Uint16(body[p:]))
		p += 2
		detailLen := int(binary.LittleEndian.Uint32(body[p:]))
		p += 4
		if detailLen < 0 || p+detailLen+2*sha256.Size > len(body) {
			return out, fmt.Errorf("audit: entry %d declares a %d-byte detail that does not fit", e.Seq, detailLen)
		}
		e.Detail = string(body[p : p+detailLen])
		p += detailLen
		copy(e.PrevHash[:], body[p:p+sha256.Size])
		p += sha256.Size
		copy(e.Hash[:], body[p:p+sha256.Size])

		out = append(out, e)
	}
	return out, nil
}

// --- the log ---

// AuditLog appends hash-chained entries to a file.
//
// Safe for concurrent use. Every append is synced before it returns: an audit
// entry that is lost in a crash is worse than useless, because the operation it
// describes did happen and the record now says otherwise.
type AuditLog struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	lastSeq  uint64
	lastHash [sha256.Size]byte
}

// OpenAuditLog opens or creates the audit log in dir, resuming its chain.
func OpenAuditLog(dir string) (*AuditLog, error) {
	path := filepath.Join(dir, auditFileName)

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("audit: %w", err)
	}
	entries, err := readAuditRecords(existing)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	a := &AuditLog{path: path, file: f}
	if n := len(entries); n > 0 {
		a.lastSeq = entries[n-1].Seq
		a.lastHash = entries[n-1].Hash
	}
	return a, nil
}

// Record appends an entry and returns it.
func (a *AuditLog) Record(kind AuditKind, actorID uint64, detail string) (AuditEntry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	e := AuditEntry{
		Seq:      a.lastSeq + 1,
		UnixNano: time.Now().UnixNano(),
		ActorID:  actorID,
		Kind:     kind,
		Detail:   detail,
		PrevHash: a.lastHash,
	}
	e.Hash = computeAuditHash(e)

	if _, err := a.file.Write(appendAuditRecord(nil, e)); err != nil {
		return AuditEntry{}, fmt.Errorf("audit: write: %w", err)
	}
	// Synced before returning: an entry lost to a crash describes an action that
	// still happened, which makes the log actively misleading rather than merely
	// incomplete.
	if err := a.file.Sync(); err != nil {
		return AuditEntry{}, fmt.Errorf("audit: sync: %w", err)
	}

	a.lastSeq = e.Seq
	a.lastHash = e.Hash
	return e, nil
}

// Close closes the log.
func (a *AuditLog) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// ReadAuditLog returns every entry in dir's audit log, oldest first.
//
// Reads the file directly and does not open the store, like the rest of the
// inspection surface.
func ReadAuditLog(dir string) ([]AuditEntry, error) {
	path := filepath.Join(dir, auditFileName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	return readAuditRecords(data)
}

// ErrAuditChainBroken is returned when the chain does not hold.
var ErrAuditChainBroken = errors.New("audit: chain is broken")

// VerifyAuditChain checks that every entry hashes to what it claims and links to
// the one before it.
//
// Two failures are reported distinctly. A **hash mismatch** means the entry's
// own contents were edited. A **link mismatch** means an entry was removed or
// inserted. They point at different actions and the error says which.
func VerifyAuditChain(entries []AuditEntry) error {
	var prev [sha256.Size]byte
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			return fmt.Errorf("%w: entry %d carries sequence %d — an entry is missing",
				ErrAuditChainBroken, i+1, e.Seq)
		}
		if e.PrevHash != prev {
			return fmt.Errorf("%w: entry %d does not follow entry %d — it names predecessor %x",
				ErrAuditChainBroken, e.Seq, e.Seq-1, e.PrevHash[:8])
		}
		if computeAuditHash(e) != e.Hash {
			return fmt.Errorf("%w: entry %d does not hash to its recorded value — its contents were changed",
				ErrAuditChainBroken, e.Seq)
		}
		prev = e.Hash
	}
	return nil
}
