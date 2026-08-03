package disk

// Key rotation: recording *when* a signing key changed, and proving it.
//
// A keyring holding several public keys is enough to keep old signatures
// verifying after a rotation, and BL-16 left it there. That is not enough for
// evidence.
//
// The problem is what happens when a key is later found to be compromised.
// Without a record of when it was replaced, every commit it ever signed becomes
// equally suspect — including the ones made years earlier, before the
// compromise. Genuine evidence turns into apparent tampering, which is a worse
// outcome than not rotating at all. The plan's §11.4 states it directly: "a
// rotated or revoked key retroactively invalidates every signature it ever made
// — which would make the system *worse* than no signatures, because it would
// produce false tamper alarms on genuine evidence."
//
// A key-transition record fixes that by pinning the change to a point in the
// commit sequence. A verifier can then say "this key was authoritative for
// commits 1 through 4,812" instead of "this key is or is not trusted".
//
// # Who signs a transition
//
// The **outgoing** key. That is what makes the record evidence rather than an
// assertion: anyone can claim a key was replaced, but only the holder of the old
// key can produce a signature the old key verifies. A transition signed by the
// incoming key would prove nothing, since the incoming key is exactly what an
// attacker substituting their own key would hold.
//
// The first key in a store has no predecessor and so cannot be introduced this
// way. It is established out of band, in whatever keyring the verifier is given
// — which is the same trust root every signature here ultimately rests on.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aoiflux/graphene/store"
)

const (
	keyTransitionBodyVersion = 1

	// publicKeySize is an Ed25519 public key.
	publicKeySize = 32

	// domainKeyTransition separates transition signatures from commit and
	// attestation signatures.
	domainKeyTransition = 0x0D

	walKeyTransitionPayload = 1 + 8 + 8 + publicKeySize + 8 + 8 + 2 + walSignatureSize
)

// KeyTransition records one signing key being replaced by another.
type KeyTransition struct {
	// PrevKeyID is the outgoing key, which signs this record. NewKeyID is the
	// incoming one.
	PrevKeyID uint64
	NewKeyID  uint64

	// NewPublicKey lets a verifier learn the incoming key from the log itself
	// rather than needing it supplied separately. It is only as trustworthy as
	// the signature over it, which is why the outgoing key signs.
	NewPublicKey [publicKeySize]byte

	// AtCommitSeq is the commit sequence from which the new key is
	// authoritative. Commits at or above it should carry NewKeyID.
	AtCommitSeq uint64
	UnixNano    int64

	Signature []byte
}

// keyTransitionSignedData is the byte string a transition signature covers.
func keyTransitionSignedData(t KeyTransition) []byte {
	buf := make([]byte, 0, 1+8+8+publicKeySize+8+8)
	buf = append(buf, domainKeyTransition)
	buf = binary.LittleEndian.AppendUint64(buf, t.PrevKeyID)
	buf = binary.LittleEndian.AppendUint64(buf, t.NewKeyID)
	buf = append(buf, t.NewPublicKey[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, t.AtCommitSeq)
	return binary.LittleEndian.AppendUint64(buf, uint64(t.UnixNano))
}

func appendKeyTransitionPayload(t KeyTransition) []byte {
	buf := make([]byte, 0, walKeyTransitionPayload)
	buf = append(buf, keyTransitionBodyVersion)
	buf = binary.LittleEndian.AppendUint64(buf, t.PrevKeyID)
	buf = binary.LittleEndian.AppendUint64(buf, t.NewKeyID)
	buf = append(buf, t.NewPublicKey[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, t.AtCommitSeq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.UnixNano))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(t.Signature)))
	return append(buf, t.Signature...)
}

func readKeyTransitionPayload(data []byte) (KeyTransition, error) {
	var t KeyTransition
	if len(data) < 1 {
		return t, errors.New("empty key transition")
	}
	if v := data[0]; v != keyTransitionBodyVersion {
		return t, fmt.Errorf("key transition version %d, this build understands %d",
			v, keyTransitionBodyVersion)
	}
	if len(data) < walKeyTransitionPayload {
		return t, fmt.Errorf("truncated key transition: %d bytes, need %d",
			len(data), walKeyTransitionPayload)
	}
	pos := 1
	t.PrevKeyID = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	t.NewKeyID = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	copy(t.NewPublicKey[:], data[pos:pos+publicKeySize])
	pos += publicKeySize
	t.AtCommitSeq = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	t.UnixNano = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	sigLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if sigLen > walSignatureSize || pos+sigLen > len(data) {
		return KeyTransition{}, fmt.Errorf("key transition declares a %d-byte signature", sigLen)
	}
	t.Signature = append([]byte(nil), data[pos:pos+sigLen]...)
	return t, nil
}

// ErrNoSigner is returned when an operation needs a configured signer.
var ErrNoSigner = errors.New("disk: store has no signer configured")

// RotateKey records that the store's current signing key is replaced by newKey
// from this point in the commit sequence, and switches to it.
//
// The transition is signed by the **outgoing** key, so it is evidence the
// rotation was authorised rather than a claim that it happened. newPublicKey is
// the incoming key's public half, carried in the record so a verifier can learn
// it from the log.
//
// The record is written and synced before the switch takes effect, so a crash
// mid-rotation leaves a log whose transition is either absent or complete — and
// in the absent case the store simply carries on with the old key.
func (s *Store) RotateKey(newKey store.Signer, newPublicKey []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.signer == nil {
		return fmt.Errorf("%w: cannot rotate a key that was never set", ErrNoSigner)
	}
	if len(newPublicKey) != publicKeySize {
		return fmt.Errorf("disk.RotateKey: public key is %d bytes, want %d",
			len(newPublicKey), publicKeySize)
	}
	if newKey.KeyID() == s.signer.KeyID() {
		return errors.New("disk.RotateKey: the new key has the same ID as the current one")
	}

	now := time.Now().UnixNano
	if s.nowUnixNano != nil {
		now = s.nowUnixNano
	}

	t := KeyTransition{
		PrevKeyID: s.signer.KeyID(),
		NewKeyID:  newKey.KeyID(),
		// The transition takes effect from the next commit, so it names one past
		// the current high-water mark rather than the current value.
		AtCommitSeq: s.commitSeq.Load() + 1,
		UnixNano:    now(),
	}
	copy(t.NewPublicKey[:], newPublicKey)

	sig, err := s.signer.Sign(keyTransitionSignedData(t))
	if err != nil {
		return fmt.Errorf("disk.RotateKey: signing with the outgoing key: %w", err)
	}
	t.Signature = sig

	if err := s.wal.AppendKeyTransition(appendKeyTransitionPayload(t)); err != nil {
		return fmt.Errorf("disk.RotateKey: %w", err)
	}
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("disk.RotateKey: sync: %w", err)
	}

	s.signer = newKey
	s.keyTimeline = append(s.keyTimeline, t)
	return nil
}

// KeyTimeline is the ordered history of a store's signing keys, reconstructed
// from the log.
type KeyTimeline struct {
	// Transitions in commit-sequence order.
	Transitions []KeyTransition
}

// KeyTimeline returns the rotations recorded in the current log.
//
// Bounded by the log's own lifetime: a compaction truncates it, so transitions
// before the last compaction are not here. Durable key history needs the WAL
// retention work, which is not built.
func (s *Store) KeyTimeline() KeyTimeline {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]KeyTransition, len(s.keyTimeline))
	copy(out, s.keyTimeline)
	sort.Slice(out, func(i, j int) bool { return out[i].AtCommitSeq < out[j].AtCommitSeq })
	return KeyTimeline{Transitions: out}
}

// AuthoritativeAt returns the key ID that should have signed the commit at seq,
// given a starting key, and whether the timeline determines it.
//
// The starting key cannot be derived from the log — the first key has no
// transition introducing it — so the caller supplies it, from the same out-of-
// band source the verifying keyring comes from.
func (kt KeyTimeline) AuthoritativeAt(firstKeyID, seq uint64) uint64 {
	current := firstKeyID
	for _, t := range kt.Transitions {
		if t.AtCommitSeq > seq {
			break
		}
		current = t.NewKeyID
	}
	return current
}

// VerifyChain checks that the recorded transitions form an unbroken chain from
// firstKeyID, and that each was signed by the key it replaces.
//
// Two failures are distinguished because they mean different things: a broken
// link means a transition is missing from the log — which after a compaction is
// expected rather than sinister — while a bad signature means a transition was
// fabricated.
func (kt KeyTimeline) VerifyChain(v store.Verifier, firstKeyID uint64) error {
	if v == nil {
		return errors.New("verify key timeline: no verifier")
	}
	expected := firstKeyID
	for i, t := range kt.Transitions {
		if t.PrevKeyID != expected {
			return fmt.Errorf("verify key timeline: transition %d replaces key %d, but key %d was authoritative — "+
				"a transition is missing from the log", i, t.PrevKeyID, expected)
		}
		// Signed by the OUTGOING key: only its holder could have authorised this.
		if err := v.Verify(t.PrevKeyID, keyTransitionSignedData(t), t.Signature); err != nil {
			return fmt.Errorf("verify key timeline: transition %d at commit %d was not authorised by key %d: %w",
				i, t.AtCommitSeq, t.PrevKeyID, err)
		}
		expected = t.NewKeyID
	}
	return nil
}
