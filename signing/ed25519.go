// Package signing provides an Ed25519 implementation of store.Signer and
// store.Verifier, plus a keyring for verifying against several keys.
//
// It is a convenience, not a requirement. The engine takes the interfaces, so a
// caller whose keys live in an HSM, a cloud KMS, or an OS keystore implements
// them directly and never imports this package — which is the point of the
// interfaces being narrow.
//
// # Ed25519, and the zero-dependency constraint
//
// `crypto/ed25519` is in the standard library, so signing costs the engine no
// dependency. That is not incidental: the plan's §10.6 records that BLS
// aggregate signatures would need a third-party module and therefore break the
// engine's most distinctive non-functional property, while Ed25519 delivers the
// whole signing programme from stdlib.
//
// Ed25519 is also deterministic — the same key over the same message always
// produces the same signature. That makes golden-vector tests possible, which
// ECDSA without RFC 6979 does not.
package signing

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownKey is returned when a verifier is asked about a key it does not
// hold. Distinct from a verification failure on purpose: "I cannot check this"
// and "this is forged" call for different responses, and a caller that collapses
// them will eventually treat one as the other.
var ErrUnknownKey = errors.New("signing: no public key for this key ID")

// ErrBadSignature is returned when a signature does not verify.
var ErrBadSignature = errors.New("signing: signature does not verify")

// Key is an Ed25519 signing key with the identifier written into each commit.
type Key struct {
	id   uint64
	priv ed25519.PrivateKey
}

// NewKey wraps a private key under the given identifier.
//
// The identifier is the caller's to choose and its meaning is the caller's to
// define; the engine records it so a verifier can select the right public key
// and so a rotation is reconstructible from the log. Zero is rejected because
// it is what an absent key ID looks like, and a key that cannot be told apart
// from "none" defeats the purpose of recording one.
func NewKey(id uint64, priv ed25519.PrivateKey) (*Key, error) {
	if id == 0 {
		return nil, errors.New("signing: key ID 0 is reserved for 'no key'")
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("signing: private key is %d bytes, want %d", len(priv), ed25519.PrivateKeySize)
	}
	return &Key{id: id, priv: priv}, nil
}

// GenerateKey creates a new key. Convenient for tests and for a caller with no
// existing key material; production key generation usually belongs elsewhere.
func GenerateKey(id uint64) (*Key, ed25519.PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	k, err := NewKey(id, priv)
	if err != nil {
		return nil, nil, err
	}
	return k, pub, nil
}

// KeyID implements store.Signer.
func (k *Key) KeyID() uint64 { return k.id }

// Sign implements store.Signer.
func (k *Key) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(k.priv, data), nil
}

// Public returns the verifying half of the key.
func (k *Key) Public() ed25519.PublicKey {
	return k.priv.Public().(ed25519.PublicKey)
}

// Keyring verifies signatures against a set of public keys, indexed by key ID.
//
// A set rather than a single key because evidence outlives keys. A store signed
// under one key and then rotated to another has commits under both, and both
// must stay verifiable — a rotation that retroactively invalidated every earlier
// signature would be worse than not rotating, since it turns genuine evidence
// into apparent tampering.
type Keyring struct {
	mu   sync.RWMutex
	keys map[uint64]ed25519.PublicKey
}

// NewKeyring returns an empty keyring.
func NewKeyring() *Keyring {
	return &Keyring{keys: make(map[uint64]ed25519.PublicKey)}
}

// Add registers a public key under an identifier.
func (r *Keyring) Add(id uint64, pub ed25519.PublicKey) error {
	if id == 0 {
		return errors.New("signing: key ID 0 is reserved for 'no key'")
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("signing: public key is %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[id] = pub
	return nil
}

// Verify implements store.Verifier.
func (r *Keyring) Verify(keyID uint64, data, sig []byte) error {
	r.mu.RLock()
	pub, ok := r.keys[keyID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownKey, keyID)
	}
	if !ed25519.Verify(pub, data, sig) {
		return fmt.Errorf("%w under key %d", ErrBadSignature, keyID)
	}
	return nil
}

// Len returns how many keys the ring holds.
func (r *Keyring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.keys)
}
