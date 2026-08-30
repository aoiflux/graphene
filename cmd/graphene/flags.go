package main

// Flag types and the shared helpers the commands lean on.

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// verifierFromFlag builds a keyring from repeated `-pubkey ID:HEX` values.
//
// The ledger dumps otherwise report "signatures unchecked", which is honest and
// useless to the one reader who most needs the check — an auditor holding the
// public key and not the store. Taking keys on the command line closes that
// without the tool ever touching private material.
func verifierFromFlag(specs []string) (store.Verifier, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	ring := signing.NewKeyring()
	for _, spec := range specs {
		id, hexKey, ok := strings.Cut(spec, ":")
		if !ok {
			return nil, fmt.Errorf("-pubkey wants ID:HEX, got %q", spec)
		}
		keyID, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("-pubkey %q: %w", spec, err)
		}
		raw, err := hex.DecodeString(hexKey)
		if err != nil {
			return nil, fmt.Errorf("-pubkey %q: %w", spec, err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("-pubkey %q: an Ed25519 public key is %d hex bytes, got %d",
				spec, ed25519.PublicKeySize, len(raw))
		}
		if err := ring.Add(keyID, ed25519.PublicKey(raw)); err != nil {
			return nil, err
		}
	}
	return ring, nil
}

// pubkeyList collects repeated -pubkey flags.
type pubkeyList []string

func (p *pubkeyList) String() string { return strings.Join(*p, ",") }
func (p *pubkeyList) Set(v string) error {
	*p = append(*p, v)
	return nil
}

// signatureNote says what was actually checked, so "intact" is never read as
// more than it is.
func signatureNote(v store.Verifier) string {
	if v == nil {
		return "signatures unchecked: no -pubkey supplied"
	}
	return "signatures verified against the supplied keys"
}

// sortedActors gives the capability listing a stable order, so two runs against
// the same store produce the same output and a diff means something.
func sortedActors(m map[uint64]disk.Capability) []uint64 {
	out := make([]uint64, 0, len(m))
	for a := range m {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
