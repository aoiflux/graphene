package graphene_test

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// What the integrity machinery costs.
//
// API_REFERENCE.md §22.2 tells readers to accept two costs — an Ed25519
// signature per batch commit, and a Merkle pass over the image at each
// compaction — and asserts the first "amortises to nothing" with good batching.
// Those were assertions. These measure them.
//
// # Method
//
// Every pair below is an **A/B between two configurations inside one binary**:
// same build, same fixture, same machine, differing only in the Options that
// enable the machinery. That is a materially easier comparison than the
// cross-commit one CONTRIBUTING.md describes — there is no build drift to
// control for, so no control worktree is needed. What still applies is that a
// single run proves nothing: use -count and compare distributions, and treat
// gaps under ~25% as unresolved.
//
// Run:
//
//	go test -run '^$' -bench 'Forensic' -benchmem -count=6 .

// benchSigner builds a signing key and the ring that verifies it.
func benchSigner(b *testing.B) (store.Signer, store.Verifier) {
	b.Helper()
	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		b.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		b.Fatal(err)
	}
	return key, ring
}

// benchOpen opens a store either plain or fully provisioned.
func benchOpen(b *testing.B, dir string, strict bool) *disk.Store {
	b.Helper()
	opts := disk.Options{}
	if strict {
		key, ring := benchSigner(b)
		opts = disk.StrictOptions(key, ring, 42)
		opts.Retention = disk.RetentionPolicy{MaxSegments: 50}
		opts.Redaction = true
	}
	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// benchOps builds a transaction's worth of node additions with IDs reserved up
// front, and returns the IDs so a caller can address one afterwards.
//
// A transaction does not write assigned IDs back into the caller's records, so
// reserving is how the IDs are known — and it is what the store's own tests do.
func benchOps(s *disk.Store, n int) ([]store.TxOp, []store.NodeID) {
	ops := make([]store.TxOp, n)
	ids := make([]store.NodeID, n)
	for i := range ops {
		ids[i] = s.ReserveNodeID()
		ops[i] = store.TxOp{Kind: store.TxOpAddNode, Node: &store.Node{
			ID:         ids[i],
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("sha256=%064x", i)),
		}}
	}
	return ops, ids
}

// --- the signing claim: "batch well and it amortises to nothing" ---

// BenchmarkForensic_CommitSigned measures one batch commit at a range of sizes,
// with and without signing. The per-node cost is what the claim is about: a
// signature is per *commit*, so it should shrink as the batch grows.
func BenchmarkForensic_CommitSigned(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000} {
		for _, strict := range []bool{false, true} {
			name := fmt.Sprintf("n=%d/%s", size, map[bool]string{false: "plain", true: "signed"}[strict])
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					dir := b.TempDir()
					s := benchOpen(b, dir, strict)
					ops, _ := benchOps(s, size)
					b.StartTimer()

					if err := s.ApplyTransactionAs(ops, store.TxContext{ActorID: 7}); err != nil {
						b.Fatal(err)
					}

					b.StopTimer()
					s.Close()
					b.StartTimer()
				}
			})
		}
	}
}

// --- what a compaction actually costs under strict options ---

// BenchmarkForensic_Compact attributes the difference rather than just measuring
// it, because the obvious reading is wrong.
//
// **Every compaction computes the Merkle roots** — `WithSnapshotRoots` is
// unconditional in compact.go — so the Merkle pass is not what strict options
// add. Three configurations separate what is:
//
//	plain     no signer, no audit, no retention  — still computes roots
//	attested  + signer, so the image is attested — one Ed25519 signature
//	full      + audit and retention              — extra durable writes
//
// Measuring only plain-vs-full would attribute the whole gap to hashing, which
// is the mistake the first version of this benchmark made and the docs repeated.
func BenchmarkForensic_Compact(b *testing.B) {
	configs := []struct {
		name  string
		build func(b *testing.B) disk.Options
	}{
		{"plain", func(*testing.B) disk.Options { return disk.Options{} }},
		{"attested", func(b *testing.B) disk.Options {
			key, ring := benchSigner(b)
			return disk.Options{Signer: key, Verifier: ring, AttestActorID: 42}
		}},
		{"full", func(b *testing.B) disk.Options {
			key, ring := benchSigner(b)
			o := disk.StrictOptions(key, ring, 42)
			o.Retention = disk.RetentionPolicy{MaxSegments: 50}
			return o
		}},
	}

	for _, size := range []int{1000, 10000} {
		for _, cfg := range configs {
			b.Run(fmt.Sprintf("n=%d/%s", size, cfg.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					dir := b.TempDir()
					s, oerr := disk.OpenWithOptions(dir, cfg.build(b))
					if oerr != nil {
						b.Fatal(oerr)
					}
					ops, _ := benchOps(s, size)
					if err := s.ApplyTransactionAs(ops, store.TxContext{ActorID: 7}); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()

					if err := s.Compact(); err != nil {
						b.Fatal(err)
					}

					b.StopTimer()
					s.Close()
					b.StartTimer()
				}
			})
		}
	}
}

// --- the open-time claim: "it costs a hash of the image per open" ---

// BenchmarkForensic_Open measures a cold open of a compacted store with and
// without VerifyOnOpen. §22.2 argues the cost is the right trade where opening
// is rare relative to querying; this says what is being traded.
func BenchmarkForensic_Open(b *testing.B) {
	const size = 10000
	for _, verify := range []bool{false, true} {
		name := map[bool]string{false: "plain", true: "verified"}[verify]
		b.Run(name, func(b *testing.B) {
			// One fixture, opened repeatedly — the store is not mutated, so it can
			// be built once outside the timed loop.
			dir := b.TempDir()
			key, ring := benchSigner(b)
			build := disk.StrictOptions(key, ring, 42)
			s, err := disk.OpenWithOptions(dir, build)
			if err != nil {
				b.Fatal(err)
			}
			ops, _ := benchOps(s, size)
			if err := s.ApplyTransactionAs(ops, store.TxContext{ActorID: 7}); err != nil {
				b.Fatal(err)
			}
			if err := s.Compact(); err != nil {
				b.Fatal(err)
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}

			opts := disk.Options{Verifier: ring, VerifyOnOpen: verify}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reopened, oerr := disk.OpenWithOptions(dir, opts)
				if oerr != nil {
					b.Fatal(oerr)
				}
				b.StopTimer()
				reopened.Close()
				b.StartTimer()
			}
		})
	}
}

// --- what a proof costs to make and to check ---

// BenchmarkForensic_Proof measures building an inclusion proof, exporting it,
// and verifying it. Verification is what a recipient pays and is the number
// worth knowing: it is the only one paid by someone who did not choose to use
// this engine.
func BenchmarkForensic_Proof(b *testing.B) {
	const size = 10000
	dir := b.TempDir()
	key, ring := benchSigner(b)
	s, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, 42))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	ops, ids := benchOps(s, size)
	if err := s.ApplyTransactionAs(ops, store.TxContext{ActorID: 7}); err != nil {
		b.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		b.Fatal(err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		b.Fatal(err)
	}
	target := ids[size/2]
	if !s.NodeExists(target) {
		b.Fatalf("fixture node %d is not in the store", target)
	}

	b.Run("build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := s.ProveNode(target); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("export", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := s.ExportNodeProof(target); err != nil {
				b.Fatal(err)
			}
		}
	})

	blob, err := s.ExportNodeProof(target)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("verify", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p, uerr := disk.UnmarshalProof(blob)
			if uerr != nil {
				b.Fatal(uerr)
			}
			if err := disk.VerifyExportedProof(roots.Snapshot, p); err != nil {
				b.Fatal(err)
			}
		}
	})
}
