package merkle

import (
	"testing"
)

// The whole value of RootBuilder is that it produces the same number Root does.
// Anything less makes it a second, incompatible tree with the same name.
//
// The sweep runs to 1025 rather than to a round number on purpose: the
// construction is decided by the binary representation of the leaf count, so the
// sizes that can go wrong are the ones on either side of a power of two. 1024
// merges all the way down to a single subtree and 1025 starts a new one beside
// it, which is the transition a naive pairing tree gets wrong.
func TestRootBuilder_EqualsRoot(t *testing.T) {
	l := leaves(1025)
	for n := 0; n <= 1025; n++ {
		var b RootBuilder
		for i := 0; i < n; i++ {
			b.Add(l[i])
		}
		if got, want := b.Root(), Root(l[:n]); got != want {
			t.Fatalf("n=%d: RootBuilder = %x, Root = %x", n, got, want)
		}
		if b.Len() != uint64(n) {
			t.Fatalf("n=%d: Len = %d", n, b.Len())
		}
	}
}

// Memory is the reason this exists, so the bound is asserted rather than
// described: one retained hash per set bit of the leaf count.
func TestRootBuilder_RetainsOneHashPerSetBit(t *testing.T) {
	l := leaves(1024)
	var b RootBuilder
	for n := 1; n <= 1024; n++ {
		b.Add(l[n-1])
		want := popcount(uint64(n))
		if len(b.stack) != want {
			t.Fatalf("after %d leaves the stack holds %d hashes, want %d (one per set bit)",
				n, len(b.stack), want)
		}
	}
}

func popcount(v uint64) int {
	n := 0
	for ; v != 0; v &= v - 1 {
		n++
	}
	return n
}

// Root is a read, not a close: a caller that reports a root and keeps streaming
// must get the same answer twice and a correct answer afterwards.
func TestRootBuilder_RootDoesNotConsume(t *testing.T) {
	l := leaves(7)
	var b RootBuilder
	for i := range 5 {
		b.Add(l[i])
	}
	first, second := b.Root(), b.Root()
	if first != second {
		t.Fatal("Root gave two different answers for the same five leaves")
	}
	if first != Root(l[:5]) {
		t.Fatal("Root disagreed with Root(leaves[:5])")
	}
	b.Add(l[5])
	b.Add(l[6])
	if got, want := b.Root(), Root(l); got != want {
		t.Fatalf("after resuming: %x, want %x", got, want)
	}
}

// AddLeafData is Add(HashLeaf(data)) with a reused hasher, so it has to agree
// with the pair it replaces for every shape of input — including empty data,
// which is a leaf like any other and not an absent one.
func TestRootBuilder_AddLeafDataMatchesHashLeaf(t *testing.T) {
	datas := [][]byte{nil, {}, []byte("a"), []byte("leaf"), bytesRepeat('z', 1000)}
	for n := 1; n <= len(datas); n++ {
		var streamed, hashed RootBuilder
		for _, d := range datas[:n] {
			streamed.AddLeafData(d)
			hashed.Add(HashLeaf(d))
		}
		if streamed.Root() != hashed.Root() {
			t.Fatalf("n=%d: AddLeafData and Add(HashLeaf(...)) disagree", n)
		}
	}

	// And the two hashers must not be entangled: the internal-node state is the
	// same sha256.Digest the leaves use, so a leaf added after a merge has to
	// still hash as a leaf.
	var b RootBuilder
	for i := range 100 {
		b.AddLeafData([]byte{byte(i)})
	}
	want := make([]Hash, 100)
	for i := range want {
		want[i] = HashLeaf([]byte{byte(i)})
	}
	if b.Root() != Root(want) {
		t.Fatal("interleaving leaf and internal hashing changed the root")
	}
}

func bytesRepeat(c byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = c
	}
	return out
}

// Reset must leave a builder indistinguishable from a fresh one, since a caller
// computing several roots in sequence reuses one.
func TestRootBuilder_Reset(t *testing.T) {
	l := leaves(9)
	var b RootBuilder
	for _, h := range l {
		b.Add(h)
	}
	b.Reset()
	if b.Root() != EmptyRoot() {
		t.Fatal("a reset builder is not the empty tree")
	}
	for i := range 3 {
		b.Add(l[i])
	}
	if got, want := b.Root(), Root(l[:3]); got != want {
		t.Fatalf("after Reset: %x, want %x", got, want)
	}
}

// The same sweep with sizes the fuzzer picks, since the sizes that break this
// construction are a property of the count's bit pattern and not of the leaves.
//
//	go test ./merkle/ -run=XXX -fuzz=FuzzRootBuilderMatchesRoot -fuzztime=60s
func FuzzRootBuilderMatchesRoot(f *testing.F) {
	f.Add(uint16(0))
	f.Add(uint16(1))
	f.Add(uint16(2))
	f.Add(uint16(255))
	f.Add(uint16(256))
	f.Add(uint16(257))

	f.Fuzz(func(t *testing.T, rawSize uint16) {
		n := int(rawSize % 2048)
		l := leaves(n)

		var b RootBuilder
		for _, h := range l {
			b.Add(h)
		}
		if got, want := b.Root(), Root(l); got != want {
			t.Fatalf("n=%d: RootBuilder = %x, Root = %x", n, got, want)
		}
	})
}
