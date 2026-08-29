//go:build stress

package graphene_test

// The read paths the live-reader work touched, measured against the control the
// benchmark method asks for.
//
// Two changes are in scope here. Store.NodesByProperty now takes the store lock
// *before* it reads the postings rather than after, so that the index and the
// records it is filtered against come from one pinned view -- which means a miss
// pays for the lock, where it used to skip it. And every index read on a query
// path goes through the view rather than through a field on the store, which is
// one pointer dereference per call.

import (
	"fmt"
	"testing"
)

// The hit: postings found, then resolved against the records.
func BenchmarkNodesByProperty_Equal_Disk(b *testing.B) {
	f := diskGraph()
	val := []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByProperty("sha256", val); err != nil {
			b.Fatal(err)
		}
	}
}

// The miss: nothing found, and the one case whose cost changed. This used to
// return before taking the store lock.
func BenchmarkNodesByProperty_Miss_Disk(b *testing.B) {
	f := diskGraph()
	val := []byte("hash-no-such-value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByProperty("sha256", val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNodesByProperty_Miss_Memory(b *testing.B) {
	f := memGraph()
	val := []byte("hash-no-such-value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByProperty("sha256", val); err != nil {
			b.Fatal(err)
		}
	}
}
