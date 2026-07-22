package disk

import (
	"encoding/binary"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func layoutFixture(n int) *CSRGraph {
	nodes := make([]nodeRecord, 0, n)
	for i := 1; i <= n; i++ {
		nodes = append(nodes, nodeRecord{ID: store.NodeID(i), Labels: []store.NodeType{store.NodeTypeTag}})
	}
	edges := make([]rawEdge, 0, n-1)
	for i := 1; i < n; i++ {
		edges = append(edges, rawEdge{
			ID: store.EdgeID(i), Src: store.NodeID(i), Dst: store.NodeID(i + 1),
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	return Build(nodes, edges)
}

// The index section must begin immediately after the last edge record.
//
// Through v6 the adjacency arrays sat in between — written on every Compact and
// never read back, because deserialiseCSR rebuilds them with Build() and then
// jumps to indexOffset. This pins that the gap is gone and does not creep back.
func TestCSRLayout_NoGapBeforeIndexSection(t *testing.T) {
	const n = 200
	data := layoutFixture(n).SerialiseWithIndex(nil, nil)

	indexOffset := int(binary.LittleEndian.Uint64(data[38:46]))

	// node: id(8) + labelCount(1) + label(2) + propLen(4)
	// edge: ids(24) + labelCount(1) + label(2) + weight(4) + propLen(4)
	wantRecords := n*(8+1+2+4) + (n-1)*(24+1+2+4+4)
	if got := indexOffset - csrV6HeaderSize; got != wantRecords {
		t.Fatalf("gap between records and index section: body is %d bytes, records need %d (%d bytes unaccounted)",
			got, wantRecords, got-wantRecords)
	}
}

// A file whose last record ends exactly at the index section must parse.
//
// The bounds checks used to demand 4 bytes more than a v3+ record needs — a
// constant carried over from v2, where the property field was 8 bytes rather
// than 4. It never fired while the never-read adjacency arrays trailed every
// record and supplied slack. Removing them exposed it: a perfectly valid file
// was rejected with "truncated edge labels".
func TestCSRLayout_LastRecordEndingAtSectionBoundaryParses(t *testing.T) {
	for _, n := range []int{2, 3, 17, 200} {
		data := layoutFixture(n).SerialiseWithIndex(nil, nil)
		csr, _, err := deserialiseCSR(data)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got := len(csr.nodes) - 1; got != n {
			t.Fatalf("n=%d: parsed %d nodes", n, got)
		}
	}
}
