package graphene_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// CSR format v6 carries the property index inside the file. These tests cover
// the round trip, the fallback for files written before v6, and what happens
// when the index section is damaged.

const (
	csrFileName   = "graphene.csr"
	walFileName   = "graphene.wal"
	csrVersionOff = 4 // byte offset of the uint16 version field
)

func buildPersistenceFixture(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{benchLabelFor(i)}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%05d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%02d", i%10)),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 0; i < n-1; i++ {
		eid, err := g.AddEdge(&store.Edge{
			Src:    ids[i],
			Dst:    ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if i%3 == 0 {
			if err := g.IndexEdgeProperty(eid, "algo", []byte(fmt.Sprintf("algo-%d", i%4))); err != nil {
				t.Fatalf("IndexEdgeProperty: %v", err)
			}
		}
	}
	return ids
}

func benchLabelFor(i int) store.NodeType {
	switch {
	case i%25 == 0:
		return store.NodeTypeCase
	case i%5 == 0:
		return store.NodeTypeEvidenceFile
	default:
		return store.NodeTypeMicroArtefact
	}
}

// assertPropertyIndexIntact checks a representative slice of the fixture's
// indexed values resolve to the right entities.
func assertPropertyIndexIntact(t *testing.T, g *graphene.Graph, ids []store.NodeID) {
	t.Helper()
	for i := 0; i < len(ids); i += 7 {
		want := ids[i]
		hits, err := g.NodesByProperty("sha256", []byte(fmt.Sprintf("hash-%05d", i)))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(hits) != 1 || hits[0] != want {
			t.Fatalf("sha256 hash-%05d = %v, want [%d]", i, hits, want)
		}
	}
	hits, err := g.NodesByProperty("bucket", []byte("bucket-03"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(hits) != len(ids)/10 {
		t.Fatalf("bucket-03 returned %d nodes, want %d", len(hits), len(ids)/10)
	}
}

func csrVersion(t *testing.T, dir string) uint16 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("read CSR: %v", err)
	}
	if len(data) < csrVersionOff+2 {
		t.Fatalf("CSR file too short: %d bytes", len(data))
	}
	return binary.LittleEndian.Uint16(data[csrVersionOff : csrVersionOff+2])
}

// After Compact the property index lives in the CSR, so the WAL is left empty
// and a reopen serves the index straight from the file.
func TestCSRv6_PropertyIndexSurvivesWithoutWAL(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ids := buildPersistenceFixture(t, g, 200)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if v := csrVersion(t, dir); v != 6 {
		t.Fatalf("CSR version = %d, want 6", v)
	}

	// Compaction used to re-emit every property entry into the fresh WAL. It
	// should now be empty, which is what makes restart cost independent of index
	// size.
	info, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL is %d bytes after compaction, want 0 (index should live in the CSR)", info.Size())
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	assertPropertyIndexIntact(t, reopened, ids)
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// Deleting the WAL entirely must not lose the index once it has been compacted
// into the CSR — the strongest form of "the CSR is now the source of truth".
func TestCSRv6_IndexSurvivesWALDeletion(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ids := buildPersistenceFixture(t, g, 120)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.Remove(filepath.Join(dir, walFileName)); err != nil {
		t.Fatalf("remove WAL: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen without WAL: %v", err)
	}
	defer reopened.Close()

	assertPropertyIndexIntact(t, reopened, ids)
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// Entries registered after a compaction live only in the WAL until the next one,
// so both sources have to merge correctly.
func TestCSRv6_MergesPersistedIndexWithPostCompactionWAL(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ids := buildPersistenceFixture(t, g, 80)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Post-compaction work: a new node, a new entry on an existing node, and a
	// deletion that must purge entries the CSR still lists.
	fresh, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperty(fresh, "sha256", []byte("post-compaction")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := g.IndexNodeProperty(ids[0], "tool", []byte("strings")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	victim := ids[len(ids)-1]
	if err := g.DeleteNode(victim); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if hits, _ := reopened.NodesByProperty("sha256", []byte("post-compaction")); len(hits) != 1 || hits[0] != fresh {
		t.Fatalf("post-compaction entry lost: %v", hits)
	}
	if hits, _ := reopened.NodesByProperty("tool", []byte("strings")); len(hits) != 1 || hits[0] != ids[0] {
		t.Fatalf("post-compaction entry on a CSR node lost: %v", hits)
	}
	if hits, _ := reopened.NodesByProperty("sha256", []byte(fmt.Sprintf("hash-%05d", len(ids)-1))); len(hits) != 0 {
		t.Fatalf("deleted node still indexed after reopen: %v", hits)
	}
	if hits, _ := reopened.NodesByProperty("sha256", []byte("hash-00000")); len(hits) != 1 || hits[0] != ids[0] {
		t.Fatalf("persisted entry lost: %v", hits)
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// A v5 file (no index section) must still open, with the WAL supplying the
// property index as it always did, and the next Compact upgrades it to v6.
func TestCSRv5_StillReadableAndUpgrades(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ids := buildPersistenceFixture(t, g, 100)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Rewrite the file in genuine v5 layout. v6 differs from v5 by exactly two
	// things: an 8-byte indexOffset appended to the header, and the trailing
	// index section. Dropping both reproduces byte-for-byte what the previous
	// release wrote — patching the version number alone would not, because the
	// header lengths differ and the reader would start parsing records 8 bytes
	// early.
	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read CSR: %v", err)
	}
	const (
		v5Header    = 38
		v6Header    = 46
		indexOffPos = 38
	)
	indexOffset := int(binary.LittleEndian.Uint64(data[indexOffPos : indexOffPos+8]))
	if indexOffset <= v6Header || indexOffset > len(data) {
		t.Fatalf("unexpected index offset %d in a %d-byte file", indexOffset, len(data))
	}
	v5 := make([]byte, 0, v5Header+(indexOffset-v6Header))
	v5 = append(v5, data[:v5Header]...)            // header without indexOffset
	v5 = append(v5, data[v6Header:indexOffset]...) // body without the index section
	binary.LittleEndian.PutUint16(v5[csrVersionOff:csrVersionOff+2], 5)
	if err := os.WriteFile(csrPath, v5, 0600); err != nil {
		t.Fatalf("write v5 CSR: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen v5 file: %v", err)
	}

	// Records survive: a v5 reader gets nodes, edges, labels and adjacency.
	count, err := reopened.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != uint64(len(ids)) {
		t.Fatalf("NodeCount = %d, want %d", count, len(ids))
	}
	byType, err := reopened.NodesByType(store.NodeTypeCase)
	if err != nil {
		t.Fatalf("NodesByType: %v", err)
	}
	if len(byType) == 0 {
		t.Fatal("label index empty after reading a v5 file")
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes on v5 file: %v", err)
	}

	// Compacting writes the current format.
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v := csrVersion(t, dir); v != 6 {
		t.Fatalf("CSR version after upgrade compact = %d, want 6", v)
	}
}

// A future version must be refused rather than misparsed.
func TestCSR_RejectsUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buildPersistenceFixture(t, g, 20)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read CSR: %v", err)
	}
	binary.LittleEndian.PutUint16(data[csrVersionOff:csrVersionOff+2], 99)
	if err := os.WriteFile(csrPath, data, 0600); err != nil {
		t.Fatalf("write CSR: %v", err)
	}

	if _, err := graphene.Open(dir); err == nil {
		t.Fatal("Open accepted a CSR with an unsupported version")
	}
}

// A damaged index section must surface as an error on open, not as silently
// missing index entries.
func TestCSRv6_CorruptIndexSectionIsDetected(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buildPersistenceFixture(t, g, 60)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	csrPath := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(csrPath)
	if err != nil {
		t.Fatalf("read CSR: %v", err)
	}
	// Point the index offset at nonsense; the magic check must catch it.
	binary.LittleEndian.PutUint64(data[38:46], uint64(len(data)-3))
	if err := os.WriteFile(csrPath, data, 0600); err != nil {
		t.Fatalf("write CSR: %v", err)
	}

	if _, err := graphene.Open(dir); err == nil {
		t.Fatal("Open accepted a CSR with a corrupt index section")
	}
}

// RebuildIndexes must be a no-op on a healthy store: same answers before and
// after, on both backends.
func TestRebuildIndexes_IsIdempotentOnHealthyStore(t *testing.T) {
	backends := map[string]func(t *testing.T) *graphene.Graph{
		"memory": func(t *testing.T) *graphene.Graph { return graphene.NewInMemory() },
		"disk": func(t *testing.T) *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { g.Close() })
			return g
		},
	}

	for name, open := range backends {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := buildPersistenceFixture(t, g, 100)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				propKeys := []string{"sha256", "bucket"}
				propValues := []string{"hash-00007", "bucket-03", "bucket-09"}
				before := takeIndexSnapshot(t, g, propKeys, propValues)
				before.normalise()

				if err := g.RebuildIndexes(); err != nil {
					t.Fatalf("RebuildIndexes: %v", err)
				}
				if err := g.VerifyIndexes(); err != nil {
					t.Fatalf("VerifyIndexes after rebuild: %v", err)
				}

				after := takeIndexSnapshot(t, g, propKeys, propValues)
				after.normalise()
				if !snapshotsEqual(before, after) {
					t.Fatalf("RebuildIndexes changed a healthy store\n before: %+v\n after:  %+v", before, after)
				}

				// Adjacency must survive too.
				deg, err := g.OutDegree(ids[0], nil)
				if err != nil {
					t.Fatalf("OutDegree: %v", err)
				}
				if deg != 1 {
					t.Fatalf("OutDegree after rebuild = %d, want 1", deg)
				}
			})
		}
	}
}

func snapshotsEqual(a, b *indexSnapshot) bool {
	if a.NodeCount != b.NodeCount || a.EdgeCount != b.EdgeCount {
		return false
	}
	if len(a.NodesByType) != len(b.NodesByType) || len(a.NodeProps) != len(b.NodeProps) {
		return false
	}
	for k, av := range a.NodesByType {
		bv := b.NodesByType[k]
		if len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
	}
	for k, av := range a.NodeProps {
		bv := b.NodeProps[k]
		if len(av) != len(bv) {
			return false
		}
		for i := range av {
			if av[i] != bv[i] {
				return false
			}
		}
	}
	return true
}
