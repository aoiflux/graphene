package index

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Scratch harness for docs/MEMORY_MODEL.md §1: what the property index costs,
// per structure, on the RSS fixture's declared shape.
//
// The method is differencing, not profiling. A heap profile attributes an
// allocation to whichever map grew last, and the index is spread over 16 shards
// × (byKey, ref1, refN, shared, perKey, keyIDs) plus five ordered indexes and
// two composites -- so the only figure a profile gives reliably is the total.
// Building the same shape with one group of keys at a time and subtracting gives
// the per-group cost instead.

var (
	modUnique  = []string{"digest", "uid0", "uid1", "uid2", "uid3", "uid4", "uid5", "uid6"}
	modOrdered = []string{"ord0", "ord1", "ord2", "ord3", "ord4"}
	modComp    = [][]string{{"ord0", "ord1"}, {"ord2", "ord3"}}
)

func modProps(i int) map[string][]byte {
	props := make(map[string][]byte, len(modUnique)+len(modOrdered))

	var digest [32]byte
	binary.BigEndian.PutUint64(digest[0:], uint64(i))
	binary.BigEndian.PutUint64(digest[8:], uint64(i)*0x9e3779b97f4a7c15)
	binary.BigEndian.PutUint64(digest[16:], ^uint64(i))
	binary.BigEndian.PutUint64(digest[24:], uint64(i)<<21|uint64(i)>>11)
	props["digest"] = digest[:]

	for k, key := range modUnique {
		if key == "digest" {
			continue
		}
		props[key] = []byte(fmt.Sprintf("%s-%09d", key, i*len(modUnique)+k))
	}
	for k, key := range modOrdered {
		props[key] = []byte(fmt.Sprintf("%06d", (i+k)%1000))
	}
	return props
}

// modBuild indexes n nodes over the named keys, declaring unique/ordered/
// composite exactly as the store does, and returns heap bytes it holds.
func modBuild(n int, unique, ordered []string, comps [][]string) (uint64, *PropertyIndex) {
	before := heapBytes()

	p := NewPropertyIndex()
	for _, k := range unique {
		if c := p.DeclareUniqueNodeKey(k, nil); len(c) > 0 {
			panic(c)
		}
	}
	for _, k := range ordered {
		p.DeclareOrderedNodeKey(k)
	}
	for _, c := range comps {
		if err := p.DeclareCompositeNodeKeys(c); err != nil {
			panic(err)
		}
	}

	uniq := make(map[string]bool, len(unique))
	for _, k := range unique {
		uniq[k] = true
	}
	want := append(append([]string{}, unique...), ordered...)

	for i := 0; i < n; i++ {
		props := modProps(i)
		id := store.NodeID(i + 1)
		for _, k := range want {
			v, ok := props[k]
			if !ok {
				continue
			}
			if uniq[k] {
				if err := p.IndexNodeUnique(id, k, v); err != nil {
					panic(err)
				}
				continue
			}
			p.IndexNode(id, k, v)
		}
	}

	after := heapBytes()
	return after - before, p
}

func TestScratchIndexModel(t *testing.T) {
	const n = 200_000

	arms := []struct {
		name    string
		unique  []string
		ordered []string
		comps   [][]string
		entries int
	}{
		{"digest only (32 B, all distinct, unique)", modUnique[:1], nil, nil, 1},
		{"uid0-uid6 only (14 B, all distinct, unique)", modUnique[1:], nil, nil, 7},
		{"ord0-ord4 only (6 B, 1000 distinct, ordered)", nil, modOrdered, nil, 5},
		{"ord0-ord4 + 2 composites", nil, modOrdered, modComp, 5},
		{"full fixture shape", modUnique, modOrdered, modComp, 13},
	}

	fmt.Printf("\nn = %d nodes\n", n)
	fmt.Printf("%-46s %10s %8s %10s\n", "arm", "MiB", "entries", "B/entry")
	for _, a := range arms {
		got, p := modBuild(n, a.unique, a.ordered, a.comps)
		e := n * a.entries
		fmt.Printf("%-46s %10.2f %8d %10.2f\n",
			a.name, float64(got)/(1<<20), e, float64(got)/float64(e))
		_ = p
	}
}

// TestScratchShardOccupancy reports how the fixture's 13 keys land on the 16
// shards, which is what decides the ref1/refN split: a node with two keys in one
// shard leaves ref1 for refN, and refN pays a slice header and a backing array
// per node on top of the map entry.
func TestScratchShardOccupancy(t *testing.T) {
	p := NewPropertyIndex()
	keys := append(append([]string{}, modUnique...), modOrdered...)

	perShard := make([][]string, propertyShards)
	for _, k := range keys {
		i := p.shardIndexFor(k)
		perShard[i] = append(perShard[i], k)
	}

	ref1, refN, occupied := 0, 0, 0
	for i, ks := range perShard {
		if len(ks) == 0 {
			continue
		}
		occupied++
		if len(ks) == 1 {
			ref1++
		} else {
			refN++
		}
		fmt.Printf("shard %2d: %d keys %v\n", i, len(ks), ks)
	}
	fmt.Printf("\n%d/%d shards occupied; per node: %d ref1 entries, %d refN entries covering %d keys\n",
		occupied, propertyShards, ref1, refN, len(keys)-ref1)
}
