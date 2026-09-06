package disk

// The catalogue's encoder, decoder and fuzz target.
//
// These are pure: no store, no directory, no lock. The behaviour that needs a
// store — that a declaration survives a reopen and is still enforced — lives in
// tests/graphene_schema_test.go, which is where the black-box suite is.
//
// A parser gets a fuzz target because §16 already names the two hand-rolled
// binary readers that do not have one as the first debt to pay. This is a new
// reader over attacker-controllable input; it starts with one.

import (
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func fullCatalogue() Catalogue {
	return Catalogue{
		OrderedNodeKeys:   []string{"score", "seen"},
		OrderedEdgeKeys:   []string{"confidence"},
		UniqueNodeKeys:    []string{"sha256"},
		UniqueEdgeKeys:    []string{"edgekey"},
		UniqueEdgeTypes:   []store.EdgeType{7, 32768},
		CompositeNodeKeys: [][]string{{"case", "bucket"}},
		CompositeEdgeKeys: [][]string{{"kind", "run"}},
	}
}

func TestCatalogue_RoundTrip(t *testing.T) {
	want := fullCatalogue()

	got, err := decodeCatalogue(want.encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got.encode()) != string(want.encode()) {
		t.Fatalf("round trip changed the catalogue:\n got %s\nwant %s", got.encode(), want.encode())
	}
}

// The file is a function of the declarations, not of the order they arrived in.
// Without that two stores holding the same schema differ on disk, and a backup
// diff reports a change that is not one.
func TestCatalogue_EncodeIsOrderIndependent(t *testing.T) {
	forward := fullCatalogue()

	reversed := Catalogue{
		OrderedNodeKeys:   []string{"seen", "score"},
		OrderedEdgeKeys:   []string{"confidence"},
		UniqueNodeKeys:    []string{"sha256"},
		UniqueEdgeKeys:    []string{"edgekey"},
		UniqueEdgeTypes:   []store.EdgeType{32768, 7},
		CompositeNodeKeys: [][]string{{"case", "bucket"}},
		CompositeEdgeKeys: [][]string{{"kind", "run"}},
	}

	if string(forward.encode()) != string(reversed.encode()) {
		t.Fatalf("declaration order changed the bytes:\n%s\n---\n%s",
			forward.encode(), reversed.encode())
	}
}

// A composite tuple's key order *is* part of its identity, so unlike the sets
// above it must survive — and (a,b) and (b,a) are two declarations, not one.
func TestCatalogue_CompositeKeyOrderIsPreserved(t *testing.T) {
	c := Catalogue{CompositeNodeKeys: [][]string{{"b", "a"}, {"a", "b"}}}

	got, err := decodeCatalogue(c.encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.CompositeNodeKeys) != 2 {
		t.Fatalf("got %d composite tuples, want 2: %v", len(got.CompositeNodeKeys), got.CompositeNodeKeys)
	}
	var seenAB, seenBA bool
	for _, tup := range got.CompositeNodeKeys {
		switch strings.Join(tup, ",") {
		case "a,b":
			seenAB = true
		case "b,a":
			seenBA = true
		default:
			t.Errorf("unexpected tuple %v", tup)
		}
	}
	if !seenAB || !seenBA {
		t.Errorf("(a,b) and (b,a) did not both survive: %v", got.CompositeNodeKeys)
	}
}

// A property key is arbitrary caller bytes. An unescaped tab would come back as
// two fields and silently become a declaration on a key nobody named.
func TestCatalogue_EscapesRoundTrip(t *testing.T) {
	keys := []string{"a\tb", "a\nb", "a\rb", "back\\slash", "trailing\\", "\\t-literal"}

	c := Catalogue{OrderedNodeKeys: keys}
	got, err := decodeCatalogue(c.encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range keys {
		found := false
		for _, g := range got.OrderedNodeKeys {
			if g == k {
				found = true
			}
		}
		if !found {
			t.Errorf("key %q did not survive; got %q", k, got.OrderedNodeKeys)
		}
	}
}

func TestCatalogue_EmptyWritesNothing(t *testing.T) {
	var c Catalogue
	if !c.Empty() {
		t.Fatal("the zero Catalogue reports itself non-empty")
	}
}

// The criticality rule, both halves. It is the whole reason the constraint
// kinds carry a "!" and the optimisations do not.
func TestCatalogue_UnknownKinds(t *testing.T) {
	optional := schemaHeader + "\nordered-node\tscore\nsome-future-thing\ta\tb\n"
	got, err := decodeCatalogue([]byte(optional))
	if err != nil {
		t.Fatalf("an unknown optional kind was refused: %v", err)
	}
	if len(got.OrderedNodeKeys) != 1 || got.OrderedNodeKeys[0] != "score" {
		t.Errorf("the understood line did not survive: %v", got.OrderedNodeKeys)
	}

	critical := schemaHeader + "\n!some-future-constraint\tk\n"
	if _, err := decodeCatalogue([]byte(critical)); err == nil {
		t.Fatal("an unknown critical kind was accepted; the constraint would be dropped silently")
	}
}

func TestCatalogue_Malformed(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"empty", "", "file is empty"},
		{"bad header", "graphene-schema v99\n", "unrecognised header"},
		{"no fields", schemaHeader + "\nordered-node\n", "at least one field"},
		{"empty key", schemaHeader + "\nordered-node\t\n", "empty key"},
		{"two fields for one", schemaHeader + "\nordered-node\ta\tb\n", "wants one field"},
		{"short composite", schemaHeader + "\ncomposite-node\tonly\n", "at least two keys"},
		{"bad type value", schemaHeader + "\n!unique-edge-type\tnope\n", "nope"},
		{"type out of range", schemaHeader + "\n!unique-edge-type\t70000\n", "70000"},
		{"dangling escape", schemaHeader + "\nordered-node\tk\\\n", "dangling escape"},
		{"unknown escape", schemaHeader + "\nordered-node\tk\\q\n", "unknown escape"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCatalogue([]byte(tc.body))
			if err == nil {
				t.Fatalf("decode accepted %q", tc.body)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// FuzzDecodeCatalogue explores the catalogue parser.
//
// Two properties. It must not panic on any input — it reads a file that other
// software writes into a store directory, so it is as attacker-reachable as the
// image. And whatever it accepts must re-encode to something it accepts again
// with the same meaning: a catalogue that decoded one way and re-encoded another
// would drift a little on every declaration, which is the failure a round-trip
// property catches and an example-based test does not.
func FuzzDecodeCatalogue(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte(schemaHeader + "\n"))
	f.Add(fullCatalogue().encode())
	f.Add([]byte(schemaHeader + "\n!unique-node\tk\n"))
	f.Add([]byte(schemaHeader + "\ncomposite-node\ta\tb\tc\n"))
	f.Add([]byte(schemaHeader + "\nordered-node\ta\\tb\n"))
	f.Add([]byte(schemaHeader + "\n!unique-edge-type\t65535\n"))
	f.Add([]byte(schemaHeader + "\nunknown-optional\tx\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := decodeCatalogue(data)
		if err != nil {
			return
		}
		again, err := decodeCatalogue(c.encode())
		if err != nil {
			t.Fatalf("a catalogue this parser produced was refused on the way back in: %v", err)
		}
		if string(again.encode()) != string(c.encode()) {
			t.Fatalf("re-encoding changed the catalogue:\n first %q\nsecond %q", c.encode(), again.encode())
		}
	})
}
