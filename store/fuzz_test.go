package store

import (
	"strings"
	"testing"
)

// Fuzz targets for the type-selector parsers.
//
// ParseNodeType and ParseEdgeType accept strings that reach the engine from
// config files, CLI flags, and query payloads — none of which the engine wrote.
// They accept several spellings of a custom type ("custom:7", "custom(7)",
// "custom-7") plus bare numerics, which is exactly the kind of hand-rolled
// grammar that hides an index panic on a malformed input.
//
// Run:
//
//	go test ./store/ -run=XXX -fuzz=FuzzParseNodeType -fuzztime=30s

func FuzzParseNodeType(f *testing.F) {
	for _, s := range []string{
		"MicroArtefact", "EvidenceFile", "Tag",
		"custom:7", "custom(7)", "custom-7", "32768", "0",
		"custom:", "custom(", "custom()", "custom(-1)", "custom:999999999999999999999",
		"", " ", "custom:0x10", "CUSTOM:7", "custom:7)", "((((",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, selector string) {
		nt, err := ParseNodeType(selector)
		if err != nil {
			return
		}
		// A selector that parsed must round-trip: String() has to yield something
		// ParseNodeType accepts again and that resolves to the same value.
		// Otherwise a type can be named in a way the engine cannot read back.
		round, err := ParseNodeType(nt.String())
		if err != nil {
			t.Fatalf("ParseNodeType(%q) = %d, but its String() %q does not parse: %v",
				selector, nt, nt.String(), err)
		}
		if round != nt {
			t.Fatalf("round-trip changed the value: %q -> %d -> %q -> %d",
				selector, nt, nt.String(), round)
		}
	})
}

func FuzzParseEdgeType(f *testing.F) {
	for _, s := range []string{
		"Contains", "SimilarTo", "Temporal",
		"custom:7", "custom(7)", "custom-7", "32768", "0",
		"custom:", "custom(", "custom()", "custom(-1)", "custom:999999999999999999999",
		"", " ", "custom:0x10", "CUSTOM:7", "custom:7)", "((((",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, selector string) {
		et, err := ParseEdgeType(selector)
		if err != nil {
			return
		}
		round, err := ParseEdgeType(et.String())
		if err != nil {
			t.Fatalf("ParseEdgeType(%q) = %d, but its String() %q does not parse: %v",
				selector, et, et.String(), err)
		}
		if round != et {
			t.Fatalf("round-trip changed the value: %q -> %d -> %q -> %d",
				selector, et, et.String(), round)
		}
	})
}

// A malformed selector must produce an error, never a panic, and the error must
// say which parser rejected it — an operator debugging a config typo has only
// the message to go on, and "invalid syntax" alone does not say whether a node
// or an edge selector was at fault.
//
// It deliberately does not require the whole input to be echoed back: the
// parser names the offending fragment instead ("custom offset out of range
// [0,32767]: -1"), which is the more useful half of a long selector.
func TestParseType_RejectionsAreUsable(t *testing.T) {
	malformed := []string{
		"custom:", "custom(", "custom()", "custom(-1)", "nonsense",
		"custom:99999999999999999999999999", "custom:-5", "custom:0x10", "((((",
	}

	for _, selector := range malformed {
		if _, err := ParseNodeType(selector); err == nil {
			t.Errorf("ParseNodeType(%q) accepted a malformed selector", selector)
		} else if !strings.Contains(err.Error(), "node type") {
			t.Errorf("ParseNodeType(%q) error does not identify the parser: %v", selector, err)
		}

		if _, err := ParseEdgeType(selector); err == nil {
			t.Errorf("ParseEdgeType(%q) accepted a malformed selector", selector)
		} else if !strings.Contains(err.Error(), "edge type") {
			t.Errorf("ParseEdgeType(%q) error does not identify the parser: %v", selector, err)
		}
	}
}
