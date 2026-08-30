package main

// Reading a subgraph pattern from a file.
//
// traversal.Pattern is a Go struct with no serialised form of its own — the
// engine has never needed one, because every caller so far built patterns in
// code. A shell needs a file format, so this defines the smallest one that can
// express what Pattern holds, and translates it.
//
// Deliberately not JSON-tagged onto traversal.Pattern directly. The struct uses
// integer NodeType and EdgeType values and a file written against those would
// break the moment a label was renumbered; going through the same
// store.ParseNodeType every other flag uses means a pattern file names its
// labels the way the rest of the tool does.

import (
	"bytes"
	"encoding/json"
	"os"

	"github.com/aoiflux/graphene/traversal"
)

// patternFile is the on-disk shape.
//
//	{
//	  "nodes": [{"labels": ["EvidenceFile"]}, {"labels": ["MicroArtefact"]}],
//	  "edges": [{"src": 0, "dst": 1, "labels": ["Contains"]}]
//	}
//
// Node indices in the edge list are positions in the node list. There is no
// explicit node ID: making the position the identity removes the one way a
// pattern file can be internally inconsistent.
type patternFile struct {
	Nodes []struct {
		Labels []string `json:"labels"`
	} `json:"nodes"`
	Edges []struct {
		Src    int      `json:"src"`
		Dst    int      `json:"dst"`
		Labels []string `json:"labels"`
	} `json:"edges"`
}

// loadPattern reads and validates a pattern file.
func loadPattern(path string) (*traversal.Pattern, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var pf patternFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Unknown fields are an error rather than an ignored typo: a pattern with
	// "label" where it meant "labels" would otherwise match everything and look
	// like a working query that found rather a lot.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&pf); err != nil {
		return nil, Usagef("-spec %s: %v", path, err)
	}

	if len(pf.Nodes) == 0 {
		return nil, Usagef("-spec %s: a pattern needs at least one node", path)
	}

	p := &traversal.Pattern{
		Nodes: make([]traversal.PatternNode, len(pf.Nodes)),
		Edges: make([]traversal.PatternEdge, len(pf.Edges)),
	}
	for i, n := range pf.Nodes {
		labels, lerr := nodeTypes(n.Labels)
		if lerr != nil {
			return nil, Usagef("-spec %s: node %d: %v", path, i, lerr)
		}
		p.Nodes[i] = traversal.PatternNode{ID: i, Labels: labels}
	}
	for i, e := range pf.Edges {
		if e.Src < 0 || e.Src >= len(pf.Nodes) || e.Dst < 0 || e.Dst >= len(pf.Nodes) {
			return nil, Usagef("-spec %s: edge %d refers to node %d/%d, "+
				"and the pattern has %d nodes (indices 0..%d)",
				path, i, e.Src, e.Dst, len(pf.Nodes), len(pf.Nodes)-1)
		}
		labels, lerr := edgeTypes(e.Labels)
		if lerr != nil {
			return nil, Usagef("-spec %s: edge %d: %v", path, i, lerr)
		}
		p.Edges[i] = traversal.PatternEdge{
			SrcPatternID: e.Src, DstPatternID: e.Dst, Labels: labels,
		}
	}
	return p, nil
}
