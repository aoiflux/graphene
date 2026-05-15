package viz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func TestExportInteractiveHTMLIncludesExplorationAndOverviewUI(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "graph.html")
	nodes := []*store.Node{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeCase}},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
	}
	edges := []*store.Edge{
		{ID: 10, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.75},
	}

	if err := ExportInteractiveHTML(nodes, edges, outPath); err != nil {
		t.Fatalf("ExportInteractiveHTML() error = %v", err)
	}

	htmlBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	html := string(htmlBytes)

	for _, needle := range []string{
		`id="edgeLabelCanvas"`,
		`id="miniMap"`,
		`id="expandOneHop"`,
		`id="expandTwoHops"`,
		`id="collapseHop"`,
		`id="clearExploration"`,
		`function drawEdgeLabels()`,
		`function drawMiniMap()`,
		`function setExplorationDepth(rootID, depth)`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("expected exported html to contain %q", needle)
		}
	}
}
