package disk

import (
	"errors"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Edge property redaction: the relationship survives, only the data goes.

func TestEdgeProp_PropertiesGoTheRelationshipStays(t *testing.T) {
	_, s, ring := redactableStore(t)
	hub := hubWithSpokes(t, s, 2)
	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	target := impact.CascadedEdges[0]

	if err := s.UpdateEdge(&store.Edge{
		ID: target, Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
		Weight: 0.75, Properties: []byte("note=jane.doe reported this"),
	}); err != nil {
		t.Fatal(err)
	}

	before, err := s.GetEdge(target)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := s.RedactEdgeProperties(target, RedactionRequest{
		ActorID: 5, RoleID: 1, Reason: "erasure request 77",
	})
	if err != nil {
		t.Fatal(err)
	}

	after, err := s.GetEdge(target)
	if err != nil {
		t.Fatalf("the edge did not survive its own property redaction: %v", err)
	}
	if len(after.Properties) != 0 {
		t.Fatalf("the properties survived: %q", after.Properties)
	}
	if after.Src != before.Src || after.Dst != before.Dst {
		t.Fatal("the endpoints changed")
	}
	if after.Weight != before.Weight {
		t.Fatalf("the weight changed from %v to %v", before.Weight, after.Weight)
	}
	if len(after.Labels) != len(before.Labels) {
		t.Fatal("the labels changed")
	}

	// Both endpoints still exist, and the other edge is untouched.
	if !s.NodeExists(hub) {
		t.Fatal("an endpoint was removed")
	}
	if _, err := s.GetEdge(impact.CascadedEdges[1]); err != nil {
		t.Fatal("an unrelated edge was removed")
	}

	if rec.Scope != ScopeProperties || rec.EdgeID != target || rec.NodeID != 0 {
		t.Fatalf("the record does not describe an edge property redaction: %+v", rec)
	}
	if rec.PriorPropertiesHash != propertiesHash(before.Properties) {
		t.Fatal("the record's property hash is not the hash of what was removed")
	}
	if rec.SurvivingHash == rec.VersionHash {
		t.Fatal("before and after hash identically; the redaction changed nothing")
	}

	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRedactionChain(ledger, ring); err != nil {
		t.Fatalf("an edge property record broke the chain: %v", err)
	}
}

// The removal is provable from the image, in the edge namespace.
func TestEdgeProp_IsProvableFromTheImage(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 1)
	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	target := impact.CascadedEdges[0]
	if err := s.UpdateEdge(&store.Edge{
		ID: target, Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
		Properties: []byte("pii"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactEdgeProperties(target, RedactionRequest{ActorID: 1, Reason: "provable"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := s.ProveEdgeRedaction(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRedactionInclusion(roots.Snapshot, proof); err != nil {
		t.Fatalf("an edge property redaction did not verify: %v", err)
	}
	if proof.Tombstone.Scope != ScopeProperties || proof.Tombstone.EdgeID != target {
		t.Fatalf("the tombstone does not describe an edge property strip: %+v", proof.Tombstone)
	}

	// It must not answer a node query for the same number.
	if _, err := s.ProveRedaction(store.NodeID(target)); !errors.Is(err, ErrNoTombstone) {
		t.Fatalf("an edge tombstone answered a node query (err=%v)", err)
	}
}

// Redacted edge content must not remain queryable either.
func TestEdgeProp_LeavesTheIndexToo(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 1)
	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	target := impact.CascadedEdges[0]

	if err := s.UpdateEdge(&store.Edge{
		ID: target, Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
		Properties: []byte("secret"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.IndexEdgeProperty(target, "note", []byte("jane@example.com")); err != nil {
		t.Fatal(err)
	}
	found, err := s.EdgesByProperty("note", []byte("jane@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("the value was never indexed, so this test proves nothing")
	}

	if _, err := s.RedactEdgeProperties(target, RedactionRequest{ActorID: 1, Reason: "purge"}); err != nil {
		t.Fatal(err)
	}

	still, err := s.EdgesByProperty("note", []byte("jane@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if len(still) != 0 {
		t.Fatalf("redacted edge content is still queryable: %v", still)
	}
}

// The same refusals as every other form.
func TestEdgeProp_SharesTheSameRefusals(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 1)
	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	target := impact.CascadedEdges[0]

	if _, err := s.RedactEdgeProperties(target, RedactionRequest{ActorID: 1}); !errors.Is(err, ErrRedactionUnexplained) {
		t.Errorf("an unexplained edge property redaction was accepted: %v", err)
	}
	// No properties to remove.
	if _, err := s.RedactEdgeProperties(target, RedactionRequest{ActorID: 1, Reason: "nothing"}); err == nil {
		t.Error("an edge with no properties was redacted anyway")
	} else if !strings.Contains(err.Error(), "no properties") {
		t.Errorf("the refusal does not explain itself: %v", err)
	}

	bare, _ := openFresh(t)
	defer bare.Close()
	if _, err := bare.RedactEdgeProperties(store.EdgeID(1), RedactionRequest{ActorID: 1, Reason: "x"}); err == nil {
		t.Error("RedactEdgeProperties worked without Options.Redaction")
	}
}
