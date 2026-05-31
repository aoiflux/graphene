package store

import (
	"fmt"
	"strconv"
	"strings"
)

// NodeID and EdgeID are globally unique uint64 identifiers.
// IDs are assigned monotonically by the store and never reused.
type NodeID uint64
type EdgeID uint64

// InvalidNodeID and InvalidEdgeID are sentinel zero values.
const (
	InvalidNodeID NodeID = 0
	InvalidEdgeID EdgeID = 0
)

// NodeType classifies the kind of entity a node represents.
type NodeType uint8

const (
	NodeTypeUnknown       NodeType = 0
	NodeTypeEvidenceFile  NodeType = 1
	NodeTypeMicroArtefact NodeType = 2
	NodeTypeTag           NodeType = 3
	NodeTypeCase          NodeType = 4

	// NodeTypeCustomBase is the first value of the user-defined range (128–255).
	// Use CustomNodeType to create values in this range.
	NodeTypeCustomBase NodeType = 128
)

// CustomNodeType returns a NodeType in the user-defined range [128, 255].
// offset must be in [0, 127]; values outside that range panic.
func CustomNodeType(offset uint8) NodeType {
	if offset > 127 {
		panic("graphene: CustomNodeType offset must be in [0, 127]")
	}
	return NodeTypeCustomBase + NodeType(offset)
}

// IsCustom reports whether t is in the user-defined custom range.
func (t NodeType) IsCustom() bool {
	return t >= NodeTypeCustomBase
}

// ParseNodeType resolves selector into a NodeType.
//
// Supported built-ins (case-insensitive):
//   - "case"
//   - "evidencefile" / "evidence_file"
//   - "microartefact" / "micro_artefact"
//   - "tag"
//
// Supported custom forms:
//   - "custom:7"
//   - "custom(7)"
//   - "custom-7"
//
// Numeric forms are also accepted:
//   - "130" -> NodeType(130)
func ParseNodeType(selector string) (NodeType, error) {
	s := strings.TrimSpace(selector)
	if s == "" {
		return NodeTypeUnknown, fmt.Errorf("parse node type: empty selector")
	}
	if v, ok, err := parseNodeTypeCustomSelector(s); ok || err != nil {
		return v, err
	}

	norm := normalizeTypeSelector(s)
	switch norm {
	case "unknown":
		return NodeTypeUnknown, nil
	case "evidencefile":
		return NodeTypeEvidenceFile, nil
	case "microartefact":
		return NodeTypeMicroArtefact, nil
	case "tag":
		return NodeTypeTag, nil
	case "case":
		return NodeTypeCase, nil
	}

	num, err := strconv.Atoi(norm)
	if err == nil {
		if num < 0 || num > 255 {
			return NodeTypeUnknown, fmt.Errorf("parse node type: numeric value out of range [0,255]: %d", num)
		}
		return NodeType(uint8(num)), nil
	}

	return NodeTypeUnknown, fmt.Errorf("parse node type: unsupported selector %q", selector)
}

func (t NodeType) String() string {
	switch t {
	case NodeTypeEvidenceFile:
		return "EvidenceFile"
	case NodeTypeMicroArtefact:
		return "MicroArtefact"
	case NodeTypeTag:
		return "Tag"
	case NodeTypeCase:
		return "Case"
	default:
		if t >= NodeTypeCustomBase {
			return fmt.Sprintf("Custom(%d)", t-NodeTypeCustomBase)
		}
		return "Unknown"
	}
}

// EdgeType classifies the semantic relationship an edge represents.
type EdgeType uint8

const (
	EdgeTypeUnknown    EdgeType = 0
	EdgeTypeContains   EdgeType = 1 // EvidenceFile → MicroArtefact (provenance)
	EdgeTypeSimilarTo  EdgeType = 2 // MicroArtefact ↔ MicroArtefact (similarity score)
	EdgeTypeReuse      EdgeType = 3 // MicroArtefact → MicroArtefact (byte reuse)
	EdgeTypeTemporal   EdgeType = 4 // any node → any node (time-ordered relation)
	EdgeTypeTaggedWith EdgeType = 5 // MicroArtefact → Tag
	EdgeTypeBelongsTo  EdgeType = 6 // EvidenceFile / MicroArtefact → Case

	// EdgeTypeCustomBase is the first value of the user-defined range (128–255).
	// Use CustomEdgeType to create values in this range.
	EdgeTypeCustomBase EdgeType = 128
)

// CustomEdgeType returns an EdgeType in the user-defined range [128, 255].
// offset must be in [0, 127]; values outside that range panic.
func CustomEdgeType(offset uint8) EdgeType {
	if offset > 127 {
		panic("graphene: CustomEdgeType offset must be in [0, 127]")
	}
	return EdgeTypeCustomBase + EdgeType(offset)
}

// IsCustom reports whether t is in the user-defined custom range.
func (t EdgeType) IsCustom() bool {
	return t >= EdgeTypeCustomBase
}

// ParseEdgeType resolves selector into an EdgeType.
//
// Supported built-ins (case-insensitive):
//   - "contains"
//   - "similarto" / "similar_to"
//   - "reuse"
//   - "temporal"
//   - "taggedwith" / "tagged_with"
//   - "belongsto" / "belongs_to"
//
// Supported custom forms:
//   - "custom:7"
//   - "custom(7)"
//   - "custom-7"
//
// Numeric forms are also accepted:
//   - "130" -> EdgeType(130)
func ParseEdgeType(selector string) (EdgeType, error) {
	s := strings.TrimSpace(selector)
	if s == "" {
		return EdgeTypeUnknown, fmt.Errorf("parse edge type: empty selector")
	}
	if v, ok, err := parseEdgeTypeCustomSelector(s); ok || err != nil {
		return v, err
	}

	norm := normalizeTypeSelector(s)
	switch norm {
	case "unknown":
		return EdgeTypeUnknown, nil
	case "contains":
		return EdgeTypeContains, nil
	case "similarto":
		return EdgeTypeSimilarTo, nil
	case "reuse":
		return EdgeTypeReuse, nil
	case "temporal":
		return EdgeTypeTemporal, nil
	case "taggedwith":
		return EdgeTypeTaggedWith, nil
	case "belongsto":
		return EdgeTypeBelongsTo, nil
	}

	num, err := strconv.Atoi(norm)
	if err == nil {
		if num < 0 || num > 255 {
			return EdgeTypeUnknown, fmt.Errorf("parse edge type: numeric value out of range [0,255]: %d", num)
		}
		return EdgeType(uint8(num)), nil
	}

	return EdgeTypeUnknown, fmt.Errorf("parse edge type: unsupported selector %q", selector)
}

func normalizeTypeSelector(s string) string {
	n := strings.ToLower(strings.TrimSpace(s))
	n = strings.ReplaceAll(n, "_", "")
	n = strings.ReplaceAll(n, " ", "")
	return n
}

func parseNodeTypeCustomSelector(s string) (NodeType, bool, error) {
	offset, ok, err := parseCustomOffset(s)
	if !ok || err != nil {
		if err != nil {
			return NodeTypeUnknown, true, fmt.Errorf("parse node type: %w", err)
		}
		return NodeTypeUnknown, false, nil
	}
	return CustomNodeType(offset), true, nil
}

func parseEdgeTypeCustomSelector(s string) (EdgeType, bool, error) {
	offset, ok, err := parseCustomOffset(s)
	if !ok || err != nil {
		if err != nil {
			return EdgeTypeUnknown, true, fmt.Errorf("parse edge type: %w", err)
		}
		return EdgeTypeUnknown, false, nil
	}
	return CustomEdgeType(offset), true, nil
}

func parseCustomOffset(s string) (uint8, bool, error) {
	raw := strings.TrimSpace(strings.ToLower(s))
	var payload string
	if strings.HasPrefix(raw, "custom:") {
		payload = strings.TrimSpace(raw[len("custom:"):])
	} else if strings.HasPrefix(raw, "custom-") {
		payload = strings.TrimSpace(raw[len("custom-"):])
	} else if strings.HasPrefix(raw, "custom(") && strings.HasSuffix(raw, ")") {
		payload = strings.TrimSpace(raw[len("custom(") : len(raw)-1])
	} else {
		return 0, false, nil
	}
	if payload == "" {
		return 0, true, fmt.Errorf("custom selector missing offset")
	}
	n, err := strconv.Atoi(payload)
	if err != nil {
		return 0, true, fmt.Errorf("invalid custom offset %q", payload)
	}
	if n < 0 || n > 127 {
		return 0, true, fmt.Errorf("custom offset out of range [0,127]: %d", n)
	}
	return uint8(n), true, nil
}

func (t EdgeType) String() string {
	switch t {
	case EdgeTypeContains:
		return "Contains"
	case EdgeTypeSimilarTo:
		return "SimilarTo"
	case EdgeTypeReuse:
		return "Reuse"
	case EdgeTypeTemporal:
		return "Temporal"
	case EdgeTypeTaggedWith:
		return "TaggedWith"
	case EdgeTypeBelongsTo:
		return "BelongsTo"
	default:
		if t >= EdgeTypeCustomBase {
			return fmt.Sprintf("Custom(%d)", t-EdgeTypeCustomBase)
		}
		return "Unknown"
	}
}

// Direction controls which edges are returned in a neighbour query.
type Direction uint8

const (
	DirectionOutbound Direction = 0 // edges where this node is Src
	DirectionInbound  Direction = 1 // edges where this node is Dst
	DirectionBoth     Direction = 2 // outbound + inbound
)

// Node is the atomic unit of the graph.
// A node may carry one or more labels (NodeType values) simultaneously,
// enabling nodes that play multiple roles (e.g. a MicroArtefact that is also
// an AntiForensicIndicator). At least one label is required; callers that do
// not need multi-label classification should use a single-element slice.
// Properties holds a msgpack-encoded blob of domain-specific fields.
type Node struct {
	ID         NodeID
	Labels     []NodeType // one or more; must not be empty
	Properties []byte     // msgpack blob; nil is valid (no properties)
}

// HasLabel reports whether the node carries the given label.
func (n *Node) HasLabel(t NodeType) bool {
	for _, l := range n.Labels {
		if l == t {
			return true
		}
	}
	return false
}

// Edge connects two nodes with a typed, weighted, optionally-propertied relationship.
// An edge may carry one or more labels (EdgeType values) to express composite
// relationship semantics (e.g. an edge that is both SimilarTo and Reuse).
// Weight is meaningful for EdgeTypeSimilarTo (0.0–1.0 similarity score).
// For other edge types Weight is 0.
type Edge struct {
	ID         EdgeID
	Src        NodeID
	Dst        NodeID
	Labels     []EdgeType // one or more; must not be empty
	Weight     float32    // similarity score for SimilarTo; 0 otherwise
	Properties []byte     // msgpack blob; nil is valid
}

// HasLabel reports whether the edge carries the given label.
func (e *Edge) HasLabel(t EdgeType) bool {
	for _, l := range e.Labels {
		if l == t {
			return true
		}
	}
	return false
}

// NeighbourResult groups a neighbouring node with the edge that connects it.
type NeighbourResult struct {
	Node *Node
	Edge *Edge
}
