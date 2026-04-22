package store

import "fmt"

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
