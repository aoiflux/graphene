package store

// Names for the custom label range.
//
// # The problem
//
// NodeTypeCustomBase opens 32 768 values an application defines the meaning of,
// and the engine renders every one of them as Custom(7). An application with a
// dozen custom types therefore carries its own number-to-name table and
// translates at every boundary — log lines, error messages, CLI tables, exported
// visualisations. That table is a second copy of the numbering, and the moment
// it drifts from the one the data was written with, every one of those surfaces
// silently misreads the database. The engine's own viz package had already grown
// a private copy of the same need.
//
// # Why only the custom range
//
// Registering a name for a built-in would change what ParseNodeType("case")
// means and what a golden CLI corpus contains, to rename something whose meaning
// the engine defines rather than the caller. The custom range is exactly the
// part where the caller owns the meaning, so it is exactly the part where the
// caller may name it.
//
// # Why a package global
//
// Because String() is a method on a uint16 with no store in scope, and moving
// rendering behind a store handle would change every call site in the engine and
// every one in every caller. The consequence is that one process has one naming,
// which is the right constraint for what this is: a numbering is a property of
// the data, and two stores in one process that disagree about what 32768 means
// cannot both be rendered correctly. Registration reports that disagreement
// rather than resolving it.
//
// Reads are lock-free. The table is replaced wholesale on the rare write and
// read through an atomic pointer on the common one, so String() costs a load and
// a map lookup and never contends.

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// typeNames is the registry. Copy-on-write behind an atomic pointer: registering
// is rare and rendering is not.
type typeNames struct {
	nodes map[NodeType]string
	edges map[EdgeType]string

	// byName maps the normalised form back, so a parser accepts an alias
	// wherever it accepts a built-in name. Kinds are separate because a node
	// type and an edge type may legitimately share a name.
	nodeByName map[string]NodeType
	edgeByName map[string]EdgeType
}

var (
	registeredNames atomic.Pointer[typeNames]

	// registerMu serialises writers so two concurrent registrations cannot each
	// copy the same table and lose one of the results. Readers never take it.
	registerMu sync.Mutex
)

func loadNames() *typeNames {
	if n := registeredNames.Load(); n != nil {
		return n
	}
	return nil
}

// clone returns a copy with room for one more entry of each kind.
func (n *typeNames) clone() *typeNames {
	out := &typeNames{
		nodes:      make(map[NodeType]string),
		edges:      make(map[EdgeType]string),
		nodeByName: make(map[string]NodeType),
		edgeByName: make(map[string]EdgeType),
	}
	if n != nil {
		maps.Copy(out.nodes, n.nodes)
		maps.Copy(out.edges, n.edges)
		maps.Copy(out.nodeByName, n.nodeByName)
		maps.Copy(out.edgeByName, n.edgeByName)
	}
	return out
}

// ErrTypeNameConflict is returned when a registration disagrees with one already
// in force.
//
// It is deliberately an error rather than a last-writer-wins overwrite. Two
// namings of one number in one process means something is wrong upstream — two
// stores written by different versions of an application, or a table that has
// drifted from the data — and silently picking one is how a forensic tool
// reports the wrong thing with total confidence.
var ErrTypeNameConflict = fmt.Errorf("graphene: type name conflicts with one already registered")

// RegisterNodeTypeName gives a custom node type a name.
//
// The name is what String() renders and what ParseNodeType accepts, in addition
// to — never instead of — the Custom(7), custom:7 and bare-numeric forms, so
// nothing that parsed before stops parsing.
//
// Registering the same type and name twice is a no-op, which is what makes it
// safe to call at every Open. Registering a different name for a type that has
// one, or the same name for a different type, returns ErrTypeNameConflict and
// changes nothing.
//
// Only the custom range can be named: the built-in types mean what the engine
// says they mean, and renaming one would change what ParseNodeType("case")
// resolves to.
func RegisterNodeTypeName(t NodeType, name string) error {
	clean, err := validTypeName(name)
	if err != nil {
		return err
	}
	if !t.IsCustom() {
		return fmt.Errorf("graphene: cannot name %s: only the custom range (>= %d) can be named",
			t, NodeTypeCustomBase)
	}

	registerMu.Lock()
	defer registerMu.Unlock()

	cur := loadNames()
	if cur != nil {
		if existing, named := cur.nodes[t]; named {
			if existing == name {
				return nil
			}
			return fmt.Errorf("%w: node type %d is already named %q, not %q",
				ErrTypeNameConflict, uint16(t), existing, name)
		}
		if owner, taken := cur.nodeByName[clean]; taken {
			return fmt.Errorf("%w: node name %q is already held by type %d",
				ErrTypeNameConflict, name, uint16(owner))
		}
	}
	next := cur.clone()
	next.nodes[t] = name
	next.nodeByName[clean] = t
	registeredNames.Store(next)
	return nil
}

// RegisterEdgeTypeName is RegisterNodeTypeName for edge types. Node and edge
// names are independent: the same name may be used for one of each.
func RegisterEdgeTypeName(t EdgeType, name string) error {
	clean, err := validTypeName(name)
	if err != nil {
		return err
	}
	if !t.IsCustom() {
		return fmt.Errorf("graphene: cannot name %s: only the custom range (>= %d) can be named",
			t, EdgeTypeCustomBase)
	}

	registerMu.Lock()
	defer registerMu.Unlock()

	cur := loadNames()
	if cur != nil {
		if existing, named := cur.edges[t]; named {
			if existing == name {
				return nil
			}
			return fmt.Errorf("%w: edge type %d is already named %q, not %q",
				ErrTypeNameConflict, uint16(t), existing, name)
		}
		if owner, taken := cur.edgeByName[clean]; taken {
			return fmt.Errorf("%w: edge name %q is already held by type %d",
				ErrTypeNameConflict, name, uint16(owner))
		}
	}
	next := cur.clone()
	next.edges[t] = name
	next.edgeByName[clean] = t
	registeredNames.Store(next)
	return nil
}

// NodeTypeNames returns the registered custom node names, keyed by type.
// The returned map is a copy and is safe to keep.
func NodeTypeNames() map[NodeType]string {
	cur := loadNames()
	out := make(map[NodeType]string)
	if cur != nil {
		maps.Copy(out, cur.nodes)
	}
	return out
}

// EdgeTypeNames is NodeTypeNames for edge types.
func EdgeTypeNames() map[EdgeType]string {
	cur := loadNames()
	out := make(map[EdgeType]string)
	if cur != nil {
		maps.Copy(out, cur.edges)
	}
	return out
}

// nodeTypeName returns the registered name for t, if any.
func nodeTypeName(t NodeType) (string, bool) {
	cur := loadNames()
	if cur == nil {
		return "", false
	}
	name, ok := cur.nodes[t]
	return name, ok
}

func edgeTypeName(t EdgeType) (string, bool) {
	cur := loadNames()
	if cur == nil {
		return "", false
	}
	name, ok := cur.edges[t]
	return name, ok
}

// nodeTypeByName resolves an already-normalised selector.
func nodeTypeByName(normalised string) (NodeType, bool) {
	cur := loadNames()
	if cur == nil {
		return NodeTypeUnknown, false
	}
	t, ok := cur.nodeByName[normalised]
	return t, ok
}

func edgeTypeByName(normalised string) (EdgeType, bool) {
	cur := loadNames()
	if cur == nil {
		return EdgeTypeUnknown, false
	}
	t, ok := cur.edgeByName[normalised]
	return t, ok
}

// builtinTypeNames is every name the parsers already resolve. A registration
// that shadowed one would make a selector mean two things.
var builtinTypeNames = []string{
	"unknown", "evidencefile", "microartefact", "tag", "case",
	"contains", "similarto", "reuse", "temporal", "taggedwith", "belongsto",
}

// validTypeName checks a proposed name and returns its normalised form.
//
// The rules exist so a name can be used everywhere a type selector is: it has to
// survive normalizeTypeSelector without vanishing, it must not be something the
// parsers already resolve to something else, and it must not be renderable as
// one of the numeric forms — a type named "42" or "custom:3" would parse as a
// different type than it prints as, which is the one property this whole
// mechanism exists to guarantee.
func validTypeName(name string) (string, error) {
	if strings.TrimSpace(name) != name {
		return "", fmt.Errorf("graphene: type name %q has leading or trailing space", name)
	}
	clean := normalizeTypeSelector(name)
	if clean == "" {
		return "", fmt.Errorf("graphene: type name must not be empty")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("graphene: type name %q contains a control character", name)
		}
	}
	if slices.Contains(builtinTypeNames, clean) {
		return "", fmt.Errorf("graphene: type name %q is a built-in type name", name)
	}
	if _, isCustomForm, _ := parseCustomOffset(name); isCustomForm {
		return "", fmt.Errorf("graphene: type name %q looks like a custom-offset selector", name)
	}
	if isAllDigits(clean) {
		return "", fmt.Errorf("graphene: type name %q is numeric, which already selects a type", name)
	}
	return clean, nil
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}
