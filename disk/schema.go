package disk

// The declaration catalogue, written beside the image.
//
// # What was wrong
//
// A declaration tells the engine something about the graph that it cannot work
// out for itself, and then did not survive being told. Ordered keys and
// composite tuples were written into the CSR image, so they came back after a
// compaction and vanished after a bare reopen. Unique property keys and edge
// cardinality constraints were never written anywhere at all: every process
// that opened a store had to re-declare them, and one that forgot got a store
// with no constraints and no indication that there had ever been any.
//
// That is worse than an inconvenience. Two processes could open one directory
// enforcing different rules, both believing they held the guarantee, and the
// one that had not declared would write the duplicates the other existed to
// refuse.
//
// # Why a sidecar and not the WAL, and not the image
//
// A WAL record would be durable at declare time, which is the property wanted,
// and would then be destroyed by the operation that makes a store permanent:
// compaction truncates the log. That is exactly what happens to the key
// rotation timeline (Store.KeyTimeline), and §11a.2 states the general rule — a
// record that disappears precisely when it becomes the only evidence. A new WAL
// record type is also a one-way format break, because replay treats an unknown
// type as an error rather than skipping it, deliberately.
//
// A CSR section alone does not work either: that is what GORD and GCMP already
// are, and their defect is the bug being fixed. Doing it properly through the
// image would need the section *and* a WAL record *and* compaction carriage
// *and* a criticality decision — four moving parts against one file.
//
// So: graphene.schema joins graphene.labels, graphene.grants,
// graphene.redactions, graphene.audit and graphene.checkpoints beside
// graphene.csr. An older engine ignores a file it does not know about, a newer
// one reads it, and a store written either way is readable by both. Backup and
// restore enumerate the directory rather than a list of names they know, so the
// catalogue travels with a copy at no cost.
//
// # Why the union with GORD and GCMP can never conflict
//
// All four declaration kinds are monotone. The image sections say "declared as
// of the last compaction"; the sidecar says "declared, ever". A union cannot
// lose a declaration, and unlike a label table — where one number can be given
// two names, which is why typenames.go is strict — there is no key here that
// could hold two values. There is no error case, and nobody should add one.
//
// # Where it is applied, and why in two places
//
// Ordered and composite declarations are applied *before* the image loads, so
// their structures are built by the same incremental path a live declaration
// uses rather than by a backfill afterwards — the argument csr_io.go already
// makes for GORD. Constraints are applied *after* the WAL replays, because they
// validate against the data and the data is not complete until then.
//
// Nothing is written at open. Open is not a writer, and OpenReadOnly and
// OpenLive must leave the directory untouched.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene/store"
)

// schemaFileName is the catalogue's name in the store directory.
const schemaFileName = "graphene.schema"

// schemaHeader is the first line, so a reader can tell the file apart from
// anything that happens to share its name, and can refuse a generation it does
// not understand rather than misreading it.
const schemaHeader = "graphene-schema v1"

// Declaration kinds.
//
// A "!" prefix marks a line critical, mirroring csrSectionCritial in the image's
// section table and reusing its argument: an unrecognised *critical* kind
// refuses the open, because it was written by a build that knew a constraint
// this one does not, and reading the line as though it were absent would
// silently drop that constraint. An unrecognised *optional* kind is skipped,
// because a reader ignoring it entirely still answers every query correctly.
//
// That is the same optimisation-versus-constraint distinction §2.3 draws
// everywhere else, and the same one graphene.go's doc comments draw when they
// explain why DeclareUniqueProperty errors on a backend that cannot enforce it
// while DeclareOrderedProperty returns nil.
const (
	kindOrderedNode   = "ordered-node"
	kindOrderedEdge   = "ordered-edge"
	kindCompositeNode = "composite-node"
	kindCompositeEdge = "composite-edge"

	kindUniqueNode     = "!unique-node"
	kindUniqueEdge     = "!unique-edge"
	kindUniqueEdgeType = "!unique-edge-type"
)

func criticalKind(kind string) bool { return strings.HasPrefix(kind, "!") }

// ConstraintPolicy decides what an open does when the catalogue names a
// constraint the data does not satisfy.
type ConstraintPolicy uint8

const (
	// ConstraintRefuse fails the open, naming every violated declaration. The
	// default, and right for a writable store: this is the same check
	// DeclareUniqueProperty makes, at the same moment, and a store that opens
	// believing a constraint it does not keep has the guarantee and none of the
	// behaviour.
	ConstraintRefuse ConstraintPolicy = iota

	// ConstraintDrop opens without the violated declarations and reports them
	// through DroppedDeclarations.
	//
	// Offered for two reasons. A store that cannot be opened cannot be repaired,
	// and repair is the only way forward from ConstraintRefuse. And a read-only
	// store — which cannot write, so cannot enforce anything — has nothing to
	// gain from refusing: reading a damaged store is exactly what a reader opens
	// one to do. OpenReadOnly and OpenLive therefore default to this.
	ConstraintDrop
)

// Catalogue is every declaration a store carries.
//
// Its zero value is a store that has declared nothing, which is most of them.
type Catalogue struct {
	OrderedNodeKeys   []string
	OrderedEdgeKeys   []string
	UniqueNodeKeys    []string
	UniqueEdgeKeys    []string
	UniqueEdgeTypes   []store.EdgeType
	CompositeNodeKeys [][]string
	CompositeEdgeKeys [][]string
}

// Empty reports whether there is nothing to record. A store that has never
// declared anything writes no file, which keeps an existing directory
// byte-identical until it acquires a reason not to be.
func (c Catalogue) Empty() bool {
	return len(c.OrderedNodeKeys) == 0 && len(c.OrderedEdgeKeys) == 0 &&
		len(c.UniqueNodeKeys) == 0 && len(c.UniqueEdgeKeys) == 0 &&
		len(c.UniqueEdgeTypes) == 0 &&
		len(c.CompositeNodeKeys) == 0 && len(c.CompositeEdgeKeys) == 0
}

// encode renders the catalogue as the file's bytes.
//
// Lines are sorted whole, by byte order, after the header. One rule, and it is
// what makes the file a function of the declarations rather than of the order
// they were made in: two stores holding the same schema produce identical
// bytes. A composite tuple keeps its declared key order *inside* its line,
// because order is part of a composite's identity even though it is not part of
// its use — so (a,b) and (b,a) are two lines and both survive, exactly as they
// do in memory today.
func (c Catalogue) encode() []byte {
	var lines []string

	for _, k := range c.OrderedNodeKeys {
		lines = append(lines, kindOrderedNode+"\t"+escapeField(k))
	}
	for _, k := range c.OrderedEdgeKeys {
		lines = append(lines, kindOrderedEdge+"\t"+escapeField(k))
	}
	for _, k := range c.UniqueNodeKeys {
		lines = append(lines, kindUniqueNode+"\t"+escapeField(k))
	}
	for _, k := range c.UniqueEdgeKeys {
		lines = append(lines, kindUniqueEdge+"\t"+escapeField(k))
	}
	for _, t := range c.CompositeNodeKeys {
		lines = append(lines, kindCompositeNode+"\t"+joinEscaped(t))
	}
	for _, t := range c.CompositeEdgeKeys {
		lines = append(lines, kindCompositeEdge+"\t"+joinEscaped(t))
	}
	for _, t := range c.UniqueEdgeTypes {
		lines = append(lines, kindUniqueEdgeType+"\t"+strconv.FormatUint(uint64(t), 10))
	}

	slices.Sort(lines)
	lines = slices.Compact(lines)

	var b strings.Builder
	b.WriteString(schemaHeader)
	b.WriteByte('\n')
	for _, l := range lines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// escapeField makes a property key safe to write in a tab-separated line.
//
// Escaped rather than refused, so the file format imposes no restriction of its
// own on what a property key may contain. A key holding a tab is pathological
// but it is not the catalogue's business to forbid it, and refusing one here
// would mean a key that could be declared before this file existed and not
// after.
func escapeField(s string) string {
	if !strings.ContainsAny(s, "\\\t\n\r") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func unescapeField(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			return "", fmt.Errorf("field %q ends in a dangling escape", s)
		}
		switch s[i] {
		case '\\':
			b.WriteByte('\\')
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		default:
			return "", fmt.Errorf("field %q holds an unknown escape %q", s, `\`+string(s[i]))
		}
	}
	return b.String(), nil
}

func joinEscaped(keys []string) string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = escapeField(k)
	}
	return strings.Join(out, "\t")
}

// decodeCatalogue parses the catalogue's bytes.
func decodeCatalogue(data []byte) (Catalogue, error) {
	var c Catalogue

	sc := bufio.NewScanner(bytes.NewReader(data))
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return Catalogue{}, err
		}
		return Catalogue{}, errors.New("file is empty")
	}
	if got := sc.Text(); got != schemaHeader {
		// A generation this build does not know rather than a file to guess at.
		// Guessing is how a catalogue becomes a constraint on the wrong key.
		return Catalogue{}, fmt.Errorf("unrecognised header %q, want %q", got, schemaHeader)
	}

	for line := 2; sc.Scan(); line++ {
		text := sc.Text()
		if text == "" {
			continue
		}
		if err := parseSchemaLine(&c, text); err != nil {
			return Catalogue{}, fmt.Errorf("line %d: %w", line, err)
		}
	}
	if err := sc.Err(); err != nil {
		return Catalogue{}, err
	}
	return c, nil
}

func parseSchemaLine(c *Catalogue, text string) error {
	parts := strings.Split(text, "\t")
	if len(parts) < 2 {
		return fmt.Errorf("want a kind and at least one field, got %d", len(parts))
	}
	kind, raw := parts[0], parts[1:]

	fields := make([]string, len(raw))
	for i, f := range raw {
		v, err := unescapeField(f)
		if err != nil {
			return err
		}
		if v == "" {
			return fmt.Errorf("%s has an empty key", kind)
		}
		fields[i] = v
	}

	one := func() (string, error) {
		if len(fields) != 1 {
			return "", fmt.Errorf("%s wants one field, got %d", kind, len(fields))
		}
		return fields[0], nil
	}

	switch kind {
	case kindOrderedNode:
		k, err := one()
		if err != nil {
			return err
		}
		c.OrderedNodeKeys = append(c.OrderedNodeKeys, k)
	case kindOrderedEdge:
		k, err := one()
		if err != nil {
			return err
		}
		c.OrderedEdgeKeys = append(c.OrderedEdgeKeys, k)
	case kindUniqueNode:
		k, err := one()
		if err != nil {
			return err
		}
		c.UniqueNodeKeys = append(c.UniqueNodeKeys, k)
	case kindUniqueEdge:
		k, err := one()
		if err != nil {
			return err
		}
		c.UniqueEdgeKeys = append(c.UniqueEdgeKeys, k)

	case kindCompositeNode, kindCompositeEdge:
		// Two keys is the minimum a composite can be declared with. The index
		// checks it too, but reporting it here names the line.
		if len(fields) < 2 {
			return fmt.Errorf("%s wants at least two keys, got %d", kind, len(fields))
		}
		if kind == kindCompositeNode {
			c.CompositeNodeKeys = append(c.CompositeNodeKeys, fields)
		} else {
			c.CompositeEdgeKeys = append(c.CompositeEdgeKeys, fields)
		}

	case kindUniqueEdgeType:
		f, err := one()
		if err != nil {
			return err
		}
		n, convErr := strconv.ParseUint(f, 10, 16)
		if convErr != nil {
			return fmt.Errorf("%s value %q: %w", kind, f, convErr)
		}
		c.UniqueEdgeTypes = append(c.UniqueEdgeTypes, store.EdgeType(n))

	default:
		// The criticality rule, and the only place it is consulted.
		if criticalKind(kind) {
			return fmt.Errorf("catalogue names critical declaration %q, which this build does not "+
				"understand — it was written by a newer version, and opening without that "+
				"constraint would give the guarantee and none of the behaviour", kind)
		}
		// An optional kind a later version added. Skipped, which is what makes
		// the optional marking mean anything.
	}
	return nil
}

// readCatalogue reads the catalogue in dir. A missing file is an empty
// catalogue and no error: most stores have never declared anything.
func readCatalogue(dir string) (Catalogue, error) {
	data, err := os.ReadFile(filepath.Join(dir, schemaFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Catalogue{}, nil
		}
		return Catalogue{}, fmt.Errorf("read %s: %w", schemaFileName, err)
	}
	c, err := decodeCatalogue(data)
	if err != nil {
		return Catalogue{}, fmt.Errorf("read %s: %w", schemaFileName, err)
	}
	return c, nil
}

// writeCatalogue replaces the catalogue atomically.
//
// Written to a temporary, fsynced, renamed, and the directory fsynced after. A
// catalogue a crash can lose is a constraint a crash can lose, and the store
// would then reopen enforcing less than the caller was told it would.
//
// An empty catalogue removes the file rather than writing a header with nothing
// under it, so a store that has never declared looks like one that never did.
func writeCatalogue(dir string, c Catalogue) error {
	final := filepath.Join(dir, schemaFileName)
	if c.Empty() {
		if err := os.Remove(final); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("write %s: %w", schemaFileName, err)
		}
		return nil
	}

	tmp := final + ".tmp"
	if err := writeFileSync(tmp, c.encode(), 0o644); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", schemaFileName, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", schemaFileName, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("write %s: %w", schemaFileName, err)
	}
	return nil
}

// Declarations reports what this store currently has declared, as one value.
func (s *Store) Declarations() Catalogue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.catalogueLocked()
}

// catalogueLocked reads back every declaration in force. The caller holds s.mu.
//
// Taken from the index and the store rather than from a field this package
// maintains alongside them: those structures are what enforce the constraints,
// so they are the only things that can say what is in force. A catalogue kept
// in parallel would drift the first time anything declared without updating it.
func (s *Store) catalogueLocked() Catalogue {
	idx := s.index()
	return Catalogue{
		OrderedNodeKeys:   idx.OrderedNodeKeys(),
		OrderedEdgeKeys:   idx.OrderedEdgeKeys(),
		UniqueNodeKeys:    idx.UniqueNodeKeys(),
		UniqueEdgeKeys:    idx.UniqueEdgeKeys(),
		CompositeNodeKeys: idx.CompositeNodeKeys(),
		CompositeEdgeKeys: idx.CompositeEdgeKeys(),
		UniqueEdgeTypes:   s.uniqueEdgeTypes.Types(),
	}
}

// persistCatalogueLocked writes the store's current declarations to the
// sidecar. The caller holds s.mu and has already validated whatever it added.
//
// A failure here leaves the declaration in force in this process but not
// recorded, which is the safe direction to fail in: the store enforces more
// than the file claims, never less. The error is returned so the caller knows
// the declaration will not survive a reopen.
func (s *Store) persistCatalogueLocked() error {
	if s.readOnly {
		return nil
	}
	return writeCatalogue(s.dir, s.catalogueLocked())
}

// applyCatalogueStructure re-declares the ordered and composite indexes.
//
// Called before the image loads and before anything is replayed, so the
// structures are built by the incremental path as entries land rather than by a
// backfill over them afterwards — the argument csr_io.go makes for GORD, which
// this now generalises to a bare reopen.
//
// It goes straight to the index rather than through the public Declare methods,
// which is what lets a read-only store get its declarations: those refuse a
// read-only store, correctly, because they are writes; this is not a write.
func (s *Store) applyCatalogueStructure(c Catalogue) {
	for _, k := range c.OrderedNodeKeys {
		s.propIdx.DeclareOrderedNodeKey(k)
	}
	for _, k := range c.OrderedEdgeKeys {
		s.propIdx.DeclareOrderedEdgeKey(k)
	}
	// A tuple this build will not accept is skipped rather than fatal, matching
	// how the image's GCMP section is treated: what is skipped is visible,
	// because CompositeNodeProperties reports what is actually declared.
	for _, keys := range c.CompositeNodeKeys {
		_ = s.propIdx.DeclareCompositeNodeKeys(keys)
	}
	for _, keys := range c.CompositeEdgeKeys {
		_ = s.propIdx.DeclareCompositeEdgeKeys(keys)
	}
}

// DroppedDeclaration names a constraint the catalogue recorded that the data did
// not satisfy, and which was therefore not applied.
type DroppedDeclaration struct {
	Kind   string         // "unique-node", "unique-edge", "unique-edge-type"
	Key    string         // the property key, for the two property kinds
	Type   store.EdgeType // the edge type, for "unique-edge-type"
	Reason error          // *store.UniqueViolationsError or *store.EdgeCardinalityViolationsError
}

func (d DroppedDeclaration) String() string {
	if d.Kind == "unique-edge-type" {
		return fmt.Sprintf("%s %s: %v", d.Kind, d.Type, d.Reason)
	}
	return fmt.Sprintf("%s %q: %v", d.Kind, d.Key, d.Reason)
}

// CatalogueViolationsError is returned by Open when the persisted catalogue
// names constraints the data does not satisfy.
//
// Every violated declaration is named, not the first, for the reason
// DeclareUniqueProperty reports every conflicting value rather than one: the
// caller is about to repair the store, and one violation per pass is not a
// repair a person can finish.
//
// Reachable because a store's data can change under a recorded constraint — an
// older build that did not read this file could have written duplicates into a
// store that a newer one then opens.
type CatalogueViolationsError struct {
	Dropped []DroppedDeclaration
}

func (e *CatalogueViolationsError) Error() string {
	parts := make([]string, len(e.Dropped))
	for i, d := range e.Dropped {
		parts[i] = d.String()
	}
	return fmt.Sprintf("%s names %d constraint(s) the data does not satisfy: %s",
		schemaFileName, len(e.Dropped), strings.Join(parts, "; "))
}

func (e *CatalogueViolationsError) Unwrap() []error {
	out := make([]error, 0, len(e.Dropped))
	for _, d := range e.Dropped {
		if d.Reason != nil {
			out = append(out, d.Reason)
		}
	}
	return out
}

// DroppedDeclarations reports the constraints an open declined to apply because
// the data did not satisfy them. Always empty on a store opened with
// ConstraintRefuse, which fails the open instead.
func (s *Store) DroppedDeclarations() []DroppedDeclaration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.dropped)
}

// applyCatalogueConstraints validates and applies the unique constraints.
//
// Called after the WAL has replayed, because the data is not complete until
// then and these are the declarations that check it.
//
// Like applyCatalogueStructure this goes to the index and the type set directly
// rather than through the public Declare methods, so a read-only store is
// covered — which is an improvement on its own: a reader previously got none of
// the writer's declarations, so NodeByProperty was total for one and not the
// other.
func (s *Store) applyCatalogueConstraints(c Catalogue, policy ConstraintPolicy) ([]DroppedDeclaration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dropped []DroppedDeclaration

	for _, k := range c.UniqueNodeKeys {
		if conflicts := s.propIdx.DeclareUniqueNodeKey(k, s.nodeExistsLocked); len(conflicts) > 0 {
			dropped = append(dropped, DroppedDeclaration{
				Kind: "unique-node", Key: k,
				Reason: &store.UniqueViolationsError{Kind: "node", Key: k, Conflicts: conflicts},
			})
		}
	}
	for _, k := range c.UniqueEdgeKeys {
		if conflicts := s.propIdx.DeclareUniqueEdgeKey(k, s.edgeExistsLocked); len(conflicts) > 0 {
			dropped = append(dropped, DroppedDeclaration{
				Kind: "unique-edge", Key: k,
				Reason: &store.UniqueViolationsError{Kind: "edge", Key: k, Conflicts: conflicts},
			})
		}
	}
	for _, t := range c.UniqueEdgeTypes {
		if s.uniqueEdgeTypes.Declared(t) {
			continue
		}
		if conflicts := s.edgeCardinalityConflictsLocked(t); len(conflicts) > 0 {
			dropped = append(dropped, DroppedDeclaration{
				Kind: "unique-edge-type", Type: t,
				Reason: &store.EdgeCardinalityViolationsError{Type: t, Conflicts: conflicts},
			})
			continue
		}
		s.uniqueEdgeTypes.Declare(t)
	}

	if len(dropped) > 0 && policy == ConstraintRefuse {
		return nil, &CatalogueViolationsError{Dropped: dropped}
	}
	return dropped, nil
}
