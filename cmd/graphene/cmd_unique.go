package main

// debug unique, node upsert, edge upsert.
//
// # Why a declaration is a command at all
//
// A unique constraint is not written into the image. It lives in the property
// index of whichever process declared it, and it dies when that process exits
// (TECHNICAL_DETAILS §6.6). So `debug unique` cannot leave a constraint behind
// for the next command to find, and pretending otherwise would be the worst
// thing this tool could do with it: a constraint an operator believes is in
// force and which nothing enforces.
//
// What it can do is the half that outlives the process, which is also the half
// somebody reaching for it actually wants — declaring *validates*. Every value
// under the key is checked against the live entities holding it, and every value
// held more than once is reported, not just the first. That is the answer to
// "can I make this key a key?", and the report is the repair list.
//
// It runs the library's own declaration rather than counting postings itself. A
// second implementation of uniqueness in the CLI would be a second thing that
// can disagree with the first, and this package has already watched three copies
// of one list drift apart (see registry.go).
//
// `debug unique-edge` is the same command for the other constraint: at most one
// edge of a type between two nodes, which is the rule for structure the way a
// unique property key is the rule for identity. Same shape, same lock, same
// answer-not-a-constraint outcome — what differs is that the conflicts it
// reports are node pairs rather than values, so it is a separate verb rather
// than another flag on this one.
//
// # Why that takes the exclusive lock
//
// Declaring is refused on a read-only store, deliberately: a store that cannot
// be written to can hold the constraint and never enforce it, which is the
// guarantee without the behaviour. The consequence here is that `debug unique`
// opens for writing and takes the exclusive lock while writing nothing at all.
// The notice says so.
//
// # Why upsert is a separate verb from create
//
// `node create` adds a record. Run twice it adds two, which is correct for what
// it says it does and exactly wrong for re-ingesting a source that was already
// ingested. `node upsert` is the idempotent one: it names the entity by a value
// under a unique key rather than by an ID, so the second run finds the first
// run's node and replaces it. That is the only way the shell can express "make
// sure this exists" without the operator keeping an ID.

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aoiflux/graphene/store"
)

// --- debug unique ---

type debugUniqueOpts struct {
	key  string
	edge bool
}

var debugUnique = cmd(Command{
	Group: "debug", Name: "unique", Usage: "<dir>",
	Short: "check whether a property key can be declared unique",
	Long: "-key names a property key already registered in the index. The check is\n" +
		"the declaration the library performs: every value under that key is\n" +
		"matched against the live entities holding it, and the key is unique when\n" +
		"no value is held by two.\n\n" +
		"Every conflicting value is reported, not the first — a graph that needs\n" +
		"repairing should need one pass and not one pass per duplicate.\n\n" +
		"-edge checks an edge property instead of a node property.\n\n" +
		"The constraint itself is not persisted. It lives in the index of the\n" +
		"process that declared it, so this command leaves nothing behind, and a\n" +
		"program that wants the constraint enforced declares it after every Open.\n" +
		"What survives is the answer.",
	Notice: "debug unique opens the store for writing and takes the exclusive lock; " +
		"it writes nothing",
	Open: OpenGraphRW, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *debugUniqueOpts) {
		fs.StringVar(&o.key, "key", "", "property key to check (required)")
		fs.BoolVar(&o.edge, "edge", false, "check an edge property rather than a node property")
	},
	runDebugUnique)

func runDebugUnique(cx *Context, o *debugUniqueOpts) (Result, error) {
	var r Result
	if o.key == "" {
		return r, Usagef("need -key; there is nothing to check without one")
	}
	g := cx.Graph()

	kind := "node"
	err := g.DeclareUniqueProperty(o.key)
	if o.edge {
		kind = "edge"
		err = g.DeclareUniqueEdgeProperty(o.key)
	}

	s := r.Section("")
	s.Add("kind", Str(kind))
	s.Add("key", Str(o.key))

	if err == nil {
		s.Add("unique", Str("yes"))
		r.Verdict = VerdictVerified
		return r, nil
	}

	var v *store.UniqueViolationsError
	if !errors.As(err, &v) {
		// Not an answer about the data at all — a backend that cannot enforce a
		// constraint, or a store that refused to open for writing.
		return r, err
	}

	s.Add("unique", Str("no"))
	s.Add("conflicting values", Int(int64(len(v.Conflicts))))

	// Conflicts arrive in ascending value order, so the table is diffable
	// between runs without sorting here.
	t := r.Table("conflicts", Col("value"), Col("held by"))
	for _, c := range v.Conflicts {
		t.Row(propValue(c.Value), Str(joinIDs(c.IDs)))
	}
	r.Find(SevBroken, "unique.violated",
		"%d value(s) under %s property %q are held by more than one entity",
		len(v.Conflicts), kind, o.key)
	r.Notes("what to do",
		"delete or re-key the extra holders of each value above, then run this again",
		"until it passes, the constraint cannot be declared and upsert cannot use this key")
	return r, nil
}

// --- debug unique-edge ---

type debugUniqueEdgeOpts struct {
	edgeType string
}

var debugUniqueEdge = cmd(Command{
	Group: "debug", Name: "unique-edge", Usage: "<dir>",
	Short: "check whether an edge type can be declared unique per node pair",
	Long: "-type names an edge type. The check is the declaration the library\n" +
		"performs: every live edge carrying that type is grouped by the ordered\n" +
		"pair it joins, and the type is unique when no pair is joined twice.\n\n" +
		"This is the constraint for structure, where `debug unique` is the one\n" +
		"for identity. An edge that exists only to say that one node owns\n" +
		"another carries no value worth indexing, so a unique property key\n" +
		"cannot name it, and paying for one per edge is what a caller writing\n" +
		"millions of them cannot afford.\n\n" +
		"Direction is part of the pair, and the rule is per label rather than\n" +
		"per label set: an edge carrying two types counts against a declaration\n" +
		"on each of them independently.\n\n" +
		"Every conflicting pair is reported, not the first.\n\n" +
		"The constraint itself is not persisted, exactly as with `debug unique`.\n" +
		"What survives is the answer.",
	Notice: "debug unique-edge opens the store for writing and takes the exclusive lock; " +
		"it writes nothing",
	Open: OpenGraphRW, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *debugUniqueEdgeOpts) {
		fs.StringVar(&o.edgeType, "type", "", "edge type to check (required)")
	},
	runDebugUniqueEdge)

func runDebugUniqueEdge(cx *Context, o *debugUniqueEdgeOpts) (Result, error) {
	var r Result
	if o.edgeType == "" {
		return r, Usagef("need -type; there is nothing to check without one")
	}
	t, err := store.ParseEdgeType(o.edgeType)
	if err != nil {
		return r, Usagef("-type %q: %v", o.edgeType, err)
	}

	s := r.Section("")
	s.Add("edge type", Str(t.String()))

	err = cx.Graph().DeclareUniqueEdge(t)
	if err == nil {
		s.Add("unique", Str("yes"))
		r.Verdict = VerdictVerified
		return r, nil
	}

	var v *store.EdgeCardinalityViolationsError
	if !errors.As(err, &v) {
		// Not an answer about the data at all — a backend that cannot enforce a
		// constraint, or a store that refused to open for writing.
		return r, err
	}

	s.Add("unique", Str("no"))
	s.Add("conflicting pairs", Int(int64(len(v.Conflicts))))

	// Conflicts arrive in ascending pair order, so the table is diffable between
	// runs without sorting here.
	tbl := r.Table("conflicts", Col("src"), Col("dst"), Col("edges"))
	for _, c := range v.Conflicts {
		tbl.Row(ID(uint64(c.Pair.Src)), ID(uint64(c.Pair.Dst)), Str(joinEdgeIDs(c.IDs)))
	}
	r.Find(SevBroken, "unique.violated",
		"%d node pair(s) are joined by more than one %s edge", len(v.Conflicts), t)
	r.Notes("what to do",
		"delete the extra edge of each pair above, then run this again",
		"until it passes, the constraint cannot be declared and duplicate edges are not prevented")
	return r, nil
}

// --- node upsert ---

type nodeUpsertOpts struct {
	key    string
	value  string
	labels stringList
	props  string
	index  stringList
}

var nodeUpsert = cmd(Command{
	Group: "node", Name: "upsert", Usage: "<dir>",
	Short: "create a node, or replace the one already holding this key",
	Long: "-key and -value name the node: a value under a property key that is\n" +
		"unique across the store. Run twice with the same -value it produces one\n" +
		"node, where `node create` run twice produces two — which is what makes\n" +
		"re-ingesting a source safe.\n\n" +
		"The key is declared unique before anything is written, and the whole\n" +
		"command is refused if the store already holds two entities under one\n" +
		"value. `debug unique` lists them.\n\n" +
		"On a hit the labels and -props of the existing node are replaced, each\n" +
		"key given with -index is replaced, and index keys not named keep the\n" +
		"entries they had. The key entry is registered for you, so -index must not\n" +
		"repeat it.\n\n" +
		"The record and its index entries are one transaction, so unlike `node\n" +
		"create` this cannot leave behind a node its own key will not find.",
	Notice:  "node upsert opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *nodeUpsertOpts) {
		fs.StringVar(&o.key, "key", "", "unique property key naming the node (required)")
		fs.StringVar(&o.value, "value", "", "this node's value under -key (required)")
		fs.Var(&o.labels, "label", "node label (repeatable; at least one required)")
		fs.StringVar(&o.props, "props", "", "file whose bytes become the node's Properties")
		fs.Var(&o.index, "index", "key=value to register in the property index (repeatable)")
	},
	runNodeUpsert)

func runNodeUpsert(cx *Context, o *nodeUpsertOpts) (Result, error) {
	var r Result

	labels, err := nodeTypes(o.labels)
	if err != nil {
		return r, err
	}
	if len(labels) == 0 {
		return r, Usagef("need at least one -label; a node without one cannot be created")
	}
	index, blob, err := upsertPayload(o.key, o.value, o.props, o.index)
	if err != nil {
		return r, err
	}
	g := cx.Graph()

	if cx.Globals.DryRun {
		// Answered with a plain equality lookup rather than a declaration,
		// because -dry-run downgraded the open mode and a declaration is a
		// write-mode operation. It still answers the interesting half: whether
		// this creates or replaces.
		held, err := g.NodesByProperty(o.key, []byte(o.value))
		if err != nil {
			return r, err
		}
		s := r.Section("would upsert")
		s.Add("key", Str(o.key))
		s.Add("value", Str(o.value))
		s.Add("labels", Strs(o.labels))
		s.Add("properties", Bytes(int64(len(blob))))
		s.Add("index entries", Int(int64(len(index))))
		switch len(held) {
		case 0:
			s.Add("effect", Str("create"))
		case 1:
			s.AddNote("effect", Str("replace"),
				"node "+strconv.FormatUint(uint64(held[0]), 10))
		default:
			s.Add("effect", Str("refuse"))
			r.Find(SevBroken, "unique.violated",
				"%d nodes already hold %q under %q, so the key cannot be declared unique",
				len(held), o.value, o.key)
		}
		return r, nil
	}

	if err := g.DeclareUniqueProperty(o.key); err != nil {
		return r, uniqueRefusal("node", o.key, err)
	}
	n := &store.Node{Labels: labels, Properties: blob}
	id, created, err := g.UpsertNode(o.key, []byte(o.value), n, index)
	if err != nil {
		return r, err
	}

	s := r.Section(verbFor(created))
	s.Add("node", ID(uint64(id)))
	s.Add("key", Str(o.key))
	s.Add("value", Str(o.value))
	s.Add("labels", Strs(o.labels))
	s.Add("properties", Bytes(int64(len(blob))))
	s.Add("index entries", Int(int64(len(index)+1)))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- edge upsert ---

type edgeUpsertOpts struct {
	key    string
	value  string
	src    uint64
	dst    uint64
	labels stringList
	weight float64
	props  string
	index  stringList
}

var edgeUpsert = cmd(Command{
	Group: "edge", Name: "upsert", Usage: "<dir>",
	Short: "create an edge, or replace the one already holding this key",
	Long: "-key and -value name the edge the way `node upsert` names a node.\n\n" +
		"Endpoints are immutable, so on a hit the existing edge keeps the -src and\n" +
		"-dst it was created with and the ones given here are ignored. An edge's\n" +
		"value should therefore identify the pair it connects — \"<src>:<dst>:<relation>\"\n" +
		"is the usual shape — because a value that stays the same while its\n" +
		"endpoints are meant to move names two different edges rather than one.\n\n" +
		"-src and -dst are still required: they are what the edge is created from\n" +
		"when the value is new.",
	Notice:  "edge upsert opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeUpsertOpts) {
		fs.StringVar(&o.key, "key", "", "unique property key naming the edge (required)")
		fs.StringVar(&o.value, "value", "", "this edge's value under -key (required)")
		fs.Uint64Var(&o.src, "src", 0, "source node ID (required)")
		fs.Uint64Var(&o.dst, "dst", 0, "destination node ID (required)")
		fs.Var(&o.labels, "label", "edge label (repeatable; at least one required)")
		fs.Float64Var(&o.weight, "weight", 0, "similarity score, for a SimilarTo edge")
		fs.StringVar(&o.props, "props", "", "file whose bytes become the edge's Properties")
		fs.Var(&o.index, "index", "key=value to register in the property index (repeatable)")
	},
	runEdgeUpsert)

func runEdgeUpsert(cx *Context, o *edgeUpsertOpts) (Result, error) {
	var r Result

	if o.src == 0 || o.dst == 0 {
		return r, Usagef("need -src and -dst; they are what the edge is created from " +
			"when -value is new")
	}
	labels, err := edgeTypes(o.labels)
	if err != nil {
		return r, err
	}
	if len(labels) == 0 {
		return r, Usagef("need at least one -label; an edge without one cannot be created")
	}
	index, blob, err := upsertPayload(o.key, o.value, o.props, o.index)
	if err != nil {
		return r, err
	}
	g := cx.Graph()

	if cx.Globals.DryRun {
		held, err := g.EdgesByProperty(o.key, []byte(o.value))
		if err != nil {
			return r, err
		}
		s := r.Section("would upsert")
		s.Add("key", Str(o.key))
		s.Add("value", Str(o.value))
		s.Add("src", ID(o.src))
		s.Add("dst", ID(o.dst))
		s.Add("labels", Strs(o.labels))
		s.Add("properties", Bytes(int64(len(blob))))
		s.Add("index entries", Int(int64(len(index))))
		switch len(held) {
		case 0:
			s.Add("effect", Str("create"))
		case 1:
			s.AddNote("effect", Str("replace"),
				"edge "+strconv.FormatUint(uint64(held[0]), 10)+", keeping its endpoints")
		default:
			s.Add("effect", Str("refuse"))
			r.Find(SevBroken, "unique.violated",
				"%d edges already hold %q under %q, so the key cannot be declared unique",
				len(held), o.value, o.key)
		}
		return r, nil
	}

	if err := g.DeclareUniqueEdgeProperty(o.key); err != nil {
		return r, uniqueRefusal("edge", o.key, err)
	}
	e := &store.Edge{
		Src: store.NodeID(o.src), Dst: store.NodeID(o.dst),
		Labels: labels, Weight: float32(o.weight), Properties: blob,
	}
	id, created, err := g.UpsertEdge(o.key, []byte(o.value), e, index)
	if err != nil {
		return r, err
	}

	s := r.Section(verbFor(created))
	s.Add("edge", ID(uint64(id)))
	s.Add("key", Str(o.key))
	s.Add("value", Str(o.value))
	s.Add("src", ID(o.src))
	s.Add("dst", ID(o.dst))
	s.Add("labels", Strs(o.labels))
	s.Add("properties", Bytes(int64(len(blob))))
	s.Add("index entries", Int(int64(len(index)+1)))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- shared ---

// upsertPayload validates the flags both upserts share and reads the blob.
func upsertPayload(key, value, propsPath string, specs []string) (map[string][]byte, []byte, error) {
	if key == "" || value == "" {
		return nil, nil, Usagef("need -key and -value; they are how the entity is named")
	}
	index, err := parseIndexEntries(specs)
	if err != nil {
		return nil, nil, err
	}
	if _, dup := index[key]; dup {
		// Letting it through would register the key twice, and with a different
		// value it would move the entity's name in the same breath as using it.
		return nil, nil, Usagef("-index must not name %q: the key entry is registered "+
			"for you from -value", key)
	}
	blob, err := readBlob(propsPath)
	if err != nil {
		return nil, nil, err
	}
	return index, blob, nil
}

// uniqueRefusal turns a failed declaration into a message that says what to run
// next, since the operator is one command away from the full list.
func uniqueRefusal(kind, key string, err error) error {
	var v *store.UniqueViolationsError
	if !errors.As(err, &v) {
		return err
	}
	edge := ""
	if kind == "edge" {
		edge = " -edge"
	}
	return fmt.Errorf("%w\n  `graphene debug unique -key %s%s <dir>` lists every one",
		err, key, edge)
}

func verbFor(created bool) string {
	if created {
		return "created"
	}
	return "replaced"
}

// propValue renders an index value. Values are caller-encoded bytes, and the
// ones this tool writes are text — so text is shown as text and anything else
// falls back to hex rather than putting control bytes on a terminal.
func propValue(b []byte) Value {
	if len(b) == 0 {
		return Nil()
	}
	if utf8.Valid(b) && strings.IndexFunc(string(b), func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) < 0 {
		return Str(string(b))
	}
	return Hex(b)
}

func joinEdgeIDs(ids []store.EdgeID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return strings.Join(parts, ", ")
}

func joinIDs(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(id, 10)
	}
	return strings.Join(parts, ", ")
}
