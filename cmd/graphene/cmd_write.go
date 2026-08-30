package main

// node create, node delete, edge create, edge delete.
//
// # The register
//
// The doctrine at the top of main.go says a mutation should be a deliberate
// decision rather than a convenience, so each of these is argued rather than
// assumed:
//
//   create   adds a record and touches nothing that was already there. The
//            only thing it can lose is the ID space, which is not a loss:
//            IDs are never reused, so a create that is later deleted leaves a
//            gap and never a collision.
//
//   delete   is the one command in this tool that destroys content, and the
//            only one whose gate is protecting something real. It is also
//            *unattributed*: nothing records who did it or why, and a node
//            deletion cascades to every edge incident to the node.
//
// There is no `node assert`. The brief asked for one, and the engine has no
// per-node attestation to write: disk.AttestNode *reads*, pairing the image's
// own signed attestation with a node's inclusion proof to make a transferable
// claim. The signature is over the snapshot root and is produced at compaction.
// `node verify` (cmd_verify.go) checks both halves of that claim, and
// `provenance export` packages it for someone who does not have the store.
//
// # delete versus redact
//
// For a store that is holding evidence, `redaction apply` is almost always the
// command wanted instead. Both remove the entity; the difference is that a
// redaction is signed into a hash-chained ledger, leaves a tombstone under the
// snapshot root, and keeps the version hash of what was destroyed — so an
// absence can be shown to be a lawful removal rather than an unexplained hole.
// A delete leaves an absence and nothing else.
//
// `node delete` says so every time it runs, and not only in the help: the
// operator most likely to reach for it is the one who has not read the help.

import (
	"flag"
	"fmt"
	"os"

	"github.com/aoiflux/graphene/store"
)

// --- node create ---

type nodeCreateOpts struct {
	labels stringList
	props  string
	index  stringList
}

var nodeCreate = cmd(Command{
	Group: "node", Name: "create", Usage: "<dir>",
	Short: "add a node",
	Long: "-label may be repeated; a node carries one or more labels and needs at\n" +
		"least one.\n\n" +
		"-props reads a file verbatim into the node's Properties. The engine treats\n" +
		"that blob as opaque — it is msgpack by convention and the schema belongs\n" +
		"to whoever built the store — so nothing here validates it, and nothing\n" +
		"here will decode it back.\n\n" +
		"-index key=value registers an entry in the property index, which is what\n" +
		"makes `node list -prop key=value` find this node. Index entries are\n" +
		"decoupled from Properties by design: the engine cannot tell that a value\n" +
		"inside an opaque blob is the one you want queryable, so you say which.",
	Notice:  "node create opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *nodeCreateOpts) {
		fs.Var(&o.labels, "label", "node label (repeatable; at least one required)")
		fs.StringVar(&o.props, "props", "", "file whose bytes become the node's Properties")
		fs.Var(&o.index, "index", "key=value to register in the property index (repeatable)")
	},
	runNodeCreate)

func runNodeCreate(cx *Context, o *nodeCreateOpts) (Result, error) {
	var r Result

	labels, err := nodeTypes(o.labels)
	if err != nil {
		return r, err
	}
	if len(labels) == 0 {
		return r, Usagef("need at least one -label; a node without one cannot be created")
	}
	blob, err := readBlob(o.props)
	if err != nil {
		return r, err
	}
	index, err := parseIndexEntries(o.index)
	if err != nil {
		return r, err
	}

	if cx.Globals.DryRun {
		s := r.Section("would create")
		s.Add("labels", Strs(o.labels))
		s.Add("properties", Bytes(int64(len(blob))))
		s.Add("index entries", Int(int64(len(index))))
		return r, nil
	}

	n := &store.Node{Labels: labels, Properties: blob}
	id, err := cx.Graph().AddNode(n)
	if err != nil {
		return r, err
	}
	if len(index) > 0 {
		if err := cx.Graph().IndexNodeProperties(id, index); err != nil {
			// The node exists and the index entries do not. Said plainly,
			// because the store is now in a state the operator has to act on:
			// queries will not find a node that is there.
			return r, fmt.Errorf("node %d was created and its index entries were not: %w\n"+
				"  re-register them with the API, or delete the node and start again", id, err)
		}
	}

	s := r.Section("created")
	s.Add("node", ID(uint64(id)))
	s.Add("labels", Strs(o.labels))
	s.Add("properties", Bytes(int64(len(blob))))
	s.Add("index entries", Int(int64(len(index))))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- edge create ---

type edgeCreateOpts struct {
	src    uint64
	dst    uint64
	labels stringList
	weight float64
	props  string
	index  stringList
}

var edgeCreate = cmd(Command{
	Group: "edge", Name: "create", Usage: "<dir>",
	Short: "add an edge between two existing nodes",
	Long: "Both endpoints must already exist. The edge is stored directed, from\n" +
		"-src to -dst, and the endpoints are immutable: to reconnect an edge you\n" +
		"delete it and add another. There is no flag for a bidirectional edge\n" +
		"because there is no such record — direction is a question you ask at\n" +
		"query time, with -direction both.\n\n" +
		"-weight is meaningful for a SimilarTo edge, where it is the similarity\n" +
		"score, and zero for everything else.",
	Notice:  "edge create opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeCreateOpts) {
		fs.Uint64Var(&o.src, "src", 0, "source node ID (required)")
		fs.Uint64Var(&o.dst, "dst", 0, "destination node ID (required)")
		fs.Var(&o.labels, "label", "edge label (repeatable; at least one required)")
		fs.Float64Var(&o.weight, "weight", 0, "similarity score, for a SimilarTo edge")
		fs.StringVar(&o.props, "props", "", "file whose bytes become the edge's Properties")
		fs.Var(&o.index, "index", "key=value to register in the property index (repeatable)")
	},
	runEdgeCreate)

func runEdgeCreate(cx *Context, o *edgeCreateOpts) (Result, error) {
	var r Result

	if o.src == 0 || o.dst == 0 {
		return r, Usagef("need -src and -dst")
	}
	labels, err := edgeTypes(o.labels)
	if err != nil {
		return r, err
	}
	if len(labels) == 0 {
		return r, Usagef("need at least one -label; an edge without one cannot be created")
	}
	blob, err := readBlob(o.props)
	if err != nil {
		return r, err
	}
	index, err := parseIndexEntries(o.index)
	if err != nil {
		return r, err
	}

	if cx.Globals.DryRun {
		// Checked here rather than left to AddEdge, because a dry run that says
		// nothing about whether the endpoints exist has answered the least
		// interesting half of the question.
		_, missing, gerr := cx.Graph().GetNodes([]store.NodeID{
			store.NodeID(o.src), store.NodeID(o.dst)})
		if gerr != nil {
			return r, gerr
		}
		s := r.Section("would create")
		s.Add("src", ID(o.src))
		s.Add("dst", ID(o.dst))
		s.Add("labels", Strs(o.labels))
		s.Add("weight", Float(o.weight))
		s.Add("properties", Bytes(int64(len(blob))))
		missingNote(s, "endpoints that do not exist", uint64s(missing))
		if len(missing) > 0 {
			r.Find(SevWarn, "edge.missing_endpoint",
				"%d endpoint(s) do not exist; the create would be refused", len(missing))
		}
		return r, nil
	}

	e := &store.Edge{
		Src: store.NodeID(o.src), Dst: store.NodeID(o.dst),
		Labels: labels, Weight: float32(o.weight), Properties: blob,
	}
	id, err := cx.Graph().AddEdge(e)
	if err != nil {
		return r, err
	}
	if len(index) > 0 {
		if err := cx.Graph().IndexEdgeProperties(id, index); err != nil {
			return r, fmt.Errorf("edge %d was created and its index entries were not: %w", id, err)
		}
	}

	s := r.Section("created")
	s.Add("edge", ID(uint64(id)))
	s.Add("src", ID(o.src))
	s.Add("dst", ID(o.dst))
	s.Add("labels", Strs(o.labels))
	s.Add("weight", Float(o.weight))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- node delete / edge delete ---

// deleteWarning is printed by both delete commands, every time, as a notice.
// The operator most likely to reach for a delete on an evidence store is the
// one who has not read the help.
const deleteWarning = "this removal is unattributed: nothing records who did it or why. " +
	"`redaction apply` removes the same thing with a signed, chained record and a " +
	"tombstone under the snapshot root"

type deleteOpts struct{ id uint64 }

var nodeDelete = cmd(Command{
	Group: "node", Name: "delete", Usage: "<dir>",
	Short: "remove a node and every edge incident to it",
	Long: "The only command in this tool that destroys content without recording\n" +
		"anything about the decision. It cascades: every edge touching the node\n" +
		"goes with it, so no edge is left pointing at a node that is not there.\n\n" +
		"For a store holding evidence this is almost certainly the wrong command.\n" +
		"`redaction apply` removes the same node, and does it with an attributed,\n" +
		"signed, hash-chained record and a tombstone committed under the snapshot\n" +
		"root — which is what lets an absence be shown to be a lawful removal\n" +
		"rather than an unexplained hole.\n\n" +
		"-dry-run reports the cascade before anything is destroyed.\n\n" +
		"IDs are never reused. A deleted ID leaves a gap and is never handed out\n" +
		"again, so a reference to it stays unambiguous.",
	Notice:  "node delete opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *deleteOpts) {
		fs.Uint64Var(&o.id, "id", 0, "node ID (required)")
	},
	func(cx *Context, o *deleteOpts) (Result, error) { return runDelete(cx, o, true) })

var edgeDelete = cmd(Command{
	Group: "edge", Name: "delete", Usage: "<dir>",
	Short: "remove an edge",
	Long: "Unattributed, like `node delete`, and the same argument applies: for an\n" +
		"evidence store `redaction apply -scope edge` records the removal instead\n" +
		"of merely performing it.\n\n" +
		"Nothing cascades — an edge has no dependants.",
	Notice:  "edge delete opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *deleteOpts) {
		fs.Uint64Var(&o.id, "id", 0, "edge ID (required)")
	},
	func(cx *Context, o *deleteOpts) (Result, error) { return runDelete(cx, o, false) })

func runDelete(cx *Context, o *deleteOpts, isNode bool) (Result, error) {
	var r Result
	noun := "edge"
	if isNode {
		noun = "node"
	}
	if o.id == 0 {
		return r, Usagef("need -id <%s>", noun)
	}
	g := cx.Graph()

	if cx.Globals.DryRun {
		s := r.Section("would delete")
		s.Add(noun, ID(o.id))

		if isNode {
			// The cascade is the cost, and it is only knowable before the fact.
			edges, err := g.EdgesOf(store.NodeID(o.id), store.DirectionBoth, nil)
			if err != nil {
				return r, err
			}
			s.Add("edges cascaded", Int(int64(len(edges))))
			if len(edges) > 0 {
				edgeTable(&r, "edges that would go with it", edges, Paging{})
			}
		}
		r.Notice("%s", deleteWarning)
		return r, nil
	}

	var err error
	if isNode {
		err = g.DeleteNode(store.NodeID(o.id))
	} else {
		err = g.DeleteEdge(store.EdgeID(o.id))
	}
	if err != nil {
		return r, err
	}

	s := r.Section("deleted")
	s.Add(noun, ID(o.id))
	s.AddNote("the ID", Str("is retired"), "(never reused, so a reference to it stays unambiguous)")
	r.Notice("%s", deleteWarning)
	r.Verdict = VerdictVerified
	return r, nil
}

// --- shared ---

// readBlob reads a properties file, or returns nil for no file.
func readBlob(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("-props %s: %w", path, err)
	}
	return b, nil
}

// parseIndexEntries reads repeated `-index key=value` into the map the index
// registration takes.
//
// Plain key=value with no operator grammar, deliberately. -prop describes a
// query and needs operators; -index describes one value being registered, and
// borrowing the query grammar here would suggest you could register a prefix.
func parseIndexEntries(specs []string) (map[string][]byte, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(specs))
	for _, spec := range specs {
		k, v, ok := cutKeyValue(spec)
		if !ok {
			return nil, Usagef("-index %q: want key=value", spec)
		}
		if _, dup := out[k]; dup {
			// Silently keeping the last would register one value and look like
			// it registered two.
			return nil, Usagef("-index %q: key %q was given twice", spec, k)
		}
		out[k] = []byte(v)
	}
	return out, nil
}

func cutKeyValue(spec string) (string, string, bool) {
	for i := 0; i < len(spec); i++ {
		if spec[i] == '=' {
			if i == 0 {
				return "", "", false
			}
			return spec[:i], spec[i+1:], true
		}
	}
	return "", "", false
}
