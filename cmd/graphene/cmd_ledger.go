package main

// redaction list, grant list, debug indexes.
//
// The two ledger dumps read their files directly rather than opening the store.
// A ledger is a standalone hash-chained file with no dependency on replayed
// state, and the moment you most want to read one is the moment somebody is
// asking what was removed from a store that is probably still in use.
//
// Both used to end with os.Exit(1) on a broken chain. They return a verdict
// now, which is what lets the framework close what it opened — see context.go.

import (
	"flag"
	"strings"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// --- redaction list ---

type redactionOpts struct {
	node uint64
	edge uint64
	keys pubkeyList
}

var redactionList = cmd(Command{
	Group: "redaction", Name: "list", Aliases: []string{"redactions"},
	Usage: "<dir>", Short: "the ledger of attributed removals: who, when, why",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *redactionOpts) {
		fs.Uint64Var(&o.node, "node", 0, "show only records for this node ID")
		fs.Uint64Var(&o.edge, "edge", 0, "show only records for this edge ID")
		fs.Var(&o.keys, "pubkey",
			"ID:HEX Ed25519 public key to verify signatures with (repeatable)")
	},
	runRedactionList)

func runRedactionList(cx *Context, o *redactionOpts) (Result, error) {
	var r Result

	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}

	records, err := disk.ReadRedactions(cx.Target)
	if err != nil {
		return r, err
	}
	if len(records) == 0 {
		r.Section("").Add("redactions", Str("none recorded"))
		return r, nil
	}

	// Verified over the whole ledger before anything is filtered: a chain break
	// anywhere makes every record suspect, including the ones asked for.
	chainErr := disk.VerifyRedactionChain(records, verifier)

	list := r.Notes("records")
	shown := 0
	for _, rec := range records {
		// A node filter must not match an edge record, which leaves NodeID zero.
		if o.node != 0 && (rec.EdgeID != 0 || uint64(rec.NodeID) != o.node) {
			continue
		}
		if o.edge != 0 && uint64(rec.EdgeID) != o.edge {
			continue
		}
		shown++
		list.Line("%s", rec)
		list.Line("    scope        %s", rec.Scope)
		list.Line("    version hash %x", rec.VersionHash)
		if rec.SurvivingHash != (merkle.Hash{}) {
			list.Line("    survives as  %x", rec.SurvivingHash)
		}
		if rec.PriorPropertiesHash != (merkle.Hash{}) {
			list.Line("    prior props  %x", rec.PriorPropertiesHash)
		}
		if len(rec.CascadedEdges) > 0 {
			note := ""
			if len(rec.CascadedHashes) != len(rec.CascadedEdges) {
				// Worth saying: without hashes these edges are named but not
				// identified, so the image carries no tombstone for them.
				note = "  (unidentified: no tombstones in the image)"
			}
			list.Line("    edges        %v%s", rec.CascadedEdges, note)
		}
		if len(rec.Signature) > 0 {
			list.Line("    signed by    key %d", rec.KeyID)
		} else {
			list.Line("    unsigned")
		}
	}

	summarizeLedger(&r, "redaction", len(records), shown,
		o.node != 0 || o.edge != 0, chainErr, verifier)
	return r, nil
}

// --- grant list ---

type grantOpts struct {
	actor uint64
	keys  pubkeyList
}

var grantList = cmd(Command{
	Group: "grant", Name: "list", Aliases: []string{"grants"},
	Usage: "<dir>", Short: "role grants, and the capabilities they imply",
	Long: "RBAC here is recorded and never enforced by the engine. This report says\n" +
		"what was granted, not what was prevented — do not read it as an access\n" +
		"control decision that the store made.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *grantOpts) {
		fs.Uint64Var(&o.actor, "actor", 0, "show only records concerning this actor")
		fs.Var(&o.keys, "pubkey",
			"ID:HEX Ed25519 public key to verify signatures with (repeatable)")
	},
	runGrantList)

func runGrantList(cx *Context, o *grantOpts) (Result, error) {
	var r Result

	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}

	records, err := disk.ReadGrants(cx.Target)
	if err != nil {
		return r, err
	}
	if len(records) == 0 {
		r.Section("").Add("role grants", Str("none recorded"))
		return r, nil
	}

	// Verified over the whole ledger before filtering: a break anywhere makes
	// every record suspect, including the ones asked for.
	chainErr := disk.VerifyGrantChain(records, verifier)

	list := r.Notes("records")
	shown := 0
	for _, g := range records {
		if o.actor != 0 && g.Subject != o.actor && g.GrantedBy != o.actor {
			continue
		}
		shown++
		list.Line("%s", g)
		if len(g.Signature) > 0 {
			list.Line("    signed by    key %d", g.KeyID)
		} else {
			list.Line("    unsigned")
		}
	}

	// The derived state, which is the point of the command: capabilities are a
	// function of this ledger and of nothing else, and a reader asking "what
	// could this actor do" should not have to replay grants and revocations in
	// their head — that replay is exactly where a mistake would be invisible.
	held := disk.CapabilitiesFrom(records)
	caps := r.Notes("capabilities now held")
	if len(held) == 0 {
		caps.Line("(none — every grant has been revoked)")
	}
	for _, a := range sortedActors(held) {
		if o.actor != 0 && a != o.actor {
			continue
		}
		caps.Line("actor %d: %s", a, held[a])
	}

	summarizeLedger(&r, "grant", len(records), shown, o.actor != 0, chainErr, verifier)
	return r, nil
}

// summarizeLedger writes the closing account shared by the two ledger dumps.
//
// The chain covers the whole ledger, so the count reported alongside its
// verdict has to be the whole ledger too. Saying "2 records, chain intact"
// after a filter would attach the verdict to the wrong set.
func summarizeLedger(r *Result, noun string, total, shown int, filtered bool,
	chainErr error, verifier store.Verifier) {

	s := r.Section("")
	if filtered {
		s.Addf("shown", "%d of %d %s in the ledger", shown, total, pluralWord(total, noun))
	} else {
		s.Addf("ledger", "%d %s", total, pluralWord(total, noun))
	}

	if chainErr != nil {
		// No key material is needed for this: signatures may be unchecked, but
		// the hash chain is not, and a break in it is the loud case.
		s.Add("hash chain", Str("BROKEN"))
		r.Find(SevBroken, noun+"s.chain_broken", "the hash chain is broken: %v", chainErr)
		return
	}
	s.AddNote("hash chain", Str("intact"), "("+signatureNote(verifier)+")")
	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
}

// pluralWord is deliberately small and deliberately not a library. It handles
// the three nouns this package actually pluralises — "record", "grant", "audit
// entry" — and an -y ending, which is the one that produced "audit entrys".
func pluralWord(n int, noun string) string {
	switch {
	case n == 1:
		return noun
	case strings.HasSuffix(noun, "y"):
		return noun[:len(noun)-1] + "ies"
	default:
		return noun + "s"
	}
}
