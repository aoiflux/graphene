// Command graphene inspects a Graphene store from a shell.
//
// # What it opens, and what it does not
//
// Most inspection reads graphene.csr, graphene.wal and the ledger files
// directly, and never opens the store. That is deliberate. Opening replays the
// log, rebuilds indexes and takes a handle on the WAL — and the moment you most
// want to look at a store is the moment something has gone wrong with it and a
// live process is probably still attached. An inspector that cannot be used
// then is not much of an inspector.
//
// The commands that do open say so on stderr before they do, and each declares
// in the registry which lock it takes. A read-only open takes a *shared* lock:
// any number of them run at once, and all are refused — with the holder's PID —
// while a writer has the store. A refusal means the store is busy, not broken.
//
// # The safety doctrine
//
// There is no repair and no truncate. A tool that is safe to point at
// production is worth more than one that can also fix things, and adding a
// mutation should be a deliberate decision rather than a convenience.
// `maintenance repair`, `maintenance vacuum` and `wal compact` are registered
// commands whose only job is to refuse and to name what does exist — an
// operator told a tool cannot help them goes looking for one that can, and the
// one they find will not have this tool's caution.
//
// Everything that writes is argued rather than assumed, in three kinds:
//
//   - append-only, and so not gated: anchor add, assertion add, grant
//     add/revoke. Nothing they write can alter or remove what is already
//     recorded, so there is nothing for a -confirm to protect — and a gate on a
//     command that cannot lose anything teaches the habit of typing -confirm
//     without reading it.
//   - writing somewhere else entirely: backup, export, import. See the register
//     at the top of cmd_transfer.go.
//   - gated behind -confirm, with -dry-run: node/edge create and delete,
//     redaction apply, maintenance compact and reindex, migrate. See the
//     registers at the top of cmd_write.go and cmd_restore.go.
//
// A dry run cannot write because the framework downgrades the open mode before
// the store is acquired, not because a handler remembered to check a flag.
//
// # Exit status
//
// Non-zero only when something is actually broken. An incomplete account — a
// store that was never signed, audited, or set to retain history — is reported
// as findings and exits zero, because most stores are legitimately not fully
// provisioned and a tool that exited non-zero on that would be useless in the
// scripts that most want it. See errors.go, where that policy is now one
// function rather than seven scattered os.Exit calls.
//
// # Where things are
//
// The command list is not repeated here. It used to be, in three places — this
// comment, the usage text, and the dispatch switch — and all three had drifted
// apart; this one described eight of the fifteen commands that existed.
// `graphene help` prints it from the registry in commands.go, which is the only
// list there is.
//
//	run.go         one invocation, start to finish
//	registry.go    the Command type; dispatch, help and completion all read it
//	commands.go    the registry itself — the only command list there is
//	dispatch.go    argv -> a command, including the legacy spellings
//	result.go      the document both renderers walk
//	errors.go      verdicts, faults, and the exit-status policy
//	context.go     what a handler gets, and who owns the store it uses
//	selectors.go   flags -> queries: types, property filters, budgets
//	config.go      the config file, and profile names for store directories
//	metrics.go     -metrics, and why it is a flag rather than a `debug profile`
package main

import "os"

func main() {
	initRegistry(registry)
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
