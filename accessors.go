package graphene

import "github.com/aoiflux/graphene/disk"

// This file forwards facts that live on disk.Store and have no home on
// store.GraphStore, in the shape StorageStats already established: the value,
// and whether the backend could answer.
//
// # Why the second return value, and not a plain one
//
// Several of these have a zero value that reads like an answer. A plain false
// from ReadOnly says "this store accepts writes"; a plain false from
// LockEnforced says "nothing excludes a second process"; a zero LockMode says
// "exclusive". None of those is what is true of the in-memory backend, which has
// no read-only posture, no lock and no directory to hold one over. The question
// does not apply, and saying so is the same discipline disk/lock_unsupported.go
// applies to a platform with no locking primitive: what it must not do is
// pretend.
//
// # Why these six and not the other seventy-seven
//
// *disk.Store has about 133 exported methods and Graph does not carry most of
// them, for the reason Forensics states: forwarding a call is a place for the
// façade's version to drift from the real one. What these six have in common is
// that they are the questions an operator asks about a running store rather than
// operations performed on it -- what is it holding, can it be written, what did
// it recover from, what does it enforce -- and every one of them was reachable
// only by type-asserting the embedded interface back to the concrete backend,
// which is the thing this package exists to save a caller from doing.
//
// disk.Store.SetSyncOnCommit is deliberately not here. It is a mutator, and a
// durability switch that silently does nothing on a backend without durability
// is worse than one a caller had to reach for the concrete type to find.

type residentEstimator interface {
	EstimateResident() disk.ResidentEstimate
}

type readOnlyReporter interface {
	ReadOnly() bool
	RecoveredFromUncleanShutdown() bool
}

type declarationReporter interface {
	Declarations() disk.Catalogue
	DroppedDeclarations() []disk.DroppedDeclaration
}

type lockReporter interface {
	LockMode() disk.LockMode
	LockEnforced() bool
}

// EstimateResident reports what the backend is holding, term by term, and
// whether it could say.
//
// StorageStats.EstimatedResidentBytes is the same total as one number. This is
// the breakdown, and the breakdown is what a caller sizing a deployment against
// a ceiling actually needs: the total says whether there is a problem, and the
// terms say which of the things you can change is causing it.
//
// Mapped and MappedIndex are page cache rather than heap and are deliberately
// not part of Total. Read disk.ResidentEstimate before comparing a total against
// a memory limit -- it is retained heap, which is a floor on resident set size
// and not the same figure an operating system reports.
//
// Taken under the store read lock, in time bounded by the number of declared
// keys rather than by the size of the store, so a periodic check is affordable.
// AutoCompact polls it.
func (g *Graph) EstimateResident() (disk.ResidentEstimate, bool) {
	e, ok := g.GraphStore.(residentEstimator)
	if !ok {
		return disk.ResidentEstimate{}, false
	}
	return e.EstimateResident(), true
}

// ReadOnly reports whether the backend refuses mutations, and whether it has a
// posture to report.
//
// True for a graph from OpenReadOnly, and for one from OpenWithOptions with
// disk.Options.ReadOnly set. It is true for an OpenLive graph too, which is
// worth stating because that one holds no lock: taking no lock and refusing no
// mutation are separate properties, and a live reader has the first without the
// second. Every mutating call on a read-only store returns disk.ErrReadOnly,
// including Compact.
//
// ok is false on the in-memory backend. That is not the same fact as "this store
// accepts writes", which is why it is not reported as one.
func (g *Graph) ReadOnly() (readOnly, ok bool) {
	r, ok := g.GraphStore.(readOnlyReporter)
	if !ok {
		return false, false
	}
	return r.ReadOnly(), true
}

// RecoveredFromUncleanShutdown reports that the process which last held this
// store exclusively did not close it, and whether the backend keeps that
// evidence.
//
// The store opened anyway, and that is not a compromise -- WAL replay is
// crash-safe by construction, and refusing would make every killed process an
// incident needing a human. What this exposes is the decision the engine cannot
// take for a caller: whether *this* store, holding *this* evidence, is one where
// an unclean restart warrants VerifyIndexes, a reopen under
// disk.Options.VerifyOnOpen, or an escalation.
//
// Always false on a read-only store, which never claims the directory and so
// cannot have been the holder that left it dirty. With disk.Options.Audit on,
// the same fact is recorded durably as disk.AuditUncleanRestart; this is the
// in-process view of it.
func (g *Graph) RecoveredFromUncleanShutdown() (recovered, ok bool) {
	r, ok := g.GraphStore.(readOnlyReporter)
	if !ok {
		return false, false
	}
	return r.RecoveredFromUncleanShutdown(), true
}

// Declarations reports the constraints this store currently has in force, and
// whether the backend persists any.
//
// OrderedProperties, UniqueProperties and CompositeProperties answer one part of
// this each; the catalogue is all of them as one value, read from the same
// declaration state, so a caller checking what a store enforces cannot see three
// figures taken at three moments.
//
// This is the declared set, which is not the set a projection reads. A key that
// was indexed with IndexNodeProperty but never declared is absent here and
// perfectly queryable. See DroppedDeclarations for what an open declined.
func (g *Graph) Declarations() (disk.Catalogue, bool) {
	d, ok := g.GraphStore.(declarationReporter)
	if !ok {
		return disk.Catalogue{}, false
	}
	return d.Declarations(), true
}

// DroppedDeclarations reports the constraints the last open declined to apply
// because the data did not satisfy them, and whether the backend has a
// catalogue at all.
//
// This is the whole of what disk.ConstraintDrop trades away. A store opened that
// way comes up enforcing less than its catalogue asks for, and does it without
// failing the open -- which is the behaviour a bulk ingest wants and an
// unpleasant surprise for anyone who assumed a declaration was a guarantee.
// Always empty under disk.ConstraintRefuse, which fails the open instead, and
// always empty on a store whose data satisfied everything declared.
//
// The result is a copy; the reasons inside it are the original errors, so
// errors.As reaches *store.UniqueViolationsError and
// *store.EdgeCardinalityViolationsError.
func (g *Graph) DroppedDeclarations() ([]disk.DroppedDeclaration, bool) {
	d, ok := g.GraphStore.(declarationReporter)
	if !ok {
		return nil, false
	}
	return d.DroppedDeclarations(), true
}

// LockState reports which process-level lock this store holds, whether the
// platform enforces it, and whether the backend takes one at all.
//
// The two facts come back together because neither means anything without the
// other. disk.LockExclusive says this process asked to be the only writer;
// enforced says whether anything made that true. On solaris, aix, plan9 and
// js/wasm the standard library offers no file-locking primitive, the store opens
// regardless, and the mode is exactly what it would be on Linux while nothing
// excludes a second process. A caller reading the mode alone would conclude the
// opposite of the truth, which is why this does not offer it alone.
//
// disk.LockNone is a different thing again and is not a platform property: it is
// OpenLive, a caller on a locking platform asking not to participate, having
// given up the guarantee that no writer is running. enforced is still true
// there -- the lock works, this store simply did not take one.
//
// ok is false on the in-memory backend, which has no directory to hold a lock
// over.
func (g *Graph) LockState() (mode disk.LockMode, enforced, ok bool) {
	l, ok := g.GraphStore.(lockReporter)
	if !ok {
		return disk.LockExclusive, false, false
	}
	return l.LockMode(), l.LockEnforced(), true
}
