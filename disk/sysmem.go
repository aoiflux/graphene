package disk

// What the operating system will tell us about our own memory, and about the
// ceiling we are already running under.
//
// # Two questions, deliberately separated
//
// "How much are we holding" and "how much are we allowed" are different
// questions with different answers on every platform, and conflating them is how
// a measurement rig ends up reporting a comfortable pass under a limit that
// never bound anything -- the failure tests/ceiling_other_test.go refuses to
// commit. So they are two functions returning two types, and each says how it
// knows.
//
// # Why the engine reads a limit and does not impose one
//
// An operating system ceiling enforces; it does not control. Exceeding one is
// SIGKILL from the cgroup OOM killer on linux, or a refused commit and a Go
// runtime "fatal error: out of memory" on windows. Neither is recoverable: an
// allocation failure in Go is a runtime throw, not a panic, so nothing the
// engine or its caller writes can catch it.
//
// Graphene is a library inside someone else's process. A self-imposed hard
// ceiling would mean the engine killing the host application to protect a
// figure the host never asked for. So what is read here feeds Options.MemoryBudget
// and the batch caps -- bounds the engine enforces by *refusing work*, which a
// caller can handle -- and nothing here ever calls the kernel to install a limit.
//
// # What each platform can actually answer
//
//	                  ceiling                              anon    resident  peak
//	linux    cgroup v2 memory.max, v1 limit_in_bytes,      yes     yes       yes
//	         else MemTotal
//	windows  Job Object JOB_OBJECT_LIMIT_JOB_MEMORY,       approx  yes       yes
//	         else GlobalMemoryStatusEx
//	darwin   sysctl hw.memsize only -- machine size, not   no      no        yes
//	         a limit on us
//	other    nothing
//
// Darwin is best-effort and says so. There is no per-process memory ceiling on
// it that constrains the class this engine cares about: RLIMIT_AS counts the
// mapped image's address space, which is the term the memory programme
// deliberately moved memory *into*, and RLIMIT_RSS is unenforced on every modern
// kernel. Reporting the machine's size is honest and useful -- it is a figure to
// take a fraction of -- and it is not a limit anyone is enforcing, which is what
// sysCeiling.Enforced exists to say.
//
// # Every field can be unknown, and unknown must not read as zero
//
// A caller deriving a budget from an unanswerable figure would derive zero, and
// zero means "unset" throughout Options. So the readers return a bool, the
// structs carry Split and Current, and a platform that cannot answer half the
// question says which half. This is the same argument tests/rss_darwin_test.go
// makes for rssCurrent, and it is here for the same reason: "the platform could
// not check" must not read the same as "the platform checked".

// sysMemory is this process's memory as the kernel reports it.
type sysMemory struct {
	// Anon is memory that cannot be reclaimed without killing the process --
	// the Go heap, its stacks and the runtime's own arenas. This is the class
	// the 2 GiB directive bounds, because it is the class that OOM-kills.
	//
	// On windows this is the commit charge, which overstates residency by
	// whatever the runtime holds committed and untouched. The error is a roughly
	// fixed process overhead rather than something that scales with the mapping,
	// and it is one-sided in the safe direction; tests/rss_windows_test.go:20-42
	// carries the measurement.
	Anon uint64

	// Resident is every resident page, anonymous and file-backed alike. The
	// difference from Anon is the mapped image's page cache, which is
	// reclaimable -- the kernel drops it under pressure rather than killing us.
	//
	// Reclaimable is not free: those pages are charged to a cgroup, and
	// reclaiming them during an ingest is a latency event in the middle of the
	// operation being bounded. It is reported so it can be bounded, not because
	// it is as dangerous as Anon.
	Resident uint64

	// Peak is the high-water mark of the resident set for the process lifetime.
	// Never resets, so it answers "did we ever come close" and not "are we close
	// now".
	Peak uint64

	// Split is false when Anon and Resident could not be told apart. Both carry
	// the total in that case rather than one of them carrying zero.
	Split bool

	// Current is false when only Peak is a real reading -- darwin, where the
	// current size needs Mach calls unreachable without cgo. A caller must not
	// read a zero Resident on such a platform as "this process holds nothing".
	Current bool
}

// sysCeiling is the memory limit this process is running under, as the kernel
// reports it rather than as anyone assumed.
type sysCeiling struct {
	// Bytes is the limit. Zero with ok=false means nothing was readable.
	Bytes uint64

	// Source names the instrument, for a caller that has to explain a figure it
	// did not choose. StorageStats reports it verbatim.
	Source string

	// Enforced is true when exceeding Bytes ends the process, and false when it
	// is merely how much the machine has. The distinction decides how much of
	// the figure it is safe to take: all of a machine is not ours to use, and
	// all of a cgroup is not survivable to use either, but for opposite reasons.
	Enforced bool
}

// sysCeilingFraction is how much of a discovered ceiling becomes the default
// budget when the caller set none.
//
// Not a tuned figure yet -- Phase 0 of docs/PLAN_BOUNDED_INGEST.md is what picks
// it -- and deliberately conservative until it is measured. The engine is one
// tenant of the process it is linked into, and a library that helps itself to
// most of a container's memory because it found the number has substituted its
// own judgement for the caller's.
//
// It applies to an enforced ceiling. An unenforced one -- machine size -- is a
// much weaker signal and takes sysCeilingFractionMachine instead.
const (
	sysCeilingFraction        = 50 // percent of an enforced ceiling
	sysCeilingFractionMachine = 25 // percent of machine size, which is not ours alone
)

// budgetFromCeiling turns a discovered ceiling into a default MemoryBudget.
//
// Separate from the readers so the arithmetic is testable without a kernel: the
// platform files answer what the kernel said, and this decides what to do about
// it. A caller who set Options.MemoryBudget explicitly never reaches here --
// see checkOpenBudget -- because a discovered figure must never override a
// stated one, in either direction.
// resolveMemoryBudget applies Options.DiscoverMemoryBudget, returning the
// options to open with and the provenance of the figure in them.
//
// Called once, at Open, before checkOpenBudget -- so the budget the open is
// refused against is the derived one, and a caller who asked for discovery gets
// it on the very first operation rather than on the first compaction.
//
// The three outcomes are all deliberate. Discovery not asked for, or a budget
// already stated, returns the options unchanged with an empty source: nothing
// was discovered, and reporting a source for a figure the caller supplied would
// misattribute it. Discovery asked for on a platform that cannot answer also
// returns unchanged, leaving MemoryBudget at zero -- there is no plausible
// figure to substitute, and inventing one would be an option asked for and not
// held. Discovery that succeeds returns the derived budget and names the
// instrument that produced it.
func resolveMemoryBudget(opts Options) (Options, string) {
	if !opts.DiscoverMemoryBudget || opts.MemoryBudget > 0 {
		return opts, ""
	}
	c, ok := readSysCeiling()
	if !ok {
		return opts, ""
	}
	budget := budgetFromCeiling(c)
	if budget <= 0 {
		return opts, ""
	}
	opts.MemoryBudget = budget
	return opts, c.Source
}

func budgetFromCeiling(c sysCeiling) int64 {
	pct := uint64(sysCeilingFractionMachine)
	if c.Enforced {
		pct = sysCeilingFraction
	}
	// Saturate rather than wrap. A ceiling large enough to overflow this is not
	// a ceiling worth deriving from, and reporting a small budget for a huge
	// machine is the safe direction to be wrong in.
	if c.Bytes == 0 || c.Bytes > 1<<62 {
		return 0
	}
	return int64(c.Bytes / 100 * pct)
}
