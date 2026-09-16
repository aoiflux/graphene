//go:build !linux && !windows && !darwin

package disk

// Every other platform, which answers neither question.
//
// Both readers return false rather than a plausible-looking zero, and that is
// the whole content of this file. A caller that derives a budget from an
// unanswerable figure derives zero, zero means "unset" throughout Options, and a
// store would then open with no bound at all while StorageStats reported one --
// an option asked for and not held, which is the failure StorageStats exists to
// make visible rather than to commit.
//
// So: no ceiling is discovered here, Options.MemoryBudget keeps whatever the
// caller set, and the batch caps fall back to their own defaults rather than to
// a derived figure. The engine is bounded on this platform; it is simply bounded
// by arithmetic the caller supplied rather than by arithmetic it discovered.

func readSysMemory() (sysMemory, bool) { return sysMemory{}, false }

func readSysCeiling() (sysCeiling, bool) { return sysCeiling{}, false }
