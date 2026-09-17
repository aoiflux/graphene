//go:build !linux && !darwin

package disk

// Every other platform, windows included, which has no post-mapping advice to
// give.
//
// Windows is the one worth naming, because it is not simply missing the call.
// Its cache manager takes its access-pattern hint at CreateFile time --
// FILE_FLAG_RANDOM_ACCESS or FILE_FLAG_SEQUENTIAL_SCAN -- rather than against a
// live mapping, and its answer to "give these pages back" is a working-set cap
// on the process rather than an instruction about a range. That is
// LimitWorkingSet, which is a function rather than an Option because it acts on
// the process; workingset.go carries the argument.
//
// So this reports unsupported rather than returning nil. A store that asked for
// advice and silently got none would be an option asked for and not held, which
// is the failure StorageStats and the metrics exist to make visible; reporting
// it once, at the point the decision is taken, is the same shape
// MetricImageFallback already has.

// adviceSupported is what adviseRange's callers test before reporting a failure
// as one. A platform that cannot advise has not failed to.
const adviceSupported = false

func adviseRange(data []byte, a mappingAdvice) error { return errAdviceUnsupported }
