//go:build !windows

package disk

// Every platform but windows, which has no enforced per-process resident cap
// worth reaching for. ErrWorkingSetUnsupported carries the argument.

func setWorkingSetLimit(maxBytes uint64) (WorkingSetLimit, error) {
	return WorkingSetLimit{}, ErrWorkingSetUnsupported
}

// restoreWorkingSetDefault has nothing to undo where nothing can be installed.
func restoreWorkingSetDefault() error { return nil }
