package main

// Formatting helpers shared by the commands.
//
// These are the human half of the Value constructors in result.go: humanBytes
// is what Bytes() renders, formatNano is what Time() renders. They stay
// separate functions because a handler occasionally needs the string on its own,
// inside a sentence that has no separate field of its own.

import (
	"fmt"
	"time"

	"github.com/aoiflux/graphene/disk"
)

// sparsityNote flags an ID space far larger than the record count, which is what
// makes an open expensive: the CSR indexes its arrays by ID, not by count.
func sparsityNote(c disk.CSRInfo) string {
	if c.NodeCount == 0 || c.MaxNodeID == 0 {
		return ""
	}
	ratio := float64(c.MaxNodeID) / float64(c.NodeCount)
	if ratio < 4 {
		return ""
	}
	return fmt.Sprintf("  (node IDs are %.0fx sparser than the count — arrays are sized by ID)", ratio)
}

// --- helpers ---

func attributedCommits(w disk.WALInfo) int {
	n := 0
	for _, c := range w.Commits {
		if c.HasDetail && c.ActorID != 0 {
			n++
		}
	}
	return n
}

func lastCommit(w disk.WALInfo) (disk.WALCommitInfo, bool) {
	for i := len(w.Commits) - 1; i >= 0; i-- {
		if w.Commits[i].HasDetail {
			return w.Commits[i], true
		}
	}
	return disk.WALCommitInfo{}, false
}

func formatNano(ns int64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// underlying unwraps to the innermost error so os.IsNotExist can see it.
func underlying(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		next := u.Unwrap()
		if next == nil {
			return err
		}
		err = next
	}
}
