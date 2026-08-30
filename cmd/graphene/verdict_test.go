package main

// Exit-status and resource-ownership tests.
//
// The policy under test is the one the tool has always documented and never
// enforced: exit status is non-zero only when something is actually broken; an
// incomplete account is reported as findings and exits zero.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/disk"
)

// TestBrokenStoreExitsOne corrupts the CSR image and checks the verdict.
//
// `csr -verify` on a mangled file has to exit 1: this is the one case where a
// non-zero status is the whole point, and it went unenforced for the life of
// the tool.
func TestBrokenStoreExitsOne(t *testing.T) {
	dir := fixture(t)
	corruptCSR(t, dir)

	out, _, code := exec(t, "csr", "-verify", dir)
	if code != 1 {
		t.Fatalf("a corrupted image exited %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "BROKEN") && !strings.Contains(out, "mismatch") {
		t.Errorf("the report does not say what is wrong:\n%s", out)
	}
}

// TestFindingsExitZero is the other half of the policy, and the half that is
// easy to get wrong. A store that was never signed, audited or anchored is not
// broken — it is unprovisioned, which is the normal state of most stores. If
// this exits non-zero the commands become useless in the scripts that most want
// them.
func TestFindingsExitZero(t *testing.T) {
	dir := fixture(t)
	for _, argv := range [][]string{
		{"redactions", dir},
		{"grants", dir},
		{"verify", dir},
	} {
		if out, errb, code := exec(t, argv...); code != 0 {
			t.Errorf("%v exited %d on an unprovisioned store, want 0\n%s\n%s",
				argv, code, out, errb)
		}
	}
}

// TestCommandsDoNotLeakTheStoreLock is the regression test for the bug this
// whole refactor was built around.
//
// Two commands used to finish with `if report.Broken() { os.Exit(1) }` while
// holding the store under a deferred Close. os.Exit does not run deferred
// functions, so a failing check left the lock file behind and the next process
// to try the store was told it was busy by a process that had already exited.
//
// The check is not "did we remember to Close" but "can the store be opened for
// writing afterwards", because that is the symptom an operator actually hits.
func TestCommandsDoNotLeakTheStoreLock(t *testing.T) {
	for _, argv := range [][]string{
		{"custody", "-node", "1"},
		{"verify"},
		{"csr", "-verify"},
		{"redactions"},
		{"grants"},
		{"prove", "-node", "1"},
	} {
		name := strings.Join(argv, "_")
		t.Run(name, func(t *testing.T) {
			dir := fixture(t)
			full := append(append([]string{}, argv...), dir)
			exec(t, full...)

			// Whatever the command decided, the lock must be gone.
			g, err := disk.Open(dir)
			if err != nil {
				t.Fatalf("after `graphene %s`, the store could not be opened for "+
					"writing: %v", strings.Join(full[:len(full)-1], " "), err)
			}
			_ = g.Close()
		})
	}
}

// TestBrokenCommandsStillReleaseTheLock is the same check on the path that
// actually leaked: the command has to *fail* for the old bug to bite.
func TestBrokenCommandsStillReleaseTheLock(t *testing.T) {
	dir := fixture(t)
	corruptCSR(t, dir)

	// Whether this exits 0 or 1 is not the point; the lock is.
	exec(t, "custody", "-node", "1", dir)

	// The store is corrupt, so Open fails either way — the assertion has to be
	// specifically about the lock, or it would pass for the wrong reason.
	if _, err := disk.Open(dir); errors.Is(err, disk.ErrStoreLocked) {
		t.Fatalf("a failed custody left the store locked: %v", err)
	}
}

// TestJSONCarriesTheVerdict: the exit code is deliberately coarse, so a caller
// that wants the distinction reads the envelope. That is only true if the
// envelope actually carries it.
func TestJSONCarriesTheVerdict(t *testing.T) {
	dir := fixture(t)
	corruptCSR(t, dir)

	out, _, code := exec(t, "-json", "csr", "-verify", dir)
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	var env struct {
		Status   string `json:"status"`
		Exit     int    `json:"exit"`
		Findings []struct {
			Code     string `json:"code"`
			Severity string `json:"severity"`
		} `json:"findings"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if env.Status != "broken" {
		t.Errorf("status %q, want broken", env.Status)
	}
	if env.Exit != 1 {
		t.Errorf("envelope exit %d, want 1", env.Exit)
	}
	if len(env.Findings) == 0 {
		t.Fatal("a broken verdict with no findings tells a machine nothing")
	}
	for _, f := range env.Findings {
		if f.Code == "" {
			t.Error("a finding with no code cannot be matched on by a monitor")
		}
	}
}

// TestHumanAndJSONReportTheSameFacts walks both renderings of the same command
// and checks the JSON carries every value the human report showed.
//
// This is the property the Result document exists to guarantee: a field cannot
// reach one renderer and not the other. Worth testing anyway, because the
// guarantee is only as good as the handlers all going through it.
func TestHumanAndJSONReportTheSameFacts(t *testing.T) {
	dir := fixture(t)

	human, _, _ := exec(t, "store", "info", dir)
	jsonOut, _, _ := exec(t, "-json", "store", "info", dir)

	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &env); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if len(env.Data) == 0 {
		t.Fatal("the JSON envelope carries no data")
	}
	// The node and edge counts appear in the human report; they must be in the
	// document too.
	img, ok := env.Data["csr_image"].(map[string]any)
	if !ok {
		t.Fatalf("no csr_image section in %v", keysOf(env.Data))
	}
	for _, want := range []string{"nodes", "edges", "version", "size"} {
		if _, ok := img[want]; !ok {
			t.Errorf("csr_image has no %q; the human report shows it", want)
		}
	}
	if !strings.Contains(human, "nodes") {
		t.Error("the human report lost its node count")
	}

	// Regression: `store info` writes two untitled sections, and both key as
	// "detail". Before they were merged, the second replaced the first and the
	// store path disappeared from the document while still appearing in the
	// human report — exactly the drift the shared Result is meant to prevent.
	detail, ok := env.Data["detail"].(map[string]any)
	if !ok {
		t.Fatalf("no detail section in %v", keysOf(env.Data))
	}
	for _, want := range []string{"store", "unreplayed_log"} {
		if _, ok := detail[want]; !ok {
			t.Errorf("detail has no %q — an untitled section was overwritten "+
				"by a later one", want)
		}
	}
}

// TestOutputIsDeterministic runs each read-only command twice and diffs.
//
// Map iteration order in Go is randomised, so a report built from a map differs
// between runs — and a report you cannot diff is most of the value of an
// inspection tool gone. The codebase already hand-sorts for this reason.
func TestOutputIsDeterministic(t *testing.T) {
	dir := fixture(t)
	for _, argv := range [][]string{
		{"store", "info", dir},
		{"store", "csr", dir},
		{"wal", "show", dir},
		{"grant", "list", dir},
		{"-json", "store", "info", dir},
		{"-json", "store", "csr", dir},
	} {
		first, _, _ := exec(t, argv...)
		second, _, _ := exec(t, argv...)
		if first != second {
			t.Errorf("%v is not deterministic between runs:\n--- first ---\n%s\n--- second ---\n%s",
				argv, first, second)
		}
	}
}

// TestMutationGateRefusesWithoutConfirm proves the gate is enforced by the
// framework rather than by each handler remembering.
func TestMutationGateRefusesWithoutConfirm(t *testing.T) {
	var gated []*Command
	for _, c := range registry {
		if c.Mutates {
			gated = append(gated, c)
		}
	}
	if len(gated) == 0 {
		t.Skip("no gated mutations registered yet")
	}
	for _, c := range gated {
		dir := fixture(t)
		before := snapshotDir(t, dir)
		argv := append(strings.Fields(c.Path()), dir)
		_, errb, code := exec(t, argv...)
		if code == 0 {
			t.Errorf("%s ran without -confirm", c.Path())
		}
		if !strings.Contains(errb, "-confirm") {
			t.Errorf("%s did not say how to proceed:\n%s", c.Path(), errb)
		}
		if after := snapshotDir(t, dir); after != before {
			t.Errorf("%s changed the store despite being refused", c.Path())
		}
	}
}

// corruptCSR flips a byte in the middle of the image, so the stored digest no
// longer matches the contents.
func corruptCSR(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "graphene.csr")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	if len(b) < 100 {
		t.Fatalf("image is only %d bytes; nothing to corrupt", len(b))
	}
	b[len(b)/2] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write image: %v", err)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestJSONEnvelopeSchema checks the envelope over every read-only command, not
// just the one `store info` case above.
//
// It unmarshals rather than diffing, deliberately. The golden corpus already
// pins five JSON documents byte for byte, which is the right check for
// formatting and the wrong one for schema: reordering a section would fail it
// without anything having broken for a consumer, and a renamed envelope field
// would fail it in a diff that looks like ordinary churn. This asserts the
// contract a script actually depends on — the envelope's shape, the status
// vocabulary, and that the exit code inside the document agrees with the one the
// process returned — and is indifferent to order.
func TestJSONEnvelopeSchema(t *testing.T) {
	dir := fixture(t)
	// The whole vocabulary, from Verdict.String in errors.go. Listed rather than
	// derived: a status the tool starts emitting should have to be added here by
	// somebody who then asks whether every consumer switching on it will cope.
	statuses := map[string]bool{"ok": true, "findings": true, "broken": true}

	for _, argv := range [][]string{
		{"store", "info", dir},
		{"store", "csr", dir},
		{"store", "stats", dir},
		{"store", "snapshot", dir},
		{"store", "health", dir},
		{"wal", "show", dir},
		{"wal", "segments", dir},
		{"wal", "verify", dir},
		{"node", "count", dir},
		{"node", "list", "-type", "MicroArtefact", dir},
		{"node", "get", "-id", "1", dir},
		{"node", "degree", "-id", "1", dir},
		{"node", "neighbours", "-id", "1", dir},
		{"node", "explain", "-type", "EvidenceFile", dir},
		{"edge", "count", dir},
		{"edge", "list", "-type", "Contains", dir},
		{"edge", "of", "-node", "1", dir},
		{"edge", "provenance", "-id", "1", dir},
		{"traverse", "bfs", "-from", "1", "-depth", "3", dir},
		{"traverse", "path", "-from", "1", "-to", "5", dir},
		{"traverse", "cycle", "-from", "1", dir},
		{"traverse", "subgraph", "-id", "1,3,4", dir},
		{"query", "relations", "-anchor", "1", dir},
		{"provenance", "chain", "-node", "4", dir},
		{"anchor", "list", dir},
		{"assertion", "list", dir},
		{"redaction", "list", dir},
		{"redaction", "tombstones", dir},
		{"grant", "list", dir},
		{"keys", "timeline", dir},
		{"debug", "hash-check", dir},
		{"debug", "indexes", dir},
		{"debug", "stats", dir},
		{"debug", "integrity", dir},
	} {
		t.Run(strings.Join(argv[:len(argv)-1], "_"), func(t *testing.T) {
			out, errb, code := exec(t, append([]string{"-json"}, argv...)...)

			var env struct {
				Status   string         `json:"status"`
				Exit     *int           `json:"exit"`
				Data     map[string]any `json:"data"`
				Findings []struct {
					Code     string `json:"code"`
					Severity string `json:"severity"`
					Message  string `json:"message"`
				} `json:"findings"`
			}
			if err := json.Unmarshal([]byte(out), &env); err != nil {
				t.Fatalf("stdout is not JSON: %v\n%s\nstderr:\n%s", err, out, errb)
			}
			if !statuses[env.Status] {
				t.Errorf("status %q is not one a consumer can switch on", env.Status)
			}
			// A pointer, so that a missing field is distinguishable from the
			// zero that a successful command legitimately reports.
			if env.Exit == nil {
				t.Fatal("the envelope has no exit field")
			}
			if *env.Exit != code {
				t.Errorf("the envelope says exit %d, the process returned %d", *env.Exit, code)
			}
			if len(env.Data) == 0 {
				t.Error("the envelope carries no data")
			}
			for i, f := range env.Findings {
				if f.Code == "" {
					t.Errorf("finding %d has no code; a monitor cannot match on it", i)
				}
				if f.Severity == "" {
					t.Errorf("finding %d (%s) has no severity", i, f.Code)
				}
				if f.Message == "" {
					t.Errorf("finding %d (%s) has no message", i, f.Code)
				}
			}
		})
	}
}
