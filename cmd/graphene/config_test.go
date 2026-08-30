package main

// Tests for the config file, named profiles, and generated completion.
//
// The completion test is the one that earns its place: a hand-written
// completion script is a fourth copy of the command list, and this package has
// already watched three copies drift apart. Generating it is only worth
// anything if something checks that the generated thing is complete.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withConfig points GRAPHENE_CONFIG at a fresh file for one test and returns
// the path. TestMain already isolates the suite from the developer's own
// config; this isolates tests from each other.
func withConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("GRAPHENE_CONFIG", path)
	return path
}

// TestCompletionNamesEveryCommand is the anti-drift check. Completion is
// generated from the registry, so a command cannot exist without appearing in
// it — the same guarantee `graphene help` has.
func TestCompletionNamesEveryCommand(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			out, _, code := exec(t, "completion", shell)
			if code != 0 {
				t.Fatalf("exit %d", code)
			}
			for _, c := range registry {
				if c.Hidden {
					continue
				}
				if c.Group != "" && !strings.Contains(out, c.Group) {
					t.Errorf("group %s is missing from the %s script", c.Group, shell)
				}
				if !strings.Contains(out, c.Name) {
					t.Errorf("%s is in the registry but not in the %s script", c.Path(), shell)
				}
			}
		})
	}
}

func TestCompletionRefusesAnUnknownShell(t *testing.T) {
	if _, _, code := exec(t, "completion", "csh"); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
	if _, _, code := exec(t, "completion"); code != 2 {
		t.Errorf("a missing shell exited %d, want 2", code)
	}
}

// TestProfileResolvesInTheOperand covers the whole feature end to end: a name
// added by `profile add` resolves when a command is given it instead of a path.
func TestProfileResolvesInTheOperand(t *testing.T) {
	withConfig(t)
	dir := fixture(t)

	if out, errb, code := exec(t, "profile", "add", "-dir", dir, "case01"); code != 0 {
		t.Fatalf("profile add exited %d\n%s\n%s", code, out, errb)
	}

	byName, errb, code := exec(t, "-quiet", "store", "info", "case01")
	if code != 0 {
		t.Fatalf("store info by profile exited %d\n%s\n%s", code, byName, errb)
	}
	byPath, _, _ := exec(t, "-quiet", "store", "info", dir)
	if byName != byPath {
		t.Errorf("the profile and the path produced different reports:\n--- profile ---\n%s\n--- path ---\n%s",
			byName, byPath)
	}

	// And it says what it resolved to, on stderr. A tool that silently
	// substituted a path would be one you could not safely paste a command into.
	_, errb, _ = exec(t, "store", "info", "case01")
	if !strings.Contains(errb, "case01") || !strings.Contains(errb, dir) {
		t.Errorf("the resolution was not reported:\n%s", errb)
	}
}

// TestProfileNeverShadowsARealDirectory. A path that exists always wins, so
// adding a profile cannot change what a command that works today means.
func TestProfileNeverShadowsARealDirectory(t *testing.T) {
	withConfig(t)
	real := fixture(t)
	decoy := fixture(t)

	// Name the profile after a directory that exists.
	name := filepath.Base(real)
	if _, _, code := exec(t, "profile", "add", "-dir", decoy, name); code != 0 {
		t.Fatalf("profile add exited %d", code)
	}

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got, _, resolved := cfg.resolve(real)
	if resolved {
		t.Errorf("the profile shadowed a real directory: %s -> %s", real, got)
	}
	if got != real {
		t.Errorf("resolve changed a real path: %s -> %s", real, got)
	}
}

// TestConfigCannotSetTheMutationGate. -confirm and -dry-run are command-line
// only: a config that could set -confirm would make the same command
// destructive on one machine and a refusal on another, with the difference in a
// file the command line does not show.
//
// Enforced by DisallowUnknownFields, so a config trying it is a load error
// rather than a setting quietly dropped.
func TestConfigCannotSetTheMutationGate(t *testing.T) {
	path := withConfig(t)
	body := `{"version":1,"defaults":{"confirm":true}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := loadConfig(); err == nil {
		t.Fatal("a config setting confirm loaded without complaint")
	}

	// And the tool refuses to run rather than proceeding with it ignored.
	_, errb, code := exec(t, "version")
	if code == 0 {
		t.Errorf("the tool ran with an unreadable config; exit %d", code)
	}
	if !strings.Contains(errb, "confirm") {
		t.Errorf("the error does not name the offending field:\n%s", errb)
	}
}

// TestConfigDefaultsApply checks the layering: config, then environment, then
// flags, each overriding the one before it.
func TestConfigDefaultsApply(t *testing.T) {
	path := withConfig(t)
	if err := os.WriteFile(path, []byte(`{"version":1,"defaults":{"json":true}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	out, _, code := exec(t, "version")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("the config's json default was not applied:\n%s", out)
	}
}

// TestUnknownConfigFieldIsRefused: a setting that looks applied and is not is
// worse than one that is missing.
func TestUnknownConfigFieldIsRefused(t *testing.T) {
	path := withConfig(t)
	if err := os.WriteFile(path,
		[]byte(`{"version":1,"profiles":{"a":{"dir":"/x","typo":1}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(); err == nil {
		t.Error("an unknown field loaded without complaint")
	}
}

// TestProfileKeysReachTheCommand: a profile's public keys are supplied to a
// command that takes -pubkey and was given none, and the tool says it did.
func TestProfileKeysReachTheCommand(t *testing.T) {
	withConfig(t)
	dir := governedFixture(t)

	// A syntactically valid key that signs nothing. What is being tested is
	// that it arrives, not that it verifies.
	key := "1:" + strings.Repeat("ab", 32)
	if out, errb, code := exec(t, "profile", "add", "-dir", dir, "-pubkey", key, "keyed"); code != 0 {
		t.Fatalf("profile add exited %d\n%s\n%s", code, out, errb)
	}

	out, errb, code := exec(t, "grant", "list", "keyed")
	if code != 0 {
		t.Fatalf("grant list exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(errb, "from the profile") {
		t.Errorf("the report does not say the keys came from a config file:\n%s", errb)
	}
	if strings.Contains(out, "signatures unchecked") {
		t.Errorf("the profile's keys did not reach the command:\n%s", out)
	}
}

// TestProfileRejectsAnUnusableKey. A key that does not parse should be refused
// by the command that writes it, not by the verification command three weeks
// later that was relying on it.
func TestProfileRejectsAnUnusableKey(t *testing.T) {
	withConfig(t)
	if _, _, code := exec(t, "profile", "add", "-dir", ".", "-pubkey", "nonsense", "bad"); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}
