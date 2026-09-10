package graphene_test

// Build-constraint guards: zero cgo, zero external dependencies, and a fixed
// set of files allowed to import unsafe.
//
// These three properties are the reason a graphene binary cross-compiles to
// every platform in the release matrix from one machine, and the reason a
// consumer auditing the tree has three files to read rather than a dependency
// tree. None of them is enforced by the compiler: adding `import "C"` to one
// file, or a single `go get`, silently costs all of it, and the loss shows up
// only when a release build fails on a platform nobody has locally.
//
// Untagged on purpose, like the alloc and footprint guards: this runs in
// `make check`, so the drift is caught by the ordinary suite.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// goListLines runs `go list` from the module root and returns its non-empty
// output lines.
//
// CGO_ENABLED is forced on rather than inherited. `.CgoFiles` is empty for
// every package when cgo is disabled, so a machine that happens to build with
// CGO_ENABLED=0 — every container in the release job, for one — would turn the
// cgo assertion below into a test that cannot fail. Asking the question with
// cgo enabled is the only way the answer means anything.
func goListLines(t *testing.T, format string) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		// Not a pass: say why, loudly. A stripped image without a toolchain
		// cannot answer this question, but it must not look like it did.
		t.Skipf("no go toolchain on PATH, cannot verify build constraints: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-deps", "-f", format, "./...")
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}

	var lines []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// moduleRoot returns the directory holding go.mod. The test binary runs in
// tests/, one level down.
func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", root, err)
	}
	return root
}

// TestZeroCgo asserts that no package in the transitive closure — graphene's
// own and every standard-library package it reaches — compiles a cgo file.
//
// The closure, not just this module: a dependency on net or os/user would put
// a C compiler on the build path just as surely as an `import "C"` here, and
// the failure would be a release build that no longer cross-compiles.
func TestZeroCgo(t *testing.T) {
	withCgo := goListLines(t, `{{if .CgoFiles}}{{.ImportPath}}{{end}}`)
	if len(withCgo) != 0 {
		t.Errorf("packages in the build closure compile cgo files: %s\n"+
			"graphene cross-compiles to every release platform from one machine, "+
			"which holds only while the whole closure is pure Go",
			strings.Join(withCgo, ", "))
	}
}

// TestZeroExternalDependencies asserts go.mod declares no requirements.
//
// Read as text rather than via `go list -m all`, because the question is about
// what the module *declares*: an indirect requirement left behind by a `go get`
// that was later reverted still puts a version constraint in the file and still
// has to be vendored by anyone building offline.
func TestZeroExternalDependencies(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(moduleRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "require") {
			t.Errorf("go.mod:%d declares a requirement: %q\n"+
				"graphene depends on the standard library only", i+1, trimmed)
		}
	}
}

// unsafeFiles is the complete set of non-test files permitted to import
// unsafe, relative to the module root and slash-separated.
//
// Each is a documented, reviewed exception:
//
//   - disk/fileshare_windows.go, disk/lock_windows.go — Windows file-sharing
//     and locking flags the syscall package does not expose as typed calls.
//   - index/property_index.go — unsafeBytes, a no-copy string→[]byte view
//     handed to a callback that is documented not to retain or mutate it.
//
// An addition here is not forbidden; it is required to be deliberate. The
// list is what makes a fourth file a review decision instead of an accident.
var unsafeFiles = map[string]bool{
	"disk/fileshare_windows.go": true,
	"disk/lock_windows.go":      true,
	"index/property_index.go":   true,
}

// TestUnsafeIsConfinedToKnownFiles walks every non-test Go file in the tree and
// asserts the set importing unsafe is exactly unsafeFiles.
//
// Exactly, in both directions. A file that drops its unsafe import should drop
// out of the list too, or the list stops describing the tree and starts being
// a comment.
func TestUnsafeIsConfinedToKnownFiles(t *testing.T) {
	root := moduleRoot(t)
	found := map[string]bool{}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Skip the toolchain's own directories and anything hidden; .git in
			// particular holds blobs that look like Go source to a suffix match.
			if name := d.Name(); name != "." && strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !importsUnsafe(string(src)) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		found[filepath.ToSlash(rel)] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for file := range found {
		if !unsafeFiles[file] {
			t.Errorf("%s imports unsafe and is not in the reviewed set\n"+
				"add it to unsafeFiles with the reason, or remove the import", file)
		}
	}
	for file := range unsafeFiles {
		if !found[file] {
			t.Errorf("unsafeFiles lists %s, which no longer imports unsafe\n"+
				"drop it from the list so the list keeps describing the tree", file)
		}
	}
}

// importsUnsafe reports whether src has unsafe in its import block.
//
// A string match on the quoted path rather than a parse: the import path is
// unambiguous in Go source outside a comment or string literal, and the two
// forms that could produce a false positive — the word in prose, and a
// different package whose path ends in "unsafe" — are separated by requiring
// the exact quoted token on its own or after an alias.
func importsUnsafe(src string) bool {
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimPrefix(trimmed, "import ")
		trimmed = strings.TrimSpace(trimmed)
		if trimmed == `"unsafe"` {
			return true
		}
		if fields := strings.Fields(trimmed); len(fields) == 2 && fields[1] == `"unsafe"` {
			return true
		}
	}
	return false
}
