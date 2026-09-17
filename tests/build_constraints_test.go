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

// goListLines runs `go list -deps` from the module root with CGO_ENABLED set as
// given, and returns its non-empty output lines.
//
// CGO_ENABLED is passed rather than inherited, because each assertion below is
// about a *setting*: with cgo on, which packages compile C; with it off, which
// packages are left with nothing to build. Inheriting it would make whichever
// question the machine happened to ask the only one asked — and `.CgoFiles` is
// empty for every package when cgo is off, which is a form of the first
// assertion that cannot fail.
func goListLines(t *testing.T, cgoEnabled, format string, extra ...string) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		// Not a pass: say why, loudly. A stripped image without a toolchain
		// cannot answer this question, but it must not look like it did.
		t.Skipf("no go toolchain on PATH, cannot verify build constraints: %v", err)
	}

	args := append([]string{"list", "-deps"}, extra...)
	args = append(args, "-f", format, "./...")
	cmd := exec.Command(goBin, args...)
	cmd.Dir = moduleRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED="+cgoEnabled)
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

// modulePath is this module's import path: the prefix that separates graphene's
// own packages from the standard library in a `go list` closure.
const modulePath = "github.com/aoiflux/graphene"

// TestZeroCgo asserts the property the release build actually rests on — that
// `CGO_ENABLED=0 GOOS=... go build` produces a binary for every target in the
// matrix from one machine, with no C toolchain anywhere on the path.
//
// That is two questions, and they are asked separately because only one of them
// is about this module.
//
// # Nothing here compiles C
//
// An `import "C"` in a graphene package is not a setting and no build flag
// undoes it: with cgo off the file is excluded and the package stops building,
// with cgo on the cross-build wants a C compiler per target. So the first
// assertion is scoped to this module and asked with cgo *on*, because that is
// the only setting under which its answer can be anything but empty.
//
// # Nothing in the closure requires C
//
// This used to be the same assertion widened to the whole closure, on the
// reasoning that a standard-library dependency puts a C compiler on the build
// path just as surely as an import here does. That reasoning is wrong about the
// standard library, and the gate was red on linux and macos for a property
// neither had lost: `cmd/graphene` writes a tar for `export bundle`,
// `archive/tar` reads owner names through `os/user`, and `os/user` compiles a
// cgo file *when cgo is enabled* and a pure-Go one when it is not. Forcing
// CGO_ENABLED=1 to stop the question being vacuous also manufactured its answer.
// Windows stayed green throughout, because os/user needs no cgo there — a gate
// whose result depends on which platform asks is measuring the platform.
//
// What matters is not whether a package *can* use C but whether it can be built
// without it. So the second assertion turns cgo off and asks whether anything in
// the closure is then left with no Go files to compile. A package that genuinely
// required cgo — net with a custom resolver, or an `import "C"` that somehow
// escaped the first check — says exactly that, and says it for every target the
// release builds.
func TestZeroCgo(t *testing.T) {
	var own []string
	for _, pkg := range goListLines(t, "1", `{{if .CgoFiles}}{{.ImportPath}}{{end}}`) {
		if strings.HasPrefix(pkg, modulePath) {
			own = append(own, pkg)
		}
	}
	if len(own) != 0 {
		t.Errorf("graphene packages compile cgo files: %s\n"+
			"an import \"C\" here costs the cross-build a C toolchain per target",
			strings.Join(own, ", "))
	}

	// -e so a package that cannot be built is named rather than collapsing the
	// whole listing: without it `go list` exits non-zero with one error and no
	// closure, and the name is the whole of what this assertion is for.
	broken := goListLines(t, "0", `{{if .Error}}{{.ImportPath}}: {{.Error}}{{end}}`, "-e")
	if len(broken) != 0 {
		t.Errorf("packages in the build closure cannot be built without cgo:\n%s\n"+
			"graphene cross-compiles to every release platform from one machine, "+
			"and build.sh sets CGO_ENABLED=0 to do it",
			strings.Join(broken, "\n"))
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
//   - disk/mmap_windows.go — sliceOfMapping, which turns the address
//     MapViewOfFile returns into the []byte the image is read from. It assigns
//     the slice header's fields rather than converting a uintptr to a pointer,
//     which is the form that leaves go vet's unsafeptr check something to do
//     everywhere else; the function says why at length.
//   - disk/sysmem_windows.go — the three kernel32 calls that report this
//     process's memory and the job object governing it. Each takes a pointer to
//     a struct mirroring a Windows layout and a size the kernel validates
//     against it, so unsafe.Pointer and unsafe.Sizeof are how the call is
//     expressible at all; the syscall package exposes none of the three.
//   - disk/workingset_windows.go — GetProcessWorkingSetSizeEx, which reports
//     the cap through three out-parameters. Reading back what the kernel
//     actually installed is the point of the call rather than a nicety, and
//     out-parameters are the only form it has.
//   - disk/advise_darwin.go — madvise, which the standard library wraps on
//     linux and not on darwin. The wrapper lives in golang.org/x/sys/unix and
//     this module takes no dependency for a hint, so the syscall is spelled out
//     and the mapping's address converted inline in the argument list, which is
//     the one form go vet's unsafeptr check accepts as safe.
//
// An addition here is not forbidden; it is required to be deliberate. The
// list is what makes the next file a review decision instead of an accident.
var unsafeFiles = map[string]bool{
	"disk/advise_darwin.go":      true,
	"disk/fileshare_windows.go":  true,
	"disk/lock_windows.go":       true,
	"disk/mmap_windows.go":       true,
	"disk/sysmem_windows.go":     true,
	"disk/workingset_windows.go": true,
	"index/property_index.go":    true,
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
