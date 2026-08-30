package main

// Build metadata.
//
// debug.ReadBuildInfo is the source and -ldflags is the override, rather than
// the other way around. Three reasons, all specific to this repo:
//
//   - There is no build wrapper today, and `go build ./cmd/graphene` is how
//     everyone builds it. If ldflags were the source, the ordinary build would
//     report "dev" and only the release script would produce a real answer.
//     ReadBuildInfo makes the plain build honest with no Makefile at all.
//   - `go install github.com/aoiflux/graphene/cmd/graphene@latest` passes no
//     ldflags and never will. Main.Version covers that path.
//   - ldflags still earns its place for the one thing ReadBuildInfo cannot do:
//     the tag. `git describe --tags --always --dirty` gives v0.4.0-3-gab12cd;
//     the build info gives a bare revision hash.
//
// This command never shells out to git. A binary whose version answer depends
// on the working directory it happens to be run from is worse than one that
// says "(devel)", and it would make the git executable a runtime dependency of
// a program that deliberately has none.

import (
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/aoiflux/graphene/disk"
)

// Overridden at link time by the release build. Empty in an ordinary build, in
// which case stamp() fills them from debug.ReadBuildInfo.
//
//	go build -ldflags "\
//	  -X main.version=$(git describe --tags --always --dirty) \
//	  -X main.commit=$(git log -1 --format=%H) \
//	  -X main.buildTime=$(git log -1 --format=%ct)" ./cmd/graphene
var (
	version   = ""
	commit    = ""
	buildTime = "" // seconds since the epoch, decimal
)

// VersionStamp is what `graphene version` prints and what every JSON envelope
// carries, so an archived report says what produced it.
type VersionStamp struct {
	Version     string `json:"version"`
	Commit      string `json:"commit,omitempty"`
	CommitShort string `json:"commit_short,omitempty"`
	Committed   string `json:"committed,omitempty"` // RFC3339 UTC
	Dirty       bool   `json:"dirty"`
	Source      string `json:"source"` // ldflags | module | vcs | unknown

	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`

	// CSRFormat is the image version this binary writes. An operator asking
	// "will this build migrate that store" is asking about this number and not
	// about the tag.
	CSRFormat uint16 `json:"csr_format"`

	// Dependencies is empty, and the field exists so that
	// `graphene version -json` answers the supply-chain question outright
	// rather than by omission: this binary links nothing outside the standard
	// library.
	Dependencies []string `json:"dependencies"`
}

var (
	stampOnce sync.Once
	stampVal  VersionStamp
)

// stamp resolves the build metadata once.
func stamp() VersionStamp {
	stampOnce.Do(func() { stampVal = resolveStamp() })
	return stampVal
}

func resolveStamp() VersionStamp {
	v := VersionStamp{
		Version:      "(devel)",
		Source:       "unknown",
		GoVersion:    runtime.Version(),
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		CSRFormat:    disk.CSRVersionCurrent,
		Dependencies: []string{},
	}

	// 1. Link-time values win when a release build supplied them.
	if version != "" {
		v.Version, v.Source = version, "ldflags"
		v.Commit = commit
		if t, err := strconv.ParseInt(buildTime, 10, 64); err == nil && t > 0 {
			v.Committed = time.Unix(t, 0).UTC().Format(time.RFC3339)
		}
		v.fill()
		return v
	}

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return v
	}

	// 2. A module install carries its own version.
	if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		v.Version, v.Source = bi.Main.Version, "module"
	}

	// 3. A git checkout is stamped by the toolchain.
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			v.Commit = s.Value
			if v.Source == "unknown" {
				v.Source = "vcs"
			}
		case "vcs.time":
			v.Committed = s.Value
		case "vcs.modified":
			v.Dirty = s.Value == "true"
		}
	}
	if v.Source == "vcs" && len(v.Commit) >= 12 {
		v.Version = "(devel)+" + v.Commit[:12]
		if v.Dirty {
			v.Version += "-dirty"
		}
	}

	v.fill()
	return v
}

// fill derives the values that follow from the ones already resolved.
func (v *VersionStamp) fill() {
	if len(v.Commit) >= 12 {
		v.CommitShort = v.Commit[:12]
	}
	if !v.Dirty && len(v.Version) > 6 && v.Version[len(v.Version)-6:] == "-dirty" {
		v.Dirty = true
	}
	if v.Dependencies == nil {
		v.Dependencies = []string{}
	}
}

// runVersion reports what this binary is.
func runVersion(cx *Context) (Result, error) {
	s := stamp()
	var r Result

	sec := r.Section("")
	sec.Add("version", Str(s.Version))
	sec.Add("source", Str(s.Source))
	if s.Commit != "" {
		sec.Add("commit", Str(s.Commit))
	}
	if s.Committed != "" {
		sec.Add("committed", Str(s.Committed))
	}
	sec.Add("dirty", Bool(s.Dirty))
	sec.Add("go", Str(s.GoVersion))
	sec.Addf("platform", "%s/%s", s.OS, s.Arch)
	sec.Add("CSR format", Uint(uint64(s.CSRFormat)))
	sec.AddNote("dependencies", Str("none"), "(standard library only)")

	return r, nil
}
