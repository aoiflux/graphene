package main

// The config file and named store profiles.
//
// # What a config may and may not set
//
// Only how a command reports: -json, -indent, -quiet, -no-color, -log-level,
// -metrics. Never what it does. There is no way to put -confirm or -dry-run in
// a config file, and that is a deliberate limit rather than an omission.
//
// A config that could set -confirm would make `graphene node delete -id 7` a
// destructive command on one machine and a refusal on another, with the
// difference living in a file nobody looking at the command line can see. The
// gate is only worth having if it means the same thing everywhere.
//
// # Profiles
//
// A profile is a name for a store directory, plus the public keys that belong
// with it. `graphene store info case01` resolves the name when there is no
// directory by that name, and says on stderr what it resolved to — a tool that
// silently substituted a path would be one you could not safely paste a command
// into.
//
// The keys are the part that earns the feature. Every verification command
// takes -pubkey, and typing an Ed25519 key on the command line is the step that
// gets skipped — which is why so many reports read "signatures unchecked". A
// profile supplies them, and every report that used them says so.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Config is the file's shape.
type Config struct {
	// Version is 1. It exists so a later format can be recognised rather than
	// misread: a config this build does not understand should be refused, not
	// silently half-applied.
	Version int `json:"version"`

	// Defaults may set only the reporting flags. See the file comment.
	Defaults ConfigDefaults `json:"defaults,omitempty"`

	Profiles map[string]Profile `json:"profiles,omitempty"`

	// path is where this was loaded from, and is not part of the file.
	path string
}

// ConfigDefaults is the subset of the globals a config is allowed to set.
type ConfigDefaults struct {
	JSON     bool   `json:"json,omitempty"`
	Indent   bool   `json:"indent,omitempty"`
	Quiet    bool   `json:"quiet,omitempty"`
	NoColor  bool   `json:"noColor,omitempty"`
	Metrics  bool   `json:"metrics,omitempty"`
	LogLevel string `json:"logLevel,omitempty"`
}

// Profile is a named store.
type Profile struct {
	Dir string `json:"dir"`

	// PubKeys are `ID:HEX` Ed25519 public keys, in the same spelling -pubkey
	// takes, so a key can be moved between the two without translation.
	PubKeys []string `json:"pubkeys,omitempty"`

	Note string `json:"note,omitempty"`
}

// configPath is where the config lives: $GRAPHENE_CONFIG if set, otherwise the
// OS config directory.
func configPath() (string, error) {
	if p := os.Getenv("GRAPHENE_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "graphene", "config.json"), nil
}

// loadConfig reads the config, returning an empty one when there is no file.
//
// A missing config is the ordinary case and never an error. A malformed one is
// an error: a config that is quietly ignored because it does not parse is worse
// than no config, because the operator believes it is in force.
func loadConfig() (*Config, error) {
	path, err := configPath()
	if err != nil {
		return &Config{Version: 1}, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Config{Version: 1, path: path}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	c := &Config{path: path}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("config %s: %w\n"+
			"  unknown fields are refused rather than ignored: a setting that looks "+
			"applied and is not is worse than one that is missing", path, err)
	}
	if c.Version != 1 {
		return nil, fmt.Errorf("config %s: version %d, and this build understands 1",
			path, c.Version)
	}
	c.path = path
	return c, nil
}

// apply writes the config's defaults into the globals.
//
// Called before applyEnv and before any flag is parsed, so the precedence is
// config, then environment, then command line — each layer overriding the one
// before it, and the command line always winning.
func (c *Config) apply(g *Globals) {
	d := c.Defaults
	g.JSON = g.JSON || d.JSON
	g.Indent = g.Indent || d.Indent
	g.Quiet = g.Quiet || d.Quiet
	g.NoColor = g.NoColor || d.NoColor
	g.Metrics = g.Metrics || d.Metrics
	if d.LogLevel != "" {
		g.LogLevel = d.LogLevel
	}
	if g.Indent {
		g.JSON = true
	}
}

// resolve maps an operand onto a profile's directory.
//
// A path that exists wins, always. A profile can never shadow a real directory,
// so a command that works today cannot start meaning something else because
// somebody added a profile with an unfortunate name.
func (c *Config) resolve(target string) (dir string, p Profile, ok bool) {
	if target == "" || c == nil {
		return target, Profile{}, false
	}
	if _, err := os.Stat(target); err == nil {
		return target, Profile{}, false
	}
	prof, found := c.Profiles[target]
	if !found || prof.Dir == "" {
		return target, Profile{}, false
	}
	return prof.Dir, prof, true
}

// applyProfileKeys supplies a profile's public keys to a command that takes
// -pubkey and was given none.
//
// Only when none was given: a key typed on the command line is a deliberate
// choice about what to check against, and quietly adding more to it would mean
// a verification passed on a key the operator did not name.
//
// And it says so. A report that reads "signatures verified" has to be able to
// answer "against which keys", and the answer here is "ones from a config file"
// — which is fine, and is not fine to leave unsaid.
func applyProfileKeys(fs *flag.FlagSet, p Profile, g *Globals, stderr io.Writer) {
	if len(p.PubKeys) == 0 {
		return
	}
	f := fs.Lookup("pubkey")
	if f == nil {
		return // this command does not check signatures
	}
	given := false
	fs.Visit(func(v *flag.Flag) {
		if v.Name == "pubkey" {
			given = true
		}
	})
	if given {
		return
	}
	for _, k := range p.PubKeys {
		// Already validated by `profile add`, so a failure here means the file
		// was hand-edited into something invalid. Reported rather than ignored.
		if err := f.Value.Set(k); err != nil {
			_, _ = io.WriteString(stderr,
				"graphene: profile key "+k+" is not usable: "+err.Error()+"\n")
			return
		}
	}
	if !g.Quiet {
		_, _ = io.WriteString(stderr, fmt.Sprintf(
			"graphene: checking signatures against %d key(s) from the profile\n",
			len(p.PubKeys)))
	}
}

// profileNames lists the profiles in a stable order.
func (c *Config) profileNames() []string {
	out := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// save writes the config, creating the directory if it is missing.
func (c *Config) save() error {
	if c.path == "" {
		p, err := configPath()
		if err != nil {
			return err
		}
		c.path = p
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')

	// Written through a temporary file and renamed, so an interrupted write
	// leaves the old config rather than half of a new one. The same argument
	// the engine makes about its image, at a much smaller scale.
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
