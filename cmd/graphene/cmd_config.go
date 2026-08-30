package main

// config path, config show, config init; profile list, add, remove, use.
//
// The only commands in the tool that touch nothing but the user's own config
// file. None of them opens a store, and `config path` does not even read one —
// it is the command to run when something is being applied from a file you
// cannot find.

import (
	"flag"
	"fmt"
	"os"
)

// --- config path ---

var configPathCmd = plain(Command{
	Group: "config", Name: "path",
	Short: "where the config file is read from",
	Long: "$GRAPHENE_CONFIG if it is set, otherwise a graphene/config.json under\n" +
		"the OS config directory. Reports the path whether or not a file is there,\n" +
		"which is the point: this is the command for finding out where to put one,\n" +
		"and for finding out which one is being applied when a setting appears\n" +
		"from nowhere.",
	Open: OpenNone, Tier: CtxAdvisory,
}, runConfigPath)

func runConfigPath(cx *Context) (Result, error) {
	var r Result
	path, err := configPath()
	if err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("path", Str(path))
	if _, serr := os.Stat(path); serr != nil {
		s.Add("exists", Bool(false))
		r.Notice("create one with: graphene config init")
	} else {
		s.Add("exists", Bool(true))
	}
	if env := os.Getenv("GRAPHENE_CONFIG"); env != "" {
		s.AddNote("from", Str("$GRAPHENE_CONFIG"), "(overrides the OS config directory)")
	}
	return r, nil
}

// --- config show ---

var configShow = plain(Command{
	Group: "config", Name: "show",
	Short: "the config as this build reads it",
	Long: "What is actually in force, not what the file says: a setting this build\n" +
		"does not understand is a load error rather than something quietly\n" +
		"dropped, so anything printed here is applied.",
	Open: OpenNone, Tier: CtxAdvisory,
}, runConfigShow)

func runConfigShow(cx *Context) (Result, error) {
	var r Result
	c, err := loadConfig()
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("path", Str(c.path))
	s.Add("version", Int(int64(c.Version)))

	d := r.Section("defaults")
	d.Add("json", Bool(c.Defaults.JSON))
	d.Add("indent", Bool(c.Defaults.Indent))
	d.Add("quiet", Bool(c.Defaults.Quiet))
	d.Add("no-color", Bool(c.Defaults.NoColor))
	d.Add("metrics", Bool(c.Defaults.Metrics))
	if c.Defaults.LogLevel != "" {
		d.Add("log-level", Str(c.Defaults.LogLevel))
	}

	names := c.profileNames()
	t := r.Table("profiles", Col("name"), Col("directory"), RCol("keys"), Col("note"))
	for _, n := range names {
		p := c.Profiles[n]
		t.Row(Str(n), Str(p.Dir), Int(int64(len(p.PubKeys))), Str(p.Note))
	}
	if len(names) == 0 {
		r.Notice("no profiles yet: graphene profile add -dir <path> <name>")
	}

	r.Notes("what a config cannot set",
		"-confirm and -dry-run are command-line only. A config that could set",
		"-confirm would make the same command destructive on one machine and a",
		"refusal on another, with the difference in a file the command line does",
		"not show.")
	return r, nil
}

// --- config init ---

var configInit = plain(Command{
	Group: "config", Name: "init",
	Short: "write a starter config file",
	Long: "Refuses to overwrite an existing one. Use `config show` to see what is\n" +
		"there, and edit the file directly — there is no `config set`, because a\n" +
		"config small enough to be safe is small enough to edit by hand.",
	Open: OpenNone, Tier: CtxAdvisory,
}, runConfigInit)

func runConfigInit(cx *Context) (Result, error) {
	var r Result
	path, err := configPath()
	if err != nil {
		return r, err
	}
	if _, serr := os.Stat(path); serr == nil {
		return r, Usagef("%s already exists; edit it, or read it with `graphene config show`", path)
	}

	c := &Config{Version: 1, path: path, Profiles: map[string]Profile{}}
	if err := c.save(); err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("written", Str(path))
	r.Notice("add a store with: graphene profile add -dir <path> <name>")
	r.Verdict = VerdictVerified
	return r, nil
}

// --- profile list ---

var profileList = plain(Command{
	Group: "profile", Name: "list",
	Short: "the named stores in the config",
	Open:  OpenNone, Tier: CtxAdvisory,
}, runProfileList)

func runProfileList(cx *Context) (Result, error) {
	var r Result
	c, err := loadConfig()
	if err != nil {
		return r, err
	}
	names := c.profileNames()
	t := r.Table("profiles", Col("name"), Col("directory"), Col("exists"), RCol("keys"), Col("note"))
	for _, n := range names {
		p := c.Profiles[n]
		_, serr := os.Stat(p.Dir)
		t.Row(Str(n), Str(p.Dir), Bool(serr == nil), Int(int64(len(p.PubKeys))), Str(p.Note))
	}
	if len(names) == 0 {
		r.Section("").Add("profiles", Str("none"))
	}
	return r, nil
}

// --- profile add ---

type profileAddOpts struct {
	dir  string
	keys pubkeyList
	note string
}

var profileAdd = cmd(Command{
	Group: "profile", Name: "add", Usage: "-dir <path> <name>",
	Short: "name a store directory, and the keys that belong with it",
	Long: "The keys are what earns this. Every verification command takes -pubkey,\n" +
		"and typing an Ed25519 key by hand is the step that gets skipped — which\n" +
		"is why so many reports read \"signatures unchecked\". A profile supplies\n" +
		"them to any command that takes -pubkey and was given none, and every\n" +
		"report that used them says on stderr that it did.\n\n" +
		"The keys are public. Nothing here ever holds private material, and a\n" +
		"config file that did would be the wrong place for it.\n\n" +
		"A profile never shadows a real directory: a path that exists always wins,\n" +
		"so adding a profile cannot change what an existing command means.\n\n" +
		"The name goes last, after the flags — the same shape as every other\n" +
		"command here, and what the flag package requires: it stops parsing at\n" +
		"the first argument that is not a flag.",
	Open: OpenNone, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *profileAddOpts) {
		fs.StringVar(&o.dir, "dir", "", "the store directory (required)")
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
		fs.StringVar(&o.note, "note", "", "what this store is, for whoever reads the list")
	},
	runProfileAdd)

func runProfileAdd(cx *Context, o *profileAddOpts) (Result, error) {
	var r Result
	name := cx.Target
	if name == "" {
		return r, Usagef("need a name: graphene profile add -dir <path> <name>\n" +
			"  the name goes last, after the flags")
	}
	if o.dir == "" {
		return r, Usagef("need -dir <path>")
	}
	// Validated now rather than at use: a key that does not parse should be
	// rejected by the command that writes it, not by the verification command
	// three weeks later that was relying on it.
	if _, err := verifierFromFlag(o.keys); err != nil {
		return r, err
	}

	c, err := loadConfig()
	if err != nil {
		return r, err
	}
	if c.Profiles == nil {
		c.Profiles = map[string]Profile{}
	}
	_, replacing := c.Profiles[name]
	c.Profiles[name] = Profile{Dir: o.dir, PubKeys: o.keys, Note: o.note}

	if cx.Globals.DryRun {
		r.Section("would write").Add("profile", Str(name))
		return r, nil
	}
	if err := c.save(); err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("profile", Str(name))
	s.Add("directory", Str(o.dir))
	s.Add("keys", Int(int64(len(o.keys))))
	if replacing {
		s.Add("replaced an existing profile", Bool(true))
	}
	if _, serr := os.Stat(o.dir); serr != nil {
		// Not refused: naming a store before it exists is reasonable, and a
		// profile pointing at a directory that has not been created yet is a
		// perfectly ordinary state. Worth saying, because a typo looks the same.
		r.Find(SevWarn, "profile.dir_missing",
			"%s does not exist yet", o.dir)
	}
	r.Verdict = VerdictVerified
	return r, nil
}

// --- profile remove ---

var profileRemove = plain(Command{
	Group: "profile", Name: "remove", Usage: "<name>",
	Short: "forget a named store",
	Long:  "Removes the name. It never touches the directory the name pointed at.",
	Open:  OpenNone, Tier: CtxAdvisory,
}, runProfileRemove)

func runProfileRemove(cx *Context) (Result, error) {
	var r Result
	name := cx.Target
	if name == "" {
		return r, Usagef("need a name")
	}
	c, err := loadConfig()
	if err != nil {
		return r, err
	}
	p, ok := c.Profiles[name]
	if !ok {
		return r, Usagef("no profile named %q; `graphene profile list` shows what there is", name)
	}
	delete(c.Profiles, name)

	if cx.Globals.DryRun {
		r.Section("would remove").Add("profile", Str(name))
		return r, nil
	}
	if err := c.save(); err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("removed", Str(name))
	s.AddNote("the directory", Str(p.Dir), "(left exactly as it was)")
	r.Verdict = VerdictVerified
	return r, nil
}

// --- profile use ---

var profileUse = plain(Command{
	Group: "profile", Name: "use", Usage: "<name>",
	Short: "show what a name resolves to",
	Long: "There is no persistent \"current profile\", deliberately. A tool whose\n" +
		"commands act on a store selected by earlier, invisible state is a tool\n" +
		"whose most destructive command can be pointed at the wrong store by a\n" +
		"shell you forgot you had open.\n\n" +
		"Name the profile in the command instead: `graphene store info case01`.\n" +
		"This tells you what that will resolve to.",
	Open: OpenNone, Tier: CtxAdvisory,
}, runProfileUse)

func runProfileUse(cx *Context) (Result, error) {
	var r Result
	name := cx.Target
	if name == "" {
		return r, Usagef("need a name")
	}
	c, err := loadConfig()
	if err != nil {
		return r, err
	}
	p, ok := c.Profiles[name]
	if !ok {
		return r, Usagef("no profile named %q; `graphene profile list` shows what there is", name)
	}

	s := r.Section("")
	s.Add("profile", Str(name))
	s.Add("directory", Str(p.Dir))
	_, serr := os.Stat(p.Dir)
	s.Add("exists", Bool(serr == nil))
	s.Add("keys", Int(int64(len(p.PubKeys))))
	if p.Note != "" {
		s.Add("note", Str(p.Note))
	}
	r.Notes("use it as the operand",
		fmt.Sprintf("graphene store info %s", name),
		fmt.Sprintf("graphene debug integrity %s", name))
	return r, nil
}
