package main

// The JSON renderer and the envelope.
//
// One schema for every command and every outcome, so a consumer parses once and
// switches on "status" rather than guessing from the exit code what shape it is
// holding. Notices that go to stderr in human mode move inside the document
// here: stdout carrying nothing but the document is the whole reason `-json`
// exists.

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
)

// SchemaVersion identifies the envelope shape. It changes only when a field is
// removed or its meaning changes — adding a field does not break a consumer
// that ignores unknown keys, and every consumer should.
const SchemaVersion = "graphene.cli/v1"

// Envelope is the JSON document.
type Envelope struct {
	Schema  string `json:"schema"`
	Command string `json:"command"`
	// Status is the extended classification: ok | findings | broken | error.
	// It carries the distinction the exit code deliberately does not, so a
	// caller that wants to tell a locked store from a corrupt one reads this.
	Status  string       `json:"status"`
	Exit    int          `json:"exit"`
	Target  string       `json:"target,omitempty"`
	Data    *object      `json:"data,omitempty"`
	Notices []string     `json:"notices,omitempty"`
	Finding []Finding    `json:"findings,omitempty"`
	Error   *ErrorObject `json:"error,omitempty"`
	Version VersionStamp `json:"version"`

	// Elapsed is omitted unless -verbose asked for it, and that is a
	// deliberate trade rather than an oversight. A wall-clock reading differs
	// between two runs of the same command against the same store, so
	// including it by default would mean no two invocations ever produce
	// identical bytes — no golden file, no `diff yesterday.json today.json`,
	// which is most of what an inspection tool is for. Timing belongs to
	// profiling, not to a report about a store.
	Elapsed string `json:"elapsed,omitempty"`
}

// ErrorObject describes a failure to a machine.
type ErrorObject struct {
	Message string `json:"message"`
	Kind    string `json:"kind"`
	Hint    string `json:"hint,omitempty"`
}

type jsonRenderer struct {
	indent bool
}

// Render writes the envelope.
func (j jsonRenderer) Render(w io.Writer, env Envelope) error {
	enc := json.NewEncoder(w)
	if j.indent {
		enc.SetIndent("", "  ")
	}
	// The document contains hex strings and prose, never HTML, and escaping
	// `<` into `<` makes a hash comparison in a shell script fail for a
	// reason nobody will guess.
	enc.SetEscapeHTML(false)
	return enc.Encode(env)
}

// envelopeFor assembles the document from a finished Result.
func envelopeFor(path string, r Result, elapsed time.Duration, exit int, ferr error, timings bool) Envelope {
	env := Envelope{
		Schema:  SchemaVersion,
		Command: path,
		Status:  r.Verdict.String(),
		Exit:    exit,
		Target:  r.Target,
		Notices: r.Notices,
		Finding: r.Findings,
		Version: stamp(),
	}
	if timings {
		env.Elapsed = elapsed.String()
	}
	if len(r.Sections) > 0 {
		env.Data = sectionsToObject(r.Sections)
	}
	if ferr != nil {
		kind, hint := classify(ferr)
		env.Status = "error"
		env.Error = &ErrorObject{Message: ferr.Error(), Kind: kind.String(), Hint: hint}
	}
	return env
}

// sectionsToObject turns the document into ordered JSON.
func sectionsToObject(sections []Section) *object {
	root := &object{}
	for _, s := range sections {
		k := key(s.Key, s.Title)
		if k == "" {
			k = "detail"
		}
		switch s.Kind {
		case KV:
			// Two untitled sections both key as "detail" — the human report
			// separates them with a blank line and JSON has no equivalent. They
			// are merged rather than one replacing the other, which is what a
			// plain Set would do: `store info` opens with an untitled block and
			// closes with another, and the second silently overwriting the
			// first dropped the store path out of the document entirely.
			o, ok := root.Get(k).(*object)
			if !ok {
				o = &object{}
				root.Set(k, o)
			}
			for _, f := range s.Fields {
				fk := key(f.Key, f.Label)
				o.Set(fk, f.Value)
				if f.Note != "" {
					o.Set(fk+"_note", f.Note)
				}
			}

		case Rows:
			rows := make([]*object, 0, len(s.Data))
			for _, row := range s.Data {
				o := &object{}
				for i, v := range row {
					if i >= len(s.Cols) {
						break
					}
					o.Set(key(s.Cols[i].Key, s.Cols[i].Label), v)
				}
				rows = append(rows, o)
			}
			if s.Truncated {
				root.Set(k, &object{
					keys: []string{"rows", "shown", "total", "truncated"},
					vals: []any{rows, len(rows), s.Total, true},
				})
			} else {
				root.Set(k, rows)
			}

		case Lines:
			root.Set(k, s.Text)
		}
	}
	return root
}

// object marshals in insertion order.
//
// A Go map would not: encoding/json sorts map keys, which is stable but not the
// order the report was written in, and the human and JSON renderings would then
// present the same facts in two different sequences for no reason. Insertion
// order keeps them the same document.
type object struct {
	keys []string
	vals []any
}

// Get returns the value stored under k, or nil.
func (o *object) Get(k string) any {
	for i, existing := range o.keys {
		if existing == k {
			return o.vals[i]
		}
	}
	return nil
}

// Set appends a key. A repeated key overwrites in place, keeping its position.
func (o *object) Set(k string, v any) {
	for i, existing := range o.keys {
		if existing == k {
			o.vals[i] = v
			return
		}
	}
	o.keys = append(o.keys, k)
	o.vals = append(o.vals, v)
}

// MarshalJSON writes the object in insertion order.
func (o *object) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(o.vals[i])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
