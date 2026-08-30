package main

// The output document.
//
// Every handler returns one Result, and both renderers walk it. That is the
// whole reason this file exists: the alternative — a human template per command
// plus reflection over the library's report structs — gives you two independent
// descriptions of the same output, and they drift. The command list in this
// package had already drifted three ways before anyone noticed, and that was a
// list of fifteen strings. A hundred fields would not stand a chance.
//
// Two library facts force a translation layer regardless, so it may as well be
// the shared one:
//
//   - merkle.Hash is [32]byte, and encoding/json renders that as a JSON array
//     of a hundred-odd characters. Handing disk.CSRInfo to json.Marshal
//     produces something no operator would recognise as a hash.
//   - disk.CapabilitiesFrom returns a map, and Go map iteration is randomised.
//     Output that reorders between runs cannot be diffed, which is most of what
//     an inspection tool is for. This package already hand-sorts for that
//     reason; an ordered document makes it structural instead of remembered.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aoiflux/graphene/merkle"
)

// Result is the whole of what a command produced.
type Result struct {
	// Target is the operand the command ran against — a store directory, a
	// proof file — echoed back so an archived report says what it describes.
	Target string

	Sections []Section
	Verdict  Verdict
	Findings []Finding

	// Notices are the asides that go to stderr in human mode: which lock is
	// about to be taken, that a dry run changed nothing. In JSON they move into
	// the envelope, because stdout carrying anything but the document is what
	// breaks `graphene ... -json | jq`.
	Notices []string
}

// Section adds a section and returns a pointer to it for filling in.
func (r *Result) Section(title string) *Section {
	r.Sections = append(r.Sections, Section{Title: title, Kind: KV})
	return &r.Sections[len(r.Sections)-1]
}

// Table adds a row-shaped section.
func (r *Result) Table(title string, cols ...Column) *Section {
	r.Sections = append(r.Sections, Section{
		Title: title, Kind: Rows, Cols: cols,
	})
	return &r.Sections[len(r.Sections)-1]
}

// Notes adds a section of prose lines — gaps, warnings, advice.
func (r *Result) Notes(title string, lines ...string) *Section {
	r.Sections = append(r.Sections, Section{Title: title, Kind: Lines, Text: lines})
	return &r.Sections[len(r.Sections)-1]
}

// Find records a finding: something the command noticed that the reader should
// know about but which is not, on its own, a failure.
func (r *Result) Find(sev Severity, code, format string, args ...any) {
	r.Findings = append(r.Findings, Finding{
		Code: code, Severity: sev, Message: fmt.Sprintf(format, args...),
	})
	if sev == SevBroken {
		r.Verdict = VerdictBroken
	} else if r.Verdict == VerdictVerified || r.Verdict == VerdictNone {
		r.Verdict = VerdictFindings
	}
}

// Notice records an aside. See Result.Notices.
func (r *Result) Notice(format string, args ...any) {
	r.Notices = append(r.Notices, fmt.Sprintf(format, args...))
}

// SectionKind distinguishes the three shapes a block of output can take.
type SectionKind uint8

const (
	// KV is a key/value report: the store summary, a CSR header.
	KV SectionKind = iota
	// Rows is a table: WAL records, ledger entries.
	Rows
	// Lines is prose: a report's gaps, a warning paragraph.
	Lines
)

// Section is one block of a Result.
type Section struct {
	Title string // heads the block in human output
	Key   string // JSON key; derived from Title when empty
	Kind  SectionKind

	Fields []Field
	Cols   []Column
	Data   [][]Value
	Text   []string

	// Truncated and Total describe a table that was cut short, so both
	// renderers report the same thing from one decision rather than two.
	Truncated bool
	Total     int
}

// Add appends a key/value line.
func (s *Section) Add(label string, v Value) *Section {
	s.Fields = append(s.Fields, Field{Label: label, Value: v})
	return s
}

// Addf appends a key/value line whose value is a formatted string.
func (s *Section) Addf(label, format string, args ...any) *Section {
	return s.Add(label, Str(fmt.Sprintf(format, args...)))
}

// AddNote appends a key/value line with a trailing parenthetical — the shape of
// the sparsity note and the "(verified: false)" suffixes.
func (s *Section) AddNote(label string, v Value, note string) *Section {
	s.Fields = append(s.Fields, Field{Label: label, Value: v, Note: note})
	return s
}

// Row appends a table row. The number of values must match the columns.
func (s *Section) Row(vs ...Value) *Section {
	s.Data = append(s.Data, vs)
	return s
}

// Line appends a prose line.
func (s *Section) Line(format string, args ...any) *Section {
	s.Text = append(s.Text, fmt.Sprintf(format, args...))
	return s
}

// Field is one key/value line.
type Field struct {
	Label string // human: "sequence high-water"
	Key   string // JSON: derived from Label when empty
	Value Value
	Note  string // trails the value in human output; "<key>_note" in JSON
}

// Column describes one column of a table.
type Column struct {
	Label string
	Key   string
	Right bool // right-align in human output
}

// Col is the usual constructor.
func Col(label string) Column { return Column{Label: label} }

// RCol is a right-aligned column, for numbers.
func RCol(label string) Column { return Column{Label: label, Right: true} }

// Value is a datum that carries both of its renderings.
//
// The mechanism is the whole point: a constructor fixes the human string and
// the JSON form together, at the one place the datum is created. Neither
// renderer ever decides how to format anything, so there is no second place for
// a decision to be made differently.
type Value struct {
	text string // human rendering, already formatted
	j    any    // what encoding/json sees; nil means "the text, as a string"
}

// String renders the value for a human.
func (v Value) String() string { return v.text }

// MarshalJSON renders the value for a machine.
func (v Value) MarshalJSON() ([]byte, error) {
	if v.j == nil {
		return json.Marshal(v.text)
	}
	return json.Marshal(v.j)
}

// Str is a plain string, the same in both renderings.
func Str(s string) Value { return Value{text: s, j: s} }

// Int is a number: "%d" for a human, a JSON number for a machine.
func Int(n int64) Value { return Value{text: fmt.Sprintf("%d", n), j: n} }

// Uint is an unsigned number.
func Uint(n uint64) Value { return Value{text: fmt.Sprintf("%d", n), j: n} }

// ID is an entity identifier. Distinct from Uint only in intent — it marks the
// values a reader would paste back into another command.
func ID(n uint64) Value { return Uint(n) }

// Bool renders true/false in both.
func Bool(b bool) Value { return Value{text: fmt.Sprintf("%v", b), j: b} }

// Nil is an absent value: "-" for a human, null for a machine.
func Nil() Value { return Value{text: "-", j: nil2{}} }

// nil2 exists because a nil `j` means "marshal the text"; an explicit JSON null
// needs a distinct sentinel.
type nil2 struct{}

func (nil2) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

// Errv renders an error.
func Errv(err error) Value {
	if err == nil {
		return Nil()
	}
	return Str(err.Error())
}

// Hash renders a Merkle root as lowercase hex in both forms. This is the
// constructor that keeps [32]byte from reaching encoding/json as an array.
func Hash(h merkle.Hash) Value {
	s := fmt.Sprintf("%x", h[:])
	return Value{text: s, j: s}
}

// Hex renders a byte slice as lowercase hex in both forms.
func Hex(b []byte) Value {
	if len(b) == 0 {
		return Nil()
	}
	s := fmt.Sprintf("%x", b)
	return Value{text: s, j: s}
}

// Bytes is a size. It emits an object in JSON deliberately: a script wants the
// number and a human reading jq output wants "1.4 MiB", and picking one loses
// the other for no gain — the field is already nested.
func Bytes(n int64) Value {
	return Value{
		text: humanBytes(n),
		j:    map[string]any{"bytes": n, "human": humanBytes(n)},
	}
}

// Time is an instant, given in Unix nanoseconds. Zero is absent.
func Time(unixNano int64) Value {
	if unixNano == 0 {
		return Nil()
	}
	t := time.Unix(0, unixNano).UTC()
	return Value{
		text: t.Format(time.RFC3339Nano),
		j: map[string]any{
			"unix_nano": unixNano,
			"rfc3339":   t.Format(time.RFC3339Nano),
		},
	}
}

// Dur is an elapsed time.
func Dur(d time.Duration) Value {
	return Value{
		text: d.String(),
		j:    map[string]any{"nanos": int64(d), "human": d.String()},
	}
}

// Count is a number with a unit for the human rendering — "3 records" — and a
// bare number for JSON.
func Count(n int64, unit string) Value {
	return Value{text: fmt.Sprintf("%d %s", n, plural(n, unit)), j: n}
}

func plural(n int64, unit string) string {
	if n == 1 || strings.HasSuffix(unit, "s") {
		return unit
	}
	return unit + "s"
}

// key derives a JSON key from a human label: "sequence high-water" becomes
// "sequence_high_water". Explicit keys win, so a label can be reworded without
// silently renaming a field a script depends on.
func key(explicit, label string) string {
	if explicit != "" {
		return explicit
	}
	var b strings.Builder
	b.Grow(len(label))
	prevUnderscore := false
	for _, r := range strings.ToLower(label) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		default:
			if !prevUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				prevUnderscore = true
			}
		}
	}
	return strings.TrimSuffix(b.String(), "_")
}
