package main

// The human renderer.
//
// Keeps the shape the tool has always had: a tabwriter with two spaces of
// padding, key/value lines aligned in a column, blank lines between blocks. The
// output of `graphene info` should look the same after this change as before
// it, which is why the golden files were captured first.

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

type humanRenderer struct {
	verbose bool
}

// Render writes the whole document at once, after the handler has returned.
//
// That is a change worth naming: `redactions` used to print every record and
// then discover the hash chain was broken, so the bad news arrived after a
// screen of output that looked fine. Rendering from a finished document puts
// the verdict where the reader is already looking.
func (h humanRenderer) Render(w io.Writer, r Result) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	for i, s := range r.Sections {
		if i > 0 {
			fmt.Fprintln(tw, "\t")
		}
		if err := h.section(tw, s); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	h.verdict(w, r)
	return nil
}

func (h humanRenderer) section(tw *tabwriter.Writer, s Section) error {
	if s.Title != "" {
		fmt.Fprintf(tw, "%s\t\n", s.Title)
	}
	switch s.Kind {
	case KV:
		indent := ""
		if s.Title != "" {
			indent = "  "
		}
		for _, f := range s.Fields {
			line := f.Value.String()
			if f.Note != "" {
				line += " " + f.Note
			}
			fmt.Fprintf(tw, "%s%s\t%s\n", indent, f.Label, line)
		}

	case Rows:
		if len(s.Data) == 0 {
			fmt.Fprintf(tw, "  (none)\t\n")
			break
		}
		heads := make([]string, len(s.Cols))
		for i, c := range s.Cols {
			heads[i] = strings.ToUpper(c.Label)
		}
		fmt.Fprintln(tw, strings.Join(heads, "\t"))
		for _, row := range s.Data {
			cells := make([]string, len(row))
			for i, v := range row {
				cells[i] = v.String()
			}
			fmt.Fprintln(tw, strings.Join(cells, "\t"))
		}
		if s.Truncated {
			// One decision, reported once: the table knows it was cut short, so
			// this line cannot disagree with the JSON field beside it.
			fmt.Fprintf(tw, "…\t(%d shown of %d; -limit 0 for all)\n",
				len(s.Data), s.Total)
		}

	case Lines:
		// No tab. A prose line is not a cell, and giving it one pads it out to
		// whatever the widest key/value block on the page happens to be — the
		// summary of a custody report would sit in a column sized by the
		// snapshot root above it. This is also what the original code did.
		for _, t := range s.Text {
			fmt.Fprintf(tw, "  %s\n", t)
		}
	}
	return nil
}

// verdict writes the closing account: findings, then the one-line judgement.
func (h humanRenderer) verdict(w io.Writer, r Result) {
	if len(r.Findings) == 0 && r.Verdict == VerdictNone {
		return
	}
	if len(r.Findings) > 0 {
		fmt.Fprintln(w)
		for _, f := range r.Findings {
			marker := " "
			switch f.Severity {
			case SevBroken:
				marker = "!"
			case SevWarn:
				marker = "-"
			}
			fmt.Fprintf(w, "  %s %s\n", marker, f.Message)
		}
	}
	switch r.Verdict {
	case VerdictVerified:
		fmt.Fprintf(w, "\nVERIFIED\n")
	case VerdictFindings:
		// Not a failure. The wording matters: an operator who reads an
		// incomplete account as corruption starts reaching for repairs this
		// tool deliberately does not offer.
		fmt.Fprintf(w, "\n%d finding(s) — the account is incomplete, not broken\n",
			len(r.Findings))
	case VerdictBroken:
		fmt.Fprintf(w, "\nBROKEN\n")
	}
}
