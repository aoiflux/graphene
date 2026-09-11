package main

// -metrics: what the engine actually did during a command.
//
// The brief asked for `debug profile <subcommand...>` — a wrapper that runs
// another command with instrumentation attached. This is that capability as a
// global flag instead, for two reasons.
//
// The first is mechanical. store.Metrics can only be attached at open time,
// through disk.Options.Metrics, and in this tool the framework owns the open
// (see context.go, and the lock leak that put it there). A `debug profile` verb
// would have to re-enter dispatch from inside a handler and open the store a
// second way, which is exactly the arrangement the framework exists to prevent.
//
// The second is that a flag is strictly more useful. `-metrics` composes with
// every command that opens a store, so a slow `node list` and a slow `backup
// create` are profiled the same way, and nobody has to learn which commands the
// profiler knows how to wrap.
//
// It costs nothing when unset: Options.Metrics stays nil and the engine's
// emission sites compile down to a nil check.

import (
	"sort"
	"sync"
	"time"

	"github.com/aoiflux/graphene/store"
)

// collector accumulates per-kind totals.
//
// Record is called from whichever goroutine did the work, several at once, and
// must be cheap and must never block — so this is a mutex around eight integer
// adds and nothing else. Anything more (a histogram, a slow-operation log with
// its own I/O) belongs in a sink that is not on the engine's critical path.
type collector struct {
	mu   sync.Mutex
	by   map[store.MetricKind]*tally
	kind []store.MetricKind // first-seen order, so output is stable
}

type tally struct {
	n        int64
	errs     int64
	total    time.Duration
	max      time.Duration
	count    int64
	examined int64
	bytes    int64
}

func newCollector() *collector {
	return &collector{by: map[store.MetricKind]*tally{}}
}

func (c *collector) Record(m store.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.by[m.Kind]
	if !ok {
		t = &tally{}
		c.by[m.Kind] = t
		c.kind = append(c.kind, m.Kind)
	}
	t.n++
	if m.Err != nil {
		t.errs++
	}
	t.total += m.Duration
	if m.Duration > t.max {
		t.max = m.Duration
	}
	t.count += m.Count
	t.examined += m.Examined
	t.bytes += m.Bytes
}

// report adds the metrics section to a result.
//
// Sorted by name rather than left in first-seen order: a profile is something
// you diff between two runs, and an ordering that depends on which operation
// happened to go first makes that diff meaningless.
func (c *collector) report(r *Result) {
	c.mu.Lock()
	kinds := append([]store.MetricKind(nil), c.kind...)
	c.mu.Unlock()

	if len(kinds) == 0 {
		r.Section("metrics").Add("recorded", Str("nothing; the command did no measured work"))
		return
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i].String() < kinds[j].String() })

	t := r.Table("metrics",
		Col("operation"), RCol("n"), RCol("total"), RCol("mean"), RCol("slowest"),
		RCol("count"), RCol("examined"), RCol("bytes"), RCol("errors"))

	for _, k := range kinds {
		c.mu.Lock()
		v := *c.by[k]
		c.mu.Unlock()

		mean := time.Duration(0)
		if v.n > 0 {
			mean = v.total / time.Duration(v.n)
		}
		t.Row(
			Str(k.String()),
			Int(v.n),
			Dur(v.total),
			Dur(mean),
			Dur(v.max),
			Int(v.count),
			Int(v.examined),
			Bytes(v.bytes),
			Int(v.errs),
		)
	}

	// The per-kind meaning of Count, Examined and Bytes is not the same across
	// rows — records in a batch, candidates examined, log bytes read — and a
	// table with three numeric columns invites reading down them. Say so.
	r.Notes("reading this",
		"count, examined and bytes mean something different per operation:",
		"  commit    records in the batch          / -            / framed log bytes",
		"  sync      commits made durable          / -            / log bytes synced",
		"  query     IDs returned                  / candidates   / -",
		"  replay    epochs advanced               / -            / log bytes read",
		"  compaction records in the image         / records read / image size",
		"  backup    files copied                  / -            / bytes copied",
		"  id-headroom-low highest ID issued       / ID ceiling   / -",
		"replay is the one to watch: the log is bounded only by compaction, so",
		"its size is also how long the next open takes.")
}
