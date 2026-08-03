package disk

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// Crash injection against WAL replay — the executable half of plan hook V-02.
//
// The property under test is PO-1 / INV-6: replay of a log truncated at ANY byte
// offset yields a state reachable by some prefix of the committed transaction
// sequence, and never one containing a partially-applied batch.
//
// This is exhaustive rather than sampled. A crash can interrupt a write at any
// byte, so every byte is a truncation point worth checking, and the log is small
// enough that checking all of them is cheaper than arguing about which ones
// matter. Exhaustive over the real implementation is also why this is worth
// having even once a TLA+ model exists: the model checks the design, this checks
// the code.

// crashFixture builds a log whose every record identifies the batch it belongs
// to, so a partially-applied batch is detectable rather than merely suspected.
//
// Shape: a loose record, two committed batches, another loose record, then a
// batch left open. The open batch is the crash-mid-transaction case, and replay
// must discard it at every truncation point including the last.
func crashFixture(t *testing.T) (log []byte, batchSizes map[string]int) {
	t.Helper()

	batchSizes = map[string]int{}
	var buf []byte

	buf = appendRecord(buf, walRecordNode, []byte("loose-1"))

	for _, b := range []struct {
		name  string
		count int
	}{{"A", 3}, {"B", 2}} {
		wb := newWALBatch(64)
		for i := 0; i < b.count; i++ {
			wb.add(walRecordNode, []byte(fmt.Sprintf("batch-%s-%d", b.name, i)))
		}
		buf = append(buf, mustFinish(wb, batchMeta{CommitSeq: uint64(len(batchSizes) + 1)})...)
		batchSizes[b.name] = b.count
	}

	buf = appendRecord(buf, walRecordEdge, []byte("loose-2"))

	// A batch that begins and never commits.
	open := newWALBatch(64)
	open.add(walRecordNode, []byte("batch-OPEN-0"))
	framed := mustFinish(open, batchMeta{CommitSeq: 99})
	buf = append(buf, framed[:len(framed)-(walRecordOverhead+walBatchCommitPayloadV2)]...)

	return buf, batchSizes
}

// replayApplied returns the payloads replay applied, in order.
func replayApplied(data []byte) ([]string, error) {
	var got []string
	collect := func(p []byte) error {
		got = append(got, string(p))
		return nil
	}
	err := replayRecords(bytes.NewReader(data), int64(len(data)), walFramingV1, ReplayCallbacks{
		NodeFunc: collect, EdgeFunc: collect,
		NodePropFunc: collect, EdgePropFunc: collect,
		NodeDeleteFunc: collect, EdgeDeleteFunc: collect,
		NodePropPurgeFunc: collect, EdgePropPurgeFunc: collect,
	})
	return got, err
}

// Truncating anywhere must never yield a half-applied batch.
func TestReplay_TruncationAtEveryOffsetIsAllOrNothing(t *testing.T) {
	full, batchSizes := crashFixture(t)

	for cut := 0; cut <= len(full); cut++ {
		applied, err := replayApplied(full[:cut])
		if err != nil {
			t.Fatalf("cut=%d: replay errored on a truncated log, which is a normal crash shape: %v", cut, err)
		}

		counts := map[string]int{}
		for _, p := range applied {
			if !strings.HasPrefix(p, "batch-") {
				continue
			}
			name := strings.Split(strings.TrimPrefix(p, "batch-"), "-")[0]
			counts[name]++
		}

		for name, n := range counts {
			if name == "OPEN" {
				t.Fatalf("cut=%d: applied %d records from a batch that never committed", cut, n)
			}
			want, known := batchSizes[name]
			if !known {
				t.Fatalf("cut=%d: applied records from unknown batch %q", cut, name)
			}
			if n != want {
				t.Fatalf("cut=%d: batch %s applied %d of %d records — a batch must be all or nothing",
					cut, name, n, want)
			}
		}
	}
}

// The applied set must always be a prefix of what the full log applies. Replay
// may lose the tail of a truncated log; it may never reorder, invent, or skip.
func TestReplay_TruncatedResultIsAlwaysAPrefix(t *testing.T) {
	full, _ := crashFixture(t)

	complete, err := replayApplied(full)
	if err != nil {
		t.Fatalf("replay of the intact log: %v", err)
	}

	for cut := 0; cut <= len(full); cut++ {
		applied, err := replayApplied(full[:cut])
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if len(applied) > len(complete) {
			t.Fatalf("cut=%d: applied %d records, more than the intact log's %d",
				cut, len(applied), len(complete))
		}
		for i, p := range applied {
			if p != complete[i] {
				t.Fatalf("cut=%d: record %d is %q, but the intact log applies %q there",
					cut, i, p, complete[i])
			}
		}
	}
}

// Truncation is monotonic: reading more of a log can only ever apply more.
// A longer prefix that applies fewer records would mean later bytes changed the
// interpretation of earlier ones.
func TestReplay_LongerPrefixNeverAppliesLess(t *testing.T) {
	full, _ := crashFixture(t)

	prev := 0
	for cut := 0; cut <= len(full); cut++ {
		applied, err := replayApplied(full[:cut])
		if err != nil {
			t.Fatalf("cut=%d: %v", cut, err)
		}
		if len(applied) < prev {
			t.Fatalf("cut=%d applied %d records; the shorter prefix applied %d",
				cut, len(applied), prev)
		}
		prev = len(applied)
	}
}

// markerTypeOffsets returns the file offsets of every batch marker's type byte.
//
// These are the bytes the framing cannot defend — see
// TestReplay_CorruptingABatchMarkerTypeDefeatsBatching, which characterises why.
func markerTypeOffsets(log []byte) map[int]bool {
	out := map[int]bool{}
	off := 0
	for off+walHeaderSize <= len(log) {
		recType := log[off]
		length := int(log[off+1]) | int(log[off+2])<<8 | int(log[off+3])<<16 | int(log[off+4])<<24
		if recType == walRecordBatchBegin || recType == walRecordBatchCommit {
			out[off] = true
		}
		next := off + walRecordOverhead + length
		if next <= off || next > len(log) {
			break
		}
		off = next
	}
	return out
}

// Corrupting any single byte must never produce a partially-applied batch —
// everywhere the framing is able to defend.
//
// The batch markers' own type bytes are excluded and tested separately. That is
// not the test avoiding an inconvenience: it is the boundary of what this
// framing guarantees, and the test below states it explicitly so it stays
// visible.
func TestReplay_SingleByteCorruptionIsAllOrNothing(t *testing.T) {
	full, batchSizes := crashFixture(t)
	markers := markerTypeOffsets(full)

	for pos := 0; pos < len(full); pos++ {
		if markers[pos] {
			continue
		}
		for _, mask := range []byte{0x01, 0x80, 0xFF} {
			damaged := append([]byte(nil), full...)
			damaged[pos] ^= mask

			applied, err := replayApplied(damaged)
			if err != nil {
				// A rejected log is a fine outcome — replay may refuse what it
				// cannot trust. What it may not do is half-apply.
				continue
			}

			counts := map[string]int{}
			for _, p := range applied {
				if !strings.HasPrefix(p, "batch-") {
					continue
				}
				name := strings.Split(strings.TrimPrefix(p, "batch-"), "-")[0]
				counts[name]++
			}
			for name, n := range counts {
				if name == "OPEN" {
					t.Fatalf("byte %d ^ %#02x: applied %d records from an uncommitted batch",
						pos, mask, n)
				}
				if want, known := batchSizes[name]; known && n != want {
					t.Fatalf("byte %d ^ %#02x: batch %s applied %d of %d records",
						pos, mask, name, n, want)
				}
			}
		}
	}
}

// A known gap, characterised rather than hidden: a single bit flip in a batch
// marker's TYPE byte defeats batch atomicity.
//
// The framing is [type:1][length:4][payload][crc32:4] and the CRC covers the
// payload only, so neither the type nor the length is checksummed. For records
// *inside* a batch that does not matter — the commit's body CRC is taken over
// appendRecord(body, recType, payload), which includes the type. The marker
// records are the exception, because nothing downstream can corroborate them:
// turn a 0x09 begin marker into 0x08 with one flipped bit and replay never
// enters batch mode, so the records that follow are applied individually. The
// batch that was supposed to be all-or-nothing becomes all-applied, and its CRC
// verifies the whole way.
//
// The exposure is narrower than that description alone suggests, and the
// difference is worth stating because it is what bounds the severity:
//
//   - A COMMITTED batch whose begin marker is corrupted fails CLOSED. Its
//     records are applied loose, but the commit marker then arrives with no
//     begin to match and replay returns "batch commit without begin". The log is
//     rejected rather than half-applied.
//   - An UNCOMMITTED batch whose begin marker is corrupted fails OPEN. There is
//     no commit marker to expose the inconsistency, so its records — a
//     transaction that never committed — are applied as ordinary ones.
//
// So the reachable case is exactly the batch in flight when the process died:
// the one the begin/commit machinery exists to discard. It needs corruption at
// one specific byte, and against a deliberate edit CRC32 is no defence anyway
// (keyless, recomputable). It is a real hole in the *accident* story, which is
// the one these markers are for.
//
// **This is now fixed for new logs.** The container header added in
// walcontainer.go versions the framing, and v2 checksums the record header along
// with the payload — see TestWALContainer_V2DetectsATypeByteFlip, which flips a
// type byte in a real log and confirms the record no longer verifies.
//
// The gap remains for a headerless log written before the container existed,
// because changing what its CRCs cover would invalidate every record already in
// it. Such a log keeps v1 framing until a compaction replaces it, which is the
// migration path. This fixture is hand-built under v1, so it still exercises
// that case — which is the case that still exists.
//
// If this starts failing, v1 framing has changed and the test should be deleted
// rather than repaired.
func TestReplay_CorruptingABatchMarkerTypeDefeatsBatching(t *testing.T) {
	full, _ := crashFixture(t)

	// The LAST begin marker, which is the uncommitted batch — the case that
	// fails open. Corrupting an earlier, committed one is caught by its orphaned
	// commit marker instead.
	beginOffset := -1
	off := 0
	for off+walHeaderSize <= len(full) {
		length := int(full[off+1]) | int(full[off+2])<<8 | int(full[off+3])<<16 | int(full[off+4])<<24
		if full[off] == walRecordBatchBegin {
			beginOffset = off
		}
		next := off + walRecordOverhead + length
		if next <= off {
			break
		}
		off = next
	}
	if beginOffset < 0 {
		t.Fatal("fixture has no batch-begin marker")
	}

	damaged := append([]byte(nil), full...)
	damaged[beginOffset] ^= 0x01 // 0x09 batch-begin -> 0x08 edge-prop-purge

	applied, err := replayApplied(damaged)
	if err != nil {
		t.Skipf("this corruption is now rejected outright (%v) — the gap may be closed", err)
	}

	intact, err := replayApplied(full)
	if err != nil {
		t.Fatalf("intact log: %v", err)
	}
	if len(applied) == len(intact) {
		t.Fatal("flipping a begin marker's type byte no longer changes what replay applies — " +
			"the gap appears to be closed; delete this test and update the plan")
	}
	t.Logf("known gap holds: flipping the begin marker at byte %d changed the applied count "+
		"from %d to %d, because the record type is outside the CRC",
		beginOffset, len(intact), len(applied))
}

// Replay is deterministic: the same bytes must always yield the same result,
// at every truncation point. INV-5.
func TestReplay_IsDeterministicAtEveryTruncation(t *testing.T) {
	full, _ := crashFixture(t)

	for cut := 0; cut <= len(full); cut++ {
		first, err1 := replayApplied(full[:cut])
		second, err2 := replayApplied(full[:cut])
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("cut=%d: replay succeeded once and failed once: %v / %v", cut, err1, err2)
		}
		if len(first) != len(second) {
			t.Fatalf("cut=%d: applied %d records then %d from identical bytes",
				cut, len(first), len(second))
		}
		for i := range first {
			if first[i] != second[i] {
				t.Fatalf("cut=%d: record %d differed between runs: %q vs %q",
					cut, i, first[i], second[i])
			}
		}
	}
}
