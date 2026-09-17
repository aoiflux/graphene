package disk

// A compaction that writes the image without building one.
//
// # What this is for
//
// CompactAndReopen compacts, closes the store and opens the directory again. So
// the CSRGraph the compaction builds is constructed, serialised, published, and
// then thrown away by the Close on the very next line -- and it is the largest
// thing a compaction holds. docs/PLAN_BOUNDED_INGEST.md Phase 4 puts it at
// roughly 118 bytes per node and 142 per edge, live for the whole write, and the
// stage metrics put a figure on the whole stage: at 400,000 records the build
// raises the process peak by 197.9 MiB and the commit after it by 0.035.
//
// This path does not build one. The merged record sequence the build already
// walks goes straight into the encoder, and what comes back out is the three
// facts the commit needs -- the counts and the snapshot root.
//
// # Why the other path keeps its graph
//
// Compact() has to publish what it built. A store that stays open serves reads
// from a view, a view holds a CSRGraph, and after a compaction that graph must
// be the new image or the store is serving the old one. Building it is the only
// way to have it: re-reading the file the compaction just wrote would map an
// image the store already has the records for, which is a second copy and a
// second parse. So Compact() is unchanged, deliberately, and this is reachable
// only from CompactAndReopenCtx.
//
// # What it costs, and what pays for it
//
// The store's in-memory view is left standing. It is the pinned image plus the
// whole delta, where a published compaction would have left the new image plus
// the tail -- the same records either way, because the merge that wrote the
// image is the merge of exactly those two things. A read through the stale view
// is correct. A write lands in the delta and in the fresh log, and the reopen
// reads image-plus-replay, which is correct too. What is lost is the memory the
// splice would have released and the index base the commit would have adopted,
// both of which the reopen takes back in full.
//
// So the handle is consistent but no longer economical, which is exactly the
// bargain a caller makes by asking for a reopen. CompactAndReopenCtx closes it
// on the next line. Nothing else may call this.
//
// # The one invariant the encoder cannot check
//
// buildSeq refused a duplicate identifier, because it placed every record in a
// slot and found the slot taken. Nothing places anything here, so the check is
// the merge's own ordering: the sequence is ascending by construction, so an
// identifier that does not exceed the last one is either a duplicate or a merge
// that has gone wrong, and either way it must not reach the file. An iter.Seq
// cannot return an error, so the violation is latched and the walk stopped, and
// buildStream reports it after the encoder returns -- while the image is still a
// temp file nobody has been told about.

import (
	"context"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	"github.com/aoiflux/graphene/store"
)

// orderedRecords is the merge with its ordering checked on the way past.
//
// One instance serves one build: the encoder walks nodes once and edges once,
// and the latch is read after both. It is not safe for two builds at a time and
// does not need to be -- a compaction holds the pin.
type orderedRecords struct {
	p *compactPlan

	// err is the first violation seen, and the first is the only one worth
	// having: the walk stops there.
	err error
}

// nodes is p.nodeSeq with ascending order enforced.
func (o *orderedRecords) nodes() iter.Seq[nodeRecord] {
	return func(yield func(nodeRecord) bool) {
		var last store.NodeID
		first := true
		for n := range o.p.nodeSeq() {
			if !first && n.ID <= last {
				o.err = fmt.Errorf("compact: node records out of order at ID %d after %d", n.ID, last)
				return
			}
			last, first = n.ID, false
			if !yield(n) {
				return
			}
		}
	}
}

// edges is nodes for edges.
func (o *orderedRecords) edges() iter.Seq[rawEdge] {
	return func(yield func(rawEdge) bool) {
		var last store.EdgeID
		first := true
		for e := range o.p.edgeSeq() {
			if !first && e.ID <= last {
				o.err = fmt.Errorf("compact: edge records out of order at ID %d after %d", e.ID, last)
				return
			}
			last, first = e.ID, false
			if !yield(e) {
				return
			}
		}
	}
}

// buildStream writes the image the plan describes, without building a graph.
//
// It is compactPlan.build with the graph taken out, and the differences are
// worth naming because they are the whole of the change:
//
//   - the encoder is handed the merge rather than a graph's arena walk, and is
//     told it does not know the header counts, which it patches after the flush;
//   - the property index filters against the plan rather than the new image,
//     which answers the same question from the merge's own inputs; and
//   - nothing prepares a mapped index base. That work exists so an open store
//     can read its index out of the file it just wrote, and this store is about
//     to be closed -- mapping the image and parsing its directory here would be
//     paid for and then dropped.
//
// Everything else -- the sort, the cancellation points, the step hooks, the
// temp name, the sync before the commit -- is what build does, in the order
// build does it.
func (p *compactPlan) buildStream(ctx context.Context, dir string) (imageIdentity, string, error) {
	var out imageIdentity

	p.sortDelta()
	if err := ctx.Err(); err != nil {
		return out, "", err
	}

	// The filter's oracle is the plan itself. See imageMembers for why that
	// answers the same question the new graph would have, and what it costs.
	if p.indexMode == IndexMapped {
		p.payload.MappedIndex = p.mappedIndexSource(p, dir)
		p.payload.Composites = p.mappedCompositeSource(p, dir)
	} else {
		p.payload.NodeProps = p.nodePropSeq(p)
		p.payload.EdgeProps = p.edgePropSeq(p)
	}

	if err := ctx.Err(); err != nil {
		return out, "", err
	}
	if err := p.fireStep(compactStepSerialise); err != nil {
		return out, "", err
	}

	tmpPath := filepath.Join(dir, csrFileName+".tmp")
	if err := p.fireStep(compactStepWriteTmp); err != nil {
		return out, "", err
	}

	recs := &orderedRecords{p: p}
	beforeSync := func() error { return p.fireStep(compactStepSyncTmp) }
	write := func(f *os.File) error {
		id, err := writeImage(f, p.imageInput(recs), p.payload)
		out = id
		return err
	}
	if err := writeStreamSync(tmpPath, 0600, beforeSync, nil, write); err != nil {
		removeTmpImage(tmpPath)
		return imageIdentity{}, "", fmt.Errorf("compact: write tmp CSR: %w", err)
	}
	// After the write rather than during it, because an iter.Seq has no way to
	// fail: the walk stopped at the violation and the encoder finished writing a
	// file that is short by every record after it. That file is never installed.
	if recs.err != nil {
		removeTmpImage(tmpPath)
		return imageIdentity{}, "", recs.err
	}
	return out, tmpPath, nil
}

// imageInput is the plan in the terms the encoder needs it.
//
// countUnknown for both counts is the one difference from a graph's, and it is
// not a shortcut: counting the merge would mean a pass over the whole pinned
// image before the first byte of the new one went down, which on a store this
// exists to bound is the read the single pass was for.
func (p *compactPlan) imageInput(recs *orderedRecords) imageInput {
	return imageInput{
		nodes:               recs.nodes(),
		edges:               recs.edges(),
		nodeCount:           countUnknown,
		edgeCount:           countUnknown,
		nodeSeqHW:           p.nodeSeqHW,
		edgeSeqHW:           p.edgeSeqHW,
		commitSeqHW:         p.commitSeqHW,
		lastCompactUnixNano: p.compactedAt.UnixNano(),
	}
}

// compactStreamCommit installs a streamed image and stops.
//
// It is compactCommit without the publish: there is no graph to publish, and the
// file comment above says what that leaves standing and why it is consistent.
// The epoch is still published, and that is not an oversight either -- a commit
// that was written but not yet fsynced when the compaction started is durable in
// the image now, and the log that held it has just been retired. Leaving the
// applied epoch behind would make those records invisible to this handle for as
// long as it lives, which is short but is not zero.
func (s *Store) compactStreamCommit(p *compactPlan, img imageIdentity, tmpPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.installImage(p, img, tmpPath); err != nil {
		return err
	}
	s.publishEpoch(s.mutEpoch.Load())
	return nil
}
