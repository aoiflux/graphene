package disk

// The image digest, computed without the image in memory.
//
// The check has two callers that matter and both of them arrive in a hurry:
// `graphene store csr -verify`, which an operator runs on a store it is not sure
// about, and the tail of every backup, which runs on a store whose machine may
// be the reason a backup is being taken. Reading the file whole to hash it asks
// for the graph's footprint a second time at exactly the wrong moment, so the
// hash is computed as the bytes go past.
//
// That splits one function into two implementations of the same answer, which is
// the hazard these tests exist for: a file whose digest verified through one path
// and failed through the other would be unexplainable to whoever had to act on
// it. So they are compared directly, over every shape that can tell them apart
// and then over arbitrary bytes.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// digestImage returns the bytes of a compacted image.
func digestImage(t *testing.T) []byte {
	t.Helper()
	dir, s := v8Fixture(t)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// withByteAt returns a copy of data with one byte changed.
func withByteAt(data []byte, at int, to byte) []byte {
	out := make([]byte, len(data))
	copy(out, data)
	out[at] = to
	return out
}

// TestCSRDigest_StreamedMatchesWholeBuffer is the agreement the split rests on.
//
// Every shape here is one the pair could disagree about: a file too short to
// carry a header, a version that predates the digest, a body that changed, the
// two header fields the digest deliberately does not cover, and the boundary
// where the header ends and the streamed remainder begins. The status *and* the
// computed digest are compared, because a path that returned the right verdict
// from the wrong bytes would pass a status-only check and still be wrong.
func TestCSRDigest_StreamedMatchesWholeBuffer(t *testing.T) {
	image := digestImage(t)
	if len(image) <= csrV8HeaderSize {
		t.Fatalf("fixture image is %d bytes, too small to have a body to stream", len(image))
	}

	preV8 := withByteAt(image, 4, byte(csrVersionWithPropIndex))

	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"one byte", []byte{'G'}},
		{"one byte short of a header", image[:csrV8HeaderSize-1]},
		{"header and nothing else", image[:csrV8HeaderSize]},
		{"header and one body byte", image[:csrV8HeaderSize+1]},
		{"whole image", image},
		{"pre-v8 version", preV8},
		{"body tampered", withByteAt(image, len(image)-1, image[len(image)-1]^0xff)},
		{"first body byte tampered", withByteAt(image, csrV8HeaderSize, image[csrV8HeaderSize]^0xff)},
		{"last header byte tampered", withByteAt(image, csrV8HeaderSize-1, image[csrV8HeaderSize-1]^0xff)},
		{"timestamp changed", withByteAt(image, csrLastCompactOffset, image[csrLastCompactOffset]^0xff)},
		{"stored digest changed", withByteAt(image, csrDigestOffset, image[csrDigestOffset]^0xff)},
		{"counts changed", withByteAt(image, 6, image[6]^0xff)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantStatus, wantDigest := csrDigestStatus(tc.data)
			gotStatus, gotDigest, err := csrDigestStatusOf(bytes.NewReader(tc.data))
			if err != nil {
				t.Fatalf("streamed: %v", err)
			}
			if gotStatus != wantStatus {
				t.Errorf("streamed status %v, whole-buffer status %v", gotStatus, wantStatus)
			}
			if gotDigest != wantDigest {
				t.Errorf("streamed digest %x, whole-buffer digest %x", gotDigest, wantDigest)
			}
		})
	}
}

// The timestamp is excluded from the digest and the streamed path must exclude
// the same eight bytes — a boundary a second implementation is likely to get
// wrong by one either way, and getting it wrong would make a recompacted but
// unchanged store report a different identity.
func TestCSRDigest_StreamedExcludesTheTimestamp(t *testing.T) {
	image := digestImage(t)

	status, before, err := csrDigestStatusOf(bytes.NewReader(image))
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Fatalf("fixture digest status %v, want match", status)
	}

	stamped := make([]byte, len(image))
	copy(stamped, image)
	binary.LittleEndian.PutUint64(stamped[csrLastCompactOffset:], 0xdeadbeefcafef00d)

	status, after, err := csrDigestStatusOf(bytes.NewReader(stamped))
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Errorf("changing the timestamp broke the streamed digest (status %v)", status)
	}
	if after != before {
		t.Errorf("streamed digest moved with the timestamp: %x then %x", before, after)
	}
}

// errAfter is a reader that fails partway, so a read error on the body is
// distinguishable from a file that is simply short.
type errAfter struct {
	r io.Reader
	n int
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, errors.New("disk: injected read failure")
	}
	if len(p) > e.n {
		p = p[:e.n]
	}
	n, err := e.r.Read(p)
	e.n -= n
	return n, err
}

// A short file is an answer; a failed read is an error. Collapsing the two would
// report a damaged disk as "this image carries no digest", which is the reading
// an operator would take as reassurance.
func TestCSRDigest_ReadFailureIsNotAnAbsentDigest(t *testing.T) {
	image := digestImage(t)

	for _, at := range []int{0, 10, csrV8HeaderSize, csrV8HeaderSize + 1} {
		status, _, err := csrDigestStatusOf(&errAfter{r: bytes.NewReader(image), n: at})
		if err == nil {
			t.Errorf("read failing after %d bytes returned status %v and no error", at, status)
		}
	}
}

// TestVerifyCSRDigest_DoesNotReadTheImageWhole is the guard on the property the
// split was made for.
//
// Total allocation rather than resident heap: it is cumulative and exact, so it
// does not move with GC timing, and a single os.ReadFile of the image would show
// up in it as one allocation the size of the file. The ceiling is far below the
// image and far above what a copy buffer and a hash state cost, so there is no
// shape of this test that passes for a whole-file read.
func TestVerifyCSRDigest_DoesNotReadTheImageWhole(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Big enough that reading it whole is unmistakable in the figures, small
	// enough to build in well under a second.
	const (
		nodes   = 2000
		payload = 4096
	)
	batch := make([]*store.Node, nodes)
	for i := range batch {
		batch[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: make([]byte, payload),
		}
	}
	if _, err := s.AddNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatal(err)
	}
	const ceiling = 1 << 20
	if info.Size() < 4*ceiling {
		// Not a pass: an image smaller than the ceiling would satisfy the
		// assertion below even if the whole of it were read into memory, which
		// is the test-that-cannot-fail CONTRIBUTING section 2 warns about.
		t.Fatalf("image is %d bytes, too small for the %d-byte ceiling to mean anything",
			info.Size(), ceiling)
	}

	// One call first: the first hash of the process allocates state that has
	// nothing to do with the file, and charging it to the measurement would
	// measure the runtime rather than the code.
	if _, _, err := VerifyCSRDigest(dir); err != nil {
		t.Fatal(err)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	status, _, err := VerifyCSRDigest(dir)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Fatalf("digest status %v, want match", status)
	}

	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("verifying a %d-byte image allocated %d bytes", info.Size(), allocated)
	if allocated > ceiling {
		t.Errorf("verifying a %d-byte image allocated %d bytes, ceiling %d\n"+
			"the digest is meant to be computed as the file is read: an allocation "+
			"that tracks the image means it is being read whole again, which is the "+
			"footprint a backup on a struggling machine cannot afford",
			info.Size(), allocated, ceiling)
	}
}

// TestImage_AHeldHandleNeverSeesTheBytesChange is the same invariant from the
// side a reader experiences, and the two platforms reach it by opposite routes.
//
// On unix the rename leaves the old inode alone, so a handle opened before a
// compaction keeps reading the image it opened while the directory entry now
// names a newer one.
//
// On windows the replacement is refused outright while any handle to the image
// is open — with FILE_SHARE_DELETE and without it alike, because renaming a file
// *aside* is not the same operation as renaming another file *over* it. The
// reader is protected just as completely, by the compaction failing rather than
// by the two files coexisting. That is worth pinning down rather than leaving as
// folklore: it is why an image that some other process is holding open — `graphene
// store csr -verify` against a live store — can stall a writer's compaction on
// windows.
//
// A *mapping* is a different thing from a handle and does not stall it. This
// comment used to predict that installing an image under a mapping would have to
// rename the old one aside first; measuring it showed otherwise, because a
// section object created from a FILE_SHARE_DELETE handle keeps the bytes
// reachable on its own and mapFile closes the handle before it returns. See
// TestImageMapped_CompactRenamesOverALiveMapping and §15.13's table.
func TestImage_AHeldHandleNeverSeesTheBytesChange(t *testing.T) {
	dir, s := v8Fixture(t)
	defer s.Close()

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, csrFileName)
	f, err := openSharedRead(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	held, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "second", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	compactErr := s.Compact()

	if runtime.GOOS == "windows" {
		if compactErr == nil {
			t.Fatalf("windows replaced an image with a handle open on it; if that is " +
				"now permitted, the comment on this test and the install sequence it " +
				"describes are both out of date")
		}
		// The refusal has to be complete in both directions. Backwards: a
		// compaction that got as far as touching the image before failing would
		// leave the reader holding half of each, which is the outcome the whole
		// invariant exists to exclude.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		again, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, held) {
			t.Fatalf("a refused compaction still changed the bytes behind the handle: "+
				"%d bytes before, %d after", len(held), len(again))
		}
		// Forwards: a writer that could not install an image has to be able to
		// install one once the reader lets go, or a single `store csr -verify`
		// would poison a store's compaction for good.
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("compaction stayed broken after the reader let go: %v", err)
		}
		return
	}

	if compactErr != nil {
		t.Fatalf("Compact: %v", compactErr)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	again, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, held) {
		t.Errorf("the bytes behind an open handle changed across a compaction: "+
			"%d bytes before, %d after", len(held), len(again))
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(current, held) {
		t.Fatal("the compaction published the same bytes; the test cannot tell an " +
			"old handle from a current one")
	}
}

// FuzzCSRDigestStream compares the two implementations over arbitrary bytes.
//
// The table above covers the shapes that were thought of. This covers the ones
// that were not — in particular any input where the header parses but the body
// does not, which is the region a hand-written table is least likely to reach.
func FuzzCSRDigestStream(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("GCSR"))
	f.Add(append([]byte("GCSR"), 8, 0))
	f.Add(make([]byte, csrV8HeaderSize))
	f.Add(make([]byte, csrV8HeaderSize+1))

	seed := make([]byte, csrV8HeaderSize+16)
	copy(seed, "GCSR")
	binary.LittleEndian.PutUint16(seed[4:], csrVersionSectioned)
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		wantStatus, wantDigest := csrDigestStatus(data)
		gotStatus, gotDigest, err := csrDigestStatusOf(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("streamed failed on %d bytes: %v", len(data), err)
		}
		if gotStatus != wantStatus || gotDigest != wantDigest {
			t.Fatalf("streamed (%v, %x) != whole-buffer (%v, %x) for %d bytes",
				gotStatus, gotDigest, wantStatus, wantDigest, len(data))
		}
	})
}
