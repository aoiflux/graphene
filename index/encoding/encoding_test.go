package encoding

import (
	"bytes"
	"math"
	"sort"
	"testing"
	"time"
)

// The whole point of these encoders is one property: byte order must agree with
// value order. Every test here checks that, not round-tripping for its own sake.

func TestInt64_ByteOrderMatchesValueOrder(t *testing.T) {
	values := []int64{
		math.MinInt64, math.MinInt64 + 1, -1 << 40, -70000, -1000, -256, -255, -1,
		0, 1, 255, 256, 1000, 70000, 1 << 40, math.MaxInt64 - 1, math.MaxInt64,
	}
	// Shuffle deterministically by sorting the encodings and checking the values
	// come back in ascending order.
	encoded := make([][]byte, len(values))
	for i, v := range values {
		encoded[i] = Int64(v)
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for i := range encoded {
		got, ok := DecodeInt64(encoded[i])
		if !ok {
			t.Fatalf("DecodeInt64 failed at %d", i)
		}
		if got != sorted[i] {
			t.Fatalf("position %d: byte order gives %d, value order gives %d", i, got, sorted[i])
		}
	}
}

func TestUint64_ByteOrderMatchesValueOrder(t *testing.T) {
	values := []uint64{0, 1, 255, 256, 1 << 32, math.MaxUint64 - 1, math.MaxUint64}
	for i := 0; i < len(values)-1; i++ {
		a, b := Uint64(values[i]), Uint64(values[i+1])
		if bytes.Compare(a, b) >= 0 {
			t.Fatalf("%d should encode below %d", values[i], values[i+1])
		}
		got, ok := DecodeUint64(a)
		if !ok || got != values[i] {
			t.Fatalf("round trip failed for %d: got %d ok=%v", values[i], got, ok)
		}
	}
}

func TestFloat64_ByteOrderMatchesValueOrder(t *testing.T) {
	values := []float64{
		math.Inf(-1), -math.MaxFloat64, -1e100, -1.5, -1, -math.SmallestNonzeroFloat64,
		0, math.SmallestNonzeroFloat64, 1, 1.5, 3.14159, 1e100, math.MaxFloat64, math.Inf(1),
	}
	for i := 0; i < len(values)-1; i++ {
		a, b := Float64(values[i]), Float64(values[i+1])
		if bytes.Compare(a, b) >= 0 {
			t.Fatalf("%v should encode below %v", values[i], values[i+1])
		}
	}
	for _, v := range values {
		got, ok := DecodeFloat64(Float64(v))
		if !ok || got != v {
			t.Fatalf("round trip failed for %v: got %v ok=%v", v, got, ok)
		}
	}

	// Negative zero and positive zero are equal as values; they need not encode
	// identically, but they must not straddle another value.
	negZero, posZero := Float64(math.Copysign(0, -1)), Float64(0)
	if bytes.Compare(negZero, Float64(-1)) <= 0 {
		t.Fatal("-0 must encode above -1")
	}
	if bytes.Compare(posZero, Float64(1)) >= 0 {
		t.Fatal("+0 must encode below 1")
	}
}

func TestTime_ByteOrderMatchesChronology(t *testing.T) {
	base := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	times := []time.Time{
		base.Add(-100 * time.Hour), base.Add(-time.Second), base,
		base.Add(time.Nanosecond), base.Add(time.Hour), base.AddDate(1, 0, 0),
	}
	for i := 0; i < len(times)-1; i++ {
		if bytes.Compare(Time(times[i]), Time(times[i+1])) >= 0 {
			t.Fatalf("%v should encode before %v", times[i], times[i+1])
		}
	}
	got, ok := DecodeTime(Time(base))
	if !ok || !got.Equal(base) {
		t.Fatalf("round trip failed: got %v ok=%v", got, ok)
	}
}

func TestString_PreservesLexicographicOrder(t *testing.T) {
	values := []string{"", "a", "aa", "ab", "b", "z", "\xff"}
	for i := 0; i < len(values)-1; i++ {
		if bytes.Compare(String(values[i]), String(values[i+1])) >= 0 {
			t.Fatalf("%q should encode below %q", values[i], values[i+1])
		}
	}
}

// The documented failure of the scan-path comparison rule. This is why declared
// keys are compared byte-wise and why these encoders exist.
func TestDecimalStrings_AreNotAValidSortOrder(t *testing.T) {
	// Under "numeric when both parse, byte-wise otherwise": 9 < 10, 10 < 1x,
	// and 1x < 9 — a cycle, so no sorted structure can be built on it.
	nine, ten, oneX := "9", "10", "1x"

	if !(9.0 < 10.0) {
		t.Fatal("numeric comparison of 9 and 10")
	}
	if bytes.Compare([]byte(ten), []byte(oneX)) >= 0 {
		t.Fatalf("%q should sort below %q byte-wise", ten, oneX)
	}
	if bytes.Compare([]byte(oneX), []byte(nine)) >= 0 {
		t.Fatalf("%q should sort below %q byte-wise, closing the cycle", oneX, nine)
	}

	// Encoding removes the ambiguity: 9 encodes below 10, unconditionally.
	if bytes.Compare(Int64(9), Int64(10)) >= 0 {
		t.Fatal("encoded 9 must sort below encoded 10")
	}
}

func TestPrefixUpperBound(t *testing.T) {
	cases := []struct {
		prefix    string
		wantUpper string
		wantOK    bool
	}{
		{"abc", "abd", true},
		{"ab\xff", "ac", true},
		{"a", "b", true},
		{"\xff", "", false},
		{"\xff\xff", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		upper, ok := PrefixUpperBound([]byte(c.prefix))
		if ok != c.wantOK {
			t.Fatalf("PrefixUpperBound(%q): ok=%v, want %v", c.prefix, ok, c.wantOK)
		}
		if ok && string(upper) != c.wantUpper {
			t.Fatalf("PrefixUpperBound(%q) = %q, want %q", c.prefix, upper, c.wantUpper)
		}
	}

	// Every extension of a prefix must fall inside [prefix, upper).
	prefix := []byte("bucket-01")
	upper, ok := PrefixUpperBound(prefix)
	if !ok {
		t.Fatal("expected a bound")
	}
	for _, suffix := range []string{"", "0", "9", "zzzz", "\xff\xff"} {
		v := append(append([]byte{}, prefix...), suffix...)
		if bytes.Compare(v, prefix) < 0 || bytes.Compare(v, upper) >= 0 {
			t.Fatalf("%q is outside [%q, %q)", v, prefix, upper)
		}
	}
	// And a value just past the prefix must fall outside.
	if bytes.Compare([]byte("bucket-02"), upper) < 0 {
		t.Fatal("bucket-02 should be at or above the upper bound")
	}
}
