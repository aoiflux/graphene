// Package encoding provides order-preserving encodings for property values.
//
// # Why this exists
//
// Graphene stores property values as opaque bytes: the engine never learns what
// a value means, only that two of them are equal. That is enough for a hash
// index, which is why equality lookups work on any encoding at all.
//
// An ordered index needs more. To answer "score > 40" with a binary search
// instead of a scan, the index must keep values in an order that agrees with the
// caller's idea of comparison. The obvious shortcut — compare bytes, and fall
// back to parsing numbers when both sides happen to look numeric — is what the
// scan path does today, and it is **not a valid sort order**:
//
//	"9"  vs "10"  → both parse as numbers → 9 < 10
//	"10" vs "1x"  → not both numeric      → bytes: "10" < "1x"
//	"9"  vs "1x"  → not both numeric      → bytes: "9"  > "1x"
//
// So 9 < 10 < 1x < 9. A predicate can be evaluated element by element under that
// rule, but a sorted structure cannot be built from it.
//
// These encoders remove the ambiguity by making one thing true:
//
//	bytes.Compare(Encode(a), Encode(b)) has the same sign as comparing a and b
//
// Encode a value once, on the way into the index and into query bounds, and
// ordering is simply byte ordering.
//
// # Choosing an encoding
//
//   - Int64/Uint64/Float64 for numbers. Do not zero-pad decimal strings by hand
//     unless every value is the same width and non-negative — that is the same
//     trap in a different costume.
//   - Time for timestamps (encoded as Unix nanoseconds).
//   - String for text. Raw bytes already sort lexicographically, so this is a
//     copy; it exists so call sites read consistently.
//
// Values encoded with different encoders must not share a key: their byte
// ranges are not comparable.
package encoding

import (
	"encoding/binary"
	"math"
	"time"
)

// Uint64 encodes v as 8 big-endian bytes.
//
// Big-endian is what makes this order-preserving: the most significant byte is
// compared first, exactly as bytes.Compare does.
func Uint64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

// DecodeUint64 reverses Uint64. ok is false if b is not 8 bytes.
func DecodeUint64(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// Int64 encodes v as 8 big-endian bytes with the sign bit flipped.
//
// Two's-complement negatives have their high bit set, which would sort them
// *above* positives under unsigned byte comparison. Flipping the sign bit maps
// the signed range onto the unsigned range in order: math.MinInt64 becomes
// 0x0000…, -1 becomes 0x7FFF…, 0 becomes 0x8000…, math.MaxInt64 becomes 0xFFFF…
func Int64(v int64) []byte {
	return Uint64(uint64(v) ^ (1 << 63))
}

// DecodeInt64 reverses Int64. ok is false if b is not 8 bytes.
func DecodeInt64(b []byte) (int64, bool) {
	u, ok := DecodeUint64(b)
	if !ok {
		return 0, false
	}
	return int64(u ^ (1 << 63)), true
}

// Float64 encodes v as 8 big-endian bytes in IEEE 754 order.
//
// Raw IEEE 754 bits nearly sort correctly already — the exponent sits above the
// mantissa — but negatives are stored sign-and-magnitude, so they run backwards
// and above the positives. The fix is standard: for a negative number flip every
// bit (reversing the magnitude order and clearing the sign), and for a
// non-negative number flip only the sign bit (lifting it above all negatives).
//
// NaN has no meaningful position in a total order; it encodes above every real
// value and must not be used as a range bound.
func Float64(v float64) []byte {
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits // negative: reverse magnitude order, clear sign
	} else {
		bits |= 1 << 63 // non-negative: sort above every negative
	}
	return Uint64(bits)
}

// DecodeFloat64 reverses Float64. ok is false if b is not 8 bytes.
func DecodeFloat64(b []byte) (float64, bool) {
	u, ok := DecodeUint64(b)
	if !ok {
		return 0, false
	}
	if u&(1<<63) != 0 {
		u &^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u), true
}

// Time encodes t as its Unix nanosecond count.
//
// Nanosecond resolution covers years 1678–2262; timestamps outside that range
// overflow int64 and must not be indexed this way.
func Time(t time.Time) []byte {
	return Int64(t.UnixNano())
}

// DecodeTime reverses Time. ok is false if b is not 8 bytes.
func DecodeTime(b []byte) (time.Time, bool) {
	ns, ok := DecodeInt64(b)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// String encodes s as its raw bytes, which already sort lexicographically.
//
// This is a copy rather than an unsafe cast: the result is handed to the index,
// which retains it.
func String(s string) []byte {
	return []byte(s)
}

// PrefixUpperBound returns the exclusive upper bound of the range of values
// starting with prefix, and reports whether one exists.
//
// Incrementing the last byte below 0xFF gives the first value that sorts after
// every extension of the prefix. A prefix of all 0xFF bytes (or an empty one)
// has no upper bound — everything from it onwards matches — and ok is false.
func PrefixUpperBound(prefix []byte) (upper []byte, ok bool) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		upper = make([]byte, i+1)
		copy(upper, prefix[:i+1])
		upper[i]++
		return upper, true
	}
	return nil, false
}
