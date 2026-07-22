package disk

import "testing"

// The WAL record checksum is part of the on-disk format.
//
// computeCRC32 was originally a bit-by-bit loop and is now crc32.ChecksumIEEE —
// the same function computed faster. These vectors are what make that swap
// verifiable rather than asserted: every WAL already written used the old loop,
// and a checksum differing by even one value would fail every record on replay
// of an existing log.
//
// Comparing against crc32.ChecksumIEEE directly would now be tautological. Fixed
// vectors are not: they pin the actual bytes, so a future change to either the
// polynomial or the implementation is caught.
func TestComputeCRC32Vectors(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
	}{
		{"", 0x00000000},
		{"a", 0xE8B7BE43},
		{"abc", 0x352441C2},
		{"123456789", 0xCBF43926}, // the standard CRC-32/ISO-HDLC check value
		{"The quick brown fox jumps over the lazy dog", 0x414FA339},
	}
	for _, tc := range cases {
		if got := computeCRC32([]byte(tc.in)); got != tc.want {
			t.Errorf("computeCRC32(%q) = %08X, want %08X", tc.in, got, tc.want)
		}
	}
}

// A checksum that ignored its input would satisfy the vectors above by accident
// only if the vectors were wrong; this pins the property they exist to protect.
func TestComputeCRC32DetectsMutation(t *testing.T) {
	payloads := [][]byte{
		[]byte("x"),
		[]byte("a slightly longer record payload"),
		make([]byte, 4096),
	}
	for i, p := range payloads {
		crc := computeCRC32(p)
		mutated := append([]byte(nil), p...)
		mutated[0] ^= 0xFF
		if computeCRC32(mutated) == crc {
			t.Errorf("payload %d: checksum unchanged after flipping a byte", i)
		}
	}
}
