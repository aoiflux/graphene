//go:build linux

package graphene_test

import (
	"bytes"
	"os"
	"strconv"
)

// Linux is the only one of the three platforms that publishes the anon/file
// split directly, so it needs no estimation: /proc/self/status carries RssAnon
// and RssFile as separate counters, and VmHWM as the peak. All three are in kB.
const rssSupported = true

func readRSS() rssSample {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return rssSample{}
	}

	var s rssSample
	var haveAnon, haveFile bool
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		key, value, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			continue
		}
		switch string(key) {
		case "RssAnon":
			s.Anon, haveAnon = parseStatusKB(value)
		case "RssFile":
			s.File, haveFile = parseStatusKB(value)
		case "RssShmem":
			// Shared memory is resident and is not swap-backed by a file the
			// process opened; fold it into the file class so Anon stays exactly
			// "what a RAM ceiling constrains".
			if v, ok := parseStatusKB(value); ok {
				s.File += v
			}
		case "VmRSS":
			s.Total, _ = parseStatusKB(value)
		case "VmHWM":
			s.Peak, _ = parseStatusKB(value)
		}
	}
	s.Split = haveAnon && haveFile
	if s.Total == 0 {
		s.Total = s.Anon + s.File
	}
	return s
}

// parseStatusKB reads the "   1234 kB" form the status file uses and returns
// bytes. A field present but unparseable reports not-ok rather than zero, so a
// kernel that changes the format surfaces as a missing split instead of as a
// confident wrong answer.
func parseStatusKB(field []byte) (uint64, bool) {
	field = bytes.TrimSpace(bytes.TrimSuffix(bytes.TrimSpace(field), []byte("kB")))
	n, err := strconv.ParseUint(string(field), 10, 64)
	if err != nil {
		return 0, false
	}
	return n * 1024, true
}
