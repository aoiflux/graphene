//go:build stress && !linux && !windows

package graphene_test

import "github.com/aoiflux/graphene"

// Darwin and everything else. There is no cache-eviction instrument here that a
// test can reach: purge(8) needs root and takes the whole machine's cache with
// it, and F_NOCACHE governs how a descriptor reads rather than what the cache
// already holds.
//
// So the harness skips, and it skips loudly with the reason attached. The
// alternative -- trimming something and calling the result cold -- is exactly
// what this measurement was declined for years to avoid, and a skip that says
// what is missing is more useful than a number nobody may rely on.
const coldEvictMethod = ""

const coldEvictNote = ""

const coldEvictHowTo = `no user-mode cache-eviction instrument on this platform.
Run the cold arm on linux (posix_fadvise POSIX_FADV_DONTNEED, genuinely cold) or
on windows (EmptyWorkingSet, a standby-warm lower bound).`

func coldEvictBeforeOpen(string) (string, error)         { return "", nil }
func coldEvictWhileOpen(*graphene.Graph) (string, error) { return "", nil }
