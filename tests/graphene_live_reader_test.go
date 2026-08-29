package graphene_test

// Live readers, across two processes and through the public API.
//
// disk/live_test.go covers the mechanism. This covers the claim: a reader in
// another process follows a writer, does not block it, and is refused nothing
// the writer needs — including a compaction, which on Windows has to replace a
// file the reader is holding open.

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// liveChild is a child process holding dir open as a live reader, which can be
// asked what it can currently see.
type liveChild struct {
	t    *testing.T
	cmd  *exec.Cmd
	in   interface{ Write([]byte) (int, error) }
	out  *bufio.Scanner
	stop func()
}

// ask tells the child to refresh and returns the node count it then reports.
func (c *liveChild) ask() (int, bool) {
	c.t.Helper()
	if _, err := c.in.Write([]byte("refresh\n")); err != nil {
		c.t.Fatalf("writing to the child: %v", err)
	}
	type reply struct {
		n        int
		reloaded bool
	}
	ch := make(chan reply, 1)
	go func() {
		for c.out.Scan() {
			line := c.out.Text()
			if strings.Contains(line, childFailed) {
				c.t.Errorf("child: %s", line)
				close(ch)
				return
			}
			if !strings.Contains(line, childCount) {
				continue
			}
			fields := strings.Fields(line)
			n, err := strconv.Atoi(fields[1])
			if err != nil {
				c.t.Errorf("child reported an unparseable count: %s", line)
				close(ch)
				return
			}
			ch <- reply{n: n, reloaded: strings.HasSuffix(fields[2], "true")}
			return
		}
		close(ch)
	}()

	select {
	case r, ok := <-ch:
		if !ok {
			c.t.Fatal("the child stopped answering")
		}
		return r.n, r.reloaded
	case <-time.After(60 * time.Second):
		c.t.Fatal("the child did not answer within 60s")
		return 0, false
	}
}

func startLiveChild(t *testing.T, dir string) *liveChild {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestLockChildProcess$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+dir, childModeEnv+"=live")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	scanner := bufio.NewScanner(stdout)
	c := &liveChild{t: t, cmd: cmd, in: stdin, out: scanner}
	c.stop = func() {
		stdin.Close()
		_ = cmd.Wait()
	}

	ready := make(chan string, 1)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, childReady) || strings.Contains(line, childFailed) {
				ready <- line
				return
			}
		}
		ready <- ""
	}()
	select {
	case line := <-ready:
		if line == "" || strings.Contains(line, childFailed) {
			c.stop()
			t.Fatalf("the live child could not open the store: %q", line)
		}
	case <-time.After(60 * time.Second):
		c.stop()
		t.Fatal("the live child did not start within 60s")
	}
	return c
}

func addAndSync(t *testing.T, g *graphene.Graph, n int, label store.NodeType) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{label}}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if err := g.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// **The whole point.** A reader in another process follows a writer in this one,
// across appends and across a compaction.
func TestMultiProcess_LiveReaderFollowsAWriter(t *testing.T) {
	dir := t.TempDir()
	seedForChild(t, dir)

	w, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("the writer was refused: %v", err)
	}
	defer w.Close()

	seeded, err := w.NodeCount()
	if err != nil {
		t.Fatal(err)
	}

	child := startLiveChild(t, dir)
	defer child.stop()

	if n, _ := child.ask(); uint64(n) != seeded {
		t.Fatalf("the child sees %d nodes at open, want %d", n, seeded)
	}

	addAndSync(t, w, 5, 1)
	if n, reloaded := child.ask(); uint64(n) != seeded+5 {
		t.Errorf("after 5 appends the child sees %d, want %d", n, seeded+5)
	} else if reloaded {
		t.Error("an append should not have forced a reload")
	}

	// The compaction is the interesting half on Windows: it replaces the log the
	// child is holding open, which a plain rename cannot do.
	addAndSync(t, w, 4, 2)
	if err := w.Compact(); err != nil {
		t.Fatalf("compacting with a live reader attached: %v", err)
	}
	if n, reloaded := child.ask(); uint64(n) != seeded+9 {
		t.Errorf("after the compaction the child sees %d, want %d", n, seeded+9)
	} else if !reloaded {
		t.Error("a replaced log should have forced a reload")
	}

	addAndSync(t, w, 3, 3)
	if n, _ := child.ask(); uint64(n) != seeded+12 {
		t.Errorf("after the compaction and 3 more appends the child sees %d, want %d", n, seeded+12)
	}
}

// A live reader holds nothing against a writer. This is the case OpenReadOnly
// deliberately gets the other way round.
func TestMultiProcess_LiveReaderDoesNotExcludeAWriter(t *testing.T) {
	dir := t.TempDir()
	seedForChild(t, dir)

	child := startLiveChild(t, dir)
	defer child.stop()

	w, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("a writer was refused beside a live reader: %v", err)
	}
	defer w.Close()

	// And an ordinary reader is still refused beside that writer, so the new mode
	// has not quietly relaxed the old one.
	if r, err := graphene.OpenReadOnly(dir); err == nil {
		r.Close()
		t.Error("OpenReadOnly succeeded beside a writer")
	} else if !errors.Is(err, disk.ErrStoreLocked) {
		t.Errorf("OpenReadOnly beside a writer returned %v, want ErrStoreLocked", err)
	}
}

// Backends that cannot follow a writer say so rather than reporting a refresh
// they did not perform.
func TestRefreshOnABackendThatCannotFollow(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		g := graphene.NewInMemory()
		defer g.Close()
		if g.IsLive() {
			t.Error("IsLive true on the memory backend")
		}
		if _, err := g.Refresh(); !errors.Is(err, disk.ErrNotLiveReader) {
			t.Errorf("Refresh on the memory backend returned %v, want ErrNotLiveReader", err)
		}
	})

	t.Run("disk writer", func(t *testing.T) {
		g, err := graphene.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		if g.IsLive() {
			t.Error("IsLive true on a writer")
		}
		if _, err := g.Refresh(); !errors.Is(err, disk.ErrNotLiveReader) {
			t.Errorf("Refresh on a writer returned %v, want ErrNotLiveReader", err)
		}
	})
}

// The public entry point, in one process: open live, follow, close.
func TestOpenLiveFollowsAWriterInProcess(t *testing.T) {
	dir := t.TempDir()
	w, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	addAndSync(t, w, 2, 1)

	r, err := graphene.OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.IsLive() {
		t.Error("IsLive false on a graph opened with OpenLive")
	}

	addAndSync(t, w, 3, 2)
	info, err := r.Refresh()
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !info.Advanced() {
		t.Errorf("Refresh reported no advance: %+v", info)
	}
	if n, err := r.NodeCount(); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Errorf("node count = %d, want 5", n)
	}

	if _, err := r.AddNode(&store.Node{Labels: []store.NodeType{9}}); !errors.Is(err, disk.ErrReadOnly) {
		t.Errorf("writing through a live reader returned %v, want ErrReadOnly", err)
	}
}
