package graphene_test

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// The cross-process claim, made across actual processes.
//
// disk/lock_test.go covers the same exclusion rules in one process, and does so
// soundly — both primitives underneath scope a lock to the open file description
// rather than to the process, so two Opens in one process genuinely conflict.
// But that soundness is a property of the implementation, and it is exactly the
// property that would be lost if someone swapped flock for fcntl. A test that
// runs in one process cannot notice. This one can, and it is the only reason it
// exists.
//
// The child is this same test binary, re-invoked with a marker in its
// environment. It opens the store, prints a line, and waits to be told to exit.

const (
	childEnv     = "GRAPHENE_LOCK_CHILD_DIR"
	childModeEnv = "GRAPHENE_LOCK_CHILD_MODE"
	childReady   = "CHILD-HOLDING"
	childFailed  = "CHILD-FAILED"
	childCount   = "CHILD-COUNT"
)

// TestLockChildProcess is the child. Under `go test` it is skipped; re-invoked
// with the marker set, it becomes the process that holds the store.
//
// It never runs as a test in its own right, which is why it asserts nothing.
func TestLockChildProcess(t *testing.T) {
	dir := os.Getenv(childEnv)
	if dir == "" {
		t.Skip("not the child process")
	}

	var (
		g    *graphene.Graph
		err  error
		mode = os.Getenv(childModeEnv)
	)
	switch mode {
	case "read":
		g, err = graphene.OpenReadOnly(dir)
	case "live":
		g, err = graphene.OpenLive(dir)
	default:
		g, err = graphene.Open(dir)
	}
	if err != nil {
		fmt.Printf("%s %v\n", childFailed, err)
		return
	}
	defer g.Close()

	// Announce, then block until the parent closes stdin. Flushed explicitly:
	// the parent is waiting on this line and a buffered write would deadlock it.
	fmt.Printf("%s %d\n", childReady, os.Getpid())
	os.Stdout.Sync()

	in := bufio.NewReader(os.Stdin)
	if mode != "live" {
		_, _ = in.ReadString('\n')
		return
	}

	// A live child answers questions instead of just waiting: each line the
	// parent sends is "refresh and tell me what you can see now". That is what
	// makes the cross-process test a test of *following* a writer rather than of
	// merely coexisting with one.
	for {
		line, rerr := in.ReadString('\n')
		if strings.TrimSpace(line) == "" && rerr != nil {
			return
		}
		info, ferr := g.Refresh()
		if ferr != nil {
			fmt.Printf("%s refresh: %v\n", childFailed, ferr)
			os.Stdout.Sync()
			return
		}
		n, cerr := g.NodeCount()
		if cerr != nil {
			fmt.Printf("%s count: %v\n", childFailed, cerr)
			os.Stdout.Sync()
			return
		}
		fmt.Printf("%s %d reloaded=%v\n", childCount, n, info.Reloaded)
		os.Stdout.Sync()
		if rerr != nil {
			return
		}
	}
}

// holdStore starts a child holding dir, and returns a function that stops it.
// It returns only once the child has actually taken the lock, so the parent's
// assertion is never racing the child's Open.
func holdStore(t *testing.T, dir, mode string) func() {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestLockChildProcess$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+dir, childModeEnv+"="+mode)

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

	stop := func() {
		stdin.Close()
		_ = cmd.Wait()
	}

	// Wait for the child to say it has the lock. A bounded wait, because a child
	// that never reports is a failure to diagnose rather than a test to hang.
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, childReady) || strings.Contains(line, childFailed) {
				ch <- result{line: line}
				return
			}
		}
		ch <- result{err: errors.New("child exited without reporting")}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			stop()
			t.Fatalf("child: %v", r.err)
		}
		if strings.Contains(r.line, childFailed) {
			stop()
			t.Fatalf("child could not open the store: %s", r.line)
		}
	case <-time.After(60 * time.Second):
		stop()
		t.Fatal("child did not take the lock within 60s")
	}

	return stop
}

// **The whole point.** A separate process holding the store excludes this one.
func TestMultiProcess_WriterExcludesWriter(t *testing.T) {
	dir := t.TempDir()
	seedForChild(t, dir)

	stop := holdStore(t, dir, "write")
	defer stop()

	g, err := graphene.Open(dir)
	if err == nil {
		g.Close()
		t.Fatal("opened a store another process holds exclusively")
	}
	if !errors.Is(err, disk.ErrStoreLocked) {
		t.Fatalf("got %v, want ErrStoreLocked", err)
	}

	// And releases it again when the process goes away — the OS drops the lock on
	// exit, so recovering from a crashed writer needs no cleanup step.
	stop()
	after, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("the lock outlived the process that held it: %v", err)
	}
	after.Close()
}

// A writer in another process excludes a reader here.
func TestMultiProcess_WriterExcludesReader(t *testing.T) {
	dir := t.TempDir()
	seedForChild(t, dir)

	stop := holdStore(t, dir, "write")
	defer stop()

	if g, err := graphene.OpenReadOnly(dir); err == nil {
		g.Close()
		t.Fatal("read a store another process is writing")
	} else if !errors.Is(err, disk.ErrStoreLocked) {
		t.Fatalf("got %v, want ErrStoreLocked", err)
	}
}

// Readers in separate processes coexist, and a writer is still excluded while
// they do. This is the case a plain exclusive lock would have got wrong.
func TestMultiProcess_ReadersCoexistAndExcludeAWriter(t *testing.T) {
	dir := t.TempDir()
	seedForChild(t, dir)

	stop := holdStore(t, dir, "read")
	defer stop()

	r, err := graphene.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("a second reader was refused: %v", err)
	}
	if _, err := r.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	defer r.Close()

	if w, err := graphene.Open(dir); err == nil {
		w.Close()
		t.Fatal("a writer opened a store two readers hold")
	} else if !errors.Is(err, disk.ErrStoreLocked) {
		t.Fatalf("got %v, want ErrStoreLocked", err)
	}
}

// seedForChild leaves a compacted store behind, closed and unlocked, so the
// child has something real to open.
func seedForChild(t *testing.T, dir string) {
	t.Helper()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
			t.Fatalf("seed AddNode: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("seed Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
}
