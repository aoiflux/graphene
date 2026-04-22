package disk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	nodePayload := []byte("node-payload-1")
	edgePayload := []byte("edge-payload-1")
	nodePropPayload := marshalNodeProp(1, "sha256", []byte("abc"))
	edgePropPayload := marshalEdgeProp(2, "bucket", []byte("high"))

	if err := wal.AppendNode(nodePayload); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendEdge(edgePayload); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendNodeProp(nodePropPayload); err != nil {
		t.Fatal(err)
	}
	if err := wal.AppendEdgeProp(edgePropPayload); err != nil {
		t.Fatal(err)
	}
	wal.Close()

	// Reopen and replay.
	wal2, _ := OpenWAL(filepath.Join(dir, "test.wal"))
	defer wal2.Close()

	var nodes, edges, nodeProps, edgeProps [][]byte
	err = wal2.Replay(ReplayCallbacks{
		NodeFunc:     func(p []byte) error { nodes = append(nodes, p); return nil },
		EdgeFunc:     func(p []byte) error { edges = append(edges, p); return nil },
		NodePropFunc: func(p []byte) error { nodeProps = append(nodeProps, p); return nil },
		EdgePropFunc: func(p []byte) error { edgeProps = append(edgeProps, p); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(nodes) != 1 || string(nodes[0]) != string(nodePayload) {
		t.Errorf("nodes: got %v", nodes)
	}
	if len(edges) != 1 || string(edges[0]) != string(edgePayload) {
		t.Errorf("edges: got %v", edges)
	}
	if len(nodeProps) != 1 {
		t.Errorf("nodeProps: got %d, want 1", len(nodeProps))
	}
	if len(edgeProps) != 1 {
		t.Errorf("edgeProps: got %d, want 1", len(edgeProps))
	}

	// Verify unmarshal round-trip for node prop.
	id, key, val, err := unmarshalNodeProp(nodeProps[0])
	if err != nil || id != 1 || key != "sha256" || string(val) != "abc" {
		t.Errorf("unmarshalNodeProp: id=%d key=%q val=%q err=%v", id, key, val, err)
	}
	// Verify unmarshal round-trip for edge prop.
	eid, ekey, eval, err := unmarshalEdgeProp(edgeProps[0])
	if err != nil || eid != 2 || ekey != "bucket" || string(eval) != "high" {
		t.Errorf("unmarshalEdgeProp: id=%d key=%q val=%q err=%v", eid, ekey, eval, err)
	}
}

func TestWAL_Checkpoint_StopsReplay(t *testing.T) {
	dir := t.TempDir()
	wal, _ := OpenWAL(filepath.Join(dir, "test.wal"))

	wal.AppendNode([]byte("before-checkpoint"))
	wal.Checkpoint()
	wal.AppendNode([]byte("after-checkpoint"))
	wal.Close()

	wal2, _ := OpenWAL(filepath.Join(dir, "test.wal"))
	defer wal2.Close()

	var nodes [][]byte
	wal2.Replay(ReplayCallbacks{
		NodeFunc: func(p []byte) error { nodes = append(nodes, p); return nil },
	})
	// Replay stops at checkpoint — only "before-checkpoint" should appear.
	if len(nodes) != 1 {
		t.Errorf("expected 1 node before checkpoint, got %d", len(nodes))
	}
	if string(nodes[0]) != "before-checkpoint" {
		t.Errorf("unexpected node payload: %q", nodes[0])
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	wal, _ := OpenWAL(path)
	wal.AppendNode([]byte("data"))
	wal.Checkpoint()
	wal.Truncate()
	wal.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL file should be empty after Truncate, size=%d", info.Size())
	}
}

func TestWAL_CorruptTailIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	wal, _ := OpenWAL(path)
	wal.AppendNode([]byte("good"))
	wal.Close()

	// Append garbage bytes to simulate a crash mid-write.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.Write([]byte{0x01, 0x05, 0x00, 0x00, 0x00, 'x', 'x', 'x'}) // incomplete record
	f.Close()

	wal2, _ := OpenWAL(path)
	defer wal2.Close()

	var nodes [][]byte
	if err := wal2.Replay(ReplayCallbacks{
		NodeFunc: func(p []byte) error { nodes = append(nodes, p); return nil },
	}); err != nil {
		t.Errorf("replay should not error on corrupt tail, got %v", err)
	}
	if len(nodes) != 1 || string(nodes[0]) != "good" {
		t.Errorf("expected 1 good record, got %v", nodes)
	}
}

func TestWAL_EmptyReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")
	wal, _ := OpenWAL(path)
	wal.Close()

	wal2, _ := OpenWAL(path)
	defer wal2.Close()

	called := false
	err := wal2.Replay(ReplayCallbacks{
		NodeFunc: func(p []byte) error { called = true; return nil },
	})
	if err != nil {
		t.Errorf("empty replay error: %v", err)
	}
	if called {
		t.Error("callback should not be called on empty WAL")
	}
}

func TestWAL_MultipleCheckpoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	wal, _ := OpenWAL(path)
	wal.AppendNode([]byte("a"))
	wal.Checkpoint() // replay stops here
	wal.AppendNode([]byte("b"))
	wal.Checkpoint()
	wal.AppendNode([]byte("c"))
	wal.Close()

	wal2, _ := OpenWAL(path)
	defer wal2.Close()

	var nodes [][]byte
	wal2.Replay(ReplayCallbacks{
		NodeFunc: func(p []byte) error { nodes = append(nodes, p); return nil },
	})
	if len(nodes) != 1 || string(nodes[0]) != "a" {
		t.Errorf("expected only 'a', got %v", nodes)
	}
}
