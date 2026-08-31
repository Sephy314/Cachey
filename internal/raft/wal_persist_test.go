package raft

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// newWALNode opens the WAL at dir with recovery wired into node's log rebuild,
// and returns the node + WAL. The store is the FSM applied on commit.
func newWALNode(dir string, st *store.CacheyStore) (*Node, *wal.WAL) {
	cfg := wal.DefaultConfig(dir)
	cfg.DisableRotation = true

	n, err := NewNode(Config{ID: "a"}, &memTransport{cluster: &cluster{nodes: map[string]*Node{}}}, fsmApply(st))
	if err != nil {
		panic(err)
	}
	w, err := wal.Open(cfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   n.ApplyRecoveredRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		panic(err)
	}
	n.SetLogStore(NewWALLogStore(w))
	return n, w
}

// fsmApply decodes a committed entry's command and applies it to the store.
func fsmApply(st *store.CacheyStore) func(Entry) {
	return func(e Entry) {
		var rec wal.Record
		if err := json.Unmarshal(e.Command, &rec); err != nil {
			panic("raft: bad command in committed entry: " + err.Error())
		}
		if err := st.ApplyRecord(rec); err != nil {
			panic("raft: apply: " + err.Error())
		}
	}
}

// proposePut builds a PUT command and waits for it to be applied.
func proposePut(t *testing.T, n *Node, key, val string) {
	t.Helper()
	rec := wal.Record{Op: wal.OpPut, Key: key, Val: val}
	cmd, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := n.Propose(cmd)
	if err != nil {
		t.Fatalf("Propose(%s=%s): %v", key, val, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, idx); err != nil {
		t.Fatalf("WaitApplied(%s=%s): %v", key, val, err)
	}
}

func storeGet(t *testing.T, st *store.CacheyStore, key string) string {
	t.Helper()
	v, err := st.Get(key)
	if err != nil {
		return ""
	}
	return *v
}

// TestWALPersistenceRestart commits entries, restarts the node from the WAL,
// and verifies the log is rebuilt and the committed state restored.
func TestWALPersistenceRestart(t *testing.T) {
	dir := t.TempDir()

	st := store.NewCacheyStore()
	n, w := newWALNode(dir, st)
	n.Run()
	waitFor(t, "node to lead", 3*time.Second, func() bool { return n.IsLeader() })
	proposePut(t, n, "k1", "v1")
	proposePut(t, n, "k2", "v2")
	if storeGet(t, st, "k1") != "v1" || storeGet(t, st, "k2") != "v2" {
		t.Fatalf("unexpected state before restart")
	}
	if n.LogLastIndex() < 3 { // no-op + k1 + k2
		t.Fatalf("expected log index >= 3, got %d", n.LogLastIndex())
	}
	n.Stop()
	w.Close()

	// --- second life: recover from the WAL ---
	st2 := store.NewCacheyStore()
	n2, w2 := newWALNode(dir, st2)
	n2.Run()
	defer n2.Stop()
	defer w2.Close()

	// The log is rebuilt from the WAL (no-op@1 + k1@2 + k2@3).
	if got := n2.LogLastIndex(); got != 3 {
		t.Fatalf("recovered log last index = %d, want 3", got)
	}
	// The node re-leads (single node) and applies the recovered committed
	// prefix to the fresh store.
	waitFor(t, "restored committed state", 5*time.Second, func() bool {
		return storeGet(t, st2, "k1") == "v1" && storeGet(t, st2, "k2") == "v2"
	})
}

// TestLogSetRebuild verifies Log.set rebuild semantics: later records at an
// index (newer term) supersede an older conflicting tail.
func TestLogSetRebuild(t *testing.T) {
	l := NewLog()
	l.set(1, Entry{Term: 1, Command: []byte("a")})
	l.set(2, Entry{Term: 1, Command: []byte("b")})
	l.set(3, Entry{Term: 1, Command: []byte("c")})
	// simulate a truncation to index 1 followed by entries 2', 3' from a new term
	l.set(2, Entry{Term: 2, Command: []byte("b2")})
	l.set(3, Entry{Term: 2, Command: []byte("c2")})
	if got := l.lastIndex(); got != 3 {
		t.Fatalf("lastIndex = %d, want 3", got)
	}
	if l.termAt(2) != 2 || l.termAt(3) != 2 {
		t.Fatalf("terms after rebuild = %d,%d, want 2,2", l.termAt(2), l.termAt(3))
	}
	// a stale tail beyond the last set index is dropped
	l.set(1, Entry{Term: 3, Command: []byte("a3")})
	if got := l.lastIndex(); got != 1 {
		t.Fatalf("after truncating to 1, lastIndex = %d, want 1", got)
	}
}
