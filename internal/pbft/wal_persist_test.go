package pbft

import (
	"testing"

	"github.com/Sephy314/Cachey/internal/wal"
)

// newWALReplica opens the WAL at dir with recovery wired into the replica's log
// rebuild, and returns the replica + WAL. No peers: a single-replica cluster
// (f=0) submits execute immediately, so this exercises the durable path without
// protocol noise.
func newWALReplica(dir string) (*Replica, *wal.WAL) {
	cfg := wal.DefaultConfig(dir)
	cfg.DisableRotation = true

	r, err := NewReplica(Config{ID: "a"}, &memTransport{}, nil)
	if err != nil {
		panic(err)
	}
	w, err := wal.Open(cfg, wal.Hooks{ApplyRecord: r.ApplyRecoveredRecord})
	if err != nil {
		panic(err)
	}
	r.SetLogStore(NewWALLogStore(w))
	return r, w
}

// TestWALPersistenceRestart submits commands to a WAL-backed single replica,
// restarts it from the same directory, and verifies the executed watermark and
// the ordered requests are rebuilt from the WAL (M4).
func TestWALPersistenceRestart(t *testing.T) {
	dir := t.TempDir()

	r, w := newWALReplica(dir)
	cmds := []string{"a", "b", "c"}
	for i, c := range cmds {
		seq, err := r.Submit([]byte(c))
		if err != nil {
			t.Fatalf("Submit(%s): %v", c, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}
	if got := r.LastExecuted(); got != 3 {
		t.Fatalf("executed %d requests before restart, want 3", got)
	}
	w.Close()

	// --- second life: recover from the WAL ---
	r2, w2 := newWALReplica(dir)
	defer w2.Close()

	// The executed watermark is restored...
	if got := r2.LastExecuted(); got != 3 {
		t.Fatalf("recovered watermark = %d, want 3", got)
	}
	// ...and the accepted requests are rebuilt at their sequence numbers.
	r2.mu.Lock()
	for i, c := range cmds {
		e := r2.log[uint64(i+1)]
		if e == nil {
			t.Fatalf("recovered log missing seq %d", i+1)
		}
		if !e.prePrepared {
			t.Fatalf("recovered seq %d is not pre-prepared", i+1)
		}
		if string(e.req.Command) != c {
			t.Fatalf("recovered seq %d = %q, want %q", i+1, e.req.Command, c)
		}
	}
	r2.mu.Unlock()

	// A restarted replica can keep ordering: the next Submit lands at seq 4.
	seq, err := r2.Submit([]byte("d"))
	if err != nil {
		t.Fatalf("Submit(d) after restart: %v", err)
	}
	if seq != 4 {
		t.Fatalf("post-restart seq = %d, want 4", seq)
	}
}

// TestRecoveryIgnoresForeignOps verifies the recovery hook is safe on a WAL
// shared with the store: non-PBFT records are ignored, not replayed as orders.
func TestRecoveryIgnoresForeignOps(t *testing.T) {
	r, err := NewReplica(Config{ID: "a"}, &memTransport{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyRecoveredRecord(wal.Record{Op: wal.OpPut, Key: "k", Val: "v"}); err != nil {
		t.Fatalf("foreign record should be ignored, got error: %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.log) != 0 {
		t.Fatalf("foreign record rebuilt %d log entries, want 0", len(r.log))
	}
}
