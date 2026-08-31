package store

import (
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/wal"
)

// TestStoreWALRecoveryRoundTrip verifies the durability contract: successful
// writes survive a restart (crash) and are replayed into a fresh store.
func TestStoreWALRecoveryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := wal.DefaultConfig(dir)
	cfg.Threshold = 1 << 40 // keep a single active WAL for this test

	st := NewCacheyStore()
	w, err := wal.Open(cfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   st.ApplyRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	st.SetWAL(w)

	if err := st.Put("a", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Put("b", "2"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.TTL("a", 100000); err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if err := st.Delete("b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Applied to memory.
	if v, err := st.Get("a"); err != nil || *v != "1" {
		t.Fatalf("Get a = %v, %v", v, err)
	}
	if _, err := st.Get("b"); err != ErrorCodeInvalidKey {
		t.Fatalf("Get b err = %v, want invalid key", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// "Crash": recover into a fresh store from the WAL only.
	st2 := NewCacheyStore()
	if _, err := wal.Recover(dir, wal.Hooks{
		ApplySnapshot: st2.ApplySnapshot,
		ApplyRecord:   st2.ApplyRecord,
	}); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if v, err := st2.Get("a"); err != nil || *v != "1" {
		t.Fatalf("recovered a = %v, %v", v, err)
	}
	if _, err := st2.Get("b"); err != ErrorCodeInvalidKey {
		t.Fatalf("recovered b err = %v, want invalid key", err)
	}
	if e := st2.data["a"]; e.Exp == 0 {
		t.Fatalf("recovered a lost its expiry")
	}
}

// TestStoreWALDoesNotResurrectExpiredKeys verifies that a key that expired
// before a crash is not brought back with a fresh relative TTL: the WAL stores
// the absolute expiry, so replay keeps the key expired.
func TestStoreWALDoesNotResurrectExpiredKeys(t *testing.T) {
	dir := t.TempDir()
	cfg := wal.DefaultConfig(dir)
	cfg.Threshold = 1 << 40

	st := NewCacheyStore()
	w, err := wal.Open(cfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   st.ApplyRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	st.SetWAL(w)

	if err := st.Put("k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.TTL("k", 50); err != nil {
		t.Fatalf("TTL: %v", err)
	}
	// Let the key expire in memory.
	time.Sleep(80 * time.Millisecond)
	if _, err := st.Get("k"); err != ErrorCodeInvalidKey {
		t.Fatalf("Get k err = %v, want invalid key (expired)", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2 := NewCacheyStore()
	if _, err := wal.Recover(dir, wal.Hooks{
		ApplySnapshot: st2.ApplySnapshot,
		ApplyRecord:   st2.ApplyRecord,
	}); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err := st2.Get("k"); err != ErrorCodeInvalidKey {
		t.Fatalf("recovered k err = %v, want invalid key (must stay expired)", err)
	}
}
