package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// memStore is a tiny in-memory KV used to exercise the recovery hooks.
type memStore struct {
	mu   sync.Mutex
	data map[string]string
}

func newMemStore() *memStore { return &memStore{data: map[string]string{}} }

func (m *memStore) applySnapshot(entries []SnapshotEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		m.data[e.Key] = e.Val
	}
	return nil
}

func (m *memStore) applyRecord(rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch rec.Op {
	case OpPut:
		m.data[rec.Key] = rec.Val
	case OpDelete:
		delete(m.data, rec.Key)
	}
	return nil
}

func (m *memStore) snapshot() ([]SnapshotEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SnapshotEntry, 0, len(m.data))
	for k, v := range m.data {
		out = append(out, SnapshotEntry{Key: k, Val: v})
	}
	return out, nil
}

func (m *memStore) get(k string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[k]
}

func testCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = cancel // released at ctx expiry
	return ctx
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func readWALFile(t *testing.T, dir string) []Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, activeWALName))
	if err != nil {
		t.Fatalf("read active WAL: %v", err)
	}
	var recs []Record
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if line == "" {
			continue
		}
		var r Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("parse record %q: %v", line, err)
		}
		recs = append(recs, r)
	}
	return recs
}

func writeRaw(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeJSON(t *testing.T, dir, name string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	writeRaw(t, dir, name, string(append(b, '\n')))
}

func TestAppendAssignsSequentialIndexes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Threshold = 1 << 40 // never rotate
	w, err := Open(cfg, Hooks{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		if err := w.Append(testCtx(), Record{Op: OpPut, Key: fmt.Sprintf("k%d", i), Val: "v"}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if got := w.writer.LastIndex(); got != 5 {
		t.Fatalf("LastIndex = %d, want 5", got)
	}
	recs := readWALFile(t, dir)
	if len(recs) != 5 {
		t.Fatalf("file has %d records, want 5", len(recs))
	}
	for i, r := range recs {
		if r.LogIndex != uint64(i+1) {
			t.Fatalf("record %d index = %d, want %d", i, r.LogIndex, i+1)
		}
		if r.Op != OpPut || r.Key != fmt.Sprintf("k%d", i) {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
}

func TestRotationSealsSnapshotsAndContinues(t *testing.T) {
	dir := t.TempDir()
	ms := newMemStore()
	ms.data["seed"] = "s"
	cfg := DefaultConfig(dir)
	cfg.Threshold = 5
	cfg.CheckInterval = 5 * time.Millisecond
	cfg.AckTimeout = time.Second

	// The real store holds its lock across append+apply; that is what makes
	// the manager's snapshot consistent with the sealed WAL. Mirror it here
	// so the snapshot always sees every acknowledged write.
	var mu sync.Mutex
	var w *WAL
	put := func(key string) {
		mu.Lock()
		defer mu.Unlock()
		rec := Record{Op: OpPut, Key: key, Val: "v"}
		if err := w.Append(testCtx(), rec); err != nil {
			t.Fatalf("append %s: %v", key, err)
		}
		ms.applyRecord(rec)
	}
	snapshot := func() ([]SnapshotEntry, error) {
		mu.Lock()
		defer mu.Unlock()
		return ms.snapshot()
	}

	var err error
	w, err = Open(cfg, Hooks{
		ApplySnapshot: ms.applySnapshot,
		ApplyRecord:   ms.applyRecord,
		Snapshot:      snapshot,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	var fatalErr error
	w.manager.onFatal = func(err error) { fatalErr = err }

	for i := 0; i < 5; i++ {
		put(fmt.Sprintf("pre%d", i))
	}
	// Wait for the manager to seal + snapshot.
	snapPath := filepath.Join(dir, snapshotName)
	waitFor(t, func() bool {
		_, err := os.Stat(snapPath)
		return err == nil
	})

	// Append fewer than the threshold so no second rotation is triggered and
	// the snapshot boundary stays deterministic at 5.
	for i := 0; i < 4; i++ {
		put(fmt.Sprintf("post%d", i))
	}
	if got := w.writer.LastIndex(); got != 9 {
		t.Fatalf("LastIndex = %d, want 9", got)
	}
	if fatalErr != nil {
		t.Fatalf("manager fatal: %v", fatalErr)
	}

	// tmp must be gone after rotation.
	if _, err := os.Stat(filepath.Join(dir, tmpWALName)); !os.IsNotExist(err) {
		t.Fatalf("temporary WAL still exists after rotation")
	}

	// Recover into a fresh store: snapshot(5) + wal(6..9).
	ms2 := newMemStore()
	last, err := Recover(dir, Hooks{ApplySnapshot: ms2.applySnapshot, ApplyRecord: ms2.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 9 {
		t.Fatalf("recovered last = %d, want 9", last)
	}
	if ms2.get("seed") != "s" {
		t.Fatalf("snapshot data lost: seed = %q", ms2.get("seed"))
	}
	for i := 0; i < 5; i++ {
		if ms2.get(fmt.Sprintf("pre%d", i)) != "v" {
			t.Fatalf("pre%d missing after recovery", i)
		}
	}
	for i := 0; i < 4; i++ {
		if ms2.get(fmt.Sprintf("post%d", i)) != "v" {
			t.Fatalf("post%d missing after recovery", i)
		}
	}

	snap, err := readSnapshot(snapPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if snap.LastLogIndex != 5 {
		t.Fatalf("snapshot last_log_index = %d, want 5", snap.LastLogIndex)
	}
}

func TestRecoverBootstrap(t *testing.T) {
	dir := t.TempDir()
	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplySnapshot: ms.applySnapshot, ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 0 {
		t.Fatalf("last = %d, want 0", last)
	}
	if len(ms.data) != 0 {
		t.Fatalf("store not empty: %v", ms.data)
	}
}

func TestRecoverTruncatesPartialTail(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"a","val":"1","log_index":1}`+"\n"+
			`{"op":"PUT","key":"b","val":"2","log_index":2}`+"\n"+
			`{"op":"PUT","key":"f`)
	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 2 {
		t.Fatalf("last = %d, want 2", last)
	}
	if ms.get("b") != "2" {
		t.Fatalf("b = %q, want 2", ms.get("b"))
	}
	b, _ := os.ReadFile(filepath.Join(dir, activeWALName))
	if strings.Count(string(b), "\n") != 2 {
		t.Fatalf("wal not truncated: %q", string(b))
	}
}

func TestRecoverFixesUnterminatedTailRecord(t *testing.T) {
	dir := t.TempDir()
	// A fully-written record whose trailing newline was lost in the crash.
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"a","val":"1","log_index":1}`+"\n"+
			`{"op":"PUT","key":"b","val":"2","log_index":2}`)
	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 2 {
		t.Fatalf("last = %d, want 2", last)
	}
	if ms.get("b") != "2" {
		t.Fatalf("b = %q, want 2", ms.get("b"))
	}
	// Framing fixed: file now ends with a newline and both records parse.
	if recs := readWALFile(t, dir); len(recs) != 2 {
		t.Fatalf("active WAL has %d records, want 2", len(recs))
	}
}

func TestRecoverFailsOnIndexGap(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"a","val":"1","log_index":1}`+"\n"+
			`{"op":"PUT","key":"b","val":"2","log_index":4}`+"\n")
	ms := newMemStore()
	if _, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord}); err == nil {
		t.Fatal("Recover succeeded, want index-gap error")
	}
}

func TestRecoverFailsOnMiddleCorruption(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"a","val":"1","log_index":1}`+"\n"+
			`this is not json`+"\n"+
			`{"op":"PUT","key":"b","val":"2","log_index":2}`+"\n")
	ms := newMemStore()
	if _, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord}); err == nil {
		t.Fatal("Recover succeeded, want corruption error")
	}
}

func TestRecoverCaseAMergesWalAndTmp(t *testing.T) {
	dir := t.TempDir()
	// Snapshot from the previous rotation covers 1..2.
	writeJSON(t, dir, snapshotName, snapshotData{
		LastLogIndex: 2,
		Data:         map[string]snapEntry{"a": {Val: "1"}, "b": {Val: "2"}},
	})
	// Crash mid-sealing: the active WAL holds 3..4, the temp WAL holds 5..6.
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"c","val":"3","log_index":3}`+"\n"+
			`{"op":"PUT","key":"d","val":"4","log_index":4}`+"\n")
	writeRaw(t, dir, tmpWALName,
		`{"op":"PUT","key":"e","val":"5","log_index":5}`+"\n"+
			`{"op":"PUT","key":"f","val":"6","log_index":6}`+"\n")

	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplySnapshot: ms.applySnapshot, ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 6 {
		t.Fatalf("last = %d, want 6", last)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6"} {
		if ms.get(k) != want {
			t.Fatalf("%s = %q, want %q", k, ms.get(k), want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, tmpWALName)); !os.IsNotExist(err) {
		t.Fatalf("temporary WAL still exists after rebuild")
	}
	// Rebuild merges wal (3..4) + tmp (5..6) into the active WAL.
	recs := readWALFile(t, dir)
	if len(recs) != 4 {
		t.Fatalf("active WAL has %d records, want 4", len(recs))
	}
	if recs[0].LogIndex != 3 || recs[3].LogIndex != 6 || recs[3].Key != "f" {
		t.Fatalf("merged WAL indices wrong: %+v", recs)
	}
}

func TestRecoverCaseBSnapshotPlusWal(t *testing.T) {
	dir := t.TempDir()
	writeJSON(t, dir, snapshotName, snapshotData{
		LastLogIndex: 2,
		Data:         map[string]snapEntry{"a": {Val: "1"}, "b": {Val: "old"}},
	})
	writeRaw(t, dir, activeWALName,
		`{"op":"PUT","key":"b","val":"2","log_index":3}`+"\n"+
			`{"op":"DEL","key":"a","log_index":4}`+"\n")
	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplySnapshot: ms.applySnapshot, ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 4 {
		t.Fatalf("last = %d, want 4", last)
	}
	if ms.get("b") != "2" {
		t.Fatalf("b = %q, want 2 (idempotent replay over snapshot)", ms.get("b"))
	}
	if ms.get("a") != "" {
		t.Fatalf("a = %q, want deleted", ms.get("a"))
	}
}

func TestRecoverCaseCRestoresTmpOnly(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, tmpWALName,
		`{"op":"PUT","key":"x","val":"1","log_index":1}`+"\n")
	ms := newMemStore()
	last, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if last != 1 {
		t.Fatalf("last = %d, want 1", last)
	}
	if ms.get("x") != "1" {
		t.Fatalf("x = %q, want 1", ms.get("x"))
	}
	if _, err := os.Stat(filepath.Join(dir, tmpWALName)); !os.IsNotExist(err) {
		t.Fatalf("tmp not removed after restore")
	}
}

func TestRecoverIgnoresIncompleteSnapshotTmp(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, snapshotTmpName, `{"last_log_index":`)
	ms := newMemStore()
	if _, err := Recover(dir, Hooks{ApplyRecord: ms.applyRecord}); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotTmpName)); !os.IsNotExist(err) {
		t.Fatalf("snapshot.tmp not removed")
	}
}

// TestConcurrentRotationPreservesAckedData verifies the durability contract
// under background rotation: every acknowledged append survives a restart, no
// matter how many rotations happened concurrently.
func TestConcurrentRotationPreservesAckedData(t *testing.T) {
	dir := t.TempDir()
	ms := newMemStore()
	cfg := DefaultConfig(dir)
	cfg.Threshold = 3
	cfg.CheckInterval = time.Millisecond
	cfg.AckTimeout = 2 * time.Second
	cfg.HoldLimit = 100000

	w, err := Open(cfg, Hooks{
		ApplySnapshot: ms.applySnapshot,
		ApplyRecord:   ms.applyRecord,
		Snapshot:      ms.snapshot,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var fatalErr error
	w.manager.onFatal = func(err error) { fatalErr = err }

	const goroutines = 8
	const per = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				rec := Record{Op: OpPut, Key: fmt.Sprintf("g%d-%d", g, i), Val: "v"}
				ms.applyRecord(rec) // mimic the store applying to memory
				if err := w.Append(testCtx(), rec); err != nil {
					t.Errorf("append %s: %v", rec.Key, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if fatalErr != nil {
		t.Fatalf("manager fatal: %v", fatalErr)
	}

	// Graceful stop: quiesce any in-flight rotation, then stop the writer.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ms2 := newMemStore()
	last, err := Recover(dir, Hooks{ApplySnapshot: ms2.applySnapshot, ApplyRecord: ms2.applyRecord})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if want := uint64(goroutines * per); last != want {
		t.Fatalf("recovered last = %d, want %d", last, want)
	}
	for g := 0; g < goroutines; g++ {
		for i := 0; i < per; i++ {
			if got := ms2.get(fmt.Sprintf("g%d-%d", g, i)); got != "v" {
				t.Fatalf("g%d-%d = %q missing after recovery", g, i, got)
			}
		}
	}
}
