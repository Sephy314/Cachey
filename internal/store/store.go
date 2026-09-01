package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Sephy314/Cachey/internal/expiration"
	"github.com/Sephy314/Cachey/internal/wal"
)

// walAppendTimeout bounds how long a write waits for WAL durability.
const walAppendTimeout = 5 * time.Second

type Store interface {
	Get(key string) (*string, error)
	Put(key string, value string) error
	Delete(key string) error
	TTL(key string, ttlMillis int64) error
	Alive() string
	// Leader returns the current cluster leader's client address, or "" if
	// this node is the leader, no leader is known, or the store is a
	// single node. Used to redirect clients.
	Leader() string
}

// Entry is a stored value with an optional expiration. Exp is a Unix
// millisecond timestamp; Exp == 0 means the entry never expires.
type Entry struct {
	Value string
	Exp   int64
}

type CacheyStore struct {
	mu    sync.RWMutex
	data  map[string]Entry
	index *expiration.Index
	wal   *wal.WAL
}

func NewCacheyStore() *CacheyStore {
	return &CacheyStore{
		data:  make(map[string]Entry),
		index: expiration.NewIndex(),
	}
}

// SetWAL enables write-ahead logging. Call before serving traffic; recovery
// (via wal.Open) must have already applied its hooks to this store.
func (s *CacheyStore) SetWAL(w *wal.WAL) {
	s.wal = w
}

// appendWAL durably records a mutation before it is applied to memory. A nil
// WAL means in-memory-only mode (used by tests and the plain constructor).
func (s *CacheyStore) appendWAL(rec wal.Record) error {
	if s.wal == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), walAppendTimeout)
	defer cancel()
	return s.wal.Append(ctx, rec)
}

func (s *CacheyStore) isExpired(e Entry) bool {
	return e.Exp != 0 && e.Exp <= time.Now().UnixMilli()
}

// expireKey lazily removes key if it is still expired, re-checking under the
// write lock in case a concurrent PUT/TTL refreshed it in the meantime.
func (s *CacheyStore) expireKey(key string, seen Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	if !ok || entry.Exp != seen.Exp || !s.isExpired(entry) {
		return
	}
	delete(s.data, key)
	s.index.Delete(entry.Exp, key)
}

func (s *CacheyStore) Get(key string) (*string, error) {
	s.mu.RLock()
	entry, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrorCodeInvalidKey
	}
	if s.isExpired(entry) {
		s.expireKey(key, entry)
		return nil, ErrorCodeInvalidKey
	}
	value := entry.Value
	return &value, nil
}

// Put stores value under key. Any existing TTL is cleared, matching the
// convention that overwriting a key resets its expiration. The mutation is
// durably recorded in the WAL before it is applied to memory.
func (s *CacheyStore) Put(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendWAL(wal.Record{Op: wal.OpPut, Key: key, Val: value}); err != nil {
		return err
	}
	if old, ok := s.data[key]; ok && old.Exp != 0 {
		s.index.Delete(old.Exp, key)
	}
	s.data[key] = Entry{Value: value}
	return nil
}

func (s *CacheyStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return ErrorCodeInvalidKey
	}
	if err := s.appendWAL(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
		return err
	}
	return s.deleteLocked(key)
}

func (s *CacheyStore) deleteLocked(key string) error {
	entry, ok := s.data[key]
	if !ok {
		return ErrorCodeInvalidKey
	}
	expired := s.isExpired(entry)
	delete(s.data, key)
	if entry.Exp != 0 {
		s.index.Delete(entry.Exp, key)
	}
	if expired {
		return ErrorCodeInvalidKey
	}
	return nil
}

// TTL sets key to expire ttlMillis from now, replacing any previous
// expiration and its stale index entry. The absolute expiry is durably logged
// to the WAL so a recovered key keeps its original expiry (an already-expired
// key is not resurrected with a fresh relative TTL).
func (s *CacheyStore) TTL(key string, ttlMillis int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return ErrorCodeInvalidKey
	}
	exp := time.Now().UnixMilli() + ttlMillis
	if err := s.appendWAL(wal.Record{Op: wal.OpTTL, Key: key, Exp: exp}); err != nil {
		return err
	}
	return s.ttlAtLocked(key, exp)
}

// ttlAtLocked sets key to expire at the absolute Unix-ms timestamp exp.
func (s *CacheyStore) ttlAtLocked(key string, exp int64) error {
	entry, ok := s.data[key]
	if !ok {
		return ErrorCodeInvalidKey
	}
	if entry.Exp != 0 {
		s.index.Delete(entry.Exp, key)
	}
	if s.isExpired(entry) {
		delete(s.data, key)
		return ErrorCodeInvalidKey
	}

	entry.Exp = exp
	s.data[key] = entry
	s.index.Insert(entry.Exp, key)
	return nil
}

// ApplySnapshot loads snapshot entries into the store (recovery replay).
func (s *CacheyStore) ApplySnapshot(entries []wal.SnapshotEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.data[e.Key] = Entry{Value: e.Val, Exp: e.Exp}
		if e.Exp != 0 {
			s.index.Insert(e.Exp, e.Key)
		}
	}
	return nil
}

// ApplyRecord replays one WAL record into the store (recovery replay). The
// operation is applied directly, never re-logged to the WAL.
func (s *CacheyStore) ApplyRecord(rec wal.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch rec.Op {
	case wal.OpPut:
		if old, ok := s.data[rec.Key]; ok && old.Exp != 0 {
			s.index.Delete(old.Exp, rec.Key)
		}
		s.data[rec.Key] = Entry{Value: rec.Val}
	case wal.OpDelete:
		s.deleteLocked(rec.Key)
	case wal.OpTTL:
		s.ttlAtLocked(rec.Key, rec.Exp)
	case wal.OpNoop:
		// Raft no-op entry: no state change.
	case wal.OpConfig:
		// Raft configuration-change entry: handled by the raft node, not the
		// store. Kept as a no-op so mixed-mode recovery is safe.
	default:
		return fmt.Errorf("wal: unknown op %q", rec.Op)
	}
	return nil
}

// Snapshot returns the store's live entries for snapshotting. Expired entries
// are skipped so they are not persisted.
func (s *CacheyStore) Snapshot() ([]wal.SnapshotEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UnixMilli()
	out := make([]wal.SnapshotEntry, 0, len(s.data))
	for k, e := range s.data {
		if e.Exp != 0 && e.Exp <= now {
			continue
		}
		out = append(out, wal.SnapshotEntry{Key: k, Val: e.Value, Exp: e.Exp})
	}
	return out, nil
}

func (s *CacheyStore) Alive() string {
	return "ALIVE"
}

// Leader reports no cluster leader for the single-node store.
func (s *CacheyStore) Leader() string { return "" }
