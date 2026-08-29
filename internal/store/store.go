package store

import (
	"sync"
	"time"

	"github.com/Sephy314/Cachey/internal/expiration"
)

type Store interface {
	Get(key string) (*string, error)
	Put(key string, value string) error
	Delete(key string) error
	TTL(key string, ttlMillis int64) error
	Alive() string
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
}

func NewCacheyStore() *CacheyStore {
	return &CacheyStore{
		data:  make(map[string]Entry),
		index: expiration.NewIndex(),
	}
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
// convention that overwriting a key resets its expiration.
func (s *CacheyStore) Put(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.data[key]; ok && old.Exp != 0 {
		s.index.Delete(old.Exp, key)
	}
	s.data[key] = Entry{Value: value}
	return nil
}

func (s *CacheyStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
// expiration and its stale index entry.
func (s *CacheyStore) TTL(key string, ttlMillis int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

	entry.Exp = time.Now().UnixMilli() + ttlMillis
	s.data[key] = entry
	s.index.Insert(entry.Exp, key)
	return nil
}

func (s *CacheyStore) Alive() string {
	return "ALIVE"
}
