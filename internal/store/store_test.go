package store

import (
	"sync"
	"testing"
	"time"
)

func TestCacheyStoreCRUD(t *testing.T) {
	cache := NewCacheyStore()
	if cache.Alive() != "ALIVE" {
		t.Fatalf("Alive() = %q, want %q", cache.Alive(), "ALIVE")
	}

	if err := cache.Put("key", "value"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	value, err := cache.Get("key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if *value != "value" {
		t.Fatalf("Get() = %q, want %q", *value, "value")
	}

	if err := cache.Delete("key"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := cache.Get("key"); err != ErrorCodeInvalidKey {
		t.Errorf("Get() after Delete() error = %v, want %v", err, ErrorCodeInvalidKey)
	}
}

func TestCacheyStoreMissingKeyErrors(t *testing.T) {
	cache := NewCacheyStore()
	if _, err := cache.Get("missing"); err != ErrorCodeInvalidKey {
		t.Errorf("Get() error = %v, want %v", err, ErrorCodeInvalidKey)
	}
	if err := cache.Delete("missing"); err != ErrorCodeInvalidKey {
		t.Errorf("Delete() error = %v, want %v", err, ErrorCodeInvalidKey)
	}
}

func TestTTLExpiresKeyAndGetReturnsNotFound(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "bar")

	if value, err := cache.Get("foo"); err != nil || *value != "bar" {
		t.Fatalf("Get() before expiry = %v, %v, want bar, nil", value, err)
	}

	if err := cache.TTL("foo", 20); err != nil {
		t.Fatalf("TTL() error = %v", err)
	}
	if value, err := cache.Get("foo"); err != nil || *value != "bar" {
		t.Fatalf("Get() before TTL expiry = %v, %v, want bar, nil", value, err)
	}

	time.Sleep(30 * time.Millisecond)
	if _, err := cache.Get("foo"); err != ErrorCodeInvalidKey {
		t.Fatalf("Get() after TTL expiry error = %v, want %v", err, ErrorCodeInvalidKey)
	}
	// Lazy expiration must also clean up the index entry.
	if _, ok := cache.index.First(); ok {
		t.Fatalf("index still has entries after lazy expiration")
	}
}

func TestTTLMissingKeyErrors(t *testing.T) {
	cache := NewCacheyStore()
	if err := cache.TTL("missing", 100); err != ErrorCodeInvalidKey {
		t.Fatalf("TTL() error = %v, want %v", err, ErrorCodeInvalidKey)
	}
}

func TestTTLResetReplacesPreviousExpiration(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "bar")

	if err := cache.TTL("foo", 20); err != nil {
		t.Fatalf("TTL() error = %v", err)
	}
	if err := cache.TTL("foo", 1000); err != nil {
		t.Fatalf("TTL() error = %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	if value, err := cache.Get("foo"); err != nil || *value != "bar" {
		t.Fatalf("Get() after reset TTL = %v, %v, want bar, nil (old 20ms TTL should not apply)", value, err)
	}

	// Only a single index entry should remain for foo.
	count := 0
	for _, e := range cache.index.Range(1 << 62) {
		if e.Key == "foo" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("index has %d entries for foo after TTL reset, want 1", count)
	}
}

func TestDeleteRemovesExpirationIndexEntry(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "bar")
	cache.TTL("foo", 10000)

	if err := cache.Delete("foo"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok := cache.index.First(); ok {
		t.Fatalf("index still has entries after Delete()")
	}
}

func TestPutClearsExistingTTL(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "bar")
	cache.TTL("foo", 20)

	cache.Put("foo", "new-value")
	time.Sleep(30 * time.Millisecond)

	value, err := cache.Get("foo")
	if err != nil || *value != "new-value" {
		t.Fatalf("Get() after PUT overwrite = %v, %v, want new-value, nil", value, err)
	}
}

func TestTTLOnAlreadyExpiredKeyReturnsNotFound(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "bar")
	cache.TTL("foo", 10)
	time.Sleep(20 * time.Millisecond)

	if err := cache.TTL("foo", 1000); err != ErrorCodeInvalidKey {
		t.Fatalf("TTL() on expired key error = %v, want %v", err, ErrorCodeInvalidKey)
	}
}

func TestActiveExpirationCleansUpWithoutFullScan(t *testing.T) {
	cache := NewCacheyStore()
	cache.Put("foo", "1")
	cache.Put("bar", "2")
	cache.Put("baz", "3")
	cache.Put("qux", "4")
	cache.TTL("foo", 10)
	cache.TTL("bar", 10)
	cache.TTL("baz", 10)
	cache.TTL("qux", 10000)

	time.Sleep(20 * time.Millisecond)
	removed := cache.cleanupExpired(maxCleanupPerRun)
	if removed != 3 {
		t.Fatalf("cleanupExpired() removed = %d, want 3", removed)
	}
	if _, err := cache.Get("qux"); err != nil {
		t.Fatalf("Get(qux) error = %v, want nil (not expired)", err)
	}
	for _, k := range []string{"foo", "bar", "baz"} {
		if _, ok := cache.data[k]; ok {
			t.Fatalf("data[%s] still present after active cleanup", k)
		}
	}
}

func TestConcurrentTTLGetDelete(t *testing.T) {
	cache := NewCacheyStore()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		key := "key"
		cache.Put(key, "value")
		wg.Add(3)
		go func() {
			defer wg.Done()
			cache.TTL(key, 5)
		}()
		go func() {
			defer wg.Done()
			cache.Get(key)
		}()
		go func() {
			defer wg.Done()
			cache.Delete(key)
		}()
	}
	wg.Wait()
}

func TestLazyAndActiveExpirationConcurrently(t *testing.T) {
	cache := NewCacheyStore()
	const n = 200
	for i := 0; i < n; i++ {
		key := "key" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		cache.Put(key, "value")
		cache.TTL(key, 5)
	}
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cache.cleanupExpired(maxCleanupPerRun)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			key := "key" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			cache.Get(key)
		}
	}()
	wg.Wait()
}
