package store

import "testing"

func TestCacheyStoreCRUD(t *testing.T) {
	cache := NewCacheyStore()
	if !cache.Alive() {
		t.Fatal("Alive() = false, want true")
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
