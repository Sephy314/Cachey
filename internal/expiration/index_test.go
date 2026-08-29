package expiration

import "testing"

func TestIndexInsertFirstDelete(t *testing.T) {
	idx := NewIndex()
	idx.Insert(200, "b")
	idx.Insert(100, "a")

	got, ok := idx.First()
	if !ok || got.Key != "a" || got.Exp != 100 {
		t.Fatalf("First() = %v, %v, want {a 100}, true", got, ok)
	}

	if err := idx.Delete(100, "a"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got, ok = idx.First()
	if !ok || got.Key != "b" {
		t.Fatalf("First() after delete = %v, %v, want b", got, ok)
	}
}

func TestIndexDeleteMissingReturnsError(t *testing.T) {
	idx := NewIndex()
	if err := idx.Delete(100, "missing"); err == nil {
		t.Fatal("Delete() on missing entry error = nil, want error")
	}
}

func TestIndexRange(t *testing.T) {
	idx := NewIndex()
	idx.Insert(100, "a")
	idx.Insert(150, "b")
	idx.Insert(500, "c")

	got := idx.Range(200)
	if len(got) != 2 {
		t.Fatalf("Range(200) len = %d, want 2", len(got))
	}
}
