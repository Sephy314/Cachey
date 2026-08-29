// Package expiration provides a concurrency-safe TTL index on top of the
// internal B+Tree, used by Store to find and range over expiring keys
// without scanning the full data set.
package expiration

import (
	"sync"

	"github.com/Sephy314/Cachey/internal/btree"
)

// Expiration re-exports btree.Expiration so callers don't need to import
// the btree package directly.
type Expiration = btree.Expiration

// Index is a thread-safe wrapper around a btree.Tree keyed by (exp, key).
type Index struct {
	mu   sync.Mutex
	tree *btree.Tree
}

// NewIndex creates an empty expiration index.
func NewIndex() *Index {
	return &Index{tree: btree.New()}
}

// Insert records that key expires at exp (Unix milliseconds).
func (idx *Index) Insert(exp int64, key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tree.Insert(exp, key)
}

// Delete removes the (exp, key) entry. Returns an error if it isn't present.
func (idx *Index) Delete(exp int64, key string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.tree.Delete(exp, key)
}

// First returns the entry with the soonest expiration.
func (idx *Index) First() (Expiration, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.tree.First()
}

// Range returns every entry with Exp <= end, in ascending order.
func (idx *Index) Range(end int64) []Expiration {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.tree.Range(end)
}
