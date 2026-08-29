// Package btree implements a B+Tree used as the TTL expiration index for Cachey.
package btree

// Expiration pairs a key with its expiration timestamp (Unix milliseconds).
// Ordering is by Exp first, then Key, so multiple keys sharing the same
// expiration timestamp remain individually addressable.
type Expiration struct {
	Key string
	Exp int64
}

func less(a, b Expiration) bool {
	if a.Exp != b.Exp {
		return a.Exp < b.Exp
	}
	return a.Key < b.Key
}

func equal(a, b Expiration) bool {
	return a.Exp == b.Exp && a.Key == b.Key
}

// order bounds the fan-out of internal nodes and the number of entries per leaf.
const (
	order   = 8
	maxKeys = order - 1 // max entries in a leaf / max separators in an internal node
	minKeys = maxKeys / 2
)

// node is either a leaf (holding actual Expiration entries, linked to the next
// leaf for range scans) or an internal node (holding separator keys and
// len(keys)+1 children).
type node struct {
	leaf     bool
	keys     []Expiration
	children []*node
	next     *node
}

func newLeaf() *node {
	return &node{leaf: true}
}

func newInternal() *node {
	return &node{leaf: false}
}

// lowerBound returns the index of the first key >= target. Used for exact
// lookups and insertion position within a leaf.
func (n *node) lowerBound(target Expiration) int {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if less(n.keys[mid], target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// childIndex returns the index of the child subtree that must contain target:
// the first index i such that target < keys[i], i.e. the number of keys <= target.
func (n *node) childIndex(target Expiration) int {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if less(target, n.keys[mid]) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (n *node) underflow() bool {
	return len(n.keys) < minKeys
}

func (n *node) canLend() bool {
	return len(n.keys) > minKeys
}

func insertKey(keys []Expiration, idx int, key Expiration) []Expiration {
	keys = append(keys, Expiration{})
	copy(keys[idx+1:], keys[idx:])
	keys[idx] = key
	return keys
}

func insertChild(children []*node, idx int, child *node) []*node {
	children = append(children, nil)
	copy(children[idx+1:], children[idx:])
	children[idx] = child
	return children
}
