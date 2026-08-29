package btree

import "errors"

// ErrNotFound is returned by Delete when the (exp, key) pair does not exist.
var ErrNotFound = errors.New("btree: expiration entry not found")

// Tree is a B+Tree keyed by (Exp, Key), used to efficiently find and range
// over the soonest-expiring keys without scanning the full data set.
//
// Complexity: Insert/Delete are O(log n); First is O(log n); Range(end) is
// O(log n + k) where k is the number of matching entries.
type Tree struct {
	root *node
}

// New creates an empty Tree.
func New() *Tree {
	return &Tree{root: newLeaf()}
}

// splitResult carries the separator key promoted to the parent and the new
// right sibling produced by a split.
type splitResult struct {
	key   Expiration
	right *node
}

// Insert adds (exp, key) to the tree. Re-inserting an existing (exp, key)
// pair is a no-op.
func (t *Tree) Insert(exp int64, key string) error {
	target := Expiration{Exp: exp, Key: key}
	if split := t.root.insert(target); split != nil {
		newRoot := newInternal()
		newRoot.keys = []Expiration{split.key}
		newRoot.children = []*node{t.root, split.right}
		t.root = newRoot
	}
	return nil
}

func (n *node) insert(target Expiration) *splitResult {
	if n.leaf {
		idx := n.lowerBound(target)
		if idx < len(n.keys) && equal(n.keys[idx], target) {
			return nil
		}
		n.keys = insertKey(n.keys, idx, target)
		if len(n.keys) <= maxKeys {
			return nil
		}
		return n.splitLeaf()
	}

	idx := n.childIndex(target)
	split := n.children[idx].insert(target)
	if split == nil {
		return nil
	}

	n.keys = insertKey(n.keys, idx, split.key)
	n.children = insertChild(n.children, idx+1, split.right)
	if len(n.keys) <= maxKeys {
		return nil
	}
	return n.splitInternal()
}

func (n *node) splitLeaf() *splitResult {
	mid := len(n.keys) / 2
	right := newLeaf()
	right.keys = append([]Expiration(nil), n.keys[mid:]...)
	n.keys = n.keys[:mid:mid]
	right.next = n.next
	n.next = right
	return &splitResult{key: right.keys[0], right: right}
}

func (n *node) splitInternal() *splitResult {
	mid := len(n.keys) / 2
	promoted := n.keys[mid]
	right := newInternal()
	right.keys = append([]Expiration(nil), n.keys[mid+1:]...)
	right.children = append([]*node(nil), n.children[mid+1:]...)
	n.keys = n.keys[:mid:mid]
	n.children = n.children[: mid+1 : mid+1]
	return &splitResult{key: promoted, right: right}
}

// Delete removes (exp, key) from the tree. Returns ErrNotFound if absent.
func (t *Tree) Delete(exp int64, key string) error {
	target := Expiration{Exp: exp, Key: key}
	if !t.root.delete(target) {
		return ErrNotFound
	}
	if !t.root.leaf && len(t.root.children) == 1 {
		t.root = t.root.children[0]
	}
	return nil
}

func (n *node) delete(target Expiration) bool {
	if n.leaf {
		idx := n.lowerBound(target)
		if idx >= len(n.keys) || !equal(n.keys[idx], target) {
			return false
		}
		n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
		return true
	}

	idx := n.childIndex(target)
	if !n.children[idx].delete(target) {
		return false
	}
	n.fixChild(idx)
	return true
}

// fixChild restores the minimum-occupancy invariant for n.children[idx] via
// borrowing from a sibling, or merging with one when borrowing isn't possible.
func (n *node) fixChild(idx int) {
	child := n.children[idx]
	if !child.underflow() {
		return
	}

	if idx > 0 && n.children[idx-1].canLend() {
		n.borrowFromLeft(idx)
		return
	}
	if idx < len(n.children)-1 && n.children[idx+1].canLend() {
		n.borrowFromRight(idx)
		return
	}
	if idx > 0 {
		n.mergeChildren(idx - 1)
	} else {
		n.mergeChildren(idx)
	}
}

func (n *node) borrowFromLeft(idx int) {
	child := n.children[idx]
	left := n.children[idx-1]

	if child.leaf {
		last := len(left.keys) - 1
		moved := left.keys[last]
		left.keys = left.keys[:last]
		child.keys = insertKey(child.keys, 0, moved)
		n.keys[idx-1] = child.keys[0]
		return
	}

	lastKey := len(left.keys) - 1
	lastChild := len(left.children) - 1
	movedChild := left.children[lastChild]
	child.keys = insertKey(child.keys, 0, n.keys[idx-1])
	child.children = insertChild(child.children, 0, movedChild)
	n.keys[idx-1] = left.keys[lastKey]
	left.keys = left.keys[:lastKey]
	left.children = left.children[:lastChild]
}

func (n *node) borrowFromRight(idx int) {
	child := n.children[idx]
	right := n.children[idx+1]

	if child.leaf {
		moved := right.keys[0]
		right.keys = append(right.keys[:0], right.keys[1:]...)
		child.keys = append(child.keys, moved)
		n.keys[idx] = right.keys[0]
		return
	}

	movedChild := right.children[0]
	child.keys = append(child.keys, n.keys[idx])
	child.children = append(child.children, movedChild)
	n.keys[idx] = right.keys[0]
	right.keys = append(right.keys[:0], right.keys[1:]...)
	right.children = append(right.children[:0], right.children[1:]...)
}

// mergeChildren merges n.children[idx] and n.children[idx+1], pulling down
// n.keys[idx] as the separator for internal nodes.
func (n *node) mergeChildren(idx int) {
	left := n.children[idx]
	right := n.children[idx+1]

	if left.leaf {
		left.keys = append(left.keys, right.keys...)
		left.next = right.next
	} else {
		left.keys = append(left.keys, n.keys[idx])
		left.keys = append(left.keys, right.keys...)
		left.children = append(left.children, right.children...)
	}

	n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
	n.children = append(n.children[:idx+1], n.children[idx+2:]...)
}
