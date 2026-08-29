package btree

// First returns the entry with the smallest (Exp, Key), i.e. the soonest
// expiration. O(log n).
func (t *Tree) First() (Expiration, bool) {
	n := t.root
	for !n.leaf {
		if len(n.children) == 0 {
			return Expiration{}, false
		}
		n = n.children[0]
	}
	if len(n.keys) == 0 {
		return Expiration{}, false
	}
	return n.keys[0], true
}

// Range returns every entry with Exp <= end, in ascending order. It descends
// once to the leftmost leaf and then walks the leaf linked list, so the cost
// is O(log n + k) where k is the number of matching entries.
func (t *Tree) Range(end int64) []Expiration {
	n := t.root
	for !n.leaf {
		if len(n.children) == 0 {
			return nil
		}
		n = n.children[0]
	}

	var result []Expiration
	for n != nil {
		for _, e := range n.keys {
			if e.Exp > end {
				return result
			}
			result = append(result, e)
		}
		n = n.next
	}
	return result
}
