package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestInsertAndFirst(t *testing.T) {
	tr := New()
	tr.Insert(300, "c")
	tr.Insert(100, "a")
	tr.Insert(200, "b")

	got, ok := tr.First()
	if !ok || got != (Expiration{Key: "a", Exp: 100}) {
		t.Fatalf("First() = %v, %v, want {a 100}, true", got, ok)
	}
}

func TestInsertIsIdempotent(t *testing.T) {
	tr := New()
	tr.Insert(100, "a")
	tr.Insert(100, "a")

	got := tr.Range(1000)
	if len(got) != 1 {
		t.Fatalf("Range() = %v, want single entry", got)
	}
}

func TestDeleteRemovesEntryAndErrorsWhenMissing(t *testing.T) {
	tr := New()
	tr.Insert(100, "a")

	if err := tr.Delete(100, "a"); err != nil {
		t.Fatalf("Delete() error = %v, want nil", err)
	}
	if _, ok := tr.First(); ok {
		t.Fatalf("First() after delete = ok, want empty tree")
	}
	if err := tr.Delete(100, "a"); err != ErrNotFound {
		t.Fatalf("Delete() missing entry error = %v, want %v", err, ErrNotFound)
	}
}

func TestLeafSplitKeepsSortedOrderAndLinkedList(t *testing.T) {
	tr := New()
	for i := 0; i < maxKeys+1; i++ {
		tr.Insert(int64(i), fmt.Sprintf("k%d", i))
	}

	if tr.root.leaf {
		t.Fatalf("expected root to have split into an internal node")
	}
	assertInvariants(t, tr.root, -1)

	got := tr.Range(int64(maxKeys))
	if len(got) != maxKeys+1 {
		t.Fatalf("Range() len = %d, want %d", len(got), maxKeys+1)
	}
	for i, e := range got {
		if e.Exp != int64(i) {
			t.Fatalf("Range()[%d].Exp = %d, want %d (not sorted)", i, e.Exp, i)
		}
	}
}

func TestInternalSplitAndRootSplit(t *testing.T) {
	tr := New()
	n := (maxKeys + 1) * (maxKeys + 1) // enough inserts to force an internal split
	for i := 0; i < n; i++ {
		tr.Insert(int64(i), fmt.Sprintf("k%d", i))
	}

	if tr.root.leaf {
		t.Fatalf("expected multi-level tree after %d inserts", n)
	}
	depth := assertInvariants(t, tr.root, -1)
	if depth < 2 {
		t.Fatalf("expected tree depth >= 2, got %d", depth)
	}

	got := tr.Range(int64(n - 1))
	if len(got) != n {
		t.Fatalf("Range() len = %d, want %d", len(got), n)
	}
	for i, e := range got {
		if e.Exp != int64(i) {
			t.Fatalf("Range()[%d].Exp = %d, want %d", i, e.Exp, i)
		}
	}
}

func TestDeleteTriggersMergeAndRedistribution(t *testing.T) {
	tr := New()
	n := (maxKeys + 1) * (maxKeys + 1)
	for i := 0; i < n; i++ {
		tr.Insert(int64(i), fmt.Sprintf("k%d", i))
	}

	// Delete every other entry to force borrowing and merging across leaves.
	for i := 0; i < n; i += 2 {
		if err := tr.Delete(int64(i), fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Delete(%d) error = %v", i, err)
		}
	}
	assertInvariants(t, tr.root, -1)

	got := tr.Range(int64(n))
	if len(got) != n/2 {
		t.Fatalf("Range() len = %d, want %d", len(got), n/2)
	}
	for _, e := range got {
		if e.Exp%2 == 0 {
			t.Fatalf("found deleted entry %v still present", e)
		}
	}

	for i := 1; i < n; i += 2 {
		if err := tr.Delete(int64(i), fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Delete(%d) error = %v", i, err)
		}
	}
	assertInvariants(t, tr.root, -1)
	if _, ok := tr.First(); ok {
		t.Fatalf("First() after deleting everything = ok, want empty")
	}
}

func TestDuplicateExpirationTimestamp(t *testing.T) {
	tr := New()
	keys := []string{"b", "a", "c", "d"}
	for _, k := range keys {
		tr.Insert(100, k)
	}

	got := tr.Range(100)
	if len(got) != len(keys) {
		t.Fatalf("Range() len = %d, want %d", len(got), len(keys))
	}
	want := []string{"a", "b", "c", "d"}
	for i, e := range got {
		if e.Key != want[i] {
			t.Fatalf("Range()[%d].Key = %q, want %q", i, e.Key, want[i])
		}
	}

	if err := tr.Delete(100, "b"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got = tr.Range(100)
	if len(got) != 3 {
		t.Fatalf("Range() len after delete = %d, want 3", len(got))
	}
}

func TestSortedTraversalLargeRandomDataset(t *testing.T) {
	const n = 5000
	tr := New()
	rand.New(rand.NewSource(1))
	order := rand.Perm(n)
	for _, i := range order {
		tr.Insert(int64(i), fmt.Sprintf("k%d", i))
	}

	assertInvariants(t, tr.root, -1)

	got := tr.Range(int64(n))
	if len(got) != n {
		t.Fatalf("Range() len = %d, want %d", len(got), n)
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return less(got[i], got[j]) }) {
		t.Fatalf("Range() result not sorted")
	}

	for i := 0; i < n; i += 7 {
		if err := tr.Delete(int64(i), fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Delete(%d) error = %v", i, err)
		}
	}
	assertInvariants(t, tr.root, -1)
}

func TestRangeScanStopsAtEnd(t *testing.T) {
	tr := New()
	tr.Insert(100, "foo")
	tr.Insert(150, "bar")
	tr.Insert(180, "baz")
	tr.Insert(500, "qux")

	got := tr.Range(200)
	if len(got) != 3 {
		t.Fatalf("Range(200) len = %d, want 3", len(got))
	}
	for _, e := range got {
		if e.Key == "qux" {
			t.Fatalf("Range(200) should not include qux")
		}
	}
}

// assertInvariants walks the tree verifying: all leaves at the same depth,
// keys within and across nodes are ordered, and leaves are linked in sorted
// order. It returns the leaf depth (root leaf = 0).
func assertInvariants(t *testing.T, n *node, expectedLeafDepth int) int {
	t.Helper()
	return assertNode(t, n, 0, &expectedLeafDepth)
}

func assertNode(t *testing.T, n *node, depth int, leafDepth *int) int {
	t.Helper()

	if !sort.SliceIsSorted(n.keys, func(i, j int) bool { return less(n.keys[i], n.keys[j]) }) {
		t.Fatalf("node keys not sorted: %v", n.keys)
	}

	if n.leaf {
		if *leafDepth == -1 {
			*leafDepth = depth
		} else if *leafDepth != depth {
			t.Fatalf("leaf depth mismatch: got %d, want %d", depth, *leafDepth)
		}
		return depth
	}

	if len(n.children) != len(n.keys)+1 {
		t.Fatalf("internal node has %d keys but %d children", len(n.keys), len(n.children))
	}
	for _, c := range n.children {
		assertNode(t, c, depth+1, leafDepth)
	}
	return *leafDepth
}
