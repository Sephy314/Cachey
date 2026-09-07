package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/store"
)

// TestE2EFollowerIsolationCommitsThenHeals isolates ONE follower from the rest
// of the cluster in BOTH directions. The majority (leader + the other
// follower) must keep committing and serving; the isolated follower must not
// see the writes; after the partition heals it must catch up to every key, and
// membership must be unchanged.
func TestE2EFollowerIsolationCommitsThenHeals(t *testing.T) {
	dirs := map[string]string{"a": t.TempDir(), "b": t.TempDir(), "c": t.TempDir()}
	ec := newE2ECluster(t, []string{"a", "b", "c"}, dirs, 0)
	defer ec.stop()

	ec.put(t, "k0", "v0")
	leader := ec.pc.waitLeader(t)
	var iso string
	for _, id := range []string{"a", "b", "c"} {
		if id != leader {
			iso = id
			break
		}
	}

	// Cut the follower off in both directions.
	ec.partition(t, iso, true)
	const n = 5
	for i := 1; i <= n; i++ {
		ec.put(t, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// The majority holds every write; the isolated follower does not.
	waitFor(t, "majority nodes to hold all writes while a follower is isolated", 15*time.Second, func() bool {
		for id, cn := range ec.pc.nodes {
			if id == iso {
				continue
			}
			if v, err := cn.store.Get(fmt.Sprintf("k%d", n)); err != nil || *v != fmt.Sprintf("v%d", n) {
				return false
			}
		}
		if _, err := ec.pc.nodes[iso].store.Get(fmt.Sprintf("k%d", n)); err == nil {
			return false // isolated follower unexpectedly has the write
		}
		return true
	})

	// Heal the partition: the isolated follower must catch up to every key.
	ec.partition(t, iso, false)
	waitFor(t, "isolated follower to catch up after heal", 30*time.Second, func() bool {
		return ec.pc.hasKey(fmt.Sprintf("k%d", n), fmt.Sprintf("v%d", n))
	})

	// Membership is unchanged and the leader still serves.
	for id, cn := range ec.pc.nodes {
		if got := len(cn.node.Voters()); got != 3 {
			t.Fatalf("%s voters = %d, want 3", id, got)
		}
	}
	if got := ec.get(t, "k0"); got != "v0" {
		t.Fatalf("GET k0 after heal = %q, want v0", got)
	}

	// A read of a missing key still surfaces the store's invalid-key error on
	// the follower side (through the leader) rather than a stale value.
	if _, err := ec.pc.nodes[iso].store.Get("missing"); err != store.ErrorCodeInvalidKey {
		t.Fatalf("missing key on recovered follower = %v, want invalid-key", err)
	}
}
