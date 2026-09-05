package server

import (
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/pbft"
	"github.com/Sephy314/Cachey/internal/store"
)

// startPbftCluster boots a 4-replica PBFT cluster over TCP and wraps each
// replica in a PbftClusterStore over an in-memory CacheyStore FSM. Returns the
// stores (indexed by replica id). The view-0 primary is the first id.
func startPbftCluster(t *testing.T, ids []string) map[string]*PbftClusterStore {
	t.Helper()
	peersOf := func(id string) []string {
		var out []string
		for _, other := range ids {
			if other != id {
				out = append(out, other)
			}
		}
		return out
	}
	type holder struct {
		tr *pbft.TCPTransport
		cs *PbftClusterStore
	}
	holders := make(map[string]*holder)
	stores := make(map[string]*PbftClusterStore)
	for _, id := range ids {
		tr := pbft.NewTCPTransport(nil)
		st := store.NewCacheyStore()
		r, err := pbft.NewReplica(pbft.Config{ID: id, Peers: peersOf(id)}, tr, NewPbftApply(st))
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		tr.SetNode(r)
		if _, err := tr.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("Listen(%s): %v", id, err)
		}
		cs := NewPbftClusterStore(r, st)
		holders[id] = &holder{tr: tr, cs: cs}
		stores[id] = cs
	}
	// Register every peer's real TCP address on every transport, and give each
	// store a fake client-address resolver for redirect hints.
	trAddrs := make(map[string]string, len(ids))
	clientAddrs := make(map[string]string, len(ids))
	for _, id := range ids {
		trAddrs[id] = holders[id].tr.Addr()
		clientAddrs[id] = "client://" + id
	}
	for _, id := range ids {
		holders[id].tr.SetPeers(trAddrs)
		stores[id].SetLeaderResolver(func(leaderID string) string { return clientAddrs[leaderID] })
	}
	t.Cleanup(func() {
		for _, id := range ids {
			holders[id].tr.Close()
		}
	})
	return stores
}

// TestPbftStoreWritesAndReads drives writes through the view-0 primary and
// reads them back, verifying every replica's FSM converges and that followers
// reject reads/writes with ErrNotPrimary and advertise the primary.
func TestPbftStoreWritesAndReads(t *testing.T) {
	stores := startPbftCluster(t, []string{"r0", "r1", "r2", "r3"})
	primary := stores["r0"]

	if err := primary.Put("k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := primary.Put("k2", "v2"); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}
	if err := primary.TTL("k2", 60000); err != nil {
		t.Fatalf("TTL: %v", err)
	}

	got, err := primary.Get("k")
	if err != nil {
		t.Fatalf("Get(k) on primary: %v", err)
	}
	if *got != "v" {
		t.Fatalf("Get(k) = %q, want v", *got)
	}

	// Every replica's FSM converged on the writes.
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		waitFor(id+" to converge", func() bool {
			v, err := stores[id].fsm.Get("k")
			return err == nil && *v == "v"
		})
	}

	// Followers reject reads and writes (clients redirect to Leader()).
	if _, err := stores["r1"].Get("k"); err != pbft.ErrNotPrimary {
		t.Fatalf("Get on follower = %v, want pbft.ErrNotPrimary", err)
	}
	if err := stores["r1"].Put("x", "y"); err != pbft.ErrNotPrimary {
		t.Fatalf("Put on follower = %v, want pbft.ErrNotPrimary", err)
	}

	// Leader hint: empty on the primary, the primary's address on a follower.
	if l := primary.Leader(); l != "" {
		t.Fatalf("primary Leader() = %q, want empty", l)
	}
	if l := stores["r1"].Leader(); l != "client://r0" {
		t.Fatalf("follower Leader() = %q, want client://r0", l)
	}

	// DEL removes the key everywhere.
	if err := primary.Delete("k"); err != nil {
		t.Fatalf("Delete(k): %v", err)
	}
	if _, err := primary.Get("k"); err != store.ErrorCodeInvalidKey {
		t.Fatalf("Get(k) after delete = %v, want invalid-key error", err)
	}
}
