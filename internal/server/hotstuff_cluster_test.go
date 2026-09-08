package server

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/hotstuff"
	"github.com/Sephy314/Cachey/internal/store"
)

// startHotStuffCluster boots a 4-replica HotStuff cluster over TCP (HS-M5)
// and wraps each replica in a HotStuffClusterStore over an in-memory CacheyStore
// FSM. Returns the stores (indexed by replica id). The view-0 leader is the
// first id. Validator public keys are fixed before listeners open; transport
// Hello verifies possession of those configured identities.
func startHotStuffCluster(t *testing.T, ids []string) map[string]*HotStuffClusterStore {
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
		tr *hotstuff.TCPTransport
		cs *HotStuffClusterStore
	}
	holders := make(map[string]*holder)
	stores := make(map[string]*HotStuffClusterStore)
	for _, id := range ids {
		tr := hotstuff.NewTCPTransport(nil)
		st := store.NewCacheyStore()
		r, err := hotstuff.NewReplica(hotstuff.Config{ID: id, Peers: peersOf(id), Leader: ids[0]}, tr, NewHotStuffApply(st))
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		tr.SetNode(r)
		cs := NewHotStuffClusterStore(r, st)
		holders[id] = &holder{tr: tr, cs: cs}
		stores[id] = cs
	}
	// Fix every validator's key before any connection is accepted. Hello only
	// proves possession of this configured key; it never learns trust from the
	// first peer that happens to connect.
	validatorKeys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		validatorKeys[id] = holders[id].cs.node.PublicKey()
	}
	for _, id := range ids {
		if err := holders[id].tr.SetValidatorKeys(validatorKeys); err != nil {
			t.Fatalf("SetValidatorKeys(%s): %v", id, err)
		}
	}
	// Now expose listeners, register every peer's real TCP address, and give
	// each store a fake client-address resolver for redirect hints.
	trAddrs := make(map[string]string, len(ids))
	clientAddrs := make(map[string]string, len(ids))
	for _, id := range ids {
		if _, err := holders[id].tr.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("Listen(%s): %v", id, err)
		}
		trAddrs[id] = holders[id].tr.Addr()
		clientAddrs[id] = "client://" + id
	}
	for _, id := range ids {
		holders[id].tr.SetPeers(trAddrs)
		stores[id].SetLeaderResolver(func(leaderID string) string { return clientAddrs[leaderID] })
	}
	// Full-mesh connectivity remains necessary for QC delivery, but it no longer
	// establishes key trust: validatorKeys above did that before Listen.
	for _, id := range ids {
		holders[id].tr.ConnectPeers(time.Now().Add(10 * time.Second))
	}
	t.Cleanup(func() {
		for _, id := range ids {
			holders[id].tr.Close()
		}
	})
	return stores
}

// waitFor polls cond until it holds or the timeout passes (see cluster_test.go).

// TestHotStuffStoreWritesAndReads drives writes through the view-0 leader and
// reads them back, verifying every replica's FSM converges and that followers
// reject reads/writes with hotstuff.ErrNotLeader and advertise the leader.
func TestHotStuffStoreWritesAndReads(t *testing.T) {
	stores := startHotStuffCluster(t, []string{"h0", "h1", "h2", "h3"})
	leader := stores["h0"]

	if err := leader.Put("k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := leader.Put("k2", "v2"); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}
	if err := leader.TTL("k2", 60000); err != nil {
		t.Fatalf("TTL: %v", err)
	}

	got, err := leader.Get("k")
	if err != nil {
		t.Fatalf("Get(k) on leader: %v", err)
	}
	if *got != "v" {
		t.Fatalf("Get(k) = %q, want v", *got)
	}

	// Every replica's FSM converged on the writes.
	for _, id := range []string{"h1", "h2", "h3"} {
		waitFor(t, id+" to converge", 10*time.Second, func() bool {
			v, err := stores[id].fsm.Get("k")
			return err == nil && v != nil && *v == "v"
		})
	}

	// Followers reject reads and writes (clients redirect to Leader()).
	if _, err := stores["h1"].Get("k"); err != hotstuff.ErrNotLeader {
		t.Fatalf("Get on follower = %v, want hotstuff.ErrNotLeader", err)
	}
	if err := stores["h1"].Put("x", "y"); err != hotstuff.ErrNotLeader {
		t.Fatalf("Put on follower = %v, want hotstuff.ErrNotLeader", err)
	}

	// Leader hint: empty on the leader, the leader's address on a follower.
	if l := leader.Leader(); l != "" {
		t.Fatalf("leader Leader() = %q, want empty", l)
	}
	if l := stores["h1"].Leader(); l != "client://h0" {
		t.Fatalf("follower Leader() = %q, want client://h0", l)
	}

	// DEL removes the key everywhere (the leader sees it applied locally).
	if err := leader.Delete("k"); err != nil {
		t.Fatalf("Delete(k): %v", err)
	}
	if _, err := leader.Get("k"); err != store.ErrorCodeInvalidKey {
		t.Fatalf("Get(k) after delete = %v, want invalid-key error", err)
	}
}
