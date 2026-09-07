package server

import (
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/hotstuff"
	"github.com/Sephy314/Cachey/internal/store"
)

// startHotStuffCluster boots a 4-replica HotStuff cluster over TCP (HS-M5)
// and wraps each replica in a HotStuffClusterStore over an in-memory CacheyStore
// FSM. Returns the stores (indexed by replica id). The view-0 leader is the
// first id. Peer identity keys are exchanged on connect (transport Hello), so
// no manual key wiring is needed here.
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
		if _, err := tr.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("Listen(%s): %v", id, err)
		}
		cs := NewHotStuffClusterStore(r, st)
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
	// Full-mesh key exchange: HotStuff messages never connect followers to each
	// other (proposals leader→all, votes all→leader), yet followers must verify
	// QCs carrying any 2f+1 members' votes — so every node learns every
	// member's identity key up front over the transport Hello.
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
