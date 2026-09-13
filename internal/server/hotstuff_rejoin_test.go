package server

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Restart / rejoin suite for the fully wired node: a member that crashes while
// the cluster keeps committing must, on restart, rebuild its own FSM from its
// WAL, reconnect, catch up on everything it missed and keep serving. This is
// the "node restart" half of the recovery matrix; TestHotStuffPersistentRestart
// covers the whole-cluster restart.

// openHSNodeAt opens one persistent node at dir with an explicit listen address
// (so a restart can reclaim the same address its peers know).
func openHSNodeAt(t *testing.T, id, dir, addr string, ids []string, keys map[string]ed25519.PublicKey) *HotStuffNode {
	t.Helper()
	n, err := OpenHotStuffNode(HotStuffNodeConfig{
		ID: id, Dir: dir, HSAddr: addr,
		Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: keys,
	})
	if err != nil {
		t.Fatalf("OpenHotStuffNode(%s) at %s: %v", id, addr, err)
	}
	return n
}

// wireHSPeers points every node's transport at the given addresses and
// establishes the full mesh (re-running the Hello key exchange).
func wireHSPeers(t *testing.T, nodes map[string]*HotStuffNode) {
	t.Helper()
	addrs := make(map[string]string, len(nodes))
	for id, n := range nodes {
		addrs[id] = n.HSAddr
	}
	for _, n := range nodes {
		n.Tr.SetPeers(addrs)
		n.CS.SetLeaderResolver(func(leader string) string { return "client://" + leader })
	}
	for _, n := range nodes {
		n.Tr.ConnectPeers(time.Now().Add(10 * time.Second))
		if err := n.SavePeerPins(); err != nil {
			t.Fatalf("SavePeerPins(%s): %v", n.ID, err)
		}
	}
}

// TestHotStuffNodeRejoinsAfterCrash: the leader commits data with one member
// down (a quorum of 3 still exists), then that member is restarted from its
// data directory. It must
//
//   - come back with the data it had durably applied (not an empty store),
//   - reconnect to peers that still hold a stale connection to its old socket,
//   - catch up on everything committed while it was down, and
//   - take part in further commits.
func TestHotStuffNodeRejoinsAfterCrash(t *testing.T) {
	base := t.TempDir()
	ids := []string{"j0", "j1", "j2", "j3"}

	// Identity is durable, so validator keys can be rebuilt from the dirs at
	// any point (including after the restart below).
	keysOf := func() map[string]ed25519.PublicKey {
		keys := make(map[string]ed25519.PublicKey, len(ids))
		for _, id := range ids {
			priv, err := loadHSIdentity(filepath.Join(base, id), id)
			if err != nil {
				t.Fatal(err)
			}
			keys[id] = priv.Public().(ed25519.PublicKey)
		}
		return keys
	}
	for _, id := range ids {
		if err := os.MkdirAll(filepath.Join(base, id), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	keys := keysOf()

	nodes := make(map[string]*HotStuffNode, len(ids))
	for _, id := range ids {
		nodes[id] = openHSNodeAt(t, id, filepath.Join(base, id), "127.0.0.1:0", ids, keys)
	}
	wireHSPeers(t, nodes)
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Tr.Close()
			n.WAL.Close()
		}
	})

	victim := ids[3]
	victimAddr := nodes[victim].HSAddr

	// Life 1: commit k1, converge everywhere.
	if err := nodes[ids[0]].CS.Put("k1", "v1"); err != nil {
		t.Fatalf("Put(k1): %v", err)
	}
	allFSMHas(t, nodes, "k1", "v1")

	// Crash the victim: drop its transport and close its WAL. Its peers keep a
	// stale connection to the (now dead) address.
	nodes[victim].Tr.Close()
	nodes[victim].WAL.Close()
	delete(nodes, victim)

	// The remaining 3 members are still a quorum, so the cluster keeps
	// committing while the victim is down.
	if err := nodes[ids[0]].CS.Put("k2", "v2"); err != nil {
		t.Fatalf("Put(k2) with one member down: %v", err)
	}
	allFSMHas(t, nodes, "k2", "v2")

	// Restart the victim on the SAME address from its own data dir.
	restarted := openHSNodeAt(t, victim, filepath.Join(base, victim), victimAddr, ids, keysOf())
	nodes[victim] = restarted

	// It comes back with what it had durably applied before the crash.
	if v, err := restarted.Store.Get("k1"); err != nil || v == nil || *v != "v1" {
		t.Fatalf("restarted node lost its pre-crash data: Get(k1)=%v err=%v", v, err)
	}
	wireHSPeers(t, nodes)

	// The next write drives the chain forward; that is what lets the restarted
	// member fill the gap it missed (there is no separate state-sync protocol:
	// a proposal whose parent is unknown triggers a fetch of the missing
	// ancestors, so catch-up rides on normal progress).
	if err := nodes[ids[0]].CS.Put("k3", "v3"); err != nil {
		t.Fatalf("Put(k3) after rejoin: %v", err)
	}
	// It catches up on everything committed while it was down, and keeps up.
	allFSMHas(t, nodes, "k2", "v2")
	allFSMHas(t, nodes, "k3", "v3")

	// Sanity: the restarted node agrees with the others on the whole history.
	for _, n := range nodes {
		for _, kv := range [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}} {
			v, err := n.Store.Get(kv[0])
			if err != nil || v == nil || *v != kv[1] {
				t.Fatalf("%s: Get(%s)=%v err=%v, want %s", n.ID, kv[0], v, err, kv[1])
			}
		}
	}
}
