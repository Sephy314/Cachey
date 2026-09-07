package server

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Persistent-node tests (HS-M5): a fully wired HotStuff node — engine over TCP
// plus a store FSM sharing one WAL — must survive a full restart: the FSM
// data is rebuilt from its own WAL records (a restarted node is NOT empty),
// the engine's recovered watermark prevents re-execution, and the node's
// Ed25519 identity is stable so signatures in past QCs still verify.

// bootPersistentCluster opens one persistent HotStuff node per id under base
// and wires the full-mesh transport (peer addresses, ConnectPeers key
// exchange, persisted peer pins, leader resolver).
func bootPersistentCluster(t *testing.T, ids []string, base string) map[string]*HotStuffNode {
	t.Helper()
	validatorKeys := make(map[string]ed25519.PublicKey, len(ids))
	// Establish immutable validator identities before any node becomes network
	// visible. OpenHotStuffNode verifies its local identity against this map and
	// the transport checks every Hello against the same configured key.
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		priv, err := loadHSIdentity(dir, id)
		if err != nil {
			t.Fatal(err)
		}
		validatorKeys[id] = priv.Public().(ed25519.PublicKey)
	}
	nodes := make(map[string]*HotStuffNode)
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		n, err := OpenHotStuffNode(HotStuffNodeConfig{
			ID: id, Dir: dir, HSAddr: "127.0.0.1:0",
			Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: validatorKeys,
		})
		if err != nil {
			t.Fatalf("OpenHotStuffNode(%s): %v", id, err)
		}
		nodes[id] = n
	}
	addrs := make(map[string]string, len(ids))
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
	return nodes
}

func peersOf(ids []string, self string) []string {
	var out []string
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

// closePersistentCluster stops transports and flushes/closes each node's WAL.
func closePersistentCluster(nodes map[string]*HotStuffNode) {
	for _, n := range nodes {
		n.Tr.Close()
		n.WAL.Close()
	}
}

// allFSMHas polls every node's store FSM until key == want.
func allFSMHas(t *testing.T, nodes map[string]*HotStuffNode, key, want string) {
	t.Helper()
	waitFor(t, key+"="+want+" on all nodes", 10*time.Second, func() bool {
		for _, n := range nodes {
			v, err := n.Store.Get(key)
			if err != nil || v == nil || *v != want {
				return false
			}
		}
		return true
	})
}

// TestHotStuffPersistentRestart commits data on a 4-node persistent cluster,
// restarts every node from its data dir, and verifies the FSM data survived
// (rebuilt from the store's WAL records) and the node keeps committing with a
// stable Ed25519 identity.
func TestHotStuffPersistentRestart(t *testing.T) {
	base := t.TempDir()
	ids := []string{"p0", "p1", "p2", "p3"}

	// Life 1: write through the leader and converge.
	nodes := bootPersistentCluster(t, ids, base)
	leaderPub := nodes[ids[0]].Node.PublicKey()
	lead := nodes[ids[0]].CS
	if err := lead.Put("k", "v"); err != nil {
		t.Fatalf("Put(k): %v", err)
	}
	if err := lead.Put("k2", "v2"); err != nil {
		t.Fatalf("Put(k2): %v", err)
	}
	allFSMHas(t, nodes, "k", "v")
	closePersistentCluster(nodes)

	// Life 2: reopen from the same data dirs.
	nodes2 := bootPersistentCluster(t, ids, base)
	defer closePersistentCluster(nodes2)

	// The FSM data survived the restart (P0: a restarted real store is not
	// empty — the store's own WAL records rebuilt it).
	got, err := nodes2[ids[0]].Store.Get("k")
	if err != nil || got == nil || *got != "v" {
		t.Fatalf("FSM data lost across restart: Get(k)=%v err=%v, want v", got, err)
	}

	// The engine recovered and the node keeps committing (and its identity is
	// stable, so recovered/peer signatures still verify). The extra write (k4)
	// is not strictly needed for correctness — the leader returns once k3 is
	// committed — but it exercises continued replication across the restarted
	// cluster.
	if pb := nodes2[ids[0]].Node.PublicKey(); string(pb) != string(leaderPub) {
		t.Fatal("node Ed25519 identity changed across restart")
	}
	lead2 := nodes2[ids[0]].CS
	if err := lead2.Put("k3", "v3"); err != nil {
		t.Fatalf("Put(k3) after restart: %v", err)
	}
	allFSMHas(t, nodes2, "k3", "v3")
	if err := lead2.Put("k4", "v4"); err != nil {
		t.Fatalf("Put(k4) after restart: %v", err)
	}
	// Everything from before the crash is present everywhere too (the leader's
	// store rebuilt from its own WAL records; lagging followers catch up as the
	// chain advances).
	allFSMHas(t, nodes2, "k", "v")
	allFSMHas(t, nodes2, "k2", "v2")
	allFSMHas(t, nodes2, "k4", "v4")
}

// TestHotStuffPeerKeyPinned verifies a peer's public key, once pinned, cannot
// be overwritten by a different key — the transport-level fix for the Hello
// TOFU impersonation.
func TestHotStuffPeerKeyPinned(t *testing.T) {
	base := t.TempDir()
	ids := []string{"p0", "p1", "p2", "p3"}
	nodes := bootPersistentCluster(t, ids, base)
	defer closePersistentCluster(nodes)

	p1 := nodes[ids[1]].Node
	pub, ok := p1.PeerKey(ids[0])
	if !ok || len(pub) == 0 {
		t.Fatal("leader key should be pinned on a follower after ConnectPeers")
	}
	// A forged, different key for the same member must be refused.
	_, attackerPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	attackerPub := attackerPriv.Public().(ed25519.PublicKey)
	if p1.SetPeerKey(ids[0], attackerPub) {
		t.Fatal("a different key for an already-pinned member was accepted")
	}
	// The original pin is intact.
	if got, _ := p1.PeerKey(ids[0]); string(got) != string(pub) {
		t.Fatal("pinned key was replaced")
	}
	// Re-pinning the SAME key is a no-op success.
	if !p1.SetPeerKey(ids[0], pub) {
		t.Fatal("re-pinning the same key should succeed")
	}
}

// TestOpenHotStuffNodeRequiresFullValidatorConfiguration ensures the durable
// node cannot accidentally fall back to Hello TOFU on a first boot: every
// validator key must be fixed before the listener is opened.
func TestOpenHotStuffNodeRequiresFullValidatorConfiguration(t *testing.T) {
	dir := t.TempDir()
	priv, err := loadHSIdentity(dir, "n0")
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenHotStuffNode(HotStuffNodeConfig{
		ID: "n0", Dir: dir, HSAddr: "127.0.0.1:0", Peers: []string{"n1"},
		ValidatorKeys: map[string]ed25519.PublicKey{"n0": priv.Public().(ed25519.PublicKey)},
	})
	if err == nil {
		t.Fatal("incomplete validator key configuration was accepted")
	}
}
