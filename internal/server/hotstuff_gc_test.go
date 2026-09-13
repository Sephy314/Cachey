package server

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Phase 2.5 GC integration tests at the fully-wired node level: the engine
// checkpoint is written durably and the shared WAL is rotated (store snapshot
// + truncation), so disk usage actually decreases; a restart recovers from the
// checkpoint + compacted WAL tail; and the node keeps committing.

// TestHotStuffNodeGCCompactsAndRestarts: a persistent node commits data, GCs
// (checkpoint + WAL rotation), restarts from the same dir, and recovers the
// retained state — the FSM data survives (store snapshot), the engine
// watermark survives (checkpoint), and the node catches up and keeps
// participating. A FOLLOWER is restarted (the rejoin pattern): the leader's
// transport re-dials it, so both directions are fresh.
func TestHotStuffNodeGCCompactsAndRestarts(t *testing.T) {
	base := t.TempDir()
	ids := []string{"g0", "g1", "g2", "g3"}
	keys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		priv, err := loadHSIdentity(dir, id)
		if err != nil {
			t.Fatal(err)
		}
		keys[id] = priv.Public().(ed25519.PublicKey)
	}
	nodes := make(map[string]*HotStuffNode)
	for _, id := range ids {
		n, err := OpenHotStuffNode(HotStuffNodeConfig{
			ID: id, Dir: filepath.Join(base, id), HSAddr: "127.0.0.1:0",
			Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: keys,
			GCRetention: 2,
		})
		if err != nil {
			t.Fatalf("OpenHotStuffNode(%s): %v", id, err)
		}
		nodes[id] = n
	}
	wireHSPeers(t, nodes)
	lead := nodes[ids[0]].CS
	for i := 0; i < 20; i++ {
		if err := lead.Put("k", "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	allFSMHas(t, nodes, "k", "v")

	// GC a follower: checkpoint + WAL rotation.
	victim := ids[1]
	if err := nodes[victim].GC(); err != nil {
		t.Fatalf("GC: %v", err)
	}
	// The checkpoint file exists and the WAL was compacted (snapshot present).
	if _, err := os.Stat(filepath.Join(base, victim, hsCheckpointName)); err != nil {
		t.Fatalf("checkpoint file missing after GC: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, victim, "snapshot")); err != nil {
		t.Fatalf("store snapshot missing after GC: %v", err)
	}

	// Restart the follower from the same dir on the SAME address: FSM data and
	// engine state survive.
	oldAddr := nodes[victim].HSAddr
	nodes[victim].Close()
	nodes[victim].WAL.Close()
	n2, err := OpenHotStuffNode(HotStuffNodeConfig{
		ID: victim, Dir: filepath.Join(base, victim), HSAddr: oldAddr,
		Peers: peersOf(ids, victim), Leader: ids[0], ValidatorKeys: keys,
		GCRetention: 2,
	})
	if err != nil {
		t.Fatalf("reopen(%s): %v", victim, err)
	}
	defer n2.Close()
	defer n2.WAL.Close()
	nodes[victim] = n2
	wireHSPeers(t, nodes) // re-establish the full mesh to the restarted follower
	got, err := n2.Store.Get("k")
	if err != nil || got == nil || *got != "v" {
		t.Fatalf("FSM data lost across GC restart: Get(k)=%v err=%v", got, err)
	}
	// The engine recovered its watermark; the cluster keeps committing and the
	// restarted follower catches up and votes.
	if err := lead.Put("k2", "v2"); err != nil {
		t.Fatalf("Put after GC restart: %v", err)
	}
	allFSMHas(t, nodes, "k2", "v2")
	for _, n := range nodes {
		n.Close()
		n.WAL.Close()
	}
}

// TestHotStuffNodeGCDiskShrinks: the shared WAL grows with commits, then a GC
// cycle (checkpoint + rotation) truncates it — disk usage decreases.
func TestHotStuffNodeGCDiskShrinks(t *testing.T) {
	base := t.TempDir()
	ids := []string{"d0", "d1", "d2", "d3"}
	keys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		priv, err := loadHSIdentity(dir, id)
		if err != nil {
			t.Fatal(err)
		}
		keys[id] = priv.Public().(ed25519.PublicKey)
	}
	nodes := make(map[string]*HotStuffNode)
	for _, id := range ids {
		n, err := OpenHotStuffNode(HotStuffNodeConfig{
			ID: id, Dir: filepath.Join(base, id), HSAddr: "127.0.0.1:0",
			Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: keys,
		})
		if err != nil {
			t.Fatalf("OpenHotStuffNode(%s): %v", id, err)
		}
		nodes[id] = n
	}
	wireHSPeers(t, nodes)
	lead := nodes[ids[0]].CS
	for i := 0; i < 30; i++ {
		if err := lead.Put("k", "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	allFSMHas(t, nodes, "k", "v")

	walPath := filepath.Join(base, ids[0], "wal.ndjson")
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if err := nodes[ids[0]].GC(); err != nil {
		t.Fatalf("GC: %v", err)
	}
	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after GC: %v", err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("WAL did not shrink: before=%d after=%d", before.Size(), after.Size())
	}
	// The node still works after compaction.
	if err := lead.Put("k3", "v3"); err != nil {
		t.Fatalf("Put after GC: %v", err)
	}
	allFSMHas(t, nodes, "k3", "v3")
	for _, n := range nodes {
		n.Close()
		n.WAL.Close()
	}
}

// TestHotStuffNodeGCBackgroundLoop: with a GC threshold set, the background
// loop compacts the WAL as it grows, and the node keeps committing.
func TestHotStuffNodeGCBackgroundLoop(t *testing.T) {
	base := t.TempDir()
	ids := []string{"b0", "b1", "b2", "b3"}
	keys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		dir := filepath.Join(base, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		priv, err := loadHSIdentity(dir, id)
		if err != nil {
			t.Fatal(err)
		}
		keys[id] = priv.Public().(ed25519.PublicKey)
	}
	nodes := make(map[string]*HotStuffNode)
	for _, id := range ids {
		n, err := OpenHotStuffNode(HotStuffNodeConfig{
			ID: id, Dir: filepath.Join(base, id), HSAddr: "127.0.0.1:0",
			Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: keys,
			GCThreshold: 50,
		})
		if err != nil {
			t.Fatalf("OpenHotStuffNode(%s): %v", id, err)
		}
		nodes[id] = n
	}
	wireHSPeers(t, nodes)
	lead := nodes[ids[0]].CS
	for i := 0; i < 40; i++ {
		if err := lead.Put("k", "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	allFSMHas(t, nodes, "k", "v")
	// The background loop should have compacted at least once.
	waitFor(t, "checkpoint written by background GC", 10*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(base, ids[0], hsCheckpointName))
		return err == nil
	})
	// The node keeps committing after background compaction.
	if err := lead.Put("k4", "v4"); err != nil {
		t.Fatalf("Put after background GC: %v", err)
	}
	allFSMHas(t, nodes, "k4", "v4")
	for _, n := range nodes {
		n.Close()
		n.WAL.Close()
	}
}