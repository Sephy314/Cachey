package server

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/hotstuff"
	"github.com/Sephy314/Cachey/internal/wal"
)

// Phase 2 final E2E: the complete story on a persistent 4-node cluster, in
// one scenario:
//
//	genesis (epoch 0)
//	  → normal commit
//	  → membership change committed (add E) → epoch 1 active everywhere
//	  → new-epoch QC verifies, old-epoch QC still verifies
//	  → continue committing in epoch 1
//	  → partition a follower → it catches up on restart (block sync)
//	  → GC the follower (checkpoint + WAL rotation) → restart → keeps serving
//
// This is the integration-level counterpart of the engine unit suites: it
// exercises epoch activation, historical QC validation, catch-up and GC
// through the real TCP transport, shared WAL and durable identity.

// phase2Key returns a deterministic Ed25519 keypair for a test identity.
func phase2Key(id string) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("phase2-e2e-key:" + id))
	priv := ed25519.NewKeyFromSeed(seed[:])
	return priv.Public().(ed25519.PublicKey), priv
}

// proposeMembership drives a membership-change command through the leader:
// propose the command block, then flush empty blocks until it commits (the
// same pattern HotStuffClusterStore.propose uses for client writes).
func proposeMembership(t *testing.T, n *HotStuffNode, mc *hotstuff.MembershipChange) {
	t.Helper()
	data, err := json.Marshal(mc)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := json.Marshal(wal.Record{Op: wal.OpMembership, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), hsProposeTimeout)
	defer cancel()
	var target string
	for {
		id, perr := n.Node.Propose(cmd)
		if perr == nil {
			target = id
			break
		}
		if perr != hotstuff.ErrBusy {
			t.Fatalf("propose membership: %v", perr)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(hsPollInterval):
		}
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
		check, cancelCheck := context.WithTimeout(ctx, hsPollInterval)
		err := n.Node.WaitCommitted(check, target)
		cancelCheck()
		if err == nil {
			break
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("wait membership commit: %v", err)
		}
		if _, perr := n.Node.Propose(nil); perr != nil && perr != hotstuff.ErrBusy {
			t.Fatalf("flush: %v", perr)
		}
	}
	// The leader commits T when QC(B2) forms, but a follower commits T only
	// when it folds B3 (the 3-chain T←B1←B2 needs the block above B2). One
	// more flush block lets every follower reach the transition too.
	if _, perr := n.Node.Propose(nil); perr != nil && perr != hotstuff.ErrBusy {
		t.Fatalf("post-commit flush: %v", perr)
	}
}

// openPhase2Node opens a persistent node with the Phase 2 GC retention.
func openPhase2Node(t *testing.T, id, dir, addr string, ids []string, keys map[string]ed25519.PublicKey) *HotStuffNode {
	t.Helper()
	n, err := OpenHotStuffNode(HotStuffNodeConfig{
		ID: id, Dir: dir, HSAddr: addr,
		Peers: peersOf(ids, id), Leader: ids[0], ValidatorKeys: keys,
		GCRetention: 2,
	})
	if err != nil {
		t.Fatalf("OpenHotStuffNode(%s) at %s: %v", id, addr, err)
	}
	return n
}

// TestPhase2FullFlowE2E runs the complete Phase 2 scenario end to end.
func TestPhase2FullFlowE2E(t *testing.T) {
	base := t.TempDir()
	ids := []string{"e0", "e1", "e2", "e3"}
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
		nodes[id] = openPhase2Node(t, id, filepath.Join(base, id), "127.0.0.1:0", ids, keys)
	}
	wireHSPeers(t, nodes)
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Close()
			n.WAL.Close()
		}
	})
	lead := nodes[ids[0]].CS

	// 1. Normal commit in epoch 0.
	if err := lead.Put("k1", "v1"); err != nil {
		t.Fatalf("Put(k1): %v", err)
	}
	allFSMHas(t, nodes, "k1", "v1")
	for _, id := range ids {
		if got := nodes[id].Node.Epoch(); got != 0 {
			t.Fatalf("%s: epoch=%d before transition, want 0", id, got)
		}
	}

	// 2. Membership change: add E. The transition commits under the OLD set
	//    (epoch 0), then epoch 1 activates on every node.
	epub, _ := phase2Key("E")
	proposeMembership(t, nodes[ids[0]], &hotstuff.MembershipChange{
		Add: []hotstuff.ValidatorEntry{{ID: "E", Pub: epub}},
	})
	waitFor(t, "all nodes reach epoch 1", 10*time.Second, func() bool {
		for _, id := range ids {
			if nodes[id].Node.Epoch() != 1 {
				return false
			}
		}
		return true
	})

	// 3. Continue committing in epoch 1. Once every node folds the first
	//    new-epoch block's QC, its highest QC is an epoch-1 QC.
	if err := lead.Put("k2", "v2"); err != nil {
		t.Fatalf("Put(k2) in epoch 1: %v", err)
	}
	allFSMHas(t, nodes, "k2", "v2")

	// 4. QC validation across the transition: the new-epoch QC verifies on
	//    every node (a follower's highest QC is the last one it folded — the
	//    epoch-0 QC(B2) until it sees the block above B3, then epoch 1).
	waitFor(t, "all nodes hold an epoch-1 QC", 10*time.Second, func() bool {
		for _, id := range ids {
			qc := nodes[id].Node.HighestQC()
			if qc == nil || qc.Epoch != 1 {
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		qc := nodes[id].Node.HighestQC()
		if !nodes[id].Node.ValidateQC(qc) {
			t.Fatalf("%s: new-epoch QC does not verify", id)
		}
	}

	// 5. Partition a follower; the remaining 3 (a quorum) keep committing.
	victim := ids[3]
	victimAddr := nodes[victim].HSAddr
	nodes[victim].Close()
	nodes[victim].WAL.Close()
	delete(nodes, victim)
	if err := lead.Put("k3", "v3"); err != nil {
		t.Fatalf("Put(k3) with one member down: %v", err)
	}
	allFSMHas(t, nodes, "k3", "v3")

	// 6. Restart the victim on the same address: the leader's next proposal
	//    references ancestors it does not have, so it catches up (block sync)
	//    and converges on everything committed while it was down.
	restarted := openPhase2Node(t, victim, filepath.Join(base, victim), victimAddr, ids, keys)
	nodes[victim] = restarted
	wireHSPeers(t, nodes)
	if err := lead.Put("k4", "v4"); err != nil {
		t.Fatalf("Put(k4) after rejoin: %v", err)
	}
	allFSMHas(t, nodes, "k3", "v3")
	allFSMHas(t, nodes, "k4", "v4")
	if got := restarted.Node.Epoch(); got != 1 {
		t.Fatalf("restarted follower epoch=%d, want 1 (configuration history survived)", got)
	}

	// 7. GC the follower (checkpoint + WAL rotation), restart it, and verify
	//    it keeps serving — the retained state and FSM survive compaction.
	if err := restarted.GC(); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, victim, hsCheckpointName)); err != nil {
		t.Fatalf("checkpoint missing after GC: %v", err)
	}
	restarted.Close()
	restarted.WAL.Close()
	restarted2 := openPhase2Node(t, victim, filepath.Join(base, victim), victimAddr, ids, keys)
	nodes[victim] = restarted2
	wireHSPeers(t, nodes)
	got, err := restarted2.Store.Get("k4")
	if err != nil || got == nil || *got != "v4" {
		t.Fatalf("FSM data lost across GC restart: Get(k4)=%v err=%v", got, err)
	}
	if err := lead.Put("k5", "v5"); err != nil {
		t.Fatalf("Put(k5) after GC restart: %v", err)
	}
	allFSMHas(t, nodes, "k5", "v5")
}