package hotstuff

import (
	"fmt"
	"testing"
	"time"
)

// HS-M2 tests: the pacemaker / view change. Safety (a view change never forks
// the committed prefix) and liveness (a quorum of correct replicas that
// suspect a dead leader elect the next leader, who resumes committing) are
// pinned deterministically via StartViewChange — no timers, no goroutines.

// TestGapAcceptedAndVoted: since HS-M2 a proposal may jump heights (the
// post-view-change block skips the deposed leader's in-flight height). A gap
// block is accepted and voted, and the chain rule works over parent links, so
// commands still commit exactly on a 3-chain.
func TestGapAcceptedAndVoted(t *testing.T) {
	f, tr := newFollower()

	// B1 at height 1, then a gap: block at height 3 whose parent is B1.
	p1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	gap := prop(3, p1.Block.ID, "b", quorumQC(p1.Block.ID, 1))
	f.HandleProposal(p1)
	f.HandleProposal(gap)

	f.mu.Lock()
	_, inTree := f.blocks[gap.Block.ID]
	f.mu.Unlock()
	if !inTree {
		t.Fatal("height-gap block must be accepted since HS-M2")
	}
	if got := tr.voteCount(gap.Block.ID); got != 1 {
		t.Fatalf("height-gap block must earn a vote, got %d", got)
	}

	// Continue the chain over the gap (heights 4, 5) — a 3-chain over parent
	// links reaches B1 and commits it.
	g2 := prop(4, gap.Block.ID, "c", quorumQC(gap.Block.ID, 3))
	f.HandleProposal(g2)
	g3 := prop(5, g2.Block.ID, "d", quorumQC(g2.Block.ID, 4))
	f.HandleProposal(g3)

	f.mu.Lock()
	applied := f.appliedCmdsLocked()
	f.mu.Unlock()
	if fmt.Sprint(applied) != "[a]" {
		t.Fatalf("3-chain over a gap should commit B1's command, applied=%v", applied)
	}
}

// TestStaleLeaderProposalIgnored: after a view change, a proposal from a past
// view's leader is stale and must be ignored (not added, not voted).
func TestStaleLeaderProposalIgnored(t *testing.T) {
	f, tr := newFollower()
	// F joins view 1 (as if it suspected the view-0 leader).
	f.mu.Lock()
	f.enterViewLocked(1)
	f.mu.Unlock()

	stale := prop(1, genesisID, "stale", quorumQC(genesisID, 0)) // view 0
	f.HandleProposal(stale)

	f.mu.Lock()
	_, inTree := f.blocks[stale.Block.ID]
	f.mu.Unlock()
	if inTree {
		t.Fatal("stale leader's proposal must not enter the tree")
	}
	if got := tr.voteCount(stale.Block.ID); got != 0 {
		t.Fatalf("stale leader's proposal must not earn a vote, got %d", got)
	}
}

// TestLeaderDeathViewChangeResumes (liveness): the view-0 leader commits "a"
// then stops. The other 2f+1 replicas suspect it (StartViewChange); the
// view-1 leader activates and commits "b". Every replica (the old leader
// rejoins as a follower when it sees the new view's proposal) converges on
// exactly [a b] — the committed prefix is preserved across the view change.
func TestLeaderDeathViewChangeResumes(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// L0 stops proposing. L1, L2, L3 suspect it and move to view 1.
	for _, id := range []string{"L1", "L2", "L3"} {
		nodes[id].StartViewChange()
	}

	// L1 (leaderOf(1)) is now active; it resumes and commits "b".
	if !nodes["L1"].IsLeader() {
		t.Fatal("L1 should be the active leader after the view change")
	}
	proposeLoop(t, nodes["L1"], []string{"b"}, 3)
	waitAllApplied(t, nw, ids, []string{"a", "b"})

	// Safety corollary: the committed prefix is unchanged — exactly [a b],
	// in that order, on every replica.
	for _, id := range ids {
		if got := nw.logs[id].snapshot(); fmt.Sprint(got) != "[a b]" {
			t.Fatalf("%s applied %v, want [a b]", id, got)
		}
	}
}

// TestNoProgressWithoutViewChange: with the leader silent and nobody
// suspecting it, no new leader emerges and nothing commits. (Liveness needs
// the timeout/view-change; safety alone never makes progress.)
func TestNoProgressWithoutViewChange(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// L0 goes silent; nobody suspects. A non-leader cannot propose and the
	// old leader does not either.
	if _, err := nodes["L1"].Propose([]byte("b")); err != ErrNotLeader {
		t.Fatalf("L1 before a view change: want ErrNotLeader, got %v", err)
	}
	// Nothing else drives the system; the applied set stays frozen.
	time.Sleep(20 * time.Millisecond)
	for _, id := range ids {
		if got := nw.logs[id].snapshot(); fmt.Sprint(got) != "[a]" {
			t.Fatalf("%s progressed without a view change: %v", id, got)
		}
	}
}

// TestViewChangeNeedsQuorum: fewer than 2f+1 view changes do not activate a
// new leader — a minority cannot depose the current leader.
func TestViewChangeNeedsQuorum(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// Only ONE replica (f = 1) suspects: not a quorum, so L1 must not become
	// active even though it is the view-1 leader.
	nodes["L2"].StartViewChange()
	if nodes["L1"].IsLeader() {
		t.Fatal("a single view change must not activate the new leader")
	}
	if _, err := nodes["L1"].Propose([]byte("b")); err != ErrNotLeader {
		t.Fatalf("L1 with a minority view change: want ErrNotLeader, got %v", err)
	}
}

// TestViewTimeoutFires (integration of the real timer): with a short view
// timeout armed on the followers, a silent leader is suspected automatically
// and the next leader resumes progress — exponential-backoff liveness without
// manual StartViewChange.
func TestViewTimeoutFires(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// L0 goes silent. Arm suspicion timers on the three other replicas; with
	// f=1 they form the quorum that elects L1 (leaderOf(1)). The base is kept
	// comfortably larger than one proposal round so an elected leader gets a
	// full grace period to produce progress before being suspected again — an
	// artificially tiny base would race the handler latency (each message now
	// costs an Ed25519 sign/verify, HS-M3), deposing the leader before it can
	// propose two blocks.
	for _, id := range []string{"L1", "L2", "L3"} {
		nodes[id].SetViewTimeout(150 * time.Millisecond)
	}
	defer func() {
		for _, id := range []string{"L1", "L2", "L3"} {
			nodes[id].Stop()
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for !nodes["L1"].IsLeader() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !nodes["L1"].IsLeader() {
		t.Fatal("the suspicion timers should have elected L1")
	}

	proposeLoop(t, nodes["L1"], []string{"b"}, 3)
	waitAllApplied(t, nw, ids, []string{"a", "b"})
}

// TestRejoinAfterPartition (catch-up / fetch): a replica partitioned away
// misses several committed commands. When the partition heals, the leader's
// next proposal references ancestors it does not have; the replica fetches
// them, commits the missed commands and converges with everyone else — no
// view change needed, because the leader never changed.
func TestRejoinAfterPartition(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// Partition L3 away while L0 commits "b" (the other 2f+1 stay in quorum).
	nw.dropPeer("L3")
	proposeLoop(t, nodes["L0"], []string{"b"}, 3)
	waitAllApplied(t, nw, []string{"L0", "L1", "L2"}, []string{"a", "b"})
	if got := nw.logs["L3"].snapshot(); fmt.Sprint(got) != "[a]" {
		t.Fatalf("partitioned L3 must not have seen b, got %v", got)
	}

	// Heal the partition; L3 must catch up via fetches once L0 proposes more.
	nw.unDropPeer("L3")
	proposeLoop(t, nodes["L0"], []string{"c"}, 3)
	waitAllApplied(t, nw, ids, []string{"a", "b", "c"})
}
