package hotstuff

import (
	"testing"
)

// Crash-recovery suite: what a restarted replica must (and must not) carry
// over. HS-M4 persists blocks, QC raises, the executed watermark and the vote
// height; recovery rebuilds the tree and derived pointers WITHOUT re-executing
// anything, and must never hand back a state weaker than the one the replica
// held when it crashed (a regressed vote height or lock would be a safety
// hole, not just a liveness hiccup).

// signedVC builds a ViewChange attributed to id, signed with the deterministic
// test key of that member.
func signedVC(id string, view uint64, high *QC) *ViewChange {
	vc := &ViewChange{View: view, HighQC: high, From: id}
	_, priv := testKeyOf(id)
	vc.Sig = signPayload(priv, vc)
	return vc
}

// TestWALQCRecoveryRestoresBase: a follower that folded QCs up to height 2
// before crashing must come back with the SAME qcHigh/head/lock — recovery
// replays the persisted QC raises through the chain rules, so a restarted
// replica never re-derives a weaker (or different) base than it had.
func TestWALQCRecoveryRestoresBase(t *testing.T) {
	dir := t.TempDir()

	r, w, _ := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	wirePhantom(r, testLeaderID, testPeer1, testPeer2)
	ids := mainChain(t, r, []string{"a", "b", "c"}) // B1..B3; F learns QC(B1), QC(B2)
	r.mu.Lock()
	before := struct {
		qcNode string
		qcH    uint64
		lock   string
		exec   string
		voted  uint64
	}{r.qcHigh.NodeID, r.qcHigh.Height, r.bLock, r.bExec, r.vHeight}
	r.mu.Unlock()
	if before.qcNode != ids[2] || before.qcH != 2 {
		t.Fatalf("test setup: qcHigh = %s@%d, want B2@2", before.qcNode, before.qcH)
	}
	w.Close()

	// Life 2: same WAL, fresh replica.
	r2, w2, _ := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	defer w2.Close()
	wirePhantom(r2, testLeaderID, testPeer1, testPeer2)
	r2.mu.Lock()
	after := struct {
		qcNode string
		qcH    uint64
		head   string
		lock   string
		exec   string
		voted  uint64
	}{r2.qcHigh.NodeID, r2.qcHigh.Height, r2.head, r2.bLock, r2.bExec, r2.vHeight}
	r2.mu.Unlock()

	if after.qcNode != before.qcNode || after.qcH != before.qcH {
		t.Fatalf("recovered qcHigh = %s@%d, want %s@%d", after.qcNode, after.qcH, before.qcNode, before.qcH)
	}
	// The recovered head is the highest QC's block — an accepted-but-uncertified
	// tail (B3 here) stays in the tree but is no longer the active head, exactly
	// as after a view change. The replica resumes from what is provably
	// certified, never from a block no quorum has endorsed.
	if after.head != after.qcNode {
		t.Fatalf("recovered head = %q, want the highest QC's block %q", after.head, after.qcNode)
	}
	if after.head != ids[2] {
		t.Fatalf("recovered head = %q, want B2", after.head)
	}
	if after.lock != before.lock {
		t.Fatalf("recovered lock = %q, want %q", after.lock, before.lock)
	}
	if after.exec != before.exec {
		t.Fatalf("recovered exec = %q, want %q", after.exec, before.exec)
	}
	if after.voted != before.voted {
		t.Fatalf("recovered vote height = %d, want %d", after.voted, before.voted)
	}

	// And the recovered state is still live: a higher block is accepted and
	// voted for, and the base lets the chain keep committing.
	next := prop(4, ids[3], "d", quorumQC(ids[3], 3))
	r2.HandleProposal(next)
	r2.mu.Lock()
	voted := r2.vHeight
	r2.mu.Unlock()
	if voted != 4 {
		t.Fatalf("recovered replica must vote above its restored height, vHeight=%d", voted)
	}
}

// TestWALRestartRejoinsAtHigherView: a replica that was in view 0 when it
// crashed comes back in view 0 (views are not persisted — they are a liveness
// hint, and the VC certificate carried by a proposal is what proves a view
// change). It must then advance to that view and vote ABOVE its recovered
// height, never below it.
func TestWALRestartRejoinsAtHigherView(t *testing.T) {
	dir := t.TempDir()

	r, w, _ := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	wirePhantom(r, testLeaderID, testPeer1, testPeer2)
	ids := mainChain(t, r, []string{"a", "b", "c"}) // F voted at heights 1..3
	w.Close()

	r2, w2, tr2 := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	defer w2.Close()
	wirePhantom(r2, testLeaderID, testPeer1, testPeer2)
	if got := r2.View(); got != 0 {
		t.Fatalf("a restarted replica starts in view 0 (views are not durable), got %d", got)
	}

	// leaderOf(1) for this member set is P1; a proposal from it carries the
	// 2f+1 view changes that justify entering view 1.
	leader1 := r2.leaderOf(1)
	vcs := []ViewChange{
		*signedVC(testLeaderID, 1, quorumQC(ids[2], 2)),
		*signedVC(testPeer1, 1, quorumQC(ids[2], 2)),
		*signedVC(testPeer2, 1, quorumQC(ids[2], 2)),
	}
	blk := Block{View: 1, Height: 4, Parent: ids[3], Cmd: []byte("d"), Justify: quorumQC(ids[3], 3)}
	blk.ID = blockID(blk.View, blk.Height, blk.Parent, blk.Cmd)
	p := &Proposal{Block: blk, From: leader1, ViewChanges: vcs}
	_, priv := testKeyOf(leader1)
	p.Sig = signPayload(priv, p)

	r2.HandleProposal(p)
	if got := r2.View(); got != 1 {
		t.Fatalf("restarted replica must join view 1 on a certificate-backed proposal, got view %d", got)
	}
	if got := tr2.voteCount(blk.ID); got != 1 {
		t.Fatalf("restarted replica must vote for a height-4 block, got %d votes", got)
	}
	r2.mu.Lock()
	voted := r2.vHeight
	r2.mu.Unlock()
	if voted != 4 {
		t.Fatalf("vote height after rejoining = %d, want 4", voted)
	}

	// A future-view proposal WITHOUT the view-change certificate is still
	// refused — a restart must not weaken the pacemaker's gate.
	fresh := Block{View: 2, Height: 5, Parent: blk.ID, Cmd: []byte("e"), Justify: quorumQC(blk.ID, 4)}
	fresh.ID = blockID(fresh.View, fresh.Height, fresh.Parent, fresh.Cmd)
	leader2 := r2.leaderOf(2)
	p2 := &Proposal{Block: fresh, From: leader2}
	_, priv2 := testKeyOf(leader2)
	p2.Sig = signPayload(priv2, p2)
	r2.HandleProposal(p2)
	if got := r2.View(); got != 1 {
		t.Fatalf("a proposal without a VC certificate moved the view to %d", got)
	}
	if got := tr2.voteCount(fresh.ID); got != 0 {
		t.Fatalf("an uncertified future-view proposal earned %d votes", got)
	}
}

// TestWALHeldVoteBlocksRestartedDoubleVote: the strongest crash form of the
// vote-once rule. A follower votes for a block, the process dies BEFORE the
// vote leaves the machine, and after restart the same block is offered again.
// The durable vote height must prevent a second, conflicting signature at that
// height even though the first was never seen by anyone.
func TestWALHeldVoteBlocksRestartedDoubleVote(t *testing.T) {
	dir := t.TempDir()

	r, w, _ := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	wirePhantom(r, testLeaderID, testPeer1, testPeer2)
	ids := mainChain(t, r, []string{"a"}) // votes at height 1
	w.Close()                             // the vote is durable; assume it never arrived

	r2, w2, tr2 := openWALReplica(t, dir, "F", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	defer w2.Close()
	wirePhantom(r2, testLeaderID, testPeer1, testPeer2)

	// The leader re-proposes the same height with DIFFERENT content (an
	// equivocation attempt) and also re-delivers the original.
	conflict := prop(1, genesisID, "A-CONFLICT", quorumQC(genesisID, 0))
	r2.HandleProposal(conflict)
	original := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	r2.HandleProposal(original)

	if got := tr2.voteCount(conflict.Block.ID); got != 0 {
		t.Fatalf("restarted replica re-voted at an already-used height (%d votes)", got)
	}
	if got := tr2.voteCount(original.Block.ID); got != 0 {
		t.Fatalf("restarted replica duplicated its held vote (%d votes)", got)
	}
	r2.mu.Lock()
	voted := r2.vHeight
	r2.mu.Unlock()
	if voted != 1 {
		t.Fatalf("vote height = %d, want the durable 1", voted)
	}
	_ = ids
}
