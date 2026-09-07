package hotstuff

import (
	"testing"
)

// Security-review regression tests (batch 1):
//
//   - P1: a single signed ViewChange must not jump a replica into an arbitrary
//     future view (view progression is quorum-gated).
//   - P1: a correctly-signed vote for the WRONG height must not poison a
//     block's vote set and wedge its QC forever.
//   - P2: a forged "genesis QC" claiming a non-zero height must not pass
//     qcValid (only the real genesis root at height 0 is trusted).

// TestSingleViewChangeCannotJumpView: a Byzantine member signs a ViewChange for
// a far-future view v whose leader this replica is. Without a quorum of 2f+1
// view changes, the replica must NOT advance to v — otherwise one message could
// strand it out of reach of the real (lower) views forever.
func TestSingleViewChangeCannotJumpView(t *testing.T) {
	f, _ := newFollower() // F is leaderOf(3 mod 4): views 3, 7, 11, ... (see all[viewIdx+v%4])
	if got := f.leaderOf(1003); got != "F" {
		t.Fatalf("test setup: F should be leader of view 1003, got %q", got)
	}

	vc := &ViewChange{View: 1003, HighQC: quorumQC(genesisID, 0), From: testPeer1}
	_, p1priv := testKeyOf(testPeer1)
	vc.Sig = signPayload(p1priv, vc)
	f.HandleViewChange(vc) // single (Byzantine-signed) view change, no quorum

	if got := f.View(); got != 0 {
		t.Fatalf("a single ViewChange moved F to view %d; view progression must be quorum-gated", got)
	}
	if f.IsLeader() {
		t.Fatal("a single ViewChange must not activate F as a future leader")
	}
	f.mu.Lock()
	vcs := len(f.vcs[1003])
	f.mu.Unlock()
	if vcs != 1 {
		t.Fatalf("the (minority) view change should be counted but not acted on, got %d", vcs)
	}
}

// TestWrongHeightVoteCannotBlockQC: a Byzantine member that signs a genuine
// vote for a block at the WRONG height must not get it into the vote set — the
// QC is built over the block's real height, so such a vote would fail the QC's
// signature check and (worse) mark the QC "formed", wedging the block.
func TestWrongHeightVoteCannotBlockQC(t *testing.T) {
	tr := &recorderTransport{}
	lead, err := NewReplica(Config{
		ID: testLeaderID, Peers: []string{"F", testPeer1, testPeer2}, Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(lead, "F", testPeer1, testPeer2)
	b1id, err := lead.Propose([]byte("a")) // B1 at height 1; self-vote = 1 of 3
	if err != nil {
		t.Fatal(err)
	}

	// A correct signature over the WRONG height (2 != B1's real height 1) must
	// be ignored entirely.
	lead.HandleVote(signVote("F", 2, b1id))
	lead.mu.Lock()
	got := len(lead.votes[b1id])
	lead.mu.Unlock()
	if got != 1 {
		t.Fatalf("wrong-height vote entered the vote set; votes=%d want 1 (self only)", got)
	}

	// Correct-height votes still form the QC and free the leader.
	lead.HandleVote(signVote("F", 1, b1id))
	lead.HandleVote(signVote(testPeer1, 1, b1id))
	if _, err := lead.Propose([]byte("b")); err != nil {
		t.Fatalf("genuine votes must form the QC and free the leader, got %v", err)
	}
}

// TestForgedGenesisQCHighHeightRejected: a malicious leader proposes the first
// block justified by a forged "genesis QC" that claims a huge height. qcValid
// must reject any genesis-named QC that is not the true root (height 0), or the
// node's height bookkeeping could be polluted by an absurd justification.
func TestForgedGenesisQCHighHeightRejected(t *testing.T) {
	f, tr := newFollower()
	// A QC over genesis with an inflated height: structurally "certifies" the
	// parent (genesis) but is not the real genesis root.
	p := prop(1, genesisID, "a", newQC(genesisID, 500))
	f.HandleProposal(p)

	f.mu.Lock()
	blocks := len(f.blocks)
	f.mu.Unlock()
	if blocks != 1 {
		t.Fatalf("forged genesis QC with height 500 was accepted, blocks=%d", blocks)
	}
	if got := tr.voteCount(p.Block.ID); got != 0 {
		t.Fatalf("forged genesis QC earned %d vote(s)", got)
	}
}
