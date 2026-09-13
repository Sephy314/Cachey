package hotstuff

import (
	"crypto/ed25519"
	"fmt"
	"testing"
)

// Phase 2.1/2.2 tests: the membership model (epochs, historical validator
// sets, epoch-bound QCs) and the membership transition (a committed
// membership-change block activates the next epoch's set). The safety
// invariants pinned here:
//
//	QC validation uses ValidatorSet(QC.Epoch), never the current membership.
//	Historical QCs remain verifiable after membership changes.
//	A new validator cannot vote before its epoch activates.
//	A removed validator cannot vote after its epoch ends.
//	A membership change activates only through a committed transition.
//	Restart/recovery reconstructs the epoch history.

// TestValidatorSetApply pins the set derivation: adds carry their public keys,
// removes drop members, the quorum is 2f+1 with f=floor((N-1)/3), and
// malformed changes (adding an existing member, removing a non-member, a bad
// key, an empty result) never produce a set.
func TestValidatorSetApply(t *testing.T) {
	epub := testPub("E")

	base := newValidatorSet(0, []string{"A", "B", "C", "D"}, map[string]ed25519.PublicKey{
		"A": testPub("A"), "B": testPub("B"), "C": testPub("C"), "D": testPub("D"),
	})
	if base.F != 1 || base.Quorum != 3 {
		t.Fatalf("genesis set f=%d quorum=%d, want f=1 quorum=3", base.F, base.Quorum)
	}

	// Add E: epoch 1, sorted, E's key present, quorum still 3 (N=5, f=1).
	s1 := base.apply(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if s1 == nil || s1.Epoch != 1 || !s1.has("E") || s1.has("X") {
		t.Fatalf("add-E derivation wrong: %+v", s1)
	}
	if got := fmt.Sprint(s1.Validators); got != "[A B C D E]" {
		t.Fatalf("sorted validators = %s, want [A B C D E]", got)
	}
	if s1.Quorum != 3 || s1.F != 1 {
		t.Fatalf("N=5 set f=%d quorum=%d, want f=1 quorum=3", s1.F, s1.Quorum)
	}
	if got := s1.PublicKeys["E"]; len(got) == 0 {
		t.Fatal("E's public key missing from the derived set")
	}

	// Remove D from the 5-set: epoch 2, N=4, quorum 3.
	s2 := s1.apply(&MembershipChange{Remove: []string{"D"}})
	if s2 == nil || s2.Epoch != 2 || s2.has("D") || !s2.has("E") {
		t.Fatalf("remove-D derivation wrong: %+v", s2)
	}
	if s2.Quorum != 3 || s2.F != 1 {
		t.Fatalf("N=4 set f=%d quorum=%d, want f=1 quorum=3", s2.F, s2.Quorum)
	}

	// Malformed changes never derive a set.
	if s := base.apply(&MembershipChange{Add: []ValidatorEntry{{ID: "A", Pub: epub}}}); s != nil {
		t.Fatal("adding an existing member must fail")
	}
	if s := base.apply(&MembershipChange{Remove: []string{"X"}}); s != nil {
		t.Fatal("removing a non-member must fail")
	}
	if s := base.apply(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: []byte("short")}}}); s != nil {
		t.Fatal("a bad public key must fail")
	}
	if s := base.apply(&MembershipChange{Remove: []string{"A", "B", "C", "D"}}); s != nil {
		t.Fatal("an empty result must fail")
	}
}

// TestEpochDerivation pins the chain-derived epoch: T (membership), B1, B2 are
// epoch 0 (voted by the old set); T commits only once B3's justification
// (QC(B2)) is folded; B3 is epoch 1. The claimed epoch must match the derived
// one — a block claiming the wrong epoch is rejected.
func TestEpochDerivation(t *testing.T) {
	f, _ := newFollower()
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// T: membership block at height 1, justified by genesis.
	tp := prop(1, genesisID, string(cmd), quorumQC(genesisID, 0))
	f.HandleProposal(tp)
	tid := tp.Block.ID
	if got := f.epochOfLocked(&tp.Block); got != 0 {
		t.Fatalf("epoch(T) = %d, want 0", got)
	}

	// B1, B2: epoch 0, each certifying its parent.
	b1p := prop(2, tid, "x", quorumQC(tid, 1))
	f.HandleProposal(b1p)
	b1id := b1p.Block.ID
	if got := f.epochOfLocked(&b1p.Block); got != 0 {
		t.Fatalf("epoch(B1) = %d, want 0", got)
	}
	b2p := prop(3, b1id, "y", quorumQC(b1id, 2))
	f.HandleProposal(b2p)
	b2id := b2p.Block.ID
	if got := f.epochOfLocked(&b2p.Block); got != 0 {
		t.Fatalf("epoch(B2) = %d, want 0", got)
	}

	// B3: the first new-epoch block. Its justification QC(B2) commits T when
	// folded, so its derived epoch is 1.
	b3p := propEpoch(1, 4, b2id, "z", quorumQC(b2id, 3))
	f.HandleProposal(b3p)
	if got := f.epochOfLocked(&b3p.Block); got != 1 {
		t.Fatalf("epoch(B3) = %d, want 1", got)
	}
	f.mu.Lock()
	epoch, hasE := f.epoch, f.sets[1].has("E")
	f.mu.Unlock()
	if epoch != 1 || !hasE {
		t.Fatalf("after B3: epoch=%d set(1).has(E)=%v, want epoch=1 hasE=true", epoch, hasE)
	}

	// A block claiming epoch 1 without a committed transition in its ancestry
	// must be rejected (the epoch is chain-derived, not proposer-chosen). Its
	// parent is B1, so its ancestry (B1, T) has no committed transition — only
	// two blocks above T, not the three the 3-chain commit needs.
	bad := propEpoch(1, 5, b1id, "w", quorumQC(b1id, 2))
	f.HandleProposal(bad)
	f.mu.Lock()
	_, known := f.blocks[bad.Block.ID]
	f.mu.Unlock()
	if known {
		t.Fatal("a block claiming epoch 1 without a committed transition was accepted")
	}
}

// TestMembershipTransitionSingleNode: a single-node cluster (f=0) commits a
// membership change (add E); the new set activates; the old QC remains
// verifiable; E's vote counts in the new epoch; a wrong-epoch QC is rejected.
func TestMembershipTransitionSingleNode(t *testing.T) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: "L0", Leader: "L0"}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// T, B1, B2 (all epoch 0): the 3-chain that commits T.
	tid, err := n.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil { // B1
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil { // B2 -> QC(B2) commits T
		t.Fatal(err)
	}
	n.mu.Lock()
	epoch := n.epoch
	set1 := n.sets[1]
	n.mu.Unlock()
	if epoch != 1 || set1 == nil || !set1.has("E") {
		t.Fatalf("transition did not activate: epoch=%d set1=%v", epoch, set1)
	}

	// The old QC (epoch 0, over T) remains verifiable with set(0).
	n.mu.Lock()
	oldQC := n.blocks[tid].Justify
	n.mu.Unlock()
	if !n.qcValid(oldQC) {
		t.Fatal("historical epoch-0 QC no longer verifies after the transition")
	}

	// B3 is the first epoch-1 block; E's vote for it counts.
	b3id, err := n.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	b3 := n.blocks[b3id]
	n.mu.Unlock()
	if b3.Epoch != 1 {
		t.Fatalf("B3 epoch = %d, want 1", b3.Epoch)
	}
	n.HandleVote(signVote("E", b3.Height, b3id))
	n.mu.Lock()
	votes := len(n.votes[b3id])
	n.mu.Unlock()
	if votes != 2 { // L0's self-vote + E's
		t.Fatalf("E's vote did not count: %d votes, want 2", votes)
	}

	// A QC claiming the wrong epoch for a known block is rejected.
	wrong := newQCEpoch(1, tid, 1)
	wrong.Votes["L0"] = signVote("L0", 1, tid).Sig
	if n.qcValid(wrong) {
		t.Fatal("a QC with epoch 1 over an epoch-0 block was accepted")
	}
}

// TestNewValidatorCannotVoteEarly: a validator added by a transition cannot
// vote for blocks of the epoch before its activation.
func TestNewValidatorCannotVoteEarly(t *testing.T) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: "L0", Peers: []string{"P1", "P2", "P3"}, Leader: "L0"}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(n, "P1", "P2", "P3")
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// T (epoch 0) with a quorum of old-set votes.
	tid, err := n.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	n.HandleVote(signVote("P1", 1, tid))
	n.HandleVote(signVote("P2", 1, tid))

	// E is not a member of set(0): its vote for T must be ignored.
	n.HandleVote(signVote("E", 1, tid))
	n.mu.Lock()
	votes := len(n.votes[tid])
	n.mu.Unlock()
	if votes != 3 { // L0 + P1 + P2
		t.Fatalf("E's pre-activation vote entered the set: %d votes, want 3", votes)
	}
}

// TestRemovedValidatorCannotVote: a validator removed by a transition cannot
// vote for blocks of the new epoch.
func TestRemovedValidatorCannotVote(t *testing.T) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: "L0", Peers: []string{"P1", "P2", "P3"}, Leader: "L0"}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(n, "P1", "P2", "P3")
	cmd, err := membershipCmd(&MembershipChange{Remove: []string{"P3"}})
	if err != nil {
		t.Fatal(err)
	}

	// Commit the transition: T, B1, B2 with quorum votes from L0, P1, P2.
	tid, err := n.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	n.HandleVote(signVote("P1", 1, tid))
	n.HandleVote(signVote("P2", 1, tid))
	b1id, err := n.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	n.HandleVote(signVote("P1", 2, b1id))
	n.HandleVote(signVote("P2", 2, b1id))
	b2id, err := n.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	n.HandleVote(signVote("P1", 3, b2id))
	n.HandleVote(signVote("P2", 3, b2id)) // QC(B2) commits T -> epoch 1

	n.mu.Lock()
	epoch := n.epoch
	set1 := n.sets[1]
	n.mu.Unlock()
	if epoch != 1 || set1 == nil || set1.has("P3") {
		t.Fatalf("removal did not activate: epoch=%d set1=%v", epoch, set1)
	}

	// B3 (epoch 1): P3's vote must be rejected (removed from set(1)).
	b3id, err := n.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	n.HandleVote(signVote("P3", 4, b3id))
	n.mu.Lock()
	votes := len(n.votes[b3id])
	n.mu.Unlock()
	if votes != 1 { // L0's self-vote only
		t.Fatalf("removed validator's vote entered the set: %d votes, want 1", votes)
	}
}

// TestMembershipRecovery: a WAL-backed node commits a membership change,
// restarts, and reconstructs the epoch history — the new set is active and a
// historical epoch-0 QC still verifies.
func TestMembershipRecovery(t *testing.T) {
	dir := t.TempDir()
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// Life 1: commit the transition on a single node.
	r, w, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	tid, err := r.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Propose(nil); err != nil { // B1
		t.Fatal(err)
	}
	if _, err := r.Propose(nil); err != nil { // B2 -> commits T
		t.Fatal(err)
	}
	r.mu.Lock()
	epoch := r.epoch
	r.mu.Unlock()
	if epoch != 1 {
		t.Fatalf("before crash epoch=%d, want 1", epoch)
	}
	w.Close()

	// Life 2: recovery reconstructs set(1) from the replayed blocks.
	r2, w2, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	defer w2.Close()
	r2.mu.Lock()
	epoch2 := r2.epoch
	set1 := r2.sets[1]
	oldQC := r2.blocks[tid].Justify
	r2.mu.Unlock()
	if epoch2 != 1 || set1 == nil || !set1.has("E") {
		t.Fatalf("recovery did not reconstruct epoch 1: epoch=%d set1=%v", epoch2, set1)
	}
	if !r2.qcValid(oldQC) {
		t.Fatal("historical epoch-0 QC does not verify after recovery")
	}

	// The recovered node keeps committing in epoch 1.
	b3id, err := r2.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	r2.mu.Lock()
	b3 := r2.blocks[b3id]
	r2.mu.Unlock()
	if b3.Epoch != 1 {
		t.Fatalf("post-recovery block epoch = %d, want 1", b3.Epoch)
	}
}

// TestMembershipChangeRequiresConsensus: a membership command that never
// reaches a quorum must not activate the new set.
func TestMembershipChangeRequiresConsensus(t *testing.T) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: "L0", Peers: []string{"P1", "P2", "P3"}, Leader: "L0"}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(n, "P1", "P2", "P3")
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// Only the leader's self-vote (1 of 3): no quorum, no transition.
	if _, err := n.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	epoch := n.epoch
	_, hasSet1 := n.sets[1]
	n.mu.Unlock()
	if epoch != 0 || hasSet1 {
		t.Fatalf("a minority membership command activated: epoch=%d set1=%v", epoch, hasSet1)
	}
}

// TestMembershipTransitionCluster: a 4-node cluster commits a membership
// change through normal consensus — every node activates epoch 1 with the new
// validator in its set, and the cluster keeps making progress in the new
// epoch (B3 earns a quorum of new-epoch votes).
func TestMembershipTransitionCluster(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	lead := nodes["L0"]
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}

	// T, B1, B2 (all epoch 0, voted by the old set) commit T; B3 is the first
	// epoch-1 block. The synchronous net delivers votes back to the leader
	// during each Propose, so the QCs form in order.
	if _, err := lead.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := lead.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lead.Propose(nil); err != nil {
		t.Fatal(err)
	}
	b3id, err := lead.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Every node activated epoch 1 with E in the set (followers commit T when
	// they fold B3's justification QC(B2)).
	for _, id := range ids {
		nodes[id].mu.Lock()
		epoch := nodes[id].epoch
		hasE := nodes[id].sets[1] != nil && nodes[id].sets[1].has("E")
		nodes[id].mu.Unlock()
		if epoch != 1 || !hasE {
			t.Fatalf("%s: epoch=%d hasE=%v, want epoch=1 hasE=true", id, epoch, hasE)
		}
	}

	// B3 (epoch 1) earned a quorum of new-epoch votes.
	lead.mu.Lock()
	votes := len(lead.votes[b3id])
	lead.mu.Unlock()
	if votes < 3 {
		t.Fatalf("B3 earned %d votes, want a quorum (3)", votes)
	}
}

// TestSequentialTransitions: two membership changes commit in sequence,
// activating epoch 1 then epoch 2; the second transition's set builds on the
// first (both new validators present).
func TestSequentialTransitions(t *testing.T) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: "L0", Leader: "L0"}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	epub := testPub("E")
	fpub := testPub("F")
	cmdE, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}
	cmdF, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "F", Pub: fpub}}})
	if err != nil {
		t.Fatal(err)
	}

	// Transition 1: T1, B1, B2 -> commit -> epoch 1.
	if _, err := n.Propose(cmdE); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil {
		t.Fatal(err)
	}
	// Transition 2: T2 (epoch 1), B3, B4 -> commit -> epoch 2.
	if _, err := n.Propose(cmdF); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Propose(nil); err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	epoch := n.epoch
	set2 := n.sets[2]
	n.mu.Unlock()
	if epoch != 2 || set2 == nil || !set2.has("E") || !set2.has("F") {
		t.Fatalf("sequential transitions: epoch=%d set2=%v", epoch, set2)
	}
	if set2.Quorum != 1 {
		t.Fatalf("N=3 set quorum=%d, want 1 (f=0)", set2.Quorum)
	}
}

// ---- helpers ----

// testPub returns the deterministic test public key of id.
func testPub(id string) ed25519.PublicKey {
	pub, _ := testKeyOf(id)
	return pub
}

// propEpoch is prop with an explicit block epoch.
func propEpoch(epoch, h uint64, parent, cmd string, j *QC) *Proposal {
	blk := Block{View: 0, Height: h, Parent: parent, Cmd: []byte(cmd), Justify: j, Epoch: epoch}
	blk.ID = blockID(0, h, parent, blk.Cmd)
	p := &Proposal{Block: blk, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	return p
}
