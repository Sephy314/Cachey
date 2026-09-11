package hotstuff

import (
	"testing"
)

// Vote and QC validation regression suite (n = 4, f = 1, quorum = 3). These
// tests attack the validation itself — a QC must not be "2f+1 map entries" but
// 2f+1 DISTINCT members with genuine signatures over the exact (height, block)
// tuple, and a vote must be a live, well-formed, correctly-signed statement
// about a real block. Everything is verified against the actual engine
// predicates (qcValid / voteForBlockLocked), not a mock.

// TestQCRejectsDuplicateVoters: one member's vote cannot be counted 2f+1 times.
// Padding a QC with copies of a single genuine signature — mapped under other
// members' ids — must fail, because each entry is verified against the vote
// tuple that names ITS voter.
func TestQCRejectsDuplicateVoters(t *testing.T) {
	f, _ := newFollower()
	f.mu.Lock()
	defer f.mu.Unlock()

	// Control: a genuine 2f+1 QC verifies.
	if !f.qcValid(quorumQC("blk", 5)) {
		t.Fatal("test setup: a genuine quorum QC must verify")
	}
	// The attack: P1's real signature, replicated under every voter id.
	dup := newQC("blk", 5)
	sig := signVote(testPeer1, 5, "blk").Sig
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		dup.Votes[v] = sig
	}
	if f.qcValid(dup) {
		t.Fatal("a QC padded with copies of one member's vote was accepted")
	}
	// Even 2f+1 entries where only P1 is genuine is sub-quorum in reality.
	half := newQC("blk", 5)
	half.Votes[testPeer1] = sig
	half.Votes[testLeaderID] = sig
	if f.qcValid(half) {
		t.Fatal("a QC below quorum was accepted")
	}
}

// TestQCRejectsUnknownValidator: entries from ids outside the validator set
// never contribute, however many of them there are or however well formed
// their signatures look.
func TestQCRejectsUnknownValidator(t *testing.T) {
	f, _ := newFollower()
	outside := []string{"mallory", "eve", "trudy"} // > quorum, all non-members
	wirePhantom(f, outside...)                     // even with pinned keys, they are not validators

	f.mu.Lock()
	defer f.mu.Unlock()
	q := newQC("blk", 5)
	for _, v := range outside {
		q.Votes[v] = signVote(v, 5, "blk").Sig
	}
	if f.qcValid(q) {
		t.Fatal("a QC made entirely of non-validators was accepted")
	}
	// A single non-validator smuggled into an otherwise valid quorum fails too.
	mixed := quorumQC("blk", 5)
	mixed.Votes["mallory"] = signVote("mallory", 5, "blk").Sig
	if f.qcValid(mixed) {
		t.Fatal("a QC containing a non-validator entry was accepted")
	}
}

// TestQCRejectsInvalidSignature: tampered or truncated signatures fail, and a
// member without a pinned key cannot be counted even with a plausible-looking
// signature.
func TestQCRejectsInvalidSignature(t *testing.T) {
	f, _ := newFollower()
	f.mu.Lock()
	defer f.mu.Unlock()

	for name, corrupt := range map[string]func(q *QC){
		"garbage":   func(q *QC) { q.Votes[testPeer1] = []byte("garbage-signature") },
		"truncated": func(q *QC) { q.Votes[testPeer1] = q.Votes[testPeer1][:8] },
		"swapped":   func(q *QC) { q.Votes[testPeer1] = q.Votes[testPeer2] },
		"empty":     func(q *QC) { q.Votes[testPeer1] = nil },
	} {
		q := quorumQC("blk", 5)
		corrupt(q)
		if f.qcValid(q) {
			t.Fatalf("%s: a QC with a corrupt signature was accepted", name)
		}
	}
	// An otherwise-complete QC whose entries are signed by the WRONG members'
	// keys (a forged quorum) fails.
	forged := newQC("blk", 5)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		other := testPeer1
		if v == testPeer1 {
			other = testPeer2
		}
		forged.Votes[v] = signVote(other, 5, "blk").Sig
	}
	if f.qcValid(forged) {
		t.Fatal("a QC whose signatures were produced by other members was accepted")
	}
}

// TestQCRejectsWrongBlock: the QC's certified block and height must be exactly
// what it claims. A QC certifying a different block (a fork or a sibling) is
// not a justification for this parent, and a QC whose height disagrees with the
// block it names is rejected outright.
func TestQCRejectsWrongBlock(t *testing.T) {
	f, tr := newFollower()
	b1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	f.HandleProposal(b1)

	// A real quorum QC — but over a DIFFERENT block than the proposal's parent.
	// (Same height, same view, different content: an equivocating leader's
	// sibling block.) Structurally this is a valid QC, just not for this parent.
	sibling := prop(1, genesisID, "a-DIFFERENT", quorumQC(genesisID, 0))
	q := newQC(sibling.Block.ID, 1)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		q.Votes[v] = signVote(v, 1, sibling.Block.ID).Sig
	}
	bad := prop(2, b1.Block.ID, "b", q) // justifies B1's parent chain with a QC for the sibling
	f.HandleProposal(bad)

	f.mu.Lock()
	_, inTree := f.blocks[bad.Block.ID]
	f.mu.Unlock()
	if inTree {
		t.Fatal("a proposal justified by a QC for another block was accepted")
	}
	if got := tr.voteCount(bad.Block.ID); got != 0 {
		t.Fatalf("a proposal with a misdirected QC earned %d vote(s)", got)
	}

	// A QC that names the right block but claims the wrong height: even with
	// 2f+1 genuine signatures over the wrong tuple (a Byzantine quorum cannot
	// outvote the height rule), it must not verify.
	f.mu.Lock()
	defer f.mu.Unlock()
	mislabelled := newQC(b1.Block.ID, 9)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		mislabelled.Votes[v] = signVote(v, 9, b1.Block.ID).Sig
	}
	if f.qcValid(mislabelled) {
		t.Fatal("a QC whose height disagrees with the block it certifies was accepted")
	}
}

// TestQCRejectsWrongView: a QC binds one block in one view. A proposal whose
// justification points at a block from a different branch of the tree cannot
// be used to carry a view forward, and a stale-view proposal is not resurrected
// by the validity of the QC it carries.
func TestQCRejectsWrongView(t *testing.T) {
	f, tr := newFollower()

	// View-0 chain: B1.
	b1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	f.HandleProposal(b1)

	// A fork block in the SAME view/height as B1 (Byzantine equivocation), with
	// a genuine QC over it. It must not justify a block whose parent is B1.
	fork := prop(1, genesisID, "fork", quorumQC(genesisID, 0))
	fq := newQC(fork.Block.ID, 1)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		fq.Votes[v] = signVote(v, 1, fork.Block.ID).Sig
	}
	crossView := prop(2, b1.Block.ID, "b", fq)
	f.HandleProposal(crossView)

	f.mu.Lock()
	_, inTree := f.blocks[crossView.Block.ID]
	f.mu.Unlock()
	if inTree {
		t.Fatal("a QC from a conflicting branch justified a block")
	}
	if got := tr.voteCount(crossView.Block.ID); got != 0 {
		t.Fatalf("a proposal justified by a conflicting-branch QC earned %d vote(s)", got)
	}

	// Move F to view 1, then deliver a NEW view-0 proposal: its QC is perfectly
	// valid, but the block belongs to a past view and a deposed leader may not
	// advance the chain.
	f.mu.Lock()
	f.enterViewLocked(1)
	f.mu.Unlock()
	stale := prop(2, b1.Block.ID, "stale", quorumQC(b1.Block.ID, 1))
	f.HandleProposal(stale)
	f.mu.Lock()
	_, staleAccepted := f.blocks[stale.Block.ID]
	f.mu.Unlock()
	if staleAccepted {
		t.Fatal("a stale-view proposal was accepted despite its valid QC")
	}
	if got := tr.voteCount(stale.Block.ID); got != 0 {
		t.Fatalf("a stale-view proposal earned %d vote(s)", got)
	}
}

// TestQCRejectsMissingValidatorKey: the validator registry is the ONLY source
// of verification keys. A member whose key was never configured cannot be
// counted, even with a well-formed signature — and configuring the keys makes
// the very same QC valid, proving the rejection came from the registry.
func TestQCRejectsMissingValidatorKey(t *testing.T) {
	tr := &recorderTransport{}
	bare, err := NewReplica(Config{
		ID: testLeaderID, Peers: []string{"F", testPeer1, testPeer2}, Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A genuine quorum over the three OTHER members (their real keys are the
	// deterministic test keys this QC is signed with).
	q := newQC("blk", 5)
	for _, v := range []string{"F", testPeer1, testPeer2} {
		q.Votes[v] = signVote(v, 5, "blk").Sig
	}

	bare.mu.Lock()
	unconfigured := bare.qcValid(q)
	bare.mu.Unlock()
	if unconfigured {
		t.Fatal("a QC was accepted while its voters' public keys were unconfigured")
	}

	wirePhantom(bare, "F", testPeer1, testPeer2)
	bare.mu.Lock()
	defer bare.mu.Unlock()
	if !bare.qcValid(q) {
		t.Fatal("the same QC was still refused after its voters' keys were configured")
	}
}

// TestQCRequiresQuorum: a QC needs strictly 2f+1 distinct members. 2f (and
// fewer) is not enough, no matter how genuine the signatures are.
func TestQCRequiresQuorum(t *testing.T) {
	f, _ := newFollower()
	f.mu.Lock()
	defer f.mu.Unlock()

	// f = 1: quorum is 3, so 2 and 1 must both fail.
	for n, voters := range map[int][]string{
		1: {testLeaderID},
		2: {testLeaderID, testPeer1},
	} {
		q := newQC("blk", 5)
		for _, v := range voters {
			q.Votes[v] = signVote(v, 5, "blk").Sig
		}
		if f.qcValid(q) {
			t.Fatalf("a QC with %d votes was accepted (quorum is %d)", n, 2*f.f+1)
		}
	}
	// Genesis is the trusted root at exactly height 0; anything else named
	// genesis is not a QC.
	if f.qcValid(newQC(genesisID, 3)) {
		t.Fatal("a genesis-named QC above height 0 was accepted")
	}
	if !f.qcValid(newQC(genesisID, 0)) {
		t.Fatal("the genesis root QC must be trusted")
	}
}

// TestVoteValidationRejectsMalformed: the leader's vote intake accepts a vote
// only from a member, only for a known block, only at that block's real height,
// and only with a genuine signature. Every other shape is ignored.
func TestVoteValidationRejectsMalformed(t *testing.T) {
	// A leader with one phantom peer set so we can drive the vote path.
	tr := &recorderTransport{}
	lead, err := NewReplica(Config{
		ID: testLeaderID, Peers: []string{"F", testPeer1, testPeer2}, Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(lead, "F", testPeer1, testPeer2)
	b1id, err := lead.Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	_, otherPriv := testKeyOf(testPeer2)

	cases := map[string]*Vote{
		"nil":                nil,
		"unknown-voter":      {Height: 1, NodeID: b1id, Voter: "mallory"},
		"unknown-block":      signVote("F", 1, "no-such-block"),
		"wrong-height":       signVote("F", 2, b1id),
		"no-signature":       {Height: 1, NodeID: b1id, Voter: "F"},
		"tampered-height":    signVote("F", 1, b1id), // sig valid; height mutated below
		"tampered-block":     signVote("F", 1, b1id), // sig valid; block mutated below
		"wrong-key":          signVote(testPeer2, 1, b1id),
		"empty-voter":        signVote("", 1, b1id),
		"signature-too-long": signVote("F", 1, b1id),
	}
	cases["tampered-height"].Height = 7
	cases["tampered-block"].NodeID = "another-block"
	cases["signature-too-long"].Sig = append(cases["signature-too-long"].Sig, 0x00)
	cases["wrong-key"].Sig = signPayload(otherPriv, &Vote{Height: 1, NodeID: b1id, Voter: testPeer1})

	for name, v := range cases {
		lead.HandleVote(v)
		lead.mu.Lock()
		got := len(lead.votes[b1id])
		lead.mu.Unlock()
		if got != 1 { // only the leader's own self-vote
			t.Fatalf("%s: malformed vote was counted (votes=%d, want 1)", name, got)
		}
	}

	// Control: a genuine vote is counted, and a duplicate of it is not.
	lead.HandleVote(signVote("F", 1, b1id))
	lead.HandleVote(signVote("F", 1, b1id)) // duplicate delivery
	lead.mu.Lock()
	got := len(lead.votes[b1id])
	lead.mu.Unlock()
	if got != 2 {
		t.Fatalf("genuine vote + duplicate: votes=%d, want 2", got)
	}
}
