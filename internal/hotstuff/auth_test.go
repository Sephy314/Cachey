package hotstuff

import "testing"

// HS-M3 authentication tests: every protocol message carries a verifiable
// Ed25519 signature, so a receiver can tell who really sent it and whether it
// was tampered with. The threat model mirrors PBFT's M3 — signatures
// authenticate, they do not discipline: a Byzantine member with a valid key
// can still equivocate, and the protocol's quorum rules are what stop a fork.

// TestForgedSenderRejected: a message claiming to be from the leader but
// signed with a different member's key is dropped — the sender identity and
// the signing key must agree.
func TestForgedSenderRejected(t *testing.T) {
	f, tr := newFollower()
	// Craft a valid-shaped proposal from L0, then re-sign it with P1's key:
	// the claim (From=L0) and the signer (P1) disagree.
	p := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	_, p1priv := testKeyOf(testPeer1)
	p.Sig = signPayload(p1priv, p)
	f.HandleProposal(p)

	f.mu.Lock()
	blocks := len(f.blocks)
	f.mu.Unlock()
	if blocks != 1 {
		t.Fatalf("forged-sender proposal entered the tree, blocks=%d", blocks)
	}
	if got := tr.voteCount(p.Block.ID); got != 0 {
		t.Fatalf("forged-sender proposal earned %d vote(s)", got)
	}
}

// TestTamperedMessageRejected: a proposal that was validly signed and then
// altered (the command changed) no longer verifies and is dropped.
func TestTamperedMessageRejected(t *testing.T) {
	f, tr := newFollower()
	p := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	p.Block.Cmd = []byte("TAMPERED") // mutate after signing
	f.HandleProposal(p)

	f.mu.Lock()
	blocks := len(f.blocks)
	f.mu.Unlock()
	if blocks != 1 {
		t.Fatalf("tampered proposal entered the tree, blocks=%d", blocks)
	}
	if got := tr.voteCount(p.Block.ID); got != 0 {
		t.Fatalf("tampered proposal earned %d vote(s)", got)
	}
}

// TestByzantineLeaderEquivocatesAuthentically pins the M3 threat model: two
// conflicting proposals at the same height are EACH valid when signed by the
// (Byzantine) leader — a correct follower cannot tell which branch is "real",
// it only checks the signature, so it accepts and votes for the one it saw.
// Nothing here is a bug: vote-once-per-height plus the 2f+1 quorum rule is
// what keeps the two branches from both committing (pinned elsewhere).
func TestByzantineLeaderEquivocatesAuthentically(t *testing.T) {
	f1, tr1 := newFollower()
	f2, tr2 := newFollower()

	pA := prop(1, genesisID, "A", quorumQC(genesisID, 0))
	pB := prop(1, genesisID, "B", quorumQC(genesisID, 0))
	f1.HandleProposal(pA)
	f2.HandleProposal(pB)

	for name, f := range map[string]struct {
		n  *Replica
		tr *recorderTransport
		p  *Proposal
	}{
		"f1": {f1, tr1, pA},
		"f2": {f2, tr2, pB},
	} {
		f.n.mu.Lock()
		blocks := len(f.n.blocks)
		f.n.mu.Unlock()
		if blocks != 2 {
			t.Fatalf("%s must accept its authentic proposal, blocks=%d", name, blocks)
		}
		if got := f.tr.voteCount(f.p.Block.ID); got != 1 {
			t.Fatalf("%s must vote for its authentic proposal, got %d", name, got)
		}
	}
}

// TestViewChangeWithFabricatedHighQCRejected: a Byzantine member can sign an
// AUTHENTIC view change but stuff it with a fabricated high QC (a QC that
// would claim a height no real quorum certified). The new leader filters it
// via qcValid and adopts the genuine highest QC instead — a Byzantine member
// can never force a view onto an unsafe base by lying about its QC.
func TestViewChangeWithFabricatedHighQCRejected(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	// Progress view 0 once so the leader L0 holds the genuine QC(B1) at
	// height 1 (the sync net folds B1's votes during the broadcast).
	b1, err := nodes["L0"].Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}

	// Move everyone who will form the view-1 quorum to view 1 (leader P1).
	nodes["P1"].StartViewChange() // P1 counts its own vc (carries the genesis QC)
	nodes["L0"].StartViewChange() // honest vc, carries QC(B1) height 1

	// Byzantine P3 sends an authentic vc whose HighQC is fabricated: it claims
	// to certify B1 at height 1000, but only P3's own vote is genuine and the
	// rest are garbage, so qcValid must reject it.
	fake := newQC(b1, 1000)
	fake.Votes["P1"] = []byte("garbage-sig")
	fake.Votes["P2"] = []byte("garbage-sig")
	fake.Votes["P3"] = nodes["P3"].sign(&Vote{Height: 1000, NodeID: b1, Voter: "P3"})
	byz := &ViewChange{View: 1, HighQC: fake, From: "P3"}
	byz.Sig = nodes["P3"].sign(byz)
	nodes["P1"].HandleViewChange(byz) // 3rd vc -> quorum, activates P1

	// P1 must be active with the GENUINE highest QC (B1 at height 1), never the
	// fabricated height-1000 one.
	nodes["P1"].mu.Lock()
	active := nodes["P1"].active
	qc := nodes["P1"].qcHigh
	head := nodes["P1"].head
	nodes["P1"].mu.Unlock()
	if !active {
		t.Fatal("P1 must be the active leader of view 1 after a quorum of vcs")
	}
	if qc == nil || qc.NodeID != b1 || qc.Height != 1 {
		t.Fatalf("P1 must adopt the genuine QC(B1) at height 1, got node=%q height=%d", qc.NodeID, qc.Height)
	}
	if head != b1 {
		t.Fatalf("P1's head must be B1, got %q", head)
	}

	// And the fabricated height-1000 QC must not have polluted qcHigh.
	if qc.Height == 1000 {
		t.Fatal("fabricated high QC was adopted")
	}

	// Sanity: P1 can now propose chained onto B1 (freshBase skips a height).
	if _, err := nodes["P1"].Propose([]byte("b")); err != nil {
		t.Fatalf("new leader must be able to propose, got %v", err)
	}
}
