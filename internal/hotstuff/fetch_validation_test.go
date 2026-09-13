package hotstuff

import "testing"

// Fetch-path validation (hardening). A BlockMsg carries a block whose Justify
// QC is NOT covered by the content-addressed block id, so the fetch response is
// the one inbound message that could inject unverified structure into the tree.
// Two gates close that: a block is only admitted if this replica actually asked
// for it, and only with the same structural evidence a proposal must carry.

// TestUnsolicitedBlockRejected: a member pushing a block nobody asked for must
// not touch the tree — otherwise any member could plant blocks (and move the
// head) at will.
func TestUnsolicitedBlockRejected(t *testing.T) {
	f, _ := newFollower()

	evil := Block{View: 0, Height: 1, Parent: genesisID, Cmd: []byte("injected"), Justify: quorumQC(genesisID, 0)}
	evil.ID = blockID(evil.View, evil.Height, evil.Parent, evil.Cmd)
	bm := &BlockMsg{Block: evil, From: testPeer1}
	_, p1priv := testKeyOf(testPeer1)
	bm.Sig = signPayload(p1priv, bm) // authentic sender, unsolicited content

	f.HandleBlock(bm)

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks[evil.ID]; ok {
		t.Fatal("an unsolicited block entered the tree")
	}
	if f.head != genesisID {
		t.Fatalf("an unsolicited block moved the head to %q", f.head)
	}
}

// TestFetchedBlockWithFabricatedQCRejected: when a replica DOES ask for a block
// (its proposal referenced a missing parent), the answer must still carry a
// genuine QC certifying that parent. A Byzantine member answering with a
// plausible block plus a fabricated justification is refused; the honest answer
// is accepted, so the gate rejects the forgery without breaking catch-up.
func TestFetchedBlockWithFabricatedQCRejected(t *testing.T) {
	f, tr := newFollower()

	// B1 is the block this replica will have to fetch: it is the parent of a
	// proposal it receives, but it has never seen B1 itself.
	b1 := Block{View: 0, Height: 1, Parent: genesisID, Cmd: []byte("a"), Justify: quorumQC(genesisID, 0)}
	b1.ID = blockID(b1.View, b1.Height, b1.Parent, b1.Cmd)

	p2 := prop(2, b1.ID, "b", quorumQC(b1.ID, 1))
	f.HandleProposal(p2)
	tr.mu.Lock()
	fetches := append([]string(nil), tr.fetchIDs...)
	tr.mu.Unlock()
	if len(fetches) != 1 || fetches[0] != b1.ID {
		t.Fatalf("test setup: expected one outstanding fetch for B1, got %v", fetches)
	}

	// (1) A fabricated justification: signatures that verify for nobody.
	forged := b1
	forgedJustify := newQC(b1.ID, 1)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		forgedJustify.Votes[v] = []byte("not-a-signature")
	}
	forged.Justify = forgedJustify
	bm := &BlockMsg{Block: forged, From: testPeer1}
	_, p1priv := testKeyOf(testPeer1)
	bm.Sig = signPayload(p1priv, bm)
	f.HandleBlock(bm)
	f.mu.Lock()
	_, injected := f.blocks[b1.ID]
	f.mu.Unlock()
	if injected {
		t.Fatal("a fetched block with a fabricated QC entered the tree")
	}

	// (2) A justification that is a genuine QC but certifies the WRONG block.
	misplaced := b1
	misplaced.Justify = quorumQC("some-other-block", 1)
	bm2 := &BlockMsg{Block: misplaced, From: testPeer1}
	bm2.Sig = signPayload(p1priv, bm2)
	f.HandleBlock(bm2)
	f.mu.Lock()
	_, injected = f.blocks[b1.ID]
	f.mu.Unlock()
	if injected {
		t.Fatal("a fetched block justified by a QC for another block entered the tree")
	}

	// (3) The honest answer is accepted, and the buffered proposal ships.
	good := &BlockMsg{Block: b1, From: testPeer1}
	good.Sig = signPayload(p1priv, good)
	f.HandleBlock(good)
	f.mu.Lock()
	_, added := f.blocks[b1.ID]
	f.mu.Unlock()
	if !added {
		t.Fatal("the honestly fetched block was refused")
	}

	// A block that was never requested, but is otherwise identical, is still
	// refused (the gate is per-block, not per-sender).
	other := Block{View: 0, Height: 1, Parent: genesisID, Cmd: []byte("never-requested"), Justify: quorumQC(genesisID, 0)}
	other.ID = blockID(other.View, other.Height, other.Parent, other.Cmd)
	bm3 := &BlockMsg{Block: other, From: testPeer1}
	bm3.Sig = signPayload(p1priv, bm3)
	f.HandleBlock(bm3)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks[other.ID]; ok {
		t.Fatal("a never-requested block entered the tree")
	}
}
