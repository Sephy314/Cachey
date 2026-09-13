package hotstuff

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Phase 2.3/2.4 tests: block sync (range/batch transfer with anchor-based
// branch selection) and catch-up (a lagging replica reconstructs its chain
// and resumes consensus). The invariants pinned here:
//
//	A large gap triggers a range sync (GetBlocks), a small gap a single Fetch.
//	A batch is validated as a whole (identity, parent continuity, QCs) before
//	  anything is inserted.
//	Unknown request IDs, wrong peers and stale responses are ignored.
//	A partial batch continues the sync from the highest block received.
//	The anchor must be on the target's branch (no silent branch reinterpretation).
//	Received != committed: a synced block commits only through the chain rules.
//	A caught-up replica resumes voting and proposing.

// chainBlocks builds n blocks at heights 1..n, each the direct child (and
// justification) of the previous, carrying cmd "c<i>". The QCs carry 2f+1
// phantom votes so any phantom-wired replica can validate them.
func chainBlocks(n int) []Block {
	blocks := make([]Block, 0, n)
	prev, prevH := genesisID, uint64(0)
	for i := 1; i <= n; i++ {
		b := Block{View: 0, Height: uint64(i), Parent: prev, Cmd: []byte(fmt.Sprintf("c%d", i)), Justify: quorumQC(prev, prevH)}
		b.ID = blockID(0, uint64(i), prev, b.Cmd)
		blocks = append(blocks, b)
		prev, prevH = b.ID, uint64(i)
	}
	return blocks
}

// feedChain delivers blocks to n as signed proposals from the phantom leader.
func feedChain(n *Replica, blocks []Block) {
	for i := range blocks {
		b := &blocks[i]
		p := &Proposal{Block: *b, From: testLeaderID}
		_, priv := testKeyOf(testLeaderID)
		p.Sig = signPayload(priv, p)
		n.HandleProposal(p)
	}
}

// newFollowerID is newFollower with a configurable replica id (a member of the
// same phantom cluster {id, L0, P1, P2}).
func newFollowerID(id string) (*Replica, *recorderTransport) {
	all := []string{testLeaderID, testPeer1, testPeer2}
	var peers []string
	for _, p := range all {
		if p != id {
			peers = append(peers, p)
		}
	}
	tr := &recorderTransport{}
	n, err := NewReplica(Config{ID: id, Peers: peers, Leader: testLeaderID}, tr, nil)
	if err != nil {
		panic(err)
	}
	wirePhantom(n, testLeaderID, testPeer1, testPeer2)
	return n, tr
}

// batchFrom signs a BlockBatch as sender.
func batchFrom(sender string, bb *BlockBatch) *BlockBatch {
	_, priv := testKeyOf(sender)
	bb.Sig = signPayload(priv, bb)
	return bb
}

// TestRangeSyncLargeGap: a replica far behind (proposal height > head+2)
// requests a range (GetBlocks) instead of a single Fetch, and a valid batch
// lets it catch up and vote for the buffered proposal.
func TestRangeSyncLargeGap(t *testing.T) {
	blocks := chainBlocks(10)
	src, _ := newFollower()
	feedChain(src, blocks) // the source holds heights 1..10

	tgt, tr := newFollowerID("F2")
	// A proposal at height 11 whose parent (height 10) is far above the head.
	top := Block{View: 0, Height: 11, Parent: blocks[9].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[9].ID, 10)}
	top.ID = blockID(0, 11, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)

	tr.mu.Lock()
	syncs := append([]GetBlocks(nil), tr.syncs...)
	tr.mu.Unlock()
	if len(syncs) != 1 || syncs[0].Target != blocks[9].ID || syncs[0].Anchor != genesisID {
		t.Fatalf("expected one range sync for %q anchored at genesis, got %+v", blocks[9].ID, syncs)
	}
	if len(tr.fetchIDs) != 0 {
		t.Fatalf("a large gap must use the range sync, not single fetches: %v", tr.fetchIDs)
	}

	// Answer with the full batch (heights 1..10), signed by the proposer.
	bb := batchFrom(testLeaderID, &BlockBatch{
		RequestID: syncs[0].RequestID, Anchor: genesisID, Target: blocks[9].ID,
		Blocks: blocks, From: testLeaderID,
	})
	tgt.HandleBlockBatch(bb)

	tgt.mu.Lock()
	_, inTree := tgt.blocks[top.ID]
	headH := tgt.blocks[tgt.head].Height
	tgt.mu.Unlock()
	if !inTree {
		t.Fatal("the buffered proposal was not processed after the batch")
	}
	if headH != 11 {
		t.Fatalf("head height = %d, want 11", headH)
	}
	if got := tr.voteCount(top.ID); got != 1 {
		t.Fatalf("the caught-up replica must vote for the proposal, got %d votes", got)
	}
}

// TestSmallGapUsesSingleFetch: a gap of one block keeps the existing
// single-block Fetch path (no range sync).
func TestSmallGapUsesSingleFetch(t *testing.T) {
	blocks := chainBlocks(2)
	src, _ := newFollower()
	feedChain(src, blocks)

	tgt, tr := newFollowerID("F2")
	// Proposal at height 3, parent at height 2: gap of 2 above head(0) — not
	// > head+2, so a single Fetch.
	top := Block{View: 0, Height: 3, Parent: blocks[1].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[1].ID, 2)}
	top.ID = blockID(0, 3, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)

	tr.mu.Lock()
	fetches := append([]string(nil), tr.fetchIDs...)
	syncs := len(tr.syncs)
	tr.mu.Unlock()
	if len(fetches) != 1 || fetches[0] != blocks[1].ID {
		t.Fatalf("a small gap must use a single Fetch for the parent, got %v", fetches)
	}
	if syncs != 0 {
		t.Fatalf("a small gap must not trigger a range sync, got %d", syncs)
	}
}

// TestBatchValidationRejects: a batch with a content mismatch, a parent
// discontinuity, or a fabricated QC is rejected as a whole — nothing is
// inserted.
func TestBatchValidationRejects(t *testing.T) {
	blocks := chainBlocks(5)
	tgt, tr := newFollowerID("F2")
	// Trigger a range sync for the height-5 block.
	top := Block{View: 0, Height: 6, Parent: blocks[4].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[4].ID, 5)}
	top.ID = blockID(0, 6, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	req := tr.syncs[0]
	tr.mu.Unlock()

	// (1) Content mismatch: mutate a block's Cmd without recomputing its id.
	bad := make([]Block, len(blocks))
	copy(bad, blocks)
	bad[2].Cmd = []byte("tampered")
	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[4].ID, Blocks: bad, From: testLeaderID})
	tgt.HandleBlockBatch(bb)
	tgt.mu.Lock()
	inserted := len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 { // genesis only
		t.Fatalf("a batch with a content mismatch inserted %d blocks, want 0", inserted-1)
	}

	// (2) Parent discontinuity: block 2's parent is not block 1.
	bad2 := make([]Block, len(blocks))
	copy(bad2, blocks)
	bad2[2].Parent = "some-other-block"
	bb2 := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[4].ID, Blocks: bad2, From: testLeaderID})
	tgt.HandleBlockBatch(bb2)
	tgt.mu.Lock()
	inserted = len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 {
		t.Fatalf("a batch with a parent discontinuity inserted %d blocks, want 0", inserted-1)
	}

	// (3) Fabricated QC: signatures that verify for nobody.
	bad3 := make([]Block, len(blocks))
	copy(bad3, blocks)
	bad3[1].Justify = newQC(blocks[0].ID, 0)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		bad3[1].Justify.Votes[v] = []byte("not-a-signature")
	}
	bb3 := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[4].ID, Blocks: bad3, From: testLeaderID})
	tgt.HandleBlockBatch(bb3)
	tgt.mu.Lock()
	inserted = len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 {
		t.Fatalf("a batch with a fabricated QC inserted %d blocks, want 0", inserted-1)
	}
}

// TestBatchUnknownRequestAndWrongPeer: a response for an unknown request id or
// from a peer other than the one asked is ignored.
func TestBatchUnknownRequestAndWrongPeer(t *testing.T) {
	blocks := chainBlocks(3)
	tgt, tr := newFollowerID("F2")
	top := Block{View: 0, Height: 4, Parent: blocks[2].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[2].ID, 3)}
	top.ID = blockID(0, 4, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	req := tr.syncs[0]
	tr.mu.Unlock()

	// Unknown request id.
	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID + 999, Anchor: genesisID, Target: blocks[2].ID, Blocks: blocks, From: testLeaderID})
	tgt.HandleBlockBatch(bb)
	tgt.mu.Lock()
	inserted := len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 {
		t.Fatalf("a response for an unknown request id inserted %d blocks, want 0", inserted-1)
	}

	// Wrong peer: the request went to L0, the response claims P1.
	bb2 := batchFrom(testPeer1, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[2].ID, Blocks: blocks, From: testPeer1})
	tgt.HandleBlockBatch(bb2)
	tgt.mu.Lock()
	inserted = len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 {
		t.Fatalf("a response from the wrong peer inserted %d blocks, want 0", inserted-1)
	}
}

// TestBatchStaleResponse: a response past the request's deadline is ignored.
func TestBatchStaleResponse(t *testing.T) {
	blocks := chainBlocks(3)
	tgt, tr := newFollowerID("F2")
	top := Block{View: 0, Height: 4, Parent: blocks[2].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[2].ID, 3)}
	top.ID = blockID(0, 4, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	req := tr.syncs[0]
	tr.mu.Unlock()

	// Expire the request.
	tgt.mu.Lock()
	r := tgt.syncReqs[req.RequestID]
	r.deadline = time.Now().Add(-time.Second)
	tgt.syncReqs[req.RequestID] = r
	tgt.mu.Unlock()

	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[2].ID, Blocks: blocks, From: testLeaderID})
	tgt.HandleBlockBatch(bb)
	tgt.mu.Lock()
	inserted := len(tgt.blocks)
	tgt.mu.Unlock()
	if inserted != 1 {
		t.Fatalf("a stale response inserted %d blocks, want 0", inserted-1)
	}
}

// TestBatchPartialContinues: a batch that does not reach the target continues
// the sync from the highest block received.
func TestBatchPartialContinues(t *testing.T) {
	blocks := chainBlocks(10)
	tgt, tr := newFollowerID("F2")
	top := Block{View: 0, Height: 11, Parent: blocks[9].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[9].ID, 10)}
	top.ID = blockID(0, 11, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	req := tr.syncs[0]
	tr.mu.Unlock()

	// Answer with only heights 1..5.
	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[9].ID, Blocks: blocks[:5], From: testLeaderID})
	tgt.HandleBlockBatch(bb)

	tr.mu.Lock()
	syncs := append([]GetBlocks(nil), tr.syncs...)
	tr.mu.Unlock()
	if len(syncs) != 2 {
		t.Fatalf("a partial batch must continue the sync, got %d requests", len(syncs))
	}
	if syncs[1].Anchor != blocks[4].ID || syncs[1].Target != blocks[9].ID {
		t.Fatalf("continuation anchored at %q target %q, want anchor %q target %q",
			syncs[1].Anchor, syncs[1].Target, blocks[4].ID, blocks[9].ID)
	}
}

// TestGetBlocksAnchorNotOnBranch: the responder stays silent when the anchor
// is not on the target's branch (no silent branch reinterpretation).
func TestGetBlocksAnchorNotOnBranch(t *testing.T) {
	blocks := chainBlocks(5)
	src, tr := newFollower()
	feedChain(src, blocks)

	// A fork: a block at height 3 on a different branch (parent = genesis).
	fork := Block{View: 0, Height: 3, Parent: genesisID, Cmd: []byte("fork"), Justify: quorumQC(genesisID, 0)}
	fork.ID = blockID(0, 3, fork.Parent, fork.Cmd)
	src.HandleProposal(&Proposal{Block: fork, From: testLeaderID})

	// Request the fork's branch anchored at blocks[2] (not on it).
	src.HandleGetBlocks(&GetBlocks{RequestID: 7, Anchor: blocks[2].ID, Target: fork.ID, From: testPeer1})
	tr.mu.Lock()
	batches := len(tr.batches)
	tr.mu.Unlock()
	if batches != 0 {
		t.Fatalf("a batch for a branch that does not contain the anchor was sent (%d)", batches)
	}

	// The honest request (anchor on the branch) is answered.
	src.HandleGetBlocks(&GetBlocks{RequestID: 8, Anchor: genesisID, Target: blocks[4].ID, From: testPeer1})
	tr.mu.Lock()
	batches = len(tr.batches)
	tr.mu.Unlock()
	if batches != 1 {
		t.Fatalf("an anchored request was not answered, got %d batches", batches)
	}
}

// TestCatchUpClusterRejoin: a partitioned replica misses several committed
// commands, catches up via the range sync when the partition heals, and the
// cluster keeps committing with a new leader (the caught-up replica votes
// again).
func TestCatchUpClusterRejoin(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	proposeLoop(t, nodes["L0"], []string{"a"}, 3)
	waitAllApplied(t, nw, ids, []string{"a"})

	// Partition L3; L0 commits b, c, d (L3 misses them all).
	nw.dropPeer("L3")
	proposeLoop(t, nodes["L0"], []string{"b", "c", "d"}, 3)
	waitAllApplied(t, nw, []string{"L0", "L1", "L2"}, []string{"a", "b", "c", "d"})

	// Heal; L0 proposes e. L3's gap is large (heights 2..5 missing), so it
	// range-syncs and converges.
	nw.unDropPeer("L3")
	proposeLoop(t, nodes["L0"], []string{"e"}, 3)
	waitAllApplied(t, nw, ids, []string{"a", "b", "c", "d", "e"})

	// L0 goes silent; L1, L2, L3 suspect it. L1 (leaderOf(1)) leads view 1
	// and proposes f; the caught-up L3 votes and the cluster converges.
	for _, id := range []string{"L1", "L2", "L3"} {
		nodes[id].StartViewChange()
	}
	if !nodes["L1"].IsLeader() {
		t.Fatal("L1 should be the active leader of view 1 after the view change")
	}
	proposeLoop(t, nodes["L1"], []string{"f"}, 3)
	waitAllApplied(t, nw, ids, []string{"a", "b", "c", "d", "e", "f"})
}

// TestCatchUpRestartThenSync: a WAL-backed replica restarts behind the
// cluster, then catches up through the range sync and resumes voting.
func TestCatchUpRestartThenSync(t *testing.T) {
	dir := t.TempDir()
	blocks := chainBlocks(8)

	// Life 1: a WAL-backed follower accepts heights 1..3, then "crashes".
	r, w, _ := openWALReplica(t, dir, "F2", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	wirePhantom(r, testLeaderID, testPeer1, testPeer2)
	feedChain(r, blocks[:3])
	w.Close()

	// Life 2: restart; the replica holds heights 1..3 only. A proposal at
	// height 9 (parent height 8) triggers the range sync.
	r2, w2, tr2 := openWALReplica(t, dir, "F2", testLeaderID, []string{testLeaderID, testPeer1, testPeer2}, nil)
	defer w2.Close()
	wirePhantom(r2, testLeaderID, testPeer1, testPeer2)
	top := Block{View: 0, Height: 9, Parent: blocks[7].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[7].ID, 8)}
	top.ID = blockID(0, 9, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	r2.HandleProposal(p)

	tr2.mu.Lock()
	syncs := append([]GetBlocks(nil), tr2.syncs...)
	tr2.mu.Unlock()
	if len(syncs) != 1 || syncs[0].Target != blocks[7].ID {
		t.Fatalf("restarted replica must range-sync for the missing parent, got %+v", syncs)
	}

	// Answer with the missing range (heights 4..8).
	bb := batchFrom(testLeaderID, &BlockBatch{
		RequestID: syncs[0].RequestID, Anchor: blocks[2].ID, Target: blocks[7].ID,
		Blocks: blocks[3:], From: testLeaderID,
	})
	r2.HandleBlockBatch(bb)
	r2.mu.Lock()
	_, inTree := r2.blocks[top.ID]
	headH := r2.blocks[r2.head].Height
	r2.mu.Unlock()
	if !inTree || headH != 9 {
		t.Fatalf("restarted replica did not catch up: inTree=%v headH=%d", inTree, headH)
	}
	if got := tr2.voteCount(top.ID); got != 1 {
		t.Fatalf("restarted replica must vote after catch-up, got %d", got)
	}
}

// TestCatchUpEpochBehind: a replica behind by a membership transition
// reconstructs the epoch history from the synced chain — the new validator set
// activates and the replica votes for a new-epoch block.
func TestCatchUpEpochBehind(t *testing.T) {
	// Source: a follower that has committed the transition (add E) and is in
	// epoch 1.
	src, _ := newFollower()
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}
	// T (height 1, membership), B1 (2), B2 (3) commit T; B3 (4) is epoch 1.
	tp := prop(1, genesisID, string(cmd), quorumQC(genesisID, 0))
	src.HandleProposal(tp)
	tid := tp.Block.ID
	b1p := prop(2, tid, "x", quorumQC(tid, 1))
	src.HandleProposal(b1p)
	b1id := b1p.Block.ID
	b2p := prop(3, b1id, "y", quorumQC(b1id, 2))
	src.HandleProposal(b2p)
	b2id := b2p.Block.ID
	b3p := propEpoch(1, 4, b2id, "z", quorumQC(b2id, 3))
	src.HandleProposal(b3p)
	b3id := b3p.Block.ID

	// Target: a fresh replica that has seen nothing. A proposal at height 5
	// (parent = B3) triggers the range sync for B3. Its justification is an
	// epoch-1 QC (B3 is the first new-epoch block, voted by set(1)) and its
	// own epoch is 1 (derived from the transition in its ancestry).
	tgt, tr := newFollowerID("F2")
	top := Block{View: 0, Height: 5, Parent: b3id, Cmd: []byte("top"), Justify: quorumQCEpoch(1, b3id, 4), Epoch: 1}
	top.ID = blockID(0, 5, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	syncs := append([]GetBlocks(nil), tr.syncs...)
	tr.mu.Unlock()
	if len(syncs) != 1 {
		t.Fatalf("expected a range sync, got %d", len(syncs))
	}

	// Answer with the whole chain: T, B1, B2, B3.
	chain := []Block{tp.Block, b1p.Block, b2p.Block, b3p.Block}
	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: syncs[0].RequestID, Anchor: genesisID, Target: b3id, Blocks: chain, From: testLeaderID})
	tgt.HandleBlockBatch(bb)

	tgt.mu.Lock()
	epoch := tgt.epoch
	hasE := tgt.sets[1] != nil && tgt.sets[1].has("E")
	_, inTree := tgt.blocks[top.ID]
	tgt.mu.Unlock()
	if epoch != 1 || !hasE {
		t.Fatalf("catch-up did not reconstruct epoch 1: epoch=%d hasE=%v", epoch, hasE)
	}
	if !inTree {
		t.Fatal("the new-epoch proposal was not processed after catch-up")
	}
	if got := tr.voteCount(top.ID); got != 1 {
		t.Fatalf("the caught-up replica must vote for the new-epoch block, got %d", got)
	}
}

// TestSyncedBlocksNotCommitted: blocks received through a batch are stored and
// QC-folded, but a block commits only when the chain rules reach it — the
// batch alone never marks anything committed. Here the buffered proposal's QC
// completes the 3-chain above B1, so exactly B1 commits; B2 and B3 stay
// stored-but-uncommitted.
func TestSyncedBlocksNotCommitted(t *testing.T) {
	blocks := chainBlocks(3)
	tgt, tr := newFollowerID("F2")
	top := Block{View: 0, Height: 4, Parent: blocks[2].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[2].ID, 3)}
	top.ID = blockID(0, 4, top.Parent, top.Cmd)
	p := &Proposal{Block: top, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	tgt.HandleProposal(p)
	tr.mu.Lock()
	req := tr.syncs[0]
	tr.mu.Unlock()

	bb := batchFrom(testLeaderID, &BlockBatch{RequestID: req.RequestID, Anchor: genesisID, Target: blocks[2].ID, Blocks: blocks, From: testLeaderID})
	tgt.HandleBlockBatch(bb)

	// The 3-chain above B1 is complete (B1 <- B2 <- B3 <- top), so B1 commits
	// — but only B1: B2 and B3 are stored, not committed.
	tgt.mu.Lock()
	exec := tgt.bExec
	applied := tgt.appliedCmdsLocked()
	tgt.mu.Unlock()
	if exec != blocks[0].ID || fmt.Sprint(applied) != "[c1]" {
		t.Fatalf("commit rule violated: exec=%q applied=%v, want B1 [c1]", exec, applied)
	}
}

// TestCatchUpThenProposeSingleNode: a single-node replica that restarts behind
// its own WAL (missing tail blocks) recovers, then proposes and commits again.
func TestCatchUpThenProposeSingleNode(t *testing.T) {
	dir := t.TempDir()

	// Life 1: a WAL-backed solo node accepts heights 1..6 (nothing committed —
	// a solo node commits only with a 3-chain, which needs 3 proposals).
	r, w, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	firstID, err := r.Propose([]byte("c1"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 6; i++ {
		if _, err := r.Propose([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Life 2: restart. The tree is rebuilt from the WAL; the node proposes
	// and commits.
	r2, w2, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	defer w2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if _, err := r2.Propose([]byte("new")); err != nil {
		t.Fatalf("restarted solo node could not propose: %v", err)
	}
	if _, err := r2.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if err := r2.WaitCommitted(ctx, firstID); err != nil {
		t.Fatalf("recovered block did not commit after restart: %v", err)
	}
}
