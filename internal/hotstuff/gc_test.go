package hotstuff

import (
	"crypto/ed25519"
	"fmt"
	"testing"

	"github.com/Sephy314/Cachey/internal/wal"
)

// Phase 2.5 GC tests: the engine-level GC boundary, idempotence, historical
// QC/ValidatorSet preservation, epoch derivation after GC, catch-up after GC,
// and crash-safe recovery (checkpoint + WAL replay).

// newFollowerRet is newFollower with a small GC retention so tests can drive
// actual deletions.
func newFollowerRet(retention uint64) (*Replica, *recorderTransport) {
	return newFollowerRetID("F", retention)
}

// newFollowerRetID is newFollowerRet with a configurable replica id (a member
// of the same phantom cluster {id, L0, P1, P2}). The replica's identity is the
// deterministic test key of id, so its own signatures verify against the
// phantom-wired set(0) keys.
func newFollowerRetID(id string, retention uint64) (*Replica, *recorderTransport) {
	all := []string{testLeaderID, testPeer1, testPeer2}
	var peers []string
	for _, p := range all {
		if p != id {
			peers = append(peers, p)
		}
	}
	_, priv := testKeyOf(id)
	tr := &recorderTransport{}
	n, err := NewReplica(Config{
		ID: id, Peers: peers, Leader: testLeaderID, GCRetention: retention, PrivateKey: priv,
	}, tr, nil)
	if err != nil {
		panic(err)
	}
	wirePhantom(n, testLeaderID, testPeer1, testPeer2)
	return n, tr
}

// TestGCDeletesOldBlocks: after committing a chain, GC deletes blocks below
// bExec-retention while keeping the committed prefix from the boundary up,
// the uncommitted tail, and genesis. The GC base is the lowest retained block.
func TestGCDeletesOldBlocks(t *testing.T) {
	f, _ := newFollowerRet(2)
	ids := mainChain(t, f, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	// mainChain feeds 10 blocks; the 3-chain commits up to height 7 (QC(B9)
	// commits B7: B7<-B8<-B9).
	f.mu.Lock()
	execH := f.blocks[f.bExec].Height
	f.mu.Unlock()
	if execH != 7 {
		t.Fatalf("test setup: bExec height = %d, want 7", execH)
	}

	cp, deleted, err := f.GC()
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("GC deleted nothing")
	}
	f.mu.Lock()
	base := f.gcBase
	baseH := f.blocks[base].Height
	blockCount := len(f.blocks)
	hasGenesis := f.blocks[genesisID] != nil
	hasOld := f.blocks[ids[1]] != nil // height 1, below the boundary
	hasBoundary := f.blocks[ids[5]] != nil
	f.mu.Unlock()
	if baseH != 5 { // bExec(7) - retention(2)
		t.Fatalf("GC base height = %d, want 5", baseH)
	}
	if hasOld {
		t.Fatal("a block below the GC boundary was not deleted")
	}
	if !hasBoundary {
		t.Fatal("the GC boundary block was deleted")
	}
	if !hasGenesis {
		t.Fatal("genesis was deleted")
	}
	if blockCount != 7 { // genesis + heights 5,6,7,8,9,10
		t.Fatalf("retained %d blocks, want 7", blockCount)
	}
	// The checkpoint carries the retained blocks and the full set history.
	if len(cp.Blocks) != 7 {
		t.Fatalf("checkpoint has %d blocks, want 7", len(cp.Blocks))
	}
	if cp.GCBase != base || cp.BExec != f.bExec {
		t.Fatalf("checkpoint pointers wrong: base=%q exec=%q", cp.GCBase, cp.BExec)
	}
}

// TestGCIdempotent: a second GC with no new commits deletes nothing.
func TestGCIdempotent(t *testing.T) {
	f, _ := newFollowerRet(2)
	mainChain(t, f, []string{"a", "b", "c", "d", "e", "f", "g", "h"})
	if _, deleted, err := f.GC(); err != nil || !deleted {
		t.Fatalf("first GC: deleted=%v err=%v", deleted, err)
	}
	_, deleted, err := f.GC()
	if err != nil {
		t.Fatal(err)
	}
	if deleted {
		t.Fatal("a second GC with no new commits deleted something")
	}
}

// TestGCHistoricalQC: after GC, a QC from a deleted epoch still verifies —
// the ValidatorSet history is never GC'd.
func TestGCHistoricalQC(t *testing.T) {
	f, _ := newFollowerRet(2)
	// Commit a membership transition (add E) then more blocks.
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}
	tp := prop(1, genesisID, string(cmd), quorumQC(genesisID, 0))
	f.HandleProposal(tp)
	tid := tp.Block.ID
	b1p := prop(2, tid, "x", quorumQC(tid, 1))
	f.HandleProposal(b1p)
	b1id := b1p.Block.ID
	b2p := prop(3, b1id, "y", quorumQC(b1id, 2))
	f.HandleProposal(b2p)
	b2id := b2p.Block.ID
	b3p := propEpoch(1, 4, b2id, "z", quorumQC(b2id, 3))
	f.HandleProposal(b3p)
	b3id := b3p.Block.ID
	// More epoch-1 blocks so GC has something to delete.
	b4p := propEpoch(1, 5, b3id, "w", quorumQCEpoch(1, b3id, 4))
	f.HandleProposal(b4p)
	b4id := b4p.Block.ID
	b5p := propEpoch(1, 6, b4id, "v", quorumQCEpoch(1, b4id, 5))
	f.HandleProposal(b5p)
	b5id := b5p.Block.ID
	b6p := propEpoch(1, 7, b5id, "u", quorumQCEpoch(1, b5id, 6))
	f.HandleProposal(b6p)
	b6id := b6p.Block.ID
	b7p := propEpoch(1, 8, b6id, "t", quorumQCEpoch(1, b6id, 7))
	f.HandleProposal(b7p)
	b7id := b7p.Block.ID
	b8p := propEpoch(1, 9, b7id, "s", quorumQCEpoch(1, b7id, 8))
	f.HandleProposal(b8p)
	b8id := b8p.Block.ID
	b9p := propEpoch(1, 10, b8id, "r", quorumQCEpoch(1, b8id, 9))
	f.HandleProposal(b9p)
	b9id := b9p.Block.ID
	b10p := propEpoch(1, 11, b9id, "q", quorumQCEpoch(1, b9id, 10))
	f.HandleProposal(b10p)
	b10id := b10p.Block.ID
	_ = b10id

	// The epoch-0 QC over T (height 1) is below the GC boundary after GC.
	f.mu.Lock()
	oldQC := f.blocks[tid].Justify
	f.mu.Unlock()
	if !f.qcValid(oldQC) {
		t.Fatal("pre-GC: epoch-0 QC must verify")
	}
	if _, deleted, err := f.GC(); err != nil || !deleted {
		t.Fatalf("GC: deleted=%v err=%v", deleted, err)
	}
	// The QC still verifies: the epoch-0 ValidatorSet is retained.
	if !f.qcValid(oldQC) {
		t.Fatal("post-GC: historical epoch-0 QC no longer verifies")
	}
	// The epoch-1 set is retained too.
	f.mu.Lock()
	hasE := f.sets[1] != nil && f.sets[1].has("E")
	f.mu.Unlock()
	if !hasE {
		t.Fatal("post-GC: epoch-1 ValidatorSet lost")
	}
}

// TestGCEpochDerivationAfterGC: after GC, a new block's epoch is still derived
// correctly from the retained ancestry (the GC base anchors the derivation).
func TestGCEpochDerivationAfterGC(t *testing.T) {
	f, _ := newFollowerRet(2)
	// Commit a transition (add E) and enough blocks to GC past it.
	epub := testPub("E")
	cmd, err := membershipCmd(&MembershipChange{Add: []ValidatorEntry{{ID: "E", Pub: epub}}})
	if err != nil {
		t.Fatal(err)
	}
	tp := prop(1, genesisID, string(cmd), quorumQC(genesisID, 0))
	f.HandleProposal(tp)
	tid := tp.Block.ID
	b1p := prop(2, tid, "x", quorumQC(tid, 1))
	f.HandleProposal(b1p)
	b1id := b1p.Block.ID
	b2p := prop(3, b1id, "y", quorumQC(b1id, 2))
	f.HandleProposal(b2p)
	b2id := b2p.Block.ID
	b3p := propEpoch(1, 4, b2id, "z", quorumQC(b2id, 3))
	f.HandleProposal(b3p)
	b3id := b3p.Block.ID
	b4p := propEpoch(1, 5, b3id, "w", quorumQCEpoch(1, b3id, 4))
	f.HandleProposal(b4p)
	b4id := b4p.Block.ID
	b5p := propEpoch(1, 6, b4id, "v", quorumQCEpoch(1, b4id, 5))
	f.HandleProposal(b5p)
	b5id := b5p.Block.ID
	b6p := propEpoch(1, 7, b5id, "u", quorumQCEpoch(1, b5id, 6))
	f.HandleProposal(b6p)
	b6id := b6p.Block.ID
	b7p := propEpoch(1, 8, b6id, "t", quorumQCEpoch(1, b6id, 7))
	f.HandleProposal(b7p)
	b7id := b7p.Block.ID
	b8p := propEpoch(1, 9, b7id, "s", quorumQCEpoch(1, b7id, 8))
	f.HandleProposal(b8p)
	b8id := b8p.Block.ID
	b9p := propEpoch(1, 10, b8id, "r", quorumQCEpoch(1, b8id, 9))
	f.HandleProposal(b9p)
	b9id := b9p.Block.ID
	b10p := propEpoch(1, 11, b9id, "q", quorumQCEpoch(1, b9id, 10))
	f.HandleProposal(b10p)
	b10id := b10p.Block.ID

	// GC: the transition block T (height 1) is deleted; the GC base is in
	// epoch 1.
	if _, deleted, err := f.GC(); err != nil || !deleted {
		t.Fatalf("GC: deleted=%v err=%v", deleted, err)
	}
	f.mu.Lock()
	baseEpoch := f.blocks[f.gcBase].Epoch
	f.mu.Unlock()
	if baseEpoch != 1 {
		t.Fatalf("GC base epoch = %d, want 1", baseEpoch)
	}

	// A new epoch-1 block chained onto the head still derives epoch 1.
	head := f.blocks[f.head]
	np := propEpoch(1, head.Height+1, head.ID, "new", quorumQCEpoch(1, head.ID, head.Height))
	f.HandleProposal(np)
	f.mu.Lock()
	_, inTree := f.blocks[np.Block.ID]
	f.mu.Unlock()
	if !inTree {
		t.Fatal("a post-GC epoch-1 block was rejected")
	}
	_ = b10id
}

// TestGCCatchUpAfterGC: after GC, a lagging replica can still range-sync the
// retained blocks (the GC'd node serves them). The requester must anchor at a
// block the GC'd node still holds (its gcBase or above) — the GC'd node can
// no longer serve ancestry below its base.
func TestGCCatchUpAfterGC(t *testing.T) {
	// Source: the proposer (L0) that committed a long chain and GC'd it
	// (retention 5, so its GC base is well below its executed prefix). The
	// target's range request goes to the proposer, so the source must be L0.
	src, srcTr := newFollowerRetID(testLeaderID, 5)
	blocks := chainBlocks(12)
	feedChain(src, blocks)
	if _, deleted, err := src.GC(); err != nil || !deleted {
		t.Fatalf("GC: deleted=%v err=%v", deleted, err)
	}
	src.mu.Lock()
	baseH := src.blocks[src.gcBase].Height
	src.mu.Unlock()

	// Target: a replica that holds up to the source's GC base (its executed
	// prefix reaches baseH) and needs the tail above it. Feeding baseH+3
	// blocks puts bExec at baseH (bExec = fed-3) while keeping the gap to the
	// top proposal large enough to trigger the range sync.
	tgt, tr := newFollowerID("F2")
	feedChain(tgt, blocks[:baseH+3])
	top := Block{View: 0, Height: 13, Parent: blocks[11].ID, Cmd: []byte("top"), Justify: quorumQC(blocks[11].ID, 12)}
	top.ID = blockID(0, 13, top.Parent, top.Cmd)
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

	// The GC'd source answers with the retained range (baseH+1 .. 12). The
	// requester must be a member the source knows (testPeer1).
	src.HandleGetBlocks(&GetBlocks{RequestID: syncs[0].RequestID, Anchor: syncs[0].Anchor, Target: blocks[11].ID, From: testPeer1})
	srcTr.mu.Lock()
	batches := append([]BlockBatch(nil), srcTr.batches...)
	srcTr.mu.Unlock()
	if len(batches) != 1 {
		t.Fatalf("the GC'd source did not answer the range request, got %d batches", len(batches))
	}
	bb := batches[0]
	if want := int(12 - baseH); len(bb.Blocks) != want {
		t.Fatalf("the GC'd source served %d blocks, want %d (heights %d..12)", len(bb.Blocks), want, baseH+1)
	}
	tgt.HandleBlockBatch(&bb)
	tgt.mu.Lock()
	_, inTree := tgt.blocks[top.ID]
	tgt.mu.Unlock()
	if !inTree {
		t.Fatal("the target did not catch up from the GC'd source")
	}
}

// TestGCRestartRecovery: a WAL-backed replica GCs, restarts, and recovers the
// retained state — the GC base, watermark, and validator sets survive, and the
// replica keeps committing.
func TestGCRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	// A fixed identity key across both lives (production loads it from disk),
	// so the checkpoint's validator keys verify the restarted node's votes.
	_, priv := testKeyOf("solo")
	// Life 1: a WAL-backed solo node commits a chain, then GCs.
	r, w, _ := openWALReplicaKey(t, dir, "solo", "", nil, nil, priv)
	r.mu.Lock()
	r.gcRetention = 2
	r.mu.Unlock()
	firstID, err := r.Propose([]byte("c1"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 10; i++ {
		if _, err := r.Propose([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Solo node: 3-chain commits up to height 8.
	cp, deleted, err := r.GC()
	if err != nil || !deleted {
		t.Fatalf("GC: deleted=%v err=%v", deleted, err)
	}
	r.mu.Lock()
	base := r.gcBase
	r.mu.Unlock()
	w.Close()

	// Life 2: restart with the checkpoint loaded BEFORE the WAL replay (the
	// production order: checkpoint first, then the WAL tail on top). The WAL
	// was NOT rotated (engine-level GC does not rotate; the server does), so
	// the full WAL replays idempotently on top of the checkpoint — the tree is
	// rebuilt completely, while the GC base and watermark are preserved.
	r2, w2 := openWALReplicaCP(t, dir, "solo", "", nil, nil, cp, priv)
	defer w2.Close()
	r2.mu.Lock()
	base2 := r2.gcBase
	exec2 := r2.bExec
	blocks2 := len(r2.blocks)
	r2.mu.Unlock()
	if base2 != base {
		t.Fatalf("GC base lost across restart: %q != %q", base2, base)
	}
	if exec2 != cp.BExec {
		t.Fatalf("executed watermark lost across restart: %q != %q", exec2, cp.BExec)
	}
	if blocks2 != 11 { // genesis + heights 1..10 (full WAL replay, no rotation)
		t.Fatalf("recovered %d blocks, want 11 (full WAL replay)", blocks2)
	}

	// The recovered node keeps committing.
	if _, err := r2.Propose([]byte("new")); err != nil {
		t.Fatalf("post-GC restart could not propose: %v", err)
	}
	if _, err := r2.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Propose(nil); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := r2.WaitCommitted(ctx, firstID); err != nil {
		t.Fatalf("recovered block did not commit after GC restart: %v", err)
	}
}

// TestGCCrashBeforeRotation: a crash after the checkpoint is written but
// before the WAL rotation recovers correctly — the full WAL replays
// idempotently on top of the checkpoint.
func TestGCCrashBeforeRotation(t *testing.T) {
	dir := t.TempDir()
	// Life 1: commit a chain, GC (checkpoint computed), but do NOT rotate the
	// WAL — simulate a crash between checkpoint and rotation.
	r, w, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	r.mu.Lock()
	r.gcRetention = 2
	r.mu.Unlock()
	firstID, err := r.Propose([]byte("c1"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 10; i++ {
		if _, err := r.Propose([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	cp, deleted, err := r.GC()
	if err != nil || !deleted {
		t.Fatalf("GC: deleted=%v err=%v", deleted, err)
	}
	w.Close() // crash: checkpoint never written, WAL never rotated

	// Life 2: restart WITHOUT the checkpoint (it was never written) — the full
	// WAL replays and rebuilds everything.
	r2, w2, _ := openWALReplica(t, dir, "solo", "", nil, nil)
	defer w2.Close()
	r2.FinishRecovery()
	r2.mu.Lock()
	exec2 := r2.bExec
	r2.mu.Unlock()
	if exec2 != cp.BExec {
		t.Fatalf("recovery without checkpoint: exec=%q want %q", exec2, cp.BExec)
	}
	ctx := t.Context()
	if err := r2.WaitCommitted(ctx, firstID); err != nil {
		t.Fatalf("recovered block did not commit: %v", err)
	}
}

// openWALReplicaCP boots a WAL-backed replica that loads an engine checkpoint
// BEFORE the WAL replay (the production order in OpenHotStuffNode), so records
// at or before the checkpoint are idempotently skipped. key must be the SAME
// identity used in the checkpoint's life (production loads it from disk), so
// self-votes and replayed QCs verify against the checkpoint's validator keys.
func openWALReplicaCP(t *testing.T, dir, id, leader string, peers []string, apply func(Block), cp Checkpoint, key ed25519.PrivateKey) (*Replica, *wal.WAL) {
	t.Helper()
	cfg := wal.DefaultConfig(dir)
	cfg.DisableRotation = true
	tr := &recorderTransport{}
	r, err := NewReplica(Config{ID: id, Leader: leader, Peers: peers, PrivateKey: key}, tr, apply)
	if err != nil {
		t.Fatal(err)
	}
	r.LoadCheckpoint(cp)
	w, err := wal.Open(cfg, wal.Hooks{ApplyRecord: r.ApplyRecoveredRecord})
	if err != nil {
		t.Fatal(err)
	}
	r.SetLogStore(NewWALLogStore(w))
	r.FinishRecovery()
	return r, w
}

// openWALReplicaKey is openWALReplica with a fixed identity key (so a
// checkpoint taken in one life verifies in the next).
func openWALReplicaKey(t *testing.T, dir, id, leader string, peers []string, apply func(Block), key ed25519.PrivateKey) (*Replica, *wal.WAL, *recorderTransport) {
	t.Helper()
	cfg := wal.DefaultConfig(dir)
	cfg.DisableRotation = true
	tr := &recorderTransport{}
	r, err := NewReplica(Config{ID: id, Leader: leader, Peers: peers, PrivateKey: key}, tr, apply)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(cfg, wal.Hooks{ApplyRecord: r.ApplyRecoveredRecord})
	if err != nil {
		t.Fatal(err)
	}
	r.SetLogStore(NewWALLogStore(w))
	r.FinishRecovery()
	return r, w, tr
}
