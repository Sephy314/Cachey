package hotstuff

import (
	"context"
	"time"
)

// This file implements Phase 2.3 block sync: a lagging replica requests a
// contiguous range of blocks on a branch (anchored at a block it holds) and
// validates the whole batch before inserting anything. The existing
// single-block Fetch/BlockMsg path stays for small gaps; the range sync kicks
// in when a proposal's parent is far above the replica's head.
//
// The anchor is the requester's executed prefix (bExec), which every valid
// branch extends (safeNode chains onto the lock, which descends from bExec),
// so the responder can always find it in the target's ancestry. The batch is
// validated bottom-up: content identity, parent continuity, genuine QCs at the
// parent's height and epoch, and the derived epoch per block (addBlockLocked).
// Nothing is inserted unless the entire batch is sound.

// syncTTL bounds how long a range request counts as outstanding. Without an
// expiry a request lost while a peer was unreachable would keep the range
// "already requested" forever.
const syncTTL = 2 * time.Second

// maxSyncBlocks caps one BlockBatch response. A larger range is answered in
// chunks; the requester continues the sync from the highest block received.
const maxSyncBlocks = 512

// syncRequest is one outstanding range request.
type syncRequest struct {
	id       uint64
	anchor   string
	target   string
	peer     string
	deadline time.Time
}

// syncInFlightLocked reports whether a range request for target is outstanding.
// Must hold n.mu.
func (n *Replica) syncInFlightLocked(target string) bool {
	now := time.Now()
	for _, r := range n.syncReqs {
		if r.target == target && now.Before(r.deadline) {
			return true
		}
	}
	return false
}

// markSyncLocked records a range request for target to peer, anchored at
// anchor, and returns it. Expired entries are pruned once the set grows. Must
// hold n.mu.
func (n *Replica) markSyncLocked(anchor, target, peer string) syncRequest {
	now := time.Now()
	if len(n.syncReqs) > pruneFetchThreshold {
		for id, r := range n.syncReqs {
			if now.After(r.deadline) {
				delete(n.syncReqs, id)
			}
		}
	}
	n.syncSeq++
	id := n.syncSeq
	req := syncRequest{id: id, anchor: anchor, target: target, peer: peer, deadline: now.Add(syncTTL)}
	n.syncReqs[id] = req
	return req
}

// HandleGetBlocks answers a peer's range request: the blocks on Target's
// branch from Anchor's child up to Target, parent-contiguous. If Anchor is not
// on Target's branch (or the responder does not hold it), it stays silent —
// the requester falls back to single-block fetch on timeout. The response is
// capped at maxSyncBlocks; a larger range is continued by the requester.
func (n *Replica) HandleGetBlocks(g *GetBlocks) {
	if g == nil {
		return
	}
	n.mu.Lock()
	known := n.knownMemberLocked(g.From)
	var blocks []Block
	if known {
		blocks = n.collectRangeLocked(g.Anchor, g.Target)
	}
	n.mu.Unlock()
	if len(blocks) == 0 {
		return
	}
	bb := &BlockBatch{RequestID: g.RequestID, Anchor: g.Anchor, Target: g.Target, Blocks: blocks, From: n.id}
	bb.Sig = n.sign(bb)
	_ = n.tr.SendBlockBatch(context.Background(), g.From, bb)
}

// collectRangeLocked walks target's ancestry down to anchor, returning the
// blocks strictly above the anchor (anchor-first order), or nil when the
// anchor is not on the branch. The walk runs under the lock: the tree is
// mutated by concurrent handlers, so reading it outside would race. Must hold
// n.mu.
func (n *Replica) collectRangeLocked(anchor, target string) []Block {
	t := n.blocks[target]
	if t == nil {
		return nil
	}
	var rev []Block
	found := false
	for b := t; b != nil; b = n.blocks[b.Parent] {
		rev = append(rev, *b)
		if b.ID == anchor {
			found = true
			break
		}
		if b.Parent == "" {
			break
		}
	}
	if !found {
		return nil // anchor not on the target's branch
	}
	// Reverse to anchor-first, drop the anchor itself (the requester holds
	// it), and cap the chunk.
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	rev = rev[1:]
	if len(rev) > maxSyncBlocks {
		rev = rev[:maxSyncBlocks]
	}
	return rev
}

// HandleBlockBatch folds a range response into the tree. The request must be
// one this replica sent (known id), from the peer it asked, and not stale; the
// response must be authentic. The batch is validated as a whole before
// anything is inserted. A partial batch (the target still missing) continues
// the sync from the highest block received.
func (n *Replica) HandleBlockBatch(bb *BlockBatch) {
	if bb == nil {
		return
	}
	var kids []*Proposal
	var cont *GetBlocks
	n.mu.Lock()
	req, ok := n.syncReqs[bb.RequestID]
	if !ok || req.peer != bb.From || time.Now().After(req.deadline) {
		n.mu.Unlock()
		return // unknown request, wrong peer, or stale
	}
	if !n.verify(bb.From, bb.Sig, *bb) {
		n.mu.Unlock()
		return // unauthenticated response
	}
	kids = n.insertBatchLocked(bb)
	delete(n.syncReqs, bb.RequestID)
	// A partial batch continues from the highest block received.
	if n.blocks[bb.Target] == nil && len(bb.Blocks) > 0 {
		last := bb.Blocks[len(bb.Blocks)-1]
		if n.blocks[last.ID] != nil {
			req := n.markSyncLocked(last.ID, bb.Target, bb.From)
			cont = &GetBlocks{RequestID: req.id, Anchor: req.anchor, Target: bb.Target, To: bb.From, From: n.id}
		}
	}
	n.mu.Unlock()
	for _, k := range kids {
		n.HandleProposal(k)
	}
	if cont != nil {
		_ = n.tr.SendGetBlocks(context.Background(), bb.From, cont)
	}
}

// insertBatchLocked validates a BlockBatch as a whole and inserts it
// bottom-up. It returns the buffered proposals unblocked by the inserted
// blocks. The batch must be a contiguous parent chain anchored in the tree,
// with content-consistent ids, genuine QCs certifying each parent at its
// height and epoch, and strictly increasing heights. Nothing is inserted
// unless the entire batch is sound. Must hold n.mu.
func (n *Replica) insertBatchLocked(bb *BlockBatch) []*Proposal {
	var kids []*Proposal
	if len(bb.Blocks) == 0 {
		return kids
	}
	// The lowest block's parent must anchor the batch in the tree.
	lowest := &bb.Blocks[0]
	anchor := n.blocks[lowest.Parent]
	if anchor == nil {
		return nil // not anchored — reject
	}
	prevH, wantEpoch := anchor.Height, anchor.Epoch
	for i := range bb.Blocks {
		b := &bb.Blocks[i]
		if b.ID != blockID(b.View, b.Height, b.Parent, b.Cmd) {
			return nil // content identity mismatch
		}
		if i > 0 && b.Parent != bb.Blocks[i-1].ID {
			return nil // parent discontinuity
		}
		// The justification must be a genuine QC certifying the parent at the
		// parent's height and epoch (the parent is known for the anchor; for
		// batch-internal parents the height/epoch are checked here and the
		// signatures by qcValid).
		if b.Justify == nil || !n.qcValid(b.Justify) ||
			b.Justify.NodeID != b.Parent || b.Justify.Height != prevH ||
			b.Justify.Epoch != wantEpoch || b.Height <= prevH {
			return nil
		}
		prevH, wantEpoch = b.Height, b.Epoch
	}
	// Insert bottom-up; addBlockLocked re-checks the derived epoch and folds
	// each QC (locking/committing as the chain rule dictates).
	for i := range bb.Blocks {
		b := &bb.Blocks[i]
		if n.blocks[b.ID] != nil {
			continue // already known (dedup)
		}
		kids = append(kids, n.addBlockLocked(b, b.Parent)...)
	}
	return kids
}
