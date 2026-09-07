package hotstuff

import (
	"context"
	"time"
)

// This file implements the HS-M2 pacemaker / view change — liveness when the
// current leader stops making progress (crashes or is silent).
//
// Model: the cluster runs in a sequence of views; the leader of view v is the
// deterministic leaderOf(v) (round-robin over the sorted members starting at
// the view-0 leader). The view-0 leader is born active; any later leader must
// first gather 2f+1 ViewChanges for its view (its own included), which is how
// a quorum of correct replicas proves they have given up on the previous
// leader. The new leader then adopts the HIGHEST QC reported by that quorum as
// its base and starts proposing from it.
//
// Why the +1 height skip (freshBase): the deposed leader may have left behind
// one in-flight block (above the highest QC, at base.height+1) that some
// correct replicas already voted for. The new leader's first block chains onto
// the QC-certified base at base.height+2, so it is strictly higher than
// anything any correct replica voted for in the previous view. Together with
// the monotone-height vote rule this means no replica ever votes for two
// conflicting blocks at the same height, and the 3-chain commit rule is
// unchanged — a view change can never fork a committed prefix.
//
// A replica that is behind (partitioned, or joining after several view
// changes) catches up two ways: a proposal from a future view's leader makes
// it advance its view, and a proposal/QC whose ancestor block it is missing
// makes it Fetch that block (and, recursively, its ancestors) from the peer
// that has it. HS-M2 is still in-memory and without signatures.

// StartViewChange suspects the current leader. If this replica has not yet
// announced its move to its current view it does so now (sending a ViewChange
// carrying its highest QC to the current view's leader — itself, if it is that
// leader, which may complete the quorum and activate it). If it already
// announced and still sees no progress, it advances to the next view and
// announces there. It is normally invoked by the suspicion timer (see
// SetViewTimeout); tests call it directly to stay deterministic (mirrors
// pbft.Replica.StartViewChange).
func (n *Replica) StartViewChange() {
	var vc *ViewChange
	var sendTo string
	var fetch *Fetch
	n.mu.Lock()
	target := n.view
	if target == 0 || n.vcSent[target] {
		// Genesis view needs no announcement; otherwise we already moved to
		// this view and it made no progress — suspect it too.
		target = n.view + 1
		n.enterViewLocked(target)
	}
	if target != 0 && !n.vcSent[target] {
		n.vcSent[target] = true
		vc = &ViewChange{View: target, HighQC: n.qcHigh, From: n.id}
		vc.Sig = n.sign(vc)
		sendTo = n.leader
		if n.id == sendTo {
			n.addVC(vc) // we are the target leader: count our own view change
			fetch = n.maybeActivateLocked(target)
		}
	}
	n.mu.Unlock()
	if fetch != nil {
		n.sendFetch(fetch)
	}
	if vc != nil && sendTo != n.id {
		_ = n.tr.SendViewChange(context.Background(), sendTo, vc)
	}
}

// enterViewLocked advances this replica into a strictly newer view (updating
// the current leader and dropping leader activity — a leader only becomes
// active again by gathering a quorum of view changes). Must hold n.mu.
func (n *Replica) enterViewLocked(view uint64) bool {
	if view <= n.view {
		return false
	}
	n.view = view
	n.leader = n.leaderOf(view)
	n.active = false
	return true
}

// HandleViewChange records a peer's view change. Only the leader of the target
// view counts them; once 2f+1 distinct members (including itself) have moved
// to that view, it adopts the highest reported QC and becomes active. A
// replica behind the target view joins it when a quorum has already moved.
func (n *Replica) HandleViewChange(vc *ViewChange) {
	if vc == nil || !n.members[vc.From] || n.id != n.leaderOf(vc.View) {
		return
	}
	var fetch *Fetch
	n.mu.Lock()
	if !n.verify(vc.From, vc.Sig, *vc) || vc.View < n.view {
		n.mu.Unlock()
		return // unauthenticated/tampered, or stale
	}
	if n.view < vc.View {
		n.enterViewLocked(vc.View)
	}
	n.addVC(vc)
	fetch = n.maybeActivateLocked(vc.View)
	n.mu.Unlock()
	if fetch != nil {
		n.sendFetch(fetch)
	}
}

// addVC stores one member's view change for the target view. Must hold n.mu
// and be the target view's leader.
func (n *Replica) addVC(vc *ViewChange) {
	if !n.members[vc.From] || n.id != n.leaderOf(vc.View) {
		return
	}
	set := n.vcs[vc.View]
	if set == nil {
		set = make(map[string]*ViewChange)
		n.vcs[vc.View] = set
	}
	set[vc.From] = vc
}

// maybeActivateLocked activates this replica as the leader of view when it has
// collected 2f+1 view changes for it, adopting the highest reported QC as the
// new base. Returns a Fetch when the base block must first be pulled from a
// peer (the leader has not seen it yet). Must hold n.mu.
func (n *Replica) maybeActivateLocked(view uint64) *Fetch {
	if n.id != n.leaderOf(view) || view != n.view {
		return nil
	}
	if len(n.vcs[view]) < 2*n.f+1 {
		return nil // not a quorum yet
	}
	// The highest QC is chosen only among GENUINE QCs (a Byzantine member can
	// sign a view change carrying a fabricated QC; qcValid filters those out).
	high := n.qcHigh
	for _, vc := range n.vcs[view] {
		if vc.HighQC != nil && n.qcValid(vc.HighQC) &&
			(high == nil || vc.HighQC.Height > high.Height) {
			high = vc.HighQC
		}
	}
	return n.adoptBaseLocked(high)
}

// adoptBaseLocked makes this (leader) replica adopt high as its view base: the
// head moves to the highest QC's certified block and the next proposal skips a
// height. If that block is not in the tree yet it must be fetched first. Must
// hold n.mu.
func (n *Replica) adoptBaseLocked(high *QC) *Fetch {
	if n.id != n.leader {
		return nil
	}
	if high == nil {
		n.activateWithBaseLocked(n.qcHigh)
		return nil
	}
	if n.qcHigh == nil || high.Height > n.qcHigh.Height {
		if n.blocks[high.NodeID] == nil {
			from := n.vcReporterLocked(high)
			if from == "" { // defensive: no one to fetch the base from
				n.activateWithBaseLocked(n.qcHigh)
				return nil
			}
			n.wantBase = high
			n.baseFrom = from
			if n.fetches[high.NodeID] {
				return nil
			}
			n.fetches[high.NodeID] = true
			return &Fetch{BlockID: high.NodeID, To: from}
		}
		n.qcHigh = high
		n.onNewQCLocked(high)
	}
	n.activateWithBaseLocked(n.qcHigh)
	return nil
}

// activateWithBaseLocked resumes proposing from the highest QC's certified
// block: head moves there (discarding any un-QC'd tail from the deposed
// leader) and the first new proposal skips one height above it. Must hold
// n.mu.
func (n *Replica) activateWithBaseLocked(base *QC) {
	if base != nil && base.NodeID != "" {
		n.head = base.NodeID
	}
	n.freshBase = true
	n.active = n.id == n.leader
	n.wantBase = nil
	n.baseFrom = ""
}

// vcReporterLocked returns the peer that reported the given high QC, so the
// leader can fetch the QC's certified block from it. Must hold n.mu.
func (n *Replica) vcReporterLocked(high *QC) string {
	for _, vc := range n.vcs[n.view] {
		if vc.HighQC != nil && vc.HighQC.NodeID == high.NodeID && vc.HighQC.Height == high.Height {
			return vc.From
		}
	}
	return ""
}

// completeBaseLocked is called after a block is inserted: if it was the
// missing base a leader was waiting for, adopt it. Must hold n.mu.
func (n *Replica) completeBaseLocked(id string) {
	if n.wantBase == nil || id != n.wantBase.NodeID {
		return
	}
	high := n.wantBase
	if n.qcHigh == nil || high.Height > n.qcHigh.Height {
		n.qcHigh = high
		n.onNewQCLocked(high)
	}
	n.activateWithBaseLocked(n.qcHigh)
}

// HandleFetch answers a peer's request for a block this replica holds, signing
// the reply so the requester can authenticate it.
func (n *Replica) HandleFetch(f *Fetch) {
	if f == nil || !n.members[f.From] {
		return
	}
	n.mu.Lock()
	b := n.blocks[f.BlockID]
	n.mu.Unlock()
	if b == nil {
		return
	}
	bm := &BlockMsg{Block: *b, From: n.id}
	bm.Sig = n.sign(bm)
	_ = n.tr.SendBlock(context.Background(), f.From, bm)
}

// HandleBlock inserts a block a peer sent in answer to a Fetch. If the block's
// own parent is unknown it is parked (and its parent requested); otherwise it
// is inserted, which also unblocks any proposals and parked blocks waiting on
// it — a partitioned replica catches up by pulling ancestors bottom-up.
func (n *Replica) HandleBlock(bm *BlockMsg) {
	if bm == nil || !n.members[bm.From] {
		return
	}
	var kids []*Proposal
	var needParent string
	n.mu.Lock()
	if !n.verify(bm.From, bm.Sig, *bm) {
		n.mu.Unlock()
		return // unauthenticated block reply
	}
	b := bm.Block
	if b.ID == blockID(b.View, b.Height, b.Parent, b.Cmd) && n.blocks[b.ID] == nil &&
		b.Height > n.blocks[n.bExec].Height {
		if b.Parent != "" && n.blocks[b.Parent] == nil {
			n.pendingBlocks[b.Parent] = append(n.pendingBlocks[b.Parent], b)
			needParent = b.Parent // fetch ancestors bottom-up
		} else {
			kids = n.addBlockLocked(&b, b.Parent)
		}
	}
	n.mu.Unlock()
	for _, k := range kids {
		n.HandleProposal(k)
	}
	if needParent != "" {
		n.requestBlock(bm.From, needParent)
	}
}

// requestBlock asks peer for the block with id (once; deduplicated by the
// in-flight set). Sends outside the lock.
func (n *Replica) requestBlock(peer, id string) {
	n.mu.Lock()
	if n.blocks[id] != nil || n.fetches[id] {
		n.mu.Unlock()
		return
	}
	n.fetches[id] = true
	n.mu.Unlock()
	n.sendFetch(&Fetch{BlockID: id, To: peer})
}

func (n *Replica) sendFetch(f *Fetch) {
	if f == nil || f.To == "" {
		return
	}
	_ = n.tr.SendFetch(context.Background(), f.To, &Fetch{BlockID: f.BlockID, From: n.id})
}

// SetViewTimeout arms the real suspicion timer: when no progress (a rising
// qcHigh) is observed for the interval within the current view, StartViewChange
// is fired and the interval doubles (exponential backoff) until progress
// resumes. Zero disables the timer — deterministic tests call StartViewChange
// directly instead. The goroutine stops on Stop.
func (n *Replica) SetViewTimeout(base time.Duration) {
	if base <= 0 {
		return
	}
	go func() {
		interval := base
		n.mu.Lock()
		last := n.qcHighHeightLocked()
		n.mu.Unlock()
		for {
			select {
			case <-n.stopCh:
				return
			case <-time.After(interval):
			}
			n.mu.Lock()
			now := n.qcHighHeightLocked()
			n.mu.Unlock()
			if now > last {
				interval = base // progress within the interval resets the backoff
			} else if !n.IsLeader() {
				n.StartViewChange() // no progress and not the leader: suspect
				interval *= 2
			}
			// An active leader never suspects itself — it proposes instead; the
			// caller (driver) is responsible for keeping it proposing.
			last = now
		}
	}()
}

func (n *Replica) qcHighHeightLocked() uint64 {
	if n.qcHigh == nil {
		return 0
	}
	return n.qcHigh.Height
}

// Stop halts any background timers started by SetViewTimeout.
func (n *Replica) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}
