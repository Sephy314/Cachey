package hotstuff

import (
	"sort"
)

// This file implements Phase 2.5 GC / compaction: long-running HotStuff
// storage is bounded by deleting committed blocks below a retention boundary
// and compacting the shared WAL, while preserving crash recovery, consensus
// safety, historical QC validation, ValidatorSet validation and catch-up.
//
// GC boundary. The boundary is NOT "latest committed height" alone: it is
// bExec (the executed prefix head) minus a retention window, so a node can
// still serve near-behind catch-up requests. Blocks strictly below the
// boundary are garbage; the committed prefix from the boundary up to bExec,
// every block above bExec (the uncommitted tail, including forks), genesis,
// the ValidatorSet history, qcHigh/bLock/head and the vote-height guard all
// remain.
//
// Why the retained set is sufficient:
//
//	restart/recovery          the checkpoint (below) + the compacted WAL tail
//	                          rebuild the exact retained state; recovery is
//	                          idempotent whether or not the rotation completed.
//	latest valid checkpoint   the store snapshot (taken by the WAL rotation)
//	                          plus this engine checkpoint.
//	still-relevant QCs        qcHigh and every justification QC ride along with
//	                          the retained blocks; older QCs are below the
//	                          boundary and no longer referenced by any valid
//	                          proposal (proposals chain onto the lock, which is
//	                          at/above bExec).
//	historical ValidatorSets  n.sets is never GC'd — it is small (one set per
//	                          epoch) and required to validate QCs from any past
//	                          epoch, which may still arrive (e.g. in a catch-up
//	                          batch or a view-change HighQC).
//	continuing consensus      the chain from the boundary up to head is intact,
//	                          so epoch derivation, safeNode, lock/commit rules
//	                          and voting all work unchanged.
//	continuing catch-up       the node serves range requests for retained
//	                          blocks (within the retention window below bExec
//	                          and the whole tail above it) and can itself catch
//	                          up from its retained state.
//
// Crash-safe ordering (driven by the server): the engine computes the
// checkpoint and deletes in-memory garbage (GC), the server writes the
// checkpoint file atomically (temp + fsync + rename), and only then rotates
// the WAL (store snapshot + truncation). A crash at any point recovers
// correctly: before the checkpoint is durable nothing changed; after it is
// durable the WAL replay is idempotent on top of the checkpoint whether or
// not the rotation ran.

// defaultGCRetention is the number of committed blocks below bExec retained
// for near-behind catch-up serving. Blocks below bExec-retention are garbage.
const defaultGCRetention = 100

// Checkpoint captures the retained engine state for crash-safe GC. It is
// written durably (by the server) before the WAL is compacted, and loaded on
// recovery so the WAL tail replays on top of it.
type Checkpoint struct {
	GCBase  string                   `json:"gc_base"`  // lowest retained block id
	BExec   string                   `json:"b_exec"`   // executed prefix head
	BLock   string                   `json:"b_lock"`   // locked block id
	Head    string                   `json:"head"`     // newest block
	QC      *QC                      `json:"qc"`       // highest known QC
	Epoch   uint64                   `json:"epoch"`    // current epoch
	VHeight uint64                   `json:"v_height"` // vote-height guard
	Sets    map[uint64]*ValidatorSet `json:"sets"`     // full ValidatorSet history
	Blocks  []Block                  `json:"blocks"`   // retained blocks
}

// GC computes the GC boundary, snapshots the retained state into a
// Checkpoint, and deletes garbage blocks from the in-memory tree. It returns
// the checkpoint (which the caller must write durably before compacting the
// WAL) and whether anything was deleted. The in-memory deletion is safe even
// if the caller never persists the checkpoint: the WAL still holds every
// record until the rotation, so recovery is unaffected.
func (n *Replica) GC() (Checkpoint, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.gcLocked()
}

func (n *Replica) gcLocked() (Checkpoint, bool, error) {
	exec := n.blocks[n.bExec]
	if exec == nil {
		return Checkpoint{}, false, nil // defensive: no executed prefix
	}
	// GC base: the block on the committed prefix at height
	// max(0, bExec.Height - retention).
	retention := n.gcRetention
	target := exec.Height
	if target > retention {
		target -= retention
	} else {
		target = 0
	}
	base := exec
	for base != nil && base.Height > target {
		base = n.blocks[base.Parent]
	}
	if base == nil {
		base = n.blocks[genesisID] // defensive
	}
	if base == nil {
		return Checkpoint{}, false, nil
	}
	if base.ID == n.gcBase {
		return Checkpoint{}, false, nil // nothing new to delete
	}
	// Retained set: the committed prefix from base up to bExec, every block at
	// height >= bExec.Height (the uncommitted tail, including forks), and
	// genesis (always — the tree's root and the recovery fallback).
	retained := make(map[string]bool, len(n.blocks))
	for b := exec; b != nil && b.Height >= base.Height; b = n.blocks[b.Parent] {
		retained[b.ID] = true
	}
	for id, b := range n.blocks {
		if b.Height >= exec.Height {
			retained[id] = true
		}
	}
	retained[genesisID] = true
	// Collect the retained blocks (deterministic order: height, then id).
	blocks := make([]Block, 0, len(retained))
	for id := range retained {
		if b := n.blocks[id]; b != nil {
			blocks = append(blocks, *b)
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Height != blocks[j].Height {
			return blocks[i].Height < blocks[j].Height
		}
		return blocks[i].ID < blocks[j].ID
	})
	// Deep-copy the ValidatorSet history: the replica mutates n.sets (new
	// epochs) while the caller serializes the checkpoint.
	sets := make(map[uint64]*ValidatorSet, len(n.sets))
	for e, s := range n.sets {
		sets[e] = s
	}
	cp := Checkpoint{
		GCBase: base.ID, BExec: n.bExec, BLock: n.bLock, Head: n.head,
		QC: n.qcHigh, Epoch: n.epoch, VHeight: n.vHeight,
		Sets: sets, Blocks: blocks,
	}
	// Delete garbage: blocks not in the retained set. Their per-block
	// bookkeeping (votes, QC-formed marks, applied marks, waiters, pending
	// proposals/blocks, in-flight fetches) goes with them.
	for id, b := range n.blocks {
		if retained[id] {
			continue
		}
		delete(n.blocks, id)
		delete(n.votes, id)
		delete(n.qcFormed, id)
		delete(n.applied, id)
		if ch := n.notify[id]; ch != nil {
			close(ch) // the block is committed (below bExec); waiters resolve
			delete(n.notify, id)
		}
		delete(n.pending, id)
		delete(n.pendingBlocks, id)
		delete(n.fetches, id)
		_ = b
	}
	n.gcBase = base.ID
	return cp, true, nil
}

// LoadCheckpoint restores the retained engine state from a checkpoint before
// the WAL replay. Records at or before the checkpoint are then idempotently
// skipped (blocks dedup by id, QCs below qcHigh are dropped, the watermark
// and vote-height guards are max-height), so recovery is correct whether the
// WAL was compacted or not.
func (n *Replica) LoadCheckpoint(cp Checkpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocks = make(map[string]*Block, len(cp.Blocks)+1)
	for i := range cp.Blocks {
		b := cp.Blocks[i]
		n.blocks[b.ID] = &b
	}
	if n.blocks[genesisID] == nil {
		g, _ := genesisBlock(n.all)
		n.blocks[genesisID] = g // defensive: genesis is always retained
	}
	n.gcBase = cp.GCBase
	if n.gcBase == "" || n.blocks[n.gcBase] == nil {
		n.gcBase = genesisID
	}
	n.qcHigh = cp.QC
	n.head = cp.Head
	n.bExec = cp.BExec
	n.bLock = cp.BLock
	n.epoch = cp.Epoch
	n.vHeight = cp.VHeight
	if cp.Sets != nil {
		n.sets = cp.Sets
	}
	// Mark the executed prefix applied so WaitCommitted resolves and applyFn
	// never re-runs it.
	for b := n.blocks[n.bExec]; b != nil; b = n.blocks[b.Parent] {
		n.markAppliedLocked(b.ID)
		if b.Parent == "" {
			break
		}
	}
}