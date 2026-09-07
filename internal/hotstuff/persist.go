package hotstuff

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Sephy314/Cachey/internal/wal"
)

// This file implements durable persistence of a replica's consensus knowledge
// (HS-M4), mirroring how PBFT persists its ordered log through Cachey's WAL:
// every accepted block and every QC the replica learns is durably written
// before it is acted on, and recovery replays the WAL to rebuild the tree,
// the watermark and the vote-height guard.
//
// What survives a crash is:
//
//	pkBlock  every accepted block (its Justify QC rides along, so the tree
//	         preserves QC knowledge),
//	pkQC     every quorum-certificate raise (a leader's vote-aggregated QC is
//	         not embedded in any block, so raises are recorded separately),
//	pkApplied the executed watermark — the last block applied — so a restarted
//	         replica never re-executes below it (idempotent recovery),
//	pkVoted  the highest height voted for, so a restarted replica never votes
//	         twice for one height (the QC-uniqueness invariant).
//
// What does NOT survive is volatile protocol state — pending proposals,
// collected votes, view-change bookkeeping — which a restarted replica regains
// by re-participating, exactly as PBFT M4 does with its prepared/commit
// certificates.
//
// ponytail ceiling (same as PBFT M4): the watermark writes are best-effort — a
// failed write logs and does not abort the handler, so a later crash *could*
// re-execute the last span. Real deployments would make durability failures
// fatal or retried. Also, WAL growth is unbounded (no engine-level snapshot);
// compaction is a post-M5 concern.

// persistTimeout bounds a single durable write.
const persistTimeout = 5 * time.Second

// LogStore durably persists this replica's consensus state. The WAL-backed
// implementation writes each record as a wal.Record with Op wal.OpHotStuff.
type LogStore interface {
	// AppendBlock persists one accepted block (its Justify QC rides along).
	AppendBlock(ctx context.Context, b Block) error
	// AppendQC persists that the replica learned the QC qc (a qcHigh raise).
	AppendQC(ctx context.Context, qc *QC) error
	// AppendApplied persists that the replica has executed through execID.
	AppendApplied(ctx context.Context, execID string) error
	// AppendVoteHeight persists that the replica voted at (up to) height h.
	AppendVoteHeight(ctx context.Context, h uint64) error
}

// persistKind discriminates the HotStuff WAL payloads.
type persistKind string

const (
	pkBlock   persistKind = "block"
	pkQC      persistKind = "qc"
	pkApplied persistKind = "applied"
	pkVoted   persistKind = "voted"
)

// persistEntry is the serialized HotStuff WAL payload.
type persistEntry struct {
	Kind   persistKind `json:"kind"`
	Block  Block       `json:"block,omitempty"`
	QC     *QC         `json:"qc,omitempty"`
	ExecID string      `json:"exec,omitempty"`
	VotedH uint64      `json:"voted_h,omitempty"`
}

// SetLogStore enables durable persistence. Call after recovery has rebuilt the
// in-memory state (records replay through the WAL's ApplyRecord hook, not
// through the log store).
func (n *Replica) SetLogStore(ls LogStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logStore = ls
}

// persistLocked durably writes one record and reports failure. A nil log store
// means in-memory only (tests), which never fails. Must be called with n.mu
// held.
func (n *Replica) persistLocked(pe persistEntry) error {
	if n.logStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	var err error
	switch pe.Kind {
	case pkBlock:
		err = n.logStore.AppendBlock(ctx, pe.Block)
	case pkQC:
		err = n.logStore.AppendQC(ctx, pe.QC)
	case pkApplied:
		err = n.logStore.AppendApplied(ctx, pe.ExecID)
	case pkVoted:
		err = n.logStore.AppendVoteHeight(ctx, pe.VotedH)
	}
	return err
}

// persistRecordLocked writes a record best-effort (blocks/QCs/watermark): a
// failed write is logged and the handler proceeds. Losing one on a later crash
// only means a re-join, never a safety break. Must hold n.mu.
func (n *Replica) persistRecordLocked(pe persistEntry) {
	if err := n.persistLocked(pe); err != nil {
		log.Printf("hotstuff[%s]: persist %s: %v", n.id, pe.Kind, err)
	}
}

// persistVotedLocked durably records a vote-height advance. Unlike the
// best-effort records, the vote-once guard MUST be durable before the matching
// vote leaves this replica: a vote cast at height h that was never durably
// recorded could be re-cast at h after a crash, breaking QC uniqueness. On
// failure it logs and returns the error; the caller must then suppress the
// vote (the in-memory vHeight stays advanced, so this run never double-votes
// either). Must hold n.mu.
func (n *Replica) persistVotedLocked(h uint64) error {
	if err := n.persistLocked(persistEntry{Kind: pkVoted, VotedH: h}); err != nil {
		log.Printf("hotstuff[%s]: persist voted height %d: %v", n.id, h, err)
		return err
	}
	return nil
}

// walLogStore persists through Cachey's existing WAL.
type walLogStore struct{ w *wal.WAL }

// NewWALLogStore returns a LogStore backed by w.
func NewWALLogStore(w *wal.WAL) LogStore { return walLogStore{w: w} }

func marshalEntry(pe persistEntry) ([]byte, error) { return json.Marshal(pe) }

func (s walLogStore) AppendBlock(ctx context.Context, b Block) error {
	pe, err := marshalEntry(persistEntry{Kind: pkBlock, Block: b})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpHotStuff, Data: pe})
}

func (s walLogStore) AppendQC(ctx context.Context, qc *QC) error {
	pe, err := marshalEntry(persistEntry{Kind: pkQC, QC: qc})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpHotStuff, Data: pe})
}

func (s walLogStore) AppendApplied(ctx context.Context, execID string) error {
	pe, err := marshalEntry(persistEntry{Kind: pkApplied, ExecID: execID})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpHotStuff, Data: pe})
}

func (s walLogStore) AppendVoteHeight(ctx context.Context, h uint64) error {
	pe, err := marshalEntry(persistEntry{Kind: pkVoted, VotedH: h})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpHotStuff, Data: pe})
}

// ApplyRecoveredRecord rebuilds in-memory state from one persisted HotStuff
// record. It is the WAL's ApplyRecord hook during recovery. Records are
// replayed in append order; derived pointers (qcHigh/head/lock/exec) are
// recomputed by FinishRecovery once replay is complete. Non-HotStuff records
// are ignored, so the same hook is safe in a dispatcher.
func (n *Replica) ApplyRecoveredRecord(rec wal.Record) error {
	if rec.Op != wal.OpHotStuff {
		return nil
	}
	var pe persistEntry
	if err := json.Unmarshal(rec.Data, &pe); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	switch pe.Kind {
	case pkBlock:
		// Structural trust: the block was validated (signature, QC, safeNode)
		// when it was accepted. Re-check only content-addressed identity and
		// parent presence; no QC/signature re-verification (peer keys may not
		// be wired yet during recovery). Parent-first append order means the
		// parent is already replayed.
		b := pe.Block
		if b.ID == genesisID || b.ID != blockID(b.View, b.Height, b.Parent, b.Cmd) {
			return nil // genesis is bootstrap; anything with a bad id is corrupt
		}
		if b.Parent != "" && n.blocks[b.Parent] == nil {
			return nil // corrupt or out of order; the block cannot be anchored
		}
		if n.blocks[b.ID] != nil {
			return nil // duplicate delivery (idempotent recovery)
		}
		n.blocks[b.ID] = &b
	case pkQC:
		// QCs that raised qcHigh are replayed in order by FinishRecovery.
		if pe.QC != nil {
			n.recoverQCs = append(n.recoverQCs, pe.QC)
		}
	case pkApplied:
		// Authoritative executed watermark; FinishRecovery folds it with the
		// QC-derived prefix (taking the higher of the two).
		if n.blocks[pe.ExecID] != nil {
			n.recoverExec = pe.ExecID
		}
	case pkVoted:
		// Vote heights are strictly monotone: restore the highest.
		if pe.VotedH > n.vHeight {
			n.vHeight = pe.VotedH
		}
	}
	return nil
}

// FinishRecovery recomputes the derived protocol pointers after the WAL replay
// has rebuilt the block tree: qcHigh/head from the highest recovered QC, the
// lock/executed prefix by replaying the recovered QCs through the structural
// chain rules, and the applied set up to the executed watermark. It never
// re-runs applyFn — recovery is idempotent: a command executed before the
// crash is marked applied, not executed again.
func (n *Replica) FinishRecovery() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.finishRecoveryLocked()
}

func (n *Replica) finishRecoveryLocked() {
	// Recovered QCs were persisted in raise order, so they are strictly
	// increasing in height; fold them in order through the structural rules.
	// (Persisted QCs were validated when accepted — no signature re-check.)
	qcHigh := n.qcHigh // genesis QC floor
	lock := n.blocks[n.bLock]
	exec := n.blocks[n.bExec]
	for _, qc := range n.recoverQCs {
		if qc == nil || qc.Height <= qcHigh.Height {
			continue // defensive: stale or duplicate raise
		}
		qcHigh = qc
		c := n.blocks[qc.NodeID]
		if c == nil {
			continue // certified block not in the tree
		}
		parent := n.blocks[c.Parent]
		if parent == nil || !oneChainedBy(parent, c) {
			continue
		}
		if parent.Height > lock.Height {
			lock = parent
		}
		if gp := n.blocks[parent.Parent]; gp != nil && oneChainedBy(gp, parent) && gp.Height > exec.Height {
			exec = gp
		}
	}
	// The executed watermark is authoritative and may cover a commit whose QC
	// raise was itself persisted — the two agree; take the higher defensively.
	if n.recoverExec != "" {
		if wm := n.blocks[n.recoverExec]; wm != nil && wm.Height > exec.Height {
			exec = wm
		}
	}
	if _, ok := n.blocks[qcHigh.NodeID]; !ok {
		qcHigh = n.qcHigh // defensive: resume from genesis
		exec = n.blocks[genesisID]
	}
	n.qcHigh = qcHigh
	n.head = qcHigh.NodeID
	n.bLock = lock.ID
	n.bExec = exec.ID
	// Everything at or below the executed prefix has already been applied —
	// mark it so WaitCommitted resolves and applyFn never re-runs it.
	for b := exec; b != nil && b.ID != ""; b = n.blocks[b.Parent] {
		n.markAppliedLocked(b.ID)
		if b.Parent == "" {
			break
		}
	}
	n.recoverQCs = nil
	n.recoverExec = ""
}
