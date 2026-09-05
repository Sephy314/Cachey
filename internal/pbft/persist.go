package pbft

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Sephy314/Cachey/internal/wal"
)

// This file implements durable persistence of a replica's ordered requests
// (M4), mirroring how Raft persists its log through Cachey's WAL: every request
// this replica accepts is durably written before it is acted on, and recovery
// replays the WAL to rebuild the in-memory log.
//
// What survives a crash is the replica's KNOWLEDGE of the requests it accepted
// (their view, sequence number and content) plus its executed watermark, so it
// does not forget an accepted request or re-execute below its watermark. What
// does NOT survive is the volatile protocol state — prepared/commit
// certificates, pending view changes — which a restarted replica regains by
// re-participating. Catching a replica up on requests it never received (it was
// partitioned during the crash) needs state transfer, which is out of scope
// until checkpoints exist (ponytail: same ceiling as the rest of M2).

// persistTimeout bounds a single log-entry durability write.
const persistTimeout = 5 * time.Second

// LogStore durably persists this replica's consensus state. The WAL-backed
// implementation writes each request and each executed-watermark advance as a
// wal.Record with Op wal.OpPBFT.
type LogStore interface {
	// AppendRequest persists one accepted request at (view, seq).
	AppendRequest(ctx context.Context, view, seq uint64, req Request) error
	// AppendApplied persists that the replica has executed through lastExec.
	AppendApplied(ctx context.Context, lastExec uint64) error
}

// persistKind discriminates the two PBFT WAL payloads.
type persistKind string

const (
	pkRequest persistKind = "request"
	pkApplied persistKind = "applied"
)

// persistEntry is the serialized PBFT WAL payload.
type persistEntry struct {
	Kind persistKind `json:"kind"`
	View uint64      `json:"view,omitempty"`
	Seq  uint64      `json:"seq"`
	Req  Request     `json:"req,omitempty"`
}

// SetLogStore enables durable persistence of the ordered log. Call after
// recovery has rebuilt the in-memory log (recovery happens through the WAL's
// ApplyRecord hook, not through the log store).
func (n *Replica) SetLogStore(ls LogStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logStore = ls
}

// persistRequestLocked durably writes an accepted request before it is acted
// on. A nil log store means in-memory only (tests). Must be called with n.mu
// held.
func (n *Replica) persistRequestLocked(view, seq uint64, req Request) error {
	if n.logStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	return n.logStore.AppendRequest(ctx, view, seq, req)
}

// persistAppliedLocked durably records an executed-watermark advance so a
// restarted replica does not re-execute below it. Best-effort: a failed
// watermark write only delays re-execution safety on a later crash, never
// affects correctness in this run. Must be called with n.mu held.
func (n *Replica) persistAppliedLocked(lastExec uint64) {
	if n.logStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	if err := n.logStore.AppendApplied(ctx, lastExec); err != nil {
		log.Printf("pbft[%s]: persist applied watermark %d: %v", n.id, lastExec, err)
	}
}

// walLogStore persists the ordered log through Cachey's existing WAL.
type walLogStore struct{ w *wal.WAL }

// NewWALLogStore returns a LogStore backed by w.
func NewWALLogStore(w *wal.WAL) LogStore { return walLogStore{w: w} }

func (s walLogStore) AppendRequest(ctx context.Context, view, seq uint64, req Request) error {
	pe, err := json.Marshal(persistEntry{Kind: pkRequest, View: view, Seq: seq, Req: req})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpPBFT, Data: pe})
}

func (s walLogStore) AppendApplied(ctx context.Context, lastExec uint64) error {
	pe, err := json.Marshal(persistEntry{Kind: pkApplied, Seq: lastExec})
	if err != nil {
		return err
	}
	return s.w.Append(ctx, wal.Record{Op: wal.OpPBFT, Data: pe})
}

// ApplyRecoveredRecord rebuilds the in-memory log from a persisted PBFT record.
// It is the WAL's ApplyRecord hook during recovery. Non-PBFT records (e.g. from
// a WAL shared with the store) are ignored, so the same hook is safe in a
// dispatcher.
func (n *Replica) ApplyRecoveredRecord(rec wal.Record) error {
	if rec.Op != wal.OpPBFT {
		return nil
	}
	var pe persistEntry
	if err := json.Unmarshal(rec.Data, &pe); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	switch pe.Kind {
	case pkApplied:
		// The executed watermark: everything at or below lastExec already ran.
		if last := pe.Seq + 1; last > n.nextExec {
			n.nextExec = last
		}
		if pe.Seq > n.lastAssigned {
			n.lastAssigned = pe.Seq
		}
	case pkRequest:
		if pe.Seq == 0 {
			return nil
		}
		d := digestOf(pe.Req)
		e := n.newEntry(pe.View, pe.Seq, d, pe.Req)
		e.prePrepared = true // the replica had accepted the order before the crash
		n.log[pe.Seq] = e
		n.seen[reqKey{pe.Req.Client, pe.Req.Timestamp}] = d
		// A (re-)elected primary must never reuse a sequence number it already
		// handed out: continue from the highest recovered sequence.
		if pe.Seq > n.lastAssigned {
			n.lastAssigned = pe.Seq
		}
	default:
		// ignore unknown kinds
	}
	return nil
}
