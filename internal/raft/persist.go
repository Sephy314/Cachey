package raft

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Sephy314/Cachey/internal/wal"
)

// persistTimeout bounds how long a single log-entry durability write may take.
const persistTimeout = 5 * time.Second

// LogStore durably persists raft log entries before they are acknowledged.
// The WAL-backed implementation writes each entry as a wal.Record carrying the
// raft term and index.
type LogStore interface {
	AppendEntry(ctx context.Context, idx, term uint64, cmd []byte) error
}

// SetLogStore enables durable persistence of the replicated log. Call after
// recovery has rebuilt the in-memory log (recovery happens through the WAL's
// ApplyRecord hook, not through the log store).
func (n *Node) SetLogStore(ls LogStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logStore = ls
}

// persistEntryLocked writes e at idx before it is acknowledged. A nil log
// store means in-memory only (tests). Must be called with n.mu held.
//
// ponytail: durability writes run while the node lock is held, so a slow WAL
// stalls RPC handling. Fine for a cache with a local WAL; the upgrade path is
// a dedicated persist/apply goroutine with an acked-index watermark.
func (n *Node) persistEntryLocked(idx uint64, e Entry) error {
	if n.logStore == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	defer cancel()
	return n.logStore.AppendEntry(ctx, idx, e.Term, e.Command)
}

// walLogStore persists raft log entries through Cachey's existing WAL, mapping
// each entry to a wal.Record that reuses the PUT/DEL/TTL ops as the command.
type walLogStore struct{ w *wal.WAL }

// NewWALLogStore returns a LogStore backed by w.
func NewWALLogStore(w *wal.WAL) LogStore { return walLogStore{w: w} }

func (s walLogStore) AppendEntry(ctx context.Context, idx, term uint64, cmd []byte) error {
	var rec wal.Record
	if cmd != nil {
		if err := json.Unmarshal(cmd, &rec); err != nil {
			return err
		}
	} else {
		rec.Op = wal.OpNoop
	}
	rec.Term = term
	rec.RaftIndex = idx
	return s.w.Append(ctx, rec)
}

// ApplyRecoveredRecord rebuilds the in-memory log from a persisted record. It
// is the WAL's ApplyRecord hook during recovery: each record places an entry
// at its raft index, and a later record at the same index (a newer term)
// supersedes an older conflicting tail (see Log.set).
func (n *Node) ApplyRecoveredRecord(rec wal.Record) error {
	var cmd []byte
	if rec.Op != wal.OpNoop {
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		cmd = b
	}
	n.mu.Lock()
	n.log.set(rec.RaftIndex, Entry{Term: rec.Term, Command: cmd})
	n.mu.Unlock()
	return nil
}
