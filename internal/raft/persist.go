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
	AppendEntry(ctx context.Context, idx, term uint64, entry Entry) error
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
	return n.logStore.AppendEntry(ctx, idx, e.Term, e)
}

// walLogStore persists raft log entries through Cachey's existing WAL: data
// entries reuse the PUT/DEL/TTL ops as the command, no-ops use OpNoop, and
// membership changes use OpConfig with the serialized configuration.
type walLogStore struct{ w *wal.WAL }

// NewWALLogStore returns a LogStore backed by w.
func NewWALLogStore(w *wal.WAL) LogStore { return walLogStore{w: w} }

func (s walLogStore) AppendEntry(ctx context.Context, idx, term uint64, entry Entry) error {
	var rec wal.Record
	switch {
	case entry.Config != nil:
		cfg, err := json.Marshal(entry.Config)
		if err != nil {
			return err
		}
		rec.Op = wal.OpConfig
		rec.Config = cfg
	case entry.Command != nil:
		if err := json.Unmarshal(entry.Command, &rec); err != nil {
			return err
		}
	default:
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
	var entry Entry
	switch rec.Op {
	case wal.OpConfig:
		var cfg Configuration
		if err := json.Unmarshal(rec.Config, &cfg); err != nil {
			return err
		}
		entry.Config = &cfg
	case wal.OpNoop:
		// entry stays empty
	default:
		cmd, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		entry.Command = cmd
	}
	entry.Term = rec.Term
	n.mu.Lock()
	n.log.set(rec.RaftIndex, entry)
	// Adopt the voter set from the last config entry in the recovered log
	// (n.peers stores only the other voters).
	// ponytail: this assumes the last config entry was committed; an uncommitted
	// tail is corrected by the live leader's LeaderCommit. Persisting the
	// committed config separately (with term/votedFor) is the upgrade path.
	if entry.Config != nil {
		var peers []string
		for _, id := range entry.Config.Voters {
			if id != n.id {
				peers = append(peers, id)
			}
		}
		n.peers = peers
	}
	n.mu.Unlock()
	return nil
}
