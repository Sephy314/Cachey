package raft

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

// Snapshot captures the state machine at a compaction point.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte // serialized store state
}

// SnapshotStore persists snapshots so recovery can skip replayed history.
type SnapshotStore interface {
	Save(Snapshot) error
	Load() (Snapshot, bool, error)
}

// FileSnapshotStore persists a single snapshot as JSON in dir.
type FileSnapshotStore struct {
	path string
}

// NewFileSnapshotStore creates a snapshot store rooted at dir.
func NewFileSnapshotStore(dir string) *FileSnapshotStore {
	return &FileSnapshotStore{path: filepath.Join(dir, "raft.snapshot")}
}

func (s *FileSnapshotStore) Save(snap Snapshot) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileSnapshotStore) Load() (Snapshot, bool, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

// SetSnapshotStore enables snapshot persistence. Call before Run.
func (n *Node) SetSnapshotStore(ss SnapshotStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotStore = ss
}

// SetSnapshotCallbacks wires the node to the state machine's snapshot methods:
// take serializes the current store state, install replaces it. Call before
// Run (and before RestoreSnapshot on recovery).
func (n *Node) SetSnapshotCallbacks(take func() ([]byte, error), install func([]byte) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.takeSnapshotFn = take
	n.applySnapshotFn = install
}

// RestoreSnapshot loads a persisted snapshot into the node at startup (before
// the WAL is replayed): it applies the state to the FSM and rebases the log so
// records at or before the snapshot index are skipped.
func (n *Node) RestoreSnapshot(snap Snapshot) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.applySnapshotFn != nil {
		if err := n.applySnapshotFn(snap.Data); err != nil {
			return err
		}
	}
	n.log.reset(snap.LastIncludedIndex, snap.LastIncludedTerm)
	if n.commitIndex < snap.LastIncludedIndex {
		n.commitIndex = snap.LastIncludedIndex
	}
	if n.lastApplied < snap.LastIncludedIndex {
		n.lastApplied = snap.LastIncludedIndex
	}
	return nil
}

// HandleInstallSnapshot processes a snapshot from a leader (Raft §7). Returns
// a reply carrying the current term so the leader can step down if stale.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshot) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.role = RoleFollower
	n.leaderID = args.LeaderID
	n.notifyRoleLocked()
	n.resetElectionTimer()

	// A snapshot at or behind what we've already applied is redundant.
	if args.LastIncludedIndex <= n.lastApplied {
		reply.Term = n.currentTerm
		return reply
	}
	if n.applySnapshotFn != nil {
		if err := n.applySnapshotFn(args.Data); err != nil {
			n.logf("install snapshot failed: %v", err)
			return reply
		}
	}
	if n.snapshotStore != nil {
		if err := n.snapshotStore.Save(Snapshot{
			LastIncludedIndex: args.LastIncludedIndex,
			LastIncludedTerm:  args.LastIncludedTerm,
			Data:              args.Data,
		}); err != nil {
			n.logf("persist installed snapshot failed: %v", err)
		}
	}
	// Rebase: if the log conflicts at the snapshot point, discard it; otherwise
	// keep the matching tail.
	if args.LastIncludedIndex > n.log.lastIndex() || n.log.termAt(args.LastIncludedIndex) != args.LastIncludedTerm {
		n.log.reset(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		n.log.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	n.lastApplied = args.LastIncludedIndex
	reply.Term = n.currentTerm
	return reply
}

// sendSnapshot installs the leader's current snapshot on a lagging follower.
func (n *Node) sendSnapshot(peer string) {
	n.mu.Lock()
	if n.role != RoleLeader || n.snapshotStore == nil {
		n.mu.Unlock()
		return
	}
	snap, ok, err := n.snapshotStore.Load()
	if err != nil || !ok {
		n.mu.Unlock()
		return
	}
	args := &InstallSnapshot{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	}
	n.mu.Unlock()

	reply, err := n.tr.SendInstallSnapshot(context.Background(), peer, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.stepDownLocked(reply.Term)
		return
	}
	if n.role != RoleLeader || reply.Term < n.currentTerm {
		return
	}
	// After installing the snapshot the follower has everything through
	// LastIncludedIndex; resume log replication from the next index.
	if snap.LastIncludedIndex+1 > n.matchIndex[peer] {
		n.matchIndex[peer] = snap.LastIncludedIndex
		n.nextIndex[peer] = snap.LastIncludedIndex + 1
	}
	n.updateCommitIndexLocked()
}

// maybeCompactLocked snapshots and truncates the log once it exceeds the
// configured threshold, bounding memory and speeding recovery.
//
// ponytail: the on-disk WAL files are not truncated here (rotation stays
// disabled); recovery is fast because the snapshot skips the prefix. Truly
// bounded disk usage needs raft-aware WAL rotation/compaction (upgrade path).
func (n *Node) maybeCompactLocked() {
	if n.snapshotThreshold <= 0 {
		return
	}
	if n.log.lastIndex()-n.log.baseIndex() < n.snapshotThreshold {
		return
	}
	idx := n.lastApplied
	if idx <= n.log.baseIndex() {
		return
	}
	term := n.log.termAt(idx)
	var data []byte
	if n.takeSnapshotFn != nil {
		var err error
		data, err = n.takeSnapshotFn()
		if err != nil {
			n.logf("snapshot failed: %v", err)
			return
		}
	}
	if n.snapshotStore != nil {
		if err := n.snapshotStore.Save(Snapshot{LastIncludedIndex: idx, LastIncludedTerm: term, Data: data}); err != nil {
			n.logf("persist snapshot failed: %v", err)
			return
		}
	}
	n.log.compact(idx, term)
}
