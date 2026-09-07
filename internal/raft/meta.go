package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// CommittedMeta is the durable record of the most recent COMMITTED
// configuration. It is persisted whenever a membership change applies (see
// Node.SetMetaStore) so a node that restarts after its log was compacted —
// when the config entries themselves were compacted away — can still rejoin
// the cluster as a full member instead of believing it is a 1-node cluster.
//
// Index is the raft-log index of the configuration entry this record reflects.
// Recovery compares it with the newest configuration recovered from the WAL
// and adopts the meta only when it is not older, so a stale meta file (e.g. a
// crash between a config committing on this node and the meta file write) can
// never overwrite a newer committed configuration.
//
// Raft §3.5 requires persisting currentTerm, votedFor, and the log. This raft
// persists the log (via the WAL) and the FSM (via snapshots); CommittedMeta
// closes the membership gap. currentTerm/votedFor are still in-memory only
// (see the note in persist.go) — a crash mid-term can in principle let a
// node re-vote in the same term, which this cache tolerates.
type CommittedMeta struct {
	Voters []string          `json:"voters"`
	Addrs  map[string]string `json:"addrs,omitempty"`
	Index  uint64            `json:"index,omitempty"`
}

// MetaStore persists the committed membership so recovery can restore it.
type MetaStore interface {
	Save(CommittedMeta) error
	Load() (CommittedMeta, bool, error)
}

// FileMetaStore persists a single committed-config record as JSON in dir.
type FileMetaStore struct {
	path string
}

// NewFileMetaStore creates a meta store rooted at dir. It must be called
// before Run (and any config change is applied).
func NewFileMetaStore(dir string) *FileMetaStore {
	return &FileMetaStore{path: filepath.Join(dir, "raft.meta")}
}

func (s *FileMetaStore) Save(m CommittedMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileMetaStore) Load() (CommittedMeta, bool, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return CommittedMeta{}, false, nil
		}
		return CommittedMeta{}, false, err
	}
	var m CommittedMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return CommittedMeta{}, false, err
	}
	return m, true, nil
}

// SetMetaStore enables durable persistence of the committed membership. Call
// before Run so every applied membership change is recorded.
func (n *Node) SetMetaStore(ms MetaStore) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.metaStore = ms
}

// AdoptCommittedMeta restores the durable committed membership (persisted on
// every applied config) into a node that just recovered from disk. Call after
// log/WAL recovery and before Run. It overrides whatever the (possibly
// compacted) log replay produced, so a member that restarted after compaction
// still knows the cluster and its peers' addresses.
//
// The meta is adopted only when it is NOT older than the newest configuration
// recovered from the WAL (recoveredConfigIndex). A stale meta file — the crash
// window between a config committing on this node and the meta file write —
// must not undo a newer committed configuration.
func (n *Node) AdoptCommittedMeta(m CommittedMeta) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.recoveredConfigIndex != 0 && m.Index < n.recoveredConfigIndex {
		n.logf("keeping WAL-recovered committed config (index %d) over stale meta (index %d)",
			n.recoveredConfigIndex, m.Index)
		return
	}
	var peers []string
	included := false
	for _, id := range m.Voters {
		if id == n.id {
			included = true
			continue
		}
		peers = append(peers, id)
	}
	n.peers = peers
	n.removed = !included
	if pr, ok := n.tr.(PeerRegistrar); ok {
		for id, addr := range m.Addrs {
			if addr != "" {
				pr.RegisterPeer(id, addr)
			}
		}
	}
}
