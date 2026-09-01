package raft

// Configuration is a cluster membership: the set of voting server IDs.
// Membership changes are themselves log entries carrying the full new
// configuration, applied (and effective for majorities) only once committed.
type Configuration struct {
	Voters []string `json:"voters"`
}

// Entry is one replicated log entry. Term is the term in which the leader
// received it; Command is the client mutation (a wal.Record payload) applied
// to the state machine when the entry commits. Config marks a membership
// change (its payload, not a store command); a nil Command and nil Config is
// a no-op entry used to commit previous-term entries when a new leader is
// elected.
type Entry struct {
	Term    uint64         `json:"term"`
	Command []byte         `json:"command,omitempty"`
	Config  *Configuration `json:"config,omitempty"`
}

// Log is the replicated log. entries[0] is a dummy entry at index 0 (never
// sent or applied); real entries live at indexes 1..len-1.
type Log struct {
	entries []Entry
}

func NewLog() *Log {
	return &Log{entries: []Entry{{}}} // dummy entry at index 0
}

// lastIndex returns the index of the last entry (0 for an empty log).
func (l *Log) lastIndex() uint64 { return uint64(len(l.entries) - 1) }

// lastTerm returns the term of the last entry (0 for an empty log).
func (l *Log) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// termAt returns the term of the entry at index i. Index 0 returns 0 (dummy).
func (l *Log) termAt(i uint64) uint64 { return l.entries[i].Term }

// entryAt returns the entry at index i.
func (l *Log) entryAt(i uint64) Entry { return l.entries[i] }

// append adds entries after the last index.
func (l *Log) append(es ...Entry) { l.entries = append(l.entries, es...) }

// truncate removes all entries at index >= from, keeping entries < from.
// from must be >= 1 (index 0 is never removed).
func (l *Log) truncate(from uint64) { l.entries = l.entries[:from] }

// set places e at index i, removing any existing entry at i and everything
// after it. Used when rebuilding the log from the WAL after recovery: records
// replay in WAL order and a later record at the same index (a newer term)
// supersedes an older conflicting tail. i must be >= 1.
func (l *Log) set(i uint64, e Entry) {
	if i < uint64(len(l.entries)) {
		l.entries = l.entries[:i]
	}
	l.entries = append(l.entries, e)
}

// slice returns entries from index from (inclusive) to the end.
func (l *Log) slice(from uint64) []Entry { return l.entries[from:] }
