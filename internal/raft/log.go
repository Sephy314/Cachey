package raft

// Configuration is a cluster membership: the set of voting server IDs.
// Membership changes are themselves log entries carrying the full new
// configuration, applied (and effective for majorities) only once committed.
type Configuration struct {
	Voters []string `json:"voters"`
	// Addrs carries the transport addresses of members introduced by this
	// change. Only the leader that proposes the change knows the new member's
	// address; shipping it in the configuration lets every node register the
	// address when it applies the change, so any node can still reach the new
	// member after a leadership change (otherwise the member is orphaned: a
	// later leader cannot replicate to or request votes from it).
	Addrs map[string]string `json:"addrs,omitempty"`
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

// Log is the replicated log with a compaction base offset. entries[0] is a
// dummy entry at index base (never sent or applied); real entries live at
// indexes base+1..lastIndex. base advances when the log is compacted after a
// snapshot (Raft §7).
type Log struct {
	base    uint64 // last included index (compaction point)
	entries []Entry
}

func NewLog() *Log {
	return &Log{entries: []Entry{{}}} // dummy entry at index 0
}

// lastIndex returns the index of the last entry (base for an empty tail).
func (l *Log) lastIndex() uint64 { return l.base + uint64(len(l.entries)) - 1 }

// lastTerm returns the term of the last entry.
func (l *Log) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// baseIndex returns the compaction base (last included index).
func (l *Log) baseIndex() uint64 { return l.base }

// termAt returns the term of the entry at index i. Must have i >= l.base.
func (l *Log) termAt(i uint64) uint64 { return l.entries[i-l.base].Term }

// entryAt returns the entry at index i. Must have i >= l.base.
func (l *Log) entryAt(i uint64) Entry { return l.entries[i-l.base] }

// append adds entries after the last index.
func (l *Log) append(es ...Entry) { l.entries = append(l.entries, es...) }

// truncate removes all entries at index >= from, keeping entries < from.
// Must have from >= l.base.
func (l *Log) truncate(from uint64) { l.entries = l.entries[:from-l.base] }

// set places e at index i, removing any existing entry at i and everything
// after it. Used when rebuilding the log from the WAL after recovery: records
// replay in WAL order and a later record at the same index (a newer term)
// supersedes an older conflicting tail. Records at or before the base (already
// covered by a snapshot) are ignored.
func (l *Log) set(i uint64, e Entry) {
	if i < l.base {
		return // covered by a snapshot
	}
	if i < l.base+uint64(len(l.entries)) {
		l.entries = l.entries[:i-l.base]
	}
	l.entries = append(l.entries, e)
}

// slice returns entries from index from (inclusive) to the end. Must have
// from >= l.base.
func (l *Log) slice(from uint64) []Entry { return l.entries[from-l.base:] }

// compact truncates the log up to index lastIncluded, keeping a dummy entry
// at lastIncluded with lastIncludedTerm as the new base.
func (l *Log) compact(lastIncluded, lastIncludedTerm uint64) {
	if lastIncluded >= l.lastIndex() {
		l.entries = []Entry{{Term: lastIncludedTerm}}
	} else {
		l.entries = append([]Entry{{Term: lastIncludedTerm}}, l.entries[lastIncluded-l.base+1:]...)
	}
	l.base = lastIncluded
}

// reset discards the entire log and rebases at index i with term t (used when
// installing a snapshot that supersedes the whole log).
func (l *Log) reset(i, t uint64) {
	l.entries = []Entry{{Term: t}}
	l.base = i
}
