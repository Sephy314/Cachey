package raft

// RequestVote is the candidate's vote request (Raft §5.2).
type RequestVote struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply is the voter's response.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntries replicates log entries / acts as a heartbeat (Raft §5.3).
type AppendEntries struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesReply is the follower's response.
type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// InstallSnapshot sends a state-machine snapshot to a lagging follower whose
// next needed log entry has been compacted away (Raft §7).
type InstallSnapshot struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply is the follower's response.
type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}
