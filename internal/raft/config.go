package raft

import "time"

// Config configures a Raft node.
type Config struct {
	ID    string   // this node's ID (must be unique in the cluster)
	Peers []string // peer IDs, excluding self

	// HeartbeatInterval is how often the leader sends AppendEntries.
	HeartbeatInterval time.Duration
	// ElectionTimeout is the base follower timeout; the actual timeout is
	// randomized to base..2*base to avoid split votes.
	ElectionTimeout time.Duration
	// SnapshotThreshold is the log length (entries after the compaction base)
	// that triggers a snapshot + log compaction (Raft §7).
	SnapshotThreshold uint64
}
