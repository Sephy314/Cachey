// Package raft implements a Raft consensus node from scratch (Raft paper
// sections 5.2-5.4): leader election, log replication, and commit/apply to a
// state machine. The log and durable state live in memory and are persisted
// via Cachey's existing WAL (see internal/wal) by the recovery/transport
// layers; this package is the pure consensus core.
package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ErrNotLeader is returned by Propose when this node is not the leader.
var ErrNotLeader = errors.New("raft: not leader")

// Role is the node's current Raft role.
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	}
	return "Unknown"
}

// Transport delivers Raft RPCs to peers. Implementations must be safe for
// concurrent use. The in-memory test transport wires nodes directly; the
// NDJSON transport (milestone 2) encodes these messages over TCP.
type Transport interface {
	// SendRequestVote delivers a vote request to peer and returns its reply.
	SendRequestVote(ctx context.Context, peer string, args *RequestVote) (*RequestVoteReply, error)
	// SendAppendEntries delivers entries to peer and returns its reply.
	SendAppendEntries(ctx context.Context, peer string, args *AppendEntries) (*AppendEntriesReply, error)
}

// Node is a single Raft peer.
type Node struct {
	id      string
	peers   []string
	cfg     Config
	tr      Transport
	applyFn func(Entry)        // applies one committed entry to the FSM
	onRole  func(Role, uint64) // notifies leadership transitions

	mu          sync.Mutex
	commitCond  *sync.Cond
	role        Role
	currentTerm uint64
	votedFor    string
	log         *Log
	logStore    LogStore
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// leader volatile state (Raft §5.4)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionReset chan struct{}
	stopOnce      sync.Once
	stopCh        chan struct{}
	doneCh        chan struct{}
	rng           *rand.Rand
}

// NewNode creates a Raft node. applyFn is called once per committed entry, in
// order, and must not call back into this node (it runs while the node lock is
// held). tr delivers RPCs to peers.
func NewNode(cfg Config, tr Transport, applyFn func(Entry)) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: node id is required")
	}
	if tr == nil {
		return nil, errors.New("raft: transport is required")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 100 * time.Millisecond
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 500 * time.Millisecond
	}
	n := &Node{
		id:            cfg.ID,
		peers:         cfg.Peers,
		cfg:           cfg,
		tr:            tr,
		applyFn:       applyFn,
		log:           NewLog(),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		electionReset: make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	n.commitCond = sync.NewCond(&n.mu)
	return n, nil
}

// Run starts the node's background goroutines (election timer, heartbeats).
func (n *Node) Run() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

// Stop shuts down the node's background goroutines. Safe to call multiple times.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		<-n.doneCh
	})
}

// ---- accessors ----

func (n *Node) ID() string          { return n.id }
func (n *Node) Term() uint64        { n.mu.Lock(); defer n.mu.Unlock(); return n.currentTerm }
func (n *Node) Leader() string      { n.mu.Lock(); defer n.mu.Unlock(); return n.leaderID }
func (n *Node) CommitIndex() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.commitIndex }
func (n *Node) LastApplied() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.lastApplied }
func (n *Node) LogLastIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.lastIndex()
}

// IsLeader reports whether this node is currently the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// Role returns the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// ---- background loops ----

func (n *Node) electionLoop() {
	defer close(n.doneCh)
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.electionReset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.mu.Lock()
			isLeader := n.role == RoleLeader
			n.mu.Unlock()
			if !isLeader {
				if n.becomeCandidate() {
					n.startElection()
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if n.IsLeader() {
				n.broadcastAppendEntries()
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	base := int64(n.cfg.ElectionTimeout)
	return n.cfg.ElectionTimeout + time.Duration(n.rng.Int63n(base))
}

// resetElectionTimer extends the current election window (a valid heartbeat
// or vote was received). Non-blocking; the window is already randomized.
func (n *Node) resetElectionTimer() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

// ---- election (Raft §5.2) ----

func (n *Node) becomeCandidate() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader {
		return false
	}
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	return true
}

func (n *Node) startElection() {
	n.mu.Lock()
	term := n.currentTerm
	lastLogIndex := n.log.lastIndex()
	lastLogTerm := n.log.lastTerm()
	n.mu.Unlock()

	// A single-node cluster is always its own majority.
	if len(n.peers) == 0 {
		n.tryBecomeLeader(term)
		n.broadcastAppendEntries()
		return
	}

	votes := 1 // self
	majority := len(n.peers)/2 + 1
	var mu sync.Mutex

	for _, peer := range n.peers {
		go func(peer string) {
			args := &RequestVote{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := n.tr.SendRequestVote(context.Background(), peer, args)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDownLocked(reply.Term)
				n.mu.Unlock()
				return
			}
			stillCandidate := n.role == RoleCandidate && n.currentTerm == term
			n.mu.Unlock()
			if !stillCandidate || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= majority && n.tryBecomeLeader(term) {
				n.broadcastAppendEntries()
			}
		}(peer)
	}
}

// SetRoleObserver registers a callback invoked (under the node lock) whenever
// this node changes role or term. Used by higher layers for leader discovery.
func (n *Node) SetRoleObserver(fn func(Role, uint64)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onRole = fn
}

// tryBecomeLeader promotes the candidate to leader if it is still a candidate
// in the given term. Returns whether it became (or already was) leader.
func (n *Node) tryBecomeLeader(term uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader && n.currentTerm == term {
		return true
	}
	if n.role != RoleCandidate || n.currentTerm != term {
		return false
	}
	// Append a no-op entry in the current term so entries from previous terms
	// get committed (Raft §5.4.2). It must be durable first so the log stays
	// contiguous across a crash+recovery (later entries reference its index).
	noopIdx := n.log.lastIndex() + 1
	if err := n.persistEntryLocked(noopIdx, Entry{Term: term}); err != nil {
		n.logf("persist no-op at %d failed: %v", noopIdx, err)
		return false // stay a candidate; a later election retries
	}
	n.role = RoleLeader
	n.leaderID = n.id
	for _, p := range n.peers {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.log.append(Entry{Term: term})
	// The leader's own entries are always on a majority (itself); a single-node
	// cluster commits immediately, larger clusters wait for followers.
	n.updateCommitIndexLocked()
	n.notifyRoleLocked()
	return true
}

// stepDownLocked converts the node to a follower of the (higher) term.
func (n *Node) stepDownLocked(term uint64) {
	n.currentTerm = term
	n.role = RoleFollower
	n.votedFor = ""
	n.leaderID = ""
	n.notifyRoleLocked()
}

func (n *Node) notifyRoleLocked() {
	if n.onRole != nil {
		n.onRole(n.role, n.currentTerm)
	}
}

// ---- RPC handlers ----

// HandleRequestVote processes a vote request (called by the transport).
func (n *Node) HandleRequestVote(args *RequestVote) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply // reject stale term
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.role = RoleFollower
		n.leaderID = ""
		n.notifyRoleLocked()
	}
	lastLogIndex := n.log.lastIndex()
	lastLogTerm := n.log.lastTerm()
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimer()
	}
	return reply
}

// HandleAppendEntries processes an AppendEntries RPC / heartbeat.
func (n *Node) HandleAppendEntries(args *AppendEntries) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply // reject stale term
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.role = RoleFollower
	n.leaderID = args.LeaderID
	n.notifyRoleLocked()

	if args.PrevLogIndex > n.log.lastIndex() {
		return reply // log too short; leader will decrement nextIndex
	}
	if args.PrevLogIndex > 0 && n.log.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		return reply // conflict; leader will back off
	}
	// Durably persist every entry we don't already have before ACKing, so a
	// committed entry survives a follower crash (Raft §5.4).
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.log.lastIndex() && n.log.termAt(idx) == e.Term {
			continue // already present and durable
		}
		if err := n.persistEntryLocked(idx, e); err != nil {
			return reply // not acknowledged; the leader will retry
		}
	}
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.log.lastIndex() {
			if n.log.termAt(idx) != e.Term {
				n.log.truncate(idx)
				n.log.append(e)
			}
		} else {
			n.log.append(e)
		}
	}
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.log.lastIndex())
	}
	n.applyCommittedLocked()
	reply.Success = true
	n.resetElectionTimer()
	return reply
}

// ---- replication (Raft §5.3, leader side) ----

// Propose submits a client command for replication. Returns the log index of
// the entry. Only valid on the leader; followers get ErrNotLeader.
func (n *Node) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	idx := n.log.lastIndex() + 1
	e := Entry{Term: n.currentTerm, Command: command}
	// Durable before the entry enters the replicated log (crash safety: a
	// committed entry must survive on the leader).
	if err := n.persistEntryLocked(idx, e); err != nil {
		n.mu.Unlock()
		return 0, err
	}
	n.log.append(e)
	// The leader always has its own entries; a single-node cluster commits
	// immediately, larger clusters wait for a majority of followers.
	n.updateCommitIndexLocked()
	n.mu.Unlock()
	n.broadcastAppendEntries()
	return idx, nil
}

// WaitApplied blocks until the entry at idx is applied to the FSM, or ctx is
// done. Safe to call from any node (useful on the leader after Propose).
func (n *Node) WaitApplied(ctx context.Context, idx uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.lastApplied < idx {
		if err := ctx.Err(); err != nil {
			return err
		}
		n.commitCond.Wait()
	}
	return nil
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		go n.replicateTo(peer)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.log.termAt(prevLogIndex)
	args := &AppendEntries{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      n.log.slice(nextIdx),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.tr.SendAppendEntries(context.Background(), peer, args)
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
	if reply.Success {
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
			n.nextIndex[peer] = newMatch + 1
		}
		n.updateCommitIndexLocked()
		return
	}
	// Follower rejected: back off one entry and retry on the next heartbeat.
	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// updateCommitIndexLocked advances commitIndex to the largest index replicated
// on a majority whose term is the current term (Raft §5.4.2), then applies.
func (n *Node) updateCommitIndexLocked() {
	for N := n.commitIndex + 1; N <= n.log.lastIndex(); N++ {
		if n.log.termAt(N) != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, m := range n.matchIndex {
			if m >= N {
				count++
			}
		}
		if count > len(n.peers)/2 {
			n.commitIndex = N
		} else {
			break
		}
	}
	n.applyCommittedLocked()
}

// applyCommittedLocked applies entries (lastApplied, commitIndex] to the FSM
// in order. No-op entries (nil Command) are skipped.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log.entryAt(n.lastApplied)
		if e.Command != nil && n.applyFn != nil {
			n.applyFn(e)
		}
	}
	n.commitCond.Broadcast()
}

// logf logs a message with the node's identity and term.
func (n *Node) logf(format string, args ...any) {
	log.Printf("[raft %s] "+format, append([]any{n.id}, args...)...)
}
