package raft

import (
	"context"
	"sync"
)

// ReadIndex implements the linearizable-read protocol (Raft §6.4). It records
// the current commit index, waits until an entry from the current term (the
// leadership no-op) is committed, confirms via a quorum heartbeat that this
// node is still the leader, and blocks until the state machine has applied
// through the recorded index. After it returns, a read of the local FSM is
// linearizable.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// The leader must commit an entry in its current term before serving
	// linearizable reads, or it could miss a write committed in a prior term.
	for n.role != RoleLeader || n.commitIndex < n.noopIndex {
		if err := ctx.Err(); err != nil {
			n.mu.Unlock()
			return 0, err
		}
		n.commitCond.Wait()
	}
	readIdx := n.commitIndex
	n.mu.Unlock()

	if err := n.confirmLeadership(ctx); err != nil {
		return 0, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for n.lastApplied < readIdx {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n.commitCond.Wait()
	}
	return readIdx, nil
}

// confirmLeadership verifies this node is still the leader by getting a
// majority of peers to acknowledge a heartbeat in the current term.
func (n *Node) confirmLeadership(ctx context.Context) error {
	if len(n.peers) == 0 {
		return nil // a single-node cluster is always a majority
	}
	majority := len(n.peers)/2 + 1
	acks := 1 // self

	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	args := &AppendEntries{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: n.log.lastIndex(),
		PrevLogTerm:  n.log.lastTerm(),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	var mu sync.Mutex
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			reply, err := n.tr.SendAppendEntries(ctx, peer, args)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if reply.Success {
				acks++
				if acks >= majority {
					select {
					case done <- struct{}{}:
					default:
					}
				}
			}
		}(peer)
	}
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.role != RoleLeader {
			return ErrNotLeader
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
