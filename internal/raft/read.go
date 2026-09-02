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
		if err := n.pollWake(ctx); err != nil {
			n.mu.Unlock()
			return 0, err
		}
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
		if err := n.pollWake(ctx); err != nil {
			return 0, err
		}
	}
	return readIdx, nil
}

// confirmLeadership verifies this node is still the leader by getting a
// majority of peers to acknowledge a heartbeat in the current term.
func (n *Node) confirmLeadership(ctx context.Context) error {
	if len(n.peers) == 0 {
		return nil // a single-node cluster is always a majority
	}
	majority := n.majorityLocked()
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
	steppedDown := false
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			reply, err := n.tr.SendAppendEntries(ctx, peer, args)
			if err != nil {
				return
			}
			// A higher term means this node is no longer the leader; step down
			// and fail fast (ErrNotLeader) instead of waiting out the caller's
			// context — a read during an election must redirect, not hang.
			n.mu.Lock()
			higher := reply.Term > n.currentTerm
			if higher {
				n.stepDownLocked(reply.Term)
			}
			n.mu.Unlock()
			if higher {
				mu.Lock()
				steppedDown = true
				mu.Unlock()
				select {
				case done <- struct{}{}:
				default:
				}
				return
			}
			if reply.Success {
				mu.Lock()
				acks++
				ok := acks >= majority
				mu.Unlock()
				if ok {
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
		if steppedDown || n.role != RoleLeader {
			return ErrNotLeader
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
