package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
)

// TestOpenRaftNodeCluster exercises the production raft-node builder the way a
// cacheyd cluster is brought up: a first node bootstraps and becomes leader,
// later nodes are added over TCP (raft.AddServer — the same call the JOIN
// control path makes), every FSM converges, a follower rejects reads with
// ErrNotLeader, and killing the leader lets the remaining majority elect a
// replacement (which only works because the committed configurations carry
// every member's raft address, not just the newcomer's).
func TestOpenRaftNodeCluster(t *testing.T) {
	nodes := make(map[string]*RaftNode)
	open := func(id string) *RaftNode {
		t.Helper()
		rn, err := OpenRaftNode(RaftNodeConfig{
			ID:                id,
			Dir:               t.TempDir(),
			RaftAddr:          "127.0.0.1:0",
			HeartbeatInterval: 100 * time.Millisecond,
			ElectionTimeout:   600 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("OpenRaftNode(%s): %v", id, err)
		}
		nodes[id] = rn
		return rn
	}
	defer func() {
		for _, rn := range nodes {
			rn.Stop()
		}
	}()
	stop := func(id string) {
		nodes[id].Stop()
		delete(nodes, id)
	}
	waitCond := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	add := func(leader *RaftNode, id string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := leader.Node.AddServer(ctx, id, nodes[id].RaftAddr); err != nil {
			t.Fatalf("AddServer(%s): %v", id, err)
		}
	}
	allFSM := func(key, val string) bool {
		for _, rn := range nodes {
			v, err := rn.Store.Get(key)
			if err != nil || *v != val {
				return false
			}
		}
		return true
	}
	leader := func() *RaftNode {
		for _, rn := range nodes {
			if rn.Node.IsLeader() {
				return rn
			}
		}
		return nil
	}

	// Bootstrap n1 and bring up two fresh nodes listening but not yet members.
	n1 := open("n1")
	n2 := open("n2")
	n3 := open("n3")
	n1.Node.Run()
	waitCond("n1 to lead a 1-node cluster", func() bool { return n1.Node.IsLeader() })

	// Join n2, then n3, through the leader (single-server changes, one at a
	// time). The joiners are not Run yet, so they never self-elect.
	add(n1, "n2")
	waitCond("n2 to learn the 2-voter config", func() bool {
		return len(n2.Node.Voters()) == 2
	})
	add(n1, "n3")
	waitCond("every member to learn the 3-voter config", func() bool {
		for _, id := range []string{"n1", "n2", "n3"} {
			if len(nodes[id].Node.Voters()) != 3 {
				return false
			}
		}
		return true
	})
	n2.Node.Run()
	n3.Node.Run()

	// A write through the leader replicates to every member's FSM.
	if err := n1.CS.Put("k1", "v1"); err != nil {
		t.Fatalf("Put via leader: %v", err)
	}
	waitCond("k1 to replicate to all FSMs", func() bool { return allFSM("k1", "v1") })

	// A follower rejects reads (clients rely on the leader redirect instead).
	follower := func() *RaftNode {
		for _, rn := range []*RaftNode{n2, n3} {
			if !rn.Node.IsLeader() {
				return rn
			}
		}
		return nil
	}()
	if follower == nil {
		t.Fatal("expected n2 or n3 to be a follower")
	}
	if _, err := follower.CS.Get("k1"); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower Get error = %v, want raft.ErrNotLeader", err)
	}

	// Kill the leader: the remaining majority (n2+n3) must be able to elect a
	// replacement, which requires each of them to know the other's raft
	// address — provided by the self-describing committed configurations.
	stop("n1")
	waitCond("a replacement leader among n2/n3", func() bool {
		return leader() != nil && len(nodes) == 2
	})
	if err := leader().CS.Put("k2", "v2"); err != nil {
		t.Fatalf("Put via replacement leader: %v", err)
	}
	waitCond("k2 to replicate to the surviving members", func() bool {
		return allFSM("k2", "v2")
	})
}
