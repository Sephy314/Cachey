package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ---- in-memory test transport ----

type fsm struct {
	mu      sync.Mutex
	applied []string // commands in apply order
}

func (f *fsm) apply(e Entry) {
	if e.Command != nil {
		f.mu.Lock()
		f.applied = append(f.applied, string(e.Command))
		f.mu.Unlock()
	}
}

func (f *fsm) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.applied))
	copy(out, f.applied)
	return out
}

type memTransport struct {
	cluster *cluster
	from    string
}

func (t *memTransport) SendRequestVote(_ context.Context, peer string, args *RequestVote) (*RequestVoteReply, error) {
	if t.cluster.dropped(t.from, peer) {
		return nil, errors.New("raft: test: message dropped")
	}
	target := t.cluster.node(peer)
	if target == nil {
		return nil, errors.New("raft: test: no such peer " + peer)
	}
	return target.HandleRequestVote(args), nil
}

func (t *memTransport) SendAppendEntries(_ context.Context, peer string, args *AppendEntries) (*AppendEntriesReply, error) {
	if t.cluster.dropped(t.from, peer) {
		return nil, errors.New("raft: test: message dropped")
	}
	target := t.cluster.node(peer)
	if target == nil {
		return nil, errors.New("raft: test: no such peer " + peer)
	}
	return target.HandleAppendEntries(args), nil
}

func (t *memTransport) SendInstallSnapshot(_ context.Context, peer string, args *InstallSnapshot) (*InstallSnapshotReply, error) {
	if t.cluster.dropped(t.from, peer) {
		return nil, errors.New("raft: test: message dropped")
	}
	target := t.cluster.node(peer)
	if target == nil {
		return nil, errors.New("raft: test: no such peer " + peer)
	}
	return target.HandleInstallSnapshot(args), nil
}

type cluster struct {
	mu    sync.Mutex
	nodes map[string]*Node
	dropF func(from, to string) bool
}

func (c *cluster) node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) dropped(from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropF != nil && c.dropF(from, to)
}

// setDrop installs a fault-injection predicate (nil disables drops).
func (c *cluster) setDrop(f func(from, to string) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropF = f
}

// clusterOpts tunes the test cluster.
type clusterOpts struct {
	heartbeat time.Duration
	election  time.Duration
}

func startCluster(t *testing.T, ids []string, opts clusterOpts) (*cluster, map[string]*Node, map[string]*fsm) {
	t.Helper()
	if opts.heartbeat == 0 {
		opts.heartbeat = 20 * time.Millisecond
	}
	if opts.election == 0 {
		opts.election = 100 * time.Millisecond
	}
	c := &cluster{nodes: make(map[string]*Node)}
	nodes := make(map[string]*Node)
	fsms := make(map[string]*fsm)
	peersOf := func(id string) []string {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		return peers
	}
	for _, id := range ids {
		f := &fsm{}
		cfg := Config{
			ID:                id,
			Peers:             peersOf(id),
			HeartbeatInterval: opts.heartbeat,
			ElectionTimeout:   opts.election,
		}
		n, err := NewNode(cfg, &memTransport{cluster: c, from: id}, f.apply)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		c.nodes[id] = n
		nodes[id] = n
		fsms[id] = f
	}
	for _, id := range ids {
		nodes[id].Run()
	}
	return c, nodes, fsms
}

func stopAll(nodes map[string]*Node) {
	for _, n := range nodes {
		n.Stop()
	}
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitLeader returns the cluster's single leader id, failing if none/ambiguous.
func waitLeader(t *testing.T, nodes map[string]*Node) string {
	t.Helper()
	var leader string
	waitFor(t, "a leader to be elected", 3*time.Second, func() bool {
		leaders := 0
		for id, n := range nodes {
			if n.IsLeader() {
				leader = id
				leaders++
			}
		}
		return leaders == 1
	})
	return leader
}

// propose waits for a command to be committed and applied on the leader.
func propose(t *testing.T, n *Node, cmd string) {
	t.Helper()
	idx, err := n.Propose([]byte(cmd))
	if err != nil {
		t.Fatalf("Propose(%q): %v", cmd, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, idx); err != nil {
		t.Fatalf("WaitApplied(%q idx=%d): %v", cmd, idx, err)
	}
}

// ---- tests ----

func TestSingleNodeBecomesLeader(t *testing.T) {
	c, nodes, _ := startCluster(t, []string{"a"}, clusterOpts{})
	defer stopAll(nodes)
	_ = c
	waitFor(t, "single node to lead", 2*time.Second, func() bool {
		return nodes["a"].IsLeader()
	})

	// A single-node leader commits a no-op and can apply proposals.
	propose(t, nodes["a"], "hello")
	if got := nodes["a"].LastApplied(); got == 0 {
		t.Fatalf("expected applied index > 0, got %d", got)
	}
}

func TestLeaderElectionThreeNodes(t *testing.T) {
	_, nodes, _ := startCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopAll(nodes)

	leader := waitLeader(t, nodes)
	// Followers agree on who the leader is.
	waitFor(t, "followers to know the leader", 3*time.Second, func() bool {
		for id, n := range nodes {
			if id == leader {
				continue
			}
			if n.Leader() != leader {
				return false
			}
		}
		return true
	})
	if nodes[leader].Term() == 0 {
		t.Fatalf("leader term should be > 0")
	}
}

func TestProposeFromFollowerRejected(t *testing.T) {
	_, nodes, _ := startCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopAll(nodes)
	leader := waitLeader(t, nodes)

	var follower *Node
	for id, n := range nodes {
		if id != leader {
			follower = n
			break
		}
	}
	if _, err := follower.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader from follower, got %v", err)
	}
}

func TestLogReplicationAndCommit(t *testing.T) {
	c, nodes, fsms := startCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopAll(nodes)
	_ = c
	leader := waitLeader(t, nodes)

	cmds := []string{"k1=v1", "k2=v2", "k3=v3"}
	for _, cmd := range cmds {
		propose(t, nodes[leader], cmd)
	}

	// All nodes apply the same commands, in the same order.
	waitFor(t, "all nodes apply all commands", 3*time.Second, func() bool {
		for _, f := range fsms {
			if got := f.snapshot(); !equalStrings(got, cmds) {
				return false
			}
		}
		return true
	})

	// All logs are identical.
	waitFor(t, "logs to converge", 3*time.Second, func() bool {
		ref := nodes[leader].logEntryTerms(1, uint64(len(cmds)+1)) // +no-op
		for _, n := range nodes {
			if !equalUint64s(n.logEntryTerms(1, uint64(len(cmds)+1)), ref) {
				return false
			}
		}
		return true
	})
}

func TestLeaderFailover(t *testing.T) {
	_, nodes, fsms := startCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopAll(nodes)

	oldLeader := waitLeader(t, nodes)
	propose(t, nodes[oldLeader], "before-failover")

	// Kill the leader.
	nodes[oldLeader].Stop()
	delete(nodes, oldLeader)

	// The remaining two elect a new leader.
	waitFor(t, "a new leader after failover", 5*time.Second, func() bool {
		leaders := 0
		for _, n := range nodes {
			if n.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	})
	var newLeader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			newLeader = n
			break
		}
	}
	if newLeader == nil {
		t.Fatalf("no new leader elected")
	}

	// The new leader must have the previously committed command.
	propose(t, newLeader, "after-failover")
	waitFor(t, "both live nodes apply both commands", 3*time.Second, func() bool {
		want := []string{"before-failover", "after-failover"}
		for _, f := range fsms {
			if !equalStrings(f.snapshot(), want) {
				return false
			}
		}
		return true
	})
}

func TestLogMatchingTruncation(t *testing.T) {
	// A follower with a divergent tail (from a stale leader term) must
	// truncate it and adopt the current leader's entries.
	c := &cluster{nodes: make(map[string]*Node)}
	f := &fsm{}
	leader, err := NewNode(Config{ID: "leader"}, &memTransport{cluster: c, from: "leader"}, f.apply)
	if err != nil {
		t.Fatal(err)
	}
	follower, err := NewNode(Config{ID: "follower"}, &memTransport{cluster: c, from: "follower"}, f.apply)
	if err != nil {
		t.Fatal(err)
	}
	_ = leader

	// follower diverges at index 2 with a stale higher term.
	follower.log.append(Entry{Term: 1}, Entry{Term: 2})

	reply := follower.HandleAppendEntries(&AppendEntries{
		Term:         3,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []Entry{{Term: 3, Command: []byte("x")}, {Term: 3, Command: []byte("y")}},
		LeaderCommit: 3,
	})
	if !reply.Success {
		t.Fatal("expected successful append")
	}
	if got := follower.logEntryTerms(1, 3); !equalUint64s(got, []uint64{1, 3, 3}) {
		t.Fatalf("follower log terms = %v, want [1 3 3]", got)
	}
	waitFor(t, "follower applies committed entries", 1*time.Second, func() bool {
		return equalStrings(f.snapshot(), []string{"x", "y"})
	})
}

func TestVoteUpToDate(t *testing.T) {
	// A candidate with a more up-to-date log wins the vote over a lagging one.
	c := &cluster{nodes: make(map[string]*Node)}
	f := &fsm{}

	// node "candidate" has term-1 entries up to index 3.
	cand, err := NewNode(Config{ID: "candidate"}, &memTransport{cluster: c, from: "candidate"}, f.apply)
	if err != nil {
		t.Fatal(err)
	}
	cand.log.append(Entry{Term: 1}, Entry{Term: 1}, Entry{Term: 1})

	// voter has only index 1..2.
	voter, err := NewNode(Config{ID: "voter"}, &memTransport{cluster: c, from: "voter"}, f.apply)
	if err != nil {
		t.Fatal(err)
	}
	voter.log.append(Entry{Term: 1})

	reply := voter.HandleRequestVote(&RequestVote{
		Term:         2,
		CandidateID:  "candidate",
		LastLogIndex: cand.log.lastIndex(),
		LastLogTerm:  cand.log.lastTerm(),
	})
	if !reply.VoteGranted {
		t.Fatalf("up-to-date candidate should win the vote")
	}

	// A lagging candidate must be rejected.
	reply2 := voter.HandleRequestVote(&RequestVote{
		Term:         2,
		CandidateID:  "lagging",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if reply2.VoteGranted {
		t.Fatalf("lagging candidate must be rejected")
	}
}

// logEntryTerms returns the terms of entries in [from, to] (inclusive).
func (n *Node) logEntryTerms(from, to uint64) []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]uint64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, n.log.termAt(i))
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
