package pbft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---- in-memory test transport ----

type fsm struct {
	mu      sync.Mutex
	applied []string // "seq:command" in apply order
}

func (f *fsm) apply(seq uint64, req Request) {
	f.mu.Lock()
	f.applied = append(f.applied, fmt.Sprintf("%d:%s", seq, req.Command))
	f.mu.Unlock()
}

func (f *fsm) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.applied))
	copy(out, f.applied)
	return out
}

type cluster struct {
	mu    sync.Mutex
	nodes map[string]*Replica
	dropF func(from, to string) bool
}

func (c *cluster) node(id string) *Replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) dropped(from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropF != nil && c.dropF(from, to)
}

func (c *cluster) setDrop(f func(from, to string) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropF = f
}

type memTransport struct {
	cluster *cluster
	from    string
}

func (t *memTransport) deliver(peer string, fn func(*Replica)) error {
	if t.cluster.dropped(t.from, peer) {
		return errors.New("pbft: test: dropped")
	}
	target := t.cluster.node(peer)
	if target == nil {
		return errors.New("pbft: test: no such peer " + peer)
	}
	fn(target)
	return nil
}

func (t *memTransport) SendPrePrepare(_ context.Context, peer string, m *PrePrepare) error {
	return t.deliver(peer, func(r *Replica) { r.HandlePrePrepare(m) })
}
func (t *memTransport) SendPrepare(_ context.Context, peer string, m *Prepare) error {
	return t.deliver(peer, func(r *Replica) { r.HandlePrepare(m) })
}
func (t *memTransport) SendCommit(_ context.Context, peer string, m *Commit) error {
	return t.deliver(peer, func(r *Replica) { r.HandleCommit(m) })
}
func (t *memTransport) SendViewChange(_ context.Context, peer string, m *ViewChange) error {
	return t.deliver(peer, func(r *Replica) { r.HandleViewChange(m) })
}
func (t *memTransport) SendNewView(_ context.Context, peer string, m *NewView) error {
	return t.deliver(peer, func(r *Replica) { r.HandleNewView(m) })
}

func startCluster(t *testing.T, ids []string) (map[string]*Replica, map[string]*fsm) {
	t.Helper()
	c := &cluster{nodes: make(map[string]*Replica)}
	nodes := make(map[string]*Replica)
	fsms := make(map[string]*fsm)
	peersOf := func(id string) []string {
		var out []string
		for _, other := range ids {
			if other != id {
				out = append(out, other)
			}
		}
		return out
	}
	for _, id := range ids {
		f := &fsm{}
		n, err := NewReplica(Config{ID: id, Peers: peersOf(id)}, &memTransport{cluster: c, from: id}, f.apply)
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		c.nodes[id] = n
		nodes[id] = n
		fsms[id] = f
	}
	wirePeerKeys(nodes)
	return nodes, fsms
}

// wirePeerKeys registers every node's identity public key on every peer so
// handlers accept their (signed) messages (M3 dynamic key exchange, done
// out-of-band in tests).
func wirePeerKeys(nodes map[string]*Replica) {
	for id, n := range nodes {
		for other := range nodes {
			if other != id {
				n.SetPeerKey(other, nodes[other].PublicKey())
			}
		}
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

func equal(a, b []string) bool {
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

// ---- tests ----

func TestSingleNodeExecutes(t *testing.T) {
	f := &fsm{}
	n, err := NewReplica(Config{ID: "a"}, &memTransport{}, f.apply)
	if err != nil {
		t.Fatalf("NewReplica: %v", err)
	}
	seq, err := n.Submit([]byte("x"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.WaitApplied(ctx, seq); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if got := f.snapshot(); !equal(got, []string{"1:x"}) {
		t.Fatalf("applied = %v, want [1:x]", got)
	}
}

// TestAllReplicasExecuteInOrder drives the happy path on a 4-replica cluster
// (f = 1): two requests submitted to the primary must execute on every replica
// in the same sequence order.
func TestAllReplicasExecuteInOrder(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms := startCluster(t, ids)
	primary := nodes["r0"] // sorted replica id at view 0
	for _, other := range []string{"r1", "r2", "r3"} {
		if nodes[other].IsPrimary() {
			t.Fatalf("%s is primary; want only r0", other)
		}
	}
	if primary.Primary() != "r0" {
		t.Fatalf("Primary() = %q, want r0", primary.Primary())
	}

	seq1, err := primary.Submit([]byte("a"))
	if err != nil {
		t.Fatalf("Submit(a): %v", err)
	}
	seq2, err := primary.Submit([]byte("b"))
	if err != nil {
		t.Fatalf("Submit(b): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := primary.WaitApplied(ctx, seq1); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq1, err)
	}
	if err := primary.WaitApplied(ctx, seq2); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq2, err)
	}
	want := []string{"1:a", "2:b"}
	for _, id := range ids {
		waitFor(t, id+" to apply both requests", 3*time.Second, func() bool {
			return equal(fsms[id].snapshot(), want)
		})
	}
	if got := fsms["r0"].snapshot(); !equal(got, want) {
		t.Fatalf("r0 applied %v, want %v", got, want)
	}
}

func TestSubmitFromBackupRejected(t *testing.T) {
	nodes, _ := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	if _, err := nodes["r1"].Submit([]byte("x")); err != ErrNotPrimary {
		t.Fatalf("Submit on backup = %v, want ErrNotPrimary", err)
	}
}

// TestQuorumRequired verifies that no request commits when too few replicas
// are responsive: with f = 1 a commit needs 2f+1 = 3 correct replicas, so
// silencing two backups must stall the request forever on the survivors.
func TestQuorumRequired(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	c := &cluster{nodes: make(map[string]*Replica)}
	nodes := make(map[string]*Replica)
	fsms := make(map[string]*fsm)
	peersOf := func(id string) []string {
		var out []string
		for _, other := range ids {
			if other != id {
				out = append(out, other)
			}
		}
		return out
	}
	for _, id := range ids {
		f := &fsm{}
		n, err := NewReplica(Config{ID: id, Peers: peersOf(id)}, &memTransport{cluster: c, from: id}, f.apply)
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		c.nodes[id] = n
		nodes[id] = n
		fsms[id] = f
	}
	wirePeerKeys(nodes)
	// r2 and r3 never deliver anything (silent Byzantine/crashed backups).
	c.setDrop(func(from, to string) bool { return from == "r2" || from == "r3" })

	seq, err := nodes["r0"].Submit([]byte("a"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := nodes["r0"].WaitApplied(ctx, seq); err == nil {
		t.Fatalf("WaitApplied succeeded without a quorum")
	}
	if got := fsms["r0"].snapshot(); len(got) != 0 {
		t.Fatalf("r0 applied %v with no quorum", got)
	}
	if got := fsms["r1"].snapshot(); len(got) != 0 {
		t.Fatalf("r1 applied %v with no quorum", got)
	}
}

// TestByzantinePrimaryEquivocation simulates a faulty primary (r0) sending two
// different requests for the same sequence number to two different backups.
// The backups prepare different digests, so neither reaches a 2f prepared
// certificate and nothing commits.
func TestByzantinePrimaryEquivocation(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA := Request{Client: "c", Timestamp: 7, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 7, Command: []byte("B")}
	if digestOf(reqA) == digestOf(reqB) {
		t.Fatal("test digests collide")
	}
	// r0 (Byzantine primary) sends seq 1 = A to r1 and seq 1 = B to r2.
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqA), Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	nodes["r1"].HandlePrePrepare(ppA)
	ppB := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB.Sig = nodes["r0"].sign(ppB)
	nodes["r2"].HandlePrePrepare(ppB)
	// Each backup multicasts its prepare; the other's prepare has a different
	// digest and must not count. No replica can collect 2f matching prepares,
	// so nothing may execute.
	time.Sleep(150 * time.Millisecond)
	for _, id := range []string{"r0", "r1", "r2", "r3"} {
		if got := fsms[id].snapshot(); len(got) != 0 {
			t.Fatalf("%s executed %v after equivocating primary", id, got)
		}
	}
}

// TestIgnoreConflictingProposalAtSameSeq delivers a conflicting pre-prepare
// (same seq, different request) to a backup that already accepted one. The
// backup must stay on its first proposal: even a full set of crafted
// prepares/commits for the conflicting request must not make it execute.
// A control run on another backup proves the crafted messages can drive
// execution when they match the accepted proposal.
func TestIgnoreConflictingProposalAtSameSeq(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA := Request{Client: "c", Timestamp: 9, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 9, Command: []byte("B")}

	// r1 accepts A for seq 1...
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqA), Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	nodes["r1"].HandlePrePrepare(ppA)
	// ...then the primary tries to overwrite seq 1 with B (equivocation).
	ppB := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB.Sig = nodes["r0"].sign(ppB)
	nodes["r1"].HandlePrePrepare(ppB)
	// Craft a full B certificate for r1: it must be ignored (r1 is on A).
	for _, from := range []string{"r2", "r3"} {
		p := &Prepare{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: from}
		p.Sig = nodes[from].sign(p)
		nodes["r1"].HandlePrepare(p)
	}
	for _, from := range []string{"r2", "r3"} {
		c := &Commit{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: from}
		c.Sig = nodes[from].sign(c)
		nodes["r1"].HandleCommit(c)
	}

	// Control: r2 accepts B for seq 1 and the same crafted certificate drives
	// it to execute B, proving the crafted messages are otherwise effective.
	ppB2 := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB2.Sig = nodes["r0"].sign(ppB2)
	nodes["r2"].HandlePrePrepare(ppB2)
	for _, from := range []string{"r1", "r3"} {
		p := &Prepare{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: from}
		p.Sig = nodes[from].sign(p)
		nodes["r2"].HandlePrepare(p)
	}
	for _, from := range []string{"r1", "r3"} {
		c := &Commit{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: from}
		c.Sig = nodes[from].sign(c)
		nodes["r2"].HandleCommit(c)
	}

	waitFor(t, "r2 to execute the control request", time.Second, func() bool {
		return equal(fsms["r2"].snapshot(), []string{"1:B"})
	})
	// r1 must not have executed anything.
	time.Sleep(100 * time.Millisecond)
	if got := fsms["r1"].snapshot(); len(got) != 0 {
		t.Fatalf("r1 executed %v after ignoring a conflicting proposal", got)
	}
}

// TestPrepareRequiresMatchingPrePrepare ensures prepares/commits for a
// sequence number this replica never pre-prepared are ignored (no panic, no
// state change).
func TestPrepareRequiresMatchingPrePrepare(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	req := Request{Client: "c", Timestamp: 3, Command: []byte("x")}
	d := digestOf(req)
	pr := func(from string) *Prepare {
		p := &Prepare{View: 0, Seq: 5, Digest: d, Sender: from}
		p.Sig = nodes[from].sign(p)
		return p
	}
	nodes["r1"].HandlePrepare(pr("r2"))
	cm := &Commit{View: 0, Seq: 5, Digest: d, Sender: "r2"}
	cm.Sig = nodes["r2"].sign(cm)
	nodes["r1"].HandleCommit(cm)
	wv := &PrePrepare{View: 1, Seq: 1, Digest: d, Req: req, Sender: "r0"}
	wv.Sig = nodes["r0"].sign(wv)
	nodes["r1"].HandlePrePrepare(wv) // wrong view
	bd := &PrePrepare{View: 0, Seq: 1, Digest: "bogus", Req: req, Sender: "r0"}
	bd.Sig = nodes["r0"].sign(bd)
	nodes["r1"].HandlePrePrepare(bd)
	time.Sleep(50 * time.Millisecond)
	if got := fsms["r1"].snapshot(); len(got) != 0 {
		t.Fatalf("r1 executed %v despite invalid messages", got)
	}
}
