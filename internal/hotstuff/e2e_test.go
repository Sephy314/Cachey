package hotstuff

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// This file is the HS-M1 cluster-level suite. It wires several real replicas
// through an in-memory network (synchronous for the deterministic tests,
// asynchronous with delay/duplication for the chaos test) and pins the two
// big invariants that single-replica unit tests cannot:
//
//	Safety:   every correct replica executes the SAME command sequence (single
//	          committed prefix) even under delivery reordering.
//	Liveness: a correct leader with a quorum of correct replicas eventually
//	          commits every proposed command.
//
// Still no transport crypto, WAL or TLS — those are later milestones.

// orderLog records the commands a replica executed, oldest first.
type orderLog struct {
	mu   sync.Mutex
	cmds []string
}

func (o *orderLog) add(cmd string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cmds = append(o.cmds, cmd)
}

func (o *orderLog) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.cmds...)
}

// net is an in-memory message network between real replicas.
type net struct {
	mu     sync.Mutex
	nodes  map[string]*Replica
	logs   map[string]*orderLog
	async  bool
	delay  time.Duration // when async, per-delivery delay (reorders messages)
	dup    bool          // when async, delivers each message twice
	drop   map[string]bool
	stopCh chan struct{}
}

type link struct {
	n    *net
	from string
}

func (l *link) SendProposal(_ context.Context, peer string, p *Proposal) error {
	return l.n.deliver(peer, func(r *Replica) { r.HandleProposal(p) })
}

func (l *link) SendVote(_ context.Context, peer string, v *Vote) error {
	return l.n.deliver(peer, func(r *Replica) { r.HandleVote(v) })
}

func (l *link) SendViewChange(_ context.Context, peer string, vc *ViewChange) error {
	return l.n.deliver(peer, func(r *Replica) { r.HandleViewChange(vc) })
}

func (l *link) SendFetch(_ context.Context, peer string, f *Fetch) error {
	return l.n.deliver(peer, func(r *Replica) { r.HandleFetch(f) })
}

func (l *link) SendBlock(_ context.Context, peer string, bm *BlockMsg) error {
	return l.n.deliver(peer, func(r *Replica) { r.HandleBlock(bm) })
}

func (n *net) deliver(peer string, fn func(*Replica)) error {
	n.mu.Lock()
	dst := n.nodes[peer]
	async := n.async
	delay := n.delay
	dup := n.dup
	dropped := n.drop[peer]
	n.mu.Unlock()
	if dst == nil {
		return fmt.Errorf("net: no such replica %q", peer)
	}
	if dropped {
		return nil // simulate a crashed/unreachable replica
	}
	if !async {
		fn(dst)
		return nil
	}
	go func() {
		if delay > 0 {
			time.Sleep(time.Duration(rand.Int63n(int64(delay))))
		}
		fn(dst)
		if dup {
			fn(dst)
		}
	}()
	return nil
}

// dropPeer simulates a crashed/unreachable replica: every message addressed to
// it is silently dropped (so it never learns new blocks and never votes).
func (n *net) dropPeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop[id] = true
}

// unDropPeer heals a partition opened by dropPeer.
func (n *net) unDropPeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.drop, id)
}

func (n *net) stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopCh != nil {
		close(n.stopCh)
		n.stopCh = nil
	}
}

// startCluster boots replicas with the given ids; ids[0] is the leader.
// applyFn records executed commands into the per-replica order log.
func startCluster(t *testing.T, ids []string, async bool, delay time.Duration, dup bool) (*net, map[string]*Replica) {
	t.Helper()
	nw := &net{
		nodes:  make(map[string]*Replica),
		logs:   make(map[string]*orderLog),
		async:  async,
		delay:  delay,
		dup:    dup,
		drop:   make(map[string]bool),
		stopCh: make(chan struct{}),
	}
	leader := ids[0]
	nodes := make(map[string]*Replica, len(ids))
	for _, id := range ids {
		var peers []string
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		nw.logs[id] = &orderLog{}
		log := nw.logs[id]
		r, err := NewReplica(Config{ID: id, Peers: peers, Leader: leader}, &link{n: nw, from: id}, func(b Block) {
			if len(b.Cmd) > 0 {
				log.add(string(b.Cmd))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = r
		nw.nodes[id] = r
	}
	// HS-M3: register every member's identity public key on every peer before
	// any message flows, so signed proposals/votes/QCs verify.
	for _, id := range ids {
		for _, o := range ids {
			if o != id {
				nodes[id].SetPeerKey(o, nodes[o].PublicKey())
			}
		}
	}
	return nw, nodes
}

// proposeLoop drives the leader to propose cmds (and flush empty blocks until
// the last command is committed), retrying while the leader is busy forming the
// previous QC (async only).
func proposeLoop(t *testing.T, lead *Replica, cmds []string, flush int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	propose := func(cmd []byte) bool {
		for {
			if _, err := lead.Propose(cmd); err == nil {
				return true
			} else if err != ErrBusy {
				t.Fatalf("Propose: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for the leader to be free")
			}
			time.Sleep(time.Millisecond)
		}
	}
	for _, c := range cmds {
		propose([]byte(c))
	}
	for i := 0; i < flush; i++ {
		propose(nil) // empty block keeps the chain advancing
	}
}

// waitAllApplied polls until every replica has applied exactly want.
func waitAllApplied(t *testing.T, nw *net, ids []string, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		all := true
		for _, id := range ids {
			if got := nw.logs[id].snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range ids {
				t.Logf("%s applied: %v", id, nw.logs[id].snapshot())
			}
			t.Fatalf("replicas did not converge on %v", want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestClusterAllReplicasExecuteInOrder (liveness, deterministic): a 4-node
// cluster with a synchronous network commits every proposed command on every
// replica, in the same order. Empty blocks flush the tail of the 3-chain so
// even the last command commits at the followers.
func TestClusterAllReplicasExecuteInOrder(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	cmds := []string{"set-a", "set-b", "set-c", "set-d"}
	proposeLoop(t, nodes["L0"], cmds, 3) // +3 empty blocks -> followers commit the last cmd

	waitAllApplied(t, nw, ids, cmds)

	// Safety corollary: every replica applied exactly the same prefix, once.
	for _, id := range ids {
		if got := nw.logs[id].snapshot(); fmt.Sprint(got) != fmt.Sprint(cmds) {
			t.Fatalf("%s applied %v, want %v", id, got, cmds)
		}
	}
}

// TestClusterWaitCommitted: the leader's Propose returns a block id whose
// WaitCommitted resolves once the 3-chain above it completes.
func TestClusterWaitCommitted(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	lead := nodes["L0"]
	id, err := lead.Propose([]byte("only-write"))
	if err != nil {
		t.Fatal(err)
	}
	// Two more proposals (empty) complete the 3-chain above the write.
	for i := 0; i < 3; i++ {
		if _, err := lead.Propose(nil); err != nil {
			t.Fatalf("flush propose %d: %v", i, err)
		}
	}
	mustWaitCommitted(t, lead, id)
	waitAllApplied(t, nw, ids, []string{"only-write"})
}

// TestClusterAsyncSinglePrefix (safety under chaos): with an asynchronous
// network that delays and duplicates every message (reordering delivery), all
// correct replicas still converge on one identical command sequence — no
// replica ever executes a conflicting fork.
func TestClusterAsyncSinglePrefix(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, true, 6*time.Millisecond, true)
	defer nw.stop()

	cmds := []string{"a", "b", "c", "d", "e"}
	proposeLoop(t, nodes["L0"], cmds, 6)
	waitAllApplied(t, nw, ids, cmds)
}

// waitSomeApplied polls until exactly the given replicas have applied want
// (used when the remaining replica is crashed and can never catch up).
func waitSomeApplied(t *testing.T, nw *net, ids []string, want []string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	wantStr := fmt.Sprint(want)
	for {
		all := true
		for _, id := range ids {
			if got := nw.logs[id].snapshot(); fmt.Sprint(got) != wantStr {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range ids {
				t.Logf("%s applied: %v", id, nw.logs[id].snapshot())
			}
			t.Fatalf("replicas %v did not converge on %v", ids, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestClusterToleratesFaultyReplica (liveness under a crash): a 4-node cluster
// (f=1) keeps committing every command with one replica down — the quorum of
// 2f+1 correct members still forms QCs and applies in order.
func TestClusterToleratesFaultyReplica(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()
	nw.dropPeer("P3") // one crashed replica: f=1 tolerated

	cmds := []string{"a", "b", "c", "d"}
	proposeLoop(t, nodes["L0"], cmds, 3)
	waitSomeApplied(t, nw, []string{"L0", "P1", "P2"}, cmds)

	// The crashed replica learned nothing (it was never delivered anything).
	if got := nw.logs["P3"].snapshot(); len(got) != 0 {
		t.Fatalf("crashed replica must not have applied anything, got %v", got)
	}
}

// TestClusterHaltsWithFPlusOneDown (safety when liveness is impossible): with
// f+1 replicas down there is no quorum, so the leader stays busy and NOTHING
// commits — a correct replica never commits an unquorumed block.
func TestClusterHaltsWithFPlusOneDown(t *testing.T) {
	ids := []string{"L0", "P1", "P2", "P3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()
	nw.dropPeer("P2")
	nw.dropPeer("P3") // f+1 down: quorum unreachable

	id, err := nodes["L0"].Propose([]byte("a"))
	if err != nil {
		t.Fatalf("first propose must succeed (self vote), got %v", err)
	}
	// The leader has only its own + P1's votes (< 2f+1), so it stays busy.
	if _, err := nodes["L0"].Propose(nil); err != ErrBusy {
		t.Fatalf("with no quorum the leader must stay busy, got %v", err)
	}

	// Give any (incorrectly forwarded) messages a moment: still nothing may
	// have committed on the reachable replicas.
	time.Sleep(50 * time.Millisecond)
	for _, rid := range []string{"L0", "P1"} {
		if got := nw.logs[rid].snapshot(); len(got) != 0 {
			t.Fatalf("%s must not commit without a quorum, applied %v", rid, got)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := nodes["L0"].WaitCommitted(ctx, id); err != context.DeadlineExceeded {
		t.Fatalf("WaitCommitted without a quorum = %v, want deadline exceeded", err)
	}
}
