package pbft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// This file is the network-level E2E suite. Unlike the deterministic unit
// tests, it drives replicas through an ASYNCHRONOUS in-memory network
// (netSim/asyncTransport) that can inject the real-world faults of distributed
// systems — message loss, duplication, delay/reordering, and per-link
// partitions — plus Byzantine messages at the primary/backup. It also pins the
// PBFT quorum boundaries (3f+1 / 2f+1) and the core safety invariant (no two
// correct replicas ever commit different operations at the same sequence
// number, across views).

// ---- asynchronous fault-injecting network ----

// netSim routes messages between replicas through asyncTransport. Policies are
// read under the lock on every send, so tests can change them mid-run (e.g. to
// heal a partition). kind is "PrePrepare"/"Prepare"/"Commit"/"ViewChange"/
// "NewView".
type netSim struct {
	mu      sync.Mutex
	nodes   map[string]*Replica
	drop    func(from, to, kind string) bool
	dup     int // extra copies of every delivered message
	delayFn func(from, to, kind string) time.Duration
}

func (n *netSim) setDrop(f func(from, to, kind string) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop = f
}

// dropAll blocks every message from -> to (both a message sink and a
// partition).
func (n *netSim) dropAll(from, to string) {
	n.setDrop(func(a, b, _ string) bool { return (a == from && b == to) || (a == to && b == from) })
}

func (n *netSim) setDup(k int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dup = k
}

func (n *netSim) setDelay(f func(from, to, kind string) time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delayFn = f
}

type asyncTransport struct {
	net  *netSim
	from string
}

func (t *asyncTransport) deliver(peer, kind string, apply func(*Replica)) error {
	t.net.mu.Lock()
	node := t.net.nodes[peer]
	drop := t.net.drop != nil && t.net.drop(t.from, peer, kind)
	dup := t.net.dup
	var delay time.Duration
	if t.net.delayFn != nil {
		delay = t.net.delayFn(t.from, peer, kind)
	}
	t.net.mu.Unlock()
	if node == nil {
		return errors.New("pbft e2e: no such peer " + peer)
	}
	if drop {
		return nil
	}
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		for i := 0; i <= dup; i++ {
			apply(node)
		}
	}()
	return nil
}

func (t *asyncTransport) SendPrePrepare(_ context.Context, peer string, m *PrePrepare) error {
	return t.deliver(peer, "PrePrepare", func(r *Replica) { r.HandlePrePrepare(m) })
}
func (t *asyncTransport) SendPrepare(_ context.Context, peer string, m *Prepare) error {
	return t.deliver(peer, "Prepare", func(r *Replica) { r.HandlePrepare(m) })
}
func (t *asyncTransport) SendCommit(_ context.Context, peer string, m *Commit) error {
	return t.deliver(peer, "Commit", func(r *Replica) { r.HandleCommit(m) })
}
func (t *asyncTransport) SendViewChange(_ context.Context, peer string, m *ViewChange) error {
	return t.deliver(peer, "ViewChange", func(r *Replica) { r.HandleViewChange(m) })
}
func (t *asyncTransport) SendNewView(_ context.Context, peer string, m *NewView) error {
	return t.deliver(peer, "NewView", func(r *Replica) { r.HandleNewView(m) })
}

// startAsyncCluster boots replicas over the asynchronous fault-injecting
// network.
func startAsyncCluster(t *testing.T, ids []string) (map[string]*Replica, map[string]*fsm, *netSim) {
	t.Helper()
	peersOf := func(id string) []string {
		var out []string
		for _, other := range ids {
			if other != id {
				out = append(out, other)
			}
		}
		return out
	}
	net := &netSim{nodes: make(map[string]*Replica)}
	nodes := make(map[string]*Replica)
	fsms := make(map[string]*fsm)
	for _, id := range ids {
		f := &fsm{}
		r, err := NewReplica(Config{ID: id, Peers: peersOf(id)}, &asyncTransport{net: net, from: id}, f.apply)
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		net.mu.Lock()
		net.nodes[id] = r
		net.mu.Unlock()
		nodes[id] = r
		fsms[id] = f
	}
	// Dynamic key exchange (M3), done out-of-band: without each peer's identity
	// key registered, every signed message would fail verification.
	wirePeerKeys(nodes)
	return nodes, fsms, net
}

// agreed returns the shared applied history once all ids agree, else false.
func agreed(fsms map[string]*fsm, ids []string) ([]string, bool) {
	s := fsms[ids[0]].snapshot()
	for _, id := range ids[1:] {
		if !equal(fsms[id].snapshot(), s) {
			return nil, false
		}
	}
	return s, true
}

func waitAgreed(t *testing.T, what string, timeout time.Duration, fsms map[string]*fsm, ids []string) []string {
	t.Helper()
	// Over the async net replicas converge entry-by-entry, so a shared history
	// of length 1 may be an intermediate step on the way to a longer one. Only
	// treat the agreement as final once the shared history has stayed unchanged
	// for a quiet window (quiescence), then return it.
	var stable []string
	stableSince := time.Time{}
	waitFor(t, what, timeout, func() bool {
		cur, ok := agreed(fsms, ids)
		if !ok || len(cur) == 0 {
			stableSince = time.Time{}
			return false
		}
		if !equal(cur, stable) {
			stable, stableSince = cur, time.Now()
			return false
		}
		return time.Since(stableSince) >= 50*time.Millisecond
	})
	return stable
}

// =====================================================================
// 1. Normal case
// =====================================================================

// TestE2ESingleRequestCommits is the minimal consensus case over the async
// network: one request submitted to the 4-replica cluster commits everywhere.
func TestE2ESingleRequestCommits(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := waitAgreed(t, "single request to commit everywhere", 5*time.Second, fsms, ids); len(got) != 1 || got[0] != "1:a" {
		t.Fatalf("agreed = %v, want [1:a]", got)
	}
}

// TestE2EConsecutiveRequestsKeepOrder submits several requests back to back and
// requires every replica to apply them in the same order.
func TestE2EConsecutiveRequestsKeepOrder(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	for _, c := range []string{"a", "b", "c", "d"} {
		if _, err := nodes["r0"].Submit([]byte(c)); err != nil {
			t.Fatalf("Submit(%s): %v", c, err)
		}
	}
	want := []string{"1:a", "2:b", "3:c", "4:d"}
	if got := waitAgreed(t, "consecutive requests in order", 5*time.Second, fsms, ids); !equal(got, want) {
		t.Fatalf("agreed = %v, want %v", got, want)
	}
}

// TestE2EConcurrentClientsTotalOrder fires many submissions from concurrent
// "clients" and requires a single total order replicated identically on every
// node — no two nodes may ever disagree on which command sits at a sequence.
func TestE2EConcurrentClientsTotalOrder(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	maxSeq := uint64(0)
	var seqMu sync.Mutex
	for i := 0; i < n; i++ {
		c := string(rune('a' + i))
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			seq, err := nodes["r0"].Submit([]byte(c))
			if err != nil {
				errCh <- err
				return
			}
			seqMu.Lock()
			if seq > maxSeq {
				maxSeq = seq
			}
			seqMu.Unlock()
		}(c)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Submit: %v", err)
	}
	// The primary applies requests in order; wait until it has executed all n
	// before checking for cross-node agreement (deterministic anchor — the
	// peers converge shortly after, and waitAgreed's quiescence window then
	// returns the final shared history rather than an intermediate length).
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := nodes["r0"].WaitApplied(ctx, maxSeq); err != nil {
		t.Fatalf("primary did not apply all %d requests: %v", n, err)
	}
	cancel()
	got := waitAgreed(t, "all concurrent requests in one total order", 10*time.Second, fsms, ids)
	if len(got) != n {
		t.Fatalf("agreed %d entries, want %d", len(got), n)
	}
	// Every node applied the same history: for each sequence number a distinct
	// command, covering exactly the n submitted commands.
	bySeq := make(map[uint64]string)
	seen := make(map[string]bool)
	for _, s := range got {
		var seq uint64
		var op string
		if _, err := fmtSscanf(s, &seq, &op); err != nil {
			t.Fatalf("bad history entry %q", s)
		}
		if prev, ok := bySeq[seq]; ok && prev != op {
			t.Fatalf("seq %d holds both %q and %q", seq, prev, op)
		}
		bySeq[seq] = op
		seen[op] = true
	}
	for i := 0; i < n; i++ {
		if !seen[string(rune('a'+i))] {
			t.Fatalf("command %q missing from the total order", string(rune('a'+i)))
		}
	}
}

// TestE2EDuplicateDeliverySingleCommit re-delivers every message several times
// and verifies each request still commits exactly once (no duplicate commit).
func TestE2EDuplicateDeliverySingleCommit(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	net.setDup(4) // every message delivered 5x
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := nodes["r0"].Submit([]byte("b")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	want := []string{"1:a", "2:b"}
	if got := waitAgreed(t, "duplicates to not double-commit", 5*time.Second, fsms, ids); !equal(got, want) {
		t.Fatalf("agreed = %v, want %v (each request must commit exactly once)", got, want)
	}
}

// =====================================================================
// 2. Byzantine primary
// =====================================================================

// TestE2EPrimarySilentViewChange makes the view-0 primary unable to order (its
// PRE-PREPAREs never leave, so a submitted request stalls). The backups' view
// change promotes a new primary; the old primary, still functional as a backup,
// keeps the cluster making progress on new work.
func TestE2EPrimarySilentViewChange(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	// Primary r0 cannot order: only its PRE-PREPAREs are dropped (it still
	// functions as a backup in later views).
	net.setDrop(func(from, _, kind string) bool { return from == "r0" && kind == "PrePrepare" })
	if _, err := nodes["r0"].Submit([]byte("stuck")); err != nil {
		t.Fatalf("Submit on silent primary: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	// Nothing commits while the primary cannot pre-prepare.
	for _, id := range ids {
		if len(fsms[id].snapshot()) != 0 {
			t.Fatalf("%s committed %v despite a silent primary", id, fsms[id].snapshot())
		}
	}
	// The correct backups suspect r0; r1 (view 1) becomes the primary.
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "r1 to lead view 1", 5*time.Second, func() bool {
		return nodes["r1"].View() == 1 && nodes["r1"].Primary() == "r1"
	})
	// Wait for the old primary to join view 1 as a backup (NewView is async).
	waitFor(t, "r0 to join view 1", 5*time.Second, func() bool {
		return nodes["r0"].View() == 1
	})
	// The new primary orders new work; the old primary participates as a backup,
	// so all four replicas converge.
	seq, err := nodes["r1"].Submit([]byte("ok"))
	if err != nil {
		t.Fatalf("Submit on new primary: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := nodes["r1"].WaitApplied(ctx, seq); err != nil {
		t.Fatalf("WaitApplied on new primary: %v", err)
	}
	if got := waitAgreed(t, "all replicas to apply the post-view-change write", 5*time.Second, fsms, ids); !equal(got, []string{"1:ok"}) {
		t.Fatalf("agreed = %v, want [1:ok]", got)
	}
}

// TestE2ETamperedPrimaryMessagesIgnored sends a Byzantine primary message with
// a wrong digest / wrong view and requires it not to disturb a live cluster.
func TestE2ETamperedPrimaryMessagesIgnored(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	req := Request{Client: "c", Timestamp: 99, Command: []byte("evil")}
	// Wrong digest: signed, then the digest field is tampered with.
	pp := &PrePrepare{View: 0, Seq: 5, Digest: digestOf(req), Req: req, Sender: "r0"}
	pp.Sig = nodes["r0"].sign(pp)
	pp.Digest = "deadbeef"
	nodes["r1"].HandlePrePrepare(pp)
	// Wrong view message (view 3) from the "primary" is ignored too.
	wv := &PrePrepare{View: 3, Seq: 1, Digest: digestOf(req), Req: req, Sender: "r0"}
	wv.Sig = nodes["r0"].sign(wv)
	nodes["r1"].HandlePrePrepare(wv)
	// A legitimate request still commits everywhere afterwards.
	if _, err := nodes["r0"].Submit([]byte("good")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := waitAgreed(t, "legitimate request despite tampered messages", 5*time.Second, fsms, ids); !equal(got, []string{"1:good"}) {
		t.Fatalf("agreed = %v, want [1:good]", got)
	}
}

// =====================================================================
// 3. Byzantine replica
// =====================================================================

// TestE2EByzantineBackupEquivocationAndFakeCommit verifies a Byzantine backup
// that sends PREPAREs/COMMITs for digests that were never proposed cannot
// disturb the correct replicas: they commit the genuine request and ignore the
// garbage.
func TestE2EByzantineBackupEquivocationAndFakeCommit(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	reqA := Request{Client: "c", Timestamp: 7, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 8, Command: []byte("B")} // never proposed
	// The primary orders A@1 to every backup.
	if _, err := nodes["r0"].Submit([]byte("A")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	// Byzantine r3: a PREPARE for a never-proposed B@1, and a bogus COMMIT for
	// a sequence that does not exist.
	bogusPrep := &Prepare{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: "r3"}
	bogusPrep.Sig = nodes["r3"].sign(bogusPrep)
	nodes["r1"].HandlePrepare(bogusPrep)
	nodes["r2"].HandlePrepare(bogusPrep)
	fakeCommit := &Commit{View: 0, Seq: 99, Digest: digestOf(reqA), Sender: "r3"}
	fakeCommit.Sig = nodes["r3"].sign(fakeCommit)
	nodes["r1"].HandleCommit(fakeCommit)
	_ = reqA
	// All correct replicas still converge on exactly A@1.
	if got := waitAgreed(t, "correct replicas to commit A despite byzantine backup", 5*time.Second, fsms, ids); !equal(got, []string{"1:A"}) {
		t.Fatalf("agreed = %v, want [1:A]", got)
	}
}

// TestE2EByzantineBackupEquivocationAcrossPeers: a Byzantine backup tells r1 it
// prepared A and r2 it prepared a conflicting B (both under its own key). A
// correct replica must never count both, and the genuine A must commit.
func TestE2EByzantineBackupEquivocationAcrossPeers(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	reqB := Request{Client: "c", Timestamp: 8, Command: []byte("B")}
	if _, err := nodes["r0"].Submit([]byte("A")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	// Byzantine r3 equivocates: it "prepared" the (never proposed) B@1 for r1
	// only. r1 is on the genuine A, so this must be ignored.
	eq := &Prepare{View: 0, Seq: 1, Digest: digestOf(reqB), Sender: "r3"}
	eq.Sig = nodes["r3"].sign(eq)
	nodes["r1"].HandlePrepare(eq)
	if got := waitAgreed(t, "correct replicas to converge on A", 5*time.Second, fsms, ids); !equal(got, []string{"1:A"}) {
		t.Fatalf("agreed = %v, want [1:A]", got)
	}
}

// TestE2ETwoCorrectCannotCommit pins the 2f+1 boundary: with f=1 and only two
// correct, reachable replicas (the primary plus one backup), a request must
// never commit.
func TestE2ETwoCorrectCannotCommit(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	// r2 and r3 are Byzantine/silent: nothing they send is delivered.
	net.setDrop(func(_, to, _ string) bool { return to == "r2" || to == "r3" })
	seq, err := nodes["r0"].Submit([]byte("a"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := nodes["r0"].WaitApplied(ctx, seq); err == nil {
		t.Fatalf("request committed with only two correct replicas (f=1 needs 2f+1=3)")
	}
	for _, id := range []string{"r0", "r1"} {
		if len(fsms[id].snapshot()) != 0 {
			t.Fatalf("%s committed %v below the 2f+1 quorum", id, fsms[id].snapshot())
		}
	}
}

// =====================================================================
// 4. View change
// =====================================================================

// TestE2EViewChangeResumesWrites: after a completed view change, ordinary
// writes continue to order through the new primary (no regression right after
// the switch).
func TestE2EViewChangeResumesWrites(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	// Commit a baseline in view 0.
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := waitAgreed(t, "baseline commit", 5*time.Second, fsms, ids); !equal(got, []string{"1:a"}) {
		t.Fatalf("baseline = %v", got)
	}
	// Primary r0 fails; backups move to view 1 (r1) and keep ordering.
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "r1 to lead view 1", 5*time.Second, func() bool {
		return nodes["r1"].View() == 1 && nodes["r1"].Primary() == "r1"
	})
	// Every replica must actually enter view 1 before new writes: a backup
	// still in view 0 would reject the view-1 pre-prepares and stall ordering
	// (this can lag under load, so wait for the whole cluster to switch).
	waitFor(t, "all replicas to join view 1", 5*time.Second, func() bool {
		for _, id := range ids {
			if nodes[id].View() != 1 {
				return false
			}
		}
		return true
	})
	// Several writes right after the view change still land, in order, everywhere.
	for _, c := range []string{"b", "c"} {
		seq, err := nodes["r1"].Submit([]byte(c))
		if err != nil {
			t.Fatalf("Submit(%s) after view change: %v", c, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := nodes["r1"].WaitApplied(ctx, seq); err != nil {
			t.Fatalf("WaitApplied(%s): %v", c, err)
		}
		cancel()
	}
	if got := waitAgreed(t, "writes after view change in order", 5*time.Second, fsms, ids); !equal(got, []string{"1:a", "2:b", "3:c"}) {
		t.Fatalf("agreed = %v, want [1:a 2:b 3:c]", got)
	}
}

// TestE2EDuplicateViewChangeSingleCount sends the same VIEW-CHANGE twice and a
// late one from an already-moved replica; neither may double-count or regress
// the view.
func TestE2EDuplicateViewChangeSingleCount(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	nodes["r0"].Submit([]byte("a"))
	waitAgreed(t, "baseline", 5*time.Second, fsms, ids)
	nodes["r1"].StartViewChange()
	nodes["r1"].StartViewChange() // duplicate: must be a no-op
	nodes["r2"].StartViewChange()
	nodes["r3"].StartViewChange()
	waitFor(t, "view change to complete", 5*time.Second, func() bool {
		return nodes["r1"].View() == 1 && nodes["r2"].View() == 1 && nodes["r3"].View() == 1
	})
	// A late duplicate view-change for the already-joined view is ignored.
	nodes["r3"].StartViewChange()
	time.Sleep(100 * time.Millisecond)
	for _, id := range []string{"r1", "r2", "r3"} {
		if nodes[id].View() != 1 {
			t.Fatalf("%s regressed from view 1 to %d", id, nodes[id].View())
		}
	}
}

// TestE2EBadNewViewRejected drives a Byzantine NEW-VIEW: the (view-1) primary
// builds O that displaces a request another view-change certifies as prepared.
// A correct replica must reject it and stay in the old view.
func TestE2EBadNewViewRejected(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA, reqB := preparedA_vs_mereB(t, nodes, fsms)
	dA, dB := digestOf(reqA), digestOf(reqB)
	// Rebuild the real messages so we can hand-craft the (valid) view-changes
	// that a correct r1 and r2 would send: A@1 certified by two backups.
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: dA, Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	prep := func(from string) *Prepare {
		p := &Prepare{View: 0, Seq: 1, Digest: dA, Sender: from}
		p.Sig = nodes[from].sign(p)
		return p
	}
	vcR1 := &ViewChange{View: 1, S: 0, Sender: "r1", Entries: []ViewEntry{{Seq: 1, Digest: dA, Req: reqA,
		Cert: &PreparedCert{PrePrepare: ppA, Prepares: []*Prepare{prep("r2"), prep("r3")}}}}}
	vcR1.Sig = nodes["r1"].sign(vcR1)
	vcR2 := &ViewChange{View: 1, S: 0, Sender: "r2", Entries: []ViewEntry{{Seq: 1, Digest: dA, Req: reqA,
		Cert: &PreparedCert{PrePrepare: ppA, Prepares: []*Prepare{prep("r1"), prep("r3")}}}}}
	vcR2.Sig = nodes["r2"].sign(vcR2)
	vcR3 := &ViewChange{View: 1, S: 0, Sender: "r3", Entries: []ViewEntry{{Seq: 1, Digest: dB, Req: reqB}}}
	vcR3.Sig = nodes["r3"].sign(vcR3)
	// Byzantine NEW-VIEW from r1 (view-1 primary): V carries the certified A,
	// but O proposes the merely-known B at seq 1 — displacing a prepared request.
	badO := &PrePrepare{View: 1, Seq: 1, Digest: dB, Req: reqB, Sender: "r1"}
	badO.Sig = nodes["r1"].sign(badO)
	nv := &NewView{View: 1, V: []*ViewChange{vcR1, vcR2, vcR3}, O: []PrePrepare{*badO}, Sender: "r1"}
	nv.Sig = nodes["r1"].sign(nv)
	// A correct replica (r2) must reject it: it never moves to view 1 and does
	// not adopt the displaced request.
	nodes["r2"].HandleNewView(nv)
	nodes["r2"].mu.Lock()
	view := nodes["r2"].view
	e := nodes["r2"].log[1]
	digest := ""
	if e != nil {
		digest = e.digest
	}
	nodes["r2"].mu.Unlock()
	if view != 0 {
		t.Fatalf("r2 moved to view %d despite a NEW-VIEW displacing a prepared request", view)
	}
	if digest == dB {
		t.Fatal("r2 adopted the merely-known B displaced over the certified A")
	}
}

// TestE2ENewViewUnknownRequestRejected: a NEW-VIEW replaying a request that no
// collected view-change carried must be rejected (the request is not in V).
func TestE2ENewViewUnknownRequestRejected(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA, _ := preparedA_vs_mereB(t, nodes, fsms)
	dA := digestOf(reqA)
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: dA, Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	prep := func(from string) *Prepare {
		p := &Prepare{View: 0, Seq: 1, Digest: dA, Sender: from}
		p.Sig = nodes[from].sign(p)
		return p
	}
	mkVc := func(sender string) *ViewChange {
		vc := &ViewChange{View: 1, S: 0, Sender: sender, Entries: []ViewEntry{{Seq: 1, Digest: dA, Req: reqA,
			Cert: &PreparedCert{PrePrepare: ppA, Prepares: []*Prepare{prep("r1"), prep("r3")}}}}}
		vc.Sig = nodes[sender].sign(vc)
		return vc
	}
	extra := &PrePrepare{View: 1, Seq: 2, Digest: "extra", Req: Request{Client: "c", Timestamp: 42, Command: []byte("x")}, Sender: "r1"}
	extra.Sig = nodes["r1"].sign(extra)
	// O carries seq 2, which appears in none of the collected view-changes.
	nv := &NewView{View: 1, V: []*ViewChange{mkVc("r1"), mkVc("r2"), mkVc("r3")}, O: []PrePrepare{*extra}, Sender: "r1"}
	nv.Sig = nodes["r1"].sign(nv)
	nodes["r2"].HandleNewView(nv)
	nodes["r2"].mu.Lock()
	view := nodes["r2"].view
	nodes["r2"].mu.Unlock()
	if view != 0 {
		t.Fatalf("r2 moved to view %d despite a NEW-VIEW with a request not in V", view)
	}
}

// =====================================================================
// 5. Network faults
// =====================================================================

// TestE2EPrePrepareLossRecoversByViewChange drops a PRE-PREPARE to one backup.
// Because a prepared certificate needs 2f prepares from the other backups, the
// request cannot commit while one backup never learns it — the cluster stalls
// (safety preserved, liveness waits). Recovery is a view change, whose NEW-VIEW
// re-proposes the request to every replica, after which all four converge.
func TestE2EPrePrepareLossRecoversByViewChange(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	net.setDrop(func(from, to, kind string) bool { return from == "r0" && to == "r3" && kind == "PrePrepare" })
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	// A lost pre-prepare stalls the request: nothing commits anywhere.
	for _, id := range ids {
		if len(fsms[id].snapshot()) != 0 {
			t.Fatalf("%s committed %v despite the lost pre-prepare", id, fsms[id].snapshot())
		}
	}
	// Heal the link and recover via a view change that re-proposes the request.
	net.setDrop(nil)
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "r1 to lead view 1", 5*time.Second, func() bool {
		return nodes["r1"].View() == 1 && nodes["r1"].Primary() == "r1"
	})
	waitFor(t, "all replicas to join view 1", 5*time.Second, func() bool {
		for _, id := range ids {
			if nodes[id].View() != 1 {
				return false
			}
		}
		return true
	})
	if got := waitAgreed(t, "lost request to be recovered via view change", 5*time.Second, fsms, ids); !equal(got, []string{"1:a"}) {
		t.Fatalf("agreed = %v, want [1:a]", got)
	}
}

// TestE2ECommitLossStillCommits drops a COMMIT to one backup. A commit
// certificate needs 2f+1 commits total, so a single lost COMMIT among four
// correct replicas is masked and the request still commits everywhere.
func TestE2ECommitLossStillCommits(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	// Drop only r1's COMMITs to r2.
	net.setDrop(func(from, to, kind string) bool { return from == "r1" && to == "r2" && kind == "Commit" })
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := waitAgreed(t, "commit despite one lost COMMIT", 5*time.Second, fsms, ids); !equal(got, []string{"1:a"}) {
		t.Fatalf("agreed = %v, want [1:a]", got)
	}
}

// TestE2EReorderingConverges delays PRE-PREPAREs to r2 so prepares/commits from
// others race ahead; the engine must buffer out-of-order messages and still
// converge on one ordered history.
func TestE2EReorderingConverges(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	net.setDelay(func(from, to, kind string) time.Duration {
		if to == "r2" && kind == "PrePrepare" {
			return 60 * time.Millisecond
		}
		return 0
	})
	for _, c := range []string{"a", "b", "c"} {
		if _, err := nodes["r0"].Submit([]byte(c)); err != nil {
			t.Fatalf("Submit(%s): %v", c, err)
		}
	}
	want := []string{"1:a", "2:b", "3:c"}
	if got := waitAgreed(t, "reordered messages to converge in order", 8*time.Second, fsms, ids); !equal(got, want) {
		t.Fatalf("agreed = %v, want %v", got, want)
	}
}

// TestE2ETemporaryPartitionHeals isolates one backup from the cluster: new
// requests stall while it is out (its prepares are needed for the 2f quorum).
// Once the partition is removed, a view change re-proposes the stalled request
// and all four replicas converge — the healed node rejoins normally.
func TestE2ETemporaryPartitionHeals(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, net := startAsyncCluster(t, ids)
	net.dropAll("r3", "r0")
	net.dropAll("r3", "r1")
	net.dropAll("r3", "r2")
	if _, err := nodes["r0"].Submit([]byte("a")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	// While the backup is partitioned, the request cannot commit (no wrong
	// commit either — safety holds).
	for _, id := range ids {
		if len(fsms[id].snapshot()) != 0 {
			t.Fatalf("%s committed %v while a backup was partitioned", id, fsms[id].snapshot())
		}
	}
	// Heal the partition; a view change re-proposes the stalled request and all
	// four replicas (including the healed one) converge.
	net.setDrop(nil)
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "all replicas to join view 1", 5*time.Second, func() bool {
		for _, id := range ids {
			if nodes[id].View() != 1 {
				return false
			}
		}
		return true
	})
	if got := waitAgreed(t, "cluster to resume after the partition healed", 5*time.Second, fsms, ids); !equal(got, []string{"1:a"}) {
		t.Fatalf("agreed = %v, want [1:a]", got)
	}
}

// =====================================================================
// 6/7. Quorum boundary + safety invariant
// =====================================================================

// TestE2ESafetyNoConflictingCommitAtSameSeq runs a Byzantine-heavy sequence
// (an equivocating primary resolved through a view change, then new writes) and
// asserts the cross-cutting PBFT safety invariant after every step: no two
// correct replicas ever hold a different operation at the same sequence number,
// and no node commits two different operations at one sequence.
func TestE2ESafetyNoConflictingCommitAtSameSeq(t *testing.T) {
	ids := []string{"r0", "r1", "r2", "r3"}
	nodes, fsms, _ := startAsyncCluster(t, ids)
	// Phase 1: Byzantine primary r0 equivocates at seq 1 (A to r1, B to r2).
	reqA := Request{Client: "c", Timestamp: 7, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 7, Command: []byte("B")}
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqA), Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	nodes["r1"].HandlePrePrepare(ppA)
	ppB := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB.Sig = nodes["r0"].sign(ppB)
	nodes["r2"].HandlePrePrepare(ppB)
	time.Sleep(100 * time.Millisecond)
	// Phase 2: backups suspect; a view change must resolve seq 1 to ONE value.
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "cluster to converge after equivocation + view change", 5*time.Second, func() bool {
		_, ok := agreed(fsms, ids)
		return len(fsms["r1"].snapshot()) == 1 && ok
	})
	// Phase 3: new writes through the new primary.
	seq, err := nodes["r1"].Submit([]byte("c"))
	if err != nil {
		t.Fatalf("Submit after view change: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := nodes["r1"].WaitApplied(ctx, seq); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	history := waitAgreed(t, "final safety agreement", 5*time.Second, fsms, ids)
	// Safety: within one history, no sequence number repeats with a different op.
	bySeq := make(map[uint64]string)
	for _, s := range history {
		var seqn uint64
		var op string
		if _, err := fmtSscanf(s, &seqn, &op); err != nil {
			t.Fatalf("bad history entry %q", s)
		}
		if prev, ok := bySeq[seqn]; ok && prev != op {
			t.Fatalf("seq %d committed as both %q and %q on the same history", seqn, prev, op)
		}
		bySeq[seqn] = op
	}
}

// fmtSscanf parses a "seq:op" history entry.
func fmtSscanf(s string, seq *uint64, op *string) (int, error) {
	sep := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return 0, errors.New("no ':'")
	}
	var n uint64
	for i := 0; i < sep; i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, errors.New("bad seq")
		}
		n = n*10 + uint64(s[i]-'0')
	}
	*seq = n
	*op = s[sep+1:]
	return 2, nil
}
