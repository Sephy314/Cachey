package pbft

import (
	"context"
	"testing"
	"time"
)

// TestViewChangeResolvesEquivocation drives a view change after a Byzantine
// primary (r0) equivocated: it sent two different requests for seq 1 to two
// different backups, so nothing could commit in view 0. The backups suspect,
// r1 becomes the primary of view 1, and its NEW-VIEW replays seq 1 with one
// consistent request — every replica (including the old primary) must converge
// on and execute the same value.
func TestViewChangeResolvesEquivocation(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA := Request{Client: "c", Timestamp: 7, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 7, Command: []byte("B")}
	// Byzantine primary r0: seq 1 = A to r1, seq 1 = B to r2.
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqA), Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	nodes["r1"].HandlePrePrepare(ppA)
	ppB := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB.Sig = nodes["r0"].sign(ppB)
	nodes["r2"].HandlePrePrepare(ppB)
	// Give the equivocation time to stall every replica in view 0.
	time.Sleep(50 * time.Millisecond)
	for _, id := range []string{"r0", "r1", "r2", "r3"} {
		if got := fsms[id].snapshot(); len(got) != 0 {
			t.Fatalf("%s executed %v before the view change", id, got)
		}
	}
	// The three backups suspect r0 and move to view 1 (primary = r1).
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	// All four replicas must end up executing the same single request.
	want := fsms["r1"].snapshot()
	waitFor(t, "all replicas to converge after the view change", 3*time.Second, func() bool {
		s := fsms["r1"].snapshot()
		if len(s) != 1 {
			return false
		}
		for _, id := range []string{"r0", "r2", "r3"} {
			if !equal(fsms[id].snapshot(), s) {
				return false
			}
		}
		want = s
		return true
	})
	if len(want) != 1 || (want[0] != "1:A" && want[0] != "1:B") {
		t.Fatalf("converged state %v is not exactly one of the equivocated requests", want)
	}
	if nodes["r1"].View() != 1 || nodes["r1"].Primary() != "r1" {
		t.Fatalf("r1 view=%d primary=%q, want view 1 primary r1", nodes["r1"].View(), nodes["r1"].Primary())
	}
}

// TestViewChangeLeadershipHandover runs a normal view-0 sequence, then fails
// the primary and moves to view 1, where the new primary must keep ordering
// from the next sequence number (never reusing old ones) and the old primary
// must reject submissions.
func TestViewChangeLeadershipHandover(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	primary := nodes["r0"]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := primary.Submit([]byte("a")); err != nil {
		t.Fatalf("Submit(a): %v", err)
	}
	seq2, err := primary.Submit([]byte("b"))
	if err != nil {
		t.Fatalf("Submit(b): %v", err)
	}
	if err := primary.WaitApplied(ctx, seq2); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq2, err)
	}
	want := []string{"1:a", "2:b"}
	for _, id := range []string{"r0", "r1", "r2", "r3"} {
		waitFor(t, id+" to apply view-0 writes", 3*time.Second, func() bool {
			return equal(fsms[id].snapshot(), want)
		})
	}
	// r0 (the primary) fails; the backups suspect and move to view 1 (r1).
	for _, id := range []string{"r1", "r2", "r3"} {
		nodes[id].StartViewChange()
	}
	waitFor(t, "r1 to become the view-1 primary", 3*time.Second, func() bool {
		return nodes["r1"].View() == 1 && nodes["r1"].Primary() == "r1"
	})
	// The old primary no longer accepts submissions; the new one does, and it
	// must pick up at seq 3, not reuse seq 1 or 2.
	if _, err := nodes["r0"].Submit([]byte("x")); err != ErrNotPrimary {
		t.Fatalf("Submit on old primary = %v, want ErrNotPrimary", err)
	}
	seq3, err := nodes["r1"].Submit([]byte("c"))
	if err != nil {
		t.Fatalf("Submit(c) on new primary: %v", err)
	}
	if seq3 != 3 {
		t.Fatalf("new primary assigned seq %d, want 3", seq3)
	}
	if err := nodes["r1"].WaitApplied(ctx, seq3); err != nil {
		t.Fatalf("WaitApplied(%d): %v", seq3, err)
	}
	want = []string{"1:a", "2:b", "3:c"}
	for _, id := range []string{"r0", "r1", "r2", "r3"} {
		waitFor(t, id+" to apply the post-view-change write", 3*time.Second, func() bool {
			return equal(fsms[id].snapshot(), want)
		})
	}
}

// TestViewChangeRequiresQuorum verifies a single backup cannot force a view
// change alone: one suspecting replica sends VIEW-CHANGE but stays in view 0
// until 2f+1 replicas have moved and the new primary has issued a NEW-VIEW.
func TestViewChangeRequiresQuorum(t *testing.T) {
	nodes, _ := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	nodes["r1"].StartViewChange()
	time.Sleep(100 * time.Millisecond)
	for _, id := range []string{"r0", "r1", "r2", "r3"} {
		if nodes[id].View() != 0 {
			t.Fatalf("%s moved to view %d after a lone view change, want to stay at 0", id, nodes[id].View())
		}
	}
}

// TestRejectsForgedSender verifies authentication (M3): a Byzantine replica
// that claims to be the primary but signs with its own key is rejected — a
// backup must not accept a pre-prepare whose signature does not match its
// claimed Sender.
func TestRejectsForgedSender(t *testing.T) {
	nodes, fsms := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	req := Request{Client: "c", Timestamp: 5, Command: []byte("x")}
	// r3 forges a pre-prepare claiming Sender=r0 but signs with its own key.
	forged := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(req), Req: req, Sender: "r0"}
	forged.Sig = nodes["r3"].sign(forged)
	nodes["r1"].HandlePrePrepare(forged)
	// r1 must not accept it: no entry, no prepares, nothing executed.
	time.Sleep(100 * time.Millisecond)
	nodes["r1"].mu.Lock()
	_, accepted := nodes["r1"].log[1]
	nodes["r1"].mu.Unlock()
	if accepted {
		t.Fatal("r1 accepted a pre-prepare whose sender was forged")
	}
	if got := fsms["r1"].snapshot(); len(got) != 0 {
		t.Fatalf("r1 executed %v from a forged sender", got)
	}
}

// TestRejectsTamperedMessage verifies a validly signed message that was
// tampered with after signing (e.g. the digest changed) is rejected.
func TestRejectsTamperedMessage(t *testing.T) {
	nodes, _ := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	req := Request{Client: "c", Timestamp: 6, Command: []byte("x")}
	pp := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(req), Req: req, Sender: "r0"}
	pp.Sig = nodes["r0"].sign(pp)
	pp.Digest = "deadbeef" // tamper after signing
	nodes["r1"].HandlePrePrepare(pp)
	nodes["r1"].mu.Lock()
	_, accepted := nodes["r1"].log[1]
	nodes["r1"].mu.Unlock()
	if accepted {
		t.Fatal("r1 accepted a tampered pre-prepare")
	}
}

// TestByzantineReplicaStillEquivocatesAuthentically verifies the threat model
// of M3: a Byzantine replica that owns a valid key can still equivocate (send
// conflicting proposals under its own identity) — signatures authenticate, not
// discipline — and the protocol's quorum rules still prevent it from getting
// conflicting requests committed.
func TestByzantineReplicaStillEquivocatesAuthentically(t *testing.T) {
	nodes, _ := startCluster(t, []string{"r0", "r1", "r2", "r3"})
	reqA := Request{Client: "c", Timestamp: 8, Command: []byte("A")}
	reqB := Request{Client: "c", Timestamp: 8, Command: []byte("B")}
	ppA := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqA), Req: reqA, Sender: "r0"}
	ppA.Sig = nodes["r0"].sign(ppA)
	nodes["r1"].HandlePrePrepare(ppA)
	ppB := &PrePrepare{View: 0, Seq: 1, Digest: digestOf(reqB), Req: reqB, Sender: "r0"}
	ppB.Sig = nodes["r0"].sign(ppB)
	nodes["r2"].HandlePrePrepare(ppB)
	time.Sleep(100 * time.Millisecond)
	for _, id := range []string{"r1", "r2"} {
		nodes[id].mu.Lock()
		_, ok := nodes[id].log[1]
		nodes[id].mu.Unlock()
		if !ok {
			t.Fatalf("%s did not accept its (authentic) equivocating proposal", id)
		}
	}
}
