package hotstuff

import (
	"fmt"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls/testca"
)

// Byzantine fault suite (n = 4, f = 1, quorum = 3), at CLUSTER level. The
// single-replica suite already pins forged senders, tampering and fabricated
// high QCs; here a Byzantine leader actually splits the correct replicas with
// conflicting proposals and the cluster must come out with one committed
// history — no fork, no lost liveness.

// signedAs builds a proposal attributed to signer, with a valid signature. It
// is how these tests PLAY a Byzantine leader: the messages are authentic (a
// Byzantine validator holds a real key and signs real messages), only their
// content is malicious.
func signedAs(signer *Replica, view, height uint64, parent, cmd string, j *QC) *Proposal {
	blk := Block{View: view, Height: height, Parent: parent, Cmd: []byte(cmd), Justify: j}
	blk.ID = blockID(view, height, parent, blk.Cmd)
	p := &Proposal{Block: blk, From: signer.id}
	p.Sig = signer.sign(p)
	return p
}

// genesisQC returns a replica's current highest QC (the genesis root QC before
// any progress), which is the justification a first proposal carries.
func genesisQC(n *Replica) *QC {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.qcHigh
}

// TestClusterByzantineLeaderEquivocationNoFork: the view-0 leader sends
// conflicting blocks at one height to different replicas (equivocation, §8.2),
// and keeps the resulting QC to itself. This is the most safety-critical shape
// of the attack: the Byzantine leader can reach a quorum on the branch only it
// can see, while the correct replicas are split across two branches.
//
// What must hold:
//
//   - no correct replica ever learns a QC above genesis, so none of them moved
//     its lock — the precondition for a safe view change;
//   - neither branch commits (a single QC cannot commit its own block, and no
//     correct replica builds on either branch);
//   - after the view change the new leader resumes from genesis and the cluster
//     converges on one identical committed history that contains neither
//     equivocated block.
//
// A Byzantine leader that DOES broadcast its QC instead commits the branch it
// certified — that branch then wins outright and the other is abandoned, which
// is safety-preserving too (see TestVoteOncePerHeight for the vote rule that
// makes both cases possible).
func TestClusterByzantineLeaderEquivocationNoFork(t *testing.T) {
	ids := []string{"L0", "L1", "L2", "L3"}
	nw, nodes := startCluster(t, ids, false, 0, false)
	defer nw.stop()

	lead := nodes["L0"] // the Byzantine leader: authentic key, conflicting content
	gq := genesisQC(lead)
	pA := signedAs(lead, 0, 1, genesisID, "A", gq)
	pB := signedAs(lead, 0, 1, genesisID, "B", gq)
	// The leader self-votes for A (as any proposer does) and holds both blocks.
	nodes["L0"].HandleProposal(pA)
	nodes["L0"].HandleProposal(pB)

	// Two correct replicas see A, the third sees B — an honest replica accepts
	// whichever authentic block reaches it first and votes for it.
	nodes["L1"].HandleProposal(pA)
	nodes["L2"].HandleProposal(pA)
	nodes["L3"].HandleProposal(pB)

	// The votes reach the leader: A collects three (its own + L1 + L2, a quorum
	// the leader keeps private), B only L3's.
	waitForVotes(t, lead, 4)
	lead.mu.Lock()
	qsA := lead.qcFormed[pA.Block.ID]
	votesA := len(lead.votes[pA.Block.ID])
	votesB := len(lead.votes[pB.Block.ID])
	lead.mu.Unlock()
	if !qsA {
		t.Fatalf("test setup: the leader should hold a quorum on A (votes=%d)", votesA)
	}
	if votesB != 1 {
		t.Fatalf("B must not reach a quorum, got %d votes", votesB)
	}

	// No CORRECT replica knows any QC above genesis — the leader withheld the
	// certificate — so none of them locked on either branch.
	for _, id := range []string{"L1", "L2", "L3"} {
		nodes[id].mu.Lock()
		qcNode, lock := nodes[id].qcHigh.NodeID, nodes[id].bLock
		nodes[id].mu.Unlock()
		if qcNode != genesisID {
			t.Fatalf("%s learned a QC above genesis (%q)", id, qcNode)
		}
		if lock != genesisID {
			t.Fatalf("%s locked on an equivocated branch (%q)", id, lock)
		}
	}
	for _, id := range ids {
		if got := nw.logs[id].snapshot(); len(got) != 0 {
			t.Fatalf("%s committed %v", id, got)
		}
	}

	// The three correct replicas suspect the stalled leader and move to view 1
	// (leaderOf(1) = L1), which resumes from the highest QC it KNOWS (genesis).
	for _, id := range []string{"L1", "L2", "L3"} {
		nodes[id].StartViewChange()
	}
	if !nodes["L1"].IsLeader() {
		t.Fatal("L1 must become the active leader of view 1")
	}
	nodes["L1"].mu.Lock()
	base, bLock := nodes["L1"].head, nodes["L1"].bLock
	nodes["L1"].mu.Unlock()
	if base != genesisID || bLock != genesisID {
		t.Fatalf("new leader must resume from genesis (head=%q lock=%q)", base, bLock)
	}

	// Consensus resumes, and neither equivocated block ever appears in a
	// committed history anywhere.
	proposeLoop(t, nodes["L1"], []string{"c", "d"}, 5)
	waitAllApplied(t, nw, ids, []string{"c", "d"})
	for _, id := range ids {
		if got := fmt.Sprint(nw.logs[id].snapshot()); got != "[c d]" {
			t.Fatalf("%s applied %v, want exactly [c d]", id, got)
		}
	}
}

// waitForVotes waits until the leader has aggregated at least n votes for the
// equivocated block (the in-memory transport delivers synchronously, but the
// handlers run on the calling goroutine, so this is a cheap safety net).
func waitForVotes(t *testing.T, lead *Replica, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lead.mu.Lock()
		total := 0
		for _, set := range lead.votes {
			total += len(set)
		}
		lead.mu.Unlock()
		if total >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader aggregated %d votes, want at least %d", total, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestEquivocatedForkBlockIsNeverVotedFor: a correct replica that already voted
// at a height accepts the leader's conflicting sibling block into its tree (it
// cannot tell which branch is "real", and the block is authentic) but must
// never vote for it — that is what keeps at most one branch per height able to
// collect a quorum.
func TestEquivocatedForkBlockIsNeverVotedFor(t *testing.T) {
	f, tr := newFollower() // F votes to the phantom leader L0

	// F votes for the first block it receives at height 1...
	pA := prop(1, genesisID, "A", quorumQC(genesisID, 0))
	f.HandleProposal(pA)
	if got := tr.voteCount(pA.Block.ID); got != 1 {
		t.Fatalf("first authentic block must earn F's vote, got %d", got)
	}

	// ...and the equivocated sibling (same height, different content) must not
	// earn a second vote, whatever the leader claims.
	pB := prop(1, genesisID, "B", quorumQC(genesisID, 0))
	f.HandleProposal(pB)
	if got := tr.voteCount(pB.Block.ID); got != 0 {
		t.Fatalf("the equivocated sibling earned %d vote(s)", got)
	}
	f.mu.Lock()
	vHeight := f.vHeight
	f.mu.Unlock()
	if vHeight != 1 {
		t.Fatalf("vote height moved to %d; a replica votes once per height", vHeight)
	}
}

// waitLeader polls until n reports itself the active leader of its view (view
// changes and their certificates travel asynchronously over real TCP).
func waitLeader(t *testing.T, n *Replica, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never became the active leader (view %d)", n.ID(), n.View())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestTCPClusterByzantineLeaderViewChangeResumes (integration, §8.5): over real
// TCP with mutual TLS, a Byzantine view-0 leader equivocates and then makes no
// progress. The correct replicas time out into view 1, the new leader adopts
// the highest genuine QC, and the cluster commits the post-change commands —
// with identical committed prefixes on every replica.
func TestTCPClusterByzantineLeaderViewChangeResumes(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"b0", "b1", "b2", "b3"}
	c := startTCPCluster(t, ids, ca)

	lead := c.nodes["b0"]
	gq := genesisQC(lead)
	pA := signedAs(lead, 0, 1, genesisID, "A", gq)
	pB := signedAs(lead, 0, 1, genesisID, "B", gq)
	// The Byzantine leader holds both conflicting blocks but sends only one to
	// each side of the split.
	c.nodes["b0"].HandleProposal(pA)
	c.nodes["b0"].HandleProposal(pB)
	c.nodes["b1"].HandleProposal(pA)
	c.nodes["b2"].HandleProposal(pA)
	c.nodes["b3"].HandleProposal(pB)

	// The votes travel over the mTLS mesh to the Byzantine leader, which holds a
	// private quorum on A (A=3, B=1) and withholds it. No correct replica learns
	// a QC above genesis, so no honest replica locked on a branch it cannot see.
	deadline := time.Now().Add(5 * time.Second)
	for {
		lead.mu.Lock()
		votesA, votesB := len(lead.votes[pA.Block.ID]), len(lead.votes[pB.Block.ID])
		lead.mu.Unlock()
		if votesA+votesB >= 4 {
			if votesA < 3 {
				t.Fatalf("test setup: the leader should hold a quorum on A, got %d", votesA)
			}
			if votesB >= 3 {
				t.Fatalf("B reached a quorum (%d votes) — a correct replica voted twice", votesB)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("votes did not reach the leader over TCP")
		}
		time.Sleep(2 * time.Millisecond)
	}
	for _, id := range []string{"b1", "b2", "b3"} {
		c.nodes[id].mu.Lock()
		qcNode, lock := c.nodes[id].qcHigh.NodeID, c.nodes[id].bLock
		c.nodes[id].mu.Unlock()
		if qcNode != genesisID || lock != genesisID {
			t.Fatalf("%s learned/locked on the withheld branch (qc=%q lock=%q)", id, qcNode, lock)
		}
	}
	if got := len(c.logs["b1"].snapshot()); got != 0 {
		t.Fatal("commands were applied before any commit was possible")
	}

	// The three correct replicas suspect the stalled leader and move to view 1
	// (leaderOf(1) = b1). Over real TCP the view changes travel asynchronously,
	// so wait for the quorum to assemble.
	for _, id := range []string{"b1", "b2", "b3"} {
		c.nodes[id].StartViewChange()
	}
	waitLeader(t, c.nodes["b1"], 5*time.Second)
	proposeLoop(t, c.nodes["b1"], []string{"c", "d"}, 6)
	c.waitApplied(t, []string{"c", "d"})

	// Safety: one committed prefix everywhere, and it contains only the
	// post-view-change commands — the equivocated blocks never committed.
	for _, id := range ids {
		if got := fmt.Sprint(c.logs[id].snapshot()); got != "[c d]" {
			t.Fatalf("%s applied %v, want [c d]", id, got)
		}
	}
}
