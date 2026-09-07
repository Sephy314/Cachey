package hotstuff

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"
)

// This file is the deterministic HS-M1 unit suite. It pins, at the engine
// level, the exact chain rules from docs/hotstuff.md §4.2 — 2-chain lock,
// 3-chain commit — plus the code-level invariants of §4.8:
//
//	QC requires ≥ 2f+1 valid member votes
//	Committed block ⇒ has a valid 3-chain
//	Committed blocks ⇒ form a single prefix
//	Conflicting blocks ⇒ cannot both be committed
//	Non-validator ⇒ cannot contribute to quorum
//
// All tests are synchronous and deterministic: no goroutines, timers or real
// networking. Transport/Auth/WAL are deliberately absent (HS-M1).

// testTimeout guards every blocking wait in this suite.
const testTimeout = 5 * time.Second

// ---- single-follower harness: a follower plus a phantom leader "L0" ----
//
// The follower F believes the cluster is {F, L0, P1, P2} (n = 4, f = 1) with
// L0 the leader. Because HS-M1 has no signatures, tests craft proposals FROM
// the leader carrying quorum QCs (≥ 2f+1 member ids) directly, which lets us
// drive the exact chain rules — forks, conflicts, stale blocks — without
// standing up a whole cluster.

const (
	testLeaderID = "L0"
	testPeer1    = "P1"
	testPeer2    = "P2"
)

// testKeyOf returns a deterministic Ed25519 keypair for a fake member id, so
// crafted-proposal tests can sign messages as "L0"/"P1"/"P2" and hand the
// matching public keys to the replica under test (HS-M3). Deterministic keys
// keep the tests reproducible.
func testKeyOf(id string) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("hotstuff-test-key:" + id))
	priv := ed25519.NewKeyFromSeed(seed[:])
	return priv.Public().(ed25519.PublicKey), priv
}

// wirePhantom registers the deterministic public keys of ids on n, so n
// accepts messages signed by those (test) identities.
func wirePhantom(n *Replica, ids ...string) {
	for _, id := range ids {
		pub, _ := testKeyOf(id)
		n.SetPeerKey(id, pub)
	}
}

// signVote returns a Vote by voter over (height, nodeID) signed with the
// deterministic test key of voter.
func signVote(voter string, h uint64, nodeID string) *Vote {
	v := &Vote{Height: h, NodeID: nodeID, Voter: voter}
	_, priv := testKeyOf(voter)
	v.Sig = signPayload(priv, v)
	return v
}

// recorderTransport captures the votes a replica sends (to the phantom leader)
// and drops proposals.
type recorderTransport struct {
	mu    sync.Mutex
	votes []Vote
}

func (r *recorderTransport) SendProposal(context.Context, string, *Proposal) error { return nil }
func (r *recorderTransport) SendVote(_ context.Context, _ string, v *Vote) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votes = append(r.votes, *v)
	return nil
}
func (r *recorderTransport) SendViewChange(context.Context, string, *ViewChange) error { return nil }
func (r *recorderTransport) SendFetch(context.Context, string, *Fetch) error           { return nil }
func (r *recorderTransport) SendBlock(context.Context, string, *BlockMsg) error        { return nil }

func (r *recorderTransport) voteCount(nodeID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := 0
	for _, v := range r.votes {
		if v.NodeID == nodeID {
			c++
		}
	}
	return c
}

// newFollower returns a follower F with a recording transport and the phantom
// members' public keys installed (HS-M3), so it accepts crafted signed
// proposals/QCs.
func newFollower() (*Replica, *recorderTransport) {
	tr := &recorderTransport{}
	n, err := NewReplica(Config{
		ID:     "F",
		Peers:  []string{testLeaderID, testPeer1, testPeer2},
		Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		panic(err)
	}
	wirePhantom(n, testLeaderID, testPeer1, testPeer2)
	return n, tr
}

// quorumQC returns a QC over nodeID at height h carrying 2f+1 valid signed
// votes (f = 1) from the phantom members.
func quorumQC(nodeID string, h uint64) *QC {
	q := newQC(nodeID, h)
	for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
		q.Votes[v] = signVote(v, h, nodeID).Sig
	}
	return q
}

// prop crafts a SIGNED proposal from the phantom leader: block at height h,
// child of parent, carrying cmd, justified by j. It does NOT check that j's
// certified block matches parent — callers choose, so malformed proposals are
// easy to build for the rejection tests.
func prop(h uint64, parent, cmd string, j *QC) *Proposal {
	blk := Block{View: 0, Height: h, Parent: parent, Cmd: []byte(cmd), Justify: j}
	blk.ID = blockID(0, h, parent, blk.Cmd)
	p := &Proposal{Block: blk, From: testLeaderID}
	_, priv := testKeyOf(testLeaderID)
	p.Sig = signPayload(priv, p)
	return p
}

// mainChain feeds F one block per height, each the direct child (and direct
// justification) of the previous, so chain heights h=1..n carry cmd[i-1]. The
// returned ids are the block ids by height (index 0 = genesis).
func mainChain(t *testing.T, f *Replica, cmds []string) []string {
	t.Helper()
	ids := []string{genesisID}
	for i, cmd := range cmds {
		h := uint64(i + 1)
		prev := ids[i]
		j := newQC(prev, uint64(i)) // certifies the parent block
		for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
			j.Votes[v] = signVote(v, uint64(i), prev).Sig
		}
		p := prop(h, prev, cmd, j)
		f.HandleProposal(p)
		ids = append(ids, p.Block.ID)
	}
	return ids
}

// appliedLog reads the commands F has executed, oldest first, by walking the
// applied prefix. (F's applyFn is nil, so we instead replay the committed
// span from the tree.)
func (n *Replica) appliedCmdsLocked() []string {
	var out []string
	for b := n.blocks[n.bExec]; b != nil && b.ID != genesisID; b = n.blocks[b.Parent] {
		if len(b.Cmd) > 0 {
			out = append(out, string(b.Cmd))
		}
	}
	// out is newest-first; reverse.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func mustWaitCommitted(t *testing.T, n *Replica, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := n.WaitCommitted(ctx, id); err != nil {
		t.Fatalf("WaitCommitted(%s): %v", id, err)
	}
}

// ---- commit-rule precision (docs §4.2 table) ----

// TestCommitRulePrecision pins the exact lock/commit boundaries of the §4.2
// table on a follower, feeding one block at a time so the intermediate states
// are observable. With B1..Bn each certifying its direct parent:
//
//	feed B1,B2,B3 -> lock B1, nothing committed (2-chain only)
//	feed B4       -> lock B2, B1 committed (3-chain reaches B1)
//	feed B5       -> lock B3, B2 committed
func TestCommitRulePrecision(t *testing.T) {
	f, _ := newFollower()

	// A chain feeder: each block is the direct child (and justification) of
	// the previous, carrying one command.
	prev, prevH := genesisID, uint64(0)
	var ids []string
	feed := func(cmd string) string {
		h := prevH + 1
		j := newQC(prev, prevH)
		for _, v := range []string{testLeaderID, testPeer1, testPeer2} {
			j.Votes[v] = signVote(v, prevH, prev).Sig
		}
		p := prop(h, prev, cmd, j)
		f.HandleProposal(p)
		ids = append(ids, p.Block.ID)
		prev, prevH = p.Block.ID, h
		return p.Block.ID
	}
	snapshot := func() (lock, exec string, applied []string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.bLock, f.bExec, f.appliedCmdsLocked()
	}

	feed("a") // B1
	feed("b") // B2
	feed("c") // B3: folds QC(B2) -> 2-chain locks B1; nothing commits yet
	if lock, exec, applied := snapshot(); lock != ids[0] || exec != genesisID || len(applied) != 0 {
		t.Fatalf("after B3: lock=%q exec=%q applied=%v (want lock B1, exec genesis)", lock, exec, applied)
	}

	feed("d") // B4: folds QC(B3) -> 3-chain reaches B1: B1 commits
	if lock, exec, applied := snapshot(); lock != ids[1] || exec != ids[0] || fmt.Sprint(applied) != "[a]" {
		t.Fatalf("after B4: lock=%q exec=%q applied=%v (want lock B2, exec B1, [a])", lock, exec, applied)
	}

	feed("e") // B5: folds QC(B4) -> 3-chain reaches B2: B2 commits
	if lock, exec, applied := snapshot(); lock != ids[2] || exec != ids[1] || fmt.Sprint(applied) != "[a b]" {
		t.Fatalf("after B5: lock=%q exec=%q applied=%v (want lock B3, exec B2, [a b])", lock, exec, applied)
	}
}

// TestOneAndTwoChainDoNotCommit: a QC-less-of-depth chain never commits; a
// 2-chain only locks.
func TestOneAndTwoChainDoNotCommit(t *testing.T) {
	f, _ := newFollower()
	ids := mainChain(t, f, []string{"a", "b", "c"}) // B1..B3 -> 2-chain lock B1
	_ = ids

	f.mu.Lock()
	exec := f.blocks[f.bExec]
	lock := f.blocks[f.bLock]
	f.mu.Unlock()
	if exec.ID != genesisID {
		t.Fatalf("only 2-chain formed; nothing should commit, exec=%s", exec.ID)
	}
	if lock.ID == genesisID {
		t.Fatalf("2-chain should have locked B1, lock=%s", lock.ID)
	}
	if lock.Height != 1 {
		t.Fatalf("2-chain lock should be at height 1, got %d", lock.Height)
	}
}

// TestQCLessBlockRejected: a proposal with no (or a sub-quorum) justification
// is not accepted into the tree and earns no vote.
func TestQCLessBlockRejected(t *testing.T) {
	f, _ := newFollower()

	// No QC at all — dropped, tree stays genesis-only.
	f.HandleProposal(prop(1, genesisID, "x", nil))
	f.mu.Lock()
	onlyGenesis := len(f.blocks) == 1
	f.mu.Unlock()
	if !onlyGenesis {
		t.Fatalf("QC-less proposal must be rejected, blocks=%v", f.blocks)
	}

	// A sub-quorum QC (2 < 3 votes for f=1) over a REAL block is rejected.
	// (A QC whose NodeID is genesis is the trusted root and always valid since
	// HS-M3 — only a B1 proposal can carry it — so the rejection case must
	// certify a non-genesis block.) Chain in B1 first, then propose B2 with a
	// justification holding only 2 of the 3 votes.
	b1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	f.HandleProposal(b1)
	sub := newQC(b1.Block.ID, 1)
	sub.Votes[testLeaderID] = nil
	sub.Votes[testPeer1] = nil
	f.HandleProposal(prop(2, b1.Block.ID, "b", sub))

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks[b1.Block.ID]; !ok {
		t.Fatal("B1 must be accepted before testing sub-quorum B2")
	}
	if len(f.blocks) != 2 { // genesis + B1 only
		t.Fatalf("sub-quorum proposal must not enter the tree, blocks=%v", f.blocks)
	}
}

// TestBlockIdMismatchRejected: a leader that claims an id that does not match
// the block content is rejected (integrity without signatures).
func TestBlockIdMismatchRejected(t *testing.T) {
	f, _ := newFollower()
	p := prop(1, genesisID, "x", quorumQC(genesisID, 0))
	p.Block.ID = "forged-id"
	f.HandleProposal(p)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks["forged-id"]; ok {
		t.Fatal("block with mismatched id was accepted")
	}
}

// TestWrongParentRejected: a justification that certifies a block that is NOT
// the parent, and an unknown parent (buffered, not rejected), never enter the
// tree as accepted blocks. (Height gaps are legal since HS-M2 — a post-view-
// change proposal skips the deposed leader's in-flight height — so a gap is
// NOT a wrong parent; see TestGapAcceptedAndVoted.)
func TestWrongParentRejected(t *testing.T) {
	f, _ := newFollower()

	// Justification certifies a block that is NOT the parent.
	notParent := quorumQC("somewhere-else", 0)
	f.HandleProposal(prop(1, genesisID, "y", notParent))

	// Parent not in the tree: this one is BUFFERED, not rejected, so the
	// tree stays genesis-only until the parent arrives.
	f.HandleProposal(prop(1, "unknown-parent", "z", quorumQC(genesisID, 0)))

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.blocks) != 1 {
		t.Fatalf("malformed proposals must not enter the tree, blocks=%v", f.blocks)
	}
	if len(f.pending) == 0 {
		t.Fatal("unknown-parent proposal should be buffered as pending")
	}
}

// TestBufferedChildResolvesWhenParentArrives: an out-of-order proposal whose
// parent is missing is accepted once the parent is delivered.
func TestBufferedChildResolvesWhenParentArrives(t *testing.T) {
	f, _ := newFollower()

	p1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	p2 := prop(2, p1.Block.ID, "b", quorumQC(p1.Block.ID, 1))

	f.HandleProposal(p2) // parent missing -> buffered
	f.mu.Lock()
	buffered := len(f.pending)
	f.mu.Unlock()
	if buffered != 1 {
		t.Fatal("B2 should be buffered while its parent is missing")
	}

	f.HandleProposal(p1) // parent arrives -> B2 unblocked
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks[p2.Block.ID]; !ok {
		t.Fatal("buffered child was not accepted after its parent arrived")
	}
	if len(f.pending) != 0 {
		t.Fatalf("pending should be drained, got %d", len(f.pending))
	}
}

// TestVoteOncePerHeight: a replica votes for at most one block per height, so
// an equivocating leader can never get two QCs at one height (Lemma 1).
func TestVoteOncePerHeight(t *testing.T) {
	f, tr := newFollower()
	ids := mainChain(t, f, []string{"a", "b", "c"}) // votes at heights 1,2,3
	if got := tr.voteCount(ids[1]); got != 1 {
		t.Fatalf("expected one vote for B1, got %d", got)
	}

	// Equivocation at height 1, conflicting content, same parent.
	p := prop(1, genesisID, "A-DIFFERENT", quorumQC(genesisID, 0))
	f.HandleProposal(p)

	if got := tr.voteCount(p.Block.ID); got != 0 {
		t.Fatalf("equivocating block at height 1 must not earn a vote, got %d", got)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.vHeight != 3 {
		t.Fatalf("vHeight must stay at the highest voted height (3), got %d", f.vHeight)
	}
	if _, ok := f.blocks[p.Block.ID]; ok {
		t.Fatal("unsafe equivocating block must not enter the tree")
	}
}

// TestConflictingForkNotCommitted: after F commits the A chain, a conflicting
// fork is neither voted for nor committed (single-prefix / no-two-commits).
func TestConflictingForkNotCommitted(t *testing.T) {
	f, _ := newFollower()
	ids := mainChain(t, f, []string{"a", "b", "c", "d"}) // A chain, "a" committed
	_ = ids

	// Fork off B1 (height 1) at height 2 with a different command, then
	// extend it — as if a Byzantine leader proposed a competing branch.
	// B2b conflicts with the committed branch and does not extend F's lock
	// (B2 on the A chain), so it is rejected outright.
	forkB1 := prop(1, genesisID, "a", quorumQC(genesisID, 0)) // same as A's B1
	forkB2 := prop(2, forkB1.Block.ID, "Z", quorumQC(forkB1.Block.ID, 1))
	f.HandleProposal(forkB2) // safeNode: does not extend lock B2 -> rejected

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocks[forkB2.Block.ID]; ok {
		t.Fatal("conflicting fork block was accepted into the tree")
	}
	if got := f.appliedCmdsLocked(); fmt.Sprint(got) != "[a]" {
		t.Fatalf("fork must not change the committed prefix, got %v", got)
	}
}

// TestOlderThanCommittedIgnored: a proposal below the executed prefix is
// dropped (a leader cannot resurrect an already-committed history).
func TestOlderThanCommittedIgnored(t *testing.T) {
	f, _ := newFollower()
	mainChain(t, f, []string{"a", "b", "c", "d", "e", "f"}) // committed through B3 ("a","b","c")

	// Attempt to commit an old conflicting block at height 1.
	p := prop(1, genesisID, "OLD", quorumQC(genesisID, 0))
	f.HandleProposal(p)

	f.mu.Lock()
	defer f.mu.Unlock()
	if got := f.appliedCmdsLocked(); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("old commit attempt must be ignored, applied=%v", got)
	}
}

// TestNonLeaderCannotPropose: only the designated leader may propose.
func TestNonLeaderCannotPropose(t *testing.T) {
	f, _ := newFollower()
	if _, err := f.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("follower Propose: want ErrNotLeader, got %v", err)
	}
}

// TestNonValidatorCannotContributeToQuorum: votes from ids outside the member
// set are ignored, so a non-validator can never push a QC to a quorum.
func TestNonValidatorCannotContributeToQuorum(t *testing.T) {
	// Stand up a leader with one peer so we can drive HandleVote.
	tr := &recorderTransport{}
	lead, err := NewReplica(Config{
		ID:     testLeaderID,
		Peers:  []string{"F", testPeer1, testPeer2},
		Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(lead, "F", testPeer1, testPeer2)
	// f=1: leader's self vote + 2f more = 3 = quorum.
	// First propose B1 (self vote = 1 of 3).
	b1id, err := lead.Propose([]byte("a"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}

	// A non-member "mallory" votes three times (as three different ids all
	// outside the set). None may count.
	for _, evil := range []string{"mallory", "eve", "trudy"} {
		lead.HandleVote(&Vote{Height: 1, NodeID: b1id, Voter: evil})
	}

	lead.mu.Lock()
	got := len(lead.votes[b1id])
	lead.mu.Unlock()
	if got != 1 {
		t.Fatalf("non-member votes must be ignored; votes=%d want 1 (self only)", got)
	}

	// Two real member votes still form the QC; the leader becomes free.
	lead.HandleVote(signVote("F", 1, b1id))
	if _, err := lead.Propose([]byte("b")); err != ErrBusy {
		t.Fatalf("after 2 real votes (self+F) there is no quorum yet, Propose must be busy, got %v", err)
	}
	lead.HandleVote(signVote(testPeer1, 1, b1id))
	if _, err := lead.Propose([]byte("b")); err != nil {
		t.Fatalf("quorum (self+F+P1) should free the leader, got %v", err)
	}
}

// TestQuorumBoundary: 2f votes are not enough; the 2f+1-th vote forms the QC.
func TestQuorumBoundary(t *testing.T) {
	tr := &recorderTransport{}
	lead, err := NewReplica(Config{
		ID:     testLeaderID,
		Peers:  []string{"F", testPeer1, testPeer2},
		Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	b1id, err := lead.Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(lead, "F", testPeer1, testPeer2)
	// f=1 needs 2f+1 = 3 votes total. Self = 1, so a single peer vote (2
	// total) is NOT enough; the second peer vote (3 total) forms the QC.
	lead.HandleVote(signVote("F", 1, b1id))
	if _, err := lead.Propose([]byte("b")); err != ErrBusy {
		t.Fatalf("self + 1 peer (2 votes) must not form a QC; Propose should be busy, got %v", err)
	}
	lead.HandleVote(signVote(testPeer1, 1, b1id)) // 3rd vote
	if _, err := lead.Propose([]byte("b")); err != nil {
		t.Fatalf("2f+1 votes must form the QC, got %v", err)
	}
}

// TestLeaderCommitsItsOwnBlock: the leader participates like any replica — it
// can chain on, commit and WaitCommitted its own commands once 3 blocks
// (3-chain) have accumulated above them.
func TestLeaderCommitsItsOwnBlock(t *testing.T) {
	tr := &recorderTransport{}
	lead, err := NewReplica(Config{
		ID:     testLeaderID,
		Peers:  []string{"F", testPeer1, testPeer2},
		Leader: testLeaderID,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	wirePhantom(lead, "F", testPeer1, testPeer2)
	// n=1 self-only QC would need just 1 vote, but here f=1 needs 3 — deliver
	// the two peer votes after each proposal.
	height := uint64(0)
	proposeAndVote := func(cmd []byte) string {
		height++
		id, err := lead.Propose(cmd)
		if err != nil {
			t.Fatalf("propose: %v", err)
		}
		lead.HandleVote(signVote("F", height, id))
		lead.HandleVote(signVote(testPeer1, height, id))
		return id
	}
	b1 := proposeAndVote([]byte("a"))
	b2 := proposeAndVote([]byte("b"))
	b3 := proposeAndVote([]byte("c"))
	// After B3's QC, B1's 3-chain is complete on the leader.
	mustWaitCommitted(t, lead, b1)
	_ = b2
	_ = b3

	// A block that is not yet 3-deep is not committed.
	lead.mu.Lock()
	notYet := !lead.applied[b2]
	lead.mu.Unlock()
	if !notYet {
		t.Fatal("B2 must not be committed until one more QC forms above it")
	}
}

// TestSingleNodeCommits: a single-node cluster (f=0) needs only its own vote
// per round, so three proposals complete a 3-chain and commit the first
// command — the same rule as any other cluster.
func TestSingleNodeCommits(t *testing.T) {
	var applied []string
	var logMu sync.Mutex
	tr := &recorderTransport{}
	solo, err := NewReplica(Config{ID: "solo", Leader: "solo"}, tr, func(b Block) {
		if len(b.Cmd) > 0 {
			logMu.Lock()
			applied = append(applied, string(b.Cmd))
			logMu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	id1, err := solo.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("propose x: %v", err)
	}
	if _, err := solo.Propose([]byte("y")); err != nil {
		t.Fatalf("propose y: %v", err)
	}
	if _, err := solo.Propose(nil); err != nil { // 3rd block completes the 3-chain
		t.Fatalf("propose flush: %v", err)
	}
	mustWaitCommitted(t, solo, id1)
	logMu.Lock()
	got := fmt.Sprint(applied)
	logMu.Unlock()
	if got != "[x]" {
		t.Fatalf("single node should apply exactly [x], got %v", got)
	}
}

// TestWaitCommittedErrors: an id this replica has never seen is ErrUnknownBlock;
// a block that is known but never commits respects the caller's deadline.
func TestWaitCommittedErrors(t *testing.T) {
	f, _ := newFollower()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.WaitCommitted(ctx, "never-seen"); err != ErrUnknownBlock {
		t.Fatalf("WaitCommitted(unknown) = %v, want ErrUnknownBlock", err)
	}

	// mainChain of a,b,c only forms a 2-chain (locks B1, commits nothing), so
	// B1 is known to the tree but never commits.
	ids := mainChain(t, f, []string{"a", "b", "c"})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if err := f.WaitCommitted(ctx2, ids[1]); err != context.DeadlineExceeded {
		t.Fatalf("WaitCommitted(uncommitted known block) = %v, want deadline exceeded", err)
	}
}

// TestFollowerIgnoresVotes: only the designated leader aggregates votes. If a
// follower folded them in, the votes over B3 would complete a 3-chain and
// commit B1 — it must not.
func TestFollowerIgnoresVotes(t *testing.T) {
	f, _ := newFollower()                           // leader is the phantom L0
	ids := mainChain(t, f, []string{"a", "b", "c"}) // 2-chain only; B1 not committed

	f.HandleVote(&Vote{Height: 3, NodeID: ids[3], Voter: testLeaderID})
	f.HandleVote(&Vote{Height: 3, NodeID: ids[3], Voter: testPeer1})
	f.HandleVote(&Vote{Height: 3, NodeID: ids[3], Voter: testPeer2})

	f.mu.Lock()
	defer f.mu.Unlock()
	if got := f.appliedCmdsLocked(); len(got) != 0 {
		t.Fatalf("follower must not aggregate votes into commits, applied %v", got)
	}
	if len(f.votes) != 0 {
		t.Fatalf("follower must not collect votes, votes=%v", f.votes)
	}
}

// TestDuplicateProposalIdempotent: re-delivering the same proposal is a no-op —
// it must not re-enter the tree or earn a second vote.
func TestDuplicateProposalIdempotent(t *testing.T) {
	f, tr := newFollower()
	p := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	f.HandleProposal(p)
	f.HandleProposal(p) // duplicate delivery

	f.mu.Lock()
	blocks := len(f.blocks)
	vHeight := f.vHeight
	f.mu.Unlock()
	if blocks != 2 { // genesis + B1
		t.Fatalf("duplicate proposal re-entered the tree, blocks=%d", blocks)
	}
	if vHeight != 1 {
		t.Fatalf("duplicate proposal advanced vHeight to %d", vHeight)
	}
	if got := tr.voteCount(p.Block.ID); got != 1 {
		t.Fatalf("duplicate proposal must not earn a second vote, got %d", got)
	}
}

// TestRejectedProposalsEarnNoVote: every malformed proposal is dropped without
// a vote — a sub-quorum QC, a height gap, an id mismatch, and a proposal from
// a non-leader.
func TestRejectedProposalsEarnNoVote(t *testing.T) {
	cases := []struct {
		name string
		p    *Proposal
	}{
		{"no-qc", prop(1, genesisID, "a", nil)},
		{"wrong-parent-justify", prop(1, genesisID, "d", quorumQC("elsewhere", 0))},
		{"non-leader", func() *Proposal { p := prop(1, genesisID, "e", quorumQC(genesisID, 0)); p.From = testPeer1; return p }()},
	}
	// (A height gap is deliberately NOT in this list: since HS-M2 gaps are
	// legal — a post-view-change proposal skips the deposed leader's height —
	// see TestGapAcceptedAndVoted.)
	for _, tc := range cases {
		f, tr := newFollower()
		f.HandleProposal(tc.p)
		f.mu.Lock()
		blocks := len(f.blocks)
		f.mu.Unlock()
		if blocks != 1 {
			t.Fatalf("%s: rejected proposal entered the tree, blocks=%d", tc.name, blocks)
		}
		if got := tr.voteCount(tc.p.Block.ID); got != 0 {
			t.Fatalf("%s: rejected proposal earned %d vote(s)", tc.name, got)
		}
	}

	// A sub-quorum QC over a REAL block is the meaningful M3 rejection case
	// (over genesis it would be the trusted root and thus valid). Chain B1 in
	// first, then a B2 whose justification carries only 2 of the 3 votes.
	f, tr := newFollower()
	b1 := prop(1, genesisID, "a", quorumQC(genesisID, 0))
	f.HandleProposal(b1)
	sub := newQC(b1.Block.ID, 1)
	sub.Votes[testLeaderID] = nil
	sub.Votes[testPeer1] = nil
	b2 := prop(2, b1.Block.ID, "b", sub)
	f.HandleProposal(b2)
	f.mu.Lock()
	blocks := len(f.blocks)
	f.mu.Unlock()
	if blocks != 2 { // genesis + B1 only
		t.Fatalf("sub-quorum-qc: rejected proposal entered the tree, blocks=%d", blocks)
	}
	if got := tr.voteCount(b2.Block.ID); got != 0 {
		t.Fatalf("sub-quorum-qc: rejected proposal earned %d vote(s)", got)
	}
}
