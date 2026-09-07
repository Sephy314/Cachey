package hotstuff

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Errors returned by the replica API.
var (
	// ErrNotLeader is returned by Propose when this replica is not the
	// designated leader (mirrors raft.ErrNotLeader / pbft.ErrNotPrimary).
	ErrNotLeader = errors.New("hotstuff: not leader")
	// ErrBusy is returned by Propose when the previous block's QC has not
	// formed yet: a leader must not chain a new block onto an uncommitted head
	// (the new block would not carry a valid justification).
	ErrBusy = errors.New("hotstuff: leader busy: previous block's QC not formed")
	// ErrUnknownBlock is returned by WaitCommitted for a block this replica
	// has never seen.
	ErrUnknownBlock = errors.New("hotstuff: unknown block")
)

// Config configures a hotstuff replica. HS-M1 is the normal-case core with a
// single designated leader and no view change; Leader is fixed at startup and
// identical on every replica of a cluster.
type Config struct {
	ID     string   // this replica's id (unique in the cluster)
	Peers  []string // peer replica ids, excluding self
	Leader string   // the single proposer (defaults to ID when empty)
}

// Replica is one Chained HotStuff replica running the HS-M1 normal-case core.
//
// The protocol state is the block tree plus four pointers into it — qcHigh
// (the highest QC known), head (the newest block on the accepted chain),
// bLock (the 2-chain lock) and bExec (the committed/executed prefix) — plus
// vHeight, the height of the last block this replica voted for (votes are
// strictly monotone in height, which is what guarantees QC uniqueness at a
// height).
//
// Everything below runs under mu; message handlers fold new proposals/votes in
// and return after any newly committed commands have been applied. applyFn
// executes one committed command and runs WHILE mu IS HELD so executions are
// strictly serialized; it must not call back into the replica (same contract
// as pbft).
//
// ponytail: HS-M1 is deliberately leader-trusted (no signatures) and
// gap-free (a block must certify its direct parent at parent.Height+1). Both
// ceilings lift in later milestones: HS-M3 signs every message and verifies
// the sender; HS-M2 relaxes the direct-parent constraint for view changes.
type Replica struct {
	id      string
	all     []string
	members map[string]bool
	peers   []string
	f       int // max byzantine faults tolerated: len(all) == 3f+1
	tr      Transport
	applyFn func(Block)

	// HS-M3 identity: priv signs everything this replica sends; pub is its
	// public half; peerKeys holds the verified public key of every member
	// (own key included) so incoming signatures can be checked.
	priv     ed25519.PrivateKey
	pub      ed25519.PublicKey
	peerKeys map[string]ed25519.PublicKey

	// view/leader state (HS-M2): the cluster runs in a sequence of views; the
	// leader of view v is the deterministic round-robin leaderOf(v) starting
	// from the view-0 leader (cfg.Leader). A leader is born active for view 0
	// and becomes active for a later view only after collecting 2f+1 view
	// changes for it. active controls Propose; freshBase makes the first
	// proposal after a view change skip the deposed leader's in-flight height
	// (see viewchange.go), which is what keeps vote heights strictly growing.
	view      uint64
	viewIdx   int
	leader    string
	active    bool
	freshBase bool

	mu            sync.Mutex
	blocks        map[string]*Block            // block tree by id (genesis present)
	qcHigh        *QC                          // highest known QC
	head          string                       // newest block on the accepted chain
	bLock         string                       // locked block id (2-chain)
	bExec         string                       // committed/executed prefix head id
	vHeight       uint64                       // height of the last block voted for
	pending       map[string][]*Proposal       // proposals whose parent was not delivered yet
	pendingBlocks map[string][]Block           // fetched blocks waiting on a parent
	votes         map[string]map[string][]byte // leader: votes collected per block id (voter -> sig)
	qcFormed      map[string]bool
	notify        map[string]chan struct{} // block id -> closed once applied
	applied       map[string]bool
	// HS-M2 view change state.
	vcs      map[uint64]map[string]*ViewChange // view changes per target view, per sender
	vcSent   map[uint64]bool
	wantBase *QC             // adopted high QC whose certified block is not yet in the tree
	baseFrom string          // peer to fetch the base block from
	fetches  map[string]bool // block ids with a fetch already in flight

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewReplica creates a Chained HotStuff replica. tr delivers messages to peers;
// applyFn executes each committed command in chain order. The cluster must be
// exactly 3f+1 members (1, 4, 7, ...).
func NewReplica(cfg Config, tr Transport, applyFn func(Block)) (*Replica, error) {
	if cfg.ID == "" {
		return nil, errors.New("hotstuff: replica id is required")
	}
	if tr == nil {
		return nil, errors.New("hotstuff: transport is required")
	}
	if cfg.Leader == "" {
		cfg.Leader = cfg.ID
	}
	all := append([]string{cfg.ID}, cfg.Peers...)
	unique := make(map[string]bool, len(all))
	for _, id := range all {
		if unique[id] {
			return nil, fmt.Errorf("hotstuff: duplicate replica id %q", id)
		}
		unique[id] = true
	}
	if n := len(all); n > 1 && (n-1)%3 != 0 {
		return nil, fmt.Errorf("hotstuff: cluster of %d replicas is not 3f+1 (want 1, 4, 7, ...)", n)
	}
	if !unique[cfg.Leader] {
		return nil, fmt.Errorf("hotstuff: leader %q is not a cluster member", cfg.Leader)
	}
	sort.Strings(all)
	members := make(map[string]bool, len(all))
	var peers []string
	viewIdx := 0
	for i, id := range all {
		members[id] = true
		if id == cfg.Leader {
			viewIdx = i
		}
		if id != cfg.ID {
			peers = append(peers, id)
		}
	}
	if applyFn == nil {
		applyFn = func(Block) {}
	}
	pub, priv, err := newKeyPair()
	if err != nil {
		return nil, fmt.Errorf("hotstuff: key generation: %w", err)
	}
	g, gq := genesisBlock(all)
	return &Replica{
		id:            cfg.ID,
		priv:          priv,
		pub:           pub,
		peerKeys:      map[string]ed25519.PublicKey{cfg.ID: pub},
		all:           all,
		members:       members,
		peers:         peers,
		f:             (len(all) - 1) / 3,
		viewIdx:       viewIdx,
		leader:        cfg.Leader,
		active:        cfg.ID == cfg.Leader, // the view-0 leader is born active
		tr:            tr,
		applyFn:       applyFn,
		blocks:        map[string]*Block{genesisID: g},
		qcHigh:        gq,
		head:          genesisID,
		bLock:         genesisID,
		bExec:         genesisID,
		pending:       map[string][]*Proposal{},
		pendingBlocks: map[string][]Block{},
		votes:         map[string]map[string][]byte{},
		qcFormed:      map[string]bool{},
		notify:        map[string]chan struct{}{},
		applied:       map[string]bool{},
		vcs:           map[uint64]map[string]*ViewChange{},
		vcSent:        map[uint64]bool{},
		fetches:       map[string]bool{},
		stopCh:        make(chan struct{}),
	}, nil
}

// leaderOf returns the leader of a view: a deterministic round-robin over the
// sorted member set that starts at the view-0 leader (cfg.Leader). Every
// replica computes the same schedule, so on a view change everyone agrees who
// the next leader is (no election).
func (n *Replica) leaderOf(view uint64) string {
	return n.all[(n.viewIdx+int(view))%len(n.all)]
}

// ID returns this replica's id.
func (n *Replica) ID() string { return n.id }

// IsLeader reports whether this replica is the active proposer of its current
// view.
func (n *Replica) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id == n.leader && n.active
}

// Leader returns the current leader's id.
func (n *Replica) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// View returns the replica's current view number.
func (n *Replica) View() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.view
}

// Propose (leader only) creates the next block on the chain, carrying cmd (nil
// for an empty block that keeps the chain advancing), records this replica's
// own vote and broadcasts the proposal to its peers. It returns the new
// block's id. It fails with ErrNotLeader when this replica is not the active
// leader and ErrBusy when the previous block's QC has not formed yet.
//
// HS-M1/M2 have no client driver or auto-advance: the caller proposes the next
// block (empty or not) when it is free, which is what lets earlier blocks
// accumulate the QCs that commit them.
func (n *Replica) Propose(cmd []byte) (string, error) {
	n.mu.Lock()
	if n.id != n.leader || !n.active {
		n.mu.Unlock()
		return "", ErrNotLeader
	}
	if n.qcHigh == nil || n.qcHigh.NodeID != n.head {
		n.mu.Unlock()
		return "", ErrBusy
	}
	p := n.proposeLocked(cmd)
	n.mu.Unlock()
	if p == nil {
		return "", ErrBusy
	}
	n.broadcast(p)
	return p.Block.ID, nil
}

// proposeLocked builds the next block chained onto the head carrying cmd and
// adds it to the tree with this replica's own vote. Must hold n.mu.
func (n *Replica) proposeLocked(cmd []byte) *Proposal {
	parent := n.blocks[n.head]
	h := parent.Height + 1
	if n.freshBase {
		// First proposal after a view change: skip the deposed leader's
		// in-flight height so the new block is strictly above every height any
		// correct replica voted in the previous view (see viewchange.go).
		h = parent.Height + 2
		n.freshBase = false
	}
	b := &Block{
		View:    n.view,
		Height:  h,
		Parent:  n.head,
		ID:      blockID(n.view, h, n.head, cmd),
		Cmd:     cmd,
		Justify: n.qcHigh, // certifies head == parent
	}
	n.blocks[b.ID] = b
	n.head = b.ID
	// Self-vote: the leader is a replica too, and votes are strictly monotone
	// in height (a replica never votes twice for one height).
	self := &Vote{Height: h, NodeID: b.ID, Voter: n.id}
	self.Sig = n.sign(self)
	n.voteForBlockLocked(self) // may form a QC (n=1) and commit
	p := &Proposal{Block: *b, From: n.id}
	p.Sig = n.sign(p)
	return p
}

func (n *Replica) broadcast(p *Proposal) {
	ctx := context.Background()
	for _, peer := range n.peers {
		_ = n.tr.SendProposal(ctx, peer, p)
	}
}

// HandleProposal folds a leader proposal into this replica: view and
// structural and safeNode validation, tree insertion (unblocking any buffered
// descendants), QC folding (which may lock/commit/apply), and — when accepted
// and strictly newer than anything voted for — a vote back to the leader.
func (n *Replica) HandleProposal(p *Proposal) {
	var vote *Vote
	var fetch *Fetch
	var kids []*Proposal
	n.mu.Lock()
	vote, kids, fetch = n.handleProposalLocked(p)
	leader := n.leader // capture under the lock: a view change may re-point n.leader
	n.mu.Unlock()
	if fetch != nil {
		_ = n.tr.SendFetch(context.Background(), fetch.To, &Fetch{BlockID: fetch.BlockID, From: n.id})
	}
	if vote != nil {
		_ = n.tr.SendVote(context.Background(), leader, vote)
	}
	for _, k := range kids {
		n.HandleProposal(k)
	}
}

func (n *Replica) handleProposalLocked(p *Proposal) (*Vote, []*Proposal, *Fetch) {
	if p == nil || !n.verify(p.From, p.Sig, *p) {
		return nil, nil, nil // unauthenticated, non-member, or tampered
	}
	b := &p.Block // blocks stored in the tree are never mutated
	// Only the leader of the block's own view may propose it. A proposal for a
	// PAST view is stale (a deposed leader) and ignored; a proposal for a
	// FUTURE view means this replica is behind — it advances to that view so
	// it can keep following the current leader (fast catch-up).
	if p.From != n.leaderOf(b.View) {
		return nil, nil, nil
	}
	if b.View < n.view {
		return nil, nil, nil
	}
	if b.View > n.view {
		n.enterViewLocked(b.View)
	}
	if b.ID != blockID(b.View, b.Height, b.Parent, b.Cmd) {
		return nil, nil, nil // id does not match content (equivocation integrity)
	}
	if n.blocks[b.ID] != nil {
		return nil, nil, nil // already known (duplicate delivery)
	}
	if b.Height <= n.blocks[n.bExec].Height {
		return nil, nil, nil // at or below the already-executed prefix
	}
	if n.blocks[b.Parent] == nil {
		// The parent has not been delivered yet — buffer the proposal and, if
		// no fetch is in flight for it, ask the proposer to send the block.
		n.pending[b.Parent] = append(n.pending[b.Parent], p)
		if n.fetches[b.Parent] {
			return nil, nil, nil // already requested
		}
		n.fetches[b.Parent] = true
		return nil, nil, &Fetch{BlockID: b.Parent, To: p.From}
	}
	parent := n.blocks[b.Parent]
	// Structural validation (HS-M2/M3): the justification must be a genuine
	// quorum certificate that certifies the direct parent at a strictly higher
	// height. Gaps are legal after a view change (it skips the deposed
	// leader's in-flight height).
	if b.Justify == nil || !n.qcValid(b.Justify) ||
		b.Justify.NodeID != b.Parent || b.Height <= parent.Height {
		return nil, nil, nil
	}
	// safeNode: accept (and vote for) the proposal only if its branch extends
	// the lock, or its justification is strictly higher than the lock.
	lock := n.blocks[n.bLock]
	if !n.extendsLocked(b, lock) && b.Justify.Height <= lock.Height {
		return nil, nil, nil
	}
	kids := n.addBlockLocked(b, b.Parent)
	// Vote only at heights strictly above the last one voted for (a replica
	// never votes twice for one height — the QC-uniqueness invariant). The
	// vote is signed by the voter (HS-M3) so the leader can authenticate it.
	var vote *Vote
	if b.Height > n.vHeight {
		n.vHeight = b.Height
		vote = &Vote{Height: b.Height, NodeID: b.ID, Voter: n.id}
		vote.Sig = n.sign(vote)
	}
	return vote, kids, nil
}

// addBlockLocked inserts a validated block into the tree (raising head when it
// is newer) and, via a worklist, any parked fetched blocks whose parent this
// block provides (a catch-up replica fetches ancestors bottom-up, so a fetched
// block often unblocks others). Each inserted block has its QC folded in
// (lock/commit) and returns the buffered proposals (its children) that can now
// be accepted. Must hold n.mu.
func (n *Replica) addBlockLocked(b *Block, parentID string) []*Proposal {
	var kids []*Proposal
	work := []*Block{b}
	for len(work) > 0 {
		cur := work[len(work)-1]
		work = work[:len(work)-1]
		if n.blocks[cur.ID] != nil {
			continue
		}
		n.blocks[cur.ID] = cur
		delete(n.fetches, cur.ID)
		if cur.Height > n.blocks[n.head].Height {
			n.head = cur.ID
		}
		n.onNewQCLocked(cur.Justify)
		n.completeBaseLocked(cur.ID)
		kids = append(kids, n.pending[cur.ID]...)
		delete(n.pending, cur.ID)
		if parked := n.pendingBlocks[cur.ID]; len(parked) > 0 {
			delete(n.pendingBlocks, cur.ID)
			for i := range parked {
				pb := &parked[i]
				if n.blocks[pb.Parent] != nil {
					work = append(work, pb)
				} else { // defensive: parked only under a now-present parent
					n.pendingBlocks[pb.Parent] = append(n.pendingBlocks[pb.Parent], *pb)
				}
			}
		}
	}
	return kids
}

// extendsLocked reports whether b's branch (its parent chain) contains lock.
// Must hold n.mu.
func (n *Replica) extendsLocked(b *Block, lock *Block) bool {
	if lock == nil {
		return true
	}
	for cur := b.Parent; cur != ""; cur = n.blocks[cur].Parent {
		if cur == lock.ID {
			return true
		}
		if n.blocks[cur] == nil {
			return false // defensive: ancestry should always be present
		}
	}
	return false
}

// HandleVote records a peer's vote. Only the leader aggregates; once 2f+1
// votes for a block are in, the resulting QC is folded in (which may lock,
// commit and apply). Non-members and unknown blocks are ignored, so a
// non-validator can never contribute to a quorum.
func (n *Replica) HandleVote(v *Vote) {
	if v == nil {
		return
	}
	n.mu.Lock()
	if n.id == n.leader {
		n.voteForBlockLocked(v)
	}
	n.mu.Unlock()
}

// voteForBlockLocked records one member's authenticated vote for a block and,
// when 2f+1 distinct valid votes are in, folds the resulting QC into the state
// (locking and committing as the chain rule dictates). Non-members, unknown
// blocks and bad signatures are ignored. Must hold n.mu.
func (n *Replica) voteForBlockLocked(v *Vote) {
	if v == nil {
		return
	}
	nodeID := v.NodeID
	if !n.members[v.Voter] || n.blocks[nodeID] == nil || !n.verify(v.Voter, v.Sig, *v) {
		return // non-member, unknown block, or a bad signature
	}
	set := n.votes[nodeID]
	if set == nil {
		set = make(map[string][]byte)
		n.votes[nodeID] = set
	}
	if set[v.Voter] != nil {
		return // duplicate — one vote per member per block
	}
	set[v.Voter] = v.Sig
	if len(set) < 2*n.f+1 || n.qcFormed[nodeID] {
		return // not yet a quorum, or the QC already formed
	}
	n.qcFormed[nodeID] = true
	qc := newQC(nodeID, n.blocks[nodeID].Height)
	for voter, sig := range set {
		qc.Votes[voter] = sig
	}
	n.onNewQCLocked(qc)
}

// onNewQCLocked folds a newly-known valid QC into the replica state: it raises
// qcHigh and, when the certified block is in the tree, walks the direct
// one-chain below it — a 2-chain locks the certified block's parent, a 3-chain
// commits its grandparent (paper §5). Must hold n.mu.
func (n *Replica) onNewQCLocked(qc *QC) {
	if !n.qcValid(qc) {
		return
	}
	if n.qcHigh != nil && qc.Height <= n.qcHigh.Height {
		return // stale or duplicate; onNewQC is idempotent
	}
	n.qcHigh = qc
	c := n.blocks[qc.NodeID]
	if c == nil {
		return // certified block not known yet; re-evaluated when it arrives
	}
	parent := n.blocks[c.Parent]
	// 2-chain: c directly certifies its parent -> lock the parent.
	if !oneChainedBy(parent, c) {
		return
	}
	if parent.Height > n.blocks[n.bLock].Height {
		n.bLock = parent.ID
	}
	// 3-chain: that parent directly certifies its own parent -> commit it.
	gp := n.blocks[parent.Parent]
	if gp != nil && oneChainedBy(gp, parent) && gp.Height > n.blocks[n.bExec].Height {
		n.commitUpToLocked(gp)
	}
}

// commitUpToLocked extends the committed/executed prefix to gp (inclusive),
// applying every command-bearing block on that span in chain order. Must hold
// n.mu; gp must be a descendant of bExec on the committed prefix (the
// single-prefix invariant guarantees it).
func (n *Replica) commitUpToLocked(gp *Block) {
	// Collect the span gp..bExec in reverse (newest first).
	var span []*Block
	for b := gp; b != nil && b.ID != n.bExec; b = n.blocks[b.Parent] {
		span = append(span, b)
		if b.Parent == "" {
			break // walked off the tree without meeting bExec (defensive)
		}
	}
	// Apply oldest first.
	for i := len(span) - 1; i >= 0; i-- {
		b := span[i]
		if len(b.Cmd) > 0 && b.ID != genesisID {
			n.applyFn(*b)
		}
		n.bExec = b.ID
		n.markAppliedLocked(b.ID)
	}
}

func (n *Replica) markAppliedLocked(id string) {
	if n.applied[id] {
		return
	}
	n.applied[id] = true
	if ch := n.notify[id]; ch != nil {
		close(ch)
	}
}

// WaitCommitted blocks until the block with blockID has been committed and
// applied locally, or ctx is done. It returns ErrUnknownBlock when this
// replica has never seen the block.
func (n *Replica) WaitCommitted(ctx context.Context, blockID string) error {
	n.mu.Lock()
	if n.applied[blockID] {
		n.mu.Unlock()
		return nil
	}
	if n.blocks[blockID] == nil {
		n.mu.Unlock()
		return ErrUnknownBlock
	}
	ch := n.notify[blockID]
	if ch == nil {
		ch = make(chan struct{})
		n.notify[blockID] = ch
	}
	n.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
