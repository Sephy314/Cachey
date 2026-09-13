package hotstuff

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
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
	// PrivateKey optionally supplies this replica's persistent Ed25519 identity.
	// When nil a fresh keypair is generated (in-memory tests). A production node
	// MUST pass a key loaded from durable storage so the public key is stable
	// across restarts — otherwise signatures embedded in past QCs no longer
	// verify after a restart (HS-M3).
	PrivateKey ed25519.PrivateKey
	// GCRetention is the number of committed blocks below bExec retained for
	// near-behind catch-up serving (Phase 2.5 GC). Zero uses the default.
	GCRetention uint64
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
	peers   []string
	f       int // max byzantine faults tolerated: len(all) == 3f+1
	tr      Transport
	applyFn func(Block)

	// Phase 2.1 membership model: the current epoch and the historical
	// validator sets (epoch -> set). Every protocol decision (vote, QC,
	// quorum, leader) is derived from the set of the epoch in question, never
	// from the current membership. leader0 is the view-0 leader (cfg.Leader),
	// the anchor of the deterministic leader schedule in every epoch.
	epoch   uint64
	sets    map[uint64]*ValidatorSet
	leader0 string

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
	wantBase *QC                  // adopted high QC whose certified block is not yet in the tree
	baseFrom string               // peer to fetch the base block from
	fetches  map[string]time.Time // block ids with a fetch in flight, and when it was sent

	// HS-M4 durable persistence.
	logStore    LogStore // nil = in-memory only (tests)
	recoverQCs  []*QC    // recovered qcHigh raises, replayed by FinishRecovery
	recoverExec string   // recovered executed watermark (exec block id)

	// Phase 2.3 block sync: outstanding range requests (id -> request). The
	// single-block fetch set (fetches) stays for small gaps; the range sync
	// handles large ones.
	syncSeq  uint64
	syncReqs map[uint64]syncRequest

	// Phase 2.5 GC: the lowest retained block (the GC base). Blocks strictly
	// below it were deleted; its epoch (stored in the block) anchors epoch
	// derivation for the retained tree. gcRetention is the number of committed
	// blocks below bExec kept for near-behind catch-up serving.
	gcBase      string
	gcRetention uint64

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewReplica creates a Chained HotStuff replica. tr delivers messages to peers;
// applyFn executes each committed command in chain order. The cluster must
// have at least one member; the BFT target is N >= 3f+1 (quorum 2f+1 with
// f=floor((N-1)/3)), but intermediate sizes (N=5,6) are allowed.
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
	if !unique[cfg.Leader] {
		return nil, fmt.Errorf("hotstuff: leader %q is not a cluster member", cfg.Leader)
	}
	sort.Strings(all)
	var peers []string
	viewIdx := 0
	for i, id := range all {
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
	var (
		pub  ed25519.PublicKey
		priv ed25519.PrivateKey
	)
	if len(cfg.PrivateKey) == 0 {
		npub, npriv, kerr := newKeyPair()
		if kerr != nil {
			return nil, fmt.Errorf("hotstuff: key generation: %w", kerr)
		}
		pub, priv = npub, npriv
	} else {
		priv = cfg.PrivateKey
		pub = priv.Public().(ed25519.PublicKey)
	}
	g, gq := genesisBlock(all)
	// The genesis validator set (epoch 0): the configured members, with the
	// replica's own key. Peers' keys are pinned later via SetPeerKey (the
	// trusted bootstrap), which also records them in this set.
	set0 := newValidatorSet(0, all, map[string]ed25519.PublicKey{cfg.ID: pub})
	retention := cfg.GCRetention
	if retention == 0 {
		retention = defaultGCRetention
	}
	return &Replica{
		id:            cfg.ID,
		priv:          priv,
		pub:           pub,
		peerKeys:      map[string]ed25519.PublicKey{cfg.ID: pub},
		all:           all,
		peers:         peers,
		f:             (len(all) - 1) / 3,
		viewIdx:       viewIdx,
		epoch:         0,
		sets:          map[uint64]*ValidatorSet{0: set0},
		leader0:       cfg.Leader,
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
		fetches:       map[string]time.Time{},
		syncReqs:      map[uint64]syncRequest{},
		gcBase:        genesisID,
		gcRetention:   retention,
		stopCh:        make(chan struct{}),
	}, nil
}

// leaderOf returns the leader of a view in the replica's CURRENT epoch: a
// deterministic round-robin over the epoch's sorted validator set that starts
// at the view-0 leader (cfg.Leader). Every replica computes the same schedule,
// so on a view change everyone agrees who the next leader is (no election).
func (n *Replica) leaderOf(view uint64) string {
	return n.leaderOfEpoch(n.epoch, view)
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

// Epoch returns the replica's current epoch (the active configuration
// generation). Phase 2.1 membership model.
func (n *Replica) Epoch() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.epoch
}

// HighestQC returns the highest QC the replica knows (nil only before
// genesis). Phase 2.1: the QC carries its own epoch, so callers can verify it
// against the historical validator set.
func (n *Replica) HighestQC() *QC {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.qcHigh
}

// ValidateQC reports whether qc is a genuine quorum certificate, validated
// against the ValidatorSet of qc.Epoch (voter membership, public keys,
// signatures, quorum) — never the current membership. Phase 2.1.
func (n *Replica) ValidateQC(qc *QC) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.qcValid(qc)
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
		Epoch:   n.epoch,  // the current configuration generation
	}
	n.blocks[b.ID] = b
	n.head = b.ID
	n.persistRecordLocked(persistEntry{Kind: pkBlock, Block: *b}) // durable before the vote leaves (M4)
	// Self-vote: the leader is a replica too, and votes are strictly monotone
	// in height (a replica never votes twice for one height).
	self := &Vote{Height: h, NodeID: b.ID, Voter: n.id}
	self.Sig = n.sign(self)
	n.voteForBlockLocked(self) // may form a QC (n=1) and commit
	p := &Proposal{Block: *b, From: n.id}
	if n.view > 0 {
		// A replica that missed the pacemaker quorum may only enter this view on
		// seeing its 2f+1 signed ViewChanges. Carry the certificate with every
		// proposal in the view so lagging replicas can safely catch up.
		for _, vc := range n.vcs[n.view] {
			p.ViewChanges = append(p.ViewChanges, *vc)
		}
	}
	p.Sig = n.sign(p)
	return p
}

func (n *Replica) broadcast(p *Proposal) {
	ctx := context.Background()
	// Broadcast to the CURRENT epoch's validators: a new validator must
	// receive proposals once its epoch activates. (Its transport address is
	// provisioned out-of-band; an unknown address fails the send silently.)
	set := n.sets[n.epoch]
	for _, id := range set.Validators {
		if id == n.id {
			continue
		}
		_ = n.tr.SendProposal(ctx, id, p)
	}
}

// HandleProposal folds a leader proposal into this replica: view and
// structural and safeNode validation, tree insertion (unblocking any buffered
// descendants), QC folding (which may lock/commit/apply), and — when accepted
// and strictly newer than anything voted for — a vote back to the proposer
// (the leader that must form the next QC).
func (n *Replica) HandleProposal(p *Proposal) {
	var vote *Vote
	var fetch *Fetch
	var sync *GetBlocks
	var kids []*Proposal
	n.mu.Lock()
	vote, kids, fetch, sync = n.handleProposalLocked(p)
	n.mu.Unlock()
	if fetch != nil {
		_ = n.tr.SendFetch(context.Background(), fetch.To, &Fetch{BlockID: fetch.BlockID, From: n.id})
	}
	if sync != nil {
		_ = n.tr.SendGetBlocks(context.Background(), sync.To, sync)
	}
	if vote != nil && p != nil {
		// The vote goes to the proposer: the leader of the block's view in its
		// epoch (validated), which aggregates the votes into the next QC. This
		// is p.From rather than n.leader because during a transition n.leader
		// may still name the old epoch's schedule.
		_ = n.tr.SendVote(context.Background(), p.From, vote)
	}
	for _, k := range kids {
		n.HandleProposal(k)
	}
}

func (n *Replica) handleProposalLocked(p *Proposal) (*Vote, []*Proposal, *Fetch, *GetBlocks) {
	if p == nil {
		return nil, nil, nil, nil
	}
	b := &p.Block // blocks stored in the tree are never mutated
	// Content identity first (no keys needed): a proposal whose id does not
	// match its content is dropped before any epoch/membership work.
	if b.ID != blockID(b.View, b.Height, b.Parent, b.Cmd) {
		return nil, nil, nil, nil // id does not match content (equivocation integrity)
	}
	if n.blocks[b.ID] != nil {
		return nil, nil, nil, nil // already known (duplicate delivery)
	}
	if b.Height <= n.blocks[n.bExec].Height {
		return nil, nil, nil, nil // at or below the already-executed prefix
	}
	if n.blocks[b.Parent] == nil {
		// The parent has not been delivered yet — buffer the proposal. A large
		// gap (the proposal is far above the head) triggers the range sync; a
		// small gap keeps the single-block fetch.
		n.pending[b.Parent] = append(n.pending[b.Parent], p)
		if b.Height > n.blocks[n.head].Height+3 {
			if n.syncInFlightLocked(b.Parent) {
				return nil, nil, nil, nil // already requested
			}
			req := n.markSyncLocked(n.bExec, b.Parent, p.From)
			return nil, nil, nil, &GetBlocks{RequestID: req.id, Anchor: req.anchor, Target: b.Parent, To: p.From, From: n.id}
		}
		if n.fetchInFlightLocked(b.Parent) {
			return nil, nil, nil, nil // already requested
		}
		n.markFetchLocked(b.Parent)
		return nil, nil, &Fetch{BlockID: b.Parent, To: p.From}, nil
	}
	parent := n.blocks[b.Parent]
	// Structural validation (HS-M2/M3): the justification must be a genuine
	// quorum certificate that certifies the direct parent at a strictly higher
	// height. qcValid is epoch-aware: the QC is checked against the validator
	// set of its own epoch, which must equal the parent's epoch (the parent is
	// known, so a mismatched QC epoch is rejected there). Gaps are legal after
	// a view change (it skips the deposed leader's in-flight height).
	if b.Justify == nil || !n.qcValid(b.Justify) ||
		b.Justify.NodeID != b.Parent || b.Height <= parent.Height {
		return nil, nil, nil, nil
	}
	// Epoch: derived from the ancestry (committed membership transitions), not
	// from the proposer's claim. The claimed epoch must match, and the epoch's
	// validator set must be derivable — a new epoch with an underivable
	// configuration is not a valid claim.
	want, set := n.epochAndSetOfLocked(b)
	if b.Epoch != want || set == nil {
		return nil, nil, nil, nil
	}
	// Proposer: the leader of this view in the block's epoch, authenticated
	// with that epoch's validator keys (a removed validator's key is not in
	// the set; a new validator's key is, once the transition committed).
	if p.From != n.leaderOfSet(set, b.View) || !n.verifyInSet(set, p.From, p.Sig, *p) {
		return nil, nil, nil, nil
	}
	if b.View < n.view {
		return nil, nil, nil, nil // stale: a deposed leader's proposal
	}
	// View advancement: a future-view proposal must carry a 2f+1 ViewChange
	// certificate (validated against the vcs' own epoch's set). The leader is
	// computed for the block's epoch, which may be ahead of the current one
	// during a transition.
	if b.View > n.view {
		if !n.proposalViewCertValidLocked(b.View, p.ViewChanges) {
			return nil, nil, nil, nil
		}
		n.enterViewLockedEpoch(b.View, b.Epoch)
	}
	// safeNode: accept (and vote for) the proposal only if its branch extends
	// the lock, or its justification is strictly higher than the lock.
	lock := n.blocks[n.bLock]
	if !n.extendsLocked(b, lock) && b.Justify.Height <= lock.Height {
		return nil, nil, nil, nil
	}
	kids := n.addBlockLocked(b, b.Parent)
	// Vote only at heights strictly above the last one voted for (a replica
	// never votes twice for one height — the QC-uniqueness invariant). The
	// vote-once guard must be durable before the vote leaves this replica (see
	// persistVotedLocked); a failed write suppresses the vote, never double-votes.
	var vote *Vote
	if b.Height > n.vHeight {
		n.vHeight = b.Height
		if n.persistVotedLocked(b.Height) == nil {
			vote = &Vote{Height: b.Height, NodeID: b.ID, Voter: n.id}
			vote.Sig = n.sign(vote)
		}
	}
	return vote, kids, nil, nil
}

// proposalViewCertValidLocked validates the pacemaker evidence carried by a
// future-view proposal. A proposal alone cannot change views: it must include
// 2f+1 distinct, signed ViewChanges for exactly that view, all from the same
// epoch, validated against that epoch's validator set. The vcs' epoch may be
// older than the receiver's current epoch (a transition committed mid-view),
// but it must be a known epoch — a certificate from an unknown configuration
// cannot be validated.
func (n *Replica) proposalViewCertValidLocked(view uint64, vcs []ViewChange) bool {
	if len(vcs) == 0 {
		return false
	}
	e := vcs[0].Epoch
	set := n.sets[e]
	if set == nil {
		return false
	}
	if len(vcs) < set.Quorum {
		return false
	}
	seen := make(map[string]bool, len(vcs))
	for i := range vcs {
		vc := &vcs[i]
		if vc.View != view || vc.Epoch != e || !set.has(vc.From) || seen[vc.From] ||
			!n.verifyInSet(set, vc.From, vc.Sig, *vc) {
			return false
		}
		seen[vc.From] = true
	}
	return len(seen) >= set.Quorum
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
		// Epoch gate: a block's claimed epoch must match the epoch derived
		// from its ancestry (committed transitions), and that epoch's set must
		// be derivable. This is the single choke point covering fetched and
		// parked blocks (the proposal path checks earlier because it needs the
		// set for the proposer check; a fetch response has no proposer). All
		// ancestors are present by construction, so the derivation is exact.
		if want, set := n.epochAndSetOfLocked(cur); cur.Epoch != want || set == nil {
			continue
		}
		n.blocks[cur.ID] = cur
		delete(n.fetches, cur.ID)
		n.persistRecordLocked(persistEntry{Kind: pkBlock, Block: *cur}) // accepted block (M4)
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
// when a quorum of distinct valid votes from the block's epoch's validator set
// are in, folds the resulting QC into the state (locking and committing as the
// chain rule dictates). Non-members of the block's epoch, unknown blocks, bad
// signatures and wrong heights are ignored — a removed validator's vote is
// rejected, and a new validator's vote counts only once its epoch activated.
// Must hold n.mu.
func (n *Replica) voteForBlockLocked(v *Vote) {
	if v == nil {
		return
	}
	nodeID := v.NodeID
	blk := n.blocks[nodeID]
	// A vote must match its block's actual height: a Byzantine member can sign a
	// vote for the right block at the WRONG height, and if it slipped into the
	// vote set the QC built from those signatures would fail verification (the
	// QC certifies the block's real height) — permanently wedging the block.
	if blk == nil {
		return
	}
	set := n.sets[blk.Epoch]
	if set == nil || !set.has(v.Voter) || v.Height != blk.Height {
		return // unknown block, non-member of the block's epoch, or wrong height
	}
	pub, ok := set.PublicKeys[v.Voter]
	if !ok || !verifyPayload(pub, v.Sig, *v) {
		return // a bad signature
	}
	setVotes := n.votes[nodeID]
	if setVotes == nil {
		setVotes = make(map[string][]byte)
		n.votes[nodeID] = setVotes
	}
	if setVotes[v.Voter] != nil {
		return // duplicate — one vote per member per block
	}
	setVotes[v.Voter] = v.Sig
	if len(setVotes) < set.Quorum || n.qcFormed[nodeID] {
		return // not yet a quorum, or the QC already formed
	}
	n.qcFormed[nodeID] = true
	qc := newQCEpoch(blk.Epoch, nodeID, blk.Height)
	for voter, sig := range setVotes {
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
	n.persistRecordLocked(persistEntry{Kind: pkQC, QC: qc}) // qcHigh raise (M4)
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
		// A committed membership-transition block activates its epoch's
		// validator set (derived from the current set plus the command).
		n.activateTransitionLocked(b)
		n.bExec = b.ID
		n.markAppliedLocked(b.ID)
	}
	n.persistRecordLocked(persistEntry{Kind: pkApplied, ExecID: n.bExec}) // executed watermark (M4)
}

func (n *Replica) markAppliedLocked(id string) {
	if n.applied[id] {
		return
	}
	n.applied[id] = true
	if ch := n.notify[id]; ch != nil {
		close(ch)
		delete(n.notify, id) // the waiter has been woken; WaitCommitted short-circuits on applied
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
