// Package pbft implements a Practical Byzantine Fault Tolerance replica
// (Castro & Liskov, "Practical Byzantine Fault Tolerance", OSDI '99). A
// cluster of N = 3f+1 replicas stays correct while up to f of them misbehave
// arbitrarily (Byzantine faults: lying, equivocating, going silent). This
// This file is the normal-case core: the pre-prepare / prepare / commit
// protocol that totally orders client requests and applies them to a state
// machine once a commit certificate (2f+1 matching commits) is reached.
// There is no leader election: the primary for view v is the (v mod N)-th
// replica (p = v mod |R|). View-change and new-view (recovering from a faulty
// primary) live in viewchange.go. Later milestones add message authentication
// with dynamic key exchange (M3), WAL durability (M4) and store integration
// (M5).
package pbft

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrNotPrimary is returned by Submit when this replica is not the primary of
// the current view (mirrors raft.ErrNotLeader).
var ErrNotPrimary = errors.New("pbft: not primary")

// Config configures a PBFT replica.
type Config struct {
	ID    string   // this replica's id (must be unique in the cluster)
	Peers []string // peer replica ids, excluding self

	// ViewChangeTimeout is how long a backup waits, after accepting a
	// proposal it cannot execute, before suspecting the primary and starting
	// a view change. 0 keeps the default (2s). Tests usually trigger view
	// changes explicitly via StartViewChange to stay deterministic.
	ViewChangeTimeout time.Duration
}

// defaultViewChangeTimeout applies when Config.ViewChangeTimeout is zero.
const defaultViewChangeTimeout = 2 * time.Second

// Transport delivers PBFT messages to peers. Implementations must be safe for
// concurrent use: the in-memory test transport wires replicas directly, and
// the NDJSON transport (a later milestone) encodes the messages over TCP.
type Transport interface {
	SendPrePrepare(ctx context.Context, peer string, m *PrePrepare) error
	SendPrepare(ctx context.Context, peer string, m *Prepare) error
	SendCommit(ctx context.Context, peer string, m *Commit) error
	SendViewChange(ctx context.Context, peer string, m *ViewChange) error
	SendNewView(ctx context.Context, peer string, m *NewView) error
}

// Replica is a single PBFT replica running the normal-case protocol.
type Replica struct {
	id      string
	all     []string        // sorted replica ids, including self
	members map[string]bool // every replica id (including self)
	peers   []string        // peer replica ids (all except self)
	f       int             // max byzantine faults tolerated: len(all) == 3f+1
	tr      Transport
	// priv is this replica's Ed25519 identity key (M3): it signs every message
	// this replica sends. pub is its public half; peerKeys holds the verified
	// public key of each peer.
	priv     ed25519.PrivateKey
	pub      ed25519.PublicKey
	peerKeys map[string]ed25519.PublicKey

	// applyFn executes one request, in sequence order, once the request is
	// commit-certified. It runs while the node lock is held and must not call
	// back into the replica.
	applyFn func(seq uint64, req Request)
	// logStore, when set, durably persists accepted requests and the executed
	// watermark (M4). Nil means in-memory only (unit tests).
	logStore LogStore

	mu           sync.Mutex
	view         uint64 // current view
	lastAssigned uint64 // highest sequence number the primary has handed out
	nextExec     uint64 // first not-yet-executed sequence number
	lastExecTime time.Time
	vcTimeout    time.Duration // auto-suspect timeout
	viewChanges  map[uint64]map[string]*ViewChange
	vcSent       map[uint64]bool
	nvSent       map[uint64]bool
	log          map[uint64]*entry
	// pendingPrepares / pendingCommits buffer prepares and commits that
	// arrive before the matching pre-prepare (PBFT permits arbitrary message
	// reordering); they are folded into the entry once it is pre-prepared.
	// Prepare messages are retained so a ViewChange can later ship a verifiable
	// prepared certificate.
	pendingPrepares map[msgKey]map[string]*Prepare
	pendingCommits  map[msgKey]map[string]bool
	// seen records (client, timestamp) -> request digest, so a replica
	// detects a request replayed under a conflicting sequence number.
	//
	// ponytail: seen, log and pending grow without bound until M4/M5 add a
	// stable checkpoint watermark and gc. Fine for a milestone engine.
	seen map[reqKey]string
	// executedAt records the sequence at which each request was executed, so a
	// later view-change can never re-order an executed request.
	executedAt map[reqKey]uint64

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// reqKey identifies a client request.
type reqKey struct {
	client string
	ts     uint64
}

// msgKey identifies buffered prepare/commit messages by view, sequence number
// and digest. The view matters: a prepare for the imminent view must not be
// folded into an entry from an earlier view.
type msgKey struct {
	view   uint64
	seq    uint64
	digest string
}

// entry is the per-sequence-number protocol state at this replica. view is the
// view in which the current proposal was (re-)started; a new view supersedes
// an unexecuted entry from an older view at the same sequence number. pp is
// the signed pre-prepare that ordered the entry and prepares retains the
// signed matching prepares from other replicas, so a prepared entry can later
// ship a verifiable prepared certificate in a ViewChange.
type entry struct {
	seq         uint64
	view        uint64
	digest      string
	req         Request
	pp          *PrePrepare
	prePrepared bool // a valid pre-prepare was seen/issued for (view, seq)
	prepares    map[string]*Prepare
	sentCommit  bool
	commits     map[string]bool
}

// commitCertified reports whether this replica holds a commit certificate for
// the entry: its own commit plus 2f matching commits from other replicas
// (2f+1 matching commits in total, PBFT §4.1).
func (e *entry) commitCertified(f int) bool {
	return e.sentCommit && len(e.commits) >= 2*f
}

// primaryID returns the primary replica of a view: the (view mod N)-th member
// of the sorted replica set (PBFT §4.1, p = v mod |R|).
func primaryID(all []string, view uint64) string {
	return all[int(view)%len(all)]
}

// NewReplica creates a PBFT replica. tr delivers messages to peers; applyFn is
// invoked once per executed request, in sequence order, and must not call back
// into the replica. The cluster size must be exactly 3f+1 (1, 4, 7, ...).
func NewReplica(cfg Config, tr Transport, applyFn func(seq uint64, req Request)) (*Replica, error) {
	if cfg.ID == "" {
		return nil, errors.New("pbft: replica id is required")
	}
	if tr == nil {
		return nil, errors.New("pbft: transport is required")
	}
	all := append([]string{cfg.ID}, cfg.Peers...)
	unique := make(map[string]bool, len(all))
	for _, id := range all {
		if unique[id] {
			return nil, fmt.Errorf("pbft: duplicate replica id %q", id)
		}
		unique[id] = true
	}
	if n := len(all); n > 1 && (n-1)%3 != 0 {
		return nil, fmt.Errorf("pbft: cluster of %d replicas is not 3f+1 (want 1, 4, 7, ...)", n)
	}
	sort.Strings(all)
	members := make(map[string]bool, len(all))
	var peers []string
	for _, id := range all {
		members[id] = true
		if id != cfg.ID {
			peers = append(peers, id)
		}
	}
	vcTimeout := defaultViewChangeTimeout
	if cfg.ViewChangeTimeout > 0 {
		vcTimeout = cfg.ViewChangeTimeout
	}
	n := &Replica{
		id:              cfg.ID,
		all:             all,
		members:         members,
		peers:           peers,
		f:               (len(all) - 1) / 3,
		tr:              tr,
		applyFn:         applyFn,
		peerKeys:        make(map[string]ed25519.PublicKey),
		viewChanges:     make(map[uint64]map[string]*ViewChange),
		vcSent:          make(map[uint64]bool),
		nvSent:          make(map[uint64]bool),
		vcTimeout:       vcTimeout,
		lastExecTime:    time.Now(),
		log:             make(map[uint64]*entry),
		seen:            make(map[reqKey]string),
		executedAt:      make(map[reqKey]uint64),
		nextExec:        1, // first sequence number to execute (sequences start at 1)
		pendingPrepares: make(map[msgKey]map[string]*Prepare),
		pendingCommits:  make(map[msgKey]map[string]bool),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
	pub, priv, err := newKeyPair()
	if err != nil {
		return nil, fmt.Errorf("pbft: generating identity key: %w", err)
	}
	n.priv = priv
	n.pub = pub
	// A NEW-VIEW's collected view-changes include this replica's own, which it
	// must be able to re-verify; register our own key too.
	n.peerKeys[cfg.ID] = pub
	return n, nil
}

// Run starts background goroutines: a view-change suspicion timer (a backup
// that has accepted a proposal it cannot execute for ViewChangeTimeout starts
// a view change) and the stop waiter.
func (n *Replica) Run() {
	go func() {
		defer close(n.doneCh)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.mu.Lock()
				// A backup suspects the primary when it is stuck: it holds an
				// accepted proposal it cannot execute and no progress has been
				// made for a full timeout.
				stuck := primaryID(n.all, n.view) != n.id &&
					time.Since(n.lastExecTime) > n.vcTimeout &&
					n.hasUnexecutedLocked()
				if stuck && !n.vcSent[n.view+1] {
					n.startViewChangeLocked()
				}
				n.mu.Unlock()
			case <-n.stopCh:
				return
			}
		}
	}()
}

// Stop shuts down the replica's background goroutines. Safe to call multiple
// times.
func (n *Replica) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		<-n.doneCh
	})
}

// ---- accessors ----

func (n *Replica) ID() string   { return n.id }
func (n *Replica) F() int       { return n.f }
func (n *Replica) View() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.view }

// Primary returns the current view's primary id.
func (n *Replica) Primary() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return primaryID(n.all, n.view)
}

// IsPrimary reports whether this replica is the current view's primary.
func (n *Replica) IsPrimary() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return primaryID(n.all, n.view) == n.id
}

// LastExecuted returns the highest sequence number executed (0 if none).
func (n *Replica) LastExecuted() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nextExec - 1
}

// PublicKey returns this replica's Ed25519 identity public key, which peers
// must register via SetPeerKey before accepting this replica's messages.
func (n *Replica) PublicKey() ed25519.PublicKey { return n.pub }

// SetPeerKey registers the identity public key of a peer (the dynamic key
// exchange / handshake outcome). Messages whose Sender has no registered key
// are rejected.
func (n *Replica) SetPeerKey(peer string, pub ed25519.PublicKey) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerKeys[peer] = pub
}

// sign returns this replica's signature over m (see signPayload).
func (n *Replica) sign(m any) []byte {
	return signPayload(n.priv, m)
}

// verify reports whether sig is this replica's recorded peer's signature over
// m; an unregistered sender never verifies.
func (n *Replica) verify(sender string, sig []byte, m any) bool {
	pub, ok := n.peerKeys[sender]
	if !ok {
		return false
	}
	return verifyPayload(pub, sig, m)
}

// isPeer reports whether s is another replica in the cluster.
func (n *Replica) isPeer(s string) bool {
	return s != n.id && n.members[s]
}

// broadcast runs fn once per peer, each in its own goroutine so a slow or
// silent peer never stalls the local replica. Must be called with n.mu held;
// transport calls happen off the lock.
func (n *Replica) broadcast(fn func(peer string)) {
	for _, p := range n.peers {
		go fn(p)
	}
}

// pollWake releases the node lock, waits briefly (or until ctx is done), then
// re-acquires it. Must be called with n.mu held. Used to poll for state
// changes while remaining context-cancellable.
func (n *Replica) pollWake(ctx context.Context) error {
	n.mu.Unlock()
	select {
	case <-time.After(2 * time.Millisecond):
	case <-ctx.Done():
	}
	n.mu.Lock()
	return ctx.Err()
}

// ---- client-facing API (primary only) ----

// Submit orders command through the normal-case protocol and returns its
// sequence number. Only the current view's primary accepts submissions;
// followers return ErrNotPrimary (clients retry at the new primary after a
// view change, M2).
func (n *Replica) Submit(command []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if primaryID(n.all, n.view) != n.id {
		return 0, ErrNotPrimary
	}
	seq := n.lastAssigned + 1
	req := Request{Client: n.id + "/srv", Timestamp: seq, Command: command}
	d := digestOf(req)
	e := n.newEntry(n.view, seq, d, req)
	e.prePrepared = true
	n.log[seq] = e
	n.seen[reqKey{req.Client, req.Timestamp}] = d
	n.lastAssigned = seq
	// Durably record the order before it is acted on, so a crash does not
	// forget a request this replica accepted.
	if err := n.persistRequestLocked(n.view, seq, req); err != nil {
		return 0, err
	}
	if len(n.peers) == 0 {
		// A single-replica cluster (f = 0) has no peers to certify with.
		e.sentCommit = true
		n.executeReady()
		return seq, nil
	}
	pp := &PrePrepare{View: n.view, Seq: seq, Digest: d, Req: req, Sender: n.id}
	pp.Sig = n.sign(pp)
	e.pp = pp
	n.broadcast(func(p string) { _ = n.tr.SendPrePrepare(context.Background(), p, pp) })
	return seq, nil
}

// WaitApplied blocks until the request at seq has been executed, or ctx is
// done. Safe to call from any replica; normally used on the primary after
// Submit.
func (n *Replica) WaitApplied(ctx context.Context, seq uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.nextExec <= seq {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := n.pollWake(ctx); err != nil {
			return err
		}
	}
	return nil
}

// ---- normal-case protocol handlers (PBFT §4.1) ----

// HandlePrePrepare processes a primary's ordering proposal. A backup accepts
// it only from the view's primary, for the current view, with a digest that
// matches the shipped request and no conflicting earlier proposal at the same
// sequence number; it then multicasts a matching Prepare.
func (n *Replica) HandlePrePrepare(m *PrePrepare) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.verify(m.Sender, m.Sig, m) {
		return // unauthenticated: signature does not match the claimed sender
	}
	if !n.isPeer(m.Sender) || m.View != n.view {
		return
	}
	if m.Sender != primaryID(n.all, n.view) {
		return // only the view's primary may propose
	}
	if m.Seq == 0 {
		return // sequence numbers start at 1
	}
	d := digestOf(m.Req)
	if d != m.Digest {
		return // request content does not match the claimed digest
	}
	if prev, ok := n.seen[reqKey{m.Req.Client, m.Req.Timestamp}]; ok && prev != d {
		return // the same request id is already bound to a different digest
	}
	e := n.log[m.Seq]
	if e != nil && e.view == n.view && e.digest != d {
		return // equivocation: two different proposals for (view, seq)
	}
	if e == nil {
		e = n.newEntry(n.view, m.Seq, d, m.Req)
		n.log[m.Seq] = e
		n.seen[reqKey{m.Req.Client, m.Req.Timestamp}] = d
	} else if e.view == n.view && e.prePrepared {
		return // already processed in this view
	} else if e.digest != d {
		// An unexecuted proposal from an older view at this sequence is
		// superseded by the current view's proposal.
		e = n.newEntry(n.view, m.Seq, d, m.Req)
		n.log[m.Seq] = e
		n.seen[reqKey{m.Req.Client, m.Req.Timestamp}] = d
	}
	e.prePrepared = true
	e.view = n.view
	e.pp = m // retain the signed pre-prepare as certificate evidence
	// Discard buffered prepares/commits for a different (equivocating) digest
	// at this sequence: they can never match what we accepted.
	n.dropConflictingPending(n.view, m.Seq, d)
	// Durably record the accepted order before broadcasting the Prepare, so a
	// crash does not forget a request this replica accepted.
	if err := n.persistRequestLocked(n.view, m.Seq, m.Req); err != nil {
		return // not durable: do not act on it
	}
	pr := &Prepare{View: n.view, Seq: m.Seq, Digest: d, Sender: n.id}
	pr.Sig = n.sign(pr)
	n.broadcast(func(p string) { _ = n.tr.SendPrepare(context.Background(), p, pr) })
	n.syncEntry(m.Seq)
}

// dropConflictingPending removes buffered prepares/commits for seq in view
// whose digest differs from d. Must be called with n.mu held.
func (n *Replica) dropConflictingPending(view, seq uint64, d string) {
	for k := range n.pendingPrepares {
		if k.view == view && k.seq == seq && k.digest != d {
			delete(n.pendingPrepares, k)
		}
	}
	for k := range n.pendingCommits {
		if k.view == view && k.seq == seq && k.digest != d {
			delete(n.pendingCommits, k)
		}
	}
}

// HandlePrepare records a peer's matching Prepare. Prepares may arrive before
// the pre-prepare they match (PBFT allows arbitrary reordering), so they are
// buffered and folded in once the sequence is pre-prepared (syncEntry). When
// 2f matching prepares back the pre-prepare (a prepared certificate), the
// replica multicasts its Commit.
func (n *Replica) HandlePrepare(m *Prepare) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.verify(m.Sender, m.Sig, m) {
		return // unauthenticated
	}
	if !n.isPeer(m.Sender) {
		return
	}
	if m.Sender == primaryID(n.all, m.View) {
		// The view's primary votes with its pre-prepare, not a prepare; counting
		// its prepare too would let it contribute twice (PBFT counts 2f prepares
		// from distinct backups).
		return
	}
	// A prepare for the current view is counted; one for the imminent view is
	// buffered so it survives the view change (it would otherwise be lost to a
	// peer that has not entered the new view yet — the same out-of-order
	// hazard as within a view). Anything further ahead is dropped.
	if m.View != n.view && m.View != n.view+1 {
		return
	}
	e := n.log[m.Seq]
	if e != nil && e.view == m.View && e.digest != m.Digest {
		return // conflicts with the proposal accepted at this (view, seq)
	}
	if n.pendingPrepares[msgKey{m.View, m.Seq, m.Digest}] == nil {
		n.pendingPrepares[msgKey{m.View, m.Seq, m.Digest}] = make(map[string]*Prepare)
	}
	n.pendingPrepares[msgKey{m.View, m.Seq, m.Digest}][m.Sender] = m
	n.syncEntry(m.Seq)
}

// HandleCommit records a peer's matching Commit, buffering it like a Prepare
// when it arrives early. When this replica holds a commit certificate it
// executes the request — and any other commit-certified requests waiting at
// the next sequence numbers, in order.
func (n *Replica) HandleCommit(m *Commit) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.verify(m.Sender, m.Sig, m) {
		return // unauthenticated
	}
	if !n.isPeer(m.Sender) {
		return
	}
	if m.View != n.view && m.View != n.view+1 {
		return
	}
	e := n.log[m.Seq]
	if e != nil && e.view == m.View && e.digest != m.Digest {
		return // conflicts with the proposal accepted at this (view, seq)
	}
	if n.pendingCommits[msgKey{m.View, m.Seq, m.Digest}] == nil {
		n.pendingCommits[msgKey{m.View, m.Seq, m.Digest}] = make(map[string]bool)
	}
	n.pendingCommits[msgKey{m.View, m.Seq, m.Digest}][m.Sender] = true
	n.syncEntry(m.Seq)
}

// syncEntry folds buffered prepares/commits into the entry for seq and drives
// the prepared → commit → commit-certificate transitions. Must be called with
// n.mu held.
func (n *Replica) syncEntry(seq uint64) {
	e := n.log[seq]
	if e == nil {
		return
	}
	// Fold matching prepares once we hold the pre-prepare.
	k := msgKey{e.view, seq, e.digest}
	for s, pm := range n.pendingPrepares[k] {
		e.prepares[s] = pm
	}
	delete(n.pendingPrepares, k)
	if len(e.prepares) >= 2*n.f && !e.sentCommit {
		e.sentCommit = true
		c := &Commit{View: n.view, Seq: seq, Digest: e.digest, Sender: n.id}
		c.Sig = n.sign(c)
		n.broadcast(func(p string) { _ = n.tr.SendCommit(context.Background(), p, c) })
	}
	if !e.sentCommit {
		return
	}
	// Fold matching commits (they only matter once we have sent our own).
	for s := range n.pendingCommits[k] {
		e.commits[s] = true
	}
	delete(n.pendingCommits, k)
	if e.commitCertified(n.f) {
		n.executeReady()
	}
}

// newEntry allocates an empty per-sequence entry for view.
func (n *Replica) newEntry(view, seq uint64, d string, req Request) *entry {
	return &entry{
		seq:      seq,
		view:     view,
		digest:   d,
		req:      req,
		prepares: make(map[string]*Prepare),
		commits:  make(map[string]bool),
	}
}

// executeReady applies every commit-certified request at the next sequence
// number in order. Requests that are certified out of order wait for the gap
// to fill: replicas execute in the total order the primary assigned. Must be
// called with n.mu held.
//
// ponytail: applyFn runs under the node lock, so a slow state machine stalls
// message handling. Fine for an in-memory cache FSM; the upgrade path is a
// dedicated apply goroutine with an executed watermark (same tradeoff raft
// makes).
func (n *Replica) executeReady() {
	for {
		e := n.log[n.nextExec]
		if e == nil || !e.commitCertified(n.f) {
			return
		}
		seq, req := n.nextExec, e.req
		n.nextExec++
		n.executedAt[reqKey{req.Client, req.Timestamp}] = seq
		n.lastExecTime = time.Now()
		if n.applyFn != nil {
			n.applyFn(seq, req)
		}
		n.persistAppliedLocked(seq)
	}
}
