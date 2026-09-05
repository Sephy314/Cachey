// Package pbft implements a Practical Byzantine Fault Tolerance replica
// (Castro & Liskov, "Practical Byzantine Fault Tolerance", OSDI '99). A
// cluster of N = 3f+1 replicas stays correct while up to f of them misbehave
// arbitrarily (Byzantine faults: lying, equivocating, going silent). This
// file is milestone M1: the normal-case protocol on a static primary (view
// 0) — pre-prepare / prepare / commit — which totally orders client requests
// and applies them to a state machine once a commit certificate is reached.
// There is no leader election: the primary for view v is the (v mod N)-th
// replica, so M1's leader is fixed. Later milestones add view-change (M2),
// message authentication with dynamic key exchange (M3), WAL durability (M4)
// and store integration (M5).
package pbft

import (
	"context"
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
}

// Transport delivers PBFT messages to peers. Implementations must be safe for
// concurrent use: the in-memory test transport wires replicas directly, and
// the NDJSON transport (a later milestone) encodes the messages over TCP.
type Transport interface {
	SendPrePrepare(ctx context.Context, peer string, m *PrePrepare) error
	SendPrepare(ctx context.Context, peer string, m *Prepare) error
	SendCommit(ctx context.Context, peer string, m *Commit) error
}

// Replica is a single PBFT replica running the normal-case protocol.
type Replica struct {
	id      string
	all     []string        // sorted replica ids, including self
	members map[string]bool // every replica id (including self)
	peers   []string        // peer replica ids (all except self)
	f       int             // max byzantine faults tolerated: len(all) == 3f+1
	tr      Transport

	// applyFn executes one request, in sequence order, once the request is
	// commit-certified. It runs while the node lock is held and must not call
	// back into the replica.
	applyFn func(seq uint64, req Request)

	mu           sync.Mutex
	view         uint64 // current view (0 in M1)
	lastAssigned uint64 // highest sequence number the primary has handed out
	nextExec     uint64 // first not-yet-executed sequence number
	log          map[uint64]*entry
	// pendingPrepares / pendingCommits buffer prepares and commits that
	// arrive before the matching pre-prepare (PBFT permits arbitrary message
	// reordering); they are folded into the entry once it is pre-prepared.
	pendingPrepares map[msgKey]map[string]bool
	pendingCommits  map[msgKey]map[string]bool
	// seen records (client, timestamp) -> request digest, so a replica
	// detects a request replayed under a conflicting sequence number.
	//
	// ponytail: seen, log and pending grow without bound until M4/M5 add a
	// stable checkpoint watermark and gc. Fine for a milestone engine.
	seen map[reqKey]string

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// reqKey identifies a client request.
type reqKey struct {
	client string
	ts     uint64
}

// msgKey identifies prepare/commit messages by sequence number and digest.
type msgKey struct {
	seq    uint64
	digest string
}

// entry is the per-sequence-number protocol state at this replica.
type entry struct {
	seq         uint64
	digest      string
	req         Request
	prePrepared bool // a valid pre-prepare was seen/issued for (view, seq)
	prepares    map[string]bool
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
	return &Replica{
		id:              cfg.ID,
		all:             all,
		members:         members,
		peers:           peers,
		f:               (len(all) - 1) / 3,
		tr:              tr,
		applyFn:         applyFn,
		log:             make(map[uint64]*entry),
		seen:            make(map[reqKey]string),
		nextExec:        1, // first sequence number to execute (sequences start at 1)
		pendingPrepares: make(map[msgKey]map[string]bool),
		pendingCommits:  make(map[msgKey]map[string]bool),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}, nil
}

// Run starts background goroutines. In M1 the replica is purely reactive, so
// this only waits for Stop (M2 adds a view-change timer loop here).
func (n *Replica) Run() {
	go func() {
		<-n.stopCh
		close(n.doneCh)
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
	e := n.newEntry(seq, d, req)
	e.prePrepared = true
	n.log[seq] = e
	n.seen[reqKey{req.Client, req.Timestamp}] = d
	n.lastAssigned = seq
	if len(n.peers) == 0 {
		// A single-replica cluster (f = 0) has no peers to certify with.
		e.sentCommit = true
		n.executeReady()
		return seq, nil
	}
	pp := &PrePrepare{View: n.view, Seq: seq, Digest: d, Req: req, Sender: n.id}
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
	if e == nil {
		e = n.newEntry(m.Seq, d, m.Req)
		n.log[m.Seq] = e
		n.seen[reqKey{m.Req.Client, m.Req.Timestamp}] = d
	} else if e.digest != d {
		return // equivocation: two different proposals for (view, seq)
	} else if e.prePrepared {
		return // already processed
	}
	e.prePrepared = true
	// Discard buffered prepares/commits for a different (equivocating) digest
	// at this sequence: they can never match what we accepted.
	n.dropConflictingPending(m.Seq, d)
	pr := &Prepare{View: n.view, Seq: m.Seq, Digest: d, Sender: n.id}
	n.broadcast(func(p string) { _ = n.tr.SendPrepare(context.Background(), p, pr) })
	n.syncEntry(m.Seq)
}

// dropConflictingPending removes buffered prepares/commits for seq whose
// digest differs from d. Must be called with n.mu held.
func (n *Replica) dropConflictingPending(seq uint64, d string) {
	for k := range n.pendingPrepares {
		if k.seq == seq && k.digest != d {
			delete(n.pendingPrepares, k)
		}
	}
	for k := range n.pendingCommits {
		if k.seq == seq && k.digest != d {
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
	if !n.isPeer(m.Sender) || m.View != n.view {
		return
	}
	if e := n.log[m.Seq]; e != nil && e.digest != m.Digest {
		return // conflicts with the proposal we accepted for this sequence
	}
	if n.pendingPrepares[msgKey{m.Seq, m.Digest}] == nil {
		n.pendingPrepares[msgKey{m.Seq, m.Digest}] = make(map[string]bool)
	}
	n.pendingPrepares[msgKey{m.Seq, m.Digest}][m.Sender] = true
	n.syncEntry(m.Seq)
}

// HandleCommit records a peer's matching Commit, buffering it like a Prepare
// when it arrives early. When this replica holds a commit certificate it
// executes the request — and any other commit-certified requests waiting at
// the next sequence numbers, in order.
func (n *Replica) HandleCommit(m *Commit) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.isPeer(m.Sender) || m.View != n.view {
		return
	}
	if e := n.log[m.Seq]; e != nil && e.digest != m.Digest {
		return // conflicts with the proposal we accepted for this sequence
	}
	if n.pendingCommits[msgKey{m.Seq, m.Digest}] == nil {
		n.pendingCommits[msgKey{m.Seq, m.Digest}] = make(map[string]bool)
	}
	n.pendingCommits[msgKey{m.Seq, m.Digest}][m.Sender] = true
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
	for s := range n.pendingPrepares[msgKey{seq, e.digest}] {
		e.prepares[s] = true
	}
	delete(n.pendingPrepares, msgKey{seq, e.digest})
	if len(e.prepares) >= 2*n.f && !e.sentCommit {
		e.sentCommit = true
		c := &Commit{View: n.view, Seq: seq, Digest: e.digest, Sender: n.id}
		n.broadcast(func(p string) { _ = n.tr.SendCommit(context.Background(), p, c) })
	}
	if !e.sentCommit {
		return
	}
	// Fold matching commits (they only matter once we have sent our own).
	for s := range n.pendingCommits[msgKey{seq, e.digest}] {
		e.commits[s] = true
	}
	delete(n.pendingCommits, msgKey{seq, e.digest})
	if e.commitCertified(n.f) {
		n.executeReady()
	}
}

// newEntry allocates an empty per-sequence entry.
func (n *Replica) newEntry(seq uint64, d string, req Request) *entry {
	return &entry{
		seq:      seq,
		digest:   d,
		req:      req,
		prepares: make(map[string]bool),
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
		if n.applyFn != nil {
			n.applyFn(seq, req)
		}
	}
}
