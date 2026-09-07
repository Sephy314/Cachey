package server

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Sephy314/Cachey/internal/hotstuff"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// hsProposeTimeout bounds how long a client write may wait for a HotStuff
// commit, and hsPollInterval how often the leader re-checks while flushing the
// empty blocks that complete the 3-chain above a command.
const (
	hsProposeTimeout = 5 * time.Second
	hsPollInterval   = 20 * time.Millisecond
)

// HotStuffClusterStore is a store.Store whose mutations are replicated through
// a HotStuff replica and applied to the local FSM when they execute. It is the
// HotStuff counterpart of PbftClusterStore: writes go through the view's leader
// (followers fail with hotstuff.ErrNotLeader and the caller can redirect).
//
// Chained HotStuff commits a block only once two more blocks carry QCs above
// it, so the leader flushes empty blocks after each command until the command
// block commits locally. This store drives those flushes (there is no
// background auto-advance); a leader with no traffic simply stays idle.
//
// ponytail: reads on the leader are read-your-writes — every Put this store has
// returned is already applied locally — but they do not run a quorum
// linearizable read, and a write still in flight on another goroutine may not
// be visible yet. Fine for a cache; raft's read-index is the analogous future
// work.
type HotStuffClusterStore struct {
	node   *hotstuff.Replica
	fsm    store.Store
	addrOf func(string) string // leader node ID → client address (redirects)
}

// NewHotStuffClusterStore wraps a HotStuff replica as a store.Store. The
// replica's applyFn must already apply committed commands to fsm (see
// NewHotStuffApply).
func NewHotStuffClusterStore(node *hotstuff.Replica, fsm store.Store) *HotStuffClusterStore {
	return &HotStuffClusterStore{node: node, fsm: fsm}
}

// SetLeaderResolver maps a leader node ID to its client address, used for
// redirect hints.
func (c *HotStuffClusterStore) SetLeaderResolver(fn func(string) string) { c.addrOf = fn }

// NewHotStuffApply builds the applyFn that decodes a committed block's Cmd (a
// wal.Record payload) into fsm.ApplyRecord. Empty blocks (chain-advancing
// flushes) carry no command and are skipped. Store apply errors such as a
// missing key on DEL/TTL are benign (no state change) and logged, not fatal.
func NewHotStuffApply(fsm *store.CacheyStore) func(hotstuff.Block) {
	return func(b hotstuff.Block) {
		if len(b.Cmd) == 0 {
			return // an empty flush block
		}
		var rec wal.Record
		if err := json.Unmarshal(b.Cmd, &rec); err != nil {
			log.Printf("hotstuff apply: bad command: %v", err)
			return
		}
		if err := fsm.ApplyRecord(rec); err != nil {
			log.Printf("hotstuff apply: %v", err)
		}
	}
}

// propose replicates one mutation: the leader proposes the command block, then
// flushes empty blocks until that block commits locally (a Chained HotStuff
// block needs two QCs above it before it commits).
func (c *HotStuffClusterStore) propose(rec wal.Record) error {
	if !c.node.IsLeader() {
		return hotstuff.ErrNotLeader
	}
	cmd, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), hsProposeTimeout)
	defer cancel()

	// Propose the command block, retrying while the previous QC is still
	// forming (the leader is "busy" until it can justify the next block).
	var target string
	for {
		id, perr := c.node.Propose(cmd)
		if perr == nil {
			target = id
			break
		}
		if perr != hotstuff.ErrBusy {
			return perr // e.g. ErrNotLeader: a view change happened mid-write
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(hsPollInterval):
		}
	}

	// Keep the chain advancing until the command block commits and applies.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		check, cancelCheck := context.WithTimeout(ctx, hsPollInterval)
		err := c.node.WaitCommitted(check, target)
		cancelCheck()
		if err == nil {
			// Committed and applied on the leader. Hand followers the one block
			// they need to fold the final QC and apply it too — in Chained
			// HotStuff a follower commits a block only once it sees a block
			// justified by the QC two heights above, i.e. one block AFTER the
			// leader. One best-effort empty flush (the leader is free now that
			// the QC above the command block has formed) closes that gap.
			c.node.Propose(nil) //nolint:errcheck
			return nil
		}
		if err != context.DeadlineExceeded {
			return err
		}
		if _, perr := c.node.Propose(nil); perr != nil && perr != hotstuff.ErrBusy {
			return perr
		}
	}
}

func (c *HotStuffClusterStore) Get(key string) (*string, error) {
	// Read-your-writes on the leader (see the ponytail note); followers must
	// not serve possibly-stale reads and instead redirect.
	if !c.node.IsLeader() {
		return nil, hotstuff.ErrNotLeader
	}
	return c.fsm.Get(key)
}

func (c *HotStuffClusterStore) Put(key, value string) error {
	return c.propose(wal.Record{Op: wal.OpPut, Key: key, Val: value})
}

func (c *HotStuffClusterStore) Delete(key string) error {
	return c.propose(wal.Record{Op: wal.OpDelete, Key: key})
}

func (c *HotStuffClusterStore) TTL(key string, ttlMillis int64) error {
	exp := time.Now().UnixMilli() + ttlMillis
	return c.propose(wal.Record{Op: wal.OpTTL, Key: key, Exp: exp})
}

func (c *HotStuffClusterStore) Alive() string { return c.fsm.Alive() }

// Leader returns the current leader's client address ("" if this node is the
// leader, no leader is known, or no resolver is configured).
func (c *HotStuffClusterStore) Leader() string {
	if c.addrOf == nil {
		return ""
	}
	if c.node.IsLeader() {
		return ""
	}
	return c.addrOf(c.node.Leader())
}
