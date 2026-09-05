package server

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Sephy314/Cachey/internal/pbft"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// proposeTimeout bounds how long a client write may wait for PBFT commit.
const pbftProposeTimeout = 5 * time.Second

// PbftClusterStore is a store.Store whose mutations are replicated through a
// PBFT replica and applied to the local FSM when they execute. It is the PBFT
// counterpart of ClusterStore (Raft): writes go through the view's primary
// (followers fail with pbft.ErrNotPrimary and the caller can redirect), and
// reads are served by the primary after it has applied everything it ordered.
//
// ponytail: reads wait for this primary to catch up to its own ordered requests
// (read-your-writes) but do not run a full linearizable read against a quorum;
// a linearizable PBFT read would confirm the view is stable first (or order the
// read). Fine for a cache; raft's read-index is the analogous future work.
type PbftClusterStore struct {
	node   *pbft.Replica
	fsm    store.Store
	addrOf func(string) string // primary node ID → client address (redirects)
}

// NewPbftClusterStore wraps a PBFT replica as a store.Store. The replica's
// applyFn must already apply committed requests to fsm (see NewPbftApply).
func NewPbftClusterStore(node *pbft.Replica, fsm store.Store) *PbftClusterStore {
	return &PbftClusterStore{node: node, fsm: fsm}
}

// SetLeaderResolver maps a primary node ID to its client address, used for
// redirect hints.
func (c *PbftClusterStore) SetLeaderResolver(fn func(string) string) { c.addrOf = fn }

// NewPbftApply builds the applyFn that decodes an executed request's command (a
// wal.Record payload) into fsm.ApplyRecord. Store apply errors such as a
// missing key on DEL/TTL are benign (no state change) and logged, not fatal.
func NewPbftApply(fsm *store.CacheyStore) func(seq uint64, req pbft.Request) {
	return func(seq uint64, req pbft.Request) {
		var rec wal.Record
		if err := json.Unmarshal(req.Command, &rec); err != nil {
			log.Printf("pbft apply: bad command: %v", err)
			return
		}
		if err := fsm.ApplyRecord(rec); err != nil {
			log.Printf("pbft apply: %v", err)
		}
	}
}

func (c *PbftClusterStore) propose(rec wal.Record) error {
	cmd, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	seq, err := c.node.Submit(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), pbftProposeTimeout)
	defer cancel()
	return c.node.WaitApplied(ctx, seq)
}

func (c *PbftClusterStore) Get(key string) (*string, error) {
	// Read-your-writes: as the primary, wait until everything this replica
	// ordered has applied, then read the local FSM (see the ponytail note).
	ctx, cancel := context.WithTimeout(context.Background(), pbftProposeTimeout)
	defer cancel()
	if err := c.node.WaitCaughtUp(ctx); err != nil {
		return nil, err
	}
	return c.fsm.Get(key)
}

func (c *PbftClusterStore) Put(key, value string) error {
	return c.propose(wal.Record{Op: wal.OpPut, Key: key, Val: value})
}

func (c *PbftClusterStore) Delete(key string) error {
	return c.propose(wal.Record{Op: wal.OpDelete, Key: key})
}

func (c *PbftClusterStore) TTL(key string, ttlMillis int64) error {
	exp := time.Now().UnixMilli() + ttlMillis
	return c.propose(wal.Record{Op: wal.OpTTL, Key: key, Exp: exp})
}

func (c *PbftClusterStore) Alive() string { return c.fsm.Alive() }

// Leader returns the current primary's client address, or "" when this node is
// the primary, no primary is known, or no resolver is configured.
func (c *PbftClusterStore) Leader() string {
	if c.addrOf == nil {
		return ""
	}
	if c.node.IsPrimary() {
		return ""
	}
	return c.addrOf(c.node.Primary())
}
