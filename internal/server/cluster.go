package server

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// proposeTimeout bounds how long a client write may wait for raft commit.
const proposeTimeout = 5 * time.Second

// ClusterStore is a store.Store whose mutations are replicated through a Raft
// node and applied to the local FSM (store.CacheyStore) when committed. Reads
// are served from the local FSM (upgraded to linearizable reads in a later
// milestone). Writes must be sent to the leader; followers fail with
// raft.ErrNotLeader and the caller can redirect.
type ClusterStore struct {
	node *raft.Node
	fsm  store.Store
}

// NewClusterStore wraps a raft node as a store.Store. The node's applyFn must
// already apply committed entries to fsm (see NewRaftApply).
func NewClusterStore(node *raft.Node, fsm store.Store) *ClusterStore {
	return &ClusterStore{node: node, fsm: fsm}
}

// NewRaftApply builds the applyFn that decodes a committed entry (a wal.Record
// payload) into fsm.ApplyRecord. Store apply errors such as a missing key on
// DEL/TTL are benign (no state change) and logged, not fatal.
func NewRaftApply(fsm *store.CacheyStore) func(raft.Entry) {
	return func(e raft.Entry) {
		var rec wal.Record
		if err := json.Unmarshal(e.Command, &rec); err != nil {
			log.Printf("raft apply: bad command: %v", err)
			return
		}
		if err := fsm.ApplyRecord(rec); err != nil {
			log.Printf("raft apply: %v", err)
		}
	}
}

func (c *ClusterStore) propose(rec wal.Record) error {
	cmd, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	idx, err := c.node.Propose(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	defer cancel()
	return c.node.WaitApplied(ctx, idx)
}

func (c *ClusterStore) Get(key string) (*string, error) {
	return c.fsm.Get(key)
}

func (c *ClusterStore) Put(key, value string) error {
	return c.propose(wal.Record{Op: wal.OpPut, Key: key, Val: value})
}

func (c *ClusterStore) Delete(key string) error {
	return c.propose(wal.Record{Op: wal.OpDelete, Key: key})
}

func (c *ClusterStore) TTL(key string, ttlMillis int64) error {
	exp := time.Now().UnixMilli() + ttlMillis
	return c.propose(wal.Record{Op: wal.OpTTL, Key: key, Exp: exp})
}

func (c *ClusterStore) Alive() string { return c.fsm.Alive() }
