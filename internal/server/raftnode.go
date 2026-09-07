package server

import (
	"encoding/json"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/internal/wal"
)

// RaftNode is a fully wired, persistent raft node exposing its replicated
// store (ClusterStore) for client traffic. It is the production counterpart of
// the persistent test cluster's node (internal/server/cluster_test.go): an FSM
// plus a raft node over TCP, WAL-durable log, and snapshots, recovered in the
// order snapshot → WAL.
type RaftNode struct {
	ID       string
	Dir      string
	Store    *store.CacheyStore
	Node     *raft.Node
	Tr       *raft.TCPTransport
	WAL      *wal.WAL
	CS       *ClusterStore
	RaftAddr string // bound raft RPC address (host:port)
}

// RaftNodeConfig configures OpenRaftNode. Zero Heartbeat/Election/Snapshot
// fields fall back to raft defaults.
type RaftNodeConfig struct {
	ID                string
	Dir               string
	RaftAddr          string // raft RPC listen address, e.g. "127.0.0.1:9101"
	Peers             []string
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
}

// OpenRaftNode opens (or recovers) a persistent raft node and returns it with
// its transport already listening on cfg.RaftAddr.
//
// A recovered member (cfg.Dir already holds a log/snapshot) is a full member
// again once the caller calls Node.Run(). A brand-new node that will join an
// existing cluster must NOT Run until the leader has added it (raft.AddServer
// via the -join control flow), so it never briefly self-elects as a
// single-node cluster.
func OpenRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	st := store.NewCacheyStore()

	tr := raft.NewTCPTransport(nil)
	bound, err := tr.Listen(cfg.RaftAddr)
	if err != nil {
		return nil, err
	}
	closeOnErr := func() {
		tr.Close()
	}

	n, err := raft.NewNode(raft.Config{
		ID:                cfg.ID,
		Peers:             cfg.Peers,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ElectionTimeout:   cfg.ElectionTimeout,
		SnapshotThreshold: cfg.SnapshotThreshold,
	}, tr, NewRaftApply(st))
	if err != nil {
		closeOnErr()
		return nil, err
	}
	n.SetSnapshotCallbacks(
		func() ([]byte, error) {
			entries, err := st.Snapshot()
			if err != nil {
				return nil, err
			}
			return json.Marshal(entries)
		},
		func(data []byte) error {
			var entries []wal.SnapshotEntry
			if err := json.Unmarshal(data, &entries); err != nil {
				return err
			}
			return st.ApplySnapshot(entries)
		},
	)
	ss := raft.NewFileSnapshotStore(cfg.Dir)
	n.SetSnapshotStore(ss)
	// Recovery order: restore a persisted snapshot (FSM + log base) before
	// replaying the WAL so records at or before the snapshot are skipped.
	if snap, ok, err := ss.Load(); err == nil && ok {
		if err := n.RestoreSnapshot(snap); err != nil {
			closeOnErr()
			return nil, err
		}
	}
	wcfg := wal.DefaultConfig(cfg.Dir)
	wcfg.DisableRotation = true
	w, err := wal.Open(wcfg, wal.Hooks{
		ApplySnapshot: st.ApplySnapshot,
		ApplyRecord:   n.ApplyRecoveredRecord,
		Snapshot:      st.Snapshot,
	})
	if err != nil {
		closeOnErr()
		return nil, err
	}
	n.SetLogStore(raft.NewWALLogStore(w))
	tr.SetNode(n)
	// Advertise our own raft address so membership configurations carry it
	// (see raft.Node.AddServer) and every member learns to reach us.
	tr.RegisterPeer(cfg.ID, bound)
	return &RaftNode{
		ID:       cfg.ID,
		Dir:      cfg.Dir,
		Store:    st,
		Node:     n,
		Tr:       tr,
		WAL:      w,
		CS:       NewClusterStore(n, st),
		RaftAddr: bound,
	}, nil
}

// Stop shuts the node down cleanly: raft loops, then transport, then WAL.
func (rn *RaftNode) Stop() {
	rn.Node.Stop()
	rn.Tr.Close()
	rn.WAL.Close()
}
