package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/server"
	"github.com/Sephy314/Cachey/pkg/client"
)

const (
	raftHeartbeat = 100 * time.Millisecond
	raftElection  = 600 * time.Millisecond // randomized 600-1200ms, see raft tests
)

// clusterFlags holds the -consensus raft command-line options.
type clusterFlags struct {
	nodeID      string
	clientAddr  string // client-facing (data plane)
	controlAddr string // cluster control plane (JOIN)
	raftAddr    string
	dataDir     string
	bootstrap   bool
	join        string // an existing member's CONTROL address to join through
}

// runRaftCluster opens a persistent raft node, attaches a client-facing server
// over it, and either bootstraps a fresh cluster or joins an existing one,
// then serves until SIGINT/SIGTERM.
func runRaftCluster(cf *clusterFlags, opts []server.Option) error {
	if cf.nodeID == "" || cf.clientAddr == "" || cf.controlAddr == "" || cf.raftAddr == "" || cf.dataDir == "" {
		return fmt.Errorf("-consensus raft requires -node-id, -client-addr, -control-addr, -raft-addr and -data-dir")
	}
	if cf.bootstrap && cf.join != "" {
		return errors.New("use either -bootstrap or -join, not both")
	}
	if !cf.bootstrap && cf.join == "" {
		return errors.New("-consensus raft needs -bootstrap (first node) or -join <member-control-addr>")
	}

	rn, err := server.OpenRaftNode(server.RaftNodeConfig{
		ID:                cf.nodeID,
		Dir:               cf.dataDir,
		RaftAddr:          cf.raftAddr,
		HeartbeatInterval: raftHeartbeat,
		ElectionTimeout:   raftElection,
	})
	if err != nil {
		return fmt.Errorf("open raft node %s: %w", cf.nodeID, err)
	}
	defer rn.Stop()
	rn.Store.StartActiveExpiration(1 * time.Second)

	// Data-plane redirects: resolve a leader's node id to its client address
	// from the replicated member table every node keeps in its FSM.
	rn.CS.SetLeaderResolver(func(id string) string {
		if id == "" {
			return ""
		}
		if v, err := rn.Store.Get(server.MemberKey(id)); err == nil {
			return *v
		}
		return ""
	})

	switch {
	case cf.bootstrap:
		// First node of a new cluster (or a member restarting from an existing
		// data dir — recovery restores membership; -bootstrap means "go live
		// and register my own addresses").
		rn.Node.Run()
		if err := registerSelf(rn, cf); err != nil {
			return err
		}
	case hasWALData(cf.dataDir):
		// Restarting an existing member: recovery restores membership, so just
		// go live; -join re-announces this node's addresses through the leader.
		rn.Node.Run()
		if err := joinCluster(rn, cf, cf.join); err != nil {
			return err
		}
	default:
		// A brand-new node joining an existing cluster must be added by the
		// leader (raft.AddServer) BEFORE it runs, so it never briefly
		// self-elects as a 1-node cluster.
		if err := joinCluster(rn, cf, cf.join); err != nil {
			return err
		}
		rn.Node.Run()
		if err := waitFor("the cluster to catch this node up", 60*time.Second, func() error {
			if v, err := rn.Store.Get(server.MemberKey(rn.ID)); err != nil || *v != cf.clientAddr {
				return errRetry
			}
			if v, err := rn.Store.Get(server.ControlKey(rn.ID)); err != nil || *v != cf.controlAddr {
				return errRetry
			}
			return nil
		}); err != nil {
			return err
		}
	}

	// Start the two planes only once this node is a member: the client-facing
	// DATA server and the separate CONTROL server (JOIN). The control endpoint
	// is where membership changes happen — never on the data endpoint.
	dataSrv := server.NewServer(cf.clientAddr, server.NewClusterHandler(rn.CS), opts...)
	if err := dataSrv.Start(); err != nil {
		return fmt.Errorf("start client server on %s: %w", cf.clientAddr, err)
	}
	defer dataSrv.Stop()
	ctlSrv := server.NewServer(cf.controlAddr, server.NewControlHandler(rn.Node, rn.CS, rn.Store), opts...)
	if err := ctlSrv.Start(); err != nil {
		return fmt.Errorf("start control server on %s: %w", cf.controlAddr, err)
	}
	defer ctlSrv.Stop()

	fmt.Printf("cacheyd: node %s ready (raft %s, client %s, control %s, data %s)\n",
		rn.ID, rn.RaftAddr, cf.clientAddr, cf.controlAddr, cf.dataDir)

	// Serve until SIGINT/SIGTERM, then shut down cleanly (see defers above).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	return nil
}

// registerSelf waits until this node is the leader of its (single-node or
// recovered) cluster, then registers its own client and control addresses in
// the replicated registry.
func registerSelf(rn *server.RaftNode, cf *clusterFlags) error {
	return waitFor("this node to lead and register its addresses", 30*time.Second, func() error {
		if !rn.Node.IsLeader() {
			return errRetry
		}
		for key, addr := range map[string]string{
			server.MemberKey(rn.ID):  cf.clientAddr,
			server.ControlKey(rn.ID): cf.controlAddr,
		} {
			if err := rn.CS.Put(key, addr); err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					return errRetry
				}
				return err
			}
		}
		return nil
	})
}

// hasWALData reports whether dir already holds a raft log or snapshot (i.e.
// the node has run before and recovery will restore its membership).
func hasWALData(dir string) bool {
	for _, name := range []string{"wal.ndjson", "raft.snapshot"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// errRetry marks a condition that has not succeeded yet but is worth polling
// again within waitFor.
var errRetry = errors.New("retry")

// waitFor polls cond until it returns nil, cond returns a non-retry error, or
// timeout elapses.
func waitFor(what string, timeout time.Duration, cond func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return nil
		}
		if !errors.Is(err, errRetry) {
			return fmt.Errorf("%s: %w", what, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// joinCluster asks an existing member's CONTROL endpoint (contact) to add this
// node, following "not leader: <control addr>" redirects until the current
// leader acknowledges. Idempotent for an existing member: the leader then just
// refreshes this node's addresses.
func joinCluster(rn *server.RaftNode, cf *clusterFlags, contact string) error {
	payload, err := json.Marshal(server.JoinRequest{
		Raft:    rn.RaftAddr,
		Client:  cf.clientAddr,
		Control: cf.controlAddr,
	})
	if err != nil {
		return err
	}
	cmd := protocol.Command{Type: protocol.JOIN, Key: rn.ID, Val: string(payload)}
	return waitFor("joining cluster at "+contact, 120*time.Second, func() error {
		c, err := client.NewClient(contact)
		if err != nil {
			return errRetry // the member may still be starting
		}
		_, err = c.SendCommand(cmd)
		c.Close()
		if err == nil {
			return nil // acknowledged
		}
		if next, ok := client.RedirectLeader(err); ok {
			contact = next
			return errRetry
		}
		var st *protocol.Status
		switch {
		case errors.As(err, &st) && st.Code == protocol.CodeUnavailable:
			return errRetry // leader unknown yet (registry not ready)
		case errors.As(err, &st):
			return err // a definitive server verdict (e.g. not a cluster node)
		default:
			return errRetry // network-level failure: retry
		}
	})
}
