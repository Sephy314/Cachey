package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/server"
)

const (
	raftHeartbeat = 100 * time.Millisecond
	raftElection  = 600 * time.Millisecond // randomized 600-1200ms, see raft tests
)

// clusterFlags holds the -consensus raft command-line options.
type clusterFlags struct {
	nodeID     string
	clientAddr string // client-facing (data plane)
	raftAddr   string
	dataDir    string
	bootstrap  bool
	join       string // an existing member, "nodeID@raft-addr", to join through
}

// joinTarget splits a "-join" value of the form "nodeID@raftAddr".
type joinTarget struct {
	id   string
	addr string
}

func parseJoinTarget(s string) (joinTarget, error) {
	id, addr, ok := strings.Cut(s, "@")
	if !ok || id == "" || addr == "" {
		return joinTarget{}, fmt.Errorf("invalid -join %q (want nodeID@raft-addr)", s)
	}
	return joinTarget{id: id, addr: addr}, nil
}

// runRaftCluster opens a persistent raft node and either bootstraps a fresh
// cluster or joins an existing one over the raft transport (the control plane
// rides node-to-node, authenticated by node mTLS when enabled), then serves
// cache clients until SIGINT/SIGTERM.
func runRaftCluster(cf *clusterFlags, tls *resolvedTLS) error {
	if cf.nodeID == "" || cf.clientAddr == "" || cf.raftAddr == "" || cf.dataDir == "" {
		return fmt.Errorf("-consensus raft requires -node-id, -client-addr, -raft-addr and -data-dir")
	}
	if cf.bootstrap && cf.join != "" {
		return errors.New("use either -bootstrap or -join, not both")
	}
	if !cf.bootstrap && cf.join == "" {
		return errors.New("-consensus raft needs -bootstrap (first node) or -join <nodeID@raft-addr>")
	}

	// Node-to-node mTLS on the raft transport when certificates are given: the
	// node's certificate SAN is its id, so peers authenticate each other.
	var tlsCA, tlsCert, tlsKey []byte
	if !tls.insecure {
		tlsCA, tlsCert, tlsKey = tls.ca, tls.cert, tls.key
	}
	rn, err := server.OpenRaftNode(server.RaftNodeConfig{
		ID:                cf.nodeID,
		Dir:               cf.dataDir,
		RaftAddr:          cf.raftAddr,
		HeartbeatInterval: raftHeartbeat,
		ElectionTimeout:   raftElection,
		TLSCA:             tlsCA,
		TLSCert:           tlsCert,
		TLSKey:            tlsKey,
	})
	if err != nil {
		return fmt.Errorf("open raft node %s: %w", cf.nodeID, err)
	}
	defer rn.Stop()
	rn.Store.StartActiveExpiration(1 * time.Second)

	// Cluster control (JOIN) rides the raft transport: the leader adds members
	// and, under mTLS, binds the joining node's id to its certificate identity.
	rn.Tr.SetControlHandler(server.NewControlHandler(rn).HandleControl)

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
		// and register my own client address").
		rn.Node.Run()
		if err := registerSelf(rn, cf); err != nil {
			return err
		}
	case hasWALData(cf.dataDir):
		// Restarting an existing member: recovery restores membership, so just
		// go live; -join re-announces this node's client address through the
		// leader (idempotent for an existing member).
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
			return nil
		}); err != nil {
			return err
		}
	}

	// Only once this node is a member, start the client-facing DATA server.
	dataSrv := server.NewServer(cf.clientAddr, server.NewClusterHandler(rn.CS), dataServerOpts(tls)...)
	if err := dataSrv.Start(); err != nil {
		return fmt.Errorf("start client server on %s: %w", cf.clientAddr, err)
	}
	defer dataSrv.Stop()

	fmt.Printf("cacheyd: node %s ready (raft %s, client %s, data %s)\n",
		rn.ID, rn.RaftAddr, cf.clientAddr, cf.dataDir)

	// Serve until SIGINT/SIGTERM, then shut down cleanly (see defers above).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	return nil
}

// registerSelf waits until this node is the leader of its (single-node or
// recovered) cluster, then registers its own client address in the replicated
// member registry.
func registerSelf(rn *server.RaftNode, cf *clusterFlags) error {
	return waitFor("this node to lead and register its client address", 30*time.Second, func() error {
		if !rn.Node.IsLeader() {
			return errRetry
		}
		if err := rn.CS.Put(server.MemberKey(rn.ID), cf.clientAddr); err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				return errRetry
			}
			return err
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

// joinCluster sends a JOIN over the raft transport to the -join contact,
// following "leader/addr" redirects until the current leader acknowledges.
// Under mTLS the joiner authenticates as its own node (certificate identity);
// the leader binds that identity to the requested node id. Idempotent for an
// existing member (restart re-announce): the leader just refreshes its client
// address.
func joinCluster(rn *server.RaftNode, cf *clusterFlags, contact string) error {
	start, err := parseJoinTarget(contact)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(server.JoinRequest{
		ID:     rn.ID,
		Raft:   rn.RaftAddr,
		Client: cf.clientAddr,
	})
	if err != nil {
		return err
	}
	target := start
	return waitFor("joining cluster through "+start.id+"@"+start.addr, 120*time.Second, func() error {
		rn.Tr.RegisterPeer(target.id, target.addr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		reply, err := rn.Tr.SendControl(ctx, target.id, payload)
		cancel()
		if err != nil {
			return errRetry // transport/network: the member may still be starting
		}
		var res struct {
			OK     string `json:"ok"`
			Leader string `json:"leader"`
			Addr   string `json:"addr"`
		}
		if err := json.Unmarshal(reply, &res); err != nil {
			return err // definitive malformed reply
		}
		if res.OK != "" {
			return nil // acknowledged by the leader
		}
		if res.Leader != "" && res.Addr != "" {
			target = joinTarget{id: res.Leader, addr: res.Addr}
			return errRetry
		}
		return errRetry // no leader known yet
	})
}
