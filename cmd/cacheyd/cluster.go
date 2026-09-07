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

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/server"
	"github.com/Sephy314/Cachey/pkg/client"
)

// memberPrefix keys the replicated registry that maps a node id to its
// client-facing address. The leader writes member/<id> when a node bootstraps
// or joins, so every node's FSM keeps the full table and can answer "not
// leader: <addr>" redirects for clients. The prefix is reserved: the cluster
// handler rejects ordinary client commands that touch it.
//
// ponytail: the member table lives in the data namespace (reserved keys)
// rather than a separate metadata log; a store snapshot therefore captures it
// with the data. A client that knows the prefix could still read it over the
// raw protocol if it bypasses this handler — acceptable for a cache cluster.
const memberPrefix = "member/"

const (
	raftHeartbeat = 100 * time.Millisecond
	raftElection  = 600 * time.Millisecond // randomized 600-1200ms, see raft tests
)

// clusterFlags holds the -consensus raft command-line options.
type clusterFlags struct {
	nodeID     string
	clientAddr string
	raftAddr   string
	dataDir    string
	bootstrap  bool
	join       string
}

// runRaftCluster opens a persistent raft node, attaches a client-facing server
// over it, and either bootstraps a fresh cluster or joins an existing one,
// then serves until SIGINT/SIGTERM.
func runRaftCluster(cf *clusterFlags, opts []server.Option) error {
	if cf.nodeID == "" || cf.clientAddr == "" || cf.raftAddr == "" || cf.dataDir == "" {
		return fmt.Errorf("-consensus raft requires -node-id, -client-addr, -raft-addr and -data-dir")
	}
	if cf.bootstrap && cf.join != "" {
		return errors.New("use either -bootstrap or -join, not both")
	}
	if !cf.bootstrap && cf.join == "" {
		return errors.New("-consensus raft needs -bootstrap (first node) or -join <member-client-addr>")
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

	// Redirects: resolve a leader's node id to its client address from the
	// replicated member table every node keeps in its FSM.
	rn.CS.SetLeaderResolver(func(id string) string {
		if id == "" {
			return ""
		}
		v, err := rn.Store.Get(memberPrefix + id)
		if err != nil {
			return ""
		}
		return *v
	})

	hdl := &clusterHandler{
		node: rn.Node,
		cs:   rn.CS,
		base: server.NewCacheyHandler(rn.CS),
	}
	srv := server.NewServer(cf.clientAddr, hdl, opts...)
	if err := srv.Start(); err != nil {
		return fmt.Errorf("start client server on %s: %w", cf.clientAddr, err)
	}
	defer srv.Stop()

	if cf.bootstrap {
		// First node of a new cluster (or a member restarting from an existing
		// data dir — recovery restores membership; -bootstrap means "go live
		// and register my own client address").
		rn.Node.Run()
		if err := waitFor("this node to lead and register its client address", 30*time.Second, func() error {
			if !rn.Node.IsLeader() {
				return errRetry
			}
			if err := rn.CS.Put(memberPrefix+rn.ID, cf.clientAddr); err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					return errRetry
				}
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	} else if hasWALData(cf.dataDir) {
		// Restarting an existing member: recovery restores membership, so just
		// go live; -join re-announces the client address through the leader.
		rn.Node.Run()
		if err := joinCluster(rn, cf.clientAddr, cf.join); err != nil {
			return err
		}
	} else {
		// A brand-new node joining an existing cluster must be added by the
		// leader (raft.AddServer) BEFORE it runs, so it never briefly
		// self-elects as a 1-node cluster.
		if err := joinCluster(rn, cf.clientAddr, cf.join); err != nil {
			return err
		}
		rn.Node.Run()
		if err := waitFor("the cluster to catch this node up", 60*time.Second, func() error {
			v, err := rn.Store.Get(memberPrefix + rn.ID)
			if err != nil || *v != cf.clientAddr {
				return errRetry
			}
			return nil
		}); err != nil {
			return err
		}
	}

	fmt.Printf("cacheyd: node %s ready (raft %s, client %s, data %s)\n",
		rn.ID, rn.RaftAddr, cf.clientAddr, cf.dataDir)

	// Serve until SIGINT/SIGTERM, then shut down cleanly (see defers above).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	return nil
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

// clusterHandler is the client-facing handler of a raft-cluster node. It
// intercepts the JOIN control command (used to add nodes) and guards the
// reserved member/ registry keys, delegating every ordinary cache command to
// CacheyHandler over the replicated ClusterStore.
type clusterHandler struct {
	node *raft.Node
	cs   *server.ClusterStore
	base *server.CacheyHandler
}

func (h *clusterHandler) HandleRequest(data []byte) ([]byte, error) {
	cmd, err := protocol.DeSerializeCommand(data)
	if err != nil {
		return nil, err
	}
	if cmd.Type == protocol.JOIN {
		return h.handleJoin(cmd)
	}
	switch cmd.Type {
	case protocol.GET, protocol.PUT, protocol.DEL, protocol.TTL:
		if strings.HasPrefix(cmd.Key, memberPrefix) {
			return nil, protocol.Statusf(protocol.CodeInvalidArgument, "key %q is reserved", cmd.Key)
		}
	}
	// Ordinary cache command: let CacheyHandler decode and dispatch.
	return h.base.HandleRequest(data)
}

// joinPayload is the body of a JOIN request: the joining node's raft RPC
// address (so the leader can replicate to it) and its client address (so the
// cluster can redirect clients to it).
type joinPayload struct {
	Raft   string `json:"raft"`
	Client string `json:"client"`
}

// handleJoin runs on the node that received a JOIN. Only the raft leader may
// add the newcomer: it calls raft.AddServer (idempotent for an existing
// member, which just re-announces) and writes the member's client address into
// the replicated registry. A follower answers with a leader redirect so the
// joining process retries against the actual leader.
func (h *clusterHandler) handleJoin(cmd *protocol.Command) ([]byte, error) {
	var p joinPayload
	if cmd.Key == "" || json.Unmarshal([]byte(cmd.Val), &p) != nil || p.Raft == "" || p.Client == "" {
		return nil, protocol.Statusf(protocol.CodeInvalidArgument, "malformed join request")
	}
	if !h.node.IsLeader() {
		return nil, h.redirect()
	}
	already := false
	for _, id := range h.node.Voters() {
		if id == cmd.Key {
			already = true
			break
		}
	}
	if !already {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := h.node.AddServer(ctx, cmd.Key, p.Raft)
		cancel()
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				return nil, h.redirect()
			}
			return nil, protocol.Statusf(protocol.CodeInternal, "add %s: %v", cmd.Key, err)
		}
	}
	if err := h.cs.Put(memberPrefix+cmd.Key, p.Client); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, h.redirect()
		}
		return nil, protocol.Statusf(protocol.CodeInternal, "register %s: %v", cmd.Key, err)
	}
	// Echo the JOIN back as the acknowledgement.
	return cmd.Serialize()
}

// redirect builds the leader-redirect status this node answers with when it is
// not the leader. The hint carries the leader's client address when known.
func (h *clusterHandler) redirect() error {
	if addr := h.cs.Leader(); addr != "" {
		return protocol.Statusf(protocol.CodeUnavailable, "not leader: %s", addr)
	}
	return protocol.Statusf(protocol.CodeUnavailable, "not leader")
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

// joinCluster asks an existing member (contact's client address) to add this
// node to the cluster, following "not leader: <addr>" redirects until the
// current leader acknowledges. Idempotent for a member that already exists:
// the leader then just refreshes its client address.
func joinCluster(rn *server.RaftNode, selfClientAddr, contact string) error {
	payload, err := json.Marshal(joinPayload{Raft: rn.RaftAddr, Client: selfClientAddr})
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
			return errRetry // leader unknown yet (member table not ready)
		case errors.As(err, &st):
			return err // a definitive server verdict (e.g. not a cluster node)
		default:
			return errRetry // network-level failure: retry
		}
	})
}
