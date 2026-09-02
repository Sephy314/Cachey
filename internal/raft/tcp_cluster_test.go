package raft

import (
	"testing"
	"time"
)

// startTCPCluster starts a real multi-node cluster communicating over TCP with
// newline-delimited JSON framing.
func startTCPCluster(t *testing.T, ids []string, opts clusterOpts) (map[string]*Node, map[string]*fsm, map[string]*TCPTransport) {
	t.Helper()
	if opts.heartbeat == 0 {
		opts.heartbeat = 20 * time.Millisecond
	}
	if opts.election == 0 {
		opts.election = 100 * time.Millisecond
	}

	transports := make(map[string]*TCPTransport)
	for _, id := range ids {
		transports[id] = NewTCPTransport(nil)
	}
	// Bind all listeners first so we know every peer's address.
	addrs := make(map[string]string)
	for _, id := range ids {
		addr, err := transports[id].Listen("127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		addrs[id] = addr
	}

	nodes := make(map[string]*Node)
	fsms := make(map[string]*fsm)
	peerAddrsOf := func(id string) map[string]string {
		m := make(map[string]string)
		for other := range addrs {
			if other != id {
				m[other] = addrs[other]
			}
		}
		return m
	}
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		f := &fsm{}
		cfg := Config{
			ID:                id,
			Peers:             peers,
			HeartbeatInterval: opts.heartbeat,
			ElectionTimeout:   opts.election,
		}
		tr := transports[id]
		n, err := NewNode(cfg, tr, f.apply)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		tr.node = n
		tr.SetPeers(peerAddrsOf(id))
		nodes[id] = n
		fsms[id] = f
	}
	for _, id := range ids {
		nodes[id].Run()
	}
	return nodes, fsms, transports
}

func stopTCPCluster(nodes map[string]*Node, transports map[string]*TCPTransport) {
	for _, n := range nodes {
		n.Stop()
	}
	for _, tr := range transports {
		tr.Close()
	}
}

func TestTCPClusterLeaderAndReplication(t *testing.T) {
	nodes, fsms, transports := startTCPCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopTCPCluster(nodes, transports)

	leader := waitLeader(t, nodes)
	cmds := []string{"k1=v1", "k2=v2"}
	for _, cmd := range cmds {
		propose(t, nodes[leader], cmd)
	}
	waitFor(t, "all nodes apply all commands over TCP", 5*time.Second, func() bool {
		for _, f := range fsms {
			if !equalStrings(f.snapshot(), cmds) {
				return false
			}
		}
		return true
	})
}

func TestTCPClusterFailover(t *testing.T) {
	nodes, fsms, transports := startTCPCluster(t, []string{"a", "b", "c"}, clusterOpts{})
	defer stopTCPCluster(nodes, transports)

	old := waitLeader(t, nodes)
	propose(t, nodes[old], "before-failover")

	// Kill the leader and its listener.
	nodes[old].Stop()
	transports[old].Close()
	delete(nodes, old)
	delete(fsms, old)
	delete(transports, old)

	// Remaining two elect a new leader over TCP.
	waitFor(t, "a new leader after TCP failover", 5*time.Second, func() bool {
		leaders := 0
		for _, n := range nodes {
			if n.IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	})
	var newLeader *Node
	for _, n := range nodes {
		if n.IsLeader() {
			newLeader = n
			break
		}
	}
	if newLeader == nil {
		t.Fatal("no new leader elected")
	}

	propose(t, newLeader, "after-failover")
	want := []string{"before-failover", "after-failover"}
	waitFor(t, "live nodes apply both commands", 5*time.Second, func() bool {
		for _, f := range fsms {
			if !equalStrings(f.snapshot(), want) {
				return false
			}
		}
		return true
	})
}
