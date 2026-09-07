package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/pkg/client"
)

// This file pins the control-plane separation (Option B): a raft-cluster node
// serves JOIN — the only membership-changing operation — on the node-to-node
// raft transport (raft.TCPTransport control channel), and the client-facing
// DATA endpoint rejects JOIN and any access to the reserved membership
// registry. All through the real wire handlers (NewClusterHandler /
// NewControlHandler) and transport.

// testCtlNode is a raft-cluster node with its raft transport (control plane
// registered) and its client-facing data server listening.
type testCtlNode struct {
	id         string
	rn         *RaftNode
	clientAddr string // data plane
}

// ctlPeer identifies a member's raft-transport endpoint for a JOIN.
type ctlPeer struct {
	id   string
	addr string
}

func newTestCtlNode(t *testing.T, id string) *testCtlNode {
	t.Helper()
	rn, err := OpenRaftNode(RaftNodeConfig{
		ID:                id,
		Dir:               t.TempDir(),
		RaftAddr:          "127.0.0.1:0",
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   600 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenRaftNode(%s): %v", id, err)
	}
	rn.CS.SetLeaderResolver(func(leaderID string) string {
		if leaderID == "" {
			return ""
		}
		if v, err := rn.Store.Get(MemberKey(leaderID)); err == nil {
			return *v
		}
		return ""
	})
	rn.Tr.SetControlHandler(NewControlHandler(rn).HandleControl)
	data := NewServer("127.0.0.1:0", NewClusterHandler(rn.CS))
	if err := data.Start(); err != nil {
		t.Fatalf("data server %s: %v", id, err)
	}
	t.Cleanup(func() {
		data.Stop()
		rn.Stop()
	})
	return &testCtlNode{id: id, rn: rn, clientAddr: data.Addr()}
}

// bootstrap makes node the leader of a fresh 1-node cluster and registers its
// own client address in the replicated registry.
func (c *testCtlNode) bootstrap(t *testing.T) {
	t.Helper()
	c.rn.Node.Run()
	c.waitCond(t, c.id+" to lead", 30*time.Second, func() bool { return c.rn.Node.IsLeader() })
	c.waitCond(t, c.id+" to register its client address", 30*time.Second, func() bool {
		return c.put(MemberKey(c.id), c.clientAddr) == nil
	})
}

// put writes a registry key through this node's replicated store, tolerating a
// transient not-leader (the leader can change between checks).
func (c *testCtlNode) put(key, val string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := c.rn.CS.Put(key, val)
		if err == nil {
			return nil
		}
		if !errors.Is(err, raft.ErrNotLeader) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *testCtlNode) waitCond(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// joinReply is the JSON shape of a JOIN control reply: either an ack {"ok":
// id} or a redirect {"leader": id, "addr": raft-addr}.
type joinReply struct {
	OK     string `json:"ok"`
	Leader string `json:"leader"`
	Addr   string `json:"addr"`
}

// sendJoin sends a raw JOIN control message from this node's transport to
// peer and returns the parsed reply (an ack or a redirect). peer must already
// be reachable (its address registered in this node's transport).
func (c *testCtlNode) sendJoin(peer string, req JoinRequest) (*joinReply, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	raw, err := c.rn.Tr.SendControl(ctx, peer, payload)
	cancel()
	if err != nil {
		return nil, err
	}
	var res joinReply
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// joinTransport drives the JOIN control message over the raft transport:
// contact a member, follow "leader/addr" redirects until the leader acks.
func (c *testCtlNode) joinTransport(t *testing.T, contact ctlPeer) {
	t.Helper()
	req := JoinRequest{ID: c.id, Raft: c.rn.RaftAddr, Client: c.clientAddr}
	target := contact
	c.waitCond(t, c.id+" to join over the raft transport", 60*time.Second, func() bool {
		c.rn.Tr.RegisterPeer(target.id, target.addr)
		res, err := c.sendJoin(target.id, req)
		if err != nil {
			return false
		}
		if res.OK != "" {
			return true
		}
		if res.Leader != "" && res.Addr != "" {
			target = ctlPeer{id: res.Leader, addr: res.Addr}
		}
		return false
	})
}

// sendCommand sends one command over plaintext to a data endpoint and returns
// the error (nil on a command echo).
func sendCommand(addr string, cmd protocol.Command) error {
	cl, err := client.NewClient(addr)
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = cl.SendCommand(cmd)
	return err
}

func derefOr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// TestClusterControlPlaneSeparation: JOIN works only over the raft transport
// (the control plane); the data endpoint rejects JOIN and the reserved
// registry keys; a non-leader control endpoint redirects to the leader's raft
// endpoint.
func TestClusterControlPlaneSeparation(t *testing.T) {
	A := newTestCtlNode(t, "A")
	A.bootstrap(t)

	// (1) The DATA endpoint never serves JOIN — a cache client cannot change
	// membership.
	err := sendCommand(A.clientAddr, protocol.Command{Type: protocol.JOIN, Key: "intruder", Val: "{}"})
	var st *protocol.Status
	if !errors.As(err, &st) || st.Code != protocol.CodeInvalidArgument {
		t.Fatalf("JOIN on data endpoint = %v, want InvalidArgument", err)
	}

	// (2) Reserved registry keys are not reachable through the data endpoint.
	for _, cmd := range []protocol.Command{
		{Type: protocol.GET, Key: MemberKey("A")},
		{Type: protocol.PUT, Key: MemberKey("evil"), Val: "x"},
		{Type: protocol.GET, Key: MemberKey("evil")},
	} {
		if err := sendCommand(A.clientAddr, cmd); !errors.As(err, &st) || st.Code != protocol.CodeInvalidArgument {
			t.Fatalf("%s %s on data endpoint = %v, want InvalidArgument", cmd.Type, cmd.Key, err)
		}
	}

	// (3) A JOIN over the raft transport (control plane) adds a real member.
	B := newTestCtlNode(t, "B") // raft listening, not yet a member, not running
	B.joinTransport(t, ctlPeer{id: "A", addr: A.rn.RaftAddr})
	B.rn.Node.Run()
	B.waitCond(t, "B to be caught up with its registered client address", 60*time.Second, func() bool {
		v, err := B.rn.Store.Get(MemberKey("B"))
		if err != nil || *v != B.clientAddr {
			return false
		}
		return len(B.rn.Node.Voters()) == 2
	})
	// The leader's FSM also learned B's client address (registry replicated).
	if v, err := A.rn.Store.Get(MemberKey("B")); err != nil || *v != B.clientAddr {
		t.Fatalf("leader does not know B's client address: %v %q", err, derefOr(v))
	}

	// (4) A non-leader's control endpoint redirects to the leader's raft
	// endpoint, so the joiner retries against the actual leader.
	B.waitCond(t, "B to learn that A is the leader", 30*time.Second, func() bool {
		return B.rn.Node.Leader() == "A"
	})
	J := newTestCtlNode(t, "J") // fresh joiner, not running, not a member
	J.rn.Tr.RegisterPeer("B", B.rn.RaftAddr)
	res, err := J.sendJoin("B", JoinRequest{ID: "J", Raft: "127.0.0.1:1", Client: "c"})
	if err != nil {
		t.Fatalf("JOIN to follower = %v, want a redirect reply", err)
	}
	if res.Leader != "A" || res.Addr != A.rn.RaftAddr {
		t.Fatalf("follower redirect = {%q %q}, want {%q %q}", res.Leader, res.Addr, "A", A.rn.RaftAddr)
	}
	// Following the redirect to the leader completes the JOIN.
	J.rn.Tr.RegisterPeer(res.Leader, res.Addr)
	res, err = J.sendJoin("A", JoinRequest{ID: "J", Raft: "127.0.0.1:1", Client: "c"})
	if err != nil {
		t.Fatalf("JOIN to leader after redirect = %v, want an ack", err)
	}
	if res.OK != "J" {
		t.Fatalf("leader ack = %q, want J", res.OK)
	}
}
