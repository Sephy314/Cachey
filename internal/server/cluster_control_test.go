package server

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/pkg/client"
)

// This file pins the control-plane separation (Option A): a raft-cluster node
// serves JOIN — the only membership-changing operation — on a dedicated
// CONTROL endpoint, and the client-facing DATA endpoint rejects JOIN and any
// access to the reserved membership registry. All through the real wire
// handlers (NewClusterHandler / NewControlHandler) and servers.

// testCtlNode is a raft-cluster node with both planes listening.
type testCtlNode struct {
	id          string
	rn          *RaftNode
	clientAddr  string // data plane
	controlAddr string // control plane (JOIN)
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
	data := NewServer("127.0.0.1:0", NewClusterHandler(rn.CS))
	if err := data.Start(); err != nil {
		t.Fatalf("data server %s: %v", id, err)
	}
	ctl := NewServer("127.0.0.1:0", NewControlHandler(rn.Node, rn.CS, rn.Store))
	if err := ctl.Start(); err != nil {
		t.Fatalf("control server %s: %v", id, err)
	}
	t.Cleanup(func() {
		data.Stop()
		ctl.Stop()
		rn.Stop()
	})
	return &testCtlNode{id: id, rn: rn, clientAddr: data.Addr(), controlAddr: ctl.Addr()}
}

// bootstrap makes node the leader of a fresh 1-node cluster and registers its
// own client + control addresses.
func (c *testCtlNode) bootstrap(t *testing.T) {
	t.Helper()
	c.rn.Node.Run()
	c.waitCond(t, c.id+" to lead", 30*time.Second, func() bool { return c.rn.Node.IsLeader() })
	c.waitCond(t, c.id+" to register its addresses", 30*time.Second, func() bool {
		return c.put(MemberKey(c.id), c.clientAddr) == nil &&
			c.put(ControlKey(c.id), c.controlAddr) == nil
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

// joinCtl drives the JOIN wire protocol against a member's CONTROL endpoint,
// following "not leader: <control addr>" redirects until a leader acks.
func (c *testCtlNode) joinCtl(t *testing.T, contactControl string) {
	t.Helper()
	payload, err := json.Marshal(JoinRequest{
		Raft:    c.rn.RaftAddr,
		Client:  c.clientAddr,
		Control: c.controlAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := protocol.Command{Type: protocol.JOIN, Key: c.rn.ID, Val: string(payload)}
	target := contactControl
	c.waitCond(t, c.id+" to join", 60*time.Second, func() bool {
		cl, err := client.NewClient(target)
		if err != nil {
			return false
		}
		_, err = cl.SendCommand(cmd)
		cl.Close()
		if err == nil {
			return true
		}
		if next, ok := client.RedirectLeader(err); ok {
			target = next
		}
		return false
	})
}

// sendCommand sends one command over plaintext and returns the error (nil on a
// command echo).
func sendCommand(addr string, cmd protocol.Command) error {
	cl, err := client.NewClient(addr)
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = cl.SendCommand(cmd)
	return err
}

// TestClusterControlPlaneSeparation: JOIN works only on the control endpoint;
// the data endpoint rejects JOIN and the reserved registry keys; a non-leader
// control endpoint redirects to the leader's CONTROL address.
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
		{Type: protocol.GET, Key: ControlKey("A")},
	} {
		if err := sendCommand(A.clientAddr, cmd); !errors.As(err, &st) || st.Code != protocol.CodeInvalidArgument {
			t.Fatalf("%s %s on data endpoint = %v, want InvalidArgument", cmd.Type, cmd.Key, err)
		}
	}

	// (3) A JOIN on the CONTROL endpoint adds a real member (via the wire).
	B := newTestCtlNode(t, "B") // raft listening, not yet a member, not running
	B.joinCtl(t, A.controlAddr)
	B.rn.Node.Run()
	B.waitCond(t, "B to be caught up with its registered addresses", 60*time.Second, func() bool {
		v, err := B.rn.Store.Get(MemberKey("B"))
		if err != nil || *v != B.clientAddr {
			return false
		}
		v, err = B.rn.Store.Get(ControlKey("B"))
		if err != nil || *v != B.controlAddr {
			return false
		}
		return len(B.rn.Node.Voters()) == 2
	})
	// The leader's FSM also learned B's control address (registry replicated).
	if v, err := A.rn.Store.Get(ControlKey("B")); err != nil || *v != B.controlAddr {
		t.Fatalf("leader does not know B's control address: %v %q", err, derefOr(v))
	}

	// (4) A non-leader's control endpoint redirects to the leader's CONTROL
	// address, so the joiner retries against the actual leader.
	B.waitCond(t, "B to learn that A is the leader", 30*time.Second, func() bool {
		return B.rn.Node.Leader() == "A"
	})
	third := JoinRequest{Raft: "127.0.0.1:1", Client: "c", Control: "ctl"}
	payload, _ := json.Marshal(third)
	cerr := sendCommand(B.controlAddr, protocol.Command{Type: protocol.JOIN, Key: "C", Val: string(payload)})
	next, ok := client.RedirectLeader(cerr)
	if !ok || next != A.controlAddr {
		t.Fatalf("follower control redirect = (%q, %v), want (%q, true)", next, ok, A.controlAddr)
	}
}

func derefOr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
