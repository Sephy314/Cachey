package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/mtls/testca"
	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/pkg/client"
)

// This file is the cluster-internal mTLS E2E: every plane of a raft cluster is
// run over mutual TLS with a single per-node certificate whose DNS SAN is the
// node id —
//
//	node ↔ node raft transport: peers pin each other's node-id certificates
//	client ↔ server data plane: a cache client (allow-listed, CA-signed) talks
//	                            to a node's data endpoint, pinning the node id
//	control plane (JOIN):      a joining node authenticates with its own node
//	                            certificate against the leader's control endpoint
//
// Replication only succeeds if the node-to-node raft transport admitted the
// dynamically-joined member over TLS (the pre-membership admission rule), so
// the test proves node mTLS end to end.

func newTLSNode(t *testing.T, ca *testca.CA, id string, certPEM, keyPEM []byte, allowClient func(string) bool) *testCtlNode {
	t.Helper()
	rn, err := OpenRaftNode(RaftNodeConfig{
		ID:                id,
		Dir:               t.TempDir(),
		RaftAddr:          "127.0.0.1:0",
		HeartbeatInterval: 100 * time.Millisecond,
		ElectionTimeout:   600 * time.Millisecond,
		TLSCA:             ca.CertPEM(),
		TLSCert:           certPEM,
		TLSKey:            keyPEM,
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
	dcfg, err := mtls.Server(ca.CertPEM(), certPEM, keyPEM, allowClient)
	if err != nil {
		t.Fatal(err)
	}
	ccfg, err := mtls.Server(ca.CertPEM(), certPEM, keyPEM, func(identity string) bool {
		return mtls.ValidName(identity) // node role: any CA-signed node cert
	})
	if err != nil {
		t.Fatal(err)
	}
	data := NewServer("127.0.0.1:0", NewClusterHandler(rn.CS), WithTLSConfig(dcfg))
	if err := data.Start(); err != nil {
		t.Fatalf("data server %s: %v", id, err)
	}
	ctl := NewServer("127.0.0.1:0", NewControlHandler(rn.Node, rn.CS, rn.Store), WithTLSConfig(ccfg))
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

// tlsCmd sends cmd over mutual TLS to addr, authenticating as certPEM/keyPEM
// and pinning the expected server identity serverName.
func tlsCmd(t *testing.T, ca *testca.CA, certPEM, keyPEM []byte, addr, serverName string, cmd protocol.Command) (*string, error) {
	t.Helper()
	cfg, err := mtls.Client(ca.CertPEM(), certPEM, keyPEM, serverName)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.NewClient(addr, client.WithTLSConfig(cfg))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.SendCommand(cmd)
}

// registerSelfTLS makes node the leader of its fresh 1-node cluster and writes
// its client + control addresses into the replicated registry.
func (c *testCtlNode) registerSelfTLS(t *testing.T) {
	t.Helper()
	c.rn.Node.Run()
	c.waitT(t, c.id+" to lead", 30*time.Second, func() bool { return c.rn.Node.IsLeader() })
	c.waitT(t, c.id+" to register its addresses", 30*time.Second, func() bool {
		return c.put(MemberKey(c.id), c.clientAddr) == nil &&
			c.put(ControlKey(c.id), c.controlAddr) == nil
	})
}

// joinTLS authenticates as this node's own certificate and asks the contact
// node's CONTROL endpoint to add it, retrying until acknowledged.
func (c *testCtlNode) joinTLS(t *testing.T, ca *testca.CA, certPEM, keyPEM []byte, contact *testCtlNode) {
	t.Helper()
	payload, err := json.Marshal(JoinRequest{
		Raft:    c.rn.RaftAddr,
		Client:  c.clientAddr,
		Control: c.controlAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd := protocol.Command{Type: protocol.JOIN, Key: c.id, Val: string(payload)}
	c.waitT(t, c.id+" to join over mTLS", 60*time.Second, func() bool {
		_, err := tlsCmd(t, ca, certPEM, keyPEM, contact.controlAddr, contact.id, cmd)
		return err == nil
	})
}

func (c *testCtlNode) waitT(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// TestClusterMTLSEndToEnd: a 2-node raft cluster where node↔node raft, the
// data plane and the control plane all run over mutual TLS.
func TestClusterMTLSEndToEnd(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	mint := func(id string) ([]byte, []byte) {
		cert, key, err := ca.Issue(id)
		if err != nil {
			t.Fatalf("issue %s: %v", id, err)
		}
		return cert, key
	}
	certA, keyA := mint("a")
	certB, keyB := mint("b")
	certAlice, keyAlice := mint("alice")
	certBob, keyBob := mint("bob")

	allow := func(id string) bool { return id == "alice" }

	// Node a: bootstrap a fresh TLS cluster. Node b: a fresh node listening on
	// the raft transport (TLS) but not yet a member and not running.
	A := newTLSNode(t, ca, "a", certA, keyA, allow)
	A.registerSelfTLS(t)
	B := newTLSNode(t, ca, "b", certB, keyB, allow)

	// b joins through a's CONTROL endpoint, authenticating with its own node
	// certificate and pinning a's identity.
	B.joinTLS(t, ca, certB, keyB, A)
	B.rn.Node.Run()
	B.waitT(t, "b to be caught up with its registered addresses", 60*time.Second, func() bool {
		if v, err := B.rn.Store.Get(MemberKey("b")); err != nil || *v != B.clientAddr {
			return false
		}
		if v, err := B.rn.Store.Get(ControlKey("b")); err != nil || *v != B.controlAddr {
			return false
		}
		return len(B.rn.Node.Voters()) == 2
	})

	// A cache client (alice, allow-listed) writes over client↔server mTLS.
	if _, err := tlsCmd(t, ca, certAlice, keyAlice, A.clientAddr, "a",
		protocol.Command{Type: protocol.PUT, Key: "user", Val: "alice"}); err != nil {
		t.Fatalf("alice PUT over mTLS: %v", err)
	}
	// Replication over node↔node mTLS: the write must reach b's FSM.
	B.waitT(t, "write to replicate to b over node mTLS", 30*time.Second, func() bool {
		if v, err := B.rn.Store.Get("user"); err == nil && *v == "alice" {
			return true
		}
		return false
	})
	// And read back through the leader's data endpoint.
	resp, err := tlsCmd(t, ca, certAlice, keyAlice, A.clientAddr, "a",
		protocol.Command{Type: protocol.GET, Key: "user"})
	if err != nil || resp == nil || !strings.Contains(*resp, `"alice"`) {
		t.Fatalf("alice GET over mTLS = %v %q", err, derefOr(resp))
	}

	// bob is CA-signed but not on the data allowlist: writes must fail.
	if _, err := tlsCmd(t, ca, certBob, keyBob, A.clientAddr, "a",
		protocol.Command{Type: protocol.PUT, Key: "nope", Val: "x"}); err == nil {
		t.Fatal("bob (off allowlist) must not be able to write over mTLS")
	}

	// JOIN stays off the data endpoint even over TLS.
	if _, err := tlsCmd(t, ca, certB, keyB, A.clientAddr, "a",
		protocol.Command{Type: protocol.JOIN, Key: "x", Val: "{}"}); err == nil {
		t.Fatal("JOIN on the data endpoint must be rejected over mTLS")
	}
}
