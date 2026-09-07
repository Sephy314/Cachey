package server

import (
	"context"
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
//	                            and JOIN rides this transport (the control plane)
//	client ↔ server data plane: a cache client (allow-listed, CA-signed) talks
//	                            to a node's data endpoint, pinning the node id
//	JOIN identity binding:      the leader binds the sender's certificate
//	                            identity to the claimed node id — a node cannot
//	                            join as someone else
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
	rn.Tr.SetControlHandler(NewControlHandler(rn).HandleControl)
	dcfg, err := mtls.Server(ca.CertPEM(), certPEM, keyPEM, allowClient)
	if err != nil {
		t.Fatal(err)
	}
	data := NewServer("127.0.0.1:0", NewClusterHandler(rn.CS), WithTLSConfig(dcfg))
	if err := data.Start(); err != nil {
		t.Fatalf("data server %s: %v", id, err)
	}
	t.Cleanup(func() {
		data.Stop()
		rn.Stop()
	})
	return &testCtlNode{id: id, rn: rn, clientAddr: data.Addr()}
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
// its own client address into the replicated registry.
func (c *testCtlNode) registerSelfTLS(t *testing.T) {
	t.Helper()
	c.rn.Node.Run()
	c.waitCond(t, c.id+" to lead", 30*time.Second, func() bool { return c.rn.Node.IsLeader() })
	c.waitCond(t, c.id+" to register its client address", 30*time.Second, func() bool {
		return c.put(MemberKey(c.id), c.clientAddr) == nil
	})
}

// sendJoinRaw sends a JOIN request with arbitrary fields over the raft
// transport to peer, returning the raw error (nil if acknowledged). Used to
// exercise the identity binding on mis-claimed ids.
func (c *testCtlNode) sendJoinRaw(peer string, req JoinRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = c.rn.Tr.SendControl(ctx, peer, payload)
	return err
}

// TestClusterMTLSEndToEnd: a 2-node raft cluster where node↔node raft (with
// JOIN riding it), the data plane and identity binding all run over mutual
// TLS.
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

	// b joins through a's raft-transport control channel, authenticating with
	// its own node certificate and pinning a's identity.
	B.joinTransport(t, ctlPeer{id: "a", addr: A.rn.RaftAddr})
	B.rn.Node.Run()
	B.waitCond(t, "b to be caught up with its registered client address", 60*time.Second, func() bool {
		if v, err := B.rn.Store.Get(MemberKey("b")); err != nil || *v != B.clientAddr {
			return false
		}
		return len(B.rn.Node.Voters()) == 2
	})

	// Identity binding: b (certificate "b") may not join claiming to be "c" —
	// the leader must reject the mis-claimed id.
	B.rn.Tr.RegisterPeer("a", A.rn.RaftAddr)
	if err := B.sendJoinRaw("a", JoinRequest{ID: "c", Raft: "127.0.0.1:1", Client: "x"}); err == nil {
		t.Fatal("join claiming a different id (c) with certificate b must be rejected")
	}

	// A cache client (alice, allow-listed) writes over client↔server mTLS.
	if _, err := tlsCmd(t, ca, certAlice, keyAlice, A.clientAddr, "a",
		protocol.Command{Type: protocol.PUT, Key: "user", Val: "alice"}); err != nil {
		t.Fatalf("alice PUT over mTLS: %v", err)
	}
	// Replication over node↔node mTLS: the write must reach b's FSM.
	B.waitCond(t, "write to replicate to b over node mTLS", 30*time.Second, func() bool {
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
