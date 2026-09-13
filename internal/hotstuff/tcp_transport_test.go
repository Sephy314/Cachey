package hotstuff

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	gonet "net"
	"sync"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/mtls/testca"
)

// Transport-level suite for the TCP transport (HS-M5 + hardening): mTLS
// identity enforcement, transport faults (partition, peer restart, dead
// peers, rogue peers) and shutdown hygiene. Everything here runs over real
// sockets — the consensus logic has its own deterministic in-memory suites.

// tcpCluster is a set of real replicas on real TCP transports.
type tcpCluster struct {
	ids   []string
	nodes map[string]*Replica
	trs   map[string]*TCPTransport
	logs  map[string]*orderLog
}

// startTCPCluster boots len(ids) replicas over TCP (ids[0] is the view-0
// leader) with every validator's Ed25519 key fixed before any listener opens.
// When ca is non-nil the transports run mTLS with a per-node certificate.
func startTCPCluster(t *testing.T, ids []string, ca *testca.CA) *tcpCluster {
	t.Helper()
	c := &tcpCluster{
		ids:   ids,
		nodes: make(map[string]*Replica, len(ids)),
		trs:   make(map[string]*TCPTransport, len(ids)),
		logs:  make(map[string]*orderLog, len(ids)),
	}
	certs := make(map[string][]byte, len(ids))
	keys := make(map[string][]byte, len(ids))
	for _, id := range ids {
		if ca == nil {
			continue
		}
		cert, key, err := ca.Issue(id)
		if err != nil {
			t.Fatalf("issue %s: %v", id, err)
		}
		certs[id], keys[id] = cert, key
	}
	for _, id := range ids {
		log := &orderLog{}
		tr := NewTCPTransport(nil)
		r, err := NewReplica(Config{ID: id, Peers: peersExcept(ids, id), Leader: ids[0]}, tr, func(b Block) {
			if len(b.Cmd) > 0 {
				log.add(string(b.Cmd))
			}
		})
		if err != nil {
			t.Fatalf("NewReplica(%s): %v", id, err)
		}
		c.logs[id], c.nodes[id], c.trs[id] = log, r, tr
		tr.SetNode(r)
	}
	// Consensus identities are fixed before any connection: Hello only proves
	// possession of a configured key, never trust on first use.
	validatorKeys := make(map[string]ed25519.PublicKey, len(ids))
	for _, id := range ids {
		validatorKeys[id] = c.nodes[id].PublicKey()
	}
	for _, id := range ids {
		if err := c.trs[id].SetValidatorKeys(validatorKeys); err != nil {
			t.Fatalf("SetValidatorKeys(%s): %v", id, err)
		}
		if ca != nil {
			c.trs[id].EnableTLS(ca.CertPEM(), certs[id], keys[id])
		}
	}
	addrs := make(map[string]string, len(ids))
	for _, id := range ids {
		if _, err := c.trs[id].Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("Listen(%s): %v", id, err)
		}
		addrs[id] = c.trs[id].Addr()
	}
	for _, id := range ids {
		c.trs[id].SetPeers(addrs)
	}
	for _, id := range ids {
		c.trs[id].ConnectPeers(time.Now().Add(10 * time.Second))
	}
	t.Cleanup(c.stop)
	return c
}

func peersExcept(ids []string, self string) []string {
	var out []string
	for _, id := range ids {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func (c *tcpCluster) stop() {
	for _, tr := range c.trs {
		tr.Close()
	}
}

// waitApplied polls until every replica has applied exactly want.
func (c *tcpCluster) waitApplied(t *testing.T, want []string) {
	t.Helper()
	wantStr := fmt.Sprint(want)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, id := range c.ids {
			if fmt.Sprint(c.logs[id].snapshot()) != wantStr {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range c.ids {
				t.Logf("%s applied %v", id, c.logs[id].snapshot())
			}
			t.Fatalf("replicas did not converge on %v", want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ---- test-only transport introspection ----

// inboundCount reports how many accepted connections are currently handled.
func (t *TCPTransport) inboundCount() int {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return len(t.inbound)
}

// peerConnVersion reports the TLS version negotiated on the cached outbound
// connection to peer (0 when absent or plaintext).
func (t *TCPTransport) peerConnVersion(peer string) uint16 {
	t.connMu.Lock()
	pc := t.conns[peer]
	t.connMu.Unlock()
	if pc == nil {
		return 0
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	tc, ok := pc.conn.(*tls.Conn)
	if !ok {
		return 0
	}
	return tc.ConnectionState().Version
}

// hasConn reports whether an outbound connection to peer is cached.
func (t *TCPTransport) hasConn(peer string) bool {
	t.connMu.Lock()
	pc := t.conns[peer]
	t.connMu.Unlock()
	if pc == nil {
		return false
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.conn != nil
}

// ---- happy paths over real TCP ----

// TestTCPClusterConvergesPlaintext: a 4-node cluster over real plaintext TCP
// commits every proposed command on every replica, in one order.
func TestTCPClusterConvergesPlaintext(t *testing.T) {
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, nil)
	cmds := []string{"a", "b", "c"}
	proposeLoop(t, c.nodes["t0"], cmds, 5)
	c.waitApplied(t, cmds)
}

// TestTCPClusterConvergesOverMTLS: the same cluster under mutual TLS — every
// connection is authenticated by certificate on both sides, and consensus
// still progresses (mTLS must never block legitimate peers).
func TestTCPClusterConvergesOverMTLS(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, ca)
	cmds := []string{"a", "b", "c"}
	proposeLoop(t, c.nodes["t0"], cmds, 5)
	c.waitApplied(t, cmds)

	// Every established connection negotiated TLS 1.3 (Go's default with these
	// configs): the transport's own traffic is modern TLS, not a fallback.
	for _, id := range ids {
		for _, peer := range peersExcept(ids, id) {
			if got := c.trs[id].peerConnVersion(peer); got != tls.VersionTLS13 {
				t.Fatalf("%s -> %s negotiated TLS version %#x, want TLS 1.3", id, peer, got)
			}
		}
	}
}

// ---- mTLS identity enforcement ----

// helloProbe drives a Hello exchange on an established connection and reports
// whether the listener KEPT the connection. Both sides introduce themselves
// before validating the peer, so receiving the peer's Hello proves nothing —
// what distinguishes acceptance is whether the listener keeps reading: a
// rejected peer is closed immediately (probe read -> EOF/reset), an accepted
// one stays open (probe read -> deadline).
func helloProbe(conn gonet.Conn, claimID string, claimPub ed25519.PublicKey) (bool, error) {
	hb, err := encodeMsg(kindHello, Hello{ID: claimID, Pub: claimPub})
	if err != nil {
		return false, err
	}
	if _, err := conn.Write(hb); err != nil {
		return false, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return false, err
	}
	rd := bufio.NewReader(conn)
	line, err := readWireLine(rd)
	if err != nil {
		return false, err // the listener closed instead of introducing itself
	}
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil || wm.Kind != kindHello {
		return false, fmt.Errorf("first line was not a Hello: %s", line)
	}
	// Probe: an accepted peer stays open and sends nothing.
	if err := conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		return false, err
	}
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		return false, nil // unexpected data after the Hello
	} else {
		var ne gonet.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return true, nil // still open: accepted
		}
		return false, nil // closed: rejected
	}
}

// dialAsTLS opens an mTLS connection to addr (presenting cert/key, pinning the
// listener as serverName) and runs the Hello probe as claimID.
func dialAsTLS(caPEM, certPEM, keyPEM []byte, addr, serverName, claimID string, claimPub ed25519.PublicKey) (bool, error) {
	cfg, err := mtls.Client(caPEM, certPEM, keyPEM, serverName)
	if err != nil {
		return false, err
	}
	conn, err := mtls.Dial("tcp", addr, cfg, 2*time.Second)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return helloProbe(conn, claimID, claimPub)
}

// TestTCPTransportTLSRejectsUnknownIdentity: a certificate the cluster CA
// signed but for a name that is not a validator must be refused — being
// CA-vouched is not enough, membership is what admits a transport peer. The
// impostor also must not get a key pinned on the target replica.
func TestTCPTransportTLSRejectsUnknownIdentity(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, ca)

	malloryCert, malloryKey, err := ca.Issue("mallory")
	if err != nil {
		t.Fatal(err)
	}
	_, malloryPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	// She dials t1 and introduces herself as "mallory" — a real identity, just
	// not one of the validators. A rejection may surface either at the TLS
	// handshake or at the Hello exchange (TLS 1.3 verifies the client
	// certificate after the server's Finished), so both count as refused.
	accepted, err := dialAsTLS(ca.CertPEM(), malloryCert, malloryKey, c.trs["t1"].Addr(), "t1", "mallory", malloryPriv.Public().(ed25519.PublicKey))
	if err == nil && accepted {
		t.Fatal("a non-validator identity was admitted by the TLS listener")
	}
	if _, pinned := c.nodes["t1"].PeerKey("mallory"); pinned {
		t.Fatal("an unknown identity got a consensus key pinned")
	}
	// Control: a configured validator performing the same exchange is admitted.
	validatorCert, validatorKey, err := ca.Issue("t2")
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = dialAsTLS(ca.CertPEM(), validatorCert, validatorKey, c.trs["t1"].Addr(), "t1", "t2", c.nodes["t2"].PublicKey())
	if err != nil {
		t.Fatalf("dial as a validator: %v", err)
	}
	if !accepted {
		t.Fatal("a configured validator was refused by the TLS listener")
	}
}

// TestTCPTransportTLSRejectsIdentityMismatch: a validator's certificate must
// agree with the id it claims in Hello. t2's own certificate introducing itself
// as t0 is rejected — mTLS identity and consensus identity cannot disagree, or
// a member could open a connection as itself and speak for another validator.
func TestTCPTransportTLSRejectsIdentityMismatch(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, ca)

	cert, key, err := ca.Issue("t2") // a genuine validator certificate...
	if err != nil {
		t.Fatal(err)
	}
	// ...claiming to be t0 (the leader), whose configured Ed25519 key it copies.
	accepted, err := dialAsTLS(ca.CertPEM(), cert, key, c.trs["t1"].Addr(), "t1", "t0", c.nodes["t0"].PublicKey())
	if err == nil && accepted {
		t.Fatal("a peer whose certificate identity disagreed with its Hello was admitted")
	}
}

// TestTCPTransportTLSRejectsUntrustedCA: a server presenting a certificate
// from a foreign CA must not be dialed into — the dialer pins the peer's
// identity against the cluster CA, so a rogue endpoint cannot impersonate a
// validator's address (DNS spoofing, stale DNS, a hijacked port).
func TestTCPTransportTLSRejectsUntrustedCA(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, ca)

	// A rogue endpoint holding a certificate for "t1" from a DIFFERENT CA.
	rogue, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	rogueCert, rogueKey, err := rogue.Issue("t1")
	if err != nil {
		t.Fatal(err)
	}
	rogueCfg, err := mtls.Server(rogue.CertPEM(), rogueCert, rogueKey, func(string) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", rogueCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 256)
				_, _ = conn.Read(buf) // drive the handshake, then drop
			}()
		}
	}()

	// A transport for t0 under the cluster CA, told that t1 lives at the rogue
	// address: the dial must fail on the certificate chain, not be delivered.
	tr := NewTCPTransport(c.nodes["t0"])
	keys := map[string]ed25519.PublicKey{}
	for _, id := range ids {
		keys[id] = c.nodes[id].PublicKey()
	}
	if err := tr.SetValidatorKeys(keys); err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue("t0")
	if err != nil {
		t.Fatal(err)
	}
	tr.EnableTLS(ca.CertPEM(), cert, key)
	tr.SetPeers(map[string]string{"t1": ln.Addr().String()})
	if err := tr.SendVote(context.Background(), "t1", &Vote{Height: 1, NodeID: "x", Voter: "t0"}); err == nil {
		t.Fatal("a foreign-CA certificate was accepted while dialing a validator")
	}
}

// TestTCPTransportPlaintextRejectsMismatchedHelloKey: without TLS the Ed25519
// key pre-share is the only transport gate — a peer whose claimed validator key
// does not match the configured one is refused before any message is handled.
func TestTCPTransportPlaintextRejectsMismatchedHelloKey(t *testing.T) {
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, nil)

	// An impostor claims to be t1 but introduces itself with its own key.
	_, impostorPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := gonet.DialTimeout("tcp", c.trs["t2"].Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	accepted, err := helloProbe(conn, "t1", impostorPriv.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if accepted {
		t.Fatal("an impostor claiming a validator id with the wrong key was admitted")
	}
	if _, pinned := c.nodes["t2"].PeerKey("t1"); !pinned {
		t.Fatal("test setup: t2 should already know t1's configured key")
	}
	pub, _ := c.nodes["t2"].PeerKey("t1")
	if bytes.Equal(pub, impostorPriv.Public().(ed25519.PublicKey)) {
		t.Fatal("the impostor's key replaced a pinned validator key")
	}
	// Control: the real t1 key is admitted over the same plaintext path.
	conn2, err := gonet.DialTimeout("tcp", c.trs["t2"].Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	accepted, err = helloProbe(conn2, "t1", c.nodes["t1"].PublicKey())
	if err != nil {
		t.Fatalf("probe as the real validator: %v", err)
	}
	if !accepted {
		t.Fatal("the real validator's key was refused over plaintext")
	}
}

// ---- transport faults ----

// TestTCPTransportPartitionAndHeal: dropping outbound messages to a peer
// (partition) keeps the rest of the cluster committing; once healed, the
// partitioned replica catches up through the fetch path.
func TestTCPTransportPartitionAndHeal(t *testing.T) {
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, nil)
	proposeLoop(t, c.nodes["t0"], []string{"a"}, 3)
	c.waitApplied(t, []string{"a"})

	c.trs["t0"].SetFaultInjector(func(from, to string) bool { return to == "t3" })
	proposeLoop(t, c.nodes["t0"], []string{"b"}, 3)
	c.trs["t0"].SetFaultInjector(nil)

	// Everything the leader sent while partitioned is simply missing at t3;
	// after healing, further proposals pull it back into sync.
	proposeLoop(t, c.nodes["t0"], []string{"c"}, 5)
	c.waitApplied(t, []string{"a", "b", "c"})
}

// TestTCPTransportReconnectsAfterPeerRestart: a peer that closes its listener
// (crash) is redialed, with the whole Hello — and, under mTLS, the TLS
// handshake — performed again on the new connection.
func TestTCPTransportReconnectsAfterPeerRestart(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"r0", "r1", "r2", "r3"}
	c := startTCPCluster(t, ids, ca)
	proposeLoop(t, c.nodes["r0"], []string{"a"}, 3)
	c.waitApplied(t, []string{"a"})

	// Restart r1 with the SAME address and the SAME consensus identity (its
	// private key is reused, so the stored public key still verifies). A peer
	// whose identity changed across a restart would be refused by every
	// validator that pinned it — that is the point of the identity file.
	addr := c.trs["r1"].Addr()
	oldPriv := c.nodes["r1"].priv // the restarted node keeps its identity
	c.trs["r1"].Close()
	tr := NewTCPTransport(nil)
	r1, err := NewReplica(Config{
		ID: "r1", Peers: peersExcept(ids, "r1"), Leader: "r0", PrivateKey: oldPriv,
	}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	tr.SetNode(r1)
	keys := map[string]ed25519.PublicKey{}
	peerAddrs := map[string]string{}
	for _, id := range ids {
		keys[id] = c.nodes[id].PublicKey()
		if id != "r1" {
			peerAddrs[id] = c.trs[id].Addr()
		}
	}
	if err := tr.SetValidatorKeys(keys); err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue("r1")
	if err != nil {
		t.Fatal(err)
	}
	tr.EnableTLS(ca.CertPEM(), cert, key)
	tr.SetPeers(peerAddrs)
	if _, err := tr.Listen(addr); err != nil {
		t.Fatalf("rebind r1 on %s: %v", addr, err)
	}
	defer tr.Close()
	c.trs["r1"] = tr
	c.nodes["r1"] = r1

	// The leader still holds its old (now dead) connection. Later proposals
	// must redial, and the redial must re-run the TLS handshake and the Hello
	// exchange — the restarted peer has no memory of the previous session.
	//
	// The first write to the dead socket can be swallowed by the kernel, so
	// keep proposing until one proposal reaches the restarted peer (which
	// catches up through the fetch path).
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; ; i++ {
		if _, err := c.nodes["r0"].Propose([]byte(fmt.Sprintf("b%d", i))); err != nil && err != ErrBusy {
			t.Fatalf("propose after peer restart: %v", err)
		}
		r1.mu.Lock()
		received := len(r1.blocks) > 1
		r1.mu.Unlock()
		if received {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted peer never received a proposal: reconnect failed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.trs["r0"].peerConnVersion("r1"); got != tls.VersionTLS13 {
		t.Fatalf("reconnected peer negotiated TLS version %#x, want TLS 1.3", got)
	}
}

// TestTCPTransportConnectPeersSkipsDeadPeer: a peer that is down must not stop
// the mesh from reaching the others (a single dead address cannot cost a node
// every other peer's key).
func TestTCPTransportConnectPeersSkipsDeadPeer(t *testing.T) {
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, nil)
	// A fresh transport with one dead address listed first, then two live ones.
	tr := NewTCPTransport(c.nodes["t0"])
	if err := tr.SetValidatorKeys(map[string]ed25519.PublicKey{
		"t0": c.nodes["t0"].PublicKey(), "t1": c.nodes["t1"].PublicKey(),
		"t2": c.nodes["t2"].PublicKey(), "t3": c.nodes["t3"].PublicKey(),
	}); err != nil {
		t.Fatal(err)
	}
	dead := deadAddr(t)
	tr.SetPeers(map[string]string{"dead": dead, "t1": c.trs["t1"].Addr(), "t2": c.trs["t2"].Addr()})
	start := time.Now()
	tr.ConnectPeers(time.Now().Add(300 * time.Millisecond))
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("ConnectPeers took %v with a dead peer", elapsed)
	}
	if !tr.hasConn("t1") || !tr.hasConn("t2") {
		t.Fatal("ConnectPeers stopped at the dead peer instead of reaching the live ones")
	}
}

// deadAddr returns an address nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestTCPTransportCloseDropsInboundConnections: Close must tear down accepted
// connections too, not just the listener — otherwise every inbound read loop
// blocks forever on a peer that never closes its side (goroutine + socket
// leak), and the peer keeps a half-dead connection.
func TestTCPTransportCloseDropsInboundConnections(t *testing.T) {
	ids := []string{"c0", "c1", "c2", "c3"}
	c := startTCPCluster(t, ids, nil)
	// ConnectPeers made every node dial every other (plus itself, since the
	// address map includes self), so each node handles one inbound connection
	// per member.
	before := c.trs["c1"].inboundCount()
	if before < 3 {
		t.Fatalf("c1 should handle an inbound connection per peer, got %d", before)
	}
	c.trs["c1"].Close() // NOTE: the other transports keep their outbound connections open
	deadline := time.Now().Add(5 * time.Second)
	for c.trs["c1"].inboundCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("Close left %d inbound read loops running", c.trs["c1"].inboundCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// peerAddr returns the recorded address for a peer (test helper).
func (t *TCPTransport) peerAddr(id string) string {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return t.peerAddrs[id]
}

// TestTCPTransportConcurrentSendsDuringPeerFlap: many goroutines send to the
// same peer while its address flaps between reachable and dead, and the
// transport is closed underneath them. This exercises the lock order the
// transport must keep (connMu before pc.mu — the reverse deadlocks against
// Close), the dial race when several senders redial at once, and the
// requirement that Close never blocks behind an in-flight dial.
func TestTCPTransportConcurrentSendsDuringPeerFlap(t *testing.T) {
	ids := []string{"q0", "q1", "q2", "q3"}
	c := startTCPCluster(t, ids, nil)
	tr := c.trs["q0"]
	good := tr.peerAddr("q1")
	dead := deadAddr(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = tr.SendVote(context.Background(), "q1", &Vote{Height: 1, NodeID: "blk", Voter: "q0"})
			}
		}()
	}
	// While the sends are in flight: flap the peer's address (forcing the
	// dial/redial race) and close the transport repeatedly (forcing the
	// connMu -> pc.mu order to interleave with senders that hold pc.mu).
	hammer := make(chan struct{})
	var hwg sync.WaitGroup
	hwg.Add(2)
	go func() {
		defer hwg.Done()
		for {
			select {
			case <-hammer:
				return
			default:
			}
			tr.RegisterPeer("q1", dead)
			tr.RegisterPeer("q1", good)
		}
	}()
	go func() {
		defer hwg.Done()
		for {
			select {
			case <-hammer:
				return
			default:
			}
			tr.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond)
	close(hammer)
	hwg.Wait()
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent sends/Close deadlocked")
	}

	// Close is still safe to repeat and left no inbound handler behind.
	tr.Close()
	if got := tr.inboundCount(); got != 0 {
		t.Fatalf("Close left %d inbound read loops running", got)
	}
}

// TestTCPTransportOversizedMessageClosesConnection: framing is newline-based,
// so an unbounded line is a trivial memory attack. The transport must refuse
// the message and drop the connection instead of growing its buffer.
func TestTCPTransportOversizedMessageClosesConnection(t *testing.T) {
	ids := []string{"t0", "t1", "t2", "t3"}
	c := startTCPCluster(t, ids, nil)

	// Writing on the established t0 -> t1 connection: t1 must refuse the
	// message and close its side, which t0 observes as EOF/reset on a read.
	pc, err := c.trs["t0"].peerConn("t1")
	if err != nil {
		t.Fatal(err)
	}
	pc.mu.Lock()
	conn := pc.conn
	_, _ = conn.Write(bytes.Repeat([]byte("x"), maxWireLine+16))
	pc.mu.Unlock()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("receiver stayed open after an oversized message")
	} else {
		var ne gonet.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("oversized message did not close the connection: receiver is still reading")
		}
	}
}
