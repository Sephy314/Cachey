package pbft

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
)

// This file implements the TCP NDJSON transport for PBFT messages (M5),
// mirroring Raft's tcp_transport.go. Unlike Raft's request/reply RPCs, PBFT
// messages are one-way multicasts: the transport writes each message to a
// peer's persistent connection and never waits for a reply.
//
// Key exchange (M3) happens on every (re)connection: each side first sends a
// Hello carrying its identity public key, and the peer registers it (trust on
// first use) so message signatures can be verified.

// tcpWriteTimeout bounds a single outbound message write.
const tcpWriteTimeout = 5 * time.Second

// Wire message kinds.
const (
	kindPrePrepare = "PrePrepare"
	kindPrepare    = "Prepare"
	kindCommit     = "Commit"
	kindViewChange = "ViewChange"
	kindNewView    = "NewView"
	kindHello      = "Hello"
)

// wireMsg is the NDJSON envelope: one JSON object per line.
type wireMsg struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// Hello introduces a replica on a fresh connection: its id and identity public
// key. The receiver registers the key (TOFU) so it can verify the sender's
// message signatures.
type Hello struct {
	ID  string `json:"id"`
	Pub []byte `json:"pub"`
}

// encodeMsg marshals v into a framed wire message with a trailing newline.
func encodeMsg(kind string, v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(wireMsg{Kind: kind, Data: data})
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// TCPTransport implements Transport over TCP. It keeps one persistent
// connection per peer and reconnects automatically after failures.
type TCPTransport struct {
	node      *Replica
	peerAddrs map[string]string
	ln        net.Listener
	conns     map[string]*peerConn
	connMu    sync.Mutex
	stopCh    chan struct{}
	doneCh    chan struct{}
	fault     func(from, to string) bool

	// mTLS (see EnableTLS). When tlsOn, connections are wrapped in TLS and the
	// Hello key exchange is trusted only from the mTLS-authenticated peer
	// (TOFU removed). peerTLS caches one client config per peer; serverTLS is
	// the listener config built once at Listen.
	tlsOn     bool
	tlsCA     []byte
	tlsCert   []byte
	tlsKey    []byte
	peerTLS   map[string]*tls.Config
	serverTLS *tls.Config
}

// peerConn is one outbound connection to a peer.
type peerConn struct {
	addr     string
	mu       sync.Mutex
	conn     net.Conn
	rd       *bufio.Reader
	helloSet bool
}

// NewTCPTransport creates a transport for node. peerAddrs can be populated
// after construction via SetPeers / RegisterPeer.
func NewTCPTransport(node *Replica) *TCPTransport {
	return &TCPTransport{
		node:      node,
		peerAddrs: make(map[string]string),
		conns:     make(map[string]*peerConn),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// SetPeers records the peer address map (id → host:port).
func (t *TCPTransport) SetPeers(addrs map[string]string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	for id, a := range addrs {
		t.peerAddrs[id] = a
	}
}

// RegisterPeer adds or updates the address for a peer.
func (t *TCPTransport) RegisterPeer(id, addr string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.peerAddrs[id] = addr
}

// SetNode wires the local replica that inbound messages are dispatched to.
func (t *TCPTransport) SetNode(n *Replica) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.node = n
}

// Close stops accepting and drops all peer connections. Safe to call once.
func (t *TCPTransport) Close() {
	t.connMu.Lock()
	if t.ln != nil {
		t.ln.Close()
	}
	for _, pc := range t.conns {
		pc.mu.Lock()
		if pc.conn != nil {
			pc.conn.Close()
			pc.conn = nil
		}
		pc.mu.Unlock()
	}
	t.conns = make(map[string]*peerConn)
	t.connMu.Unlock()
}

// SetFaultInjector installs a predicate that drops outbound messages to a
// target (network-partition simulation for tests). Pass nil to disable.
func (t *TCPTransport) SetFaultInjector(fn func(from, to string) bool) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.fault = fn
}

func (t *TCPTransport) partitioned(peer string) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.fault == nil || t.node == nil {
		return false
	}
	return t.fault(t.node.id, peer)
}

// Listen binds the local listener and starts accepting connections. It returns
// the bound address (useful with ":0" for tests).
func (t *TCPTransport) Listen(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	if t.tlsEnabled() {
		cfg, err := t.serverTLSConfig()
		if err != nil {
			ln.Close()
			return "", err
		}
		ln = tls.NewListener(ln, cfg)
	}
	t.ln = ln
	go t.acceptLoop()
	return ln.Addr().String(), nil
}

// Addr returns the bound listen address ("" if not listening).
func (t *TCPTransport) Addr() string {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.ln == nil {
		return ""
	}
	return t.ln.Addr().String()
}

// EnableTLS turns on mutual TLS for every PBFT message. This replica
// identifies itself with certPEM/keyPEM (whose DNS SAN must be this replica's
// id, see internal/mtls) and requires every peer to present a certificate
// signed by caPEM whose DNS SAN is a known replica id. It must be called
// before Listen. Plaintext stays the default (tests and local development).
func (t *TCPTransport) EnableTLS(caPEM, certPEM, keyPEM []byte) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.tlsOn = true
	t.tlsCA = caPEM
	t.tlsCert = certPEM
	t.tlsKey = keyPEM
	t.peerTLS = make(map[string]*tls.Config)
}

func (t *TCPTransport) tlsEnabled() bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return t.tlsOn
}

// serverTLSConfig builds (once) the listener's *tls.Config, whose accept
// predicate admits only certificates of known replicas (self or a configured
// peer). The predicate reads the live peer map, so members added via
// RegisterPeer are admitted without rebuilding the config.
func (t *TCPTransport) serverTLSConfig() (*tls.Config, error) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.serverTLS != nil {
		return t.serverTLS, nil
	}
	cfg, err := mtls.Server(t.tlsCA, t.tlsCert, t.tlsKey, t.acceptPeer)
	if err != nil {
		return nil, err
	}
	t.serverTLS = cfg
	return cfg, nil
}

// acceptPeer reports whether an inbound certificate's identity (its DNS SAN)
// belongs to a known replica: this replica or a configured peer.
func (t *TCPTransport) acceptPeer(identity string) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.node != nil && identity == t.node.id {
		return true
	}
	_, known := t.peerAddrs[identity]
	return known
}

// peerTLSConfig returns the cached client *tls.Config for dialing peer,
// pinning the peer's expected identity (its replica id) via ServerName.
func (t *TCPTransport) peerTLSConfig(peer string) (*tls.Config, error) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if c, ok := t.peerTLS[peer]; ok {
		return c, nil
	}
	cfg, err := mtls.Client(t.tlsCA, t.tlsCert, t.tlsKey, peer)
	if err != nil {
		return nil, err
	}
	t.peerTLS[peer] = cfg
	return cfg, nil
}

// dialPeer dials peer at addr, wrapping the connection in TLS when enabled:
// the peer must present a certificate signed by our CA whose DNS SAN is peer.
func (t *TCPTransport) dialPeer(addr, peer string) (net.Conn, error) {
	if !t.tlsEnabled() {
		return net.DialTimeout("tcp", addr, tcpWriteTimeout)
	}
	cfg, err := t.peerTLSConfig(peer)
	if err != nil {
		return nil, err
	}
	return mtls.Dial("tcp", addr, cfg, tcpWriteTimeout)
}

func (t *TCPTransport) acceptLoop() {
	defer close(t.doneCh)
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		go t.handleConn(conn)
	}
}

// handleConn runs an inbound connection: read the peer's Hello (register its
// key), then dispatch every following message to the local node.
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	peer, ok := t.exchangeHello(conn, rd, nil)
	if !ok {
		return
	}
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		if err := t.dispatch(peer, line); err != nil {
			log.Printf("pbft transport: dispatch from %s: %v", peer, err)
			return
		}
	}
}

// exchangeHello sends our Hello on conn and reads the peer's, registering its
// key. It returns the peer's id. peerConn (outbound) may be nil on inbound.
func (t *TCPTransport) exchangeHello(conn net.Conn, rd *bufio.Reader, pc *peerConn) (string, bool) {
	t.connMu.Lock()
	node := t.node
	myHello := Hello{ID: node.id, Pub: node.PublicKey()}
	t.connMu.Unlock()
	hb, err := encodeMsg(kindHello, myHello)
	if err != nil {
		return "", false
	}
	if _, err := conn.Write(hb); err != nil {
		return "", false
	}
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return "", false
	}
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		return "", false
	}
	var h Hello
	if wm.Kind != kindHello {
		return "", false // a peer must introduce itself first
	}
	if err := json.Unmarshal(wm.Data, &h); err != nil {
		return "", false
	}
	// With mTLS the connection's peer is authenticated by certificate, so the
	// Hello may only introduce that same peer — an impostor's Hello (TOFU) no
	// longer works. Plaintext keeps the legacy behavior for tests/dev.
	if t.tlsEnabled() && mtls.PeerIdentity(conn) != h.ID {
		return "", false
	}
	node.SetPeerKey(h.ID, h.Pub)
	return h.ID, true
}

// dispatch routes one wire message from peer to the local node's handler.
func (t *TCPTransport) dispatch(peer string, line []byte) error {
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		return err
	}
	t.connMu.Lock()
	node := t.node
	t.connMu.Unlock()
	if node == nil {
		return errors.New("pbft transport: no node")
	}
	switch wm.Kind {
	case kindPrePrepare:
		var m PrePrepare
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandlePrePrepare(&m)
	case kindPrepare:
		var m Prepare
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandlePrepare(&m)
	case kindCommit:
		var m Commit
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleCommit(&m)
	case kindViewChange:
		var m ViewChange
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleViewChange(&m)
	case kindNewView:
		var m NewView
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleNewView(&m)
	default:
		return errors.New("pbft transport: unknown kind " + wm.Kind)
	}
	return nil
}

// send writes a message to peer, dialing and introducing ourselves first.
func (t *TCPTransport) send(peer string, kind string, v any) error {
	if t.partitioned(peer) {
		return errors.New("pbft transport: partitioned")
	}
	pc, err := t.peerConn(peer)
	if err != nil {
		return err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn == nil {
		pc.mu.Unlock()
		t.forget(peer)
		return errors.New("pbft transport: no connection")
	}
	b, err := encodeMsg(kind, v)
	if err != nil {
		return err
	}
	if err := pc.conn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout)); err != nil {
		return err
	}
	if _, err := pc.conn.Write(b); err != nil {
		pc.conn.Close()
		pc.conn = nil
		return err
	}
	return nil
}

// peerConn returns (and lazily dials) the connection to peer. The dial
// completes the key handshake synchronously — both sides exchange their Hello
// and register keys before any real message is sent — so a message write never
// races a background handshake.
func (t *TCPTransport) peerConn(peer string) (*peerConn, error) {
	t.connMu.Lock()
	pc := t.conns[peer]
	addr := t.peerAddrs[peer]
	t.connMu.Unlock()
	if pc == nil {
		pc = &peerConn{addr: addr}
		t.connMu.Lock()
		t.conns[peer] = pc
		t.connMu.Unlock()
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		return pc, nil
	}
	conn, err := t.dialPeer(pc.addr, peer)
	if err != nil {
		return nil, err
	}
	rd := bufio.NewReader(conn)
	if _, ok := t.exchangeHello(conn, rd, pc); !ok {
		conn.Close()
		return nil, errors.New("pbft transport: key handshake with " + peer + " failed")
	}
	pc.conn = conn
	pc.rd = rd
	return pc, nil
}

// forget drops the cached connection to peer so the next send redials.
func (t *TCPTransport) forget(peer string) {
	t.connMu.Lock()
	if pc, ok := t.conns[peer]; ok {
		pc.mu.Lock()
		if pc.conn != nil {
			pc.conn.Close()
			pc.conn = nil
		}
		pc.mu.Unlock()
		delete(t.conns, peer)
	}
	t.connMu.Unlock()
}

// ---- Transport interface ----

func (t *TCPTransport) SendPrePrepare(_ context.Context, peer string, m *PrePrepare) error {
	return t.send(peer, kindPrePrepare, m)
}
func (t *TCPTransport) SendPrepare(_ context.Context, peer string, m *Prepare) error {
	return t.send(peer, kindPrepare, m)
}
func (t *TCPTransport) SendCommit(_ context.Context, peer string, m *Commit) error {
	return t.send(peer, kindCommit, m)
}
func (t *TCPTransport) SendViewChange(_ context.Context, peer string, m *ViewChange) error {
	return t.send(peer, kindViewChange, m)
}
func (t *TCPTransport) SendNewView(_ context.Context, peer string, m *NewView) error {
	return t.send(peer, kindNewView, m)
}
