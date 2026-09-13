package hotstuff

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	gonet "net"
	"sync"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
)

// This file implements the TCP NDJSON transport for HotStuff messages (HS-M5),
// mirroring Raft/PBFT's tcp_transport.go. Unlike Raft's request/reply RPCs,
// HotStuff messages are one-way: the transport writes each message to a peer's
// persistent outbound connection and never waits for a reply.
//
// Two authentication layers stack, and neither replaces the other:
//
//	mTLS (EnableTLS) — authenticates the TRANSPORT peer: the connection's
//	  certificate must be signed by the cluster CA and carry the peer's node id
//	  as its DNS SAN, so a connection is only ever accepted from a validator.
//	Ed25519 Hello + message signatures (HS-M3) — authenticate the CONSENSUS
//	  sender: every proposal/vote/view change/block is signed by a key fixed in
//	  the validator configuration, and a QC is 2f+1 such signatures.
//
// Plaintext (the default) is for tests and local development; a production
// node runs the transport under mTLS (see server.HotStuffNodeConfig.TLSCA).

// tcpWriteTimeout bounds a single outbound message write, a dial and the
// blocking Hello exchange on a fresh connection.
const tcpWriteTimeout = 5 * time.Second

// maxWireLine bounds one wire message. NDJSON framing means a malicious or
// corrupt peer could otherwise force an unbounded allocation with a single
// endless line.
const maxWireLine = 4 << 20 // 4 MiB

// Wire message kinds.
const (
	kindProposal   = "Proposal"
	kindVote       = "Vote"
	kindViewChange = "ViewChange"
	kindFetch      = "Fetch"
	kindBlock      = "Block"
	kindGetBlocks  = "GetBlocks"
	kindBlockBatch = "BlockBatch"
	kindHello      = "Hello"
)

// wireMsg is the NDJSON envelope: one JSON object per line.
type wireMsg struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// Hello introduces a replica on a fresh connection: its id and identity public
// key. The receiver compares that key with the preconfigured validator key
// before accepting any message signatures (HS-M3).
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

// TCPTransport implements Transport over TCP. It keeps one persistent outbound
// connection per peer and reconnects automatically after failures. Inbound
// connections each run a read loop that dispatches to the local node.
type TCPTransport struct {
	node          *Replica
	peerAddrs     map[string]string
	validatorKeys map[string]ed25519.PublicKey
	ln            gonet.Listener
	conns         map[string]*peerConn
	inbound       map[gonet.Conn]struct{} // accepted connections, closed by Close
	connMu        sync.Mutex
	stopCh        chan struct{}
	doneCh        chan struct{}
	closeOnce     sync.Once
	fault         func(from, to string) bool

	// mTLS (see EnableTLS). When tlsOn, the listener wraps connections in TLS
	// (admitting only certificates whose SAN is a configured validator) and
	// outbound dials present our certificate and pin the peer's node id.
	// peerTLS caches one client *tls.Config per peer; serverTLS is the listener
	// config, built once at Listen.
	tlsOn     bool
	tlsCA     []byte
	tlsCert   []byte
	tlsKey    []byte
	peerTLS   map[string]*tls.Config
	serverTLS *tls.Config
}

// peerConn is one outbound connection to a peer.
type peerConn struct {
	addr string
	mu   sync.Mutex
	conn gonet.Conn
	rd   *bufio.Reader
}

// NewTCPTransport creates a transport for node. Peer addresses can be populated
// after construction via SetPeers / RegisterPeer.
func NewTCPTransport(node *Replica) *TCPTransport {
	return &TCPTransport{
		node:          node,
		peerAddrs:     make(map[string]string),
		validatorKeys: make(map[string]ed25519.PublicKey),
		conns:         make(map[string]*peerConn),
		inbound:       make(map[gonet.Conn]struct{}),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// EnableTLS turns on mutual TLS for every peer connection: this node identifies
// itself with certPEM/keyPEM (whose DNS SAN must be its id, see internal/mtls)
// and requires every peer to present a certificate signed by caPEM whose DNS
// SAN is a configured validator. It must be called before Listen. Plaintext
// stays the default (tests and local development); a production node uses mTLS.
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

// serverTLSConfig builds (once) the listener's *tls.Config. mtls.Server
// requires a CA-signed client certificate, so the handshake itself is what
// rejects a plaintext or unknown-CA peer; the accept predicate then narrows it
// to configured validators only.
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

// acceptPeer reports whether an inbound TLS peer may connect at all: its
// certificate's DNS SAN must be this node or a configured validator. This is
// the transport-level identity check — a certificate for any other name is
// refused before a single HotStuff byte is exchanged. (Per-message consensus
// authentication still runs on top; mTLS never replaces it.)
func (t *TCPTransport) acceptPeer(identity string) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.node != nil && identity == t.node.id {
		return true
	}
	_, known := t.validatorKeys[identity]
	return known
}

// peerTLSConfig returns the cached client *tls.Config for dialing peer, pinning
// the peer's expected identity (its node id) via ServerName, so Go's standard
// chain + hostname verification rejects a certificate for any other member.
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

// dialConn opens the raw (or TLS-wrapped) connection to addr, pinning peer's
// identity when TLS is enabled.
func (t *TCPTransport) dialConn(peer, addr string) (gonet.Conn, error) {
	if !t.tlsEnabled() {
		return gonet.DialTimeout("tcp", addr, tcpWriteTimeout)
	}
	cfg, err := t.peerTLSConfig(peer)
	if err != nil {
		return nil, err
	}
	return mtls.Dial("tcp", addr, cfg, tcpWriteTimeout)
}

// SetValidatorKeys fixes the identity public key for every validator. It must
// be called before Listen or ConnectPeers. Hello messages are accepted only
// when they present the exact configured key for their claimed ID.
func (t *TCPTransport) SetValidatorKeys(keys map[string]ed25519.PublicKey) error {
	t.connMu.Lock()
	node := t.node
	t.connMu.Unlock()
	if node == nil {
		return errors.New("hotstuff transport: no node")
	}
	for id, pub := range keys {
		if len(pub) != ed25519.PublicKeySize || !node.SetPeerKey(id, pub) {
			return errors.New("hotstuff transport: invalid or conflicting validator key for " + id)
		}
	}
	t.connMu.Lock()
	t.validatorKeys = make(map[string]ed25519.PublicKey, len(keys))
	for id, pub := range keys {
		t.validatorKeys[id] = append(ed25519.PublicKey(nil), pub...)
	}
	t.connMu.Unlock()
	return nil
}

// SetPeers records the peer address map (id -> host:port).
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

// Close stops accepting and drops all peer connections — outbound and inbound
// — then closes stopCh so the accept loop (and anything else waiting on it)
// unwinds and doneCh closes. Closing the accepted connections is what lets
// their read-loop goroutines exit: without it they would block on a peer that
// never closes its side, leaking a goroutine and a socket per inbound
// connection. Safe to call more than once.
func (t *TCPTransport) Close() {
	t.closeOnce.Do(func() { close(t.stopCh) })
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
	for conn := range t.inbound {
		conn.Close()
	}
	t.inbound = make(map[gonet.Conn]struct{})
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
// the bound address (useful with ":0" for tests). With TLS enabled the
// listener requires every peer to present a CA-signed certificate whose SAN is
// a configured validator.
func (t *TCPTransport) Listen(addr string) (string, error) {
	ln, err := gonet.Listen("tcp", addr)
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
	t.connMu.Lock()
	t.ln = ln
	t.connMu.Unlock()
	go t.acceptLoop(ln)
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

func (t *TCPTransport) acceptLoop(ln gonet.Listener) {
	defer close(t.doneCh)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Exiting on either the listener being closed (Close) or stopCh
			// being closed — otherwise a closed listener spins on Accept errors.
			if errors.Is(err, gonet.ErrClosed) {
				return
			}
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		if !t.trackInbound(conn) {
			conn.Close() // transport already closed
			return
		}
		go t.handleConn(conn)
	}
}

// trackInbound records an accepted connection so Close can drop it. It reports
// false when the transport is already closed (the connection must then be
// dropped instead of handled).
func (t *TCPTransport) trackInbound(conn gonet.Conn) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	select {
	case <-t.stopCh:
		return false
	default:
	}
	t.inbound[conn] = struct{}{}
	return true
}

func (t *TCPTransport) untrackInbound(conn gonet.Conn) {
	t.connMu.Lock()
	delete(t.inbound, conn)
	t.connMu.Unlock()
}

// handleConn runs an inbound connection: read the peer's Hello (register its
// key), then dispatch every following message to the local node.
func (t *TCPTransport) handleConn(conn gonet.Conn) {
	defer func() {
		conn.Close()
		t.untrackInbound(conn)
	}()
	rd := bufio.NewReader(conn)
	peer, ok := t.exchangeHello(conn, rd, "")
	if !ok {
		return
	}
	for {
		line, err := readWireLine(rd)
		if err != nil {
			return
		}
		if err := t.dispatch(peer, line); err != nil {
			log.Printf("hotstuff transport: dispatch from %s: %v", peer, err)
			return
		}
	}
}

// readWireLine reads one newline-terminated wire message. A message must be
// strictly smaller than maxWireLine: the size check runs BEFORE the reader
// blocks again, so a peer streaming an endless unterminated line is cut off as
// soon as the limit is reached rather than pinning the goroutine (ReadSlice
// alone would keep waiting for a delimiter).
func readWireLine(rd *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := rd.ReadSlice('\n')
		buf = append(buf, chunk...)
		if len(buf) >= maxWireLine {
			return nil, errors.New("hotstuff transport: message exceeds size limit")
		}
		if err == nil {
			return buf, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

// exchangeHello sends our Hello and checks the peer's response against the
// configured validator key. expected is set for an outbound dial, binding the
// connection target to the claimed validator identity. Under TLS the
// certificate's DNS SAN must additionally equal the claimed id, so the
// transport identity and the consensus identity cannot disagree.
func (t *TCPTransport) exchangeHello(conn gonet.Conn, rd *bufio.Reader, expected string) (string, bool) {
	t.connMu.Lock()
	node := t.node
	tlsOn := t.tlsOn
	t.connMu.Unlock()
	if node == nil {
		return "", false
	}
	myHello := Hello{ID: node.id, Pub: node.PublicKey()}
	hb, err := encodeMsg(kindHello, myHello)
	if err != nil {
		return "", false
	}
	// The Hello exchange is the one blocking read on a fresh connection that a
	// peer fully controls: bound it, or a peer that connects and never
	// introduces itself pins this connection's goroutine indefinitely. (On a
	// TLS listener this write is also what drives the handshake.)
	if err := conn.SetReadDeadline(time.Now().Add(tcpWriteTimeout)); err != nil {
		return "", false
	}
	defer conn.SetReadDeadline(time.Time{})
	if _, err := conn.Write(hb); err != nil {
		return "", false
	}
	line, err := readWireLine(rd)
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
	if tlsOn {
		// The certificate is the transport's claim of who this is; the Hello is
		// the consensus layer's. They must agree, or a validator could open a
		// connection with its own certificate and introduce itself as a peer.
		// (A failed handshake yields no peer certificate: "" fails here too.)
		if ident := mtls.PeerIdentity(conn); ident == "" || ident != h.ID {
			return "", false
		}
	}
	t.connMu.Lock()
	configured, known := t.validatorKeys[h.ID]
	t.connMu.Unlock()
	if !known || (expected != "" && h.ID != expected) || !bytes.Equal(h.Pub, configured) {
		return "", false
	}
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
		return errors.New("hotstuff transport: no node")
	}
	switch wm.Kind {
	case kindProposal:
		var m Proposal
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleProposal(&m)
	case kindVote:
		var m Vote
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleVote(&m)
	case kindViewChange:
		var m ViewChange
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleViewChange(&m)
	case kindFetch:
		var m Fetch
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleFetch(&m)
	case kindBlock:
		var m BlockMsg
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleBlock(&m)
	case kindGetBlocks:
		var m GetBlocks
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleGetBlocks(&m)
	case kindBlockBatch:
		var m BlockBatch
		if err := json.Unmarshal(wm.Data, &m); err != nil {
			return err
		}
		node.HandleBlockBatch(&m)
	default:
		return errors.New("hotstuff transport: unknown kind " + wm.Kind)
	}
	return nil
}

// send writes a message to peer, dialing and introducing ourselves first.
func (t *TCPTransport) send(peer string, kind string, v any) error {
	if t.partitioned(peer) {
		return errors.New("hotstuff transport: partitioned")
	}
	pc, err := t.peerConn(peer)
	if err != nil {
		return err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn == nil {
		// A concurrent send failed and dropped the connection; the next send
		// redials (peerConn dials whenever conn is nil). Do NOT drop the cached
		// entry from here: that would take connMu while this holds pc.mu, which
		// inverts the connMu -> pc.mu order Close relies on.
		return errors.New("hotstuff transport: no connection")
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

// ConnectPeers dials every configured peer and completes the Hello key
// handshake, so this node holds every member's identity key before traffic
// flows. HotStuff's message pattern (proposals leader→all, votes all→leader)
// never otherwise connects followers to each other, yet a follower must verify
// QCs carrying any 2f+1 members' votes — so identity keys must be exchanged up
// front across the whole mesh.
//
// Best-effort: a peer that is not up yet is retried until deadline, and a peer
// still down then is SKIPPED rather than aborting the loop — one dead address
// must not cost this node every remaining peer's key. Peers skipped here are
// still dialed lazily on the first message sent to them, so the mesh heals.
func (t *TCPTransport) ConnectPeers(deadline time.Time) {
	t.connMu.Lock()
	peers := make([]string, 0, len(t.peerAddrs))
	for p := range t.peerAddrs {
		peers = append(peers, p)
	}
	t.connMu.Unlock()
	for _, p := range peers {
		for {
			if _, err := t.peerConn(p); err == nil {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// peerConn returns (and lazily dials) the connection to peer. The dial
// completes the key handshake synchronously — both sides exchange their Hello
// and register keys before any real message is sent — so a message write never
// races a background handshake.
//
// Lock order: connMu is always taken BEFORE pc.mu (Close holds connMu and then
// each pc.mu). The dial and handshake therefore run WITHOUT pc.mu held — the
// handshake itself takes connMu, so holding pc.mu across it would invert the
// order and deadlock against a concurrent Close.
func (t *TCPTransport) peerConn(peer string) (*peerConn, error) {
	t.connMu.Lock()
	pc := t.conns[peer]
	addr := t.peerAddrs[peer]
	if pc == nil {
		pc = &peerConn{addr: addr}
		t.conns[peer] = pc
	}
	t.connMu.Unlock()
	if addr == "" {
		return nil, errors.New("hotstuff transport: unknown peer " + peer)
	}
	pc.mu.Lock()
	if pc.conn != nil {
		pc.mu.Unlock()
		return pc, nil
	}
	pc.mu.Unlock()

	conn, err := t.dialConn(peer, pc.addr)
	if err != nil {
		return nil, err
	}
	rd := bufio.NewReader(conn)
	if _, ok := t.exchangeHello(conn, rd, peer); !ok {
		conn.Close()
		return nil, errors.New("hotstuff transport: key handshake with " + peer + " failed")
	}
	pc.mu.Lock()
	if pc.conn != nil { // another sender won the dial race; keep its connection
		pc.mu.Unlock()
		conn.Close()
		return pc, nil
	}
	pc.conn = conn
	pc.rd = rd
	pc.mu.Unlock()
	return pc, nil
}

// ---- Transport interface ----

func (t *TCPTransport) SendProposal(_ context.Context, peer string, m *Proposal) error {
	return t.send(peer, kindProposal, m)
}
func (t *TCPTransport) SendGetBlocks(_ context.Context, peer string, m *GetBlocks) error {
	return t.send(peer, kindGetBlocks, m)
}
func (t *TCPTransport) SendBlockBatch(_ context.Context, peer string, m *BlockBatch) error {
	return t.send(peer, kindBlockBatch, m)
}
func (t *TCPTransport) SendVote(_ context.Context, peer string, m *Vote) error {
	return t.send(peer, kindVote, m)
}
func (t *TCPTransport) SendViewChange(_ context.Context, peer string, m *ViewChange) error {
	return t.send(peer, kindViewChange, m)
}
func (t *TCPTransport) SendFetch(_ context.Context, peer string, m *Fetch) error {
	return t.send(peer, kindFetch, m)
}
func (t *TCPTransport) SendBlock(_ context.Context, peer string, m *BlockMsg) error {
	return t.send(peer, kindBlock, m)
}
