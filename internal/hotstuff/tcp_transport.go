package hotstuff

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"log"
	gonet "net"
	"sync"
	"time"
)

// This file implements the TCP NDJSON transport for HotStuff messages (HS-M5),
// mirroring Raft/PBFT's tcp_transport.go. Unlike Raft's request/reply RPCs,
// HotStuff messages are one-way: the transport writes each message to a peer's
// persistent outbound connection and never waits for a reply.
//
// Key exchange (HS-M3) happens on every (re)connection, but Hello is only a
// proof of possession check against a public key fixed in the validator
// configuration. It is never trust-on-first-use: validator identities must be
// established before the transport becomes network-visible.

// tcpWriteTimeout bounds a single outbound message write and a dial.
const tcpWriteTimeout = 5 * time.Second

// Wire message kinds.
const (
	kindProposal   = "Proposal"
	kindVote       = "Vote"
	kindViewChange = "ViewChange"
	kindFetch      = "Fetch"
	kindBlock      = "Block"
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
	connMu        sync.Mutex
	stopCh        chan struct{}
	doneCh        chan struct{}
	closeOnce     sync.Once
	fault         func(from, to string) bool
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
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
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

// Close stops accepting and drops all peer connections, then closes stopCh so
// the accept loop (and anything else waiting on it) unwinds and doneCh closes.
// Safe to call more than once.
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
	t.closeOnce.Do(func() { close(t.stopCh) })
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
	ln, err := gonet.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	t.ln = ln
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
		go t.handleConn(conn)
	}
}

// handleConn runs an inbound connection: read the peer's Hello (register its
// key), then dispatch every following message to the local node.
func (t *TCPTransport) handleConn(conn gonet.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	peer, ok := t.exchangeHello(conn, rd, "")
	if !ok {
		return
	}
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		if err := t.dispatch(peer, line); err != nil {
			log.Printf("hotstuff transport: dispatch from %s: %v", peer, err)
			return
		}
	}
}

// exchangeHello sends our Hello and checks the peer's response against the
// configured validator key. expected is set for an outbound dial, binding the
// connection target to the claimed validator identity.
func (t *TCPTransport) exchangeHello(conn gonet.Conn, rd *bufio.Reader, expected string) (string, bool) {
	t.connMu.Lock()
	node := t.node
	t.connMu.Unlock()
	if node == nil {
		return "", false
	}
	myHello := Hello{ID: node.id, Pub: node.PublicKey()}
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
		t.forget(peer)
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
// front across the whole mesh. Best-effort: peers that are not up yet are
// retried until deadline.
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
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// peerConn returns (and lazily dials) the connection to peer. The dial
// completes the key handshake synchronously — both sides exchange their Hello
// and register keys before any real message is sent — so a message write never
// races a background handshake.
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
	defer pc.mu.Unlock()
	if pc.conn != nil {
		return pc, nil
	}
	conn, err := gonet.DialTimeout("tcp", pc.addr, tcpWriteTimeout)
	if err != nil {
		return nil, err
	}
	rd := bufio.NewReader(conn)
	if _, ok := t.exchangeHello(conn, rd, peer); !ok {
		conn.Close()
		return nil, errors.New("hotstuff transport: key handshake with " + peer + " failed")
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

func (t *TCPTransport) SendProposal(_ context.Context, peer string, m *Proposal) error {
	return t.send(peer, kindProposal, m)
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
