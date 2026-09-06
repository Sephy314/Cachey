package raft

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Sephy314/Cachey/internal/mtls"
)

// rpcTimeout bounds a single Raft RPC round-trip over TCP.
const rpcTimeout = 2 * time.Second

// wireMsg is the NDJSON envelope for Raft RPCs: one JSON object per line.
type wireMsg struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// Wire message kinds.
const (
	kindRequestVote        = "RequestVote"
	kindRequestVoteReply   = "RequestVoteReply"
	kindAppendEntries      = "AppendEntries"
	kindAppendEntriesReply = "AppendEntriesReply"
	kindInstallSnapshot    = "InstallSnapshot"
	kindInstallSnapReply   = "InstallSnapshotReply"
)

// TCPTransport implements Transport over TCP with newline-delimited JSON
// framing. It keeps one persistent connection per peer (requests serialized
// per connection) and reconnects automatically after failures.
type TCPTransport struct {
	addr      string
	node      *Node
	peerAddrs map[string]string
	ln        net.Listener
	conns     map[string]*peerConn
	connMu    sync.Mutex
	stopCh    chan struct{}
	doneCh    chan struct{}

	// mTLS (see EnableTLS). When tlsOn, the listener wraps connections in TLS
	// and outbound dials present our cert and pin the peer's identity.
	// peerTLS caches one client *tls.Config per peer; serverTLS is the
	// listener config, built once at Listen.
	tlsOn     bool
	tlsCA     []byte
	tlsCert   []byte
	tlsKey    []byte
	peerTLS   map[string]*tls.Config
	serverTLS *tls.Config

	// fault, when non-nil, drops outbound RPCs from this node to a target
	// (network-partition simulation for tests).
	fault func(from, to string) bool
}

// SetFaultInjector installs a predicate that drops outbound RPCs: given this
// node's ID and the destination peer's ID it returns true to simulate a
// network partition. Pass nil to disable.
func (t *TCPTransport) SetFaultInjector(fn func(from, to string) bool) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.fault = fn
}

// partitioned reports whether an RPC from this node to peer should be dropped.
func (t *TCPTransport) partitioned(peer string) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.fault == nil || t.node == nil {
		return false
	}
	return t.fault(t.node.ID(), peer)
}

// NewTCPTransport creates a transport for node. peerAddrs maps peer ID to its
// TCP listen address; it can be populated after construction via SetPeers.
func NewTCPTransport(node *Node) *TCPTransport {
	return &TCPTransport{
		node:      node,
		peerAddrs: make(map[string]string),
		conns:     make(map[string]*peerConn),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// SetPeers records the peer address map (ID → host:port).
func (t *TCPTransport) SetPeers(addrs map[string]string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.peerAddrs = addrs
}

// RegisterPeer adds or updates the address for a peer (dynamic membership).
// It implements raft.PeerRegistrar.
func (t *TCPTransport) RegisterPeer(id, addr string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.peerAddrs[id] = addr
}

// SetNode wires the local raft node that inbound RPCs are dispatched to.
func (t *TCPTransport) SetNode(n *Node) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.node = n
}

// Listen binds the local listener and starts accepting Raft RPCs. It returns
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
	t.addr = ln.Addr().String()
	go t.acceptLoop()
	return t.addr, nil
}

// Addr returns the bound listen address ("" if not listening).
func (t *TCPTransport) Addr() string { return t.addr }

// EnableTLS turns on mutual TLS for every Raft RPC. This node identifies
// itself with certPEM/keyPEM (whose DNS SAN must be this node's id, see
// internal/mtls) and requires every peer to present a certificate signed by
// caPEM whose DNS SAN is a known node id. It must be called before Listen.
// Plaintext stays the default (tests and local development).
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

// serverTLSConfig builds (once) the listener's *tls.Config. Its accept
// predicate reads the live peer map, so dynamically added members are
// admitted without rebuilding the config.
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
// belongs to a known node: this node or a configured peer.
func (t *TCPTransport) acceptPeer(identity string) bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.node != nil && identity == t.node.ID() {
		return true
	}
	_, known := t.peerAddrs[identity]
	return known
}

// peerTLSConfig returns the cached client *tls.Config for dialing peer,
// pinning the peer's expected identity (its node id) via ServerName.
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
		return net.DialTimeout("tcp", addr, rpcTimeout)
	}
	cfg, err := t.peerTLSConfig(peer)
	if err != nil {
		return nil, err
	}
	return mtls.Dial("tcp", addr, cfg, rpcTimeout)
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
				// transient accept error (e.g. too many fds); retry
			}
			continue
		}
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		reply, err := t.dispatch(line)
		if err != nil {
			return
		}
		if _, err := conn.Write(reply); err != nil {
			return
		}
	}
}

// dispatch routes one wire message to the local node's Raft handler.
func (t *TCPTransport) dispatch(line []byte) ([]byte, error) {
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		return nil, err
	}
	t.connMu.Lock()
	node := t.node
	t.connMu.Unlock()
	if node == nil {
		return nil, errors.New("raft: transport has no node")
	}
	var out []byte
	switch wm.Kind {
	case kindRequestVote:
		var args RequestVote
		if err := json.Unmarshal(wm.Data, &args); err != nil {
			return nil, err
		}
		b, err := encodeMsg(kindRequestVoteReply, node.HandleRequestVote(&args))
		if err != nil {
			return nil, err
		}
		out = b
	case kindAppendEntries:
		var args AppendEntries
		if err := json.Unmarshal(wm.Data, &args); err != nil {
			return nil, err
		}
		b, err := encodeMsg(kindAppendEntriesReply, node.HandleAppendEntries(&args))
		if err != nil {
			return nil, err
		}
		out = b
	case kindInstallSnapshot:
		var args InstallSnapshot
		if err := json.Unmarshal(wm.Data, &args); err != nil {
			return nil, err
		}
		b, err := encodeMsg(kindInstallSnapReply, node.HandleInstallSnapshot(&args))
		if err != nil {
			return nil, err
		}
		out = b
	default:
		return nil, errors.New("raft: unknown message kind " + wm.Kind)
	}
	return out, nil
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

// ---- Transport interface ----

func (t *TCPTransport) SendRequestVote(ctx context.Context, peer string, args *RequestVote) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.roundTrip(ctx, peer, kindRequestVote, args, &reply, kindRequestVoteReply)
	return &reply, err
}

func (t *TCPTransport) SendAppendEntries(ctx context.Context, peer string, args *AppendEntries) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.roundTrip(ctx, peer, kindAppendEntries, args, &reply, kindAppendEntriesReply)
	return &reply, err
}

func (t *TCPTransport) SendInstallSnapshot(ctx context.Context, peer string, args *InstallSnapshot) (*InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	err := t.roundTrip(ctx, peer, kindInstallSnapshot, args, &reply, kindInstallSnapReply)
	return &reply, err
}

func (t *TCPTransport) roundTrip(ctx context.Context, peer, reqKind string, req, reply any, replyKind string) error {
	if t.partitioned(peer) {
		return errors.New("raft: partitioned")
	}
	pc, err := t.peerConn(peer)
	if err != nil {
		return err
	}
	pc.mu.Lock()
	if pc.conn == nil {
		pc.mu.Unlock()
		t.forget(peer)
		return errors.New("raft: peer connection closed")
	}
	if err := pc.writeReq(reqKind, req); err != nil {
		pc.mu.Unlock()
		t.forget(peer)
		return err
	}
	line, err := pc.readReply()
	pc.mu.Unlock()
	if err != nil {
		t.forget(peer)
		return err
	}
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		t.forget(peer)
		return err
	}
	if wm.Kind != replyKind {
		t.forget(peer)
		return errors.New("raft: unexpected reply kind " + wm.Kind)
	}
	if err := json.Unmarshal(wm.Data, reply); err != nil {
		t.forget(peer)
		return err
	}
	return nil
}

// peerConn returns the persistent connection to peer, dialing if needed. The
// dial and any TLS handshake happen outside connMu: the peer's identity
// callback and this node's own inbound accept both take connMu, so holding it
// across a handshake would deadlock concurrent node-to-node dials. Dials to
// the same peer are serialized by the per-peer pc.mu.
func (t *TCPTransport) peerConn(peer string) (*peerConn, error) {
	t.connMu.Lock()
	pc := t.conns[peer]
	if pc == nil {
		addr, ok := t.peerAddrs[peer]
		if !ok {
			t.connMu.Unlock()
			return nil, errors.New("raft: no address for peer " + peer)
		}
		pc = &peerConn{addr: addr}
		t.conns[peer] = pc
	}
	addr := pc.addr
	t.connMu.Unlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		return pc, nil
	}
	conn, err := t.dialPeer(addr, peer)
	if err != nil {
		return nil, err
	}
	pc.conn = conn
	pc.rd = bufio.NewReader(conn)
	return pc, nil
}

// forget drops a broken peer connection so the next send redials.
func (t *TCPTransport) forget(peer string) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if pc, ok := t.conns[peer]; ok {
		pc.close()
		delete(t.conns, peer)
	}
}

// Close stops the listener and closes all peer connections.
func (t *TCPTransport) Close() error {
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
	if t.ln != nil {
		t.ln.Close() // unblock Accept so acceptLoop can exit
		<-t.doneCh
	}
	t.connMu.Lock()
	defer t.connMu.Unlock()
	for _, pc := range t.conns {
		pc.close()
	}
	return nil
}

// peerConn is one persistent connection to a peer; requests on it are
// serialized by mu so wire frames never interleave, and conn is only touched
// while mu is held.
type peerConn struct {
	addr string
	conn net.Conn
	rd   *bufio.Reader
	mu   sync.Mutex
}

func (pc *peerConn) writeReq(kind string, v any) error {
	b, err := encodeMsg(kind, v)
	if err != nil {
		return err
	}
	if err := pc.conn.SetWriteDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return err
	}
	_, err = pc.conn.Write(b)
	return err
}

func (pc *peerConn) readReply() ([]byte, error) {
	if err := pc.conn.SetReadDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return nil, err
	}
	line, err := pc.rd.ReadBytes('\n')
	if err == io.EOF && len(line) > 0 {
		return line, nil
	}
	return line, err
}

func (pc *peerConn) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}
