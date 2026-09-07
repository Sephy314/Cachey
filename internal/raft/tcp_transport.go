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
	// kindCtl is an opaque application control message (e.g. JOIN) carried on
	// the raft transport. It is dispatched to the transport's control handler
	// (SetControlHandler) rather than to the raft node, and — unlike raft RPCs
	// — is served to any certificate the cluster CA vouches for, which is how
	// a node that is not a member yet can ask to join.
	kindCtl      = "ctl"
	kindCtlReply = "ctl-reply"
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

	// ctl, when non-nil, handles inbound control messages. It receives the
	// sender's identity (its certificate DNS SAN, "" in plaintext) and the raw
	// request payload and returns the raw reply payload.
	ctl func(peer string, data []byte) ([]byte, error)
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

// SetControlHandler installs the application control handler. When set, inbound
// control (ctl) messages are delivered to fn with the sender's identity ("" in
// plaintext). Control is served to any certificate the cluster CA vouches for,
// so a not-yet-member node can ask to join; raft RPCs, in contrast, are only
// served to known members (see dispatch).
func (t *TCPTransport) SetControlHandler(fn func(peer string, data []byte) ([]byte, error)) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.ctl = fn
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

// PeerAddrs returns a copy of the peer address map (ID → host:port). It lets
// Node.AddServer embed the current membership's addresses in a committed
// configuration, making it self-describing.
func (t *TCPTransport) PeerAddrs() map[string]string {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	out := make(map[string]string, len(t.peerAddrs))
	for k, v := range t.peerAddrs {
		out[k] = v
	}
	return out
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

// acceptPeer reports whether an inbound TLS peer may CONNECT. It returns true
// for every certificate the cluster CA already vouched for (mtls runs its chain
// check before this callback): an established member must be able to accept a
// not-yet-member node's control (JOIN) connection. Per-message gating in
// dispatch then ensures such a peer can only send control messages, never raft
// RPCs.
func (t *TCPTransport) acceptPeer(string) bool { return true }

// isMemberPeer reports whether identity (a peer's certificate DNS SAN) is this
// node or a known peer (a member, or a member being caught up). Raft RPCs are
// only served to known peers; control messages are served to any CA-vouched
// peer. An empty identity (plaintext) is always treated as a member.
func (t *TCPTransport) isMemberPeer(identity string) bool {
	if identity == "" {
		return true // plaintext: no identity enforcement (development)
	}
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
	peer := "" // the peer's certificate identity; learned after the lazy TLS handshake
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		if peer == "" {
			peer = mtls.PeerIdentity(conn)
		}
		reply, err := t.dispatch(line, peer)
		if err != nil {
			return
		}
		if _, err := conn.Write(reply); err != nil {
			return
		}
	}
}

// dispatch routes one wire message. Raft RPCs are served only to known peers
// (isMemberPeer); the application control (ctl) message is served to any
// CA-vouched peer via the control handler, with the sender's identity passed
// through so the handler can bind it to what the message claims.
func (t *TCPTransport) dispatch(line []byte, peer string) ([]byte, error) {
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		return nil, err
	}
	if wm.Kind == kindCtl {
		t.connMu.Lock()
		ctl := t.ctl
		t.connMu.Unlock()
		if ctl == nil {
			return nil, errors.New("raft: no control handler")
		}
		reply, err := ctl(peer, wm.Data)
		if err != nil {
			return nil, err
		}
		return rawMsg(kindCtlReply, reply)
	}
	if !t.isMemberPeer(peer) {
		// A non-member (CA-vouched but not in the cluster) may send control
		// messages but never raft RPCs.
		return nil, errors.New("raft: rpc from non-member " + peer)
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

// rawMsg frames raw JSON payload bytes (no re-encoding) as a wire message.
func rawMsg(kind string, raw []byte) ([]byte, error) {
	b, err := json.Marshal(wireMsg{Kind: kind, Data: json.RawMessage(raw)})
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
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

// SendControl sends an application control message (raw JSON payload) to peer
// over the shared per-peer connection and returns the raw reply payload. It is
// the transport-level channel for cluster control such as JOIN.
func (t *TCPTransport) SendControl(ctx context.Context, peer string, req []byte) ([]byte, error) {
	if t.partitioned(peer) {
		return nil, errors.New("raft: partitioned")
	}
	pc, err := t.peerConn(peer)
	if err != nil {
		return nil, err
	}
	pc.mu.Lock()
	if pc.conn == nil {
		pc.mu.Unlock()
		t.forget(peer)
		return nil, errors.New("raft: peer connection closed")
	}
	if err := pc.writeRawBytes(kindCtl, req); err != nil {
		pc.mu.Unlock()
		t.forget(peer)
		return nil, err
	}
	line, err := pc.readReply()
	pc.mu.Unlock()
	if err != nil {
		t.forget(peer)
		return nil, err
	}
	var wm wireMsg
	if err := json.Unmarshal(line, &wm); err != nil {
		t.forget(peer)
		return nil, err
	}
	if wm.Kind != kindCtlReply {
		t.forget(peer)
		return nil, errors.New("raft: unexpected reply kind " + wm.Kind)
	}
	return []byte(wm.Data), nil
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
	return pc.writeBytes(b)
}

// writeRawBytes frames raw JSON payload bytes as a control message and writes
// them without re-encoding (the payload is already JSON).
func (pc *peerConn) writeRawBytes(kind string, raw []byte) error {
	b, err := rawMsg(kind, raw)
	if err != nil {
		return err
	}
	return pc.writeBytes(b)
}

func (pc *peerConn) writeBytes(b []byte) error {
	if err := pc.conn.SetWriteDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return err
	}
	_, err := pc.conn.Write(b)
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
