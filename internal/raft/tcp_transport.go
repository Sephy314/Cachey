package raft

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
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

// SetNode wires the local raft node that inbound RPCs are dispatched to.
func (t *TCPTransport) SetNode(n *Node) {
	t.node = n
}

// Listen binds the local listener and starts accepting Raft RPCs. It returns
// the bound address (useful with ":0" for tests).
func (t *TCPTransport) Listen(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	t.ln = ln
	t.addr = ln.Addr().String()
	go t.acceptLoop()
	return t.addr, nil
}

// Addr returns the bound listen address ("" if not listening).
func (t *TCPTransport) Addr() string { return t.addr }

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
	if t.node == nil {
		return nil, errors.New("raft: transport has no node")
	}
	var out []byte
	switch wm.Kind {
	case kindRequestVote:
		var args RequestVote
		if err := json.Unmarshal(wm.Data, &args); err != nil {
			return nil, err
		}
		b, err := encodeMsg(kindRequestVoteReply, t.node.HandleRequestVote(&args))
		if err != nil {
			return nil, err
		}
		out = b
	case kindAppendEntries:
		var args AppendEntries
		if err := json.Unmarshal(wm.Data, &args); err != nil {
			return nil, err
		}
		b, err := encodeMsg(kindAppendEntriesReply, t.node.HandleAppendEntries(&args))
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

func (t *TCPTransport) roundTrip(ctx context.Context, peer, reqKind string, req, reply any, replyKind string) error {
	pc, err := t.peerConn(peer)
	if err != nil {
		return err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.writeReq(reqKind, req); err != nil {
		t.forget(peer)
		return err
	}
	line, err := pc.readReply()
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

// peerConn returns the persistent connection to peer, dialing if needed.
func (t *TCPTransport) peerConn(peer string) (*peerConn, error) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if pc, ok := t.conns[peer]; ok && pc.isOpen() {
		return pc, nil
	}
	addr, ok := t.peerAddrs[peer]
	if !ok {
		return nil, errors.New("raft: no address for peer " + peer)
	}
	conn, err := net.DialTimeout("tcp", addr, rpcTimeout)
	if err != nil {
		return nil, err
	}
	pc := &peerConn{conn: conn, rd: bufio.NewReader(conn)}
	t.conns[peer] = pc
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
// serialized by mu so wire frames never interleave.
type peerConn struct {
	conn net.Conn
	rd   *bufio.Reader
	mu   sync.Mutex
}

func (pc *peerConn) isOpen() bool {
	return pc.conn != nil
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
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}
