package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
)

// MemberKeyPrefix keys the replicated registry that maps a node id to its
// client-facing (data-plane) address. The leader writes member/<id> when a node
// bootstraps or joins, so every node's FSM keeps the full table and can answer
// "not leader: <client addr>" redirects for cache clients. The prefix is
// reserved: the client data handler rejects any command touching it, and only
// the control plane ever writes it.
const MemberKeyPrefix = "member/"

// MemberKey returns the registry key storing id's client-facing address.
func MemberKey(id string) string { return MemberKeyPrefix + id }

func isRegistryKey(key string) bool {
	return strings.HasPrefix(key, MemberKeyPrefix)
}

// JoinRequest is the wire payload of a JOIN control message carried on the raft
// transport (raft.TCPTransport control channel). ID is the joining node's id;
// under mTLS the sender's certificate identity (DNS SAN == node id) is
// authoritative and must match ID. Raft is the joining node's raft RPC address
// (so the leader can replicate to it) and Client is its data-plane address (so
// clients can be redirected to it).
type JoinRequest struct {
	ID     string `json:"id"`
	Raft   string `json:"raft"`
	Client string `json:"client"`
}

// joinRedirect is the reply a non-leader control handler returns: where to find
// the current leader's raft endpoint so the joiner can re-send the JOIN there.
type joinRedirect struct {
	Leader string `json:"leader"`
	Addr   string `json:"addr"`
}

// ClusterHandler is the client-facing handler on a raft-cluster node: the DATA
// plane only. It serves ordinary cache commands over the replicated store and
// deliberately rejects JOIN and any command that touches the reserved
// membership registry, so a cache client can neither mutate nor read cluster
// membership.
type ClusterHandler struct {
	base *CacheyHandler
}

// NewClusterHandler wraps the replicated store (cs) with the reserved-key and
// JOIN guards. Client leader redirects still work through cs.Leader() (the
// caller configures its leader resolver).
func NewClusterHandler(cs *ClusterStore) *ClusterHandler {
	return &ClusterHandler{base: NewCacheyHandler(cs)}
}

func (h *ClusterHandler) HandleRequest(data []byte) ([]byte, error) {
	cmd, err := protocol.DeSerializeCommand(data)
	if err != nil {
		return nil, err
	}
	switch cmd.Type {
	case protocol.JOIN:
		return nil, protocol.Statusf(protocol.CodeInvalidArgument, "JOIN is a control-plane command carried on the raft transport")
	case protocol.GET, protocol.PUT, protocol.DEL, protocol.TTL:
		if isRegistryKey(cmd.Key) {
			return nil, protocol.Statusf(protocol.CodeInvalidArgument, "key %q is reserved", cmd.Key)
		}
	}
	return h.base.HandleRequest(data)
}

// ControlHandler serves the cluster control plane over the raft transport: only
// the leader may add a member, and under mTLS the sender's certificate identity
// is bound to the claimed node id (a node cannot join as someone else).
type ControlHandler struct {
	rn *RaftNode
}

// NewControlHandler builds the control handler for a raft node. Register it on
// the node's transport with rn.Tr.SetControlHandler(h.HandleControl).
func NewControlHandler(rn *RaftNode) *ControlHandler {
	return &ControlHandler{rn: rn}
}

// HandleControl is the raft-transport control handler (peer identity, raw JSON
// JoinRequest). It adds the joining node when this node is the leader and
// returns either an acknowledgement or a redirect to the current leader's raft
// endpoint.
func (h *ControlHandler) HandleControl(peer string, data []byte) ([]byte, error) {
	var req JoinRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, errors.New("join: malformed request")
	}
	// Identity binding: under mTLS the sender's certificate identity (peer) is
	// the joining node; it must match the claimed id if one is given.
	id := req.ID
	if peer != "" {
		if req.ID != "" && req.ID != peer {
			return nil, errors.New("join: certificate identity " + peer + " does not match claimed id " + req.ID)
		}
		id = peer
	}
	if id == "" || req.Raft == "" || req.Client == "" {
		return nil, errors.New("join: malformed request")
	}

	n := h.rn.Node
	if !n.IsLeader() {
		return h.redirect()
	}
	if !isMember(n, id) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := n.AddServer(ctx, id, req.Raft)
		cancel()
		if err != nil {
			return nil, errors.New("join: add " + id + ": " + err.Error())
		}
	}
	if err := h.rn.CS.Put(MemberKey(id), req.Client); err != nil {
		return nil, errors.New("join: register " + id + ": " + err.Error())
	}
	return json.Marshal(map[string]string{"ok": id})
}

// redirect answers on a non-leader with the leader's raft endpoint (id + addr)
// so the joining node can re-send the JOIN to the actual leader.
func (h *ControlHandler) redirect() ([]byte, error) {
	leader := h.rn.Node.Leader()
	addr := ""
	if leader != "" {
		addr = h.rn.Tr.PeerAddrs()[leader]
	}
	if leader == "" || addr == "" {
		return nil, errors.New("join: no leader known")
	}
	return json.Marshal(joinRedirect{Leader: leader, Addr: addr})
}

// isMember reports whether id is a voting member of rn's cluster.
func isMember(n *raft.Node, id string) bool {
	for _, v := range n.Voters() {
		if v == id {
			return true
		}
	}
	return false
}
