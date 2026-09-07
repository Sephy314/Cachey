package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
)

// Replicated registry keys that map a node id to its two addresses, written by
// the leader when a node bootstraps or joins:
//
//	member/<id>   client-facing address (data-plane leader redirects)
//	control/<id>  control-plane address  (JOIN redirects)
//
// Every node's FSM keeps the full table. The prefixes are reserved: the client
// data handler rejects any command touching them, and only the control plane
// ever writes them.
const (
	MemberKeyPrefix  = "member/"
	ControlKeyPrefix = "control/"
)

// MemberKey returns the registry key storing id's client-facing address.
func MemberKey(id string) string { return MemberKeyPrefix + id }

// ControlKey returns the registry key storing id's control-plane address.
func ControlKey(id string) string { return ControlKeyPrefix + id }

func isRegistryKey(key string) bool {
	return strings.HasPrefix(key, MemberKeyPrefix) || strings.HasPrefix(key, ControlKeyPrefix)
}

// JoinRequest is the payload of a JOIN control command: the joining node's raft
// RPC address (so the leader can replicate to it), its client address (data
// plane) and its control address (control plane).
type JoinRequest struct {
	Raft    string `json:"raft"`
	Client  string `json:"client"`
	Control string `json:"control"`
}

// ClusterHandler is the client-facing handler on a raft-cluster node: the DATA
// plane only. It serves ordinary cache commands over the replicated store and
// deliberately rejects JOIN and any command that touches the reserved
// membership registry keys, so a cache client can neither mutate nor even read
// cluster membership.
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
		return nil, protocol.Statusf(protocol.CodeInvalidArgument, "JOIN is a control-plane command; use the control endpoint")
	case protocol.GET, protocol.PUT, protocol.DEL, protocol.TTL:
		if isRegistryKey(cmd.Key) {
			return nil, protocol.Statusf(protocol.CodeInvalidArgument, "key %q is reserved", cmd.Key)
		}
	}
	return h.base.HandleRequest(data)
}

// ControlHandler is the handler on a cluster node's control endpoint. It serves
// JOIN (the only membership-changing operation) and rejects everything else.
//
// ponytail: in plaintext development mode this separate listener is the
// authorization boundary, not real authentication — anyone who can reach the
// control endpoint may JOIN. The intended upgrade (node-to-node mTLS) binds
// the control plane to node identity (certificate SAN == node id == member),
// which is also when JOIN moves onto the raft transport.
type ControlHandler struct {
	node *raft.Node
	cs   *ClusterStore
	reg  *store.CacheyStore // local FSM registry (control addresses for redirects)
}

// NewControlHandler builds the control-plane handler. cs is the replicated
// store (JOIN registers members through it on the leader) and reg is this
// node's local FSM, used to read the registry for leader redirects.
func NewControlHandler(node *raft.Node, cs *ClusterStore, reg *store.CacheyStore) *ControlHandler {
	return &ControlHandler{node: node, cs: cs, reg: reg}
}

func (h *ControlHandler) HandleRequest(data []byte) ([]byte, error) {
	cmd, err := protocol.DeSerializeCommand(data)
	if err != nil {
		return nil, err
	}
	if cmd.Type != protocol.JOIN {
		return nil, protocol.Statusf(protocol.CodeInvalidArgument, "control endpoint accepts only JOIN")
	}
	return h.handleJoin(cmd)
}

// handleJoin runs on the node that received a JOIN. Only the raft leader may
// add the newcomer: it calls raft.AddServer (idempotent for an existing
// member, which just re-announces) and writes the member's client and control
// addresses into the replicated registry. A non-leader answers with a redirect
// to the leader's CONTROL address so the joining process retries against the
// actual leader.
func (h *ControlHandler) handleJoin(cmd *protocol.Command) ([]byte, error) {
	var req JoinRequest
	if cmd.Key == "" || json.Unmarshal([]byte(cmd.Val), &req) != nil ||
		req.Raft == "" || req.Client == "" || req.Control == "" {
		return nil, protocol.Statusf(protocol.CodeInvalidArgument, "malformed join request")
	}
	if !h.node.IsLeader() {
		return nil, h.redirect()
	}
	already := false
	for _, id := range h.node.Voters() {
		if id == cmd.Key {
			already = true
			break
		}
	}
	if !already {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := h.node.AddServer(ctx, cmd.Key, req.Raft)
		cancel()
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				return nil, h.redirect()
			}
			return nil, protocol.Statusf(protocol.CodeInternal, "add %s: %v", cmd.Key, err)
		}
	}
	if err := h.cs.Put(MemberKey(cmd.Key), req.Client); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, h.redirect()
		}
		return nil, protocol.Statusf(protocol.CodeInternal, "register %s: %v", cmd.Key, err)
	}
	if err := h.cs.Put(ControlKey(cmd.Key), req.Control); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, h.redirect()
		}
		return nil, protocol.Statusf(protocol.CodeInternal, "register control %s: %v", cmd.Key, err)
	}
	// Echo the JOIN back as the acknowledgement.
	return cmd.Serialize()
}

// redirect builds the leader-redirect status this control endpoint answers
// with when it is not the leader. The hint carries the leader's CONTROL
// address when the registry knows it.
func (h *ControlHandler) redirect() error {
	leader := h.node.Leader()
	if leader == "" {
		return protocol.Statusf(protocol.CodeUnavailable, "not leader")
	}
	if v, err := h.reg.Get(ControlKey(leader)); err == nil && *v != "" {
		return protocol.Statusf(protocol.CodeUnavailable, "not leader: %s", *v)
	}
	return protocol.Statusf(protocol.CodeUnavailable, "not leader")
}
