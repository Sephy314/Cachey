package server

import (
	"errors"
	"log"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/raft"
	"github.com/Sephy314/Cachey/internal/store"
)

type Handler interface {
	HandleRequest(data []byte) ([]byte, error)
}

type CacheyHandler struct {
	store store.Store
}

func NewCacheyHandler(store store.Store) *CacheyHandler {
	return &CacheyHandler{
		store: store,
	}
}

// redirectOr converts a raft.ErrNotLeader into a redirect status carrying the
// current leader's address so the client can reconnect there. Any other error
// passes through unchanged.
func (h *CacheyHandler) redirectOr(err error) error {
	if errors.Is(err, raft.ErrNotLeader) {
		if leader := h.store.Leader(); leader != "" {
			return protocol.Statusf(protocol.CodeUnavailable, "not leader: %s", leader)
		}
		return protocol.Statusf(protocol.CodeUnavailable, "not leader")
	}
	return err
}

func (h *CacheyHandler) HandleRequest(data []byte) ([]byte, error) {
	cmd, err := protocol.DeSerializeCommand(data)
	if err != nil {
		log.Default().Printf("Error deserializing command: %v", err)
		return nil, err
	}

	log.Default().Printf("Received command: Type=%s, Key=%s, Val=%s", cmd.Type, cmd.Key, cmd.Val)

	switch cmd.Type {
	case protocol.GET:
		value, err := h.store.Get(cmd.Key)
		if err != nil {
			log.Default().Printf("Error getting key %s: %v", cmd.Key, err)
			return nil, h.redirectOr(err)
		}
		cmd.Val = *value
	case protocol.PUT:
		err := h.store.Put(cmd.Key, cmd.Val)
		if err != nil {
			log.Default().Printf("Error putting key %s: %v", cmd.Key, err)
			return nil, h.redirectOr(err)
		}
	case protocol.DEL:
		err := h.store.Delete(cmd.Key)
		if err != nil {
			log.Default().Printf("Error deleting key %s: %v", cmd.Key, err)
			return nil, h.redirectOr(err)
		}

	case protocol.TTL:
		err := h.store.TTL(cmd.Key, cmd.TTL)
		if err != nil {
			log.Default().Printf("Error setting TTL for key %s: %v", cmd.Key, err)
			return nil, h.redirectOr(err)
		}

	case protocol.ALV:
		cmd.Val = h.store.Alive()

	default:
		log.Default().Printf("Invalid command type: %s", cmd.Type)
		return nil, ErrorCodeInvalidCommand
	}

	respData, err := cmd.Serialize()
	if err != nil {
		log.Default().Printf("Error serializing response: %v", err)
		return nil, err
	}

	log.Default().Printf("Sending response: %s", string(respData))

	return respData, nil
}
