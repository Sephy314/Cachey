package server

import (
	"log"

	"github.com/Sephy314/Cachey/internal/protocol"
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
			return nil, err
		}
		cmd.Val = *value
	case protocol.PUT:
		err := h.store.Put(cmd.Key, cmd.Val)
		if err != nil {
			log.Default().Printf("Error putting key %s: %v", cmd.Key, err)
			return nil, err
		}
	case protocol.DEL:
		err := h.store.Delete(cmd.Key)
		if err != nil {
			log.Default().Printf("Error deleting key %s: %v", cmd.Key, err)
			return nil, err
		}
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
