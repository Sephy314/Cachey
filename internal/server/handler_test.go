package server

import (
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/store"
)

func TestCacheyHandlerCommands(t *testing.T) {
	handler := NewCacheyHandler(store.NewCacheyStore())

	putResponse := handleCommand(t, handler, protocol.Command{Type: protocol.PUT, Key: "key", Val: "value"})
	if putResponse.Val != "value" {
		t.Fatalf("PUT response value = %q, want %q", putResponse.Val, "value")
	}

	getResponse := handleCommand(t, handler, protocol.Command{Type: protocol.GET, Key: "key"})
	if getResponse.Val != "value" {
		t.Fatalf("GET response value = %q, want %q", getResponse.Val, "value")
	}

	handleCommand(t, handler, protocol.Command{Type: protocol.DEL, Key: "key"})
	if _, err := handler.HandleRequest([]byte(`{"Type":"GET","Key":"key"}`)); err != store.ErrorCodeInvalidKey {
		t.Fatalf("GET after DEL error = %v, want %v", err, store.ErrorCodeInvalidKey)
	}
}

func TestCacheyHandlerRejectsInvalidCommands(t *testing.T) {
	handler := NewCacheyHandler(store.NewCacheyStore())

	if _, err := handler.HandleRequest(nil); err != protocol.ErrorCodeInvalidCommand {
		t.Errorf("HandleRequest(nil) error = %v, want %v", err, protocol.ErrorCodeInvalidCommand)
	}
	if _, err := handler.HandleRequest([]byte(`{"Type":"NOPE"}`)); err != ErrorCodeInvalidCommand {
		t.Errorf("HandleRequest(unknown type) error = %v, want %v", err, ErrorCodeInvalidCommand)
	}
}

func TestCacheyHandlerTTLCommand(t *testing.T) {
	handler := NewCacheyHandler(store.NewCacheyStore())

	handleCommand(t, handler, protocol.Command{Type: protocol.PUT, Key: "key", Val: "value"})
	handleCommand(t, handler, protocol.Command{Type: protocol.TTL, Key: "key", TTL: 20})

	getResponse := handleCommand(t, handler, protocol.Command{Type: protocol.GET, Key: "key"})
	if getResponse.Val != "value" {
		t.Fatalf("GET before TTL expiry = %q, want %q", getResponse.Val, "value")
	}

	time.Sleep(30 * time.Millisecond)
	if _, err := handler.HandleRequest(mustSerialize(t, protocol.Command{Type: protocol.GET, Key: "key"})); err != store.ErrorCodeInvalidKey {
		t.Fatalf("GET after TTL expiry error = %v, want %v", err, store.ErrorCodeInvalidKey)
	}
}

func mustSerialize(t *testing.T, cmd protocol.Command) []byte {
	t.Helper()
	data, err := cmd.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	return data
}

func handleCommand(t *testing.T, handler *CacheyHandler, command protocol.Command) protocol.Command {
	t.Helper()
	data, err := command.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	responseData, err := handler.HandleRequest(data)
	if err != nil {
		t.Fatalf("HandleRequest() error = %v", err)
	}
	response, err := protocol.DeSerializeCommand(responseData)
	if err != nil {
		t.Fatalf("DeSerializeCommand() error = %v", err)
	}
	return *response
}
