package server

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/store"
)

func TestServerProcessesLineDelimitedCommands(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewCacheyHandler(store.NewCacheyStore()))
	if err := server.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer server.Stop()

	conn, err := net.DialTimeout("tcp", server.ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("DialTimeout() error = %v", err)
	}
	defer conn.Close()

	command := protocol.Command{Type: protocol.PUT, Key: "key", Val: "value"}
	data, err := command.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if _, err := conn.Write(append(data, '\n')); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString() error = %v", err)
	}
	if !strings.HasSuffix(response, "\n") {
		t.Fatalf("response = %q, want newline-delimited response", response)
	}
	got, err := protocol.DeSerializeCommand([]byte(strings.TrimSuffix(response, "\n")))
	if err != nil {
		t.Fatalf("DeSerializeCommand() error = %v", err)
	}
	if got.Key != command.Key || got.Val != command.Val {
		t.Fatalf("response = %#v, want key/value %#v", got, command)
	}
}

// TestServerRespondsWithStatusOnError verifies that failed commands get a
// gRPC-style status response instead of being dropped (which would hang the
// client).
func TestServerRespondsWithStatusOnError(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewCacheyHandler(store.NewCacheyStore()))
	if err := server.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer server.Stop()

	conn, err := net.DialTimeout("tcp", server.ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("DialTimeout() error = %v", err)
	}
	defer conn.Close()

	// GET a missing key → the server must reply with NotFound, not drop it.
	command := protocol.Command{Type: protocol.GET, Key: "missing"}
	data, err := command.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	if _, err := conn.Write(append(data, '\n')); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString() error = %v (server must respond with a status, not drop the request)", err)
	}
	var st protocol.Status
	if err := json.Unmarshal([]byte(strings.TrimSpace(response)), &st); err != nil {
		t.Fatalf("response %q is not a status: %v", response, err)
	}
	if st.Code != protocol.CodeNotFound {
		t.Fatalf("status code = %d, want %d (NotFound)", st.Code, protocol.CodeNotFound)
	}
	if st.Message == "" {
		t.Fatalf("status message is empty")
	}
}
