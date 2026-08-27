package server

import (
	"bufio"
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
