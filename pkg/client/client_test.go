package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/Sephy314/Cachey/internal/protocol"
)

func TestSendCommandUsesLineDelimitedJSON(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &Client{conn: clientConn}
	command := protocol.Command{Type: protocol.GET, Key: "key"}
	serverDone := make(chan error, 1)
	go func() {
		request, err := bufio.NewReader(serverConn).ReadString('\n')
		if err != nil {
			serverDone <- err
			return
		}
		if request != `{"Type":"GET","Key":"key","Val":""}`+"\n" {
			serverDone <- fmt.Errorf("request = %q, want newline-delimited command", request)
			return
		}
		_, err = serverConn.Write([]byte(`{"Type":"GET","Key":"key","Val":"value"}` + "\n"))
		serverDone <- err
	}()

	response, err := client.SendCommand(command)
	if err != nil {
		t.Fatalf("SendCommand() error = %v", err)
	}
	if *response != `{"Type":"GET","Key":"key","Val":"value"}` {
		t.Fatalf("SendCommand() = %q, want JSON response", *response)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

// TestSendCommandSurfacesStatusError verifies that a gRPC-style status
// response is returned as an error carrying the code.
func TestSendCommandSurfacesStatusError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &Client{conn: clientConn}
	command := protocol.Command{Type: protocol.GET, Key: "missing"}
	serverDone := make(chan error, 1)
	go func() {
		if _, err := bufio.NewReader(serverConn).ReadString('\n'); err != nil {
			serverDone <- err
			return
		}
		_, err := serverConn.Write([]byte(`{"code":5,"message":"invalid key"}` + "\n"))
		serverDone <- err
	}()

	_, err := client.SendCommand(command)
	if err == nil {
		t.Fatal("SendCommand() = nil error, want a status error")
	}
	var st *protocol.Status
	if !errors.As(err, &st) {
		t.Fatalf("error = %v, want *protocol.Status", err)
	}
	if st.Code != protocol.CodeNotFound {
		t.Fatalf("code = %d, want %d (NotFound)", st.Code, protocol.CodeNotFound)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
