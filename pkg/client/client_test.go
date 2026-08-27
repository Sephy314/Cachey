package client

import (
	"bufio"
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
