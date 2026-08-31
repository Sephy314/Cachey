package client

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"

	"github.com/Sephy314/Cachey/internal/protocol"
)

type Client struct {
	conn net.Conn
}

func NewClient(address string) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) SendCommand(cmd protocol.Command) (*string, error) {
	data, err := cmd.Serialize()
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	if _, err := c.conn.Write(data); err != nil {
		return nil, err
	}

	response, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	response = strings.TrimSpace(response)

	// A gRPC-style status response (e.g. {"code":5,"message":"..."}) means the
	// command failed; command responses never carry a non-zero code.
	var st protocol.Status
	if json.Unmarshal([]byte(response), &st) == nil && st.Code != 0 {
		return nil, &st
	}
	return &response, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
