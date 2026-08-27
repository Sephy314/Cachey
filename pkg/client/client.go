package client

import (
	"bufio"
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
	_, err = c.conn.Write(data)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(c.conn)
	response, err := reader.ReadString('\n')

	if err != nil && err.Error() != "EOF" {
		return nil, err
	}

	response = strings.TrimSpace(response)
	return &response, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
