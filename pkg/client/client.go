package client

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"strings"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/protocol"
)

type Client struct {
	conn net.Conn
}

type options struct {
	tlsConfig *tls.Config
}

// Option configures a Client.
type Option func(*options)

// WithTLSConfig authenticates the server and this client over (mutual) TLS
// using cfg — typically built with internal/mtls.Client, which pins the
// expected server identity. The default (no option) is plaintext.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

func NewClient(address string, opts ...Option) (*Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	var (
		conn net.Conn
		err  error
	)
	if o.tlsConfig != nil {
		conn, err = mtls.Dial("tcp", address, o.tlsConfig, 0)
	} else {
		conn, err = net.Dial("tcp", address)
	}
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

// RedirectLeader extracts the leader's address from a redirect error returned
// by SendCommand, if the server indicated one (a CodeUnavailable status whose
// message is "not leader: <addr>"). Returns ok=false otherwise.
func RedirectLeader(err error) (string, bool) {
	var st *protocol.Status
	if !errors.As(err, &st) || st.Code != protocol.CodeUnavailable {
		return "", false
	}
	addr, found := strings.CutPrefix(st.Message, "not leader: ")
	if !found || addr == "" {
		return "", false
	}
	return addr, true
}
