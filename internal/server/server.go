package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"log"
	"net"
)

type Server struct {
	addr      string
	ln        net.Listener
	hdlr      Handler
	tlsConfig *tls.Config
}

type HandlerInterface interface {
	HandleRequest(data []byte) ([]byte, error)
}

// Option configures a Server.
type Option func(*Server)

// WithTLSConfig serves client connections over TLS (typically mutual TLS; see
// internal/mtls.Server for a ready-made config). The default — no option — is
// plaintext, which is intended for tests and local development; the cacheyd
// binary defaults to TLS in production.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(s *Server) { s.tlsConfig = cfg }
}

func NewServer(addr string, hdlr Handler, opts ...Option) *Server {
	s := &Server{
		addr: addr,
		hdlr: hdlr,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.ln = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("Error accepting connection: %v", err)
				continue
			}

			log.Printf(
				"Accepted connection from %s",
				conn.RemoteAddr(),
			)

			go func(conn net.Conn) {
				defer conn.Close()

				scanner := bufio.NewScanner(conn)

				for scanner.Scan() {
					data := scanner.Bytes()

					log.Printf(
						"Received data: %s",
						string(data),
					)

					resp, err := s.hdlr.HandleRequest(data)
					if err != nil {
						log.Printf(
							"Error handling request: %v",
							err,
						)
						// Reply with a gRPC-style status instead of dropping
						// the request so clients never wait forever.
						resp = statusBytes(err)
					}

					if _, err := conn.Write(append(resp, '\n')); err != nil {
						log.Printf(
							"Error writing response: %v",
							err,
						)
						return
					}
				}

				if err := scanner.Err(); err != nil {
					log.Printf(
						"Error reading from connection: %v",
						err,
					)
				}
			}(conn)
		}
	}()

	return nil
}
func (s *Server) Stop() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// Addr returns the bound listen address ("" before Start or if not listening).
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return ""
}
