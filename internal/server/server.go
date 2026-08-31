package server

import (
	"bufio"
	"errors"
	"log"
	"net"
)

type Server struct {
	addr string
	ln   net.Listener
	hdlr Handler
}

type HandlerInterface interface {
	HandleRequest(data []byte) ([]byte, error)
}

func NewServer(addr string, hdlr Handler) *Server {
	return &Server{
		addr: addr,
		hdlr: hdlr,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
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
