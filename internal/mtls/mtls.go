// Package mtls implements Cachey's mutual-TLS (mTLS) transport authentication:
// cache clients authenticate to a cache server, and consensus nodes
// (Raft) authenticate to one another.
//
// Identity model. A principal's identity is its certificate's DNS
// subjectAltName (SAN) — never CN, which X.509 has deprecated for identity. A
// consensus node's identity is its node id, a cache client's identity is its
// client name. DNS SANs are used deliberately rather than URI SANs: Go TLS
// clients must set ServerName (a client config with an empty ServerName and
// InsecureSkipVerify unset is rejected outright), and ServerName is verified
// against the peer's DNS SANs. A dialer therefore sets ServerName to the
// identity it expects and Go's own chain + hostname verification performs the
// identity pinning — no hand-rolled verification, no InsecureSkipVerify.
//
// One operator-issued CA signs every certificate. What a certificate may reach
// is decided by allowlists at each listener: a node listener admits only
// certificates whose SAN is a known node id, and the cache-server listener
// admits only SANs on its client allowlist. Operators must not issue a client
// whose name collides with a node id; separate CAs per role are the upgrade
// path if that becomes a concern.
//
// Verification. No config built here ever sets InsecureSkipVerify. Listeners
// use ClientAuth = RequireAndVerifyClientCert, so Go verifies the client chain
// against the CA first and only then invokes VerifyPeerCertificate, which is
// used solely to apply the allowlist on top of that chain check. Dialers rely
// on Go's chain + hostname verification with ServerName = expected identity.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// ValidName reports whether name is usable as a DNS-SAN identity: a non-empty
// DNS hostname with no underscores, no leading/trailing/empty labels. Names
// are matched literally (they appear verbatim in ServerName), so they must be
// unambiguous hostnames.
func ValidName(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if !validLabel(label) {
			return false
		}
	}
	return true
}

func validLabel(label string) bool {
	if len(label) == 0 || len(label) > 63 {
		return false
	}
	for i, r := range label {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' && i > 0 && i < len(label)-1:
		default:
			return false
		}
	}
	return true
}

// Identity returns the DNS SAN identity carried by cert, or "" if it has none.
func Identity(cert *x509.Certificate) string {
	if len(cert.DNSNames) == 0 {
		return ""
	}
	return cert.DNSNames[0]
}

// PeerIdentity returns the DNS SAN identity of the remote peer of an
// established TLS connection, or "" when conn is not TLS or the peer presented
// no identity SAN. Call after the handshake completes.
func PeerIdentity(conn net.Conn) string {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return ""
	}
	st := tc.ConnectionState()
	if len(st.PeerCertificates) == 0 {
		return ""
	}
	return Identity(st.PeerCertificates[0])
}

// Server returns a *tls.Config for an mTLS listener. Every client must present
// a certificate signed by caPEM (ClientAuth = RequireAndVerifyClientCert) and —
// via VerifyPeerCertificate, which Go runs only after its own chain check —
// satisfy accept(identity) on the client's DNS SAN. A nil accept accepts any
// CA-signed certificate. certPEM/keyPEM identify this listener.
func Server(caPEM, certPEM, keyPEM []byte, accept func(identity string) bool) (*tls.Config, error) {
	roots, err := pool(caPEM)
	if err != nil {
		return nil, err
	}
	keypair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("mtls: load key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{keypair},
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("mtls: parse client cert: %w", err)
			}
			ident := Identity(leaf)
			if ident == "" {
				return errors.New("mtls: client certificate has no identity SAN")
			}
			if accept != nil && !accept(ident) {
				return fmt.Errorf("mtls: client identity %q not allowed", ident)
			}
			return nil
		},
	}, nil
}

// Client returns a *tls.Config for dialing the peer expected to hold identity
// want. ServerName is set to want, so Go's standard chain + hostname
// verification pins the peer: it must present a certificate signed by caPEM
// whose DNS SAN is want. certPEM/keyPEM identify this caller to the peer.
func Client(caPEM, certPEM, keyPEM []byte, want string) (*tls.Config, error) {
	if !ValidName(want) {
		return nil, fmt.Errorf("mtls: expected peer identity %q is not a valid name", want)
	}
	roots, err := pool(caPEM)
	if err != nil {
		return nil, err
	}
	keypair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("mtls: load key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{keypair},
		RootCAs:      roots,
		ServerName:   want,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// Dial opens a TCP connection to addr and completes a TLS handshake with cfg,
// returning the established connection. It performs the handshake explicitly
// (rather than via tls.Dial) so the caller controls the dial timeout and sees
// handshake failures as errors at dial time. A zero timeout means no timeout.
func Dial(network, addr string, cfg *tls.Config, timeout time.Duration) (*tls.Conn, error) {
	raw, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, cfg)
	if err := conn.Handshake(); err != nil {
		raw.Close()
		return nil, err
	}
	return conn, nil
}

// pool builds a CA pool from a PEM bundle.
func pool(caPEM []byte) (*x509.CertPool, error) {
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("mtls: no certificates in CA PEM")
	}
	return p, nil
}
