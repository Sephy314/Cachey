package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"testing"
	"time"
)

func TestValidName(t *testing.T) {
	for _, name := range []string{"r1", "alice", "n1", "cachey-1", "a.b.example"} {
		if !ValidName(name) {
			t.Errorf("ValidName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "under_score", "-lead", "trail-", "a..b", "cachey://node/r1", "r 1"} {
		if ValidName(name) {
			t.Errorf("ValidName(%q) = true, want false", name)
		}
	}
}

func TestIdentityFromDNSName(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := ca.Issue("srv")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := Identity(leaf); got != "srv" {
		t.Fatalf("Identity = %q, want srv", got)
	}
}

// runServer starts an mTLS echo listener that admits clients passing accept,
// and returns its address. Each accepted connection reads one byte and echoes
// it back, so a client can tell acceptance from rejection by a round trip.
func runServer(t *testing.T, ca *CA, certPEM, keyPEM []byte, accept func(string) bool) string {
	t.Helper()
	cfg, err := Server(ca.CertPEM(), certPEM, keyPEM, accept)
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := tls.NewListener(raw, cfg)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				// The first Read drives the (lazy) handshake; a rejected or
				// unauthenticated client errors here and is never echoed.
				var b [1]byte
				if _, err := c.Read(b[:]); err != nil {
					return
				}
				c.Write(b[:])
			}(c)
		}
	}()
	return ln.Addr().String()
}

// tryRoundTrip dials with the given identity and performs one request/echo
// round trip. TLS 1.3 surfaces a server-side client-cert rejection only when
// the client first reads, so a successful handshake alone is not proof of
// acceptance — a full round trip is.
func tryRoundTrip(ca *CA, certPEM, keyPEM []byte, want, addr string) error {
	cfg, err := Client(ca.CertPEM(), certPEM, keyPEM, want)
	if err != nil {
		return err
	}
	conn, err := Dial("tcp", addr, cfg, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("x")); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		return err
	}
	if buf[0] != 'x' {
		return errors.New("echo mismatch")
	}
	return nil
}

func TestMTLSAllowlistAndPinning(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, err := ca.Issue("srv")
	if err != nil {
		t.Fatal(err)
	}
	aliceCert, aliceKey, _ := ca.Issue("alice")
	r1Cert, r1Key, _ := ca.Issue("r1")
	r2Cert, r2Key, _ := ca.Issue("r2")

	// A node listener that only admits node r1.
	addr := runServer(t, ca, srvCert, srvKey, func(identity string) bool {
		return identity == "r1"
	})

	if err := tryRoundTrip(ca, r1Cert, r1Key, "srv", addr); err != nil {
		t.Fatalf("r1 should be admitted and echo: %v", err)
	}
	if err := tryRoundTrip(ca, r2Cert, r2Key, "srv", addr); err == nil {
		t.Fatal("r2 should be rejected by the allowlist")
	}
	if err := tryRoundTrip(ca, aliceCert, aliceKey, "srv", addr); err == nil {
		t.Fatal("a client cert should be rejected by a node listener's allowlist")
	}
	if err := tryRoundTrip(ca, r1Cert, r1Key, "nope", addr); err == nil {
		t.Fatal("a mismatched server identity pin should fail hostname verification")
	}

	// A certificate from a foreign CA must not pass, even with a matching name.
	evilCA, _ := NewCA()
	evilCert, evilKey, _ := evilCA.Issue("r1")
	if err := tryRoundTrip(evilCA, evilCert, evilKey, "srv", addr); err == nil {
		t.Fatal("a foreign-CA certificate should be rejected")
	}
}

func TestClientWithoutCertificateRejected(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, _ := ca.Issue("srv")
	addr := runServer(t, ca, srvCert, srvKey, nil)

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	conn := tls.Client(raw, &tls.Config{
		RootCAs:    mustPool(t, ca),
		ServerName: "srv",
		MinVersion: tls.VersionTLS12,
	})
	if err := conn.Handshake(); err == nil {
		// TLS 1.3 completes the client handshake before the server rejects the
		// missing client certificate; the rejection then shows up on the first
		// read. Either way the client must not be able to exchange data.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("a client without a certificate should not be able to exchange data")
		}
	}
}

func TestPlaintextAgainstTLSListenerFails(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, _ := ca.Issue("srv")
	addr := runServer(t, ca, srvCert, srvKey, nil)

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := raw.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if _, err := raw.Read(buf); err == nil {
		t.Fatal("plaintext client should not get a usable response from a TLS listener")
	}
}

// TestMTLSRejectsExpiredCert pins that a same-CA client certificate that is no
// longer valid is refused even though its identity SAN is fine.
func TestMTLSRejectsExpiredCert(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, _ := ca.Issue("srv")
	now := time.Now()
	expiredCert, expiredKey, _ := ca.issue("r1", now.Add(-48*time.Hour), now.Add(-24*time.Hour), []string{"r1"})

	addr := runServer(t, ca, srvCert, srvKey, nil)
	if err := tryRoundTrip(ca, expiredCert, expiredKey, "srv", addr); err == nil {
		t.Fatal("an expired client certificate should be rejected")
	}
}

// TestMTLSRejectsCertWithoutIdentitySAN pins that a same-CA certificate signed
// for the right purpose but carrying no identity SAN is refused: a principal's
// identity must come from the SAN, so a bare certificate cannot authenticate.
func TestMTLSRejectsCertWithoutIdentitySAN(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, _ := ca.Issue("srv")
	noSANCert, noSANKey, _ := ca.issue("r1", time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil)

	addr := runServer(t, ca, srvCert, srvKey, nil)
	if err := tryRoundTrip(ca, noSANCert, noSANKey, "srv", addr); err == nil {
		t.Fatal("a certificate without an identity SAN should be rejected")
	}
}

func mustPool(t *testing.T, ca *CA) *x509.CertPool {
	t.Helper()
	p, err := pool(ca.CertPEM())
	if err != nil {
		t.Fatal(err)
	}
	return p
}
