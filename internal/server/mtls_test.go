package server

import (
	"errors"
	"strings"
	"testing"

	"github.com/Sephy314/Cachey/internal/mtls"
	"github.com/Sephy314/Cachey/internal/mtls/testca"
	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/internal/store"
	"github.com/Sephy314/Cachey/pkg/client"
)

// TestServerMTLS exercises the cache protocol over mutual TLS end to end: a
// client on the server's allowlist reads and writes, while an off-allowlist
// client of the same CA, a plaintext client, and a client pinning the wrong
// server identity cannot exchange data.
func TestServerMTLS(t *testing.T) {
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, err := ca.Issue("srv")
	if err != nil {
		t.Fatal(err)
	}
	aliceCert, aliceKey, _ := ca.Issue("alice")
	bobCert, bobKey, _ := ca.Issue("bob")

	cfg, err := mtls.Server(ca.CertPEM(), srvCert, srvKey, func(id string) bool {
		return id == "alice"
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("127.0.0.1:0", NewCacheyHandler(store.NewCacheyStore()), WithTLSConfig(cfg))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	newClient := func(certPEM, keyPEM []byte, want string) *client.Client {
		ccfg, err := mtls.Client(ca.CertPEM(), certPEM, keyPEM, want)
		if err != nil {
			t.Fatal(err)
		}
		c, err := client.NewClient(srv.Addr(), client.WithTLSConfig(ccfg))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	put := func(c *client.Client, k, v string) bool {
		resp, err := c.SendCommand(protocol.Command{Type: protocol.PUT, Key: k, Val: v})
		return err == nil && resp != nil && *resp != ""
	}
	get := func(c *client.Client, k string) string {
		resp, err := c.SendCommand(protocol.Command{Type: protocol.GET, Key: k})
		if err != nil || resp == nil {
			return ""
		}
		return *resp
	}

	alice := newClient(aliceCert, aliceKey, "srv")
	if !put(alice, "k", "v") {
		t.Fatal("alice should be able to PUT over mTLS")
	}
	if resp := get(alice, "k"); !strings.Contains(resp, `"v"`) {
		t.Fatalf("alice GET did not return the value: %q", resp)
	}

	// bob is signed by the same CA but is not on the allowlist: his connection
	// cannot carry data (TLS 1.3 surfaces the server's rejection on first I/O,
	// not at the client handshake).
	bob := newClient(bobCert, bobKey, "srv")
	if put(bob, "k2", "x") {
		t.Fatal("bob should not be able to write")
	}

	// A plaintext client cannot talk to a TLS listener.
	raw, err := client.NewClient(srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	if put(raw, "k3", "x") {
		t.Fatal("a plaintext client should not be able to write")
	}

	// A client pinning the wrong server identity fails at dial time.
	evilCfg, err := mtls.Client(ca.CertPEM(), aliceCert, aliceKey, "evil")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.NewClient(srv.Addr(), client.WithTLSConfig(evilCfg)); err == nil {
		t.Fatal("a client pinning the wrong server identity should fail to dial")
	}
}

// startMTLSServer boots a single-node cache server over mTLS that admits only
// the given client identities (server identity "srv") and returns its address
// plus the signing CA.
func startMTLSServer(t *testing.T, allowed ...string) (addr string, ca *testca.CA) {
	t.Helper()
	ca, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, err := ca.Issue("srv")
	if err != nil {
		t.Fatal(err)
	}
	allow := make(map[string]bool, len(allowed))
	for _, id := range allowed {
		allow[id] = true
	}
	cfg, err := mtls.Server(ca.CertPEM(), srvCert, srvKey, func(id string) bool {
		return allow[id]
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("127.0.0.1:0", NewCacheyHandler(store.NewCacheyStore()), WithTLSConfig(cfg))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })
	return srv.Addr(), ca
}

// mtlsConnect dials the mTLS server at addr (pinning server identity "srv")
// while presenting cert/key. It returns the dial error so callers can treat a
// handshake-time rejection (which TLS 1.2 surfaces) the same as a
// post-handshake one (which TLS 1.3 surfaces on first I/O).
func mtlsConnect(ca *testca.CA, certPEM, keyPEM []byte, addr string) (*client.Client, error) {
	ccfg, err := mtls.Client(ca.CertPEM(), certPEM, keyPEM, "srv")
	if err != nil {
		return nil, err
	}
	return client.NewClient(addr, client.WithTLSConfig(ccfg))
}

// mtlsPut reports whether a PUT got a successful (non-empty) response.
func mtlsPut(c *client.Client, k, v string) bool {
	resp, err := c.SendCommand(protocol.Command{Type: protocol.PUT, Key: k, Val: v})
	return err == nil && resp != nil && *resp != ""
}

// TestServerMTLSRejectsForeignCA: a client whose certificate is signed by an
// untrusted CA cannot write even with an otherwise-allowed identity.
func TestServerMTLSRejectsForeignCA(t *testing.T) {
	addr, ca := startMTLSServer(t, "alice")

	foreign, err := testca.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	fCert, fKey, err := foreign.Issue("alice")
	if err != nil {
		t.Fatal(err)
	}
	// The client trusts the server's CA to verify the server, but presents a
	// certificate the server's CA did not sign.
	c, err := mtlsConnect(ca, fCert, fKey, addr)
	if err == nil {
		defer c.Close()
		if mtlsPut(c, "k", "v") {
			t.Fatal("a foreign-CA client should not be able to write")
		}
	}
}

// TestServerMTLSProtocolErrorsSurvive pins that protocol-level errors over a
// valid mTLS session are answered with status errors and do not tear the
// connection down: the same session keeps serving valid commands afterwards.
func TestServerMTLSProtocolErrorsSurvive(t *testing.T) {
	addr, ca := startMTLSServer(t, "alice")
	aliceCert, aliceKey, err := ca.Issue("alice")
	if err != nil {
		t.Fatal(err)
	}
	c, err := mtlsConnect(ca, aliceCert, aliceKey, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Unknown command type → Unimplemented.
	_, err = c.SendCommand(protocol.Command{Type: protocol.CommandType("BOGUS")})
	var st *protocol.Status
	if !errors.As(err, &st) || st.Code != protocol.CodeUnimplemented {
		t.Fatalf("invalid command error = %v, want CodeUnimplemented", err)
	}
	// GET of a missing key → NotFound.
	_, err = c.SendCommand(protocol.Command{Type: protocol.GET, Key: "missing"})
	if !errors.As(err, &st) || st.Code != protocol.CodeNotFound {
		t.Fatalf("missing-key GET error = %v, want CodeNotFound", err)
	}
	// The same mTLS connection is still usable.
	if !mtlsPut(c, "k", "v") {
		t.Fatal("connection unusable after protocol errors")
	}
	resp, err := c.SendCommand(protocol.Command{Type: protocol.GET, Key: "k"})
	if err != nil {
		t.Fatalf("GET after protocol errors: %v", err)
	}
	if resp == nil || !strings.Contains(*resp, `"v"`) {
		t.Fatalf("GET after protocol errors returned %q", deref(resp))
	}
}

// TestServerMTLSIsolation: a rejected client does not take the server down — an
// allowed client can still write afterwards.
func TestServerMTLSIsolation(t *testing.T) {
	addr, ca := startMTLSServer(t, "alice")
	aliceCert, aliceKey, _ := ca.Issue("alice")
	bobCert, bobKey, _ := ca.Issue("bob")

	alice, err := mtlsConnect(ca, aliceCert, aliceKey, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	bob, err := mtlsConnect(ca, bobCert, bobKey, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close()

	if mtlsPut(bob, "k2", "x") {
		t.Fatal("bob should be rejected")
	}
	if !mtlsPut(alice, "k", "v") {
		t.Fatal("alice should still be served after a rejected peer")
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
