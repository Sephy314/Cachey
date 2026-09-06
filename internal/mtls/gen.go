package mtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

// CA is a self-signed certificate authority for minting identity certificates.
// Tests use it to build throwaway CAs and role certificates; operators can use
// it too, though cacheyd does not expose a generate command — production
// deployments are expected to bring their own CA and certificates.
type CA struct {
	certPEM, keyPEM []byte
	key             *ecdsa.PrivateKey
	cert            *x509.Certificate
}

// NewCA creates a fresh self-signed CA valid for ten years.
func NewCA() (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Cachey CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{certPEM: certPEM, keyPEM: keyPEM, key: key, cert: cert}, nil
}

// CertPEM returns the CA certificate in PEM form.
func (ca *CA) CertPEM() []byte { return ca.certPEM }

// Issue mints a leaf certificate whose DNS SAN is identity — the principal's
// name (a consensus node id, cache server name, or cache client name). The
// leaf carries both server and client EKUs; which role it may actually play is
// decided by the allowlists at each listener.
func (ca *CA) Issue(identity string) (certPEM, keyPEM []byte, err error) {
	now := time.Now()
	return ca.issue(identity, now.Add(-time.Hour), now.Add(10*365*24*time.Hour), []string{identity})
}

// issue is Issue with an explicit validity window and DNS SAN list. Tests use
// it to mint certificates that are expired or that carry no identity SAN.
func (ca *CA) issue(identity string, notBefore, notAfter time.Time, dnsNames []string) (certPEM, keyPEM []byte, err error) {
	if !ValidName(identity) {
		return nil, nil, fmt.Errorf("mtls: identity %q is not a valid DNS name", identity)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: identity},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}
