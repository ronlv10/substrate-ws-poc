package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"
)

// certMinter terminates TLS for whatever hostname an actor believes it is
// dialing (slack.com, wss-primary.slack.com, ...). On each handshake it mints a
// leaf certificate for the requested SNI signed by the broker CA. Because the
// broker CA is trusted by actors (mounted into the sandbox by atelet, see
// ATE_ACTOR_CA_BUNDLE), the actor's TLS validation succeeds and it proceeds as
// if it reached real Slack. This is the same technique mitmproxy uses.
type certMinter struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey

	mu    sync.Mutex
	cache map[string]*tls.Certificate

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// newCertMinterFromFiles loads a PEM CA certificate and its EC private key.
func newCertMinterFromFiles(certPath, keyPath string) (*certMinter, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA cert %q: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA key %q: %w", keyPath, err)
	}
	return newCertMinter(certPEM, keyPEM)
}

// newCertMinter builds a minter from PEM-encoded CA material.
func newCertMinter(certPEM, keyPEM []byte) (*certMinter, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("CA cert PEM: no PEM block found")
	}
	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing CA cert: %w", err)
	}
	if !caCert.IsCA {
		return nil, fmt.Errorf("CA cert is not a CA (BasicConstraints CA=false)")
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("CA key PEM: no PEM block found")
	}
	caKey, err := parseECKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing CA key: %w", err)
	}
	return &certMinter{
		caCert: caCert,
		caKey:  caKey,
		cache:  make(map[string]*tls.Certificate),
		now:    time.Now,
	}, nil
}

func parseECKey(der []byte) (*ecdsa.PrivateKey, error) {
	if k, err := x509.ParseECPrivateKey(der); err == nil {
		return k, nil
	}
	// Fall back to PKCS#8 (openssl's default for `openssl genpkey`).
	k, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, err
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("CA key is %T, want *ecdsa.PrivateKey", k)
	}
	return ec, nil
}

// TLSConfig returns a *tls.Config whose GetCertificate mints per-SNI leaves.
func (m *certMinter) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: m.getCertificate,
	}
}

func (m *certMinter) getCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := hello.ServerName
	if name == "" {
		// Some clients omit SNI; fall back to the API host so the handshake
		// still completes rather than failing outright.
		name = "slack.com"
	}
	return m.certFor(name)
}

// certFor returns a cached or freshly minted leaf for the given DNS name.
func (m *certMinter) certFor(name string) (*tls.Certificate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.cache[name]; ok {
		return c, nil
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating leaf key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generating serial: %w", err)
	}
	now := m.now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{name},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, m.caCert, &leafKey.PublicKey, m.caKey)
	if err != nil {
		return nil, fmt.Errorf("signing leaf for %q: %w", name, err)
	}
	tlsCert := &tls.Certificate{
		Certificate: [][]byte{der, m.caCert.Raw},
		PrivateKey:  leafKey,
		Leaf:        tmpl,
	}
	m.cache[name] = tlsCert
	return tlsCert, nil
}
