// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// genTestCA produces a self-signed CA and returns its PEM cert and key, for
// exercising the minter without touching disk.
func genTestCA(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "WS-PoC Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling CA key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func TestCertMinterMintsTrustedLeafPerSNI(t *testing.T) {
	certPEM, keyPEM := genTestCA(t)
	m, err := newCertMinter(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("newCertMinter() error = %v", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to load CA into verification pool")
	}

	for _, name := range []string{"slack.com", "wss-primary.slack.com"} {
		cert, err := m.certFor(name)
		if err != nil {
			t.Fatalf("certFor(%q) error = %v", name, err)
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			t.Fatalf("parsing minted leaf: %v", err)
		}
		// A client that trusts the CA must accept the leaf for that hostname.
		if _, err := leaf.Verify(x509.VerifyOptions{DNSName: name, Roots: pool}); err != nil {
			t.Errorf("minted leaf for %q failed verification against CA: %v", name, err)
		}
	}
}

func TestCertMinterCachesPerName(t *testing.T) {
	certPEM, keyPEM := genTestCA(t)
	m, err := newCertMinter(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("newCertMinter() error = %v", err)
	}
	a, err := m.certFor("slack.com")
	if err != nil {
		t.Fatalf("certFor error = %v", err)
	}
	b, err := m.certFor("slack.com")
	if err != nil {
		t.Fatalf("certFor error = %v", err)
	}
	if a != b {
		t.Error("certFor returned a fresh certificate for a cached name; want the cached instance")
	}
}

func TestNewCertMinterRejectsNonCA(t *testing.T) {
	// A leaf (non-CA) cert must be rejected as signing material.
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "not-a-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if _, err := newCertMinter(certPEM, keyPEM); err == nil {
		t.Fatal("newCertMinter() accepted a non-CA certificate; want error")
	}
}
