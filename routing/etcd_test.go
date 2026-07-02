package routing

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// writeTestCertPair generates a self-signed ECDSA certificate and writes cert + key PEM files
// into dir, returning their paths. The same file doubles as its own CA for the test, which is
// all the TLS-assembly path needs to load a key pair and a CA pool.
func writeTestCertPair(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "catalog-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	return certFile, keyFile
}

// No TLS configured must reproduce the historical plaintext dial: nil cfg.TLS, endpoints set,
// and the default dial timeout applied. This is the backward-compatibility guarantee.
func TestBuildEtcdClientConfig_NoTLS(t *testing.T) {
	cfg, err := buildEtcdClientConfig(EtcdConfig{Endpoints: []string{"127.0.0.1:2379"}})
	require.NoError(t, err)
	require.Nil(t, cfg.TLS, "no TLS config must dial plaintext")
	require.Equal(t, []string{"127.0.0.1:2379"}, cfg.Endpoints)
	require.Equal(t, defaultDialTimeout, cfg.DialTimeout)
}

// An explicit dial timeout must be honored (only zero falls back to the default).
func TestBuildEtcdClientConfig_ExplicitDialTimeout(t *testing.T) {
	cfg, err := buildEtcdClientConfig(EtcdConfig{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, cfg.DialTimeout)
}

// Mutual mTLS: a client key pair and CA produce a *tls.Config carrying the client certificate
// (so etcd can authenticate the node) and a RootCAs pool (so the node can authenticate etcd),
// with the TLS 1.3 floor by default and ServerName propagated.
func TestBuildEtcdClientConfig_MutualTLS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertPair(t, dir)

	cfg, err := buildEtcdClientConfig(EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		TLS: &EtcdTLSConfig{
			CertFile:   certFile,
			KeyFile:    keyFile,
			CACertFile: certFile, // self-signed cert is its own CA here
			ServerName: "etcd.internal",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.TLS)
	require.Len(t, cfg.TLS.Certificates, 1, "client cert must be presented for mutual mTLS")
	require.NotNil(t, cfg.TLS.RootCAs, "CA pool must be set so the server is verified")
	require.Equal(t, uint16(tls.VersionTLS13), cfg.TLS.MinVersion, "default floor is TLS 1.3")
	require.Equal(t, "etcd.internal", cfg.TLS.ServerName)
}

// MinVersion strings map to the right crypto/tls constants; unknown values are rejected so a
// typo can never silently weaken the handshake floor.
func TestBuildEtcdClientConfig_MinVersion(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertPair(t, dir)

	cases := map[string]uint16{
		"":    tls.VersionTLS13,
		"1.3": tls.VersionTLS13,
		"1.2": tls.VersionTLS12,
		"1.1": tls.VersionTLS11,
		"1.0": tls.VersionTLS10,
	}
	for in, want := range cases {
		cfg, err := buildEtcdClientConfig(EtcdConfig{
			Endpoints: []string{"127.0.0.1:2379"},
			TLS: &EtcdTLSConfig{
				CertFile: certFile, KeyFile: keyFile, CACertFile: certFile, MinVersion: in,
			},
		})
		require.NoError(t, err, "min version %q", in)
		require.Equal(t, want, cfg.TLS.MinVersion, "min version %q", in)
	}

	_, err := buildEtcdClientConfig(EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		TLS: &EtcdTLSConfig{
			CertFile: certFile, KeyFile: keyFile, CACertFile: certFile, MinVersion: "9.9",
		},
	})
	require.Error(t, err, "unknown TLS min version must be rejected")
}

// Missing cert/key material and a missing CA file must each surface an error instead of
// silently dialing without the intended TLS material.
func TestBuildEtcdClientConfig_TLSFileErrors(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertPair(t, dir)

	_, err := buildEtcdClientConfig(EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		TLS:       &EtcdTLSConfig{CertFile: "/nope/cert.pem", KeyFile: "/nope/key.pem", CACertFile: certFile},
	})
	require.Error(t, err, "missing client cert/key must error")

	_, err = buildEtcdClientConfig(EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		TLS:       &EtcdTLSConfig{CertFile: certFile, KeyFile: keyFile, CACertFile: "/nope/ca.pem"},
	})
	require.Error(t, err, "missing CA cert must error")
}

// End-to-end backward compatibility: the new constructor's plaintext path still connects to a
// real etcd and serves a Get, exactly like the pre-existing clientv3.New call it replaces.
// Skipped when no etcd is reachable so the pure-config tests above still run anywhere.
func TestNewEtcdClient_PlaintextConnects(t *testing.T) {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	cli, err := NewEtcdClient(EtcdConfig{Endpoints: strings.Split(ep, ","), DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := cli.Get(ctx, "catalog-test/mtls-smoke", clientv3.WithLimit(1)); err != nil {
		t.Skipf("etcd not reachable at %s, skipping plaintext connectivity check: %v", ep, err)
	}
}
