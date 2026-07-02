package routing

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// defaultDialTimeout is used when EtcdConfig.DialTimeout is zero.
const defaultDialTimeout = 5 * time.Second

// EtcdConfig describes how a catalog-service node dials the pooled control-plane etcd. The
// control plane (membership leases, ownership keys) lives in etcd because TiKV has no
// lease/watch, so every Coordinator/Membership needs a *clientv3.Client — this is the one
// place that builds it. TLS is optional: leave it nil to keep the historical plaintext dial.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration // 0 -> defaultDialTimeout

	// TLS, when non-nil, dials etcd over mutual mTLS. Nil preserves plaintext (backward compatible).
	TLS *EtcdTLSConfig
}

// EtcdTLSConfig configures mutual mTLS between a catalog-service node and etcd. Mutual means
// both directions are authenticated: the node presents CertFile/KeyFile so etcd authenticates
// the client, and CACertFile verifies etcd's server certificate so the client authenticates
// etcd. This matches the "catalog <-> etcd are protected by mutual mTLS" design goal.
type EtcdTLSConfig struct {
	CertFile   string // client certificate presented to etcd
	KeyFile    string // private key for CertFile
	CACertFile string // CA bundle used to verify the etcd server certificate
	MinVersion string // "1.0".."1.3"; empty defaults to "1.3"
	ServerName string // optional SNI / certificate-hostname override
}

// tlsMinVersion maps the configured minimum TLS version string to a crypto/tls constant.
// An empty string defaults to TLS 1.3; anything unrecognized is an error so a typo can never
// silently weaken the handshake floor.
func tlsMinVersion(minVersion string) (uint16, error) {
	switch minVersion {
	case "", "1.3":
		return tls.VersionTLS13, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.0":
		return tls.VersionTLS10, nil
	default:
		return 0, fmt.Errorf("routing: unknown etcd TLS min version %q (want 1.0, 1.1, 1.2 or 1.3)", minVersion)
	}
}

// buildTLSConfig loads the client key pair and CA bundle into a *tls.Config suitable for
// mutual mTLS against etcd. Kept separate from client construction so it is unit-testable
// without a live etcd server.
func buildTLSConfig(cfg EtcdTLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("routing: load etcd client cert/key pair: %w", err)
	}

	caPEM, err := os.ReadFile(cfg.CACertFile)
	if err != nil {
		return nil, fmt.Errorf("routing: read etcd CA cert %q: %w", cfg.CACertFile, err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("routing: no valid certificate found in etcd CA cert %q", cfg.CACertFile)
	}

	minVer, err := tlsMinVersion(cfg.MinVersion)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:   minVer,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   cfg.ServerName,
	}, nil
}

// buildEtcdClientConfig assembles a clientv3.Config from an EtcdConfig, wiring in TLS when
// requested. It is a pure function (no network) so the TLS assembly and the plaintext
// backward-compatible path can both be exercised in unit tests.
func buildEtcdClientConfig(cfg EtcdConfig) (clientv3.Config, error) {
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}

	out := clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: dialTimeout,
	}
	if cfg.TLS != nil {
		tlsCfg, err := buildTLSConfig(*cfg.TLS)
		if err != nil {
			return clientv3.Config{}, err
		}
		out.TLS = tlsCfg
	}
	return out, nil
}

// NewEtcdClient dials the pooled control-plane etcd, optionally over mutual mTLS. When
// cfg.TLS is nil it dials plaintext exactly as before, so existing insecure deployments are
// unaffected; when cfg.TLS is set both peers are authenticated. The returned client is what
// callers pass to JoinMembership / NewCoordinator.
func NewEtcdClient(cfg EtcdConfig) (*clientv3.Client, error) {
	clientCfg, err := buildEtcdClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientCfg)
}
