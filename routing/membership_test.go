package routing

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// These tests use a REAL etcd (the pooled control plane). TiKV has no lease/watch, so
// membership + ownership live in etcd. Set ETCD_ENDPOINTS to override 127.0.0.1:2379.
func testEtcdClient(t *testing.T) *clientv3.Client {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// freshPrefix wipes a test prefix so reruns against persistent etcd start clean.
func freshPrefix(t *testing.T, cli *clientv3.Client, p string) string {
	_, err := cli.Delete(context.Background(), p, clientv3.WithPrefix())
	require.NoError(t, err)
	return p
}

func TestMembershipJoinListLeave(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/memb-jll")
	ctx := context.Background()

	m1, err := JoinMembership(ctx, cli, prefix, "node-1", 3)
	require.NoError(t, err)
	m2, err := JoinMembership(ctx, cli, prefix, "node-2", 3)
	require.NoError(t, err)

	members, err := ListMembers(ctx, cli, prefix)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"node-1", "node-2"}, members)

	// graceful leave revokes the lease -> immediately gone.
	m1.Close()
	require.Eventually(t, func() bool {
		ms, _ := ListMembers(ctx, cli, prefix)
		return len(ms) == 1 && ms[0] == "node-2"
	}, 3*time.Second, 100*time.Millisecond)

	m2.Close()
}

// Crash (keepalive stops, no graceful revoke) -> lease TTL expires -> member evaporates.
// This is the foundation of seconds-level failover.
func TestMembershipCrashExpiry(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/memb-crash")
	ctx := context.Background()

	m1, err := JoinMembership(ctx, cli, prefix, "node-1", 2) // 2s ttl
	require.NoError(t, err)
	members, _ := ListMembers(ctx, cli, prefix)
	require.Contains(t, members, "node-1")

	m1.simulateCrash() // stop keepalive without revoking

	require.Eventually(t, func() bool {
		ms, _ := ListMembers(ctx, cli, prefix)
		return len(ms) == 0
	}, 8*time.Second, 250*time.Millisecond, "member must evaporate after lease TTL on crash")
}

// Lease lost out from under the node (here: revoked externally, as a partition-beyond-TTL would
// do) -> Lost() fires, so the node can surface a fatal error and be restarted instead of becoming
// a permanent zombie that neither serves nor claims.
func TestMembershipLostFiresWhenLeaseRevoked(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/memb-lost")
	ctx := context.Background()

	m, err := JoinMembership(ctx, cli, prefix, "node-1", 2)
	require.NoError(t, err)

	_, err = cli.Revoke(context.Background(), m.LeaseID()) // etcd then closes the keepalive channel
	require.NoError(t, err)

	select {
	case <-m.Lost():
	case <-time.After(5 * time.Second):
		t.Fatal("Lost() must fire once the lease is gone")
	}
}

// A deliberate Close must NOT be reported as a lost lease — otherwise a graceful shutdown would
// trip the supervisor's restart path.
func TestMembershipCloseDoesNotSignalLost(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/memb-close-nolost")
	ctx := context.Background()

	m, err := JoinMembership(ctx, cli, prefix, "node-1", 3)
	require.NoError(t, err)
	m.Close()

	select {
	case <-m.Lost():
		t.Fatal("a deliberate Close must not be reported as a lost lease")
	case <-time.After(1 * time.Second):
	}
}
