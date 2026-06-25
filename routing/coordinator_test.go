package routing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The Coordinator ties membership + ownership + Reconcile into a live loop: join the pooled
// etcd, periodically reconcile desired vs actual placement, claim/release via CAS. These
// tests drive it against real etcd and assert convergence, discovery, and seconds-level
// failover.

func TestCoordinatorConvergesToDisjointCover(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/coord-conv")
	ctx := context.Background()

	c1 := NewCoordinator(cli, prefix, "node-1", 3, 200*time.Millisecond)
	require.NoError(t, c1.Start(ctx))
	defer c1.Close()
	c2 := NewCoordinator(cli, prefix, "node-2", 3, 200*time.Millisecond)
	require.NoError(t, c2.Start(ctx))
	defer c2.Close()

	require.Eventually(t, func() bool {
		return len(c1.OwnedShards())+len(c2.OwnedShards()) == ShardCount
	}, 10*time.Second, 200*time.Millisecond, "two nodes must cover all 256 shards disjointly")

	require.Greater(t, len(c1.OwnedShards()), 0, "node-1 should own a fair share")
	require.Greater(t, len(c2.OwnedShards()), 0, "node-2 should own a fair share")
}

func TestCoordinatorDiscovery(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/coord-disc")
	ctx := context.Background()

	c1 := NewCoordinator(cli, prefix, "node-1", 3, 200*time.Millisecond)
	require.NoError(t, c1.Start(ctx))
	defer c1.Close()

	// single node owns everything -> any namespace resolves to it.
	require.Eventually(t, func() bool {
		return len(c1.OwnedShards()) == ShardCount
	}, 10*time.Second, 200*time.Millisecond)

	owner, ok, err := OwnerOf(ctx, cli, prefix, "clusterA")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "node-1", owner)
}

func TestCoordinatorFailoverSecondsLevel(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/coord-failover")
	ctx := context.Background()

	c1 := NewCoordinator(cli, prefix, "node-1", 2, 200*time.Millisecond) // short ttl for fast failover
	require.NoError(t, c1.Start(ctx))
	c2 := NewCoordinator(cli, prefix, "node-2", 2, 200*time.Millisecond)
	require.NoError(t, c2.Start(ctx))
	defer c2.Close()

	// converge to a shared cover.
	require.Eventually(t, func() bool {
		return len(c1.OwnedShards())+len(c2.OwnedShards()) == ShardCount &&
			len(c1.OwnedShards()) > 0 && len(c2.OwnedShards()) > 0
	}, 10*time.Second, 200*time.Millisecond)

	// node-1 crashes (lease will expire, freeing its shards).
	c1.simulateCrash()

	// node-2 takes over ALL shards within seconds (lease TTL + a few reconcile ticks).
	require.Eventually(t, func() bool {
		return len(c2.OwnedShards()) == ShardCount
	}, 12*time.Second, 250*time.Millisecond, "surviving node must take over all shards after a crash")
}
