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

// Same-id fast restart: a node owns a shard, restarts with the same id but a fresh lease while
// the old ownership key still rides the dead incarnation's lease. Reconcile adopts it as a
// no-op (actual==me && suggested==me), so without a rebind the key would evaporate when the old
// lease expires while the node still serves the shard. The fix re-binds adopted shards to the
// current lease — proven here by revoking the OLD lease and showing the shard survives.
func TestCoordinatorRebindsAdoptedShardToCurrentLease(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/coord-rebind")
	ctx := context.Background()

	// Old incarnation claims shard 0 under lease L1, then "goes away" (we keep L1 alive to model
	// a fast restart inside the TTL window).
	m1, err := JoinMembership(ctx, cli, prefix, "node-1", 10)
	require.NoError(t, err)
	ok, _, err := ClaimShard(ctx, cli, prefix, 0, "node-1", m1.LeaseID())
	require.NoError(t, err)
	require.True(t, ok)

	// New incarnation: same id, fresh lease. The shard-0 key still rides L1.
	c := NewCoordinator(cli, prefix, "node-1", 10, 50*time.Millisecond)
	require.NoError(t, c.Start(ctx))
	defer c.Close()

	require.Eventually(t, func() bool {
		om, _ := LoadOwnershipMap(ctx, cli, prefix)
		return om[0].Owner == "node-1" && om[0].Lease == c.membership.LeaseID() && om[0].Lease != m1.LeaseID()
	}, 3*time.Second, 50*time.Millisecond, "adopted shard must be rebound to the current lease")

	// The proof: revoking the OLD lease must not delete the shard key now that it rides L2.
	_, err = cli.Revoke(context.Background(), m1.LeaseID())
	require.NoError(t, err)
	require.Never(t, func() bool {
		owner, ok, _ := ShardOwner(ctx, cli, prefix, 0)
		return !ok || owner != "node-1"
	}, 1*time.Second, 200*time.Millisecond, "shard must survive the old lease expiry once rebound")
}

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
