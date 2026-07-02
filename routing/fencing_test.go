package routing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Lease-window fencing: a node that stops keeping its lease alive (crash / etcd partition)
// must stop considering itself a live member BEFORE its lease TTL expires, so it stops
// serving before another node can claim its shards — no stale-owner reads or writes.
func TestMembershipFreshWindow(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/fresh")
	m, err := JoinMembership(context.Background(), cli, prefix, "node-1", 3)
	require.NoError(t, err)
	defer m.Close()

	require.True(t, m.Fresh(2*time.Second), "just-joined member is fresh")

	m.simulateCrash() // keepalive stops; no more lease renewals
	require.Eventually(t, func() bool {
		return !m.Fresh(1 * time.Second)
	}, 5*time.Second, 100*time.Millisecond, "must go stale within the lease window after keepalive stops")
}

// Ownership records the etcd term (modRevision) of the claim so a later owner has a strictly
// higher term — the basis for fencing stale-owner writes.
func TestClaimRecordsTerm(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/term")
	ctx := context.Background()
	m1, _ := JoinMembership(ctx, cli, prefix, "node-1", 5)
	defer m1.Close()

	ok, term1, err := ClaimShard(ctx, cli, prefix, 3, "node-1", m1.LeaseID())
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, term1, int64(0), "claim must record a positive term")

	// a re-claim after release gets a strictly higher term (key recreated at a later revision).
	require.NoError(t, ReleaseShard(ctx, cli, prefix, 3, "node-1"))
	m2, _ := JoinMembership(ctx, cli, prefix, "node-2", 5)
	defer m2.Close()
	ok, term2, err := ClaimShard(ctx, cli, prefix, 3, "node-2", m2.LeaseID())
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, term2, term1, "re-claim must get a higher term to fence the old owner")
}

// IsServable is the gate the service uses per request: serve only if I own the namespace's
// shard, it is not being released, and my lease is fresh.
func TestCoordinatorIsServable(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/servable")
	c := NewCoordinator(cli, prefix, "node-1", 2, 200*time.Millisecond)
	require.NoError(t, c.Start(context.Background()))

	// single node converges to owning all shards -> any namespace is servable.
	require.Eventually(t, func() bool { return len(c.OwnedShards()) == ShardCount }, 10*time.Second, 200*time.Millisecond)
	require.True(t, c.IsServable("clusterA"), "owner with a fresh lease must serve")
	require.Greater(t, c.OwnerTerm(ShardOf("clusterA")), int64(0))

	// marking the shard releasing makes it non-servable (graceful handoff: reject new requests).
	c.MarkReleasing(ShardOf("clusterA"))
	require.False(t, c.IsServable("clusterA"), "a releasing shard must not serve")

	// a crash (lease no longer fresh) makes everything non-servable before TTL.
	c.simulateCrash()
	require.Eventually(t, func() bool { return !c.IsServable("clusterB") }, 5*time.Second, 100*time.Millisecond,
		"stale lease must stop serving")
}
