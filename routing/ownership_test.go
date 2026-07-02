package routing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Ownership of a shard is an etcd key written under the owner's membership lease, claimed
// via compare-and-swap so two racing nodes can't both win. Because the key rides the lease,
// a crashed owner's shards free themselves automatically.

func TestClaimShardMutualExclusion(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/own-mx")
	ctx := context.Background()

	m1, err := JoinMembership(ctx, cli, prefix, "node-1", 5)
	require.NoError(t, err)
	defer m1.Close()
	m2, err := JoinMembership(ctx, cli, prefix, "node-2", 5)
	require.NoError(t, err)
	defer m2.Close()

	ok, _, err := ClaimShard(ctx, cli, prefix, 5, "node-1", m1.LeaseID())
	require.NoError(t, err)
	require.True(t, ok, "first claim of an unowned shard wins")

	ok2, _, err := ClaimShard(ctx, cli, prefix, 5, "node-2", m2.LeaseID())
	require.NoError(t, err)
	require.False(t, ok2, "second node cannot steal an owned shard")

	owner, found, err := ShardOwner(ctx, cli, prefix, 5)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-1", owner)
}

func TestReleaseShard(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/own-rel")
	ctx := context.Background()

	m1, _ := JoinMembership(ctx, cli, prefix, "node-1", 5)
	defer m1.Close()
	m2, _ := JoinMembership(ctx, cli, prefix, "node-2", 5)
	defer m2.Close()

	_, _, err := ClaimShard(ctx, cli, prefix, 7, "node-1", m1.LeaseID())
	require.NoError(t, err)

	// release by a non-owner is a no-op.
	require.NoError(t, ReleaseShard(ctx, cli, prefix, 7, "node-2"))
	owner, found, _ := ShardOwner(ctx, cli, prefix, 7)
	require.True(t, found)
	require.Equal(t, "node-1", owner)

	// release by the owner frees it; another node can then claim.
	require.NoError(t, ReleaseShard(ctx, cli, prefix, 7, "node-1"))
	_, found, _ = ShardOwner(ctx, cli, prefix, 7)
	require.False(t, found)

	ok, _, err := ClaimShard(ctx, cli, prefix, 7, "node-2", m2.LeaseID())
	require.NoError(t, err)
	require.True(t, ok)
}

func TestOwnershipEvaporatesOnCrash(t *testing.T) {
	cli := testEtcdClient(t)
	prefix := freshPrefix(t, cli, "catalog-test/own-crash")
	ctx := context.Background()

	m1, _ := JoinMembership(ctx, cli, prefix, "node-1", 2) // 2s ttl
	_, _, err := ClaimShard(ctx, cli, prefix, 9, "node-1", m1.LeaseID())
	require.NoError(t, err)

	m1.simulateCrash() // lease will expire, taking the ownership key with it

	require.Eventually(t, func() bool {
		_, found, _ := ShardOwner(ctx, cli, prefix, 9)
		return !found
	}, 8*time.Second, 250*time.Millisecond, "crashed owner's shard must free itself via lease expiry")
}
