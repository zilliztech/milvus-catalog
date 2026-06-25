package routing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Rendezvous (highest-random-weight) hashing: each shard is owned by the member with the
// highest hash(member, shard). Chosen over consistent hashing because removing a member
// only re-homes that member's shards (minimal reshuffle), and the 2nd-highest is a natural
// warm shadow for fast failover.

func TestRendezvousDeterministicAndValid(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	got := Rendezvous(5, members)
	require.Equal(t, got, Rendezvous(5, members), "must be deterministic")
	require.Contains(t, members, got, "must pick a real member")
}

func TestRendezvousEmptyMembers(t *testing.T) {
	require.Equal(t, "", Rendezvous(5, nil))
	require.Equal(t, "", Rendezvous(5, []string{}))
}

// The defining property: removing a member only moves the shards that member owned; every
// other shard keeps its owner. Consistent hashing cannot guarantee this per-shard.
func TestRendezvousMinimalReshuffleOnRemoval(t *testing.T) {
	full := []string{"node-1", "node-2", "node-3", "node-4"}
	reduced := []string{"node-1", "node-2", "node-4"} // node-3 removed

	for s := 0; s < 256; s++ {
		before := Rendezvous(s, full)
		after := Rendezvous(s, reduced)
		if before != "node-3" {
			require.Equal(t, before, after, "shard %d owned by %s must not move when node-3 leaves", s, before)
		}
	}
}

// Fair-ish spread: 256 shards over 4 members, no member should be wildly over/under loaded.
func TestRendezvousSpread(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4"}
	count := map[string]int{}
	for s := 0; s < 256; s++ {
		count[Rendezvous(s, members)]++
	}
	require.Len(t, count, len(members), "every member should own at least one shard")
	for m, c := range count {
		require.Greater(t, c, 256/len(members)/3, "member %s too cold: %d", m, c)
		require.Less(t, c, 256/len(members)*3, "member %s too hot: %d", m, c)
	}
}

// ShardOf maps a namespace (cluster-id) to one of the fixed shard slots.
func TestShardOfStableInRange(t *testing.T) {
	for _, ns := range []string{"clusterA", "clusterB", "by-dev", "tenant-42"} {
		s := ShardOf(ns)
		require.GreaterOrEqual(t, s, 0)
		require.Less(t, s, ShardCount)
		require.Equal(t, s, ShardOf(ns), "must be stable for %s", ns)
	}
}

// Different namespaces should not all collapse to one shard.
func TestShardOfDistributes(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 64; i++ {
		seen[ShardOf(fmt.Sprintf("cluster-%d", i))] = true
	}
	require.Greater(t, len(seen), 1, "namespaces must spread across shards")
}
