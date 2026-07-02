package routing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Reconcile compares the desired placement (rendezvous over live members) against the
// actual ownership map and returns what THIS node should claim and release. It is the
// per-node half of the balance loop:
//
//	实际主 == 我 and 建议主 != 我 -> release
//	实际主 == 空 and 建议主 == 我 -> claim (until fair share reached)

func emptyMap() [ShardCount]string { return [ShardCount]string{} }

func TestReconcileClaimsUnownedSuggestedShards(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	sm := emptyMap() // nothing owned yet (cold start)

	claim, release := Reconcile("node-1", members, sm)

	require.Empty(t, release, "nothing owned, nothing to release")
	require.NotEmpty(t, claim, "should claim the shards rendezvous assigns to me")
	for _, s := range claim {
		require.Equal(t, "node-1", Rendezvous(s, members), "only claim shards I'm the suggested owner of")
	}
}

func TestReconcileReleasesShardsNoLongerMine(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	sm := emptyMap()
	// I currently own a shard whose suggested owner is someone else.
	victim := -1
	for s := 0; s < ShardCount; s++ {
		if Rendezvous(s, members) != "node-1" {
			victim = s
			break
		}
	}
	require.GreaterOrEqual(t, victim, 0)
	sm[victim] = "node-1"

	_, release := Reconcile("node-1", members, sm)
	require.Contains(t, release, victim, "must release a shard I own but am no longer suggested for")
}

func TestReconcileDoesNotClaimAlreadyOwned(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	sm := emptyMap()
	// Mark every node-1-suggested shard as already owned by node-1.
	owned := 0
	for s := 0; s < ShardCount; s++ {
		if Rendezvous(s, members) == "node-1" {
			sm[s] = "node-1"
			owned++
		}
	}
	require.Greater(t, owned, 0)

	claim, release := Reconcile("node-1", members, sm)
	require.Empty(t, claim, "already own all my suggested shards")
	require.Empty(t, release, "all owned shards are still mine")
}

// On a cold start every node claims exactly its rendezvous shards, and the union across all
// nodes covers all 256 with no overlap — no shard is orphaned (the bug a fair-share cap caused).
func TestReconcileColdStartCoversAllShards(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	sm := emptyMap()

	covered := map[int]int{} // shard -> how many nodes claimed it
	for _, me := range members {
		claim, release := Reconcile(me, members, sm)
		require.Empty(t, release, "nothing owned yet, nothing to release")
		require.NotEmpty(t, claim, "%s should claim its rendezvous shards", me)
		for _, s := range claim {
			require.Equal(t, me, Rendezvous(s, members), "only claim shards I'm the suggested owner of")
			covered[s]++
		}
	}
	require.Len(t, covered, ShardCount, "every shard must be claimed by exactly one node (full cover)")
	for s, n := range covered {
		require.Equal(t, 1, n, "shard %d claimed by %d nodes (must be exactly one)", s, n)
	}
}

func TestReconcileDoesNotTouchOthersValidShards(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	sm := emptyMap()
	// node-2 correctly owns its suggested shards.
	for s := 0; s < ShardCount; s++ {
		if Rendezvous(s, members) == "node-2" {
			sm[s] = "node-2"
		}
	}
	claim, release := Reconcile("node-1", members, sm)
	for _, s := range claim {
		require.NotEqual(t, "node-2", sm[s], "never claim a shard validly owned by another node")
	}
	require.Empty(t, release)
}
