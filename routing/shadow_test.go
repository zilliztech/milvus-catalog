package routing

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Warm-shadow placement: the shard's second-highest rendezvous weight is the node that will
// inherit it when the primary fails, so it can preheat ahead of failover. These tests pin the
// algorithm: RendezvousTop2 returns (primary, shadow) correctly, and — the property that makes
// preheating correct — the shadow is exactly who rendezvous re-homes the shard to on primary
// loss.

// brute-forces the two highest-weight members for a shard, independent of RendezvousTop2.
func top2ByWeight(shard int, members []string) (string, string) {
	type mw struct {
		m string
		w uint64
	}
	ranked := make([]mw, 0, len(members))
	for _, m := range members {
		ranked = append(ranked, mw{m, weight(m, shard)})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].w > ranked[j].w })
	var p, s string
	if len(ranked) > 0 {
		p = ranked[0].m
	}
	if len(ranked) > 1 {
		s = ranked[1].m
	}
	return p, s
}

func TestRendezvousTop2PrimaryMatchesRendezvous(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4"}
	for s := 0; s < ShardCount; s++ {
		primary, _ := RendezvousTop2(s, members)
		require.Equal(t, Rendezvous(s, members), primary, "primary must match Rendezvous for shard %d", s)
	}
}

func TestRendezvousTop2ShadowIsSecondHighest(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	for s := 0; s < ShardCount; s++ {
		primary, shadow := RendezvousTop2(s, members)
		wantP, wantS := top2ByWeight(s, members)
		require.Equal(t, wantP, primary, "shard %d primary", s)
		require.Equal(t, wantS, shadow, "shard %d shadow", s)
		require.NotEqual(t, primary, shadow, "shard %d shadow must differ from primary", s)
		require.Contains(t, members, shadow, "shard %d shadow must be a real member", s)
	}
}

func TestRendezvousTop2Degenerate(t *testing.T) {
	p, s := RendezvousTop2(3, nil)
	require.Equal(t, "", p)
	require.Equal(t, "", s)

	p, s = RendezvousTop2(3, []string{})
	require.Equal(t, "", p)
	require.Equal(t, "", s)

	p, s = RendezvousTop2(3, []string{"node-1"})
	require.Equal(t, "node-1", p, "single member is the primary")
	require.Equal(t, "", s, "single member has no shadow")
}

// The core property: preheating the shadow is correct because rendezvous re-homes a shard to
// precisely its second-highest-weight member when the primary leaves. Remove each shard's
// primary and the new Rendezvous must equal the old shadow.
func TestShadowBecomesPrimaryOnFailover(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4"}
	for s := 0; s < ShardCount; s++ {
		primary, shadow := RendezvousTop2(s, members)

		survivors := make([]string, 0, len(members)-1)
		for _, m := range members {
			if m != primary {
				survivors = append(survivors, m)
			}
		}
		require.Equal(t, shadow, Rendezvous(s, survivors),
			"shard %d must fail over to its shadow when primary %s leaves", s, primary)
	}
}

// Every shard has exactly one shadow, and the shadows partition all 256 shards across the
// members — the shadow view is a complete, disjoint cover just like ownership.
func TestShadowShardsPartition(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4"}

	covered := map[int]int{}
	for _, me := range members {
		for _, s := range ShadowShards(me, members) {
			_, shadow := RendezvousTop2(s, members)
			require.Equal(t, me, shadow, "ShadowShards(%s) returned shard %d it does not shadow", me, s)
			covered[s]++
		}
	}
	require.Len(t, covered, ShardCount, "every shard must have exactly one shadow")
	for s, n := range covered {
		require.Equal(t, 1, n, "shard %d shadowed by %d nodes (must be exactly one)", s, n)
	}
}

func TestShadowShardsSingleMemberNil(t *testing.T) {
	require.Nil(t, ShadowShards("node-1", []string{"node-1"}), "a lone member has no shadow duties")
	require.Nil(t, ShadowShards("node-1", nil))
}

// A node never shadows the shards it already owns — shadow and primary are always distinct.
func TestShadowShardsDisjointFromOwned(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3"}
	for _, me := range members {
		shadowed := map[int]bool{}
		for _, s := range ShadowShards(me, members) {
			shadowed[s] = true
		}
		for s := 0; s < ShardCount; s++ {
			if Rendezvous(s, members) == me {
				require.False(t, shadowed[s], "%s shadows shard %d it already owns", me, s)
			}
		}
	}
}
