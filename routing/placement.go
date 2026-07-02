// Package routing implements the catalog service's control plane: which service node
// owns which namespace. Namespaces (cluster-ids) are hashed onto a fixed set of shards;
// shards are assigned to live members by rendezvous (highest-random-weight) hashing.
//
// Rendezvous is chosen over consistent hashing so that losing a member only re-homes that
// member's shards, and the second-highest-weight member is a natural warm shadow for fast
// failover.
package routing

import (
	"github.com/cespare/xxhash/v2"
)

// ShardCount is the fixed number of shard slots. The control plane is fixed-size so adding
// or removing namespaces never rewrites ownership keys; only membership changes do.
const ShardCount = 256

// ShardOf maps a namespace (cluster-id) to its shard slot.
func ShardOf(namespace string) int {
	return int(xxhash.Sum64String(namespace) % ShardCount)
}

// Rendezvous returns the member that owns the given shard: the one with the highest
// hash(member, shard). Returns "" if there are no members.
func Rendezvous(shard int, members []string) string {
	best := ""
	var bestScore uint64
	for _, m := range members {
		score := weight(m, shard)
		if best == "" || score > bestScore {
			best, bestScore = m, score
		}
	}
	return best
}

// RendezvousTop2 returns the two highest-weight members for a shard: the primary owner and the
// warm shadow (second highest). The shadow is exactly the member rendezvous re-homes the shard
// to when the primary leaves (removing the primary makes the second-highest the new highest),
// so a node that preheats the shards it shadows preloads precisely what it will inherit on
// failover. Returns ("", "") when there are no members and (primary, "") for a single member.
//
// primary always equals Rendezvous(shard, members): both rank by the same weight.
func RendezvousTop2(shard int, members []string) (primary, shadow string) {
	var pScore, sScore uint64
	for _, m := range members {
		score := weight(m, shard)
		switch {
		case primary == "" || score > pScore:
			shadow, sScore = primary, pScore // old primary slides down to shadow
			primary, pScore = m, score
		case shadow == "" || score > sScore:
			shadow, sScore = m, score
		}
	}
	return primary, shadow
}

// ShadowShards returns the shards for which me is the warm shadow (rendezvous second-highest
// weight) — the shards this node would inherit if their current primary failed, and therefore
// the set to preheat for fast failover. Returns nil when there are fewer than two members (a
// single member owns everything with nothing to shadow).
func ShadowShards(me string, members []string) []int {
	if len(members) < 2 {
		return nil
	}
	var out []int
	for s := 0; s < ShardCount; s++ {
		if _, shadow := RendezvousTop2(s, members); shadow == me {
			out = append(out, s)
		}
	}
	return out
}

func weight(member string, shard int) uint64 {
	d := xxhash.New()
	_, _ = d.WriteString(member)
	var b [8]byte
	u := uint64(shard)
	for i := 0; i < 8; i++ {
		b[i] = byte(u >> (8 * i))
	}
	_, _ = d.Write(b[:])
	return d.Sum64()
}
