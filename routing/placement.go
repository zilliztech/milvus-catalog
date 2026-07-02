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
