package routing

// Reconcile compares the desired placement (rendezvous over the live members) with the
// actual ownership map and returns the shards THIS node should claim and release.
//
//	release: a shard I own but am no longer the rendezvous-suggested owner of.
//	claim:   an unowned shard I am the rendezvous-suggested owner of.
//
// Rendezvous already balances: every shard has exactly one suggested owner among the live
// members, so claiming all of my suggested shards yields a complete, balanced, disjoint
// cover. (There is deliberately NO fair-share cap on claims — capping would orphan a shard
// whose node happens to be the rendezvous owner of more than the even split, since no other
// node would ever claim it.)
//
// Claiming an unowned slot is only a proposal; the etcd CAS in the ownership layer is the
// final arbiter, so two nodes proposing the same slot is fine — one wins, the other retries
// next round.
func Reconcile(me string, members []string, shardMap [ShardCount]string) (claim, release []int) {
	for s := 0; s < ShardCount; s++ {
		suggested := Rendezvous(s, members)
		actual := shardMap[s]
		switch {
		case actual == me && suggested != me:
			release = append(release, s)
		case actual == "" && suggested == me:
			claim = append(claim, s)
		}
	}
	return claim, release
}
