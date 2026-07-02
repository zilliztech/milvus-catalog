package routing

// Graceful shard handoff needs two things the plain ownership CAS can't give: (1) a gate that
// stops accepting new requests for a shard the node is about to release, and (2) a barrier that
// waits for the requests already in flight to finish before the ownership key is deleted — so
// the shard's next owner never races a straggler write from the outgoing owner.
//
// This mirrors rootcoord's drainInFlightWrites (internal/rootcoord/catalog_migration.go): once
// the gate is closed, acquire the write lock and immediately release it. Acquiring it blocks
// until every in-flight request that took the read lock before the gate closed has finished.
// Here the lock is per-shard (Coordinator.inflight), so draining one shard never stalls
// requests to the shards this node still owns.

// Begin registers an in-flight request for the namespace's shard. It returns a done func and
// true when the request may proceed, or (nil, false) when the shard is not servable — not
// owned, being released, or the lease is no longer fresh — in which case the caller must reject
// the request. The done func MUST be called exactly once when the request finishes, otherwise
// a subsequent drainShard for that shard blocks forever.
//
// Begin is the drain-aware sibling of IsServable: it takes the shard's read lock BEFORE
// checking the gate, so a concurrent drainShard either observes this request (its write-lock
// acquisition waits for our RUnlock) or this request observes the release (and backs off). The
// ordering makes the handoff invariant hold: when ReleaseShard runs, no request that passed the
// gate is still in flight.
func (c *Coordinator) Begin(namespace string) (done func(), ok bool) {
	shard := ShardOf(namespace)
	c.inflight[shard].RLock() // enter the barrier first, then check the gate under it
	c.mu.Lock()
	_, own := c.owned[shard]
	_, rel := c.releasing[shard]
	c.mu.Unlock()
	if !own || rel || !c.membership.Fresh(c.leaseWindow) {
		c.inflight[shard].RUnlock() // gate closed: leave the barrier and reject
		return nil, false
	}
	return func() { c.inflight[shard].RUnlock() }, true
}

// drainShard is the in-flight barrier. It must be called AFTER MarkReleasing has closed the
// gate (so no new request can enter): it takes the shard's write lock and immediately releases
// it, which blocks until every request that entered via Begin before the gate closed has called
// its done func. Once it returns, the shard has no in-flight request and the ownership key can
// be deleted safely. The wait is bounded by the longest in-flight request; new requests are
// already gated, so it always converges.
func (c *Coordinator) drainShard(shard int) {
	c.inflight[shard].Lock()
	c.inflight[shard].Unlock() //nolint:staticcheck // intentional barrier: wait for in-flight readers, then release
}
