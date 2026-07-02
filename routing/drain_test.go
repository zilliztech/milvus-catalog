package routing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The in-flight drain barrier is the graceful-handoff half of failover: before a releasing
// node deletes a shard's ownership key it must (1) reject NEW requests for the shard and
// (2) wait for the ones already in flight to finish. These tests exercise the gate + barrier
// directly in memory — no etcd — by constructing a Coordinator with a fresh membership and a
// hand-set owned map, so the ordering guarantees are deterministic.

// newDrainTestCoordinator builds a Coordinator that never talks to etcd: nil client (Begin and
// drainShard never touch it) and a synthetic membership whose lease looks freshly renewed so
// the Fresh() gate passes.
func newDrainTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c := NewCoordinator(nil, "", "node-1", 10, time.Millisecond)
	m := &Membership{}
	m.lastKeepAlive.Store(time.Now().UnixNano()) // model a just-renewed lease
	c.membership = m
	return c
}

func TestBeginServesOwnedShard(t *testing.T) {
	c := newDrainTestCoordinator(t)
	ns := "clusterA"
	c.owned[ShardOf(ns)] = 1

	done, ok := c.Begin(ns)
	require.True(t, ok, "an owned, fresh, non-releasing shard must serve")
	require.NotNil(t, done)
	done()
}

func TestBeginRejectsUnownedShard(t *testing.T) {
	c := newDrainTestCoordinator(t)
	done, ok := c.Begin("clusterA") // nothing owned
	require.False(t, ok, "an unowned shard must not serve")
	require.Nil(t, done)
}

func TestBeginRejectsReleasingShard(t *testing.T) {
	c := newDrainTestCoordinator(t)
	ns := "clusterA"
	shard := ShardOf(ns)
	c.owned[shard] = 1

	done, ok := c.Begin(ns)
	require.True(t, ok)
	done()

	// Closing the gate must reject new requests even though the shard is still owned.
	c.MarkReleasing(shard)
	_, ok = c.Begin(ns)
	require.False(t, ok, "a releasing shard must reject new requests")
}

func TestBeginRejectsStaleLease(t *testing.T) {
	c := newDrainTestCoordinator(t)
	ns := "clusterA"
	c.owned[ShardOf(ns)] = 1
	// Lease last renewed well outside the freshness window (leaseWindow = ttl/2 = 5s).
	c.membership.lastKeepAlive.Store(time.Now().Add(-time.Hour).UnixNano())

	_, ok := c.Begin(ns)
	require.False(t, ok, "a stale lease must fence the node from serving")
}

// drainShard must block until every in-flight reader has finished. Here we hold the shard's
// read lock directly (as an in-flight request would) and prove the drain does not return until
// it is released.
func TestDrainShardWaitsForInflight(t *testing.T) {
	c := newDrainTestCoordinator(t)
	const shard = 5

	c.inflight[shard].RLock() // an in-flight request holds the read lock

	drained := make(chan struct{})
	go func() {
		c.drainShard(shard)
		close(drained)
	}()

	select {
	case <-drained:
		t.Fatal("drainShard returned while a request was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	c.inflight[shard].RUnlock() // request finishes

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drainShard must return once the in-flight request finished")
	}
}

// The end-to-end handoff ordering: a request that passed the gate via Begin must be waited for
// by drainShard, and drainShard must return promptly once that request's done func runs. This
// is the invariant that keeps ReleaseShard from cutting a live request.
func TestBeginDrainBarrierOrdering(t *testing.T) {
	c := newDrainTestCoordinator(t)
	ns := "clusterA"
	shard := ShardOf(ns)
	c.owned[shard] = 1

	done, ok := c.Begin(ns) // request in flight, holding the read lock
	require.True(t, ok)

	c.MarkReleasing(shard) // close the gate

	drained := make(chan struct{})
	go func() {
		c.drainShard(shard)
		close(drained)
	}()

	select {
	case <-drained:
		t.Fatal("drainShard returned before the in-flight request called done")
	case <-time.After(200 * time.Millisecond):
	}

	done() // request completes

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drainShard must return once the in-flight request completed")
	}

	// After draining, the gate is still closed: no new request may start.
	_, ok = c.Begin(ns)
	require.False(t, ok, "shard stays gated after drain")
}
