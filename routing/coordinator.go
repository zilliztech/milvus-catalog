package routing

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
)

// Coordinator is a catalog-service node's control-plane agent. It joins the pooled etcd
// (membership lease), then runs a reconcile loop: read live members + the shard→owner map,
// compute the claim/release deltas via Reconcile, and apply them with CAS — claims and
// ownership keys ride the membership lease so a crash frees them automatically and the
// surviving nodes' loops pick them up. The loop IS the live half that drives the pure
// Reconcile math.
type Coordinator struct {
	cli    *clientv3.Client
	prefix string
	nodeID string
	ttl    int64
	tick   time.Duration

	membership       *Membership
	leaseWindow      time.Duration // serve only if the lease was renewed within this window
	reconcileTimeout time.Duration // bound each reconcile iteration so one hung etcd call can't stall it

	mu        sync.Mutex
	owned     map[int]int64    // shard -> term (claim ModRevision)
	releasing map[int]struct{} // shards being gracefully handed off; reject new requests

	cancel context.CancelFunc
	done   chan struct{}
}

// NewCoordinator creates a control-plane agent for one catalog-service node. The lease
// window is half the TTL: a node that loses etcd connectivity stops serving at ttl/2, well
// before the lease expires at ttl and another node may claim its shards.
func NewCoordinator(cli *clientv3.Client, prefix, nodeID string, ttlSeconds int64, tick time.Duration) *Coordinator {
	return &Coordinator{
		cli: cli, prefix: prefix, nodeID: nodeID, ttl: ttlSeconds, tick: tick,
		leaseWindow:      time.Duration(ttlSeconds) * time.Second / 2,
		reconcileTimeout: time.Duration(ttlSeconds) * time.Second, // a reconcile call that outlives the lease is already pathological
		owned:            make(map[int]int64),
		releasing:        make(map[int]struct{}),
		done:             make(chan struct{}),
	}
}

// Start joins membership and launches the reconcile loop.
func (c *Coordinator) Start(ctx context.Context) error {
	m, err := JoinMembership(ctx, c.cli, c.prefix, c.nodeID, c.ttl)
	if err != nil {
		return err
	}
	c.membership = m

	loopCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.loop(loopCtx)
	return nil
}

func (c *Coordinator) loop(ctx context.Context) {
	defer close(c.done)
	t := time.NewTicker(c.tick)
	defer t.Stop()
	c.reconcileOnce(ctx) // converge promptly, don't wait a full tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.membership.Lost():
			// The lease was lost and could not be re-established: stop reconciling (claiming with
			// a dead lease only fails) and let Fatal() surface to the supervisor for a restart,
			// instead of spinning forever as a zombie that owns and serves nothing.
			log.Ctx(ctx).Error("catalog reconcile: membership lease lost, stopping reconcile loop", zap.String("node", c.nodeID))
			return
		case <-t.C:
			c.reconcileOnce(ctx)
		}
	}
}

func (c *Coordinator) reconcileOnce(ctx context.Context) {
	// Bound the whole iteration: a single etcd call that hangs must not stall all future
	// reconciliation, and every swallowed error below is now logged so persistent divergence is
	// operator-visible instead of silent.
	cctx, cancel := context.WithTimeout(ctx, c.reconcileTimeout)
	defer cancel()

	members, err := ListMembers(cctx, c.cli, c.prefix)
	if err != nil {
		log.Ctx(ctx).Warn("catalog reconcile: list members failed", zap.String("node", c.nodeID), zap.Error(err))
		return
	}
	sm, err := LoadShardMap(cctx, c.cli, c.prefix)
	if err != nil {
		log.Ctx(ctx).Warn("catalog reconcile: load shard map failed", zap.String("node", c.nodeID), zap.Error(err))
		return
	}
	claim, release := Reconcile(c.nodeID, members, sm)
	for _, s := range claim {
		if _, _, err := ClaimShard(cctx, c.cli, c.prefix, s, c.nodeID, c.membership.LeaseID()); err != nil {
			log.Ctx(ctx).Warn("catalog reconcile: claim shard failed", zap.String("node", c.nodeID), zap.Int("shard", s), zap.Error(err))
		}
	}
	for _, s := range release {
		// graceful handoff: stop serving new requests for the shard, then release the key.
		c.MarkReleasing(s)
		if err := ReleaseShard(cctx, c.cli, c.prefix, s, c.nodeID); err != nil {
			log.Ctx(ctx).Warn("catalog reconcile: release shard failed", zap.String("node", c.nodeID), zap.Int("shard", s), zap.Error(err))
		}
	}

	// refresh the authoritative owned view (with terms) from etcd.
	om, err := LoadOwnershipMap(cctx, c.cli, c.prefix)
	if err != nil {
		log.Ctx(ctx).Warn("catalog reconcile: load ownership map failed", zap.String("node", c.nodeID), zap.Error(err))
		return
	}

	// Re-bind any shard we own whose ownership key still rides a previous incarnation's lease
	// (a same-id fast restart adopts such a shard without re-claiming it, since Reconcile treats
	// actual==me && suggested==me as a no-op). Left alone, that key evaporates when the stale
	// lease expires while we still serve the shard — a split-brain window if rendezvous has since
	// moved the shard elsewhere. CAS on value==nodeID keeps it safe; the term bumps once here and
	// is picked up on the next load. Shards we just claimed already ride the current lease, and
	// shards we are releasing are skipped.
	lease := c.membership.LeaseID()
	releaseSet := make(map[int]struct{}, len(release))
	for _, s := range release {
		releaseSet[s] = struct{}{}
	}
	for s := 0; s < ShardCount; s++ {
		if om[s].Owner != c.nodeID || om[s].Lease == lease {
			continue
		}
		if _, skip := releaseSet[s]; skip {
			continue
		}
		if _, _, err := RebindShard(cctx, c.cli, c.prefix, s, c.nodeID, lease); err != nil {
			log.Ctx(ctx).Warn("catalog reconcile: rebind shard to current lease failed", zap.String("node", c.nodeID), zap.Int("shard", s), zap.Error(err))
		}
	}

	owned := make(map[int]int64)
	for s := 0; s < ShardCount; s++ {
		if om[s].Owner == c.nodeID {
			owned[s] = om[s].Term
		}
	}
	// Rebuild releasing from scratch each round: keep a mark only for a shard we tried to
	// release this round AND still own (a genuine in-flight handoff window). This clears stale
	// marks so a shard that was released-then-reclaimed never gets stuck unservable.
	releasing := make(map[int]struct{})
	for _, s := range release {
		if _, ok := owned[s]; ok {
			releasing[s] = struct{}{}
		}
	}
	c.mu.Lock()
	c.owned = owned
	c.releasing = releasing
	c.mu.Unlock()
}

// Fatal returns a channel closed when the membership lease is irrecoverably lost (etcd dropped
// it after a partition longer than the TTL). The node can no longer serve or claim anything, so
// the owner should treat this as fatal and restart the process. Call after Start.
func (c *Coordinator) Fatal() <-chan struct{} {
	return c.membership.Lost()
}

// OwnedShards returns the shards this node currently owns (last reconcile's etcd view).
func (c *Coordinator) OwnedShards() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, 0, len(c.owned))
	for s := range c.owned {
		out = append(out, s)
	}
	return out
}

// IsServable is the per-request gate: serve a namespace only if this node owns its shard,
// the shard is not being released, and the lease is fresh (fencing stale owners).
func (c *Coordinator) IsServable(namespace string) bool {
	shard := ShardOf(namespace)
	c.mu.Lock()
	_, own := c.owned[shard]
	_, rel := c.releasing[shard]
	c.mu.Unlock()
	return own && !rel && c.membership.Fresh(c.leaseWindow)
}

// OwnerTerm returns this node's term for a shard it owns (0 if not owned).
func (c *Coordinator) OwnerTerm(shard int) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owned[shard]
}

// MarkReleasing flags a shard as handing off so the gate rejects new requests for it while
// in-flight requests drain before the ownership key is deleted.
func (c *Coordinator) MarkReleasing(shard int) {
	c.mu.Lock()
	c.releasing[shard] = struct{}{}
	c.mu.Unlock()
}

// Close gracefully leaves: stop the loop and revoke the membership lease (ownership frees
// immediately).
func (c *Coordinator) Close() {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	if c.membership != nil {
		c.membership.Close()
	}
}

// simulateCrash stops the loop and lets the lease expire WITHOUT revoking — the crash path
// exercised by failover tests. Ownership frees only after the TTL, not immediately.
func (c *Coordinator) simulateCrash() {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	if c.membership != nil {
		c.membership.simulateCrash()
	}
}

// OwnerOf resolves which node owns a namespace's shard — the discovery primitive a client
// uses to route a cluster's requests to the right catalog node.
func OwnerOf(ctx context.Context, cli *clientv3.Client, prefix, namespace string) (string, bool, error) {
	return ShardOwner(ctx, cli, prefix, ShardOf(namespace))
}

// ShardTerm returns this node's ownership term for the namespace's shard (0 if not owned).
// The catalog service uses it to invalidate a namespace's cached MetaTable when ownership
// changes (re-claim bumps the term), forcing a reload from the backend before serving.
func (c *Coordinator) ShardTerm(namespace string) int64 {
	return c.OwnerTerm(ShardOf(namespace))
}

// RouteMap returns the current membership, shard->owner map, and shard->owner-term map for
// client discovery. Clients fetch this from any node (via the GetRouteMap RPC) so they never
// read the pooled etcd directly; they then route each namespace to ShardOf(ns)'s owner and
// stamp the owner's term so the service can fence requests made off a stale route map.
func (c *Coordinator) RouteMap(ctx context.Context) ([]string, map[int]string, map[int]int64, error) {
	members, err := ListMembers(ctx, c.cli, c.prefix)
	if err != nil {
		return nil, nil, nil, err
	}
	om, err := LoadOwnershipMap(ctx, c.cli, c.prefix)
	if err != nil {
		return nil, nil, nil, err
	}
	shardOwner := make(map[int]string)
	shardTerm := make(map[int]int64)
	for s := 0; s < ShardCount; s++ {
		if om[s].Owner != "" {
			shardOwner[s] = om[s].Owner
			shardTerm[s] = om[s].Term
		}
	}
	return members, shardOwner, shardTerm, nil
}
