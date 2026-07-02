package routing

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus instrumentation for the routing control plane. Metrics are registered on the
// default registry via promauto as package-level singletons, so creating many Coordinators
// (as tests do) never re-registers and panics. Every series carries a node label so an
// operator can see per-node shard ownership, failover churn, reconcile progress, lease
// health, and request fencing.
const (
	metricsNamespace = "catalog"
	metricsSubsystem = "routing"
)

var (
	// ownedShards is the number of shards this node currently owns, refreshed each reconcile
	// round from the authoritative etcd ownership view. Summed across nodes it should equal the
	// number of claimed shards; a persistent dip signals orphaned shards.
	ownedShards = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "owned_shards",
		Help:      "Number of shards currently owned by this catalog-service node.",
	}, []string{"node"})

	// ownershipChanges counts shard ownership transitions this node applies: op="claim" when it
	// takes a shard, op="release" when it hands one off. Rising rates indicate failover or
	// rebalancing churn (易主).
	ownershipChanges = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "ownership_changes_total",
		Help:      "Count of shard ownership transitions applied by this node, by op (claim/release).",
	}, []string{"node", "op"})

	// reconcileRounds counts reconcile-loop iterations. A flatlined counter means the loop has
	// stalled (e.g. a hung etcd call or a lost lease that stopped the loop).
	reconcileRounds = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reconcile_rounds_total",
		Help:      "Count of reconcile-loop iterations executed by this node.",
	}, []string{"node"})

	// leaseKeepAliveInterval observes the elapsed time between successive membership-lease
	// keepalive renewals. Intervals stretching toward the TTL warn that the node is about to
	// fence itself and lose its shards.
	leaseKeepAliveInterval = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_keepalive_interval_seconds",
		Help:      "Elapsed seconds between successive membership-lease keepalive renewals.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"node"})

	// notOwnerRejections counts serve-gate checks rejected because this node does not own the
	// namespace's shard — the routing-miss signal a client's stale route map produces.
	notOwnerRejections = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "not_owner_rejections_total",
		Help:      "Count of serve-gate checks rejected because this node does not own the shard.",
	}, []string{"node"})
)
