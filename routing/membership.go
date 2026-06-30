package routing

import (
	"context"
	"path"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Membership is a catalog-service node's liveness registration in the pooled etcd control
// plane. TiKV has no lease/watch, so the control plane lives in etcd: a node grants a lease,
// writes /{prefix}/members/{nodeID} under it, and keeps it alive. On graceful Close the lease
// is revoked (immediate departure); on crash the lease TTL expires and the member — plus any
// ownership keys written under the same lease — evaporate automatically.
type Membership struct {
	cli     *clientv3.Client
	prefix  string
	nodeID  string
	leaseID clientv3.LeaseID

	lastKeepAlive atomic.Int64 // unix-nanos of the last successful lease keepalive

	cancelKeepAlive context.CancelFunc
	closing         atomic.Bool   // set on a deliberate Close/simulateCrash so the keepalive goroutine doesn't report a loss
	lost            chan struct{} // closed when keepalive ends unexpectedly (lease lost), never on a deliberate Close
}

// Fresh reports whether the lease was renewed within window. A node that loses its etcd
// connection stops being fresh well before the lease TTL expires, so it can fence itself
// (stop serving) before another node is allowed to claim its shards.
func (m *Membership) Fresh(window time.Duration) bool {
	last := m.lastKeepAlive.Load()
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) < window
}

func membersPrefix(prefix string) string { return path.Join(prefix, "members") + "/" }

// JoinMembership grants a lease, registers this node under it, and starts keepalive.
func JoinMembership(ctx context.Context, cli *clientv3.Client, prefix, nodeID string, ttlSeconds int64) (*Membership, error) {
	lease, err := cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, err
	}
	key := membersPrefix(prefix) + nodeID
	if _, err := cli.Put(ctx, key, "", clientv3.WithLease(lease.ID)); err != nil {
		_, _ = cli.Revoke(context.Background(), lease.ID) // don't leak the granted lease on Put failure
		return nil, err
	}

	kaCtx, cancel := context.WithCancel(context.Background())
	ka, err := cli.KeepAlive(kaCtx, lease.ID)
	if err != nil {
		cancel()
		_, _ = cli.Revoke(context.Background(), lease.ID) // revoking drops the lease-bound member key too, so a KeepAlive failure leaves no phantom member lingering to TTL
		return nil, err
	}
	m := &Membership{cli: cli, prefix: prefix, nodeID: nodeID, leaseID: lease.ID, cancelKeepAlive: cancel, lost: make(chan struct{})}
	m.lastKeepAlive.Store(time.Now().UnixNano()) // the initial registration counts as fresh
	// record each successful keepalive so Fresh() reflects live etcd connectivity.
	go func() {
		for range ka {
			m.lastKeepAlive.Store(time.Now().UnixNano())
		}
		// The keepalive channel closed. If this wasn't a deliberate Close/simulateCrash, the lease
		// was lost (e.g. an etcd partition longer than the TTL) and nothing re-establishes it —
		// signal the loss so the owner can restart instead of becoming a permanent zombie.
		if !m.closing.Load() {
			close(m.lost)
		}
	}()
	return m, nil
}

// Lost returns a channel closed when keepalive ends without a deliberate Close — i.e. the lease
// was lost and the node can no longer renew it. The Coordinator surfaces this via Fatal().
func (m *Membership) Lost() <-chan struct{} { return m.lost }

// LeaseID exposes the membership lease so ownership keys can be written under it — that is
// what makes a dead node's ownership evaporate together with its membership.
func (m *Membership) LeaseID() clientv3.LeaseID { return m.leaseID }

// Close gracefully leaves: stop keepalive and revoke the lease (member + lease-bound
// ownership keys disappear immediately).
func (m *Membership) Close() {
	m.closing.Store(true) // deliberate departure: don't let the keepalive goroutine report a loss
	m.cancelKeepAlive()
	_, _ = m.cli.Revoke(context.Background(), m.leaseID)
}

// simulateCrash stops keepalive WITHOUT revoking, so the lease expires after its TTL —
// used by tests to exercise crash-driven failover.
func (m *Membership) simulateCrash() {
	m.closing.Store(true) // the process is "gone"; in a real crash the goroutine dies with it and never signals
	m.cancelKeepAlive()
}

// ListMembers returns the node IDs currently registered under the prefix.
func ListMembers(ctx context.Context, cli *clientv3.Client, prefix string) ([]string, error) {
	mp := membersPrefix(prefix)
	resp, err := cli.Get(ctx, mp, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	members := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		members = append(members, strings.TrimPrefix(string(kv.Key), mp))
	}
	return members, nil
}
