// Package client is the coord-side discovery client for the pooled catalog service. It lives
// in its own package (depending only on catalogpb, routing, nsmeta — never rootcoord or the
// service package) so both the catalog service and rootcoord can use it without import cycles.
package client

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-catalog/nsmeta"
	"github.com/zilliztech/milvus-catalog/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Router fetches the route map from any catalog node (never touching the pooled etcd
// directly), routes each namespace to the owner of its shard, and on a not-owner / unavailable
// response re-fetches the route map and redirects — making failover transparent to the caller.
type Router struct {
	bootstrap []string

	mu         sync.Mutex
	shardOwner map[int]string // shard -> owner address (node id == gRPC address)
	shardTerm  map[int]int64  // shard -> owner's ownership term, stamped on requests for fencing
	conns      map[string]*grpc.ClientConn
}

// NewRouter creates a discovery router seeded with one or more catalog node addresses.
func NewRouter(bootstrap ...string) *Router {
	return &Router{bootstrap: bootstrap, shardOwner: map[int]string{}, shardTerm: map[int]int64{}, conns: map[string]*grpc.ClientConn{}}
}

// Close shuts all pooled connections.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.conns {
		_ = c.Close()
	}
	r.conns = map[string]*grpc.ClientConn{}
}

func (r *Router) conn(addr string) (*grpc.ClientConn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	r.conns[addr] = c
	return c, nil
}

// Refresh pulls a fresh route map from any reachable node (bootstrap + current owners).
func (r *Router) Refresh(ctx context.Context) error {
	addrs := append([]string{}, r.bootstrap...)
	r.mu.Lock()
	for _, a := range r.shardOwner {
		addrs = append(addrs, a)
	}
	r.mu.Unlock()

	var lastErr error
	for _, addr := range addrs {
		c, err := r.conn(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := catalogpb.NewCatalogServiceClient(c).GetRouteMap(ctx, &catalogpb.GetRouteMapRequest{})
		if err != nil {
			lastErr = err
			continue
		}
		if err := merr.Error(resp.GetStatus()); err != nil {
			lastErr = err
			continue
		}
		owner := make(map[int]string, len(resp.GetShardOwner()))
		for shard, a := range resp.GetShardOwner() {
			owner[int(shard)] = a
		}
		term := make(map[int]int64, len(resp.GetShardTerm()))
		for shard, tm := range resp.GetShardTerm() {
			term[int(shard)] = tm
		}
		r.mu.Lock()
		r.shardOwner = owner
		r.shardTerm = term
		r.mu.Unlock()
		return nil
	}
	if lastErr == nil {
		lastErr = merr.WrapErrServiceUnavailable("no catalog node reachable for route map")
	}
	return lastErr
}

// OwnerOf returns the currently-known owner address for a namespace (empty if unknown).
func (r *Router) OwnerOf(namespace string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shardOwner[routing.ShardOf(namespace)]
}

// routeFor returns the owner address and its ownership term for a namespace in a SINGLE locked
// read, so a concurrent Refresh cannot splice a new-generation term onto an old-generation
// owner (a mismatch would only get fenced and retried, but reading both atomically avoids the
// wasted round trip). The term is stamped on each request so the owner can fence a
// stale-route-map call.
func (r *Router) routeFor(namespace string) (owner string, term int64) {
	shard := routing.ShardOf(namespace)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.shardOwner[shard], r.shardTerm[shard]
}

// Do runs fn against the catalog node that owns namespace, stamping the namespace on the
// context. On a not-owner / unavailable error it re-fetches the route map and retries at the
// (possibly new) owner — transparently surviving failover and ownership moves.
//
// The (owner, term) pair is read atomically via routeFor, but conn() and fn() then run without
// the lock; a concurrent Refresh between routeFor and fn can send a new-term request to the old
// owner, which the server fences as a retriable error — self-corrected on the next attempt.
func (r *Router) Do(ctx context.Context, namespace string, fn func(ctx context.Context, c catalogpb.CatalogServiceClient) error) error {
	const maxAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil { // caller cancelled/timed out: stop, don't churn the route map
			return err
		}
		owner, term := r.routeFor(namespace)
		if owner == "" {
			if err := r.Refresh(ctx); err != nil {
				lastErr = err
				continue
			}
			owner, term = r.routeFor(namespace)
			if owner == "" {
				lastErr = merr.WrapErrServiceUnavailable("namespace " + namespace + " has no owner yet")
				continue
			}
		}
		c, err := r.conn(owner)
		if err != nil {
			lastErr = err
			_ = r.Refresh(ctx)
			continue
		}
		err = fn(nsmeta.WithTerm(ctx, namespace, term), catalogpb.NewCatalogServiceClient(c))
		if err == nil {
			return nil
		}
		if shouldRedirect(err) {
			lastErr = err
			_ = r.Refresh(ctx)
			continue
		}
		return err // genuine business error: surface it
	}
	return lastErr
}

// shouldRedirect reports whether an error means "try the route map again at a different
// owner" — a retriable not-owner response, or a gRPC transport failure (dead node).
func shouldRedirect(err error) bool {
	if merr.IsRetryableErr(err) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}
