// Package client is the coord-side discovery client for the pooled catalog service. It lives
// in its own package (depending only on catalogpb, routing, nsmeta — never rootcoord or the
// service package) so both the catalog service and rootcoord can use it without import cycles.
package client

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	creds     credentials.TransportCredentials // how to dial catalog nodes; insecure by default

	mu         sync.Mutex
	shardOwner map[int]string // shard -> owner address (node id == gRPC address)
	shardTerm  map[int]int64  // shard -> owner's ownership term, stamped on requests for fencing
	conns      map[string]*grpc.ClientConn
}

// NewRouter creates a discovery router seeded with one or more catalog node addresses. It dials
// catalog nodes insecurely by default; call WithCredentials to enable TLS/mTLS on the data plane
// (which carries credential and RBAC RPCs) before this leaves an experimental deployment.
func NewRouter(bootstrap ...string) *Router {
	return &Router{
		bootstrap:  bootstrap,
		creds:      insecure.NewCredentials(),
		shardOwner: map[int]string{},
		shardTerm:  map[int]int64{},
		conns:      map[string]*grpc.ClientConn{},
	}
}

// WithCredentials sets the transport credentials used to dial catalog nodes (e.g. mTLS), so the
// coord→catalog data plane is not forced to plaintext. Call before the first request.
func (r *Router) WithCredentials(creds credentials.TransportCredentials) *Router {
	r.creds = creds
	return r
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

// evictStaleConnsLocked closes and drops pooled connections to addresses that are neither a
// current shard owner nor a bootstrap node, so the pool tracks only live owners instead of
// growing one dangling connection per departed owner under membership churn. Caller holds r.mu.
func (r *Router) evictStaleConnsLocked(owner map[int]string) {
	live := make(map[string]struct{}, len(owner)+len(r.bootstrap))
	for _, a := range owner {
		live[a] = struct{}{}
	}
	for _, a := range r.bootstrap {
		live[a] = struct{}{}
	}
	for addr, c := range r.conns {
		if _, ok := live[addr]; !ok {
			_ = c.Close()
			delete(r.conns, addr)
		}
	}
}

func (r *Router) conn(addr string) (*grpc.ClientConn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(r.creds))
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
		r.evictStaleConnsLocked(owner)
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
// (possibly new) owner — transparently surviving failover and ownership moves. Use this for
// idempotent calls only; for calls whose re-send could double-apply, use DoNonIdempotent.
//
// The (owner, term) pair is read atomically via routeFor, but conn() and fn() then run without
// the lock; a concurrent Refresh between routeFor and fn can send a new-term request to the old
// owner, which the server fences as a retriable error — self-corrected on the next attempt.
func (r *Router) Do(ctx context.Context, namespace string, fn func(ctx context.Context, c catalogpb.CatalogServiceClient) error) error {
	return r.do(ctx, namespace, fn, true)
}

// DoNonIdempotent is Do for calls that must not be blindly re-sent on an ambiguous transport
// failure (mid-flight Unavailable / DeadlineExceeded): the first attempt may already have been
// applied server-side, so re-sending a delta (e.g. a ref-count increment/decrement) would
// double-apply it. It still redirects on an explicit not-owner response — the server rejected
// the call before applying it, so retrying at the new owner is safe — but surfaces an ambiguous
// transport error to the caller instead of retrying.
func (r *Router) DoNonIdempotent(ctx context.Context, namespace string, fn func(ctx context.Context, c catalogpb.CatalogServiceClient) error) error {
	return r.do(ctx, namespace, fn, false)
}

func (r *Router) do(ctx context.Context, namespace string, fn func(ctx context.Context, c catalogpb.CatalogServiceClient) error, retryAmbiguous bool) error {
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
		// A retriable rejection (not-owner / not-ready / rate-limited) is refused before the call
		// is applied, so redirecting is always safe. An ambiguous transport failure may have been
		// applied already, so we only retry it when the caller marked the call idempotent.
		if isRetriableRejection(err) || (retryAmbiguous && isAmbiguousTransport(err)) {
			lastErr = err
			_ = r.Refresh(ctx)
			continue
		}
		return err // genuine business error, or an ambiguous failure we must not blindly retry
	}
	return lastErr
}

// isRetriableRejection reports whether the server refused the call with a retriable status —
// not-owner, not-ready, rate-limited and the like. Milvus's merr convention marks retriable=true
// only for rejections made before the call is applied, so redirecting and retrying is always
// safe (the not-owner case is what drives failover here).
func isRetriableRejection(err error) bool {
	return merr.IsRetryableErr(err)
}

// isAmbiguousTransport reports whether err is a gRPC transport failure that leaves the call's
// outcome unknown — the request may or may not have reached and been applied by the server.
func isAmbiguousTransport(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}
