// Package nsmeta carries the catalog-service namespace (cluster-id) on gRPC contexts. It is a
// leaf package (only grpc/metadata) so both the service side (catalogservice) and the coord
// side (rootcoord) can stamp/read the namespace without an import cycle.
package nsmeta

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
)

// Key is the gRPC metadata key carrying the caller's namespace (cluster-id).
const Key = "x-milvus-catalog-namespace"

// TermKey carries the ownership term the client routed against. The term advances on every
// ownership re-claim, so the service can fence a request that came off a stale route map.
const TermKey = "x-milvus-catalog-term"

// Default is used when a caller does not stamp a namespace (single-tenant mode).
const Default = "_default"

// With stamps the namespace on the outgoing gRPC context (coord-side client).
func With(ctx context.Context, namespace string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, Key, namespace)
}

// WithTerm stamps the namespace plus the ownership term the client routed against, so the
// service can fence a stale-route-map request. A zero term means term-unaware (opts out).
func WithTerm(ctx context.Context, namespace string, term int64) context.Context {
	return metadata.AppendToOutgoingContext(ctx, Key, namespace, TermKey, strconv.FormatInt(term, 10))
}

// TermFrom reads the routed-against ownership term off the incoming context (0 if absent).
func TermFrom(ctx context.Context) int64 {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	vals := md.Get(TermKey)
	if len(vals) == 0 || vals[0] == "" {
		return 0
	}
	t, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return 0
	}
	return t
}

// From reads the namespace off the incoming gRPC context (service-side), falling back to
// Default when none was stamped.
func From(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Default
	}
	vals := md.Get(Key)
	if len(vals) == 0 || vals[0] == "" {
		return Default
	}
	return vals[0]
}
