package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/milvus-catalog/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The router is the coord-side discovery client. These tests inject the failure modes it must
// survive without a live catalog cluster: unreachable nodes, no discoverable owner, and caller
// cancellation. (The happy path and real failover are covered by the two-node e2e.)

// unreachableAddr points at a port nothing listens on, so every dial/RPC fails fast with
// connection-refused rather than hanging.
const unreachableAddr = "127.0.0.1:1"

// TestRouterRefreshAllNodesUnreachable: when no catalog node answers, Refresh returns an error
// (the caller retries/backs off) instead of silently leaving a stale route map.
func TestRouterRefreshAllNodesUnreachable(t *testing.T) {
	r := NewRouter(unreachableAddr)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	require.Error(t, r.Refresh(ctx), "refresh with no reachable node must fail")
}

// TestRouterDoNoOwnerReturnsError: with discovery unable to find an owner, Do exhausts its
// bounded redirects and surfaces an error rather than blocking forever or panicking — and the
// caller's fn is never run against a phantom owner.
func TestRouterDoNoOwnerReturnsError(t *testing.T) {
	r := NewRouter(unreachableAddr)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	called := false
	err := r.Do(ctx, "nsX", func(context.Context, catalogpb.CatalogServiceClient) error {
		called = true
		return nil
	})
	require.Error(t, err)
	require.False(t, called, "fn must not run when no owner could be discovered")
}

// TestRouterDoRespectsContextCancellation: a cancelled context stops Do immediately (returning
// the context error) instead of churning the route map.
func TestRouterDoRespectsContextCancellation(t *testing.T) {
	r := NewRouter(unreachableAddr)
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call

	err := r.Do(ctx, "nsX", func(context.Context, catalogpb.CatalogServiceClient) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

// TestCallErrPrefersTransportThenStatus pins the wrapper helper that fixes the silent-success
// bug: a transport error wins, a success status stays a success (normal calls are unaffected),
// and an error carried in the response Status is surfaced as a Go error so Router.Do can redirect
// instead of treating a not-owner response as success.
func TestCallErrPrefersTransportThenStatus(t *testing.T) {
	transport := errors.New("transport boom")
	require.ErrorIs(t, callErr(transport, merr.Success()), transport, "transport error must win")
	require.NoError(t, callErr(nil, merr.Success()), "a success status must stay nil")
	require.Error(t, callErr(nil, merr.Status(merr.WrapErrServiceUnavailable("not owner"))), "an error status must surface so Do can act on it")
}

// seedOwner points a namespace's shard at addr so Do/DoNonIdempotent reach the fn step without a
// live route map (white-box: the test is in package client).
func seedOwner(r *Router, namespace, addr string) {
	r.mu.Lock()
	r.shardOwner[routing.ShardOf(namespace)] = addr
	r.mu.Unlock()
}

// TestDoNonIdempotentDoesNotRetryAmbiguous: a non-idempotent call (e.g. a ref-count delta) must
// not be re-sent on an ambiguous transport failure, since the first attempt may already have been
// applied server-side — re-sending would double-apply it.
func TestDoNonIdempotentDoesNotRetryAmbiguous(t *testing.T) {
	r := NewRouter(unreachableAddr)
	defer r.Close()
	seedOwner(r, "nsX", unreachableAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	calls := 0
	err := r.DoNonIdempotent(ctx, "nsX", func(context.Context, catalogpb.CatalogServiceClient) error {
		calls++
		return status.Error(codes.Unavailable, "mid-flight")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls, "non-idempotent call must not be re-sent on an ambiguous transport failure")
}

// TestDoRetriesIdempotentOnAmbiguous: an idempotent call keeps the existing transparent-failover
// behaviour — it retries on an ambiguous transport failure.
func TestDoRetriesIdempotentOnAmbiguous(t *testing.T) {
	r := NewRouter(unreachableAddr)
	defer r.Close()
	seedOwner(r, "nsX", unreachableAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	calls := 0
	err := r.Do(ctx, "nsX", func(context.Context, catalogpb.CatalogServiceClient) error {
		calls++
		return status.Error(codes.Unavailable, "mid-flight")
	})
	require.Error(t, err)
	require.Greater(t, calls, 1, "idempotent call should retry on an ambiguous transport failure")
}
