package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
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
