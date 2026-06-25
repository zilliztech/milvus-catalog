# MEP: RootCoord Catalog Service — Pooled, Externalized Metadata

Status: Experimental (flag-gated, default off) · Scope of step 1: RootCoord metadata only.

## Summary

Externalize RootCoord's metadata (the full `IMetaTable` surface — Database / Collection /
Partition / Alias / Credential / Role / Grant / PrivilegeGroup / FileResource) out of the
coordinator process into an **independent, pooled, namespace-isolated catalog service** backed by
TiKV. Multiple Milvus clusters share one catalog service; each cluster's metadata is isolated by a
namespace (its cluster id). Metadata is migrated at runtime with zero downtime behind a flag.

This document describes the full design; **this first step implements RootCoord only**.
DataCoord / QueryCoord / StreamingNode follow in later steps.

## Motivation

Today every Milvus cluster's metadata lives **inside the coordinator process memory and that
cluster's own etcd**: reads are served from in-process memory; writes take a process-local lock →
read memory → compute → write etcd → update memory → release lock. This metadata is not only the
catalog (what / where / how-to-accelerate) but also task state, runtime state, TSO, RBAC.

Because the metadata is locked inside each cluster's coordinator, **external engines (Spark, Flink,
…) have no read/write entry point**. The Vector LakeBase strategy of making external engines
first-class citizens is blocked at the metadata layer.

Success metrics:

1. **Decommission per-cluster etcd**: N sets of metadata operational objects → 1 pooled service.
2. **Pooled catalog service**: an independent Milvus (e.g. a Spark connector) reads/writes metadata
   directly with the same concurrency correctness as Milvus. Metadata operation SLA 99.9%; a single
   node failure recovers in seconds for the affected cluster and is invisible to unrelated clusters.

## Public Interfaces

- **Config (experimental, `Export:false`, default off)**:
  - `rootCoord.catalogService.enabled` (bool) — route RootCoord migrated meta to the catalog service.
  - `rootCoord.catalogService.address` (host:port) — bootstrap address of the catalog service.
- **gRPC API** (`pkg/proto/catalog_*.proto` → `catalogpb`): the `IMetaTable` method surface plus
  migration RPCs (`BulkImport`, `VerifyImport`) and discovery/admin RPCs (`GetRouteMap`,
  `DeleteNamespace`). The API exposes **business semantics**, not key-level put/get. Backward
  compatibility rule: **only add fields, never remove**.
- **New binary** `cmd/catalogservice` — the standalone catalog service process (its own etcd for the
  routing control plane, its own TiKV for data).

The catalog boundary: what the data is (schema), where it is (segment / binlog / manifest), how to
accelerate it (index). Collection is the aggregate root. The underlying TiKV and etcd are never
exposed to clients.

## Design Details

### Architecture

```
 Milvus cluster (RootCoord)                         Catalog Service (pooled, N nodes)
  Core.meta = switchable                             gRPC Server
    ├─ local *MetaTable        (default: local etcd)   └─ routingMetaTable  (routes by namespace)
    ├─ blockingMetaTable       (gate during migration)      └─ Registry: namespace → *MetaTable
    └─ remoteMetaTable ───gRPC─► routes to service               └─ TiKV  prefix <root>/<namespace>
         └─ routingClient → Router (discover owner, redirect)
  catalogConvergeLoop (watches the enable flag)        Coordinator: membership + ownership + reconcile
                                                       (on the catalog's own etcd; backend = TiKV)
```

- The coord side keeps **zero metadata cache**: all reads go to the service; no client cache means
  no invalidation / watch / dirty reads. The real lock + in-memory maps + persistence stay inside
  the service's per-namespace `*MetaTable` (the same implementation RootCoord uses today; the
  catalog service reuses it as code, runs it as an independent process).
- A namespace = the cluster id (`common.ClusterName`), carried on each gRPC request. It is both the
  `Registry` key and the TiKV key prefix, so multiple clusters are physically isolated in one
  service. The lock + maps + persistence move as one atomic unit, so the local transaction semantics
  are preserved on the server — no CAS needed.
- For the streaming-broadcast DDL writes (AlterCollection / TruncateCollection / AlterAlias /
  DropAlias / AlterCredential / DeleteCredential), only the **meta-apply** moves to the service; the
  broadcast itself stays in the cluster's own streaming layer.

### Routing

Goals: sticky, highly available, consistent, scalable, balanced. Reuses etcd's liveness mechanism;
catalog ↔ etcd are protected by mutual mTLS.

1. **Membership**: a node writes `/members/{node-id}` under a lease on startup. When it dies, its
   member entry and the ownership records it holds evaporate automatically.
2. **Placement**: rendezvous hashing (not consistent hashing) — the second-place node acts as a warm
   shadow cache so failover is sub-second. Each node watches the member table and `shard_map[256]`,
   then computes:
   ```
   fair_share = ceil(256 / num_members)
   for s in 0..255:
      suggested = rendezvous(s, members)
      actual    = shard_map[s]
      actual == me    and suggested != me -> release list
      actual == empty and suggested == me -> claim list
   ```
3. **Release (graceful)**: mark each released shard, return NOT OWNER for new requests, wait for
   in-flight requests to drain, then conditionally delete `/ownership/shard/{s}` in etcd and drop
   that shard's cache and lock.
4. **Claim**: claim from the claim list until reaching fair share; etcd arbitrates. On success the
   ownership key's modRevision (the "term") increases; the new owner stamps that term on writes to
   fence a stale old owner. Pass-through channels serve immediately; business channels reload then
   serve. On failure, recompute next round.
5. **Discovery**: a client fetches the route map from any node on startup; on NOT OWNER / timeout it
   takes one extra hop to fetch the latest route map and redirects.
6. **Membership changes**: placement re-converges, discovery follows. Crash → lease TTL expiry →
   others recompute → clients re-pull. Restart / scale-out → stop new requests, drain, delete own
   keys (no TTL wait). Cold start → each node registers + claims; one placement loop assigns routing.
7. **Shards**: control plane fixed at 256; ownership keys do not change when namespaces are
   added/removed; scaling only moves `1/N · shard_count`.

### Consistency

- **Writes**: serialized by the owning catalog node's lock; the modRevision (term) obtained at
  ownership change is carried on writes to prevent split-brain.
- **Reads**: lease-window reads — each lease renewal records a stopwatch; a read rejects if the time
  since the last renewal exceeds the lease window.

### Add / Remove Namespace

Adding or removing a namespace never changes routing. Add: `hash(cluster-id) % 256` → route map →
owner. Remove: send a delete request to the owning node; it clears the related cache.

### High Availability (99.9% SLA)

- **TiKV**: 3 TiKV + 3 PD; all state in TiKV; each region 3 Raft replicas, multi-AZ, strongly
  consistent.
- **Catalog service**: the routing design above; at ~100–1000 namespaces per region, 5–7 nodes.
- **Pooled etcd**: 3/5-node Raft. If etcd is unavailable, only membership changes are blocked;
  existing owners keep serving reads/writes.
- **Milvus side**: gRPC client with exponential backoff retry, no fallback; the proxy returns an
  error when the catalog is unavailable.

### Monitoring

Extend the existing Prometheus + Grafana stack: global (QPS, P99/P95, error rate, per-pod
CPU/mem/gRPC conns, TiKV latency / region count / leader distribution / disk), per-cluster
(rate-limit triggers), alerts (all catalog pods down = P0; single pod or single TiKV node = P1).

## Compatibility, Deprecation, and Migration Plan

**Compatibility**: the whole feature is behind `rootCoord.catalogService.enabled` (default false) —
**zero behavior change when off**. Proto changes are add-only. The catalog service must support both
Milvus version N and N-1 simultaneously — this is precisely why the catalog service is an
**independent service, not a Milvus role**: it must version independently of any Milvus cluster
(like etcd / TiKV do). TiKV upgrades via TiDB Operator (rolling, no downtime).

**Migration & rollback** — measured: 10 fields (6 scalar + 2 varchar + 1 float vector), 16 shards,
64 partitions → a stable 6 keys per segment (1 segmentInfo + 3 binlog + 1 statslog + 1 segment-index).
1k segments: 6070 keys, 1.19 MB, ~1 s. 10k segments: 61709 keys, 12.23 MB; paged streaming read of
etcd 271 ms; write TiKV 128 keys/txn 7.82 s, 1024 keys/txn 1.7 s.

Strategy: MixCoord and StreamingNode migrate independently — gate writes → freeze and full copy →
diff verify → switch backend, with an idempotent durable marker; any diff mismatch rolls back.
- **MixCoord**: gate new writes, drain existing, bulk-copy by key prefix into TiKV, full diff,
  switch to the catalog service client (or roll back).
- **StreamingNode**: each SN gates its own pchannels, copies
  `wal/{pchannel}/(WALCheckpoint + VChannel + SegmentAssignment)` into TiKV, switches after no-diff.

## Test Plan

- **Unit**: gRPC round-trip tests per method group (request → service → real MetaTable → TiKV → read
  back), including the 6 broadcast-write methods' reconstruction; fault injection (network / disabled
  backend / not-owner); term-fencing gates; a routing-completeness test that every IMetaTable method
  (not just Database) is routed (guards the nil-embed regression).
- **End-to-end** (validated): two Milvus standalones sharing one pooled catalog service (its own etcd
  + TiKV) — runtime migration (gate → bulk-import over gRPC → verify → cutover), namespace isolation,
  the broadcast/meta-apply split through a real streaming layer, and seconds-level failover (kill a
  catalog node, the other takes over, clients redirect transparently).

## Rejected Alternatives

- **Catalog as a Milvus role (`milvus run catalogservice`)**: rejected. The catalog must version
  independently and serve N / N-1 Milvus versions; a role ships lockstep with Milvus.
- **Consistent hashing for placement**: rejected in favor of rendezvous hashing, whose deterministic
  second-place enables shadow-cache warm failover.
- **Client-side metadata cache on the coord**: rejected — it reintroduces invalidation / watch /
  dirty-read complexity. The coord stays zero-cache; reads go to the service.

## References

- Implementation: `internal/catalogservice/` (server, routing, migration, client), `internal/rootcoord/`
  (switchable / blocking / remote meta, converge loop), `cmd/catalogservice/`, `pkg/proto/catalog_*.proto`.
