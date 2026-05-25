# Milvus Catalog

Domain-first catalog interface for Milvus metadata.

The canonical API is `pkg/catalog.Catalog`:

- `Metadata()` for databases, collections, partitions, aliases, segments, indexes, files, and snapshots.
- `AccessControl()` for credentials, roles, grants, policies, and privilege groups.
- `InternalState()` for persistent Milvus component state such as DataCoord jobs, QueryCoord load state, and Streaming checkpoints.
- `Migration()` for online metadata migration from an existing implementation such as etcd to a target implementation such as Catalog Service.

Open-source Milvus should default to the etcd implementation. Cloud deployments can use migration mode and then switch to a Catalog Service implementation. Backends such as etcd, Oxia, TiKV, and Catalog Service are implementations under the same interface.

The current Milvus five coordinator catalog interfaces are compatibility adapters, not the canonical public API.
This module defines the interface and compatibility adapters; backend construction is supplied by the embedding runtime.

## Verify

```bash
go test ./...
```
