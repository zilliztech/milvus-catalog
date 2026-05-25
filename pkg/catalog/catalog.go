// Package catalog defines the metadata catalog boundary for Milvus.
//
// EXPERIMENTAL: this API may break between minor versions until v1.0. Callers
// should pin a specific commit or tag and review the CHANGELOG before bumping.
package catalog

import (
	"context"
)

type Catalog interface {
	Metadata() MetadataCatalog
	AccessControl() AccessControlCatalog
	InternalState() MilvusStateCatalog
	Migration() MigrationCatalog
	Close(ctx context.Context) error
}

type Implementation string

const (
	ImplEtcd           Implementation = "etcd"
	ImplOxia           Implementation = "oxia"
	ImplTiKV           Implementation = "tikv"
	ImplCatalogService Implementation = "catalog-service"
	ImplMigration      Implementation = "migration"
)
