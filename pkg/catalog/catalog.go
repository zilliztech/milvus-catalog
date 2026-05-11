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
