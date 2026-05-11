// Package api is deprecated. Use github.com/milvus-io/milvus-catalog/pkg/catalog.
package api

import "github.com/milvus-io/milvus-catalog/pkg/catalog"

type Catalog = catalog.Catalog
type MetadataCatalog = catalog.MetadataCatalog
type AccessControlCatalog = catalog.AccessControlCatalog
type MilvusStateCatalog = catalog.MilvusStateCatalog
type MigrationCatalog = catalog.MigrationCatalog

type ReadOptions = catalog.ReadOptions
type WriteOptions = catalog.WriteOptions
type Version = catalog.Version
type Epoch = catalog.Epoch
