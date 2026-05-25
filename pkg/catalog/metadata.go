package catalog

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type MetadataCatalog interface {
	Databases() DatabaseCatalog
	Collections() CollectionCatalog
	Partitions() PartitionCatalog
	Aliases() AliasCatalog
	Segments() SegmentCatalog
	Indexes() IndexCatalog
	Files() FileResourceCatalog
	Snapshots() SnapshotCatalog
}

type DatabaseCatalog interface {
	Create(ctx context.Context, db *model.Database, opts WriteOptions) error
	Get(ctx context.Context, ref DatabaseRef, opts ReadOptions) (*model.Database, error)
	List(ctx context.Context, opts ReadOptions) ([]*model.Database, error)
	Alter(ctx context.Context, db *model.Database, opts WriteOptions) error
	Drop(ctx context.Context, ref DatabaseRef, opts WriteOptions) error
}

type AlterCollectionRequest struct {
	Old         *model.Collection
	New         *model.Collection
	AlterType   AlterType
	FieldModify bool
}

type CollectionCatalog interface {
	Create(ctx context.Context, collection *model.Collection, opts WriteOptions) error
	Get(ctx context.Context, ref CollectionRef, opts ReadOptions) (*model.Collection, error)
	List(ctx context.Context, db DatabaseRef, opts ReadOptions) ([]*model.Collection, error)
	Exists(ctx context.Context, ref CollectionRef, opts ReadOptions) (bool, error)
	Alter(ctx context.Context, req AlterCollectionRequest, opts WriteOptions) error
	Drop(ctx context.Context, collection *model.Collection, opts WriteOptions) error
}

type AlterPartitionRequest struct {
	DatabaseID UniqueID
	Old        *model.Partition
	New        *model.Partition
	AlterType  AlterType
}

type PartitionCatalog interface {
	Create(ctx context.Context, db DatabaseRef, partition *model.Partition, opts WriteOptions) error
	List(ctx context.Context, collection CollectionRef, opts ReadOptions) ([]*model.Partition, error)
	Alter(ctx context.Context, req AlterPartitionRequest, opts WriteOptions) error
	Drop(ctx context.Context, ref PartitionRef, opts WriteOptions) error
}

type AliasCatalog interface {
	Create(ctx context.Context, alias *model.Alias, opts WriteOptions) error
	List(ctx context.Context, db DatabaseRef, opts ReadOptions) ([]*model.Alias, error)
	Alter(ctx context.Context, alias *model.Alias, opts WriteOptions) error
	Drop(ctx context.Context, ref AliasRef, opts WriteOptions) error
}

type ListSegmentsRequest struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	SegmentIDs   []UniqueID
}

type UpdateSegmentsRequest struct {
	Segments []*datapb.SegmentInfo
	Binlogs  []BinlogsIncrement
}

type SegmentCatalog interface {
	Save(ctx context.Context, segment *datapb.SegmentInfo, opts WriteOptions) error
	Get(ctx context.Context, ref SegmentRef, opts ReadOptions) (*datapb.SegmentInfo, error)
	List(ctx context.Context, req ListSegmentsRequest, opts ReadOptions) ([]*datapb.SegmentInfo, error)
	UpdateBatch(ctx context.Context, req UpdateSegmentsRequest, opts WriteOptions) error
	MarkDropped(ctx context.Context, segments []*datapb.SegmentInfo, opts WriteOptions) error
	Drop(ctx context.Context, segment *datapb.SegmentInfo, opts WriteOptions) error
}

type ListIndexesRequest struct {
	CollectionID UniqueID
	IndexIDs     []UniqueID
}

type ListSegmentIndexesRequest struct {
	CollectionID UniqueID
	SegmentID    UniqueID
}

type IndexCatalog interface {
	CreateIndex(ctx context.Context, index *model.Index, opts WriteOptions) error
	ListIndexes(ctx context.Context, req ListIndexesRequest, opts ReadOptions) ([]*model.Index, error)
	AlterIndexes(ctx context.Context, indexes []*model.Index, opts WriteOptions) error
	DropIndex(ctx context.Context, ref IndexRef, opts WriteOptions) error
	SaveSegmentIndex(ctx context.Context, index *model.SegmentIndex, opts WriteOptions) error
	ListSegmentIndexes(ctx context.Context, req ListSegmentIndexesRequest, opts ReadOptions) ([]*model.SegmentIndex, error)
	AlterSegmentIndexes(ctx context.Context, indexes []*model.SegmentIndex, opts WriteOptions) error
	DropSegmentIndex(ctx context.Context, ref SegmentIndexRef, opts WriteOptions) error
}

type FileResourceCatalog interface {
	Save(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64, opts WriteOptions) error
	Remove(ctx context.Context, resourceID int64, version uint64, opts WriteOptions) error
	List(ctx context.Context, opts ReadOptions) ([]*internalpb.FileResourceInfo, uint64, error)
}

type GetSnapshotRequest struct {
	CollectionID UniqueID
	Timestamp    Timestamp
}

type ListManifestsRequest struct {
	CollectionID UniqueID
	PartitionID  UniqueID
}

type Snapshot struct {
	CollectionID UniqueID
	Timestamp    Timestamp
	Segments     []*datapb.SegmentInfo
}

type Manifest struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	SegmentID    UniqueID
	Files        []string
}

// SnapshotCatalog covers two access shapes for collection snapshots:
//   - Save / Drop / List delegate to the legacy DataCoord snapshot KV through
//     the milvuscompat adapter today.
//   - Get and ListManifests are reserved for future implementations (catalog
//     service, TiKV native). The milvuscompat adapter returns
//     ErrUnsupportedImplementation for those two methods by design.
type SnapshotCatalog interface {
	Get(ctx context.Context, req GetSnapshotRequest, opts ReadOptions) (*Snapshot, error)
	ListManifests(ctx context.Context, req ListManifestsRequest, opts ReadOptions) ([]*Manifest, error)
	Save(ctx context.Context, snapshot *datapb.SnapshotInfo, opts WriteOptions) error
	Drop(ctx context.Context, collectionID UniqueID, snapshotID UniqueID, opts WriteOptions) error
	List(ctx context.Context, opts ReadOptions) ([]*datapb.SnapshotInfo, error)
}
