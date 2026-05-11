package milvuscompat

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-catalog/pkg/catalog"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/stretchr/testify/require"
)

type recordingRootCatalog struct {
	metastore.RootCoordCatalog
	db           *model.Database
	ts           uint64
	dbs          []*model.Database
	collection   *model.Collection
	oldColl      *model.Collection
	newColl      *model.Collection
	fileResource *internalpb.FileResourceInfo
}

func (r *recordingRootCatalog) CreateDatabase(ctx context.Context, db *model.Database, ts uint64) error {
	r.db = db
	r.ts = ts
	return nil
}

func (r *recordingRootCatalog) ListDatabases(ctx context.Context, ts uint64) ([]*model.Database, error) {
	r.ts = ts
	return r.dbs, nil
}

func (r *recordingRootCatalog) GetCollectionByID(ctx context.Context, dbID int64, ts uint64, collectionID int64) (*model.Collection, error) {
	r.ts = ts
	return r.collection, nil
}

func (r *recordingRootCatalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts uint64) error {
	r.oldColl = oldColl
	r.newColl = newColl
	r.ts = ts
	return nil
}

func (r *recordingRootCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	r.fileResource = resource
	r.ts = version
	return nil
}

func TestWrapMilvusCatalogsCanRoundTripThroughCatalogInterface(t *testing.T) {
	root := &recordingRootCatalog{}
	c := Wrap(Catalogs{RootCoord: root})

	catalogs := New(c)
	db := &model.Database{ID: 10, Name: "db"}
	require.NoError(t, catalogs.RootCoord.CreateDatabase(context.Background(), db, 100))

	require.Same(t, db, root.db)
	require.EqualValues(t, 100, root.ts)
}

func TestWrapDatabasesGetUsesListAndFilters(t *testing.T) {
	root := &recordingRootCatalog{
		dbs: []*model.Database{
			{ID: 1, Name: "default"},
			{ID: 2, Name: "analytics"},
		},
	}
	c := Wrap(Catalogs{RootCoord: root})

	byID, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{ID: 2}, catalog.ReadOptions{At: 101})
	require.NoError(t, err)
	require.Equal(t, int64(2), byID.ID)
	require.EqualValues(t, 101, root.ts)

	byName, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{Name: "default"}, catalog.ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), byName.ID)
}

func TestWrapPartitionsListReadsCollectionPartitions(t *testing.T) {
	partitions := []*model.Partition{{PartitionID: 10, PartitionName: "p1"}}
	root := &recordingRootCatalog{
		collection: &model.Collection{CollectionID: 100, Partitions: partitions},
	}
	c := Wrap(Catalogs{RootCoord: root})

	got, err := c.Metadata().Partitions().List(
		context.Background(),
		catalog.CollectionRef{Database: catalog.DatabaseRef{ID: 1}, ID: 100},
		catalog.ReadOptions{At: 202},
	)
	require.NoError(t, err)
	require.Same(t, partitions[0], got[0])
	require.EqualValues(t, 202, root.ts)
}

func TestWrapCollectionsMoveDatabaseDelegatesToAlterCollectionDB(t *testing.T) {
	root := &recordingRootCatalog{}
	c := Wrap(Catalogs{RootCoord: root})

	err := c.Metadata().Collections().MoveDatabase(
		context.Background(),
		catalog.CollectionRef{Database: catalog.DatabaseRef{ID: 1, Name: "old_db"}, ID: 10, Name: "coll"},
		catalog.DatabaseRef{ID: 2, Name: "new_db"},
		catalog.WriteOptions{Timestamp: 303},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), root.oldColl.DBID)
	require.Equal(t, "old_db", root.oldColl.DBName)
	require.Equal(t, int64(2), root.newColl.DBID)
	require.Equal(t, "new_db", root.newColl.DBName)
	require.EqualValues(t, 303, root.ts)
}

func TestWrapSnapshotsWithoutOldEquivalentReturnUnsupported(t *testing.T) {
	c := Wrap(Catalogs{DataCoord: &recordingDataCatalog{}})

	_, err := c.Metadata().Snapshots().Get(context.Background(), catalog.GetSnapshotRequest{}, catalog.ReadOptions{})
	require.ErrorIs(t, err, catalog.ErrUnsupportedImplementation)

	_, err = c.Metadata().Snapshots().ListManifests(context.Background(), catalog.ListManifestsRequest{}, catalog.ReadOptions{})
	require.ErrorIs(t, err, catalog.ErrUnsupportedImplementation)
}

func TestWrapFilesPrefersRootCatalog(t *testing.T) {
	root := &recordingRootCatalog{}
	data := &recordingDataCatalog{}
	c := Wrap(Catalogs{RootCoord: root, DataCoord: data})

	resource := &internalpb.FileResourceInfo{Name: "dictionary"}
	require.NoError(t, c.Metadata().Files().Save(context.Background(), resource, 404, catalog.WriteOptions{}))

	require.Same(t, resource, root.fileResource)
	require.Nil(t, data.fileResource)
	require.EqualValues(t, 404, root.ts)
}

func TestNewOldBoolAPICollapsesErrorsToFalse(t *testing.T) {
	catalogs := New(failingCollectionExistsCatalog{})

	require.False(t, catalogs.RootCoord.CollectionExists(context.Background(), 1, 2, 3))
}

type recordingDataCatalog struct {
	metastore.DataCoordCatalog
	fileResource *internalpb.FileResourceInfo
}

func (r *recordingDataCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	r.fileResource = resource
	return nil
}

func TestWrapDatabasesGetReturnsNotFound(t *testing.T) {
	c := Wrap(Catalogs{RootCoord: &recordingRootCatalog{dbs: []*model.Database{{ID: 1, Name: "default"}}}})

	_, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{ID: 2}, catalog.ReadOptions{})
	require.True(t, errors.Is(err, catalog.ErrNotFound))
}

type failingCollectionExistsCatalog struct {
	catalog.Catalog
}

func (failingCollectionExistsCatalog) Metadata() catalog.MetadataCatalog {
	return failingCollectionExistsMetadata{}
}

type failingCollectionExistsMetadata struct {
	catalog.MetadataCatalog
}

func (failingCollectionExistsMetadata) Collections() catalog.CollectionCatalog {
	return failingCollectionExistsCollections{}
}

type failingCollectionExistsCollections struct {
	catalog.CollectionCatalog
}

func (failingCollectionExistsCollections) Exists(ctx context.Context, ref catalog.CollectionRef, opts catalog.ReadOptions) (bool, error) {
	return true, errors.New("collection exists failed")
}
