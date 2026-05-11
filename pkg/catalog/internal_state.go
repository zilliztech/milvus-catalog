package catalog

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type MilvusStateCatalog interface {
	DataCoord() DataCoordStateCatalog
	QueryCoord() QueryCoordStateCatalog
	Streaming() StreamingStateCatalog
}

type DataCoordStateCatalog interface {
	MarkChannelAdded(ctx context.Context, channel string, opts WriteOptions) error
	MarkChannelDeleted(ctx context.Context, channel string, opts WriteOptions) error
	ShouldDropChannel(ctx context.Context, channel string, opts ReadOptions) (bool, error)
	ChannelExists(ctx context.Context, channel string, opts ReadOptions) (bool, error)
	DropChannel(ctx context.Context, channel string, opts WriteOptions) error
	SaveImportJob(ctx context.Context, job *datapb.ImportJob, opts WriteOptions) error
	ListImportJobs(ctx context.Context, opts ReadOptions) ([]*datapb.ImportJob, error)
	DropImportJob(ctx context.Context, jobID int64, opts WriteOptions) error
	SavePreImportTask(ctx context.Context, task *datapb.PreImportTask, opts WriteOptions) error
	ListPreImportTasks(ctx context.Context, opts ReadOptions) ([]*datapb.PreImportTask, error)
	DropPreImportTask(ctx context.Context, taskID int64, opts WriteOptions) error
	SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2, opts WriteOptions) error
	ListImportTasks(ctx context.Context, opts ReadOptions) ([]*datapb.ImportTaskV2, error)
	DropImportTask(ctx context.Context, taskID int64, opts WriteOptions) error
	SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob, opts WriteOptions) error
	ListCopySegmentJobs(ctx context.Context, opts ReadOptions) ([]*datapb.CopySegmentJob, error)
	DropCopySegmentJob(ctx context.Context, jobID int64, opts WriteOptions) error
	SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask, opts WriteOptions) error
	SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask, opts WriteOptions) error
	ListCopySegmentTasks(ctx context.Context, opts ReadOptions) ([]*datapb.CopySegmentTask, error)
	DropCopySegmentTask(ctx context.Context, taskID int64, opts WriteOptions) error
	GcConfirm(ctx context.Context, collectionID UniqueID, partitionID UniqueID, opts ReadOptions) (bool, error)
	SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts WriteOptions) error
	ListCompactionTasks(ctx context.Context, opts ReadOptions) ([]*datapb.CompactionTask, error)
	DropCompactionTask(ctx context.Context, task *datapb.CompactionTask, opts WriteOptions) error
	SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask, opts WriteOptions) error
	ListAnalyzeTasks(ctx context.Context, opts ReadOptions) ([]*indexpb.AnalyzeTask, error)
	DropAnalyzeTask(ctx context.Context, taskID UniqueID, opts WriteOptions) error
	SaveStatsTask(ctx context.Context, task *indexpb.StatsTask, opts WriteOptions) error
	ListStatsTasks(ctx context.Context, opts ReadOptions) ([]*indexpb.StatsTask, error)
	DropStatsTask(ctx context.Context, taskID UniqueID, opts WriteOptions) error
	SaveChannelCheckpoint(ctx context.Context, vchannel string, position *msgpb.MsgPosition, opts WriteOptions) error
	SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition, opts WriteOptions) error
	ListChannelCheckpoints(ctx context.Context, opts ReadOptions) (map[string]*msgpb.MsgPosition, error)
	DropChannelCheckpoint(ctx context.Context, vchannel string, opts WriteOptions) error
	ListPartitionStatsInfos(ctx context.Context, opts ReadOptions) ([]*datapb.PartitionStatsInfo, error)
	SavePartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts WriteOptions) error
	DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo, opts WriteOptions) error
	SaveCurrentPartitionStatsVersion(ctx context.Context, collID UniqueID, partID UniqueID, vchannel string, currentVersion int64, opts WriteOptions) error
	GetCurrentPartitionStatsVersion(ctx context.Context, collID UniqueID, partID UniqueID, vchannel string, opts ReadOptions) (int64, error)
	DropCurrentPartitionStatsVersion(ctx context.Context, collID UniqueID, partID UniqueID, vchannel string, opts WriteOptions) error
	ListExternalCollectionRefreshJobs(ctx context.Context, opts ReadOptions) ([]*datapb.ExternalCollectionRefreshJob, error)
	SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob, opts WriteOptions) error
	DropExternalCollectionRefreshJob(ctx context.Context, jobID UniqueID, opts WriteOptions) error
	ListExternalCollectionRefreshTasks(ctx context.Context, opts ReadOptions) ([]*datapb.ExternalCollectionRefreshTask, error)
	SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask, opts WriteOptions) error
	DropExternalCollectionRefreshTask(ctx context.Context, taskID UniqueID, opts WriteOptions) error
}

type QueryCoordStateCatalog interface {
	SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions []*querypb.PartitionLoadInfo, opts WriteOptions) error
	SavePartition(ctx context.Context, partitions []*querypb.PartitionLoadInfo, opts WriteOptions) error
	SaveReplica(ctx context.Context, replicas []*querypb.Replica, opts WriteOptions) error
	GetCollections(ctx context.Context, opts ReadOptions) ([]*querypb.CollectionLoadInfo, error)
	GetPartitions(ctx context.Context, collectionIDs []int64, opts ReadOptions) (map[int64][]*querypb.PartitionLoadInfo, error)
	GetReplicas(ctx context.Context, opts ReadOptions) ([]*querypb.Replica, error)
	ReleaseCollection(ctx context.Context, collectionID UniqueID, opts WriteOptions) error
	ReleasePartition(ctx context.Context, collectionID UniqueID, partitionIDs []UniqueID, opts WriteOptions) error
	ReleaseReplicas(ctx context.Context, collectionID UniqueID, opts WriteOptions) error
	ReleaseReplica(ctx context.Context, collectionID UniqueID, replicaIDs []UniqueID, opts WriteOptions) error
	SaveResourceGroup(ctx context.Context, groups []*querypb.ResourceGroup, opts WriteOptions) error
	RemoveResourceGroup(ctx context.Context, rgName string, opts WriteOptions) error
	GetResourceGroups(ctx context.Context, opts ReadOptions) ([]*querypb.ResourceGroup, error)
	SaveCollectionTargets(ctx context.Context, targets []*querypb.CollectionTarget, opts WriteOptions) error
	RemoveCollectionTarget(ctx context.Context, collectionID UniqueID, opts WriteOptions) error
	RemoveCollectionTargets(ctx context.Context, opts WriteOptions) error
	GetCollectionTargets(ctx context.Context, opts ReadOptions) (map[int64]*querypb.CollectionTarget, error)
}

type StreamingStateCatalog interface {
	GetCChannel(ctx context.Context, opts ReadOptions) (*streamingpb.CChannelMeta, error)
	SaveCChannel(ctx context.Context, channel *streamingpb.CChannelMeta, opts WriteOptions) error
	GetVersion(ctx context.Context, opts ReadOptions) (*streamingpb.StreamingVersion, error)
	SaveVersion(ctx context.Context, version *streamingpb.StreamingVersion, opts WriteOptions) error
	ListPChannels(ctx context.Context, opts ReadOptions) ([]*streamingpb.PChannelMeta, error)
	SavePChannels(ctx context.Context, channels []*streamingpb.PChannelMeta, opts WriteOptions) error
	ListBroadcastTasks(ctx context.Context, opts ReadOptions) ([]*streamingpb.BroadcastTask, error)
	SaveBroadcastTask(ctx context.Context, broadcastID uint64, task *streamingpb.BroadcastTask, opts WriteOptions) error
	SaveReplicateConfiguration(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta, opts WriteOptions) error
	GetReplicateConfiguration(ctx context.Context, opts ReadOptions) (*streamingpb.ReplicateConfigurationMeta, error)
	ListVChannels(ctx context.Context, pchannel string, opts ReadOptions) ([]*streamingpb.VChannelMeta, error)
	SaveVChannels(ctx context.Context, pchannel string, channels map[string]*streamingpb.VChannelMeta, opts WriteOptions) error
	ListSegmentAssignments(ctx context.Context, pchannel string, opts ReadOptions) ([]*streamingpb.SegmentAssignmentMeta, error)
	SaveSegmentAssignments(ctx context.Context, pchannel string, assignments map[int64]*streamingpb.SegmentAssignmentMeta, opts WriteOptions) error
	GetConsumeCheckpoint(ctx context.Context, pchannel string, opts ReadOptions) (*streamingpb.WALCheckpoint, error)
	SaveConsumeCheckpoint(ctx context.Context, pchannel string, checkpoint *streamingpb.WALCheckpoint, opts WriteOptions) error
	SaveSalvageCheckpoint(ctx context.Context, pchannel string, checkpoint *commonpb.ReplicateCheckpoint, opts WriteOptions) error
	GetSalvageCheckpoint(ctx context.Context, pchannel string, opts ReadOptions) ([]*commonpb.ReplicateCheckpoint, error)
}
