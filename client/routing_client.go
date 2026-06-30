package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// callErr reduces a wrapped call to the error Router.Do should act on: the transport error if
// the RPC itself failed, otherwise the business error carried in the response Status. Milvus
// returns not-owner / fencing failures inside Status with a nil gRPC error, so without this a
// not-owner response would look like success to Router.Do and the redirect would never fire.
func callErr(transport error, st *commonpb.Status) error {
	if transport != nil {
		return transport
	}
	return merr.Error(st)
}

// routingClient is a catalogpb.CatalogServiceClient bound to one namespace that sends every
// call through the Router: it resolves the namespace's owner, dials it, and on a not-owner /
// unavailable response re-discovers and redirects. Plugging it into rootcoord's remoteMetaTable
// makes the coord talk to the pooled catalog service through discovery + failover, unchanged.
type routingClient struct {
	router    *Router
	namespace string
}

// NewRoutingClient wraps a Router as a namespace-bound CatalogServiceClient.
func NewRoutingClient(router *Router, namespace string) catalogpb.CatalogServiceClient {
	return &routingClient{router: router, namespace: namespace}
}

var _ catalogpb.CatalogServiceClient = (*routingClient)(nil)

func (rc *routingClient) CreateDatabase(ctx context.Context, in *catalogpb.CreateDatabaseRequest, opts ...grpc.CallOption) (*catalogpb.CreateDatabaseResponse, error) {
	var out *catalogpb.CreateDatabaseResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CreateDatabase(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropDatabase(ctx context.Context, in *catalogpb.DropDatabaseRequest, opts ...grpc.CallOption) (*catalogpb.DropDatabaseResponse, error) {
	var out *catalogpb.DropDatabaseResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropDatabase(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AlterDatabase(ctx context.Context, in *catalogpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*catalogpb.AlterDatabaseResponse, error) {
	var out *catalogpb.AlterDatabaseResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AlterDatabase(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListDatabases(ctx context.Context, in *catalogpb.ListDatabasesRequest, opts ...grpc.CallOption) (*catalogpb.ListDatabasesResponse, error) {
	var out *catalogpb.ListDatabasesResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListDatabases(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetDatabaseByName(ctx context.Context, in *catalogpb.GetDatabaseByNameRequest, opts ...grpc.CallOption) (*catalogpb.GetDatabaseResponse, error) {
	var out *catalogpb.GetDatabaseResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetDatabaseByName(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetDatabaseByID(ctx context.Context, in *catalogpb.GetDatabaseByIDRequest, opts ...grpc.CallOption) (*catalogpb.GetDatabaseResponse, error) {
	var out *catalogpb.GetDatabaseResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetDatabaseByID(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AddCollection(ctx context.Context, in *catalogpb.AddCollectionRequest, opts ...grpc.CallOption) (*catalogpb.AddCollectionResponse, error) {
	var out *catalogpb.AddCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AddCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropCollection(ctx context.Context, in *catalogpb.DropCollectionRequest, opts ...grpc.CallOption) (*catalogpb.DropCollectionResponse, error) {
	var out *catalogpb.DropCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) RemoveCollection(ctx context.Context, in *catalogpb.RemoveCollectionRequest, opts ...grpc.CallOption) (*catalogpb.RemoveCollectionResponse, error) {
	var out *catalogpb.RemoveCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.RemoveCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCollectionID(ctx context.Context, in *catalogpb.GetCollectionIDRequest, opts ...grpc.CallOption) (*catalogpb.GetCollectionIDResponse, error) {
	var out *catalogpb.GetCollectionIDResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCollectionID(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCollectionByName(ctx context.Context, in *catalogpb.GetCollectionByNameRequest, opts ...grpc.CallOption) (*catalogpb.GetCollectionByNameResponse, error) {
	var out *catalogpb.GetCollectionByNameResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCollectionByName(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCollectionByID(ctx context.Context, in *catalogpb.GetCollectionByIDRequest, opts ...grpc.CallOption) (*catalogpb.GetCollectionByIDResponse, error) {
	var out *catalogpb.GetCollectionByIDResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCollectionByID(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCollectionByIDWithMaxTs(ctx context.Context, in *catalogpb.GetCollectionByIDWithMaxTsRequest, opts ...grpc.CallOption) (*catalogpb.GetCollectionByIDWithMaxTsResponse, error) {
	var out *catalogpb.GetCollectionByIDWithMaxTsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCollectionByIDWithMaxTs(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListCollections(ctx context.Context, in *catalogpb.ListCollectionsRequest, opts ...grpc.CallOption) (*catalogpb.ListCollectionsResponse, error) {
	var out *catalogpb.ListCollectionsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListCollections(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListAllAvailCollections(ctx context.Context, in *catalogpb.ListAllAvailCollectionsRequest, opts ...grpc.CallOption) (*catalogpb.ListAllAvailCollectionsResponse, error) {
	var out *catalogpb.ListAllAvailCollectionsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListAllAvailCollections(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListAllAvailPartitions(ctx context.Context, in *catalogpb.ListAllAvailPartitionsRequest, opts ...grpc.CallOption) (*catalogpb.ListAllAvailPartitionsResponse, error) {
	var out *catalogpb.ListAllAvailPartitionsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListAllAvailPartitions(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListCollectionPhysicalChannels(ctx context.Context, in *catalogpb.ListCollectionPhysicalChannelsRequest, opts ...grpc.CallOption) (*catalogpb.ListCollectionPhysicalChannelsResponse, error) {
	var out *catalogpb.ListCollectionPhysicalChannelsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListCollectionPhysicalChannels(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCollectionVirtualChannels(ctx context.Context, in *catalogpb.GetCollectionVirtualChannelsRequest, opts ...grpc.CallOption) (*catalogpb.GetCollectionVirtualChannelsResponse, error) {
	var out *catalogpb.GetCollectionVirtualChannelsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCollectionVirtualChannels(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetPChannelInfo(ctx context.Context, in *catalogpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*catalogpb.GetPChannelInfoResponse, error) {
	var out *catalogpb.GetPChannelInfoResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetPChannelInfo(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfCollectionRenamable(ctx context.Context, in *catalogpb.CheckIfCollectionRenamableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfCollectionRenamableResponse, error) {
	var out *catalogpb.CheckIfCollectionRenamableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfCollectionRenamable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) BeginTruncateCollection(ctx context.Context, in *catalogpb.BeginTruncateCollectionRequest, opts ...grpc.CallOption) (*catalogpb.BeginTruncateCollectionResponse, error) {
	var out *catalogpb.BeginTruncateCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.BeginTruncateCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetGeneralCount(ctx context.Context, in *catalogpb.GetGeneralCountRequest, opts ...grpc.CallOption) (*catalogpb.GetGeneralCountResponse, error) {
	var out *catalogpb.GetGeneralCountResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetGeneralCount(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AlterCollection(ctx context.Context, in *catalogpb.AlterCollectionRequest, opts ...grpc.CallOption) (*catalogpb.AlterCollectionResponse, error) {
	var out *catalogpb.AlterCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AlterCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) TruncateCollection(ctx context.Context, in *catalogpb.TruncateCollectionRequest, opts ...grpc.CallOption) (*catalogpb.TruncateCollectionResponse, error) {
	var out *catalogpb.TruncateCollectionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.TruncateCollection(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AddPartition(ctx context.Context, in *catalogpb.AddPartitionRequest, opts ...grpc.CallOption) (*catalogpb.AddPartitionResponse, error) {
	var out *catalogpb.AddPartitionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AddPartition(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetPartitionIDByName(ctx context.Context, in *catalogpb.GetPartitionIDByNameRequest, opts ...grpc.CallOption) (*catalogpb.GetPartitionIDByNameResponse, error) {
	var out *catalogpb.GetPartitionIDByNameResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetPartitionIDByName(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropPartition(ctx context.Context, in *catalogpb.DropPartitionRequest, opts ...grpc.CallOption) (*catalogpb.DropPartitionResponse, error) {
	var out *catalogpb.DropPartitionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropPartition(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) RemovePartition(ctx context.Context, in *catalogpb.RemovePartitionRequest, opts ...grpc.CallOption) (*catalogpb.RemovePartitionResponse, error) {
	var out *catalogpb.RemovePartitionResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.RemovePartition(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AlterAlias(ctx context.Context, in *catalogpb.AlterAliasRequest, opts ...grpc.CallOption) (*catalogpb.AlterAliasResponse, error) {
	var out *catalogpb.AlterAliasResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AlterAlias(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropAlias(ctx context.Context, in *catalogpb.DropAliasRequest, opts ...grpc.CallOption) (*catalogpb.DropAliasResponse, error) {
	var out *catalogpb.DropAliasResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropAlias(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DescribeAlias(ctx context.Context, in *catalogpb.DescribeAliasRequest, opts ...grpc.CallOption) (*catalogpb.DescribeAliasResponse, error) {
	var out *catalogpb.DescribeAliasResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DescribeAlias(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListAliases(ctx context.Context, in *catalogpb.ListAliasesRequest, opts ...grpc.CallOption) (*catalogpb.ListAliasesResponse, error) {
	var out *catalogpb.ListAliasesResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListAliases(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) IsAlias(ctx context.Context, in *catalogpb.IsAliasRequest, opts ...grpc.CallOption) (*catalogpb.IsAliasResponse, error) {
	var out *catalogpb.IsAliasResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.IsAlias(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListAliasesByID(ctx context.Context, in *catalogpb.ListAliasesByIDRequest, opts ...grpc.CallOption) (*catalogpb.ListAliasesByIDResponse, error) {
	var out *catalogpb.ListAliasesByIDResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListAliasesByID(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfAliasCreatable(ctx context.Context, in *catalogpb.CheckIfAliasCreatableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfAliasCreatableResponse, error) {
	var out *catalogpb.CheckIfAliasCreatableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfAliasCreatable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfAliasAlterable(ctx context.Context, in *catalogpb.CheckIfAliasAlterableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfAliasAlterableResponse, error) {
	var out *catalogpb.CheckIfAliasAlterableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfAliasAlterable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfAliasDroppable(ctx context.Context, in *catalogpb.CheckIfAliasDroppableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfAliasDroppableResponse, error) {
	var out *catalogpb.CheckIfAliasDroppableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfAliasDroppable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) InitCredential(ctx context.Context, in *catalogpb.InitCredentialRequest, opts ...grpc.CallOption) (*catalogpb.InitCredentialResponse, error) {
	var out *catalogpb.InitCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.InitCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AlterCredential(ctx context.Context, in *catalogpb.AlterCredentialRequest, opts ...grpc.CallOption) (*catalogpb.AlterCredentialResponse, error) {
	var out *catalogpb.AlterCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AlterCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DeleteCredential(ctx context.Context, in *catalogpb.DeleteCredentialRequest, opts ...grpc.CallOption) (*catalogpb.DeleteCredentialResponse, error) {
	var out *catalogpb.DeleteCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DeleteCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetCredential(ctx context.Context, in *catalogpb.GetCredentialRequest, opts ...grpc.CallOption) (*catalogpb.GetCredentialResponse, error) {
	var out *catalogpb.GetCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListCredentialUsernames(ctx context.Context, in *catalogpb.ListCredentialUsernamesRequest, opts ...grpc.CallOption) (*catalogpb.ListCredentialUsernamesResponse, error) {
	var out *catalogpb.ListCredentialUsernamesResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListCredentialUsernames(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfAddCredential(ctx context.Context, in *catalogpb.CheckIfAddCredentialRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfAddCredentialResponse, error) {
	var out *catalogpb.CheckIfAddCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfAddCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfUpdateCredential(ctx context.Context, in *catalogpb.CheckIfUpdateCredentialRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfUpdateCredentialResponse, error) {
	var out *catalogpb.CheckIfUpdateCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfUpdateCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfDeleteCredential(ctx context.Context, in *catalogpb.CheckIfDeleteCredentialRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfDeleteCredentialResponse, error) {
	var out *catalogpb.CheckIfDeleteCredentialResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfDeleteCredential(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CreateRole(ctx context.Context, in *catalogpb.CreateRoleRequest, opts ...grpc.CallOption) (*catalogpb.CreateRoleResponse, error) {
	var out *catalogpb.CreateRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CreateRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AlterRole(ctx context.Context, in *catalogpb.AlterRoleRequest, opts ...grpc.CallOption) (*catalogpb.AlterRoleResponse, error) {
	var out *catalogpb.AlterRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AlterRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropRole(ctx context.Context, in *catalogpb.DropRoleRequest, opts ...grpc.CallOption) (*catalogpb.DropRoleResponse, error) {
	var out *catalogpb.DropRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) OperateUserRole(ctx context.Context, in *catalogpb.OperateUserRoleRequest, opts ...grpc.CallOption) (*catalogpb.OperateUserRoleResponse, error) {
	var out *catalogpb.OperateUserRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.OperateUserRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) SelectRole(ctx context.Context, in *catalogpb.SelectRoleRequest, opts ...grpc.CallOption) (*catalogpb.SelectRoleResponse, error) {
	var out *catalogpb.SelectRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.SelectRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) SelectUser(ctx context.Context, in *catalogpb.SelectUserRequest, opts ...grpc.CallOption) (*catalogpb.SelectUserResponse, error) {
	var out *catalogpb.SelectUserResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.SelectUser(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListUserRole(ctx context.Context, in *catalogpb.ListUserRoleRequest, opts ...grpc.CallOption) (*catalogpb.ListUserRoleResponse, error) {
	var out *catalogpb.ListUserRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListUserRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfCreateRole(ctx context.Context, in *catalogpb.CheckIfCreateRoleRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfCreateRoleResponse, error) {
	var out *catalogpb.CheckIfCreateRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfCreateRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfAlterRole(ctx context.Context, in *catalogpb.CheckIfAlterRoleRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfAlterRoleResponse, error) {
	var out *catalogpb.CheckIfAlterRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfAlterRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfDropRole(ctx context.Context, in *catalogpb.CheckIfDropRoleRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfDropRoleResponse, error) {
	var out *catalogpb.CheckIfDropRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfDropRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfOperateUserRole(ctx context.Context, in *catalogpb.CheckIfOperateUserRoleRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfOperateUserRoleResponse, error) {
	var out *catalogpb.CheckIfOperateUserRoleResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfOperateUserRole(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) OperatePrivilege(ctx context.Context, in *catalogpb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*catalogpb.OperatePrivilegeResponse, error) {
	var out *catalogpb.OperatePrivilegeResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.OperatePrivilege(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropGrant(ctx context.Context, in *catalogpb.DropGrantRequest, opts ...grpc.CallOption) (*catalogpb.DropGrantResponse, error) {
	var out *catalogpb.DropGrantResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropGrant(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) RestoreRBAC(ctx context.Context, in *catalogpb.RestoreRBACRequest, opts ...grpc.CallOption) (*catalogpb.RestoreRBACResponse, error) {
	var out *catalogpb.RestoreRBACResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.RestoreRBAC(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) SelectGrant(ctx context.Context, in *catalogpb.SelectGrantRequest, opts ...grpc.CallOption) (*catalogpb.SelectGrantResponse, error) {
	var out *catalogpb.SelectGrantResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.SelectGrant(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListPolicy(ctx context.Context, in *catalogpb.ListPolicyRequest, opts ...grpc.CallOption) (*catalogpb.ListPolicyResponse, error) {
	var out *catalogpb.ListPolicyResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListPolicy(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) BackupRBAC(ctx context.Context, in *catalogpb.BackupRBACRequest, opts ...grpc.CallOption) (*catalogpb.BackupRBACResponse, error) {
	var out *catalogpb.BackupRBACResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.BackupRBAC(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfRBACRestorable(ctx context.Context, in *catalogpb.CheckIfRBACRestorableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfRBACRestorableResponse, error) {
	var out *catalogpb.CheckIfRBACRestorableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfRBACRestorable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CreatePrivilegeGroup(ctx context.Context, in *catalogpb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*catalogpb.CreatePrivilegeGroupResponse, error) {
	var out *catalogpb.CreatePrivilegeGroupResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CreatePrivilegeGroup(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DropPrivilegeGroup(ctx context.Context, in *catalogpb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*catalogpb.DropPrivilegeGroupResponse, error) {
	var out *catalogpb.DropPrivilegeGroupResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DropPrivilegeGroup(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) OperatePrivilegeGroup(ctx context.Context, in *catalogpb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*catalogpb.OperatePrivilegeGroupResponse, error) {
	var out *catalogpb.OperatePrivilegeGroupResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.OperatePrivilegeGroup(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) IsCustomPrivilegeGroup(ctx context.Context, in *catalogpb.IsCustomPrivilegeGroupRequest, opts ...grpc.CallOption) (*catalogpb.IsCustomPrivilegeGroupResponse, error) {
	var out *catalogpb.IsCustomPrivilegeGroupResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.IsCustomPrivilegeGroup(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListPrivilegeGroups(ctx context.Context, in *catalogpb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*catalogpb.ListPrivilegeGroupsResponse, error) {
	var out *catalogpb.ListPrivilegeGroupsResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListPrivilegeGroups(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetPrivilegeGroupRoles(ctx context.Context, in *catalogpb.GetPrivilegeGroupRolesRequest, opts ...grpc.CallOption) (*catalogpb.GetPrivilegeGroupRolesResponse, error) {
	var out *catalogpb.GetPrivilegeGroupRolesResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetPrivilegeGroupRoles(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfPrivilegeGroupCreatable(ctx context.Context, in *catalogpb.CheckIfPrivilegeGroupCreatableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfPrivilegeGroupCreatableResponse, error) {
	var out *catalogpb.CheckIfPrivilegeGroupCreatableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfPrivilegeGroupCreatable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfPrivilegeGroupAlterable(ctx context.Context, in *catalogpb.CheckIfPrivilegeGroupAlterableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfPrivilegeGroupAlterableResponse, error) {
	var out *catalogpb.CheckIfPrivilegeGroupAlterableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfPrivilegeGroupAlterable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfPrivilegeGroupDropable(ctx context.Context, in *catalogpb.CheckIfPrivilegeGroupDropableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfPrivilegeGroupDropableResponse, error) {
	var out *catalogpb.CheckIfPrivilegeGroupDropableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfPrivilegeGroupDropable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfDatabaseCreatable(ctx context.Context, in *catalogpb.CheckIfDatabaseCreatableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfDatabaseCreatableResponse, error) {
	var out *catalogpb.CheckIfDatabaseCreatableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfDatabaseCreatable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) CheckIfDatabaseDroppable(ctx context.Context, in *catalogpb.CheckIfDatabaseDroppableRequest, opts ...grpc.CallOption) (*catalogpb.CheckIfDatabaseDroppableResponse, error) {
	var out *catalogpb.CheckIfDatabaseDroppableResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.CheckIfDatabaseDroppable(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) AddFileResource(ctx context.Context, in *catalogpb.AddFileResourceRequest, opts ...grpc.CallOption) (*catalogpb.AddFileResourceResponse, error) {
	var out *catalogpb.AddFileResourceResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.AddFileResource(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) RemoveFileResource(ctx context.Context, in *catalogpb.RemoveFileResourceRequest, opts ...grpc.CallOption) (*catalogpb.RemoveFileResourceResponse, error) {
	var out *catalogpb.RemoveFileResourceResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.RemoveFileResource(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) ListFileResource(ctx context.Context, in *catalogpb.ListFileResourceRequest, opts ...grpc.CallOption) (*catalogpb.ListFileResourceResponse, error) {
	var out *catalogpb.ListFileResourceResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.ListFileResource(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

// IncFileResourceRefCnt applies a +1 ref-count delta, so a blind re-send on an ambiguous
// transport failure would double-increment — routed through DoNonIdempotent, which redirects on
// not-owner but does not auto-retry ambiguous transport errors.
func (rc *routingClient) IncFileResourceRefCnt(ctx context.Context, in *catalogpb.IncFileResourceRefCntRequest, opts ...grpc.CallOption) (*catalogpb.IncFileResourceRefCntResponse, error) {
	var out *catalogpb.IncFileResourceRefCntResponse
	err := rc.router.DoNonIdempotent(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.IncFileResourceRefCnt(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

// DecFileResourceRefCnt applies a -1 ref-count delta; see IncFileResourceRefCnt for why it must
// not be blindly retried. RecoverFileResourceRefCnt is left on Do: it rebuilds counts from an
// absolute snapshot, so re-sending it is idempotent.
func (rc *routingClient) DecFileResourceRefCnt(ctx context.Context, in *catalogpb.DecFileResourceRefCntRequest, opts ...grpc.CallOption) (*catalogpb.DecFileResourceRefCntResponse, error) {
	var out *catalogpb.DecFileResourceRefCntResponse
	err := rc.router.DoNonIdempotent(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DecFileResourceRefCnt(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) RecoverFileResourceRefCnt(ctx context.Context, in *catalogpb.RecoverFileResourceRefCntRequest, opts ...grpc.CallOption) (*catalogpb.RecoverFileResourceRefCntResponse, error) {
	var out *catalogpb.RecoverFileResourceRefCntResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.RecoverFileResourceRefCnt(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) BulkImport(ctx context.Context, in *catalogpb.BulkImportRequest, opts ...grpc.CallOption) (*catalogpb.BulkImportResponse, error) {
	var out *catalogpb.BulkImportResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.BulkImport(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) VerifyImport(ctx context.Context, in *catalogpb.VerifyImportRequest, opts ...grpc.CallOption) (*catalogpb.VerifyImportResponse, error) {
	var out *catalogpb.VerifyImportResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.VerifyImport(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) GetRouteMap(ctx context.Context, in *catalogpb.GetRouteMapRequest, opts ...grpc.CallOption) (*catalogpb.GetRouteMapResponse, error) {
	var out *catalogpb.GetRouteMapResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.GetRouteMap(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}

func (rc *routingClient) DeleteNamespace(ctx context.Context, in *catalogpb.DeleteNamespaceRequest, opts ...grpc.CallOption) (*catalogpb.DeleteNamespaceResponse, error) {
	var out *catalogpb.DeleteNamespaceResponse
	err := rc.router.Do(ctx, rc.namespace, func(c context.Context, cli catalogpb.CatalogServiceClient) error {
		var e error
		out, e = cli.DeleteNamespace(c, in, opts...)
		return callErr(e, out.GetStatus())
	})
	return out, err
}
