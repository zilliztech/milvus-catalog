package routing

import (
	"context"
	"path"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Ownership keys record which node owns each shard. They are written under the owner's
// membership lease so a crashed owner's shards free themselves when the lease expires.
// Claims use compare-and-swap (txn on key-absent) so two nodes proposing the same shard
// cannot both win — etcd is the final arbiter, matching the reconcile loop's optimistic
// claim model.

func shardKey(prefix string, shard int) string {
	return path.Join(prefix, "ownership", "shard", strconv.Itoa(shard))
}

// ClaimShard atomically claims an unowned shard for nodeID under its lease. On success it
// returns the term (the etcd revision at which the claim committed): a later owner always
// gets a strictly higher term, which fences a stale owner's writes. Returns (false, 0, nil)
// if the shard is already owned.
func ClaimShard(ctx context.Context, cli *clientv3.Client, prefix string, shard int, nodeID string, lease clientv3.LeaseID) (bool, int64, error) {
	key := shardKey(prefix, shard)
	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)). // key absent
		Then(clientv3.OpPut(key, nodeID, clientv3.WithLease(lease))).
		Commit()
	if err != nil {
		return false, 0, err
	}
	if !resp.Succeeded {
		return false, 0, nil
	}
	return true, resp.Header.Revision, nil
}

// OwnerInfo is a shard's current owner and its claim term (the ownership key's ModRevision).
type OwnerInfo struct {
	Owner string
	Term  int64
}

// LoadOwnershipMap reads the full shard -> {owner, term} map. Unowned shards are zero-valued.
func LoadOwnershipMap(ctx context.Context, cli *clientv3.Client, prefix string) ([ShardCount]OwnerInfo, error) {
	var om [ShardCount]OwnerInfo
	base := path.Join(prefix, "ownership", "shard") + "/"
	resp, err := cli.Get(ctx, base, clientv3.WithPrefix())
	if err != nil {
		return om, err
	}
	for _, kv := range resp.Kvs {
		s, err := strconv.Atoi(string(kv.Key)[len(base):])
		if err != nil || s < 0 || s >= ShardCount {
			continue
		}
		om[s] = OwnerInfo{Owner: string(kv.Value), Term: kv.ModRevision}
	}
	return om, nil
}

// ReleaseShard deletes the ownership key only if nodeID still owns it (CAS on value), so a
// stale node never deletes another's ownership. Releasing a shard not owned by nodeID is a
// no-op.
func ReleaseShard(ctx context.Context, cli *clientv3.Client, prefix string, shard int, nodeID string) error {
	key := shardKey(prefix, shard)
	_, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", nodeID)).
		Then(clientv3.OpDelete(key)).
		Commit()
	return err
}

// ShardOwner returns the current owner of a shard, if any.
func ShardOwner(ctx context.Context, cli *clientv3.Client, prefix string, shard int) (string, bool, error) {
	resp, err := cli.Get(ctx, shardKey(prefix, shard))
	if err != nil {
		return "", false, err
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	return string(resp.Kvs[0].Value), true, nil
}

// LoadShardMap reads the full shard→owner map (unowned shards left as "").
func LoadShardMap(ctx context.Context, cli *clientv3.Client, prefix string) ([ShardCount]string, error) {
	var sm [ShardCount]string
	resp, err := cli.Get(ctx, path.Join(prefix, "ownership", "shard")+"/", clientv3.WithPrefix())
	if err != nil {
		return sm, err
	}
	base := path.Join(prefix, "ownership", "shard") + "/"
	for _, kv := range resp.Kvs {
		s, err := strconv.Atoi(string(kv.Key)[len(base):])
		if err != nil || s < 0 || s >= ShardCount {
			continue
		}
		sm[s] = string(kv.Value)
	}
	return sm, nil
}
