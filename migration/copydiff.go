// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package migration provides the bulk move + verification primitives used to copy
// RootCoord metadata from the cluster's source backend (etcd) into the pooled catalog
// service's TiKV backend before cutover.
package migration

import (
	"context"
	"sort"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// multiSaveBatch bounds a single MultiSave transaction. etcd has a hard 128-op txn limit
// and TiKV transactions grow unboundedly in memory; batching keeps both happy.
const multiSaveBatch = 64

// Mismatch describes one logical key that differs between source and destination.
type Mismatch struct {
	Key    string
	Reason string // "missing-in-dst" | "missing-in-src" | "value-differs"
}

const (
	reasonMissingInDst = "missing-in-dst"
	reasonMissingInSrc = "missing-in-src"
	reasonValueDiffers = "value-differs"
)

// loadLogical loads every key under root from m and returns a map of LOGICAL key -> value.
//
// kv.MetaKv.LoadWithPrefix returns FULL keys: each is GetPath(rootPath, logicalKey) =
// rootPath + "/" + logicalKey (both EtcdKV and txnTiKV behave this way — verified against
// internal/kv/etcd/etcd_kv.go and internal/kv/tikv/txn_tikv.go). Because src and dst are
// distinct MetaKv instances with different rootPaths, we strip the instance rootPath here so
// callers compare/copy by logical sub-key. The logical key always begins with root, so we cut
// the full key at the first segment boundary where root starts.
func loadLogical(ctx context.Context, m kv.MetaKv, root string) (map[string]string, error) {
	fullKeys, vals, err := m.LoadWithPrefix(ctx, root)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(fullKeys))
	for i, fk := range fullKeys {
		logical, err := stripRootPath(fk, root)
		if err != nil {
			return nil, err
		}
		out[logical] = vals[i]
	}
	return out, nil
}

// stripRootPath turns a full backend key into its logical key by removing the instance
// rootPath. The full key is rootPath + "/" + logicalKey and logicalKey starts with root, so
// we locate root at a path-segment boundary and slice from there.
func stripRootPath(fullKey, root string) (string, error) {
	// Prefer the rootPath case: root appears preceded by "/" (fullKey = rootPath + "/" + root/...).
	// Check this BEFORE the empty-rootPath fast path so a non-empty rootPath whose key already
	// starts with root is still stripped correctly.
	if idx := strings.Index(fullKey, "/"+root); idx >= 0 {
		return fullKey[idx+1:], nil
	}
	// Empty-rootPath instance: the full key already IS the logical key.
	if strings.HasPrefix(fullKey, root) {
		return fullKey, nil
	}
	return "", merr.WrapErrParameterInvalidMsg(
		"migration: key %q does not contain root %q at a segment boundary", fullKey, root)
}

// CopyPrefixes loads every key under each root from src and writes it into dst under the same
// logical key. Writes are batched. It returns the total number of keys copied.
func CopyPrefixes(ctx context.Context, src, dst kv.MetaKv, roots []string) (int, error) {
	copied := 0
	for _, root := range roots {
		logical, err := loadLogical(ctx, src, root)
		if err != nil {
			return copied, err
		}
		batch := make(map[string]string, multiSaveBatch)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := dst.MultiSave(ctx, batch); err != nil {
				return err
			}
			batch = make(map[string]string, multiSaveBatch)
			return nil
		}
		for k, v := range logical {
			batch[k] = v
			if len(batch) >= multiSaveBatch {
				if err := flush(); err != nil {
					return copied, err
				}
			}
			copied++
		}
		if err := flush(); err != nil {
			return copied, err
		}
		log.Ctx(ctx).Info("migration: copied prefix",
			zap.String("root", root), zap.Int("keys", len(logical)))
	}
	return copied, nil
}

// DiffPrefixes performs a full logical key->value comparison under each root. An empty result
// means src and dst are identical. Results are sorted by key for deterministic reporting.
func DiffPrefixes(ctx context.Context, src, dst kv.MetaKv, roots []string) ([]Mismatch, error) {
	srcKV, err := LoadLogical(ctx, src, roots)
	if err != nil {
		return nil, err
	}
	dstKV, err := LoadLogical(ctx, dst, roots)
	if err != nil {
		return nil, err
	}
	return DiffMaps(srcKV, dstKV), nil
}

// LoadLogical loads every key under each root from m into one logical key -> value map.
// Used by the bulk-import path: the coord loads its source snapshot to ship over gRPC, and
// the service loads its destination to verify against — neither exposes the backend layout.
func LoadLogical(ctx context.Context, m kv.MetaKv, roots []string) (map[string]string, error) {
	out := make(map[string]string)
	for _, root := range roots {
		part, err := loadLogical(ctx, m, root)
		if err != nil {
			return nil, err
		}
		for k, v := range part {
			out[k] = v
		}
	}
	return out, nil
}

// DiffMaps compares two logical key->value maps and returns the mismatches, sorted by key.
// Empty result == identical.
func DiffMaps(src, dst map[string]string) []Mismatch {
	var mismatches []Mismatch
	for k, sv := range src {
		dv, ok := dst[k]
		switch {
		case !ok:
			mismatches = append(mismatches, Mismatch{Key: k, Reason: reasonMissingInDst})
		case dv != sv:
			mismatches = append(mismatches, Mismatch{Key: k, Reason: reasonValueDiffers})
		}
	}
	for k := range dst {
		if _, ok := src[k]; !ok {
			mismatches = append(mismatches, Mismatch{Key: k, Reason: reasonMissingInSrc})
		}
	}
	sort.Slice(mismatches, func(i, j int) bool { return mismatches[i].Key < mismatches[j].Key })
	return mismatches
}
