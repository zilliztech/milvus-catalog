package migration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// stripRootPath must anchor on a segment boundary. The collision case is the dangerous one: a
// rootPath segment that merely begins with root must NOT anchor the strip, or the copy lands
// under the wrong key and DiffPrefixes (which strips both sides the same way) can't catch it.
func TestStripRootPathSegmentBoundary(t *testing.T) {
	cases := []struct {
		name, fullKey, root, want string
	}{
		{"normal", "by-dev/kv/foo", "kv", "kv/foo"},
		{"prefix-collision", "a/kv-store/kv/foo", "kv", "kv/foo"},
		{"empty-rootpath", "kv/foo", "kv", "kv/foo"},
		{"root-at-end", "x/kv", "kv", "kv"},
		{"multi-segment-root", "by-dev/root-coord/database/1", "root-coord/database", "root-coord/database/1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := stripRootPath(c.fullKey, c.root)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestStripRootPathNoMatchErrors(t *testing.T) {
	_, err := stripRootPath("a/other/foo", "kv")
	require.Error(t, err, "a key that does not contain root at a segment boundary must error, not silently mis-strip")
}
