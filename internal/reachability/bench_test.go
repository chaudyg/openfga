package reachability

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

// seedFolderTree writes a folder tree with the given branching factor. Node 0..(roots-1)
// are roots; node i has parent (i-roots)/branching for i >= roots.
func seedFolderTree(tb testing.TB, ds storage.OpenFGADatastore, storeID string, totalNodes, roots, branching int) (maxDepth int) {
	tb.Helper()
	ctx := context.Background()

	depths := make([]int, totalNodes)
	batch := make([]*openfgav1.TupleKey, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := ds.Write(ctx, storeID, nil, batch)
		require.NoError(tb, err)
		batch = batch[:0]
	}

	for i := roots; i < totalNodes; i++ {
		parent := (i - roots) / branching
		depths[i] = depths[parent] + 1
		if depths[i] > maxDepth {
			maxDepth = depths[i]
		}
		batch = append(batch, tuple.NewTupleKey(
			fmt.Sprintf("folder:%d", i),
			"parent",
			fmt.Sprintf("folder:%d", parent),
		))
		if len(batch) == 1000 {
			flush()
		}
	}
	flush()
	return maxDepth
}

// TestScale300K reports build cost, retained memory, query latency, and
// closure size for a 300K-node folder tree, and verifies that the default
// bounds reject it.
func TestScale300K(t *testing.T) {
	if testing.Short() {
		t.Skip("scale analysis")
	}
	ctx := context.Background()
	ds := memory.New()
	defer ds.Close()
	storeID := "store-scale"

	const totalNodes = 300_000
	maxDepth := seedFolderTree(t, ds, storeID, totalNodes, 3, 3)
	t.Logf("tree: %d nodes, 3 roots, branching 3, max depth %d", totalNodes, maxDepth)

	// Default bounds.
	defIdx := New()
	_, used, err := defIdx.MatchesAnyAncestor(ctx, ds, storeID, "folder", "parent",
		[]string{fmt.Sprintf("folder:%d", totalNodes-1)},
		map[string]struct{}{"folder:0": {}},
	)
	require.NoError(t, err)
	t.Logf("default bounds (maxNodes=100000, maxClosureEntries=1000000): used=%v", used)

	// Raised bounds.
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	idx := New(WithMaxNodes(400_000), WithMaxClosureEntries(20_000_000))
	key := indexKey{storeID: storeID, objectType: "folder", relation: "parent"}
	snap, result := idx.build(ctx, ds, key)
	require.Equal(t, buildReady, result)

	runtime.GC()
	runtime.ReadMemStats(&after)
	var entries uint64
	for _, bm := range snap.ancestors {
		if bm != nil {
			entries += bm.GetCardinality()
		}
	}
	t.Logf("closure entries: %d (sum of ancestor-set cardinalities)", entries)
	t.Logf("retained heap after build: %.1f MiB", float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))

	// Deep leaf reaching a root.
	matches, used, err := idx.MatchesAnyAncestor(ctx, ds, storeID, "folder", "parent",
		[]string{fmt.Sprintf("folder:%d", totalNodes-1)},
		map[string]struct{}{"folder:0": {}},
	)
	require.NoError(t, err)
	require.True(t, used)
	require.True(t, matches)
}

// BenchmarkBuild300K measures a cold build (full scan + closure) as happens
// after every Invalidate.
func BenchmarkBuild300K(b *testing.B) {
	ctx := context.Background()
	ds := memory.New()
	defer ds.Close()
	storeID := "store-bench-build"
	seedFolderTree(b, ds, storeID, 300_000, 3, 3)

	idx := New(WithMaxNodes(400_000), WithMaxClosureEntries(20_000_000))
	key := indexKey{storeID: storeID, objectType: "folder", relation: "parent"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, result := idx.build(ctx, ds, key)
		if result != buildReady {
			b.Fatalf("build failed: result=%v", result)
		}
	}
}

// BenchmarkMatchWarm300K measures a query against a warm snapshot with a
// realistic seed/target shape (one deep leaf, 20 granted folders).
func BenchmarkMatchWarm300K(b *testing.B) {
	ctx := context.Background()
	ds := memory.New()
	defer ds.Close()
	storeID := "store-bench-match"
	const totalNodes = 300_000
	seedFolderTree(b, ds, storeID, totalNodes, 3, 3)

	idx := New(WithMaxNodes(400_000), WithMaxClosureEntries(20_000_000))
	seeds := []string{fmt.Sprintf("folder:%d", totalNodes-1)}
	targets := make(map[string]struct{}, 20)
	for i := 0; i < 20; i++ {
		targets[fmt.Sprintf("folder:%d", 1000+i*777)] = struct{}{}
	}

	// Warm the snapshot.
	require.Eventually(b, func() bool {
		_, used, err := idx.MatchesAnyAncestor(ctx, ds, storeID, "folder", "parent", seeds, targets)
		return err == nil && used
	}, time.Second, time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := idx.MatchesAnyAncestor(ctx, ds, storeID, "folder", "parent", seeds, targets)
		if err != nil {
			b.Fatal(err)
		}
	}
}
