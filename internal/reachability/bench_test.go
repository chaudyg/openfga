package reachability

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type staticRelationshipReader struct {
	storage.RelationshipTupleReader
	tuples []*openfgav1.Tuple
}

func (r *staticRelationshipReader) Read(
	context.Context,
	string,
	storage.ReadFilter,
	storage.ReadOptions,
) (storage.TupleIterator, error) {
	return storage.NewStaticTupleIterator(r.tuples), nil
}

// hierarchyReader builds a hierarchy with the given branching factor. Node
// 0..(roots-1) are roots; node i has parent (i-roots)/branching thereafter.
func hierarchyReader(tb testing.TB, totalNodes, roots, branching int) *staticRelationshipReader {
	tb.Helper()
	tuples := make([]*openfgav1.Tuple, 0, totalNodes-roots)
	for i := roots; i < totalNodes; i++ {
		parent := (i - roots) / branching
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(
			fmt.Sprintf("node:%d", i),
			"parent",
			fmt.Sprintf("node:%d", parent),
		)})
	}
	return &staticRelationshipReader{tuples: tuples}
}

func TestHierarchyBuildBounds(t *testing.T) {
	ctx := context.Background()
	const totalNodes = 10_000
	ds := hierarchyReader(t, totalNodes, 3, 3)
	storeID := "store-bounds"
	key := indexKey{storeID: storeID, objectType: "node", relation: "parent"}

	leafRoot := totalNodes - 1
	for leafRoot >= 3 {
		leafRoot = (leafRoot - 3) / 3
	}
	root := fmt.Sprintf("node:%d", leafRoot)

	boundedIndex := New(WithMaxNodes(5_000))
	_, result := boundedIndex.build(ctx, ds, key)
	require.Equal(t, buildDisabled, result)

	index := New(WithMaxNodes(20_000), WithMaxClosureEntries(1_000_000))
	snapshot, result := index.build(ctx, ds, key)
	require.Equal(t, buildReady, result)

	require.True(t, matchesSnapshot(
		snapshot,
		[]string{fmt.Sprintf("node:%d", totalNodes-1)},
		map[string]struct{}{root: {}},
	))
}

// BenchmarkBuild300K measures a cold build (full scan + closure) as happens
// after every Invalidate.
func BenchmarkBuild300K(b *testing.B) {
	ctx := context.Background()
	ds := hierarchyReader(b, 300_000, 3, 3)
	storeID := "store-bench-build"

	idx := New(WithMaxNodes(400_000), WithMaxClosureEntries(20_000_000))
	key := indexKey{storeID: storeID, objectType: "node", relation: "parent"}

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
	ds := hierarchyReader(b, 300_000, 3, 3)
	storeID := "store-bench-match"
	const totalNodes = 300_000

	idx := New(WithMaxNodes(400_000), WithMaxClosureEntries(20_000_000))
	seeds := []string{fmt.Sprintf("node:%d", totalNodes-1)}
	targets := make(map[string]struct{}, 20)
	for i := 0; i < 20; i++ {
		targets[fmt.Sprintf("node:%d", 1000+i*777)] = struct{}{}
	}

	// Warm the snapshot.
	require.Eventually(b, func() bool {
		_, used, err := idx.MatchesAnyAncestor(ctx, ds, storeID, "node", "parent", seeds, targets)
		return err == nil && used
	}, time.Second, time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := idx.MatchesAnyAncestor(ctx, ds, storeID, "node", "parent", seeds, targets)
		if err != nil {
			b.Fatal(err)
		}
	}
}
