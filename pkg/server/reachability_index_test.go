package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/featureflags"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type blockingWriteDatastore struct {
	storage.OpenFGADatastore
	writeStarted chan struct{}
	releaseWrite <-chan struct{}
	once         sync.Once
}

func (d *blockingWriteDatastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	d.once.Do(func() { close(d.writeStarted) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.releaseWrite:
		return d.OpenFGADatastore.Write(ctx, store, deletes, writes, opts...)
	}
}

func TestReachabilityIndexRequiresSingleWriterGuard(t *testing.T) {
	server, request := setupCheckServer(t, "", nil, WithFeatureFlagClient(featureflags.NewDefaultClient([]string{
		serverconfig.ExperimentalWeightedGraphCheck,
		serverconfig.ExperimentalReachabilityIndex,
	})))

	require.Nil(t, server.reachabilityIndexForStore(request.GetStoreId()))
}

func TestCheck_ReachabilityIndexResolvesNestedTTU(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type folder
			relations
				define parent: [folder]
				define viewer: [user] or viewer from parent
	`

	server, request := setupCheckServer(t, modelDSL, []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:engineering", "parent", "folder:root"),
		tuple.NewTupleKey("folder:platform", "parent", "folder:engineering"),
		tuple.NewTupleKey("folder:api", "parent", "folder:platform"),
		tuple.NewTupleKey("folder:root", "viewer", "user:anne"),
	}, WithFeatureFlagClient(featureflags.NewDefaultClient([]string{
		serverconfig.ExperimentalWeightedGraphCheck,
		serverconfig.ExperimentalReachabilityIndex,
	})), WithReachabilityIndexSingleWriter())

	request.TupleKey = &openfgav1.CheckRequestTupleKey{
		Object:   "folder:api",
		Relation: "viewer",
		User:     "user:anne",
	}

	response, err := server.Check(context.Background(), request)
	require.NoError(t, err)
	require.True(t, response.GetAllowed())
}

func TestCheck_ReachabilityIndexFallsBackForConditionalTupleset(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type folder
			relations
				define parent: [folder with is_allowed]
				define viewer: [user] or viewer from parent
		condition is_allowed(granted: bool) {
			granted == true
		}
	`

	server, request := setupCheckServer(t, modelDSL, []*openfgav1.TupleKey{
		{
			Object:   "folder:api",
			Relation: "parent",
			User:     "folder:root",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "is_allowed",
				Context: &structpb.Struct{Fields: map[string]*structpb.Value{"granted": structpb.NewBoolValue(false)}},
			},
		},
		tuple.NewTupleKey("folder:root", "viewer", "user:anne"),
	}, WithFeatureFlagClient(featureflags.NewDefaultClient([]string{
		serverconfig.ExperimentalWeightedGraphCheck,
		serverconfig.ExperimentalReachabilityIndex,
	})), WithReachabilityIndexSingleWriter())

	request.TupleKey = &openfgav1.CheckRequestTupleKey{
		Object:   "folder:api",
		Relation: "viewer",
		User:     "user:anne",
	}

	response, err := server.Check(context.Background(), request)
	require.NoError(t, err)
	require.False(t, response.GetAllowed())
}

func TestCheck_ReachabilityIndexDoesNotRetainSnapshotAcrossWrite(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type folder
			relations
				define parent: [folder]
				define viewer: [user] or viewer from parent
	`

	server, request := setupCheckServer(t, modelDSL, []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:engineering", "parent", "folder:root"),
		tuple.NewTupleKey("folder:platform", "parent", "folder:engineering"),
		tuple.NewTupleKey("folder:api", "parent", "folder:platform"),
		tuple.NewTupleKey("folder:root", "viewer", "user:anne"),
	}, WithFeatureFlagClient(featureflags.NewDefaultClient([]string{
		serverconfig.ExperimentalWeightedGraphCheck,
		serverconfig.ExperimentalReachabilityIndex,
	})), WithReachabilityIndexSingleWriter())

	request.TupleKey = &openfgav1.CheckRequestTupleKey{
		Object:   "folder:api",
		Relation: "viewer",
		User:     "user:anne",
	}

	// Block after the index has acquired its mutation guard but before the
	// datastore commits the delete. A Check issued in this interval may use
	// the normal resolver's pre-commit view, but must not retain it as an index
	// snapshot after the mutation completes.
	releaseWrite := make(chan struct{})
	blockingDatastore := &blockingWriteDatastore{
		OpenFGADatastore: server.datastore,
		writeStarted:     make(chan struct{}),
		releaseWrite:     releaseWrite,
	}
	server.datastore = blockingDatastore

	writeDone := make(chan error, 1)
	go func() {
		_, err := server.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              request.GetStoreId(),
			AuthorizationModelId: request.GetAuthorizationModelId(),
			Deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "folder:api",
					Relation: "parent",
					User:     "folder:platform",
				}},
			},
		})
		writeDone <- err
	}()
	<-blockingDatastore.writeStarted

	checkDone := make(chan *openfgav1.CheckResponse, 1)
	checkErr := make(chan error, 1)
	go func() {
		response, err := server.Check(context.Background(), request)
		checkDone <- response
		checkErr <- err
	}()

	// This Check forces the recursive strategy to inspect the old graph while
	// the write is blocked. It either waits for the mutation guard or resolves
	// against the old datastore state; in neither case may that old state
	// remain indexed once the write commits.
	checkFinished := false
	select {
	case err := <-checkErr:
		require.NoError(t, err)
		require.True(t, (<-checkDone).GetAllowed())
		checkFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseWrite)
	require.NoError(t, <-writeDone)
	if !checkFinished {
		select {
		case err := <-checkErr:
			require.NoError(t, err)
			require.True(t, (<-checkDone).GetAllowed())
		case <-time.After(time.Second):
			t.Fatal("check did not complete after write committed")
		}
	}

	response, err := server.Check(context.Background(), request)
	require.NoError(t, err)
	require.False(t, response.GetAllowed())
}

func TestCheck_ReachabilityIndexInvalidatesAfterFailedWrite(t *testing.T) {
	modelDSL := `
		model
			schema 1.1
		type user
		type folder
			relations
				define parent: [folder]
				define viewer: [user] or viewer from parent
	`

	server, request := setupCheckServer(t, modelDSL, []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:engineering", "parent", "folder:root"),
		tuple.NewTupleKey("folder:platform", "parent", "folder:engineering"),
		tuple.NewTupleKey("folder:api", "parent", "folder:platform"),
		tuple.NewTupleKey("folder:root", "viewer", "user:anne"),
	}, WithFeatureFlagClient(featureflags.NewDefaultClient([]string{
		serverconfig.ExperimentalWeightedGraphCheck,
		serverconfig.ExperimentalReachabilityIndex,
	})), WithReachabilityIndexSingleWriter())

	request.TupleKey = &openfgav1.CheckRequestTupleKey{
		Object:   "folder:api",
		Relation: "viewer",
		User:     "user:anne",
	}
	response, err := server.Check(context.Background(), request)
	require.NoError(t, err)
	require.True(t, response.GetAllowed())

	_, err = server.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              request.GetStoreId(),
		AuthorizationModelId: request.GetAuthorizationModelId(),
		Deletes: &openfgav1.WriteRequestDeletes{
			TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
				Object:   "folder:missing",
				Relation: "parent",
				User:     "folder:root",
			}},
		},
	})
	require.Error(t, err)

	// Simulate a mutation that bypasses Server.Write. The preceding failed
	// request must already have invalidated the warm snapshot.
	require.NoError(t, server.datastore.Write(context.Background(), request.GetStoreId(),
		[]*openfgav1.TupleKeyWithoutCondition{{
			Object:   "folder:api",
			Relation: "parent",
			User:     "folder:platform",
		}},
		nil,
	))

	response, err = server.Check(context.Background(), request)
	require.NoError(t, err)
	require.False(t, response.GetAllowed())
}
