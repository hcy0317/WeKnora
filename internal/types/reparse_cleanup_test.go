package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReparseCleanupCheckpointRoundTripsInAttemptRootInput(t *testing.T) {
	input := JSONMap{"langfuse_trace_id": "trace-1"}
	checkpoint := ReparseCleanupCheckpoint{
		Version:                1,
		Attempt:                4,
		Phase:                  ReparseCleanupPrepared,
		SourceEmbeddingModelID: "old-model",
		TargetEmbeddingModelID: "new-model",
		SourceVectorStoreID:    stringPointer("vector-store-1"),
		SourceEffectiveEngines: []RetrieverEngineParams{{
			RetrieverEngineType: PostgresRetrieverEngineType,
			RetrieverType:       VectorRetrieverType,
		}},
		TargetVectorStoreID: stringPointer("vector-store-2"),
		TargetEffectiveEngines: []RetrieverEngineParams{{
			RetrieverEngineType: QdrantRetrieverEngineType,
			RetrieverType:       VectorRetrieverType,
		}},
		KnowledgeType:             "file",
		EmbeddingDimensions:       1536,
		ImageURLs:                 []string{"local://image-1", "minio://image-2"},
		WikiPendingIngestScrubbed: true,
		VectorsDeleted:            true,
	}

	updated, err := PutReparseCleanupCheckpoint(input, checkpoint)
	require.NoError(t, err)
	require.Equal(t, "trace-1", updated["langfuse_trace_id"])

	got, err := DecodeReparseCleanupCheckpoint(updated)
	require.NoError(t, err)
	require.Equal(t, &checkpoint, got)
	require.NotContains(t, input, reparseCleanupInputKey, "caller input must not be mutated in place")
}

func TestReparseCleanupCheckpointRejectsInvalidPreparedPlan(t *testing.T) {
	_, err := PutReparseCleanupCheckpoint(JSONMap{}, ReparseCleanupCheckpoint{
		Version: 1, Attempt: 2, Phase: ReparseCleanupPrepared, SourceEmbeddingModelID: "old-model",
	})
	require.ErrorContains(t, err, "embedding dimensions")

	_, err = PutReparseCleanupCheckpoint(JSONMap{}, ReparseCleanupCheckpoint{
		Version: 2, Attempt: 2, Phase: ReparseCleanupPending,
	})
	require.ErrorContains(t, err, "version")
}

func TestKnowledgeIndexRouteSnapshotRoundTripsInAttemptRootInput(t *testing.T) {
	snapshot := KnowledgeIndexRouteSnapshot{
		Version:          1,
		EmbeddingModelID: "embedding-model-a",
		EffectiveEngines: []RetrieverEngineParams{{
			RetrieverEngineType: PostgresRetrieverEngineType,
			RetrieverType:       VectorRetrieverType,
		}},
	}
	input, err := PutKnowledgeIndexRouteSnapshot(JSONMap{"other": "kept"}, snapshot)
	require.NoError(t, err)
	got, err := DecodeKnowledgeIndexRouteSnapshot(input)
	require.NoError(t, err)
	require.Equal(t, &snapshot, got)
	require.Equal(t, "kept", input["other"])
}

func stringPointer(value string) *string { return &value }
