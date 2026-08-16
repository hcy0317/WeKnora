package service

import (
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestResetKnowledgeForReparseClearsPreviousAttemptState(t *testing.T) {
	processedAt := time.Now()
	knowledge := &types.Knowledge{
		ParseStatus:          types.ParseStatusFailed,
		EnableStatus:         "enabled",
		Description:          "previous description",
		ProcessedAt:          &processedAt,
		ErrorMessage:         "embedding request failed: context deadline exceeded",
		EmbeddingModelID:     "old-model",
		PendingSubtasksCount: 3,
	}
	resetKnowledgeForReparse(knowledge)

	require.Equal(t, types.ParseStatusPending, knowledge.ParseStatus)
	require.Equal(t, "disabled", knowledge.EnableStatus)
	require.Empty(t, knowledge.Description)
	require.Nil(t, knowledge.ProcessedAt)
	require.Empty(t, knowledge.ErrorMessage)
	require.Equal(t, "old-model", knowledge.EmbeddingModelID,
		"the cleanup worker owns the atomic switch to the target embedding model")
	require.Zero(t, knowledge.PendingSubtasksCount)
}
