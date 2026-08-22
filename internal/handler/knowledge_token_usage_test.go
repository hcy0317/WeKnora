package handler

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummarizeKnowledgeTokenUsage_GroupsByStageAndModel(t *testing.T) {
	rows := []types.KnowledgeProcessingSpan{
		generationUsageRow("postprocess.summary", "chat", "gpt-test", false, true, 100, 20, 120, 80),
		generationUsageRow("postprocess.summary", "chat", "gpt-test", false, true, 50, 10, 60, 0),
		generationUsageRow("embedding", "embedding", "jina-embeddings-v2-base-zh", true, true, 200, 0, 200, 0),
		generationUsageRow("postprocess.wiki", "chat", "gpt-test", false, false, 0, 0, 0, 0),
		{Kind: types.SpanKindStage, Name: types.StageEmbedding},
	}

	got := summarizeKnowledgeTokenUsage(rows)
	require.True(t, got.HasData)
	assert.Equal(t, 4, got.Usage.Calls)
	assert.Equal(t, 2, got.Usage.MeasuredCalls)
	assert.Equal(t, 1, got.Usage.EstimatedCalls)
	assert.Equal(t, 1, got.Usage.UnknownCalls)
	assert.EqualValues(t, 350, got.Usage.InputTokens)
	assert.EqualValues(t, 30, got.Usage.OutputTokens)
	assert.EqualValues(t, 380, got.Usage.TotalTokens)
	assert.EqualValues(t, 80, got.Usage.CacheReadTokens)
	require.Len(t, got.Stages, 3)
	assert.Equal(t, "embedding", got.Stages[0].Stage)
	assert.Equal(t, "postprocess.summary", got.Stages[1].Stage)
	assert.Equal(t, "postprocess.wiki", got.Stages[2].Stage)
	require.Len(t, got.Stages[1].Models, 1)
	assert.Equal(t, "gpt-test", got.Stages[1].Models[0].ModelName)
	assert.Equal(t, 2, got.Stages[1].Models[0].Usage.Calls)
}

func TestSummarizeKnowledgeTokenUsage_CollapsesIndexedFanoutStages(t *testing.T) {
	rows := []types.KnowledgeProcessingSpan{
		generationUsageRow("postprocess.graph.chunk[7]", "chat", "gpt-test", false, true, 10, 2, 12, 0),
		generationUsageRow("postprocess.graph.chunk[42]", "chat", "gpt-test", false, true, 20, 3, 23, 0),
		generationUsageRow("postprocess.question.batch[3]", "chat", "gpt-test", false, true, 30, 4, 34, 0),
		generationUsageRow("postprocess.wiki.page[concept/example]", "chat", "gpt-test", false, true, 40, 5, 45, 0),
	}

	got := summarizeKnowledgeTokenUsage(rows)
	require.Len(t, got.Stages, 3)
	assert.Equal(t, "postprocess.graph", got.Stages[0].Stage)
	assert.Equal(t, 2, got.Stages[0].Usage.Calls)
	assert.EqualValues(t, 30, got.Stages[0].Usage.InputTokens)
	assert.Equal(t, "postprocess.question", got.Stages[1].Stage)
	assert.Equal(t, "postprocess.wiki.page", got.Stages[2].Stage)
}

func generationUsageRow(
	stage, modelType, modelName string,
	estimated, available bool,
	input, output, total, cacheRead int,
) types.KnowledgeProcessingSpan {
	return types.KnowledgeProcessingSpan{
		Kind: types.SpanKindGeneration,
		Metadata: types.JSONMap{
			"processing_stage": stage,
			"model_type":       modelType,
			"model_name":       modelName,
		},
		Output: types.JSONMap{
			"usage": types.JSONMap{
				"input_tokens":      input,
				"output_tokens":     output,
				"total_tokens":      total,
				"cache_read_tokens": cacheRead,
				"unit":              "TOKENS",
				"estimated":         estimated,
				"available":         available,
			},
		},
	}
}
