package handler

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestBuildTestModelPreservesModelIDForProtocolCache(t *testing.T) {
	h := &InitializationHandler{}
	model := h.buildTestModel(&ModelTestRequest{
		ModelID:   "model-vlm-1",
		ModelName: "vision-model",
		BaseURL:   "https://example.com/v1",
		Provider:  "openai",
	}, types.ModelTypeVLLM, types.ModelSourceRemote)

	require.Equal(t, "model-vlm-1", model.ID)
	require.Equal(t, types.ModelTypeVLLM, model.Type)
	require.Equal(t, types.ModelSourceRemote, model.Source)
}
