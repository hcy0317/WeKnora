package chatpipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type lifecycleRerankModelService struct {
	interfaces.ModelService
	model rerank.Reranker
}

func (s lifecycleRerankModelService) GetRerankModel(context.Context, string) (rerank.Reranker, error) {
	return s.model, nil
}

type failingLifecycleReranker struct {
	managed bool
}

func (r failingLifecycleReranker) Rerank(context.Context, string, []string) ([]rerank.RankResult, error) {
	return nil, errors.New("both managed rerank backends are unavailable")
}

func (failingLifecycleReranker) GetModelName() string { return "managed-reranker" }
func (failingLifecycleReranker) GetModelID() string   { return "managed-reranker" }
func (r failingLifecycleReranker) LifecycleManaged() bool {
	return r.managed
}

func TestManagedRerankFailureIsExplicitInsteadOfFallingBack(t *testing.T) {
	plugin := &PluginRerank{modelService: lifecycleRerankModelService{
		model: failingLifecycleReranker{managed: true},
	}}
	nextCalled := false
	chatManage := &types.ChatManage{
		PipelineRequest: types.PipelineRequest{
			RerankModelID:   "managed-reranker",
			RerankTopK:      5,
			RerankThreshold: 0.1,
		},
		PipelineState: types.PipelineState{
			RewriteQuery: "query",
			SearchResult: []*types.SearchResult{{ID: "chunk-1", Content: "candidate"}},
		},
	}

	err := plugin.OnEvent(t.Context(), types.CHUNK_RERANK, chatManage, func() *PluginError {
		nextCalled = true
		return nil
	})

	require.NotNil(t, err)
	require.Equal(t, ErrRerank.ErrorType, err.ErrorType)
	require.ErrorContains(t, err.Err, "both managed rerank backends")
	require.False(t, nextCalled)
}

func TestExternalRerankFailureKeepsExistingFallback(t *testing.T) {
	plugin := &PluginRerank{modelService: lifecycleRerankModelService{
		model: failingLifecycleReranker{managed: false},
	}}
	nextCalled := false
	chatManage := &types.ChatManage{
		PipelineRequest: types.PipelineRequest{RerankModelID: "external-reranker", RerankTopK: 5},
		PipelineState: types.PipelineState{
			RewriteQuery: "query",
			SearchResult: []*types.SearchResult{{ID: "chunk-1", Content: "candidate"}},
		},
	}

	err := plugin.OnEvent(t.Context(), types.CHUNK_RERANK, chatManage, func() *PluginError {
		nextCalled = true
		return nil
	})

	require.Nil(t, err)
	require.True(t, nextCalled)
	require.Len(t, chatManage.SearchResult, 1)
}
