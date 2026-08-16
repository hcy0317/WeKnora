package retriever

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type capturingEmbedder struct {
	embedding.Embedder
	text       string
	batchTexts []string
	batch      [][]float32
	dimensions int
}

func (e *capturingEmbedder) GetDimensions() int {
	if e.dimensions == 0 {
		return 1
	}
	return e.dimensions
}

func (e *capturingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.text = text
	return []float32{1}, nil
}

func (e *capturingEmbedder) BatchEmbedWithPool(
	ctx context.Context,
	model embedding.Embedder,
	texts []string,
) ([][]float32, error) {
	e.batchTexts = append([]string(nil), texts...)
	if e.batch != nil {
		return e.batch, nil
	}
	embeddings := make([][]float32, len(texts))
	for i := range texts {
		embeddings[i] = []float32{1}
	}
	return embeddings, nil
}

type saveOnlyRepository struct {
	interfaces.RetrieveEngineRepository
	batchSaveCalls atomic.Int32
}

func (r *saveOnlyRepository) Save(ctx context.Context, indexInfo *types.IndexInfo, params map[string]any) error {
	return nil
}

func (r *saveOnlyRepository) BatchSave(
	ctx context.Context,
	indexInfoList []*types.IndexInfo,
	params map[string]any,
) error {
	r.batchSaveCalls.Add(1)
	return nil
}

func TestBatchIndexRejectsInvalidVectorBatchBeforeRepositorySave(t *testing.T) {
	testCases := []struct {
		name       string
		embeddings [][]float32
	}{
		{name: "empty vector", embeddings: [][]float32{{1, 2}, {}}},
		{name: "count mismatch", embeddings: [][]float32{{1, 2}}},
		{name: "short vector", embeddings: [][]float32{{1, 2}, {3}}},
		{name: "long vector", embeddings: [][]float32{{1, 2}, {3, 4, 5}}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			repository := &saveOnlyRepository{}
			service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repository}
			err := service.BatchIndex(context.Background(), &capturingEmbedder{
				batch: testCase.embeddings, dimensions: 2,
			}, []*types.IndexInfo{
				{Content: "valid", SourceID: "source-1", ChunkID: "chunk-1"},
				{Content: "sensitive content", SourceID: "source-2", ChunkID: "chunk-2"},
			}, []types.RetrieverType{types.VectorRetrieverType})

			if err == nil {
				t.Fatal("BatchIndex returned nil error")
			}
			if !strings.Contains(err.Error(), "source-2") || !strings.Contains(err.Error(), "chunk-2") {
				t.Fatalf("error lacks safe failing identity: %v", err)
			}
			if strings.Contains(err.Error(), "sensitive content") {
				t.Fatalf("error leaks content: %v", err)
			}
			if got := repository.batchSaveCalls.Load(); got != 0 {
				t.Fatalf("BatchSave calls = %d, want 0", got)
			}
		})
	}
}

func TestBatchIndexValidVectorsContinueToRepository(t *testing.T) {
	repository := &saveOnlyRepository{}
	service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repository}
	err := service.BatchIndex(context.Background(), &capturingEmbedder{batch: [][]float32{{1}, {2}}}, []*types.IndexInfo{
		{SourceID: "source-1", ChunkID: "chunk-1"},
		{SourceID: "source-2", ChunkID: "chunk-2"},
	}, []types.RetrieverType{types.VectorRetrieverType})

	if err != nil {
		t.Fatalf("BatchIndex returned error: %v", err)
	}
	if got := repository.batchSaveCalls.Load(); got != 1 {
		t.Fatalf("BatchSave calls = %d, want 1", got)
	}
}

func TestBatchIndexKeywordOnlyAllowsNoEmbeddings(t *testing.T) {
	repository := &saveOnlyRepository{}
	service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repository}
	err := service.BatchIndex(context.Background(), &capturingEmbedder{batch: [][]float32{}}, []*types.IndexInfo{
		{SourceID: "source-1", ChunkID: "chunk-1"},
	}, []types.RetrieverType{types.KeywordsRetrieverType})

	if err != nil {
		t.Fatalf("BatchIndex returned error: %v", err)
	}
	if got := repository.batchSaveCalls.Load(); got != 1 {
		t.Fatalf("BatchSave calls = %d, want 1", got)
	}
}

func TestIndexRemovesInlineImagePayloadBeforeEmbedding(t *testing.T) {
	ctx := context.Background()
	embedder := &capturingEmbedder{}
	service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: &saveOnlyRepository{}}
	payload := strings.Repeat("A", 300)
	content := "before <img src=\"data:image/png;base64," + payload + "\"> after"

	err := service.Index(ctx, embedder, &types.IndexInfo{
		Content:  content,
		SourceID: "source-1",
	}, []types.RetrieverType{types.VectorRetrieverType})
	if err != nil {
		t.Fatalf("Index returned error: %v", err)
	}
	assertImagePayloadRemoved(t, embedder.text, payload)
}

func TestBatchIndexRemovesInlineImagePayloadBeforeEmbedding(t *testing.T) {
	ctx := context.Background()
	embedder := &capturingEmbedder{}
	service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: &saveOnlyRepository{}}
	payload := strings.Repeat("A", 300)
	content := "before ![chart](data:image/png;base64," + payload + ") after"

	err := service.BatchIndex(ctx, embedder, []*types.IndexInfo{{
		Content:  content,
		SourceID: "source-1",
	}}, []types.RetrieverType{types.VectorRetrieverType})
	if err != nil {
		t.Fatalf("BatchIndex returned error: %v", err)
	}
	if len(embedder.batchTexts) != 1 {
		t.Fatalf("expected one embedding input, got %d", len(embedder.batchTexts))
	}
	assertImagePayloadRemoved(t, embedder.batchTexts[0], payload)
}

func TestBatchIndexTruncatesOversizedEmbeddingInput(t *testing.T) {
	ctx := context.Background()
	embedder := &capturingEmbedder{}
	service := &KeywordsVectorHybridRetrieveEngineService{indexRepository: &saveOnlyRepository{}}

	err := service.BatchIndex(ctx, embedder, []*types.IndexInfo{{
		Content:  strings.Repeat("x", safetyMaxChars+10),
		SourceID: "source-1",
	}}, []types.RetrieverType{types.VectorRetrieverType})
	if err != nil {
		t.Fatalf("BatchIndex returned error: %v", err)
	}
	if len(embedder.batchTexts) != 1 {
		t.Fatalf("expected one embedding input, got %d", len(embedder.batchTexts))
	}
	if got := len([]rune(embedder.batchTexts[0])); got > safetyMaxChars {
		t.Fatalf("embedding input length = %d, want <= %d", got, safetyMaxChars)
	}
}

func assertImagePayloadRemoved(t *testing.T, content string, payload string) {
	t.Helper()
	if strings.Contains(content, "data:image/png;base64") || strings.Contains(content, payload) {
		t.Fatalf("embedding input still contains inline image payload: %q", content)
	}
	if !strings.Contains(content, "before") || !strings.Contains(content, "after") {
		t.Fatalf("embedding input should preserve surrounding text, got %q", content)
	}
}
