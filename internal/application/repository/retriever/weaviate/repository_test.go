package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	localtypes "github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	weaviateclient "github.com/weaviate/weaviate-go-client/v5/weaviate"
)

func TestBatchSaveReturnsObjectFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/v1/meta" {
			_, _ = fmt.Fprint(writer, `{"version":"1.37.3"}`)
			return
		}
		assert.Equal(t, "/v1/batch/objects", request.URL.Path)
		writer.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(writer, `[
			{"class":"Weknora_embeddings_2","id":"11111111-1111-1111-1111-111111111111","result":{"status":"SUCCESS"}},
			{"class":"Weknora_embeddings_2","id":"22222222-2222-2222-2222-222222222222","result":{"status":"FAILED","errors":{"error":[{"message":"sensitive detail"}]}}}
		]`)
	}))
	t.Cleanup(server.Close)

	client, err := weaviateclient.NewClient(weaviateclient.Config{
		Host:   strings.TrimPrefix(server.URL, "http://"),
		Scheme: "http",
	})
	require.NoError(t, err)

	repository := &weaviateRepository{client: client, collectionBaseName: defaultCollectionName}
	repository.initializedCollections.Store(2, true)
	infos := []*localtypes.IndexInfo{
		{SourceID: "source-ok", ChunkID: "11111111-1111-1111-1111-111111111111"},
		{SourceID: "source-failed", ChunkID: "22222222-2222-2222-2222-222222222222"},
	}
	err = repository.BatchSave(context.Background(), infos, map[string]any{
		"embedding": map[string][]float32{
			"source-ok":     {1, 0},
			"source-failed": {0, 1},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "22222222-2222-2222-2222-222222222222")
	assert.NotContains(t, err.Error(), "sensitive detail")
}

func TestBatchSaveRequiresExactSuccessfulResponses(t *testing.T) {
	tests := []struct {
		name, response string
		wantErr        bool
	}{
		{name: "empty", response: `[]`, wantErr: true},
		{name: "partial", response: `[{"id":"11111111-1111-1111-1111-111111111111","result":{"status":"SUCCESS"}}]`, wantErr: true},
		{name: "missing result", response: `[{"id":"11111111-1111-1111-1111-111111111111"},{"id":"22222222-2222-2222-2222-222222222222","result":{"status":"SUCCESS"}}]`, wantErr: true},
		{name: "unknown status", response: `[{"id":"11111111-1111-1111-1111-111111111111","result":{"status":"PENDING"}},{"id":"22222222-2222-2222-2222-222222222222","result":{"status":"SUCCESS"}}]`, wantErr: true},
		{name: "exact", response: `[{"id":"11111111-1111-1111-1111-111111111111","result":{"status":"SUCCESS"}},{"id":"22222222-2222-2222-2222-222222222222","result":{"status":"SUCCESS"}}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v1/meta" {
					_, _ = fmt.Fprint(w, `{"version":"1.37.3"}`)
					return
				}
				_, _ = fmt.Fprint(w, tt.response)
			}))
			defer server.Close()
			client, err := weaviateclient.NewClient(weaviateclient.Config{Host: strings.TrimPrefix(server.URL, "http://"), Scheme: "http"})
			require.NoError(t, err)
			repository := &weaviateRepository{client: client, collectionBaseName: defaultCollectionName}
			repository.initializedCollections.Store(1, true)
			err = repository.BatchSave(context.Background(), []*localtypes.IndexInfo{{SourceID: "s1", ChunkID: "11111111-1111-1111-1111-111111111111"}, {SourceID: "s2", ChunkID: "22222222-2222-2222-2222-222222222222"}}, map[string]any{"embedding": map[string][]float32{"s1": {1}, "s2": {2}}})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteMethodsRequireCompleteResponseAndAcceptZeroMatch(t *testing.T) {
	tests := []struct {
		name, response string
		wantErr        bool
	}{
		{name: "missing", response: `{}`, wantErr: true},
		{name: "inconsistent", response: `{"results":{"matches":1,"successful":0,"failed":0,"objects":[]}}`, wantErr: true},
		{name: "zero match", response: `{"results":{"matches":0,"successful":0,"failed":0,"objects":[]}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v1/meta" {
					_, _ = fmt.Fprint(w, `{"version":"1.37.3"}`)
					return
				}
				_, _ = fmt.Fprint(w, tt.response)
			}))
			defer server.Close()
			client, err := weaviateclient.NewClient(weaviateclient.Config{Host: strings.TrimPrefix(server.URL, "http://"), Scheme: "http"})
			require.NoError(t, err)
			repository := &weaviateRepository{client: client, collectionBaseName: defaultCollectionName}
			methods := []func() error{func() error { return repository.DeleteByChunkIDList(context.Background(), []string{"c"}, 1, "") }, func() error { return repository.DeleteByKnowledgeIDList(context.Background(), []string{"k"}, 1, "") }, func() error { return repository.DeleteBySourceIDList(context.Background(), []string{"s"}, 1, "") }}
			for _, method := range methods {
				err = method()
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestBatchSaveRejectsMissingEmbeddingBeforeHTTP(t *testing.T) {
	testCases := []struct {
		name   string
		infos  []*localtypes.IndexInfo
		params map[string]any
	}{
		{
			name:   "all invalid",
			infos:  []*localtypes.IndexInfo{{SourceID: "source-missing", ChunkID: "chunk-missing"}},
			params: map[string]any{"embedding": map[string][]float32{}},
		},
		{
			name: "mixed valid and invalid",
			infos: []*localtypes.IndexInfo{
				{SourceID: "source-ok", ChunkID: "chunk-ok"},
				{SourceID: "source-missing", ChunkID: "chunk-missing"},
			},
			params: map[string]any{"embedding": map[string][]float32{"source-ok": {1, 0}}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				requests.Add(1)
			}))
			t.Cleanup(server.Close)
			client, err := weaviateclient.NewClient(weaviateclient.Config{
				Host: strings.TrimPrefix(server.URL, "http://"), Scheme: "http",
			})
			require.NoError(t, err)
			requests.Store(0)
			repository := &weaviateRepository{client: client, collectionBaseName: defaultCollectionName}

			err = repository.BatchSave(context.Background(), testCase.infos, testCase.params)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "chunk-missing")
			assert.Equal(t, int32(0), requests.Load())
		})
	}
}

func TestDeleteMethodsReturnBatchFailureAndUseCorrectField(t *testing.T) {
	testCases := []struct {
		name          string
		expectedField string
		delete        func(*weaviateRepository) error
	}{
		{name: "chunk", expectedField: fieldChunkID, delete: func(repository *weaviateRepository) error {
			return repository.DeleteByChunkIDList(context.Background(), []string{"chunk-failed"}, 2, "")
		}},
		{name: "knowledge", expectedField: fieldKnowledgeID, delete: func(repository *weaviateRepository) error {
			return repository.DeleteByKnowledgeIDList(context.Background(), []string{"knowledge-failed"}, 2, "")
		}},
		{name: "source", expectedField: fieldSourceID, delete: func(repository *weaviateRepository) error {
			return repository.DeleteBySourceIDList(context.Background(), []string{"source-failed"}, 2, "")
		}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				if request.URL.Path == "/v1/meta" {
					_, _ = fmt.Fprint(writer, `{"version":"1.37.3"}`)
					return
				}
				assert.Equal(t, http.MethodDelete, request.Method)
				assert.Equal(t, "/v1/batch/objects", request.URL.Path)
				var body struct {
					Match struct {
						Where struct {
							Path []string `json:"path"`
						} `json:"where"`
					} `json:"match"`
				}
				require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
				assert.Equal(t, []string{testCase.expectedField}, body.Match.Where.Path)
				_, _ = fmt.Fprint(writer, `{
					"match":{"class":"Weknora_embeddings_2"},"output":"minimal",
					"results":{"failed":1,"limit":10000,"matches":1,"successful":0,"objects":[
						{"id":"22222222-2222-2222-2222-222222222222","status":"FAILED","errors":{"error":[{"message":"sensitive detail"}]}}
					]}
				}`)
			}))
			t.Cleanup(server.Close)
			client, err := weaviateclient.NewClient(weaviateclient.Config{
				Host: strings.TrimPrefix(server.URL, "http://"), Scheme: "http",
			})
			require.NoError(t, err)
			repository := &weaviateRepository{client: client, collectionBaseName: defaultCollectionName}

			err = testCase.delete(repository)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "22222222-2222-2222-2222-222222222222")
			assert.NotContains(t, err.Error(), "sensitive detail")
		})
	}
}
