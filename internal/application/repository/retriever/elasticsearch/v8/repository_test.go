package v8

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	localtypes "github.com/Tencent/WeKnora/internal/types"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type bulkRoundTripper func(*http.Request) (*http.Response, error)

func (f bulkRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestBatchSaveReturnsBulkItemFailure(t *testing.T) {
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Transport: bulkRoundTripper(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Type":      []string{"application/json"},
					"X-Elastic-Product": []string{"Elasticsearch"},
				},
				Body: io.NopCloser(strings.NewReader(`{
					"errors": true,
					"items": [
						{"create":{"_index":"test","_id":"chunk-ok","status":201,"result":"created"}},
						{"create":{"_index":"test","_id":"chunk-failed","status":429,"error":{"type":"es_rejected_execution_exception","reason":"sensitive detail"}}}
					],
					"took": 1
				}`)),
			}, nil
		}),
	})
	require.NoError(t, err)

	repository := &elasticsearchRepository{client: client, index: "test"}
	infos := []*localtypes.IndexInfo{
		{SourceID: "source-ok", ChunkID: "chunk-ok"},
		{SourceID: "source-failed", ChunkID: "chunk-failed"},
	}
	err = repository.BatchSave(context.Background(), infos, map[string]any{
		"embedding": map[string][]float32{
			"source-ok":     {1, 0},
			"source-failed": {0, 1},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk-failed")
	assert.Contains(t, err.Error(), "es_rejected_execution_exception")
	assert.NotContains(t, err.Error(), "sensitive detail")
}

func TestBatchSaveRequiresExactRecognizedItems(t *testing.T) {
	tests := []struct {
		name, response string
		wantErr        bool
	}{
		{name: "empty", response: `{"errors":false,"items":[],"took":1}`, wantErr: true},
		{name: "partial", response: `{"errors":false,"items":[{"create":{"status":201}}],"took":1}`, wantErr: true},
		{name: "wrong operation", response: `{"errors":false,"items":[{"index":{"status":201}},{"create":{"status":201}}],"took":1}`, wantErr: true},
		{name: "missing status", response: `{"errors":false,"items":[{"create":{}},{"create":{"status":201}}],"took":1}`, wantErr: true},
		{name: "non 2xx without error", response: `{"errors":false,"items":[{"create":{"status":500}},{"create":{"status":201}}],"took":1}`, wantErr: true},
		{name: "contradictory item failure", response: `{"errors":false,"items":[{"create":{"status":429,"error":{"type":"rejected","reason":"sensitive content"}}},{"create":{"status":201}}],"took":1}`, wantErr: true},
		{name: "exact", response: `{"errors":false,"items":[{"create":{"status":200}},{"create":{"status":201}}],"took":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := elasticsearch.NewTypedClient(elasticsearch.Config{Transport: bulkRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Elastic-Product": {"Elasticsearch"}}, Body: io.NopCloser(strings.NewReader(tt.response))}, nil
			})})
			require.NoError(t, err)
			repository := &elasticsearchRepository{client: client, index: "test"}
			err = repository.BatchSave(context.Background(), []*localtypes.IndexInfo{{SourceID: "s1", ChunkID: "c1"}, {SourceID: "s2", ChunkID: "c2"}}, map[string]any{"embedding": map[string][]float32{"s1": {1}, "s2": {2}}})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBulkResponseErrorRejectsNilEnvelope(t *testing.T) {
	require.Error(t, bulkResponseError(nil, 1))
}

func TestDeleteMethodsRequireCompleteResponseAndAcceptZeroMatch(t *testing.T) {
	tests := []struct {
		name, response string
		wantErr        bool
	}{
		{name: "missing", response: `{}`, wantErr: true},
		{name: "inconsistent", response: `{"deleted":0,"total":1,"timed_out":false,"version_conflicts":0,"failures":[]}`, wantErr: true},
		{name: "zero match", response: `{"deleted":0,"total":0,"timed_out":false,"version_conflicts":0,"failures":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := elasticsearch.NewTypedClient(elasticsearch.Config{Transport: bulkRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Elastic-Product": {"Elasticsearch"}}, Body: io.NopCloser(strings.NewReader(tt.response))}, nil
			})})
			require.NoError(t, err)
			repository := &elasticsearchRepository{client: client, index: "test"}
			methods := []func() error{
				func() error { return repository.DeleteByChunkIDList(context.Background(), []string{"c"}, 0, "") },
				func() error { return repository.DeleteByKnowledgeIDList(context.Background(), []string{"k"}, 0, "") },
				func() error { return repository.DeleteBySourceIDList(context.Background(), []string{"s"}, 0, "") },
			}
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

func TestDeleteBySourceIDListReturnsResponseFailures(t *testing.T) {
	testCases := []struct {
		name     string
		response string
		contains string
	}{
		{
			name: "item failure",
			response: `{
				"deleted":0,"failures":[{"cause":{"type":"version_conflict_engine_exception","reason":"sensitive detail"},"id":"chunk-failed","index":"test","status":409}],
				"timed_out":false,"version_conflicts":0
			}`,
			contains: "chunk-failed",
		},
		{
			name:     "timeout",
			response: `{"deleted":0,"failures":[],"timed_out":true,"version_conflicts":0}`,
			contains: "timed out",
		},
		{
			name:     "version conflicts",
			response: `{"deleted":0,"failures":[],"timed_out":false,"version_conflicts":2}`,
			contains: "2 version conflicts",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
				Transport: bulkRoundTripper(func(_ *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Content-Type":      []string{"application/json"},
							"X-Elastic-Product": []string{"Elasticsearch"},
						},
						Body: io.NopCloser(strings.NewReader(testCase.response)),
					}, nil
				}),
			})
			require.NoError(t, err)

			repository := &elasticsearchRepository{client: client, index: "test"}
			err = repository.DeleteBySourceIDList(context.Background(), []string{"source-failed"}, 0, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), testCase.contains)
			assert.NotContains(t, err.Error(), "sensitive detail")
		})
	}
}

func TestDeleteMethodsReturnResponseFailureAndUseCorrectField(t *testing.T) {
	testCases := []struct {
		name          string
		expectedField string
		delete        func(*elasticsearchRepository) error
	}{
		{name: "chunk", expectedField: "chunk_id", delete: func(repository *elasticsearchRepository) error {
			return repository.DeleteByChunkIDList(context.Background(), []string{"chunk-failed"}, 0, "")
		}},
		{name: "knowledge", expectedField: "knowledge_id", delete: func(repository *elasticsearchRepository) error {
			return repository.DeleteByKnowledgeIDList(context.Background(), []string{"knowledge-failed"}, 0, "")
		}},
		{name: "source", expectedField: "source_id", delete: func(repository *elasticsearchRepository) error {
			return repository.DeleteBySourceIDList(context.Background(), []string{"source-failed"}, 0, "")
		}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
				Transport: bulkRoundTripper(func(request *http.Request) (*http.Response, error) {
					var body struct {
						Query struct {
							Terms map[string]any `json:"terms"`
						} `json:"query"`
					}
					require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
					_, ok := body.Query.Terms[testCase.expectedField]
					assert.True(t, ok, "expected terms field %s in %#v", testCase.expectedField, body.Query.Terms)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Content-Type":      []string{"application/json"},
							"X-Elastic-Product": []string{"Elasticsearch"},
						},
						Body: io.NopCloser(strings.NewReader(`{
							"deleted":0,"failures":[{"cause":{"type":"illegal_argument_exception","reason":"sensitive detail"},"id":"chunk-failed","index":"test","status":400}],
							"timed_out":false,"version_conflicts":0
						}`)),
					}, nil
				}),
			})
			require.NoError(t, err)
			repository := &elasticsearchRepository{client: client, index: "test"}

			err = testCase.delete(repository)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "illegal_argument_exception")
			assert.NotContains(t, err.Error(), "sensitive detail")
		})
	}
}
