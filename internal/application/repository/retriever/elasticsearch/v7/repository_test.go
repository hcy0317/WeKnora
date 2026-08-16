package v7

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	typesLocal "github.com/Tencent/WeKnora/internal/types"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/stretchr/testify/require"
)

func TestBatchSaveReturnsErrorWhenBulkContainsFailedItem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = io.WriteString(w, `{
			"errors": true,
			"items": [
				{"index": {"status": 201}},
				{"index": {"status": 429, "error": {"type": "es_rejected_execution_exception", "reason": "queue full"}}}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
	if err != nil {
		t.Fatalf("create Elasticsearch client: %v", err)
	}
	repository := &elasticsearchRepository{client: client, index: "test-index"}

	err = repository.BatchSave(context.Background(), []*typesLocal.IndexInfo{
		{ChunkID: "chunk-1", SourceID: "source-1", Content: "first"},
		{ChunkID: "chunk-2", SourceID: "source-2", Content: "second"},
	}, nil)
	if err == nil {
		t.Fatal("BatchSave returned nil for a bulk response containing a failed item")
	}
	if !strings.Contains(err.Error(), "1/2") {
		t.Fatalf("BatchSave error %q does not identify the partial failure", err)
	}
}

func TestProcessBulkResponseReturnsErrorWhenErrorsFlagCannotBeExplained(t *testing.T) {
	response := &esapi.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(`{
			"errors": true,
			"items": [{"create": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}}]
		}`)),
	}

	err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 1)
	if err == nil {
		t.Fatal("processBulkResponse returned nil for a bulk item failure")
	}
}

func TestProcessBulkResponseReturnsErrorForItemFailureDespiteFalseErrorsFlag(t *testing.T) {
	response := &esapi.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(`{
			"errors": false,
			"items": [{"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}}]
		}`)),
	}

	err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 1)
	if err == nil {
		t.Fatal("processBulkResponse trusted the top-level flag despite a bulk item failure")
	}
}

func TestProcessBulkResponseReturnsErrorForMalformedBody(t *testing.T) {
	response := &esapi.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"errors":`)),
	}

	err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 1)
	if err == nil {
		t.Fatal("processBulkResponse returned nil for an unreadable bulk response")
	}
}

func TestProcessBulkResponseRequiresExactRecognizedItems(t *testing.T) {
	tests := []struct {
		name, body string
		wantErr    bool
	}{
		{name: "empty", body: `{"errors":false,"items":[]}`, wantErr: true},
		{name: "partial", body: `{"errors":false,"items":[{"index":{"status":201}}]}`, wantErr: true},
		{name: "wrong operation", body: `{"errors":false,"items":[{"create":{"status":201}},{"index":{"status":201}}]}`, wantErr: true},
		{name: "missing status", body: `{"errors":false,"items":[{"index":{}},{"index":{"status":201}}]}`, wantErr: true},
		{name: "non 2xx without error", body: `{"errors":false,"items":[{"index":{"status":500}},{"index":{"status":201}}]}`, wantErr: true},
		{name: "exact", body: `{"errors":false,"items":[{"index":{"status":200}},{"index":{"status":201}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &esapi.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tt.body))}
			err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 2)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessBulkResponseRejectsNilEnvelope(t *testing.T) {
	require.Error(t, (&elasticsearchRepository{}).processBulkResponse(context.Background(), nil, 1))
	require.Error(t, (&elasticsearchRepository{}).processBulkResponse(context.Background(), &esapi.Response{StatusCode: 200}, 1))
}

func TestProcessBulkResponseErrorDoesNotLeakReason(t *testing.T) {
	response := &esapi.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(`{
			"errors": false,
			"items": [{"index": {"status": 400, "error": {"reason": "SECRET document content"}}}]
		}`)),
	}

	err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 1)
	if err == nil {
		t.Fatal("item failure must return an error")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaked backend reason: %v", err)
	}
}

func TestProcessBulkResponseHTTPErrorDoesNotLeakBody(t *testing.T) {
	response := &esapi.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"reason":"SECRET document content"}}`)),
	}
	err := (&elasticsearchRepository{}).processBulkResponse(context.Background(), response, 1)
	if err == nil {
		t.Fatal("HTTP error must fail")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaked backend response body: %v", err)
	}
}

func TestDeleteByQueryRejectsInvalidSuccessBodies(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{"deleted":`},
		{name: "timed out", body: `{"deleted":0,"timed_out":true,"version_conflicts":0,"failures":[]}`},
		{name: "version conflicts", body: `{"deleted":0,"timed_out":false,"version_conflicts":1,"failures":[]}`},
		{name: "failures", body: `{"deleted":0,"timed_out":false,"version_conflicts":0,"failures":[{"cause":{"reason":"SECRET document content"}}]}`},
		{name: "unknown shape", body: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				_, _ = io.WriteString(w, tt.body)
			}))
			t.Cleanup(server.Close)

			client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
			if err != nil {
				t.Fatalf("create Elasticsearch client: %v", err)
			}
			repository := &elasticsearchRepository{client: client, index: "test-index"}
			err = repository.DeleteByChunkIDList(context.Background(), []string{"chunk-1"}, 0, "")
			if err == nil {
				t.Fatal("invalid delete-by-query success body was accepted")
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("error leaked backend reason: %v", err)
			}
		})
	}
}

func TestDeleteByQueryAcceptsStrictSuccessBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = io.WriteString(w, `{"deleted":1,"timed_out":false,"version_conflicts":0,"failures":[]}`)
	}))
	t.Cleanup(server.Close)

	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
	if err != nil {
		t.Fatalf("create Elasticsearch client: %v", err)
	}
	repository := &elasticsearchRepository{client: client, index: "test-index"}
	if err := repository.DeleteByChunkIDList(context.Background(), []string{"chunk-1"}, 0, ""); err != nil {
		t.Fatalf("strict success body rejected: %v", err)
	}
}

func TestDeleteByQueryHTTPErrorDoesNotLeakBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":{"reason":"SECRET document content"}}`)
	}))
	t.Cleanup(server.Close)

	client, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
	if err != nil {
		t.Fatalf("create Elasticsearch client: %v", err)
	}
	err = (&elasticsearchRepository{client: client, index: "test-index"}).DeleteByChunkIDList(
		context.Background(), []string{"chunk-1"}, 0, "",
	)
	if err == nil {
		t.Fatal("HTTP error must fail")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaked backend response body: %v", err)
	}
}
