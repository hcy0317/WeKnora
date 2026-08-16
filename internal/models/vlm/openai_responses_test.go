package vlm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	openai "github.com/sashabaranov/go-openai"
	"github.com/stretchr/testify/require"
)

func TestRemoteAPIVLMRetriesResponsesWithoutUnsupportedMaxOutputTokens(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var responsesCalls atomic.Int32
	var withMax atomic.Int32
	var withoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesCalls.Add(1)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if _, ok := body["max_output_tokens"]; ok {
				withMax.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
				return
			}
			withoutMax.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"vision"}]}]}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			http.Error(w, "must not use Chat Completions", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	model := &RemoteAPIVLM{
		modelName: "test-vlm", modelID: "test-vlm-sync-capability",
		baseURL: server.URL + "/v1", httpClient: server.Client(), autoProtocol: true,
	}
	request := openai.ChatCompletionRequest{
		Model: "test-vlm", MaxCompletionTokens: 1024,
		Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "describe"}},
	}
	for range 2 {
		content, err := model.predictWithNegotiatedProtocol(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, "vision", content)
	}
	require.EqualValues(t, 3, responsesCalls.Load())
	require.EqualValues(t, 1, withMax.Load())
	require.EqualValues(t, 2, withoutMax.Load())
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIVLMSyncResponsesAcceptsSSEWithoutProtocolFallback(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			chatCalls.Add(1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"vision sse\"}\n\n"+
			"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer server.Close()
	model := &RemoteAPIVLM{modelName: "alias", modelID: "vlm-sync-sse", baseURL: server.URL + "/v1", httpClient: server.Client(), autoProtocol: true}
	content, err := model.predictWithNegotiatedProtocol(context.Background(), openai.ChatCompletionRequest{Model: "alias", Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "describe"}}})
	require.NoError(t, err)
	require.Equal(t, "vision sse", content)
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIVLMOpaqueFailureDoesNotFallbackToChat(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			call := responsesCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if call == 1 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`)
				return
			}
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses vision"}]}]}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"chat vision"}}]}`)
		}
	}))
	defer server.Close()

	model := &RemoteAPIVLM{
		modelName:    "test-vlm",
		modelID:      "test-vlm-opaque",
		baseURL:      server.URL + "/v1",
		httpClient:   server.Client(),
		autoProtocol: true,
	}
	request := openai.ChatCompletionRequest{Model: "test-vlm", Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "describe"}}}
	_, err := model.predictWithNegotiatedProtocol(context.Background(), request)
	require.Error(t, err)
	second, err := model.predictWithNegotiatedProtocol(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "responses vision", second)
	require.EqualValues(t, 2, responsesCalls.Load(), "opaque upstream failures must be reprobed")
	require.Zero(t, chatCalls.Load(), "opaque errors are not endpoint evidence")
	require.Equal(t, openaiapi.ProtocolResponses, openaiapi.PreferredProtocol(model.protocolCacheKey()))
}

func TestRemoteAPIVLMFailedFieldRetryDoesNotAlternateOrCache(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var withMax atomic.Int32
	var withoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if _, ok := body["max_output_tokens"]; ok {
				if withMax.Add(1) == 1 {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
					return
				}
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"vision"}]}]}`)
				return
			}
			withoutMax.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"detail":"unsupported request format"}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
		}
	}))
	defer server.Close()

	model := &RemoteAPIVLM{
		modelName: "test-vlm", modelID: "test-vlm-failed-retry",
		baseURL: server.URL + "/v1", httpClient: server.Client(), autoProtocol: true,
	}
	request := openai.ChatCompletionRequest{
		Model: "test-vlm", MaxCompletionTokens: 1024,
		Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "describe"}},
	}
	_, err := model.predictWithNegotiatedProtocol(context.Background(), request)
	require.ErrorContains(t, err, "unsupported request format")
	content, err := model.predictWithNegotiatedProtocol(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "vision", content)
	require.EqualValues(t, 2, withMax.Load())
	require.EqualValues(t, 1, withoutMax.Load())
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIVLMKnownResponsesNeverFallsBackToChat(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			if responsesCalls.Add(1) == 1 {
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"known"}]}]}`)
				return
			}
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(w, `{"detail":"unsupported request format"}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			_, _ = io.WriteString(w, `{"object":"chat.completion","choices":[{"message":{"content":"wrong"}}]}`)
		}
	}))
	defer server.Close()

	model := &RemoteAPIVLM{
		modelName: "known-responses", modelID: t.Name(), baseURL: server.URL + "/v1",
		httpClient: server.Client(), autoProtocol: true,
	}
	request := openai.ChatCompletionRequest{
		Model:    "known-responses",
		Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "describe"}},
	}
	content, err := model.predictWithNegotiatedProtocol(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "known", content)
	_, err = model.predictWithNegotiatedProtocol(context.Background(), request)
	require.Error(t, err)
	require.EqualValues(t, 2, responsesCalls.Load())
	require.Zero(t, chatCalls.Load())
}
