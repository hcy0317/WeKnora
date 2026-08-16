package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func newNegotiatedTestChat(t *testing.T, baseURL string) *RemoteAPIChat {
	t.Helper()
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	model, err := NewRemoteAPIChat(&ChatConfig{
		Source:    types.ModelSourceRemote,
		BaseURL:   baseURL + "/v1",
		ModelName: "test-model",
		ModelID:   "test-model",
		APIKey:    "test-key",
		Provider:  "openai",
	})
	require.NoError(t, err)
	require.True(t, model.autoProtocol)
	return model
}

func TestRemoteAPIChatAzureReasoningPreservesSDKEndpointAndCachesSuccessfulShape(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/openai/deployments/azure-chat/chat/completions", r.URL.Path)
		require.Equal(t, "2026-07-01-preview", r.URL.Query().Get("api-version"))
		require.Equal(t, "azure-test-key", r.Header.Get("api-key"))
		require.Empty(t, r.Header.Get("Authorization"))

		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "high", body["reasoning_effort"])
		if calls.Add(1) == 1 {
			require.Contains(t, body, "max_tokens")
			require.NotContains(t, body, "max_completion_tokens")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"Unsupported parameter: max_tokens; use max_completion_tokens","type":"invalid_request_error"}}`)
			return
		}
		require.Contains(t, body, "max_completion_tokens")
		require.NotContains(t, body, "max_tokens")
		require.NotContains(t, body, "temperature")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"azure ok"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()

	model, err := NewRemoteAPIChat(&ChatConfig{
		Source:    types.ModelSourceRemote,
		BaseURL:   server.URL,
		APIKey:    "azure-test-key",
		ModelName: "azure-chat",
		ModelID:   t.Name(),
		Provider:  "azure_openai",
		ExtraConfig: map[string]string{
			"api_version":      "2026-07-01-preview",
			"reasoning_effort": "high",
		},
	})
	require.NoError(t, err)
	for range 2 {
		response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128, Temperature: 0.2})
		require.NoError(t, err)
		require.Equal(t, "azure ok", response.Content)
	}
	require.EqualValues(t, 3, calls.Load(), "successful alternate SDK shape must be cached")
}

func TestRemoteAPIChatAzureStreamCachesAlternateShapeOnlyAfterCompletedStream(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var defaultCalls atomic.Int32
	var alternateCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/openai/deployments/azure-chat/chat/completions", r.URL.Path)
		require.Equal(t, "2026-07-01-preview", r.URL.Query().Get("api-version"))
		require.Equal(t, "azure-test-key", r.Header.Get("api-key"))
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, true, body["stream"])
		require.Equal(t, "high", body["reasoning_effort"])
		if _, modern := body["max_completion_tokens"]; !modern {
			defaultCalls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"Unsupported parameter: max_tokens; use max_completion_tokens","type":"invalid_request_error"}}`)
			return
		}
		attempt := alternateCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"chunk\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"azure\"}}]}\n\n")
		if attempt == 1 {
			return // HTTP 200 headers plus EOF is not a completed stream.
		}
		_, _ = io.WriteString(w, "data: {\"id\":\"chunk\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	model, err := NewRemoteAPIChat(&ChatConfig{
		Source: types.ModelSourceRemote, BaseURL: server.URL, APIKey: "azure-test-key",
		ModelName: "azure-chat", ModelID: t.Name(), Provider: "azure_openai",
		ExtraConfig: map[string]string{"api_version": "2026-07-01-preview", "reasoning_effort": "high"},
	})
	require.NoError(t, err)

	first, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
	require.NoError(t, err)
	var firstFailed bool
	for event := range first {
		firstFailed = firstFailed || event.ResponseType == types.ResponseTypeError
	}
	require.True(t, firstFailed)

	for range 2 {
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
		require.NoError(t, err)
		var completed bool
		for event := range stream {
			completed = completed || (event.Done && event.ResponseType != types.ResponseTypeError)
		}
		require.True(t, completed)
	}
	require.EqualValues(t, 2, defaultCalls.Load(), "incomplete HTTP 200 stream must not cache alternate shape")
	require.EqualValues(t, 3, alternateCalls.Load(), "completed alternate stream must be reused")
}

func TestRemoteAPIChatNegotiatedStreamCachesAlternateShapeOnlyAfterCompletedStream(t *testing.T) {
	var defaultCalls atomic.Int32
	var alternateCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/chat/completions", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		if _, modern := body["max_completion_tokens"]; !modern {
			defaultCalls.Add(1)
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(w, `{"detail":"temperature is not supported for this model"}`)
			return
		}
		attempt := alternateCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"chunk\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		if attempt == 1 {
			return
		}
		_, _ = io.WriteString(w, "data: {\"id\":\"chunk\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	cacheKey := model.protocolCacheKey(server.URL + "/v1")
	openaiapi.MarkProtocolSuccess(cacheKey, openaiapi.ProtocolChatCompletions)
	first, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128, Temperature: 0.2})
	require.NoError(t, err)
	for range first {
	}
	for range 2 {
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128, Temperature: 0.2})
		require.NoError(t, err)
		for range stream {
		}
	}
	require.EqualValues(t, 2, defaultCalls.Load(), "HTTP 200 plus incomplete stream must not cache alternate fields")
	require.EqualValues(t, 3, alternateCalls.Load())
}

func TestRemoteAPIChatProtocolFingerprintTracksFullSavedConfiguration(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	newModel := func(extra map[string]string, headers map[string]string, apiKey string) *RemoteAPIChat {
		model, err := NewRemoteAPIChat(&ChatConfig{
			Source: types.ModelSourceRemote, BaseURL: server.URL, APIKey: apiKey,
			ModelName: "azure-chat", ModelID: "saved-model", Provider: "azure_openai",
			ExtraConfig: extra, CustomHeaders: headers,
		})
		require.NoError(t, err)
		return model
	}
	base := newModel(map[string]string{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-one")
	baseKey := base.protocolCacheKey(base.baseURL)
	require.NotEqual(t, baseKey, newModel(map[string]string{"api_version": "2027-01-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-one").protocolCacheKey(base.baseURL))
	require.NotEqual(t, baseKey, newModel(map[string]string{"api_version": "2026-07-01", "vendor_flag": "b"}, map[string]string{"X-Tenant": "one"}, "key-one").protocolCacheKey(base.baseURL))
	require.NotEqual(t, baseKey, newModel(map[string]string{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "two"}, "key-one").protocolCacheKey(base.baseURL))
	require.NotEqual(t, baseKey, newModel(map[string]string{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-two").protocolCacheKey(base.baseURL))
}

func TestRemoteAPIChatNegotiatesAndCachesChatCompletionsFallback(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for range 2 {
		model := newNegotiatedTestChat(t, server.URL)
		response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		require.Equal(t, "fallback", response.Content)
	}
	require.EqualValues(t, 1, responsesCalls.Load(), "cached fallback must skip a second Responses probe")
	require.EqualValues(t, 2, chatCalls.Load())
}

func TestRemoteAPIChatSyncResponsesAcceptsSSEWithoutProtocolFallback(t *testing.T) {
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			chatCalls.Add(1)
			http.Error(w, "unexpected fallback", http.StatusInternalServerError)
			return
		}
		require.Equal(t, "/v1/responses", r.URL.Path)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"sync sse\"}\n\n"+
			"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"total_tokens\":9}}}\n\n")
	}))
	defer server.Close()

	response, err := newNegotiatedTestChat(t, server.URL).Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
	require.NoError(t, err)
	require.Equal(t, "sync sse", response.Content)
	require.Equal(t, 9, response.Usage.TotalTokens)
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIChatSyncResponsesDecodeErrorDoesNotFallbackOrCache(t *testing.T) {
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			chatCalls.Add(1)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "error: secret-model-output")
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
	var decodeErr *openaiapi.ResponsesDecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.NotContains(t, err.Error(), "secret-model-output")
	require.Zero(t, chatCalls.Load())
	require.False(t, openaiapi.ResolveProtocol(model.protocolCacheKey(server.URL+"/v1")).Known)
}

func TestRemoteAPIChatNegotiatesEveryChatCompletionsProvider(t *testing.T) {
	tests := []struct {
		name      string
		provider  string
		basePath  string
		wantPath  string
		appID     string
		appSecret string
	}{
		{name: "OpenAI", provider: "openai", basePath: "/v1", wantPath: "/v1/responses"},
		{name: "DeepSeek", provider: "deepseek", basePath: "/v1", wantPath: "/v1/responses"},
		{name: "WeKnoraCloud chat endpoint", provider: "weknoracloud", wantPath: "/api/v1/responses", appID: "app", appSecret: "secret"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SSRF_WHITELIST", "127.0.0.1")
			var capturedPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedPath = r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
			}))
			defer server.Close()

			model, err := NewRemoteAPIChat(&ChatConfig{
				Source:    types.ModelSourceRemote,
				BaseURL:   server.URL + tt.basePath,
				ModelName: "test-model",
				ModelID:   "test-model",
				APIKey:    "test-key",
				Provider:  tt.provider,
				AppID:     tt.appID,
				AppSecret: tt.appSecret,
			})
			require.NoError(t, err)
			response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
			require.NoError(t, err)
			require.Equal(t, "ok", response.Content)
			require.Equal(t, tt.wantPath, capturedPath)
		})
	}
}

func TestRemoteAPIChatDoesNotReplayNonEndpointErrors(t *testing.T) {
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			chatCalls.Add(1)
		}
		http.Error(w, "invalid request", http.StatusBadRequest)
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
	require.ErrorContains(t, err, "status 400")
	require.Zero(t, chatCalls.Load(), "generic 400 must not replay a request")
}

func TestRemoteAPIChatOpaqueOrTransientFailureDoesNotFallbackToChat(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "opaque upstream 400", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`},
		{name: "temporary 503", status: http.StatusServiceUnavailable, body: `{"error":{"message":"Service temporarily unavailable","type":"api_error"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var responsesCalls atomic.Int32
			var chatCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/responses":
					call := responsesCalls.Add(1)
					w.Header().Set("Content-Type", "application/json")
					if call == 1 {
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
						return
					}
					_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
				case "/v1/chat/completions":
					chatCalls.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"chat"},"finish_reason":"stop"}]}`)
				}
			}))
			defer server.Close()

			model := newNegotiatedTestChat(t, server.URL)
			_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
			require.Error(t, err)
			second, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "again"}}, &ChatOptions{})
			require.NoError(t, err)
			require.Equal(t, "responses", second.Content)
			require.EqualValues(t, 2, responsesCalls.Load(), "opaque upstream failures must be reprobed")
			require.Zero(t, chatCalls.Load(), "opaque or transient errors are not endpoint evidence")
			require.Equal(t, openaiapi.ProtocolResponses, openaiapi.PreferredProtocol(model.protocolCacheKey(server.URL+"/v1")))
		})
	}
}

func TestRemoteAPIChatCachedResponsesDoesNotFlipOn503(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			call := responsesCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if call == 2 {
				http.Error(w, `{"error":{"message":"Service temporarily unavailable","type":"api_error"}}`, http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "prime"}}, &ChatOptions{})
	require.NoError(t, err)
	_, err = model.Chat(context.Background(), []Message{{Role: "user", Content: "temporary failure"}}, &ChatOptions{})
	require.ErrorContains(t, err, "status 503")
	third, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "recover"}}, &ChatOptions{})
	require.NoError(t, err)
	require.Equal(t, "responses", third.Content)
	require.EqualValues(t, 3, responsesCalls.Load())
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIChatKnownResponsesSyncAndStreamDoNotFlipOn422(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			if responsesCalls.Add(1) == 1 {
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"prime"}]}]}`)
				return
			}
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(w, `{"detail":"unsupported request format"}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "prime"}}, &ChatOptions{})
	require.NoError(t, err)
	_, err = model.Chat(context.Background(), []Message{{Role: "user", Content: "sync"}}, &ChatOptions{})
	require.ErrorContains(t, err, "status 422")
	_, err = model.ChatStream(context.Background(), []Message{{Role: "user", Content: "stream"}}, &ChatOptions{})
	require.ErrorContains(t, err, "status 422")
	require.EqualValues(t, 3, responsesCalls.Load())
	require.Zero(t, chatCalls.Load())
}

func TestRemoteAPIChatNegotiatesAndCachesModernChatShapeWithoutModelNames(t *testing.T) {
	for _, modelName := range []string{"private-reasoning-alias", "qwen-custom"} {
		t.Run(modelName, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/v1/chat/completions", r.URL.Path)
				var body map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				if calls.Add(1) == 1 {
					require.Contains(t, body, "max_tokens")
					require.NotContains(t, body, "max_completion_tokens")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_tokens; use max_completion_tokens"}`)
					return
				}
				require.Contains(t, body, "max_completion_tokens")
				require.NotContains(t, body, "max_tokens")
				require.NotContains(t, body, "temperature")
				_, _ = io.WriteString(w, `{"object":"chat.completion","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer server.Close()

			model := newNegotiatedTestChat(t, server.URL)
			model.modelName = modelName
			model.modelID = t.Name()
			cacheKey := model.protocolCacheKey(server.URL + "/v1")
			openaiapi.MarkProtocolSuccess(cacheKey, openaiapi.ProtocolChatCompletions)
			for range 2 {
				response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128, Temperature: 0.2})
				require.NoError(t, err)
				require.Equal(t, "ok", response.Content)
			}
			require.EqualValues(t, 3, calls.Load())
		})
	}
}

func TestRemoteAPIChatSerializesOnlyConcurrentUnknownProbe(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	responsesStarted := make(chan struct{})
	releaseResponses := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesCalls.Add(1)
			select {
			case <-responsesStarted:
			default:
				close(responsesStarted)
			}
			<-releaseResponses
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"chat"},"finish_reason":"stop"}]}`)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	call := func(content string) {
		defer wg.Done()
		_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: content}}, &ChatOptions{})
		errs <- err
	}
	wg.Add(1)
	go call("first")
	<-responsesStarted
	wg.Add(1)
	go call("second")
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, responsesCalls.Load(), "only one unknown probe may reach Responses")
	close(releaseResponses)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.Error(t, err)
	}
	require.EqualValues(t, 2, responsesCalls.Load(), "the waiter must re-probe after an opaque failure")
	require.Zero(t, chatCalls.Load(), "opaque failures must not open Chat")
}

func TestRemoteAPIChatSwitchesProtocolOnExplicitFormatError(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesCalls.Add(1)
			http.Error(w, "unknown parameter: input", http.StatusBadRequest)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"chat"},"finish_reason":"stop"}]}`)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
	require.NoError(t, err)
	require.Equal(t, "chat", response.Content)
	require.EqualValues(t, 1, responsesCalls.Load())
	require.EqualValues(t, 1, chatCalls.Load())
}

func TestRemoteAPIChatRetriesSyncResponsesWithoutUnsupportedMaxOutputTokens(t *testing.T) {
	var responsesCalls atomic.Int32
	var responsesWithMax atomic.Int32
	var responsesWithoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesCalls.Add(1)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			_, hasMax := body["max_output_tokens"]
			if streaming, _ := body["stream"].(bool); streaming {
				require.True(t, hasMax, "sync-only capability must not strip the streaming limit")
				responsesWithMax.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"stream\"}\n\n")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"stream\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
				return
			}
			if hasMax {
				responsesWithMax.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
				return
			}
			responsesWithoutMax.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			http.Error(w, "must not use Chat Completions", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	for range 2 {
		response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
		require.NoError(t, err)
		require.Equal(t, "responses", response.Content)
	}
	stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
	require.NoError(t, err)
	for range stream {
	}

	require.EqualValues(t, 4, responsesCalls.Load())
	require.EqualValues(t, 2, responsesWithMax.Load(), "one rejected sync probe plus the unaffected stream")
	require.EqualValues(t, 2, responsesWithoutMax.Load(), "retry and later sync request must omit the field")
	require.Zero(t, chatCalls.Load())
	require.Equal(t, openaiapi.ProtocolResponses, openaiapi.PreferredProtocol(model.protocolCacheKey(server.URL+"/v1")))
}

func TestRemoteAPIChatSerializesConcurrentResponsesMaxOutputCapabilityProbe(t *testing.T) {
	const callers = 16
	var withMax atomic.Int32
	var withoutMax atomic.Int32
	followersArrived := make(chan struct{})
	var followersOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, ok := body["max_output_tokens"]; ok {
			withMax.Add(1)
			select {
			case <-followersArrived:
			case <-time.After(2 * time.Second):
				http.Error(w, "followers were blocked behind the field probe", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
			return
		}
		count := withoutMax.Add(1)
		if count == callers-1 {
			followersOnce.Do(func() { close(followersArrived) })
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	openaiapi.MarkProtocolSuccess(model.protocolCacheKey(server.URL+"/v1"), openaiapi.ProtocolResponses)
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
			if err == nil && (response == nil || response.Content != "responses") {
				err = fmt.Errorf("unexpected response: %#v", response)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, withMax.Load(), "one saved-model configuration must emit only one rejected field probe")
	require.EqualValues(t, callers, withoutMax.Load(), "one retry plus all waiting business requests must use the proven shape")
}

func TestRemoteAPIChatRetriesResponsesFieldDowngradeForExplicitOrMasked400Or422(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "explicit 400", status: http.StatusBadRequest, body: `{"detail":"Unsupported parameter: max_output_tokens"}`},
		{name: "explicit 422", status: http.StatusUnprocessableEntity, body: `{"detail":"Unsupported parameter: max_output_tokens"}`},
		{name: "masked upstream 400", status: http.StatusBadRequest, body: `{"error":{"message":"Upstream request failed","type":"upstream_error"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var withMax atomic.Int32
			var withoutMax atomic.Int32
			var chatCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/responses":
					var body map[string]any
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					if _, ok := body["max_output_tokens"]; ok {
						withMax.Add(1)
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
						return
					}
					withoutMax.Add(1)
					_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
				case "/v1/chat/completions":
					chatCalls.Add(1)
				}
			}))
			defer server.Close()

			model := newNegotiatedTestChat(t, server.URL)
			for range 2 {
				response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{MaxTokens: 128})
				require.NoError(t, err)
				require.Equal(t, "responses", response.Content)
			}
			require.EqualValues(t, 1, withMax.Load())
			require.EqualValues(t, 2, withoutMax.Load())
			require.Zero(t, chatCalls.Load())
		})
	}
}

func TestRemoteAPIChatFailedFieldRetryDoesNotAlternateOrCache(t *testing.T) {
	var withMax atomic.Int32
	var withoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if _, ok := body["max_output_tokens"]; ok {
				call := withMax.Add(1)
				if call == 1 {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
					return
				}
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
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

	model := newNegotiatedTestChat(t, server.URL)
	_, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "first"}}, &ChatOptions{MaxTokens: 128})
	require.ErrorContains(t, err, "unsupported request format")
	response, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "second"}}, &ChatOptions{MaxTokens: 128})
	require.NoError(t, err)
	require.Equal(t, "responses", response.Content)
	require.EqualValues(t, 2, withMax.Load(), "failed retry must not cache field unsupported")
	require.EqualValues(t, 1, withoutMax.Load())
	require.Zero(t, chatCalls.Load(), "failed field retry must not continue to alternate")
}

func TestRemoteAPIChatReprobesWhenCachedProtocolBecomesUnsupported(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			call := responsesCalls.Add(1)
			if call == 1 {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
		case "/v1/chat/completions":
			call := chatCalls.Add(1)
			if call > 1 {
				http.Error(w, "gone", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"chat"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
		}
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	first, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
	require.NoError(t, err)
	require.Equal(t, "chat", first.Content)
	second, err := model.Chat(context.Background(), []Message{{Role: "user", Content: "again"}}, &ChatOptions{})
	require.NoError(t, err)
	require.Equal(t, "responses", second.Content)
	require.EqualValues(t, 2, responsesCalls.Load())
	require.EqualValues(t, 2, chatCalls.Load())
}

func TestRemoteAPIChatResponsesStreamRequiresCompletedEvent(t *testing.T) {
	t.Run("completed may omit text already delivered by deltas", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/responses", r.URL.Path)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"delta-only answer\"}\n\n")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4}}}\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		var content string
		var terminal types.StreamResponse
		for event := range stream {
			if event.ResponseType == types.ResponseTypeAnswer {
				content += event.Content
			}
			if event.Done {
				terminal = event
			}
		}
		require.Equal(t, "delta-only answer", content)
		require.Equal(t, types.ResponseTypeAnswer, terminal.ResponseType)
		require.True(t, terminal.Done)
		require.Equal(t, "stop", terminal.FinishReason)
		require.NotNil(t, terminal.Usage)
		require.Equal(t, 4, terminal.Usage.TotalTokens)
	})

	t.Run("completed final body fills missing deltas", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/responses", r.URL.Path)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"final-only answer\"}]}]}}\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		var content string
		for event := range stream {
			if event.ResponseType == types.ResponseTypeAnswer {
				content += event.Content
			}
		}
		require.Equal(t, "final-only answer", content)
	})

	t.Run("completed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/responses", r.URL.Path)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, true, body["stream"])
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		var content string
		var final types.StreamResponse
		for event := range stream {
			if event.ResponseType == types.ResponseTypeAnswer {
				content += event.Content
			}
			if event.Done {
				final = event
			}
		}
		require.Equal(t, "hello", content)
		require.True(t, final.Done)
		require.Equal(t, "stop", final.FinishReason)
		require.NotNil(t, final.Usage)
		require.Equal(t, 3, final.Usage.TotalTokens)
		decision := openaiapi.ResolveProtocol(model.protocolCacheKey(server.URL + "/v1"))
		require.True(t, decision.Known)
		require.Equal(t, openaiapi.ProtocolResponses, decision.Protocol)
	})

	t.Run("interrupted", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		var sawError bool
		var sawSuccessfulDone bool
		for event := range stream {
			if event.ResponseType == types.ResponseTypeError {
				sawError = true
				require.Contains(t, event.Content, "before response.completed")
			}
			if event.ResponseType == types.ResponseTypeAnswer && event.Done {
				sawSuccessfulDone = true
			}
		}
		require.True(t, sawError)
		require.False(t, sawSuccessfulDone)
		decision := openaiapi.ResolveProtocol(model.protocolCacheKey(server.URL + "/v1"))
		require.False(t, decision.Known, "an interrupted stream must not cache protocol success")
	})

	t.Run("done sentinel without completed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		var sawError bool
		var sawSuccessfulDone bool
		for event := range stream {
			if event.ResponseType == types.ResponseTypeError {
				sawError = true
				require.Contains(t, event.Content, "ended before response.completed")
			}
			if event.ResponseType == types.ResponseTypeAnswer && event.Done {
				sawSuccessfulDone = true
			}
		}
		require.True(t, sawError)
		require.False(t, sawSuccessfulDone)
		decision := openaiapi.ResolveProtocol(model.protocolCacheKey(server.URL + "/v1"))
		require.False(t, decision.Known, "[DONE] without response.completed must not cache protocol success")
	})

	t.Run("response failed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"upstream failed\"}}}\n\n")
		}))
		defer server.Close()

		model := newNegotiatedTestChat(t, server.URL)
		stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
		require.NoError(t, err)
		event := <-stream
		require.Equal(t, types.ResponseTypeError, event.ResponseType)
		require.True(t, event.Done)
		require.Contains(t, event.Content, "upstream failed")
		decision := openaiapi.ResolveProtocol(model.protocolCacheKey(server.URL + "/v1"))
		require.False(t, decision.Known, "response.failed must not cache protocol success")
	})
}

func TestRemoteAPIChatResponsesStreamReconcilesTerminalOutput(t *testing.T) {
	for _, tc := range []struct {
		name            string
		events          string
		wantContent     string
		wantError       string
		wantToolCall    bool
		wantSuccessDone bool
	}{
		{
			name:            "completed final extends delta prefix",
			events:          "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello world\"}]}]}}\n\n",
			wantContent:     "hello world",
			wantSuccessDone: true,
		},
		{
			name:        "completed final conflicts with delta",
			events:      "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"goodbye\"}]}]}}\n\n",
			wantContent: "hello",
			wantError:   "does not match received deltas",
		},
		{
			name:            "valid tool only",
			events:          "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"wiki_read_page\",\"arguments\":\"{}\"}}\n\n" + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
			wantToolCall:    true,
			wantSuccessDone: true,
		},
		{
			name:      "invalid tool without call id",
			events:    "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"name\":\"wiki_read_page\",\"arguments\":\"{}\"}}\n\n" + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
			wantError: "without output text or function call",
		},
		{
			name:      "invalid tool item id cannot replace call id",
			events:    "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"id\":\"item_1\",\"name\":\"wiki_read_page\",\"arguments\":\"{}\"}}\n\n" + "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
			wantError: "without output text or function call",
		},
		{
			name:      "completed empty",
			events:    "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
			wantError: "without output text or function call",
		},
		{
			name:      "response incomplete",
			events:    "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\"}}\n\n",
			wantError: "status incomplete",
		},
		{
			name:      "error event",
			events:    "data: {\"type\":\"error\",\"error\":{\"message\":\"gateway exploded\",\"type\":\"upstream_error\"}}\n\n",
			wantError: "gateway exploded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.events)
			}))
			defer server.Close()

			model := newNegotiatedTestChat(t, server.URL)
			stream, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "hello"}}, &ChatOptions{})
			require.NoError(t, err)
			var content string
			var sawTool, sawSuccessDone bool
			var streamErr string
			for event := range stream {
				if event.ResponseType == types.ResponseTypeAnswer {
					content += event.Content
				}
				if len(event.ToolCalls) > 0 {
					sawTool = true
					require.NotEmpty(t, event.ToolCalls[0].ID)
					require.NotEmpty(t, event.ToolCalls[0].Function.Name)
				}
				if event.ResponseType == types.ResponseTypeError {
					streamErr = event.Content
				}
				if event.ResponseType == types.ResponseTypeAnswer && event.Done {
					sawSuccessDone = true
				}
			}
			require.Equal(t, tc.wantContent, content)
			require.Equal(t, tc.wantToolCall, sawTool)
			require.Equal(t, tc.wantSuccessDone, sawSuccessDone)
			if tc.wantError == "" {
				require.Empty(t, streamErr)
			} else {
				require.Contains(t, streamErr, tc.wantError)
			}
		})
	}
}

type responsesFailingReader struct {
	data []byte
	err  error
}

func (r *responsesFailingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestProcessResponsesStreamReportsReadInterruptionWithoutSuccess(t *testing.T) {
	wantErr := errors.New("http2: client connection lost")
	body := io.NopCloser(&responsesFailingReader{
		data: []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"),
		err:  wantErr,
	})
	stream := make(chan types.StreamResponse, 4)
	(&RemoteAPIChat{}).processResponsesStream(context.Background(), &http.Response{Body: body}, stream)

	var content, streamErr string
	var successDone bool
	for event := range stream {
		if event.ResponseType == types.ResponseTypeAnswer {
			content += event.Content
			if event.Done {
				successDone = true
			}
		}
		if event.ResponseType == types.ResponseTypeError {
			streamErr = event.Content
		}
	}
	require.Equal(t, "partial", content)
	require.Contains(t, streamErr, wantErr.Error())
	require.False(t, successDone)
}

func TestRemoteAPIChatUnknownStreamProbeLockReleasesAfterHeaders(t *testing.T) {
	var calls atomic.Int32
	firstOpened := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondOpened := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/responses", r.URL.Path)
		call := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		if call == 1 {
			close(firstOpened)
			<-releaseFirst
		} else {
			select {
			case <-secondOpened:
			default:
				close(secondOpened)
			}
		}
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}]}}\n\n")
	}))
	defer server.Close()

	model := newNegotiatedTestChat(t, server.URL)
	first, err := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "first"}}, &ChatOptions{})
	require.NoError(t, err)
	<-firstOpened

	secondResult := make(chan (<-chan types.StreamResponse), 1)
	secondErr := make(chan error, 1)
	go func() {
		stream, streamErr := model.ChatStream(context.Background(), []Message{{Role: "user", Content: "second"}}, &ChatOptions{})
		secondResult <- stream
		secondErr <- streamErr
	}()

	select {
	case <-secondOpened:
	case <-time.After(2 * time.Second):
		t.Fatal("second unknown-protocol request was blocked behind the first long stream")
	}
	second := <-secondResult
	require.NoError(t, <-secondErr)
	close(releaseFirst)
	for range first {
	}
	for range second {
	}
	require.EqualValues(t, 2, calls.Load())
}
