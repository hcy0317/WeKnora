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
	"github.com/stretchr/testify/require"
)

func TestWeKnoraCloudVLMPrefersResponsesAndMapsVisionRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/responses", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, false, body["store"])
		require.EqualValues(t, defaultMaxToks, body["max_output_tokens"])
		require.Equal(t, map[string]any{"effort": "high"}, body["reasoning"])
		input := body["input"].([]any)
		content := input[0].(map[string]any)["content"].([]any)
		require.Equal(t, "input_text", content[0].(map[string]any)["type"])
		require.Equal(t, "input_image", content[1].(map[string]any)["type"])

		w.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}`)
		require.NoError(t, err)
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName:       "gpt-5",
		appID:           "app",
		apiKey:          "secret",
		baseURL:         server.URL,
		client:          server.Client(),
		reasoningEffort: "high",
	}
	content, err := model.Predict(context.Background(), [][]byte{validOnePixelPNG(t)}, "describe")
	require.NoError(t, err)
	require.Equal(t, "ok", content)
}

func TestWeKnoraCloudVLMProtocolFingerprintTracksSavedConfiguration(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	newModel := func(extra map[string]any, appID, appSecret string) *WeKnoraCloudVLM {
		model, err := NewWeKnoraCloudVLM(&Config{
			BaseURL: server.URL, ModelName: "cloud-vlm", ModelID: "saved-cloud",
			InterfaceType: "openai", Provider: "weknora_cloud", Extra: extra,
			AppID: appID, AppSecret: appSecret,
		})
		require.NoError(t, err)
		return model
	}
	base := newModel(map[string]any{"remote_model_name": "vision-a", "reasoning_effort": "high"}, "app-one", "secret-one")
	baseKey := base.protocolCacheKey(base.protocolBaseURL())
	require.NotEqual(t, baseKey, newModel(map[string]any{"remote_model_name": "vision-b", "reasoning_effort": "high"}, "app-one", "secret-one").protocolCacheKey(base.protocolBaseURL()))
	require.NotEqual(t, baseKey, newModel(map[string]any{"remote_model_name": "vision-a", "reasoning_effort": "max"}, "app-one", "secret-one").protocolCacheKey(base.protocolBaseURL()))
	require.NotEqual(t, baseKey, newModel(map[string]any{"remote_model_name": "vision-a", "reasoning_effort": "high", "vendor_flag": "changed"}, "app-one", "secret-one").protocolCacheKey(base.protocolBaseURL()))
	require.NotEqual(t, baseKey, newModel(map[string]any{"remote_model_name": "vision-a", "reasoning_effort": "high"}, "app-two", "secret-one").protocolCacheKey(base.protocolBaseURL()))
	require.NotEqual(t, baseKey, newModel(map[string]any{"remote_model_name": "vision-a", "reasoning_effort": "high"}, "app-one", "secret-two").protocolCacheKey(base.protocolBaseURL()))
}

func TestWeKnoraCloudVLMSyncResponsesAcceptsSSEWithoutProtocolFallback(t *testing.T) {
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/chat/completions" {
			chatCalls.Add(1)
			return
		}
		require.Equal(t, "/api/v1/responses", r.URL.Path)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"cloud sse\"}\n\n"+
			"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer server.Close()
	model := &WeKnoraCloudVLM{modelName: "alias", appID: "app", apiKey: "secret", baseURL: server.URL, client: server.Client()}
	content, err := model.Predict(context.Background(), nil, "describe")
	require.NoError(t, err)
	require.Equal(t, "cloud sse", content)
	require.Zero(t, chatCalls.Load())
}

func TestWeKnoraCloudVLMFallsBackAndCachesOnlyEndpointUnsupported(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/responses":
			responsesCalls.Add(1)
			http.Error(w, "not found", http.StatusNotFound)
		case "/api/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, err := io.WriteString(w, `{"choices":[{"message":{"content":"chat"}}]}`)
			require.NoError(t, err)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "vision-compatible",
		appID:     "app",
		apiKey:    "secret",
		baseURL:   server.URL,
		client:    server.Client(),
	}
	for range 2 {
		content, err := model.Predict(context.Background(), nil, "describe")
		require.NoError(t, err)
		require.Equal(t, "chat", content)
	}
	require.EqualValues(t, 1, responsesCalls.Load())
	require.EqualValues(t, 2, chatCalls.Load())
}

func TestWeKnoraCloudVLMDoesNotReplayValidationErrors(t *testing.T) {
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/chat/completions" {
			chatCalls.Add(1)
		}
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "vision-compatible",
		appID:     "app",
		apiKey:    "secret",
		baseURL:   server.URL,
		client:    server.Client(),
	}
	_, err := model.Predict(context.Background(), nil, "describe")
	require.ErrorContains(t, err, "status 400")
	require.Zero(t, chatCalls.Load())
}

func TestWeKnoraCloudVLMSwitchesProtocolOnExplicitFormatError(t *testing.T) {
	var responsesCalls atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/responses":
			responsesCalls.Add(1)
			http.Error(w, "unsupported request format", http.StatusUnprocessableEntity)
		case "/api/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, err := io.WriteString(w, `{"choices":[{"message":{"content":"chat"}}]}`)
			require.NoError(t, err)
		}
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "vision-compatible",
		appID:     "app",
		apiKey:    "secret",
		baseURL:   server.URL,
		client:    server.Client(),
	}
	content, err := model.Predict(context.Background(), nil, "describe")
	require.NoError(t, err)
	require.Equal(t, "chat", content)
	require.EqualValues(t, 1, responsesCalls.Load())
	require.EqualValues(t, 1, chatCalls.Load())
}

func TestWeKnoraCloudVLMRetriesResponsesWithoutUnsupportedMaxOutputTokens(t *testing.T) {
	var responsesWithMax atomic.Int32
	var responsesWithoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/responses":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if _, ok := body["max_output_tokens"]; ok {
				responsesWithMax.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
				return
			}
			responsesWithoutMax.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"responses"}]}]}`)
		case "/api/v1/chat/completions":
			chatCalls.Add(1)
			http.Error(w, "unexpected chat fallback", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "vision-compatible",
		modelID:   "wkc-max-output-capability",
		appID:     "app",
		apiKey:    "secret",
		baseURL:   server.URL,
		client:    server.Client(),
	}
	for range 2 {
		content, err := model.Predict(context.Background(), nil, "describe")
		require.NoError(t, err)
		require.Equal(t, "responses", content)
	}
	require.EqualValues(t, 1, responsesWithMax.Load())
	require.EqualValues(t, 2, responsesWithoutMax.Load())
	require.Zero(t, chatCalls.Load())
}

func TestWeKnoraCloudVLMFailedFieldRetryDoesNotAlternateOrCache(t *testing.T) {
	var withMax atomic.Int32
	var withoutMax atomic.Int32
	var chatCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/responses":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if _, ok := body["max_output_tokens"]; ok {
				if withMax.Add(1) == 1 {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"detail":"Unsupported parameter: max_output_tokens"}`)
					return
				}
				_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"cloud vision"}]}]}`)
				return
			}
			withoutMax.Add(1)
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(w, `{"detail":"unsupported request format"}`)
		case "/api/v1/chat/completions":
			chatCalls.Add(1)
		}
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "vision-compatible", modelID: "wkc-failed-retry",
		appID: "app", apiKey: "secret", baseURL: server.URL, client: server.Client(),
	}
	_, err := model.Predict(context.Background(), nil, "first")
	require.ErrorContains(t, err, "unsupported request format")
	content, err := model.Predict(context.Background(), nil, "second")
	require.NoError(t, err)
	require.Equal(t, "cloud vision", content)
	require.EqualValues(t, 2, withMax.Load())
	require.EqualValues(t, 1, withoutMax.Load())
	require.Zero(t, chatCalls.Load())
}

func TestWeKnoraCloudVLMNegotiatesModernChatShapeWithoutModelNames(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/chat/completions", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		if calls.Add(1) == 1 {
			require.Contains(t, body, "max_tokens")
			require.NotContains(t, body, "max_completion_tokens")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(w, `{"detail":"temperature is not supported for this model"}`)
			return
		}
		require.Contains(t, body, "max_completion_tokens")
		require.NotContains(t, body, "max_tokens")
		require.NotContains(t, body, "temperature")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"cloud"}}]}`)
	}))
	defer server.Close()

	model := &WeKnoraCloudVLM{
		modelName: "private-cloud-alias", modelID: t.Name(), appID: "app", apiKey: "secret",
		baseURL: server.URL, client: server.Client(),
	}
	cacheKey := model.protocolCacheKey(model.protocolBaseURL())
	openaiapi.MarkProtocolSuccess(cacheKey, openaiapi.ProtocolChatCompletions)
	request := weKnoraCloudVLMRequest{
		Model: "private-cloud-alias", MaxTokens: 128, Temperature: 0.2,
		Messages: []weKnoraCloudVLMMessage{{Role: "user", Content: "describe"}},
	}
	for range 2 {
		content, err := model.predictWithNegotiatedProtocol(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, "cloud", content)
	}
	require.EqualValues(t, 3, calls.Load())
}
