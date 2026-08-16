package vlm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	openai "github.com/sashabaranov/go-openai"
	"github.com/stretchr/testify/require"
)

func newSDKTestRemoteAPIVLM(t *testing.T, server *httptest.Server, modelID string) *RemoteAPIVLM {
	t.Helper()
	config := openai.DefaultConfig("test-key")
	config.BaseURL = server.URL + "/v1"
	config.HTTPClient = server.Client()
	return &RemoteAPIVLM{
		modelName: "unclassified-model", modelID: modelID, client: openai.NewClientWithConfig(config),
		baseURL: config.BaseURL, httpClient: server.Client(), temperature: defaultTemp,
	}
}

func TestRemoteAPIVLMChatShapeNegotiatesByResponseNotModelName(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	for _, modelName := range []string{"private-reasoning-alias", "qwen-vl-custom"} {
		t.Run(modelName, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/v1/chat/completions", r.URL.Path)
				var body map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				switch calls.Add(1) {
				case 1:
					require.EqualValues(t, defaultMaxToks, body["max_tokens"])
					require.InDelta(t, defaultTemp, body["temperature"], 1e-6)
					require.NotContains(t, body, "max_completion_tokens")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"message":"Unsupported parameter: max_tokens; use max_completion_tokens"}}`)
				default:
					require.EqualValues(t, defaultMaxToks, body["max_completion_tokens"])
					require.NotContains(t, body, "max_tokens")
					require.NotContains(t, body, "temperature")
					_, _ = io.WriteString(w, `{"object":"chat.completion","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
				}
			}))
			defer server.Close()

			model := newSDKTestRemoteAPIVLM(t, server, t.Name())
			model.modelName = modelName
			for range 2 {
				content, err := model.Predict(context.Background(), nil, "describe")
				require.NoError(t, err)
				require.Equal(t, "ok", content)
			}
			require.EqualValues(t, 3, calls.Load(), "alternate success must be cached")
		})
	}
}

func TestRemoteAPIVLMChatShapeDoesNotRetryOpaqueOrTransientFailures(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "opaque", status: http.StatusBadRequest, body: `{"error":{"type":"upstream_error"}}`},
		{name: "transient", status: http.StatusServiceUnavailable, body: `{"detail":"Unsupported parameter: max_tokens"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			model := newSDKTestRemoteAPIVLM(t, server, t.Name())
			_, err := model.Predict(context.Background(), nil, "describe")
			require.Error(t, err)
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestRemoteAPIVLMAzurePreservesSDKEndpointAuthAndNegotiatesChatShape(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/openai/deployments/azure-vision/chat/completions", r.URL.Path)
		require.Equal(t, "2026-07-01-preview", r.URL.Query().Get("api-version"))
		require.Equal(t, "azure-test-key", r.Header.Get("api-key"))
		require.Empty(t, r.Header.Get("Authorization"))

		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		if calls.Add(1) == 1 {
			require.EqualValues(t, defaultMaxToks, body["max_tokens"])
			require.NotContains(t, body, "max_completion_tokens")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"Unsupported parameter: max_tokens; use max_completion_tokens","type":"invalid_request_error"}}`)
			return
		}
		require.EqualValues(t, defaultMaxToks, body["max_completion_tokens"])
		require.NotContains(t, body, "max_tokens")
		require.NotContains(t, body, "temperature")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"azure ok"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()

	model, err := NewRemoteAPIVLM(&Config{
		BaseURL:   server.URL,
		APIKey:    "azure-test-key",
		ModelName: "azure-vision",
		ModelID:   t.Name(),
		Provider:  "azure_openai",
		Extra:     map[string]any{"api_version": "2026-07-01-preview"},
	})
	require.NoError(t, err)
	for range 2 {
		content, err := model.Predict(context.Background(), nil, "describe")
		require.NoError(t, err)
		require.Equal(t, "azure ok", content)
	}
	require.EqualValues(t, 3, calls.Load(), "successful alternate SDK shape must be cached")
}

func TestRemoteAPIVLMProtocolFingerprintTracksFullSavedConfiguration(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	newModel := func(extra map[string]any, headers map[string]string, apiKey string) *RemoteAPIVLM {
		model, err := NewRemoteAPIVLM(&Config{
			BaseURL: server.URL, APIKey: apiKey, ModelName: "azure-vision", ModelID: "saved-vlm",
			InterfaceType: "openai", Provider: "azure_openai", Extra: extra, CustomHeaders: headers,
		})
		require.NoError(t, err)
		return model
	}
	base := newModel(map[string]any{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-one")
	baseKey := base.protocolCacheKey()
	require.NotEqual(t, baseKey, newModel(map[string]any{"api_version": "2027-01-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-one").protocolCacheKey())
	require.NotEqual(t, baseKey, newModel(map[string]any{"api_version": "2026-07-01", "vendor_flag": "b"}, map[string]string{"X-Tenant": "one"}, "key-one").protocolCacheKey())
	require.NotEqual(t, baseKey, newModel(map[string]any{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "two"}, "key-one").protocolCacheKey())
	require.NotEqual(t, baseKey, newModel(map[string]any{"api_version": "2026-07-01", "vendor_flag": "a"}, map[string]string{"X-Tenant": "one"}, "key-two").protocolCacheKey())
}

func TestRemoteAPIVLMPredictUsesResponsesVisionAndReasoningShape(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/responses", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"vision ok"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`)
	}))
	defer server.Close()

	model, err := NewRemoteAPIVLM(&Config{
		BaseURL:   server.URL + "/v1",
		APIKey:    "test-key",
		ModelName: "reasoning-vision-model",
		Provider:  "openai",
		Extra:     map[string]any{"reasoning_effort": "high"},
	})
	require.NoError(t, err)
	content, err := model.Predict(context.Background(), [][]byte{validOnePixelPNG(t)}, "describe")
	require.NoError(t, err)
	require.Equal(t, "vision ok", content)
	require.Equal(t, false, captured["store"])
	require.EqualValues(t, defaultMaxToks, captured["max_output_tokens"])
	require.Equal(t, map[string]any{"effort": "high"}, captured["reasoning"])
	require.NotContains(t, captured, "max_tokens")
	require.NotContains(t, captured, "max_completion_tokens")

	input := captured["input"].([]any)
	contentParts := input[0].(map[string]any)["content"].([]any)
	require.Equal(t, "input_text", contentParts[0].(map[string]any)["type"])
	require.Equal(t, "input_image", contentParts[1].(map[string]any)["type"])
	require.Contains(t, contentParts[1].(map[string]any)["image_url"], "data:image/png;base64,")
}

func TestNewRemoteAPIVLMReadsReasoningEffortFromExtra(t *testing.T) {
	model, err := NewRemoteAPIVLM(&Config{
		BaseURL:   "https://api.openai.com/v1",
		APIKey:    "test-key",
		ModelName: "gpt-5.6-luna",
		Provider:  "openai",
		Extra:     map[string]any{"reasoning_effort": " xhigh "},
	})
	require.NoError(t, err)
	require.Equal(t, "xhigh", model.reasoningEffort)
}

func TestRemoteAPIVLMAutoNegotiationRecognizesChatResponseShape(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(w, `{"id":"vlm-test","object":"chat.completion","created":0,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"chat ok"},"finish_reason":"stop"}]}`)
		require.NoError(t, err)
	}))
	defer server.Close()

	model, err := NewRemoteAPIVLM(&Config{
		BaseURL:   server.URL + "/v1",
		APIKey:    "test-key",
		ModelName: "gpt-4o",
		Provider:  "openai",
	})
	require.NoError(t, err)

	content, err := model.Predict(context.Background(), nil, "describe")
	require.NoError(t, err)
	require.Equal(t, "chat ok", content)
	require.Equal(t, []string{"/v1/responses", "/v1/chat/completions"}, paths)
}
