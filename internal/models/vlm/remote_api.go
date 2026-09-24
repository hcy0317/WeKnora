package vlm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	modelapi "github.com/Tencent/WeKnora/internal/models/api"
	modelchat "github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/models/provider"
	modelruntime "github.com/Tencent/WeKnora/internal/models/runtime"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	openai "github.com/sashabaranov/go-openai"
)

const (
	// defaultTimeout is the fallback HTTP timeout for a single VLM request.
	// Dense scanned-PDF OCR (full-page text + layout extraction) can take well
	// over a minute on slow endpoints, so this is intentionally generous and
	// can be raised further via VLM_HTTP_TIMEOUT_SECONDS.
	defaultTimeout = 180 * time.Second
	defaultMaxToks = 5000
	defaultTemp    = float32(0.1)
)

// vlmHTTPTimeout returns the HTTP client timeout for VLM requests, read from
// the VLM_HTTP_TIMEOUT_SECONDS env var when set (and positive), falling back to
// defaultTimeout otherwise. Shared by all OpenAI-compatible VLM backends.
func vlmHTTPTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("VLM_HTTP_TIMEOUT_SECONDS")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultTimeout
}

// RemoteAPIVLM implements VLM via an OpenAI-compatible chat completions API.
type RemoteAPIVLM struct {
	modelName                string
	modelID                  string
	client                   *openai.Client
	baseURL                  string
	apiKey                   string
	customHeaders            map[string]string
	httpClient               *http.Client
	autoProtocol             bool
	temperature              float32
	reasoningEffort          string
	configurationFingerprint string
	catalogChat              modelchat.Chat
	catalogTemperature       float64
}

// NewRemoteAPIVLM creates a remote-API backed VLM instance.
func NewRemoteAPIVLM(config *Config) (*RemoteAPIVLM, error) {
	if shouldUseCatalogVLM(config) {
		return newCatalogRemoteAPIVLM(config)
	}
	if err := validateVLMBaseURL(config.BaseURL); err != nil {
		return nil, err
	}

	providerName := provider.ProviderName(config.Provider)
	if providerName == "" {
		providerName = provider.DetectProvider(config.BaseURL)
	}

	var apiCfg openai.ClientConfig
	if providerName == provider.ProviderAzureOpenAI {
		apiCfg = openai.DefaultAzureConfig(config.APIKey, config.BaseURL)
		apiCfg.AzureModelMapperFunc = func(model string) string {
			return model
		}
		if config.Extra != nil {
			if v, ok := config.Extra["api_version"]; ok {
				if vs, ok := v.(string); ok && vs != "" {
					apiCfg.APIVersion = vs
				}
			}
		}
	} else {
		apiCfg = openai.DefaultConfig(config.APIKey)
		if config.BaseURL != "" {
			apiCfg.BaseURL = config.BaseURL
		}
	}
	httpClient := newVLMHTTPClient(vlmHTTPTimeout())
	requestClient := httpClient

	// 注入用户自定义 HTTP header（类似 OpenAI Python SDK 的 extra_headers）
	if len(config.CustomHeaders) > 0 {
		requestClient = secutils.WrapHTTPClientWithHeaders(httpClient, config.CustomHeaders)
	}
	apiCfg.HTTPClient = requestClient

	temp := defaultTemp
	reasoningEffort := ""
	if config.Extra != nil {
		if v, ok := config.Extra["temperature"]; ok {
			if vs, ok := v.(string); ok {
				if f, err := strconv.ParseFloat(vs, 32); err == nil {
					temp = float32(f)
				}
			}
		}
		if v, ok := config.Extra["reasoning_effort"].(string); ok {
			reasoningEffort = strings.TrimSpace(v)
		}
	}

	return &RemoteAPIVLM{
		modelName:       config.ModelName,
		modelID:         config.ModelID,
		client:          openai.NewClientWithConfig(apiCfg),
		baseURL:         strings.TrimRight(apiCfg.BaseURL, "/"),
		apiKey:          config.APIKey,
		customHeaders:   config.CustomHeaders,
		httpClient:      requestClient,
		autoProtocol:    apiCfg.APIType == openai.APITypeOpenAI && providerName != provider.ProviderWeKnoraCloud,
		temperature:     temp,
		reasoningEffort: reasoningEffort,
		configurationFingerprint: openaiapi.SavedModelConfigFingerprint(openaiapi.SavedModelConfig{
			Provider: string(providerName), InterfaceType: normalizedVLMInterface(config.InterfaceType),
			APIVersion: apiCfg.APIVersion, ExtraConfig: config.Extra, Headers: config.CustomHeaders,
			Auth: map[string]string{"api_key": config.APIKey, "app_id": config.AppID, "app_secret": config.AppSecret},
		}),
	}, nil
}

func effectiveVLMModelSpec(config *Config) *types.ModelSpecOverride {
	if config.Spec != nil {
		return config.Spec
	}
	if raw, ok := config.Extra["api"].(string); ok && strings.TrimSpace(raw) != "" {
		return &types.ModelSpecOverride{API: strings.TrimSpace(raw)}
	}
	switch normalizedVLMInterface(config.InterfaceType) {
	case "openai-responses", "responses":
		return &types.ModelSpecOverride{API: string(modelapi.APIOpenAIResponses)}
	case "openai-completions", "chat-completions":
		return &types.ModelSpecOverride{API: string(modelapi.APIOpenAICompletions)}
	default:
		return nil
	}
}

func shouldUseCatalogVLM(config *Config) bool {
	if config == nil || effectiveVLMModelSpec(config) != nil {
		return config != nil
	}
	extra := vlmExtraConfig(config)
	resolved, err := modelruntime.Resolve(modelruntime.Ref{
		Provider:  config.Provider,
		Model:     config.ModelName,
		BaseURL:   config.BaseURL,
		ModelType: types.ModelTypeVLLM,
		Extra:     extra,
	})
	if err != nil {
		return false
	}
	if resolved.Vendor.ID == string(provider.ProviderAzureOpenAI) && !resolved.Cataloged {
		// Saved Azure deployments are often user-chosen names. Preserve the
		// legacy Azure mapper and api_version behavior for those unlisted rows.
		return false
	}
	if resolved.Vendor.ID == string(provider.ProviderOpenAI) ||
		resolved.Vendor.ID == string(provider.ProviderGeneric) {
		if resolved.API != modelapi.APIOpenAICompletions {
			return true
		}
		if resolved.Cataloged && len(resolved.Capabilities().ThinkingLevels) > 0 {
			return true
		}
		// The first-party endpoint's default is Responses even for models that
		// do not advertise reasoning.
		if strings.EqualFold(strings.TrimRight(config.BaseURL, "/"), "https://api.openai.com/v1") {
			return true
		}
		// A custom OpenAI-compatible endpoint may expose Responses, Chat
		// Completions, or both. Keep the local protocol probe for those rows
		// unless a per-row Spec explicitly selected an API above.
		return false
	}
	return true
}

func vlmExtraConfig(config *Config) map[string]string {
	extra := make(map[string]string, len(config.Extra))
	for key, value := range config.Extra {
		if text, ok := value.(string); ok {
			extra[key] = text
		}
	}
	return extra
}

func newCatalogRemoteAPIVLM(config *Config) (*RemoteAPIVLM, error) {
	if err := validateVLMBaseURL(config.BaseURL); err != nil {
		return nil, err
	}
	extra := vlmExtraConfig(config)
	temperature := float64(defaultTemp)
	if raw, ok := extra["temperature"]; ok {
		if value, err := strconv.ParseFloat(raw, 64); err == nil {
			temperature = value
		}
	}
	client, err := modelchat.NewRemoteChat(&modelchat.ChatConfig{
		Source:        types.ModelSourceRemote,
		BaseURL:       config.BaseURL,
		ModelName:     config.ModelName,
		APIKey:        config.APIKey,
		ModelID:       config.ModelID,
		Provider:      config.Provider,
		ExtraConfig:   extra,
		CustomHeaders: config.CustomHeaders,
		AppID:         config.AppID,
		AppSecret:     config.AppSecret,
		Spec:          effectiveVLMModelSpec(config),
	})
	if err != nil {
		return nil, err
	}
	return &RemoteAPIVLM{
		modelName:       config.ModelName,
		modelID:         config.ModelID,
		baseURL:         config.BaseURL,
		reasoningEffort: strings.TrimSpace(extra["reasoning_effort"]),
		configurationFingerprint: openaiapi.SavedModelConfigFingerprint(openaiapi.SavedModelConfig{
			Provider: config.Provider, InterfaceType: normalizedVLMInterface(config.InterfaceType),
			ExtraConfig: config.Extra, Headers: config.CustomHeaders,
			Auth: map[string]string{"api_key": config.APIKey, "app_id": config.AppID, "app_secret": config.AppSecret},
		}),
		catalogChat:        client,
		catalogTemperature: temperature,
	}, nil
}

func normalizedVLMInterface(interfaceType string) string {
	if normalized := strings.ToLower(strings.TrimSpace(interfaceType)); normalized != "" {
		return normalized
	}
	return "openai"
}

// Predict sends an image with a text prompt to the OpenAI-compatible API.
func (v *RemoteAPIVLM) predictWithCatalogChat(ctx context.Context, imgBytesList [][]byte, prompt string) (string, error) {
	parts := []modelchat.MessageContentPart{{Type: "text", Text: prompt}}
	totalImageSize := 0
	for i, image := range imgBytesList {
		if len(image) == 0 {
			continue
		}
		mimeType, err := detectImageMIME(image)
		if err != nil {
			return "", fmt.Errorf("VLM image %d: %w", i, err)
		}
		totalImageSize += len(image)
		dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64.StdEncoding.EncodeToString(image))
		parts = append(parts, modelchat.MessageContentPart{
			Type: "image_url", ImageURL: &modelchat.ImageURL{URL: dataURI, Detail: "auto"},
		})
	}
	logger.Infof(ctx, "[VLM] Calling catalog chat protocol, model=%s, numImages=%d, totalImageSize=%d",
		v.modelName, len(imgBytesList), totalImageSize)

	requestCtx, cancel := context.WithTimeout(ctx, vlmHTTPTimeout())
	defer cancel()
	response, err := v.catalogChat.Chat(requestCtx, []modelchat.Message{{Role: "user", MultiContent: parts}}, &modelchat.ChatOptions{
		Temperature: v.catalogTemperature,
		MaxTokens:   defaultMaxToks,
	})
	if err != nil {
		return "", fmt.Errorf("VLM request: %w", err)
	}
	if strings.TrimSpace(response.Content) == "" && response.FinishReason == "length" {
		return "", fmt.Errorf(
			"VLM returned no content: completion truncated at %d tokens (finish_reason=length)",
			defaultMaxToks,
		)
	}
	logger.Infof(ctx, "[VLM] response received, len=%d", len(response.Content))
	return response.Content, nil
}

func (v *RemoteAPIVLM) Predict(ctx context.Context, imgBytesList [][]byte, prompt string) (string, error) {
	if v.catalogChat != nil {
		return v.predictWithCatalogChat(ctx, imgBytesList, prompt)
	}
	var parts []openai.ChatMessagePart

	// Add text prompt first
	parts = append(parts, openai.ChatMessagePart{
		Type: openai.ChatMessagePartTypeText,
		Text: prompt,
	})

	// Add images
	for i, imgBytes := range imgBytesList {
		if len(imgBytes) > 0 {
			mimeType, err := detectImageMIME(imgBytes)
			if err != nil {
				return "", fmt.Errorf("OpenAI VLM image %d: %w", i, err)
			}
			b64 := base64.StdEncoding.EncodeToString(imgBytes)
			dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, b64)
			parts = append(parts, openai.ChatMessagePart{
				Type: openai.ChatMessagePartTypeImageURL,
				ImageURL: &openai.ChatMessageImageURL{
					URL:    dataURI,
					Detail: openai.ImageURLDetailAuto,
				},
			})
		}
	}

	req := openai.ChatCompletionRequest{
		Model:           v.modelName,
		ReasoningEffort: v.reasoningEffort,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:         openai.ChatMessageRoleUser,
				MultiContent: parts,
			},
		},
		MaxTokens:   defaultMaxToks,
		Temperature: v.temperature,
	}

	totalImageSize := 0
	for _, img := range imgBytesList {
		totalImageSize += len(img)
	}
	logger.Infof(ctx, "[VLM] Calling OpenAI-compatible API, model=%s, baseURL=%s, numImages=%d, totalImageSize=%d",
		v.modelName, v.baseURL, len(imgBytesList), totalImageSize)
	if v.autoProtocol {
		return v.predictWithNegotiatedProtocol(ctx, req)
	}
	content, err := v.predictWithSDKChatRequestShape(ctx, req)
	if err != nil {
		return "", fmt.Errorf("OpenAI VLM request: %w", err)
	}
	logger.Infof(ctx, "[VLM] OpenAI response received, len=%d", len(content))
	return content, nil
}

func (v *RemoteAPIVLM) predictWithSDKChatRequestShape(
	ctx context.Context,
	request openai.ChatCompletionRequest,
) (string, error) {
	cacheKey := v.protocolCacheKey()
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	content, status, err := v.predictWithSDKChatProtocol(ctx, request, shape)
	if err == nil {
		if shape == openaiapi.ChatRequestShapeMaxCompletionNeutral {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, shape)
		}
		return content, nil
	}
	if shape != openaiapi.ChatRequestShapeDefault ||
		!openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		return content, err
	}
	content, _, err = v.predictWithSDKChatProtocol(
		ctx, request, openaiapi.ChatRequestShapeMaxCompletionNeutral,
	)
	if err == nil {
		openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	}
	return content, err
}

func (v *RemoteAPIVLM) predictWithSDKChatProtocol(
	ctx context.Context,
	request openai.ChatCompletionRequest,
	shape openaiapi.ChatRequestShape,
) (string, int, error) {
	body, err := openaiapi.BuildChatRequestWithShape(request, shape)
	if err != nil {
		return "", 0, err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return "", 0, fmt.Errorf("marshal shaped OpenAI VLM request: %w", err)
	}
	var shaped openai.ChatCompletionRequest
	if err := json.Unmarshal(encoded, &shaped); err != nil {
		return "", 0, fmt.Errorf("decode shaped OpenAI VLM request: %w", err)
	}
	response, err := v.client.CreateChatCompletion(ctx, shaped)
	if err != nil {
		return "", openAIRequestStatus(err), err
	}
	content, err := vlmChatCompletionContent(&response)
	return content, http.StatusOK, err
}

func openAIRequestStatus(err error) int {
	var apiErr *openai.APIError
	if errors.As(err, &apiErr) {
		return apiErr.HTTPStatusCode
	}
	var requestErr *openai.RequestError
	if errors.As(err, &requestErr) {
		return requestErr.HTTPStatusCode
	}
	return 0
}

func vlmChatCompletionContent(resp *openai.ChatCompletionResponse) (string, error) {
	if resp == nil || len(resp.Choices) == 0 {
		return "", fmt.Errorf("OpenAI VLM returned no choices")
	}
	choice := resp.Choices[0]
	content := choice.Message.Content
	if strings.TrimSpace(content) == "" && choice.FinishReason == openai.FinishReasonLength {
		return "", fmt.Errorf(
			"OpenAI VLM returned no content: completion truncated at %d tokens (finish_reason=length)",
			defaultMaxToks,
		)
	}
	return content, nil
}

func (v *RemoteAPIVLM) GetModelName() string { return v.modelName }
func (v *RemoteAPIVLM) GetModelID() string   { return v.modelID }

// detectImageMIME returns an API-supported MIME type for actual image bytes.
// Unknown formats must never be relabelled as PNG: the data URI MIME and the
// encoded payload have to describe the same format or every OpenAI-compatible
// endpoint will reject the request as an invalid image.
func detectImageMIME(data []byte) (string, error) {
	ct := http.DetectContentType(data)
	switch ct {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return ct, nil
	}
	// Go versions before WebP sniffing support report a valid WebP payload as
	// application/octet-stream. Recognize its RIFF container explicitly.
	if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return "image/webp", nil
	}
	return "", fmt.Errorf(
		"unsupported or invalid image data (%s); expected JPEG, PNG, GIF, or WebP",
		ct,
	)
}
