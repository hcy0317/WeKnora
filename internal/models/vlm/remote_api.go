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
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/models/provider"
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
}

// NewRemoteAPIVLM creates a remote-API backed VLM instance.
func NewRemoteAPIVLM(config *Config) (*RemoteAPIVLM, error) {
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

func normalizedVLMInterface(interfaceType string) string {
	if normalized := strings.ToLower(strings.TrimSpace(interfaceType)); normalized != "" {
		return normalized
	}
	return "openai"
}

// Predict sends an image with a text prompt to the OpenAI-compatible API.
func (v *RemoteAPIVLM) Predict(ctx context.Context, imgBytesList [][]byte, prompt string) (string, error) {
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
