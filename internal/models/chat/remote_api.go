package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/sashabaranov/go-openai"
)

// RemoteAPIChat 实现了基于 OpenAI 兼容 API 的聊天。
// 它本身只负责通用的请求/响应/流式处理；所有 provider 特定行为都委托给
// providerAdapter（见 provider.go），thinking 编码委托给 ThinkingStrategy
// （见 thinking.go）。
type RemoteAPIChat struct {
	modelName string
	client    *openai.Client
	modelID   string
	baseURL   string
	apiKey    string
	provider  provider.ProviderName
	appID     string
	appSecret string
	// customHeaders 为用户在模型配置中指定的自定义 HTTP 请求头（类似 OpenAI Python SDK 的 extra_headers）。
	customHeaders map[string]string

	// adapter 承载所有 provider 特定行为（thinking / 参数特判 / endpoint / 鉴权 / 消息变换）。
	adapter providerAdapter
	// thinkingOverride 来自 extra_config.thinking_control，非 nil 时覆盖 adapter.Thinking()。
	thinkingOverride ThinkingStrategy
	// reasoningEffort 来自 extra_config.reasoning_effort。OpenAI 兼容网关通常
	// 通过顶层 reasoning_effort 字段区分同一模型的推理预算（例如 medium/xhigh/max）。
	// go-openai 当前版本没有稳定暴露该字段，因此启用后统一走 raw HTTP 路径。
	reasoningEffort string
	// autoProtocol enables endpoint-based negotiation for every OpenAI-compatible
	// route whose actual outbound endpoint is /chat/completions. Azure keeps its
	// deployment-specific SDK URL layout.
	autoProtocol bool
	// sdkChatShape keeps deployment-specific transports (notably Azure) on the
	// SDK endpoint while still allowing a bounded request-field retry.
	sdkChatShape             bool
	configurationFingerprint string
}

const extraConfigReasoningEffort = "reasoning_effort"

// NewRemoteAPIChat 创建远程 API 聊天实例
func NewRemoteAPIChat(chatConfig *ChatConfig) (*RemoteAPIChat, error) {
	if chatConfig.BaseURL != "" {
		if err := secutils.ValidateURLForSSRF(chatConfig.BaseURL); err != nil {
			return nil, fmt.Errorf("baseURL SSRF check failed: %w", err)
		}
	}

	apiKey := chatConfig.APIKey
	providerName := provider.ProviderName(chatConfig.Provider)
	if providerName == "" {
		providerName = provider.DetectProvider(chatConfig.BaseURL)
	}

	var config openai.ClientConfig
	if providerName == provider.ProviderAzureOpenAI {
		config = openai.DefaultAzureConfig(apiKey, chatConfig.BaseURL)
		config.AzureModelMapperFunc = func(model string) string {
			return model
		}
		if chatConfig.ExtraConfig != nil {
			if v, ok := chatConfig.ExtraConfig["api_version"]; ok {
				config.APIVersion = v
			}
		}
	} else {
		config = openai.DefaultConfig(apiKey)
		if baseURL := chatConfig.BaseURL; baseURL != "" {
			config.BaseURL = baseURL
		} else if providerName == provider.ProviderDeepSeek {
			config.BaseURL = provider.DeepSeekBaseURL
		}
	}

	// The SDK must use the same SSRF-safe transport as the raw HTTP paths.
	// Constructor-time URL validation alone cannot prevent DNS rebinding or a
	// later redirect to an internal address.
	sdkHTTPClient := rawHTTPClient
	// 如果指定了 CustomHeaders，则给 SDK 使用的 HTTPClient 挂一层 RoundTripper，
	// 在每个请求上自动注入这些 header（raw HTTP 路径会在发送前单独处理）。
	if len(chatConfig.CustomHeaders) > 0 {
		sdkHTTPClient = secutils.WrapHTTPClientWithHeaders(sdkHTTPClient, chatConfig.CustomHeaders)
	}
	config.HTTPClient = sdkHTTPClient

	modelName := chatConfig.ModelName
	if chatConfig.ExtraConfig != nil {
		if override := strings.TrimSpace(chatConfig.ExtraConfig["remote_model_name"]); override != "" {
			modelName = override
		}
	}
	if providerName == provider.ProviderWeKnoraCloud {
		if chatConfig.AppID == "" {
			return nil, fmt.Errorf("WeKnoraCloud provider: AppID is required")
		}
		if chatConfig.AppSecret == "" {
			return nil, fmt.Errorf("WeKnoraCloud provider: AppSecret is required")
		}
	}

	return &RemoteAPIChat{
		modelName:        modelName,
		client:           openai.NewClientWithConfig(config),
		modelID:          chatConfig.ModelID,
		baseURL:          strings.TrimRight(config.BaseURL, "/"),
		apiKey:           apiKey,
		provider:         providerName,
		appID:            chatConfig.AppID,
		appSecret:        chatConfig.AppSecret,
		customHeaders:    chatConfig.CustomHeaders,
		adapter:          resolveProvider(providerName, modelName),
		thinkingOverride: parseThinkingOverride(chatConfig.ExtraConfig),
		reasoningEffort:  parseReasoningEffort(chatConfig.ExtraConfig),
		autoProtocol:     config.APIType == openai.APITypeOpenAI,
		sdkChatShape:     providerName == provider.ProviderAzureOpenAI,
		configurationFingerprint: openaiapi.SavedModelConfigFingerprint(openaiapi.SavedModelConfig{
			Provider: string(providerName), InterfaceType: "openai", APIVersion: config.APIVersion,
			ExtraConfig: chatConfig.ExtraConfig, Headers: chatConfig.CustomHeaders,
			Auth: map[string]string{"api_key": apiKey, "app_id": chatConfig.AppID, "app_secret": chatConfig.AppSecret},
		}),
	}, nil
}

func parseReasoningEffort(extraConfig map[string]string) string {
	if extraConfig == nil {
		return ""
	}
	return strings.TrimSpace(extraConfig[extraConfigReasoningEffort])
}

// injectReasoningEffort adds the OpenAI-compatible top-level reasoning_effort
// field without coupling WeKnora to a particular go-openai SDK version.
// JSON round-tripping also works for provider-specific wrapper structs and
// maps produced by the thinking adapters.
func injectReasoningEffort(body any, effort string) (any, error) {
	effort = strings.TrimSpace(effort)
	if effort == "" {
		return body, nil
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request for reasoning effort: %w", err)
	}
	request := make(map[string]any)
	if err := json.Unmarshal(encoded, &request); err != nil {
		return nil, fmt.Errorf("decode request for reasoning effort: %w", err)
	}
	request[extraConfigReasoningEffort] = effort
	return request, nil
}

// authCreds bundles the credentials passed to the adapter's Auth method.
func (c *RemoteAPIChat) authCreds() authCreds {
	return authCreds{APIKey: c.apiKey, AppID: c.appID, AppSecret: c.appSecret}
}

// shapedRequest builds the standard request and applies the adapter's message
// transform and parameter shaping (but not thinking, which may wrap the body).
func (c *RemoteAPIChat) shapedRequest(messages []Message, opts *ChatOptions, isStream bool) openai.ChatCompletionRequest {
	req := c.BuildChatCompletionRequest(messages, opts, isStream)
	req.Messages = c.adapter.TransformMessages(req.Messages)
	c.adapter.ShapeRequest(&req, opts, isStream)
	return req
}

// buildOutbound assembles the final outbound request: the body to send, the
// endpoint override (empty for the standard endpoint), and whether the raw HTTP
// path is required. This is the single place that composes adapter + thinking,
// replacing the former buildRequestCustomizer plumbing.
func (c *RemoteAPIChat) buildOutbound(
	messages []Message, opts *ChatOptions, isStream bool,
) (body any, endpoint string, useRawHTTP bool, err error) {
	req := c.shapedRequest(messages, opts, isStream)
	if c.sdkChatShape {
		req.ReasoningEffort = c.reasoningEffort
	}

	thinking := c.thinkingOverride
	if thinking == nil {
		thinking = c.adapter.Thinking()
	}
	customBody, useRaw := thinking.Apply(&req, opts, isStream)

	body = &req
	if customBody != nil {
		body = customBody
	}
	body, err = c.shapeProviderRequest(body, req, messages)
	if err != nil {
		return nil, "", false, err
	}
	if c.reasoningEffort != "" && !c.sdkChatShape {
		body, err = injectReasoningEffort(body, c.reasoningEffort)
		if err != nil {
			return nil, "", false, err
		}
		useRaw = true
	}
	endpoint = c.adapter.Endpoint(c.baseURL, c.modelID, isStream)
	useRawHTTP = useRaw || c.adapter.ForceRawHTTP() || endpoint != ""
	return body, endpoint, useRawHTTP, nil
}

// logRequest 记录请求日志
func (c *RemoteAPIChat) logRequest(ctx context.Context, req any, isStream bool) {
	if jsonData, err := json.MarshalIndent(req, "", "  "); err == nil {
		logger.Infof(ctx, "[LLM Request] model=%s, stream=%v, request:\n%s",
			c.modelName, isStream, secutils.CompactImageDataURLForLog(string(jsonData)))
	}
}

// Chat 进行非流式聊天
func (c *RemoteAPIChat) Chat(ctx context.Context, messages []Message, opts *ChatOptions) (*types.ChatResponse, error) {
	// 仅在调用方未设置 deadline 时附加一个兜底超时，防止 hung 请求永久阻塞 worker；
	// 调用方若显式设置了更短或更长的 deadline，都会被原样尊重。
	timeoutCtx, cancel := withLLMTimeout(ctx, defaultChatTimeout)
	defer cancel()

	body, endpoint, useRawHTTP, err := c.buildOutbound(messages, opts, false)
	if err != nil {
		return nil, err
	}
	if protocolBaseURL, ok := c.negotiatedProtocolBase(endpoint); ok {
		return c.chatWithNegotiatedProtocol(timeoutCtx, protocolBaseURL, body)
	}
	if c.sdkChatShape {
		req, ok := body.(*openai.ChatCompletionRequest)
		if !ok {
			return nil, fmt.Errorf("Azure Chat request requires SDK-compatible body, got %T", body)
		}
		return c.chatWithSDKRequestShape(timeoutCtx, *req)
	}
	if useRawHTTP {
		return c.chatWithRawHTTP(timeoutCtx, endpoint, body)
	}

	req := *(body.(*openai.ChatCompletionRequest))
	c.logRequest(timeoutCtx, req, false)
	resp, err := c.client.CreateChatCompletion(timeoutCtx, req)
	if err != nil {
		if isMultimodalNotSupportedError(err) {
			logger.Warnf(timeoutCtx, "[LLM Request] Model %s does not support multimodal, retrying without images", c.modelName)
			cleaned := stripImagesFromMessages(messages)
			req = c.shapedRequest(cleaned, opts, false)
			resp, err = c.client.CreateChatCompletion(timeoutCtx, req)
		}
		if err != nil {
			return nil, fmt.Errorf("create chat completion: %w", err)
		}
	}

	result, err := c.parseCompletionResponse(&resp)
	if err != nil {
		return nil, err
	}
	logUsage(timeoutCtx, c.modelName, &result.Usage)
	return result, nil
}

func (c *RemoteAPIChat) chatWithSDKRequestShape(
	ctx context.Context,
	request openai.ChatCompletionRequest,
) (*types.ChatResponse, error) {
	cacheKey := c.protocolCacheKey(c.baseURL)
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	response, status, err := c.chatWithSDKShape(ctx, request, shape)
	if err == nil {
		if shape == openaiapi.ChatRequestShapeMaxCompletionNeutral {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, shape)
		}
		return response, nil
	}
	if shape != openaiapi.ChatRequestShapeDefault ||
		!openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		return response, err
	}
	response, _, err = c.chatWithSDKShape(ctx, request, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	if err == nil {
		openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	}
	return response, err
}

func (c *RemoteAPIChat) chatWithSDKShape(
	ctx context.Context,
	request openai.ChatCompletionRequest,
	shape openaiapi.ChatRequestShape,
) (*types.ChatResponse, int, error) {
	body, err := openaiapi.BuildChatRequestWithShape(request, shape)
	if err != nil {
		return nil, 0, err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal shaped Azure Chat request: %w", err)
	}
	var shaped openai.ChatCompletionRequest
	if err := json.Unmarshal(encoded, &shaped); err != nil {
		return nil, 0, fmt.Errorf("decode shaped Azure Chat request: %w", err)
	}
	completion, err := c.client.CreateChatCompletion(ctx, shaped)
	if err != nil {
		return nil, openAIChatRequestStatus(err), err
	}
	result, err := c.parseCompletionResponse(&completion)
	if err != nil {
		return nil, http.StatusOK, err
	}
	logUsage(ctx, c.modelName, &result.Usage)
	return result, http.StatusOK, nil
}

func openAIChatRequestStatus(err error) int {
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

// chatWithRawHTTP 使用原始 HTTP 请求进行聊天（供自定义请求使用）
func (c *RemoteAPIChat) chatWithRawHTTP(ctx context.Context, endpoint string, customReq any) (*types.ChatResponse, error) {
	jsonData, err := json.Marshal(customReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	if endpoint == "" {
		endpoint = c.baseURL + "/chat/completions"
	}
	if err := secutils.ValidateURLForSSRF(endpoint); err != nil {
		return nil, fmt.Errorf("endpoint SSRF check failed: %w", err)
	}
	logger.Infof(ctx, "[LLM Request] Remote HTTP, endpoint=%s, model=%s, raw HTTP request:\n%s",
		endpoint, c.modelName, secutils.CompactImageDataURLForLog(string(jsonData)))

	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	c.adapter.Auth(httpReq, c.authCreds(), jsonData)

	// 注入用户自定义 header（保留头会在工具内部自动跳过）
	secutils.ApplyCustomHeaders(httpReq, c.customHeaders)

	logger.Infof(ctx, "[LLM Request] Remote HTTP, endpoint=%s, model=%s",
		endpoint, c.modelName)

	resp, err := rawHTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var chatResp openai.ChatCompletionResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	result, err := c.parseCompletionResponse(&chatResp)
	if err != nil {
		return nil, err
	}
	c.applyCompletionToolCallMetadata(body, result)
	applyRawPromptCacheUsage(body, &result.Usage)
	logUsage(ctx, c.modelName, &result.Usage)
	return result, nil
}

// ChatStream 进行流式聊天
func (c *RemoteAPIChat) ChatStream(ctx context.Context, messages []Message, opts *ChatOptions) (<-chan types.StreamResponse, error) {
	// 仅在调用方未设置 deadline 时附加兜底超时；流式调用默认超时更长，
	// 因为带思考/推理的模型可能数十秒甚至几分钟才产出首 token。
	timeoutCtx, cancel := withLLMTimeout(ctx, defaultStreamTimeout)

	body, endpoint, useRawHTTP, err := c.buildOutbound(messages, opts, true)
	if err != nil {
		cancel()
		return nil, err
	}
	if protocolBaseURL, ok := c.negotiatedProtocolBase(endpoint); ok {
		ch, err := c.chatStreamWithNegotiatedProtocol(timeoutCtx, protocolBaseURL, body)
		return wrapStreamCancel(timeoutCtx, ch, err, cancel)
	}
	if c.sdkChatShape {
		req, ok := body.(*openai.ChatCompletionRequest)
		if !ok {
			cancel()
			return nil, fmt.Errorf("Azure Chat stream request requires SDK-compatible body, got %T", body)
		}
		ch, err := c.chatStreamWithSDKRequestShape(timeoutCtx, *req)
		return wrapStreamCancel(timeoutCtx, ch, err, cancel)
	}
	if useRawHTTP {
		ch, err := c.chatStreamWithRawHTTP(timeoutCtx, endpoint, body)
		return wrapStreamCancel(timeoutCtx, ch, err, cancel)
	}

	req := *(body.(*openai.ChatCompletionRequest))
	c.logRequest(timeoutCtx, req, true)

	streamDumper := newStreamPacketDumper(c.modelName, &req)
	if streamDumper != nil {
		logger.Infof(timeoutCtx, "[LLM Stream Raw Dump] writing packets to %s", streamDumper.Path())
	}

	streamChan := make(chan types.StreamResponse)

	stream, err := c.client.CreateChatCompletionStream(timeoutCtx, req)
	if err != nil {
		if isMultimodalNotSupportedError(err) {
			logger.Warnf(timeoutCtx, "[LLM Stream] Model %s does not support multimodal, retrying without images", c.modelName)
			cleaned := stripImagesFromMessages(messages)
			req = c.shapedRequest(cleaned, opts, true)
			stream, err = c.client.CreateChatCompletionStream(timeoutCtx, req)
		}
		if err != nil {
			cancel()
			close(streamChan)
			return nil, fmt.Errorf("create chat completion stream: %w", err)
		}
	}

	go func() {
		defer cancel()
		if streamDumper != nil {
			defer streamDumper.Close()
		}
		c.processStream(timeoutCtx, stream, streamChan, streamDumper)
	}()

	return streamChan, nil
}

func (c *RemoteAPIChat) chatStreamWithSDKRequestShape(
	ctx context.Context,
	request openai.ChatCompletionRequest,
) (<-chan types.StreamResponse, error) {
	cacheKey := c.protocolCacheKey(c.baseURL)
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	stream, status, err := c.openSDKChatStream(ctx, request, shape)
	cacheAlternateOnSuccess := shape == openaiapi.ChatRequestShapeMaxCompletionNeutral
	if err != nil && shape == openaiapi.ChatRequestShapeDefault &&
		openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		stream, _, err = c.openSDKChatStream(ctx, request, openaiapi.ChatRequestShapeMaxCompletionNeutral)
		cacheAlternateOnSuccess = err == nil
	}
	if err != nil {
		return nil, err
	}
	providerChan := make(chan types.StreamResponse)
	go c.processStream(ctx, stream, providerChan, nil)
	out := make(chan types.StreamResponse)
	go forwardNegotiatedStream(ctx, providerChan, out, func() {
		if cacheAlternateOnSuccess {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
		}
	}, func() {})
	return out, nil
}

func (c *RemoteAPIChat) openSDKChatStream(
	ctx context.Context,
	request openai.ChatCompletionRequest,
	shape openaiapi.ChatRequestShape,
) (*openai.ChatCompletionStream, int, error) {
	body, err := openaiapi.BuildChatRequestWithShape(request, shape)
	if err != nil {
		return nil, 0, err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal shaped Azure Chat stream request: %w", err)
	}
	var shaped openai.ChatCompletionRequest
	if err := json.Unmarshal(encoded, &shaped); err != nil {
		return nil, 0, fmt.Errorf("decode shaped Azure Chat stream request: %w", err)
	}
	stream, err := c.client.CreateChatCompletionStream(ctx, shaped)
	if err != nil {
		return nil, openAIChatRequestStatus(err), err
	}
	return stream, http.StatusOK, nil
}

// wrapStreamCancel 在子 channel 关闭后执行 cancel，避免 timeout context 泄漏。
// 当底层调用直接返回 error 时，立即调用 cancel 并将 error 透出。
func wrapStreamCancel(
	ctx context.Context,
	in <-chan types.StreamResponse,
	err error,
	cancel context.CancelFunc,
) (<-chan types.StreamResponse, error) {
	if err != nil {
		cancel()
		return nil, err
	}
	out := make(chan types.StreamResponse)
	go func() {
		defer cancel()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				if !sendStreamResponse(ctx, out, v) {
					return
				}
			}
		}
	}()
	return out, nil
}

// chatStreamWithRawHTTP 使用原始 HTTP 请求进行流式聊天
func (c *RemoteAPIChat) chatStreamWithRawHTTP(ctx context.Context, endpoint string, customReq any) (<-chan types.StreamResponse, error) {
	jsonData, err := json.Marshal(customReq)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	if endpoint == "" {
		endpoint = c.baseURL + "/chat/completions"
	}
	if err := secutils.ValidateURLForSSRF(endpoint); err != nil {
		return nil, fmt.Errorf("endpoint SSRF check failed: %w", err)
	}

	if prettyJSON, pErr := json.MarshalIndent(customReq, "", "  "); pErr == nil {
		logger.Infof(ctx, "[LLM Stream Request] endpoint=%s, model=%s, stream=true, request:\n%s",
			endpoint, c.modelName, secutils.CompactImageDataURLForLog(string(prettyJSON)))
	} else {
		logger.Infof(ctx, "[LLM Stream] endpoint=%s, model=%s", endpoint, c.modelName)
	}
	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	c.adapter.Auth(httpReq, c.authCreds(), jsonData)
	httpReq.Header.Set("Accept", "text/event-stream")

	// 注入用户自定义 header（保留头会在工具内部自动跳过）
	secutils.ApplyCustomHeaders(httpReq, c.customHeaders)

	resp, err := rawHTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	streamChan := make(chan types.StreamResponse)
	streamDumper := newStreamPacketDumper(c.modelName, customReq)
	if streamDumper != nil {
		logger.Infof(ctx, "[LLM Stream Raw Dump] writing packets to %s", streamDumper.Path())
	}

	go func() {
		if streamDumper != nil {
			defer streamDumper.Close()
		}
		c.processRawHTTPStream(ctx, resp, streamChan, streamDumper)
	}()

	return streamChan, nil
}

// GetModelName 获取模型名称
func (c *RemoteAPIChat) GetModelName() string {
	return c.modelName
}

// GetModelID 获取模型ID
func (c *RemoteAPIChat) GetModelID() string {
	return c.modelID
}

// GetProvider 获取 provider 名称
func (c *RemoteAPIChat) GetProvider() provider.ProviderName {
	return c.provider
}

// GetBaseURL 获取 baseURL
func (c *RemoteAPIChat) GetBaseURL() string {
	return c.baseURL
}

// GetAPIKey 获取 apiKey
func (c *RemoteAPIChat) GetAPIKey() string {
	return c.apiKey
}
