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
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	openai "github.com/sashabaranov/go-openai"
)

func (c *RemoteAPIChat) chatWithNegotiatedProtocol(
	ctx context.Context,
	baseURL string,
	chatBody any,
) (*types.ChatResponse, error) {
	cacheKey := c.protocolCacheKey(baseURL)
	decision := openaiapi.ResolveProtocol(cacheKey)
	if !decision.Known {
		unlock, acquireErr := openaiapi.AcquireProtocolProbe(ctx, cacheKey)
		if acquireErr != nil {
			return nil, acquireErr
		}
		defer unlock()
		decision = openaiapi.ResolveProtocol(cacheKey)
	}
	protocol := decision.Protocol
	omitMaxOutputTokens := false
	releaseFieldProbe := func() {}
	if protocol == openaiapi.ProtocolResponses {
		var acquireErr error
		omitMaxOutputTokens, releaseFieldProbe, acquireErr = openaiapi.BeginResponsesSyncMaxOutputProbe(ctx, cacheKey)
		if acquireErr != nil {
			return nil, acquireErr
		}
	}
	fieldProbeReleased := false
	releaseResponsesFieldProbe := func() {
		if !fieldProbeReleased {
			releaseFieldProbe()
			fieldProbeReleased = true
		}
	}
	defer releaseResponsesFieldProbe()
	var result *types.ChatResponse
	var status int
	var hasMaxOutputTokens bool
	var chatShapeRetryAttempted bool
	var err error
	if protocol == openaiapi.ProtocolChatCompletions {
		result, status, chatShapeRetryAttempted, err = c.chatWithChatRequestShape(ctx, baseURL, chatBody, cacheKey)
	} else {
		result, status, hasMaxOutputTokens, err = c.chatWithProtocol(
			ctx, baseURL, chatBody, protocol, omitMaxOutputTokens, openaiapi.ChatRequestShapeDefault,
		)
	}
	fieldRetryAttempted := err != nil && protocol == openaiapi.ProtocolResponses && hasMaxOutputTokens &&
		openaiapi.ShouldRetryResponsesWithoutMaxOutputTokens(status, err)
	fieldRetryExplicit := fieldRetryAttempted && openaiapi.IsMaxOutputTokensUnsupported(status, err)
	var fieldRejectionEpoch uint64
	if fieldRetryAttempted {
		fieldRejectionEpoch = openaiapi.MarkResponsesSyncMaxOutputTokensRejectedPending(cacheKey)
		result, status, _, err = c.chatWithProtocol(
			ctx, baseURL, chatBody, protocol, true, openaiapi.ChatRequestShapeDefault,
		)
		if err == nil {
			openaiapi.ConfirmResponsesSyncMaxOutputTokensUnsupported(cacheKey, fieldRejectionEpoch, fieldRetryExplicit)
		} else {
			openaiapi.ClearResponsesSyncMaxOutputTokensRejectedPending(cacheKey, fieldRejectionEpoch)
		}
	}
	if err == nil && !fieldRetryAttempted && protocol == openaiapi.ProtocolResponses && hasMaxOutputTokens {
		openaiapi.MarkResponsesSyncMaxOutputTokensSupported(cacheKey)
	}
	releaseResponsesFieldProbe()
	if err == nil {
		openaiapi.MarkProtocolSuccess(cacheKey, protocol)
		return result, nil
	}
	if fieldRetryAttempted {
		return nil, err
	}
	if chatShapeRetryAttempted {
		return nil, err
	}
	chatResponseMismatch := !decision.Known && errors.Is(err, openaiapi.ErrResponsesEndpointReturnedChatCompletion)
	if !chatResponseMismatch && !openaiapi.ShouldTryAlternateForDecision(decision, status, err) {
		return nil, err
	}

	persistAlternate := chatResponseMismatch || openaiapi.ShouldTryAlternateProtocol(status, err)
	alternate := openaiapi.AlternateProtocol(protocol)
	var alternateErr error
	if alternate == openaiapi.ProtocolChatCompletions {
		result, _, _, alternateErr = c.chatWithChatRequestShape(ctx, baseURL, chatBody, cacheKey)
	} else {
		result, _, _, alternateErr = c.chatWithProtocol(
			ctx, baseURL, chatBody, alternate, false, openaiapi.ChatRequestShapeDefault,
		)
	}
	if alternateErr != nil {
		return nil, fmt.Errorf("OpenAI protocol fallback from %s to %s failed: %w", protocol, alternate, alternateErr)
	}
	if persistAlternate {
		openaiapi.MarkProtocolSuccess(cacheKey, alternate)
	}
	return result, nil
}

func (c *RemoteAPIChat) chatWithChatRequestShape(
	ctx context.Context,
	baseURL string,
	chatBody any,
	cacheKey string,
) (*types.ChatResponse, int, bool, error) {
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	result, status, _, err := c.chatWithProtocol(
		ctx, baseURL, chatBody, openaiapi.ProtocolChatCompletions, false, shape,
	)
	if err == nil {
		if shape == openaiapi.ChatRequestShapeMaxCompletionNeutral {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, shape)
		}
		return result, status, false, nil
	}
	if shape != openaiapi.ChatRequestShapeDefault ||
		!openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		return result, status, false, err
	}
	result, status, _, err = c.chatWithProtocol(
		ctx, baseURL, chatBody, openaiapi.ProtocolChatCompletions, false,
		openaiapi.ChatRequestShapeMaxCompletionNeutral,
	)
	if err == nil {
		openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	}
	return result, status, true, err
}

func (c *RemoteAPIChat) chatWithProtocol(
	ctx context.Context,
	baseURL string,
	chatBody any,
	protocol openaiapi.Protocol,
	omitMaxOutputTokens bool,
	chatShape openaiapi.ChatRequestShape,
) (*types.ChatResponse, int, bool, error) {
	body := chatBody
	hasMaxOutputTokens := false
	var err error
	if protocol == openaiapi.ProtocolResponses {
		var facts openaiapi.ResponsesRequestFacts
		body, facts, err = openaiapi.BuildResponsesRequestWithOptionsAndFacts(
			chatBody,
			openaiapi.ResponsesRequestOptions{OmitMaxOutputTokens: omitMaxOutputTokens},
		)
		if err != nil {
			return nil, 0, false, err
		}
		hasMaxOutputTokens = facts.HasMaxOutputTokens
	} else {
		body, err = openaiapi.BuildChatRequestWithShape(chatBody, chatShape)
		if err != nil {
			return nil, 0, false, err
		}
	}

	respBody, status, contentType, err := c.doOpenAIProtocolRequest(ctx, baseURL, body, protocol, false)
	if err != nil {
		return nil, status, hasMaxOutputTokens, err
	}
	if protocol == openaiapi.ProtocolResponses {
		result, facts, parseErr := openaiapi.ParseResponsesHTTPResponse(status, contentType, respBody)
		logger.Infof(ctx, "[Responses Response] body_format=%s status=%d content_type=%q bytes=%d",
			facts.BodyFormat, facts.StatusCode, facts.ContentType, facts.Bytes)
		langfuse.ReportGenerationProgress(ctx, langfuse.GenerationProgress{
			State: "response_decoded", Protocol: string(protocol),
			EventType: fmt.Sprintf("body_format=%s status=%d content_type=%q bytes=%d",
				facts.BodyFormat, facts.StatusCode, facts.ContentType, facts.Bytes),
		})
		err = parseErr
		if err != nil {
			return nil, status, hasMaxOutputTokens, err
		}
		logUsage(ctx, c.modelName, &result.Usage)
		return result, status, hasMaxOutputTokens, nil
	}

	var completion openai.ChatCompletionResponse
	if err := json.Unmarshal(respBody, &completion); err != nil {
		return nil, status, false, fmt.Errorf("decode Chat Completions response: %w", err)
	}
	result, err := c.parseCompletionResponse(&completion)
	if err != nil {
		return nil, status, false, err
	}
	c.applyCompletionToolCallMetadata(respBody, result)
	applyRawPromptCacheUsage(respBody, &result.Usage)
	logUsage(ctx, c.modelName, &result.Usage)
	return result, status, false, nil
}

func (c *RemoteAPIChat) chatStreamWithNegotiatedProtocol(
	ctx context.Context,
	baseURL string,
	chatBody any,
) (<-chan types.StreamResponse, error) {
	cacheKey := c.protocolCacheKey(baseURL)
	decision := openaiapi.ResolveProtocol(cacheKey)
	var unlock func()
	if !decision.Known {
		var acquireErr error
		unlock, acquireErr = openaiapi.AcquireProtocolProbe(ctx, cacheKey)
		if acquireErr != nil {
			return nil, acquireErr
		}
		decision = openaiapi.ResolveProtocol(cacheKey)
	}
	releaseProbe := func() {
		if unlock != nil {
			unlock()
			unlock = nil
		}
	}
	protocol := decision.Protocol
	preferredChatShape := openaiapi.PreferredChatRequestShape(cacheKey)
	persistProtocol := true
	var resp *http.Response
	var status int
	var chatShapeRetryAttempted bool
	var err error
	if protocol == openaiapi.ProtocolChatCompletions {
		resp, status, chatShapeRetryAttempted, err = c.openChatStreamWithShape(ctx, baseURL, chatBody, cacheKey)
	} else {
		resp, status, err = c.openOpenAIProtocolStream(
			ctx, baseURL, chatBody, protocol, openaiapi.ChatRequestShapeDefault,
		)
	}
	if err != nil && !chatShapeRetryAttempted && openaiapi.ShouldTryAlternateForDecision(decision, status, err) {
		persistProtocol = openaiapi.ShouldTryAlternateProtocol(status, err)
		alternate := openaiapi.AlternateProtocol(protocol)
		if alternate == openaiapi.ProtocolChatCompletions {
			resp, _, chatShapeRetryAttempted, err = c.openChatStreamWithShape(ctx, baseURL, chatBody, cacheKey)
		} else {
			resp, _, err = c.openOpenAIProtocolStream(
				ctx, baseURL, chatBody, alternate, openaiapi.ChatRequestShapeDefault,
			)
		}
		if err != nil {
			releaseProbe()
			return nil, fmt.Errorf("OpenAI protocol fallback from %s to %s failed: %w", protocol, alternate, err)
		}
		protocol = alternate
	}
	if err != nil {
		releaseProbe()
		return nil, err
	}
	// The probe lock protects only endpoint selection/opening. Holding it until
	// response.completed would serialize every first-use request behind a
	// 12-18 minute Wiki stream. Protocol success is still cached only by
	// forwardNegotiatedStream after the terminal completed event.
	releaseProbe()

	providerChan := make(chan types.StreamResponse)
	if protocol == openaiapi.ProtocolResponses {
		go c.processResponsesStream(ctx, resp, providerChan)
	} else {
		go c.processRawHTTPStream(ctx, resp, providerChan, nil)
	}
	streamChan := make(chan types.StreamResponse)
	go forwardNegotiatedStream(ctx, providerChan, streamChan, func() {
		if protocol == openaiapi.ProtocolChatCompletions &&
			(chatShapeRetryAttempted || preferredChatShape == openaiapi.ChatRequestShapeMaxCompletionNeutral) {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
		}
		if persistProtocol {
			openaiapi.MarkProtocolSuccess(cacheKey, protocol)
		}
	}, releaseProbe)
	return streamChan, nil
}

func (c *RemoteAPIChat) openChatStreamWithShape(
	ctx context.Context,
	baseURL string,
	chatBody any,
	cacheKey string,
) (*http.Response, int, bool, error) {
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	resp, status, err := c.openOpenAIProtocolStream(
		ctx, baseURL, chatBody, openaiapi.ProtocolChatCompletions, shape,
	)
	if err == nil || shape != openaiapi.ChatRequestShapeDefault ||
		!openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		return resp, status, false, err
	}
	resp, status, err = c.openOpenAIProtocolStream(
		ctx, baseURL, chatBody, openaiapi.ProtocolChatCompletions,
		openaiapi.ChatRequestShapeMaxCompletionNeutral,
	)
	return resp, status, true, err
}

func forwardNegotiatedStream(
	ctx context.Context,
	provider <-chan types.StreamResponse,
	out chan<- types.StreamResponse,
	onSuccess func(),
	onFinished func(),
) {
	defer close(out)
	defer onFinished()
	completed := false
	failed := false
	for {
		var event types.StreamResponse
		var ok bool
		select {
		case <-ctx.Done():
			return
		case event, ok = <-provider:
			if !ok {
				if completed && !failed {
					onSuccess()
				}
				return
			}
		}
		if event.ResponseType == types.ResponseTypeError {
			failed = true
		}
		if event.Done && event.ResponseType != types.ResponseTypeError {
			completed = true
		}
		if !sendStreamResponse(ctx, out, event) {
			return
		}
	}
}

func (c *RemoteAPIChat) openOpenAIProtocolStream(
	ctx context.Context,
	baseURL string,
	chatBody any,
	protocol openaiapi.Protocol,
	chatShape openaiapi.ChatRequestShape,
) (*http.Response, int, error) {
	body := chatBody
	if protocol == openaiapi.ProtocolResponses {
		var err error
		body, err = openaiapi.BuildResponsesRequest(chatBody)
		if err != nil {
			return nil, 0, err
		}
		body.(map[string]any)["stream"] = true
	} else {
		var err error
		body, err = openaiapi.BuildChatRequestWithShape(chatBody, chatShape)
		if err != nil {
			return nil, 0, err
		}
	}

	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal %s request: %w", protocol, err)
	}
	endpoint := openaiapi.Endpoint(baseURL, protocol)
	if err := secutils.ValidateURLForSSRF(endpoint); err != nil {
		return nil, 0, fmt.Errorf("endpoint SSRF check failed: %w", err)
	}
	c.logOpenAIProtocolRequest(ctx, endpoint, protocol, jsonData, true)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(jsonData))
	if err != nil {
		return nil, 0, fmt.Errorf("create %s request: %w", protocol, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	c.adapter.Auth(req, c.authCreds(), jsonData)
	secutils.ApplyCustomHeaders(req, c.customHeaders)
	langfuse.ReportGenerationProgress(ctx, langfuse.GenerationProgress{
		State:    "request_dispatched",
		Protocol: string(protocol),
		Endpoint: endpoint,
	})

	resp, err := rawHTTPClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("send %s stream request: %w", protocol, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		responseBody, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, openaiapi.NewProtocolHTTPError(
			protocol, resp.StatusCode, strings.TrimSpace(string(responseBody)),
		)
	}
	langfuse.ReportGenerationProgress(ctx, langfuse.GenerationProgress{
		State:    "stream_opened",
		Protocol: string(protocol),
		Endpoint: endpoint,
	})
	return resp, resp.StatusCode, nil
}

func (c *RemoteAPIChat) doOpenAIProtocolRequest(
	ctx context.Context,
	baseURL string,
	body any,
	protocol openaiapi.Protocol,
	stream bool,
) ([]byte, int, string, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, 0, "", fmt.Errorf("marshal %s request: %w", protocol, err)
	}
	endpoint := openaiapi.Endpoint(baseURL, protocol)
	if err := secutils.ValidateURLForSSRF(endpoint); err != nil {
		return nil, 0, "", fmt.Errorf("endpoint SSRF check failed: %w", err)
	}
	c.logOpenAIProtocolRequest(ctx, endpoint, protocol, jsonData, stream)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(jsonData))
	if err != nil {
		return nil, 0, "", fmt.Errorf("create %s request: %w", protocol, err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.adapter.Auth(req, c.authCreds(), jsonData)
	secutils.ApplyCustomHeaders(req, c.customHeaders)

	resp, err := rawHTTPClient.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("send %s request: %w", protocol, err)
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, resp.Header.Get("Content-Type"), fmt.Errorf("read %s response: %w", protocol, readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, resp.Header.Get("Content-Type"), openaiapi.NewProtocolHTTPError(
			protocol, resp.StatusCode, strings.TrimSpace(string(responseBody)),
		)
	}
	return responseBody, resp.StatusCode, resp.Header.Get("Content-Type"), nil
}

// negotiatedProtocolBase selects every OpenAI-compatible route whose actual
// outbound endpoint is Chat Completions. The decision is endpoint-based, not
// provider- or model-based, so OpenAI, DeepSeek, and other compatible services
// all receive the same Responses capability probe. Deployment-specific SDK
// layouts such as Azure remain outside this standard sibling-endpoint rule.
func (c *RemoteAPIChat) negotiatedProtocolBase(endpoint string) (string, bool) {
	if !c.autoProtocol {
		return "", false
	}
	if strings.TrimSpace(endpoint) == "" {
		return openaiapi.NormalizeBaseURL(c.baseURL), true
	}
	normalized := openaiapi.NormalizeBaseURL(endpoint)
	if normalized == strings.TrimRight(strings.TrimSpace(endpoint), "/") {
		return "", false
	}
	return normalized, true
}

func (c *RemoteAPIChat) protocolCacheKey(baseURL string) string {
	return openaiapi.ProtocolCacheKey(c.modelID, baseURL, map[string]any{
		"model_name":                c.modelName,
		"configuration_fingerprint": c.configurationFingerprint,
		"thinking_override":         fmt.Sprintf("%#v", c.thinkingOverride),
		"adapter":                   fmt.Sprintf("%T", c.adapter),
	})
}

func (c *RemoteAPIChat) logOpenAIProtocolRequest(
	ctx context.Context,
	endpoint string,
	protocol openaiapi.Protocol,
	body []byte,
	stream bool,
) {
	logger.Infof(ctx, "[LLM Request] protocol=%s, endpoint=%s, model=%s, stream=%v, request:\n%s",
		protocol, endpoint, c.modelName, stream, secutils.CompactImageDataURLForLog(string(body)))
}

func (c *RemoteAPIChat) processResponsesStream(
	ctx context.Context,
	resp *http.Response,
	streamChan chan types.StreamResponse,
) {
	defer close(streamChan)
	defer resp.Body.Close()
	emit := func(response types.StreamResponse) bool {
		return sendStreamResponse(ctx, streamChan, response)
	}

	reader := NewSSEReader(resp.Body)
	completed := false
	firstEventReported := false
	lastEventType := ""
	outputStarted := false
	usageObserved := false
	providerRequestID := responsesProviderRequestID(resp)
	reducer := openaiapi.NewResponsesStreamReducer()
	emitError := func(content, code, errorType string, httpStatus int) bool {
		return emit(types.StreamResponse{
			ResponseType: types.ResponseTypeError,
			Content:      content,
			Done:         true,
			Data: types.StreamErrorDetails{
				ProviderRequestID: providerRequestID,
				LastSSEEventType:  lastEventType,
				Code:              code,
				Type:              errorType,
				OutputStarted:     outputStarted,
				UsageObserved:     usageObserved,
				HTTPStatus:        httpStatus,
			}.Data(),
		})
	}
	for {
		event, err := reader.ReadEvent()
		if err != nil {
			if err == io.EOF {
				if !completed {
					emitError("Responses stream closed before response.completed", "stream_closed_before_completion", "transport_error", 0)
				}
			} else {
				emitError(fmt.Sprintf("Responses stream read failed: %v", err), "stream_read_error", "transport_error", 0)
			}
			return
		}
		if event == nil {
			continue
		}
		if event.Done {
			if !completed {
				emitError("Responses stream ended before response.completed", "stream_ended_before_completion", "transport_error", 0)
			}
			return
		}
		if event.Data == nil {
			continue
		}

		responseEvent, err := openaiapi.DecodeResponsesStreamEvent(event.Data)
		if err != nil {
			emitError(err.Error(), "invalid_responses_sse_envelope", "protocol_error", 0)
			return
		}
		lastEventType = responseEvent.Type
		errorMessage, errorType, errorCode, eventRequestID, httpStatus, eventUsageObserved := responsesStreamErrorFacts(responseEvent)
		if providerRequestID == "" {
			providerRequestID = normalizeResponsesRequestID(eventRequestID)
		}
		usageObserved = usageObserved || eventUsageObserved
		if !firstEventReported {
			firstEventReported = true
			langfuse.ReportGenerationProgress(ctx, langfuse.GenerationProgress{
				State:     "first_sse_event",
				Protocol:  string(openaiapi.ProtocolResponses),
				EventType: responseEvent.Type,
			})
		}
		update, reduceErr := reducer.Apply(responseEvent)
		if reduceErr != nil {
			if errorMessage == "" {
				errorMessage = reduceErr.Error()
			}
			if errorCode == "" {
				errorCode = responsesReducerErrorCode(responseEvent.Type)
			}
			emitError(errorMessage, errorCode, errorType, httpStatus)
			return
		}
		outputStarted = outputStarted || update.OutputObserved
		if update.AnswerDelta != "" && !emit(types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: update.AnswerDelta}) {
			return
		}
		if update.ThinkingDelta != "" && !emit(types.StreamResponse{ResponseType: types.ResponseTypeThinking, Content: update.ThinkingDelta}) {
			return
		}
		if update.Completed != nil {
			completed = true
			logUsage(ctx, c.modelName, &update.Completed.Usage)
			emit(types.StreamResponse{
				ResponseType: types.ResponseTypeAnswer,
				Done:         true,
				ToolCalls:    update.Completed.ToolCalls,
				Usage:        &update.Completed.Usage,
				FinishReason: update.Completed.FinishReason,
			})
			return
		}
	}
}

func responsesProviderRequestID(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	for _, name := range []string{"X-Request-Id", "Request-Id", "X-Correlation-Id"} {
		if value := strings.TrimSpace(resp.Header.Get(name)); value != "" {
			return normalizeResponsesRequestID(value)
		}
	}
	return ""
}

func normalizeResponsesRequestID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 256 {
		return value[:256]
	}
	return value
}

func responsesStreamErrorFacts(event openaiapi.ResponsesStreamEvent) (message, errorType, code, requestID string, httpStatus int, usageObserved bool) {
	usageObserved = hasResponsesJSONValue(event.Usage)
	requestID = strings.TrimSpace(event.RequestID)
	if event.Error != nil {
		message = strings.TrimSpace(event.Error.Message)
		errorType = strings.TrimSpace(event.Error.Type)
		code = strings.TrimSpace(event.Error.Code)
		if requestID == "" {
			requestID = strings.TrimSpace(event.Error.RequestID)
		}
		httpStatus = event.Error.HTTPStatus
		if httpStatus == 0 {
			httpStatus = event.Error.StatusCode
		}
	}
	if len(event.Response) == 0 {
		return
	}
	var response struct {
		Error     *openaiapi.ResponsesStreamError `json:"error"`
		Usage     json.RawMessage                 `json:"usage"`
		RequestID string                          `json:"request_id"`
	}
	if json.Unmarshal(event.Response, &response) != nil {
		return
	}
	usageObserved = usageObserved || hasResponsesJSONValue(response.Usage)
	if requestID == "" {
		requestID = strings.TrimSpace(response.RequestID)
	}
	if response.Error == nil {
		return
	}
	if requestID == "" {
		requestID = strings.TrimSpace(response.Error.RequestID)
	}
	if message == "" {
		message = strings.TrimSpace(response.Error.Message)
	}
	if errorType == "" {
		errorType = strings.TrimSpace(response.Error.Type)
	}
	if code == "" {
		code = strings.TrimSpace(response.Error.Code)
	}
	if httpStatus == 0 {
		httpStatus = response.Error.HTTPStatus
		if httpStatus == 0 {
			httpStatus = response.Error.StatusCode
		}
	}
	return
}

func responsesReducerErrorCode(eventType string) string {
	switch eventType {
	case "error":
		return "responses_error"
	case "response.failed":
		return "response_failed"
	case "response.incomplete":
		return "response_incomplete"
	default:
		return "responses_stream_reducer_error"
	}
}

func hasResponsesJSONValue(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte("{}"))
}
