package vlm

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
	secutils "github.com/Tencent/WeKnora/internal/utils"
	openai "github.com/sashabaranov/go-openai"
)

func (v *RemoteAPIVLM) predictWithNegotiatedProtocol(
	ctx context.Context,
	chatRequest openai.ChatCompletionRequest,
) (string, error) {
	cacheKey := v.protocolCacheKey()
	decision := openaiapi.ResolveProtocol(cacheKey)
	if !decision.Known {
		unlock, acquireErr := openaiapi.AcquireProtocolProbe(ctx, cacheKey)
		if acquireErr != nil {
			return "", acquireErr
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
			return "", acquireErr
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
	var content string
	var status int
	var hasMaxOutputTokens bool
	var chatShapeRetryAttempted bool
	var err error
	if protocol == openaiapi.ProtocolChatCompletions {
		content, status, chatShapeRetryAttempted, err = v.predictWithChatRequestShape(ctx, chatRequest, cacheKey)
	} else {
		content, status, hasMaxOutputTokens, err = v.predictWithProtocol(
			ctx, chatRequest, protocol, omitMaxOutputTokens, openaiapi.ChatRequestShapeDefault,
		)
	}
	fieldRetryAttempted := err != nil && protocol == openaiapi.ProtocolResponses && hasMaxOutputTokens &&
		openaiapi.ShouldRetryResponsesWithoutMaxOutputTokens(status, err)
	fieldRetryExplicit := fieldRetryAttempted && openaiapi.IsMaxOutputTokensUnsupported(status, err)
	var fieldRejectionEpoch uint64
	if fieldRetryAttempted {
		fieldRejectionEpoch = openaiapi.MarkResponsesSyncMaxOutputTokensRejectedPending(cacheKey)
		content, status, _, err = v.predictWithProtocol(
			ctx, chatRequest, protocol, true, openaiapi.ChatRequestShapeDefault,
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
		return content, nil
	}
	if fieldRetryAttempted {
		return "", err
	}
	if chatShapeRetryAttempted {
		return "", err
	}
	chatResponseMismatch := !decision.Known && errors.Is(err, openaiapi.ErrResponsesEndpointReturnedChatCompletion)
	if !chatResponseMismatch && !openaiapi.ShouldTryAlternateForDecision(decision, status, err) {
		return "", err
	}

	persistAlternate := chatResponseMismatch || openaiapi.ShouldTryAlternateProtocol(status, err)
	alternate := openaiapi.AlternateProtocol(protocol)
	var alternateErr error
	if alternate == openaiapi.ProtocolChatCompletions {
		content, _, _, alternateErr = v.predictWithChatRequestShape(ctx, chatRequest, cacheKey)
	} else {
		content, _, _, alternateErr = v.predictWithProtocol(
			ctx, chatRequest, alternate, false, openaiapi.ChatRequestShapeDefault,
		)
	}
	if alternateErr != nil {
		return "", fmt.Errorf("OpenAI VLM protocol fallback from %s to %s failed: %w", protocol, alternate, alternateErr)
	}
	if persistAlternate {
		openaiapi.MarkProtocolSuccess(cacheKey, alternate)
	}
	return content, nil
}

func (v *RemoteAPIVLM) predictWithChatRequestShape(
	ctx context.Context,
	chatRequest openai.ChatCompletionRequest,
	cacheKey string,
) (string, int, bool, error) {
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	content, status, _, err := v.predictWithProtocol(
		ctx, chatRequest, openaiapi.ProtocolChatCompletions, false, shape,
	)
	if err == nil {
		if shape == openaiapi.ChatRequestShapeMaxCompletionNeutral {
			openaiapi.MarkChatRequestShapeSuccess(cacheKey, shape)
		}
		return content, status, false, nil
	}
	if shape != openaiapi.ChatRequestShapeDefault ||
		!openaiapi.ShouldRetryChatWithMaxCompletionNeutral(status, err) {
		return content, status, false, err
	}

	content, status, _, err = v.predictWithProtocol(
		ctx,
		chatRequest,
		openaiapi.ProtocolChatCompletions,
		false,
		openaiapi.ChatRequestShapeMaxCompletionNeutral,
	)
	if err == nil {
		openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	}
	return content, status, true, err
}

func (v *RemoteAPIVLM) predictWithProtocol(
	ctx context.Context,
	chatRequest openai.ChatCompletionRequest,
	protocol openaiapi.Protocol,
	omitMaxOutputTokens bool,
	chatShape openaiapi.ChatRequestShape,
) (string, int, bool, error) {
	body := any(chatRequest)
	hasMaxOutputTokens := false
	var err error
	if protocol == openaiapi.ProtocolResponses {
		var facts openaiapi.ResponsesRequestFacts
		body, facts, err = openaiapi.BuildResponsesRequestWithOptionsAndFacts(
			chatRequest,
			openaiapi.ResponsesRequestOptions{OmitMaxOutputTokens: omitMaxOutputTokens},
		)
		if err != nil {
			return "", 0, false, err
		}
		hasMaxOutputTokens = facts.HasMaxOutputTokens
	} else {
		body, err = openaiapi.BuildChatRequestWithShape(chatRequest, chatShape)
		if err != nil {
			return "", 0, false, err
		}
	}

	jsonData, err := json.Marshal(body)
	if err != nil {
		return "", 0, false, fmt.Errorf("marshal OpenAI VLM %s request: %w", protocol, err)
	}
	endpoint := openaiapi.Endpoint(v.baseURL, protocol)
	if err := secutils.ValidateURLForSSRF(endpoint); err != nil {
		return "", 0, false, fmt.Errorf("OpenAI VLM endpoint SSRF check failed: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(jsonData))
	if err != nil {
		return "", 0, false, fmt.Errorf("create OpenAI VLM %s request: %w", protocol, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(v.apiKey) != "" {
		httpReq.Header.Set("Authorization", "Bearer "+v.apiKey)
	}
	secutils.ApplyCustomHeaders(httpReq, v.customHeaders)

	resp, err := v.httpClient.Do(httpReq)
	if err != nil {
		return "", 0, hasMaxOutputTokens, fmt.Errorf("OpenAI VLM %s request: %w", protocol, err)
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", resp.StatusCode, hasMaxOutputTokens, fmt.Errorf("read OpenAI VLM %s response: %w", protocol, readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode, hasMaxOutputTokens, openaiapi.NewProtocolHTTPError(
			protocol, resp.StatusCode, strings.TrimSpace(string(responseBody)),
		)
	}

	if protocol == openaiapi.ProtocolResponses {
		result, facts, parseErr := openaiapi.ParseResponsesHTTPResponse(
			resp.StatusCode, resp.Header.Get("Content-Type"), responseBody,
		)
		logger.Infof(ctx, "[Responses Response] body_format=%s status=%d content_type=%q bytes=%d",
			facts.BodyFormat, facts.StatusCode, facts.ContentType, facts.Bytes)
		langfuse.ReportGenerationProgress(ctx, langfuse.GenerationProgress{
			State: "response_decoded", Protocol: string(protocol),
			EventType: fmt.Sprintf("body_format=%s status=%d content_type=%q bytes=%d",
				facts.BodyFormat, facts.StatusCode, facts.ContentType, facts.Bytes),
		})
		err = parseErr
		if err != nil {
			return "", resp.StatusCode, hasMaxOutputTokens, fmt.Errorf("OpenAI VLM Responses request: %w", err)
		}
		return result.Content, resp.StatusCode, hasMaxOutputTokens, nil
	}

	var completion openai.ChatCompletionResponse
	if err := json.Unmarshal(responseBody, &completion); err != nil {
		return "", resp.StatusCode, false, fmt.Errorf("decode OpenAI VLM Chat Completions response: %w", err)
	}
	content, err := vlmChatCompletionContent(&completion)
	return content, resp.StatusCode, false, err
}

func (v *RemoteAPIVLM) protocolCacheKey() string {
	return openaiapi.ProtocolCacheKey(v.modelID, v.baseURL, map[string]any{
		"model_name":                v.modelName,
		"configuration_fingerprint": v.configurationFingerprint,
	})
}
