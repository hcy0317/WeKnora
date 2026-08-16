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
	"github.com/Tencent/WeKnora/internal/models/utils"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/google/uuid"
)

func (v *WeKnoraCloudVLM) predictWithNegotiatedProtocol(
	ctx context.Context,
	chatRequest weKnoraCloudVLMRequest,
) (string, error) {
	baseURL := v.protocolBaseURL()
	cacheKey := v.protocolCacheKey(baseURL)
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
		content, status, chatShapeRetryAttempted, err = v.predictWithChatRequestShape(ctx, baseURL, chatRequest, cacheKey)
	} else {
		content, status, hasMaxOutputTokens, err = v.predictWithProtocol(
			ctx, baseURL, chatRequest, protocol, omitMaxOutputTokens, openaiapi.ChatRequestShapeDefault,
		)
	}
	fieldRetryAttempted := err != nil && protocol == openaiapi.ProtocolResponses && hasMaxOutputTokens &&
		openaiapi.ShouldRetryResponsesWithoutMaxOutputTokens(status, err)
	fieldRetryExplicit := fieldRetryAttempted && openaiapi.IsMaxOutputTokensUnsupported(status, err)
	var fieldRejectionEpoch uint64
	if fieldRetryAttempted {
		fieldRejectionEpoch = openaiapi.MarkResponsesSyncMaxOutputTokensRejectedPending(cacheKey)
		content, status, _, err = v.predictWithProtocol(
			ctx, baseURL, chatRequest, protocol, true, openaiapi.ChatRequestShapeDefault,
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
		content, _, _, alternateErr = v.predictWithChatRequestShape(ctx, baseURL, chatRequest, cacheKey)
	} else {
		content, _, _, alternateErr = v.predictWithProtocol(
			ctx, baseURL, chatRequest, alternate, false, openaiapi.ChatRequestShapeDefault,
		)
	}
	if alternateErr != nil {
		return "", fmt.Errorf(
			"WeKnoraCloud VLM protocol fallback from %s to %s failed: %w",
			protocol, alternate, alternateErr,
		)
	}
	if persistAlternate {
		openaiapi.MarkProtocolSuccess(cacheKey, alternate)
	}
	return content, nil
}

func (v *WeKnoraCloudVLM) predictWithChatRequestShape(
	ctx context.Context,
	baseURL string,
	chatRequest weKnoraCloudVLMRequest,
	cacheKey string,
) (string, int, bool, error) {
	shape := openaiapi.PreferredChatRequestShape(cacheKey)
	content, status, _, err := v.predictWithProtocol(
		ctx, baseURL, chatRequest, openaiapi.ProtocolChatCompletions, false, shape,
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
		ctx, baseURL, chatRequest, openaiapi.ProtocolChatCompletions, false,
		openaiapi.ChatRequestShapeMaxCompletionNeutral,
	)
	if err == nil {
		openaiapi.MarkChatRequestShapeSuccess(cacheKey, openaiapi.ChatRequestShapeMaxCompletionNeutral)
	}
	return content, status, true, err
}

func (v *WeKnoraCloudVLM) protocolBaseURL() string {
	baseURL := strings.TrimRight(strings.TrimSpace(v.baseURL), "/")
	if strings.HasSuffix(strings.ToLower(baseURL), "/api/v1") {
		return baseURL
	}
	return baseURL + "/api/v1"
}

func (v *WeKnoraCloudVLM) protocolCacheKey(baseURL string) string {
	return openaiapi.ProtocolCacheKey(v.modelID, baseURL, map[string]any{
		"model_name":                v.modelName,
		"configuration_fingerprint": v.configurationFingerprint,
	})
}

func (v *WeKnoraCloudVLM) predictWithProtocol(
	ctx context.Context,
	baseURL string,
	chatRequest weKnoraCloudVLMRequest,
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

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", 0, false, fmt.Errorf("weknoracloud VLM: marshal %s request: %w", protocol, err)
	}
	requestID := uuid.NewString()
	headers := utils.Sign(v.appID, v.apiKey, requestID, string(bodyBytes))
	endpoint := openaiapi.Endpoint(baseURL, protocol)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", 0, false, fmt.Errorf("weknoracloud VLM: create %s request: %w", protocol, err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := v.client.Do(req)
	if err != nil {
		return "", 0, hasMaxOutputTokens, fmt.Errorf("weknoracloud VLM: send %s request: %w", protocol, err)
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", resp.StatusCode, hasMaxOutputTokens, fmt.Errorf("weknoracloud VLM: read %s response: %w", protocol, readErr)
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
			return "", resp.StatusCode, hasMaxOutputTokens, fmt.Errorf("weknoracloud VLM Responses request: %w", err)
		}
		return result.Content, resp.StatusCode, hasMaxOutputTokens, nil
	}

	var completion struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBody, &completion); err != nil {
		return "", resp.StatusCode, false, fmt.Errorf("weknoracloud VLM: decode Chat Completions response: %w", err)
	}
	if len(completion.Choices) == 0 {
		return "", resp.StatusCode, false, fmt.Errorf("weknoracloud VLM: no choices in response")
	}
	return completion.Choices[0].Message.Content, resp.StatusCode, false, nil
}
