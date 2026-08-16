package openaiapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
)

var (
	ErrResponsesEndpointReturnedChatCompletion = errors.New("Responses endpoint returned a Chat Completions response")
	errInvalidResponsesSSEEnvelope             = errors.New("invalid Responses SSE envelope")
	errReadResponsesSSE                        = errors.New("read Responses SSE failed")
)

type ResponsesBodyFormat string

const (
	ResponsesBodyFormatJSON    ResponsesBodyFormat = "json"
	ResponsesBodyFormatSSE     ResponsesBodyFormat = "sse"
	ResponsesBodyFormatUnknown ResponsesBodyFormat = "unknown"
)

type ResponsesResponseFacts struct {
	BodyFormat  ResponsesBodyFormat
	StatusCode  int
	ContentType string
	Bytes       int
}

// ResponsesDecodeError deliberately excludes response bytes and parser text:
// gateway error pages and model output may contain credentials or user data.
type ResponsesDecodeError struct {
	StatusCode  int
	ContentType string
	Bytes       int
	BodyFormat  ResponsesBodyFormat
}

type responsesJSONDecodeError struct{ err error }

func (e *responsesJSONDecodeError) Error() string {
	return "decode Responses response: " + e.err.Error()
}
func (e *responsesJSONDecodeError) Unwrap() error { return e.err }

func (e *ResponsesDecodeError) Error() string {
	return fmt.Sprintf("decode Responses HTTP response: status=%d content_type=%q bytes=%d body_format=%s",
		e.StatusCode, e.ContentType, e.Bytes, e.BodyFormat)
}

// BuildResponsesRequest converts an OpenAI Chat Completions request into the
// Responses request shape. Provider-specific extras are intentionally not
// forwarded: only fields defined by the Responses API are copied.
func BuildResponsesRequest(chatRequest any) (map[string]any, error) {
	return BuildResponsesRequestWithOptions(chatRequest, ResponsesRequestOptions{})
}

type ResponsesRequestOptions struct {
	OmitMaxOutputTokens bool
}

type ResponsesRequestFacts struct {
	HasMaxOutputTokens bool
}

// BuildResponsesRequestWithOptions converts a Chat Completions request while
// honoring capabilities learned for this saved model configuration.
func BuildResponsesRequestWithOptions(
	chatRequest any,
	options ResponsesRequestOptions,
) (map[string]any, error) {
	request, _, err := BuildResponsesRequestWithOptionsAndFacts(chatRequest, options)
	return request, err
}

// BuildResponsesRequestWithOptionsAndFacts also reports facts about the body
// that was actually produced. Callers use these facts to avoid retrying a
// field downgrade when the original request never contained that field.
func BuildResponsesRequestWithOptionsAndFacts(
	chatRequest any,
	options ResponsesRequestOptions,
) (map[string]any, ResponsesRequestFacts, error) {
	encoded, err := json.Marshal(chatRequest)
	if err != nil {
		return nil, ResponsesRequestFacts{}, fmt.Errorf("marshal chat request for Responses: %w", err)
	}
	var chatBody map[string]any
	if err := json.Unmarshal(encoded, &chatBody); err != nil {
		return nil, ResponsesRequestFacts{}, fmt.Errorf("decode chat request for Responses: %w", err)
	}

	input, err := convertMessagesToInput(chatBody["messages"])
	if err != nil {
		return nil, ResponsesRequestFacts{}, err
	}
	request := map[string]any{
		"model": chatBody["model"],
		"input": input,
		"store": false,
	}
	copyIfPresent(request, chatBody, "stream", "temperature", "top_p", "parallel_tool_calls", "metadata", "user")

	if !options.OmitMaxOutputTokens {
		if maxOutput, ok := chatBody["max_completion_tokens"]; ok {
			request["max_output_tokens"] = maxOutput
		} else if maxOutput, ok := chatBody["max_tokens"]; ok {
			request["max_output_tokens"] = maxOutput
		}
	}
	if effort, ok := chatBody["reasoning_effort"].(string); ok && strings.TrimSpace(effort) != "" {
		request["reasoning"] = map[string]any{"effort": strings.TrimSpace(effort)}
		delete(request, "temperature")
		delete(request, "top_p")
	}
	if tools, ok := chatBody["tools"].([]any); ok && len(tools) > 0 {
		request["tools"] = convertTools(tools)
	}
	if choice, ok := chatBody["tool_choice"]; ok {
		request["tool_choice"] = convertToolChoice(choice)
	}
	if format, ok := chatBody["response_format"].(map[string]any); ok {
		request["text"] = map[string]any{"format": convertResponseFormat(format)}
	}
	_, hasMaxOutputTokens := request["max_output_tokens"]
	return request, ResponsesRequestFacts{HasMaxOutputTokens: hasMaxOutputTokens}, nil
}

func copyIfPresent(dst, src map[string]any, keys ...string) {
	for _, key := range keys {
		if value, ok := src[key]; ok {
			dst[key] = value
		}
	}
}

func convertMessagesToInput(raw any) ([]any, error) {
	messages, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("Responses request requires a messages array")
	}
	input := make([]any, 0, len(messages))
	for _, rawMessage := range messages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Responses request contains an invalid message")
		}
		role, _ := message["role"].(string)
		if role == "tool" {
			callID, _ := message["tool_call_id"].(string)
			input = append(input, map[string]any{
				"type":    "function_call_output",
				"call_id": callID,
				"output":  contentAsString(message["content"]),
			})
			continue
		}

		if content, exists := message["content"]; exists && !isEmptyContent(content) {
			converted, err := convertMessageContent(content)
			if err != nil {
				return nil, err
			}
			input = append(input, map[string]any{"role": role, "content": converted})
		}
		if toolCalls, ok := message["tool_calls"].([]any); ok {
			for _, rawToolCall := range toolCalls {
				toolCall, _ := rawToolCall.(map[string]any)
				function, _ := toolCall["function"].(map[string]any)
				input = append(input, map[string]any{
					"type":      "function_call",
					"call_id":   toolCall["id"],
					"name":      function["name"],
					"arguments": function["arguments"],
				})
			}
		}
	}
	return input, nil
}

func convertMessageContent(content any) (any, error) {
	if text, ok := content.(string); ok {
		return text, nil
	}
	parts, ok := content.([]any)
	if !ok {
		return nil, fmt.Errorf("Responses request contains unsupported message content")
	}
	converted := make([]any, 0, len(parts))
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			continue
		}
		switch part["type"] {
		case "text", "input_text":
			converted = append(converted, map[string]any{"type": "input_text", "text": part["text"]})
		case "image_url":
			image, _ := part["image_url"].(map[string]any)
			item := map[string]any{"type": "input_image", "image_url": image["url"]}
			if detail, ok := image["detail"]; ok {
				item["detail"] = detail
			}
			converted = append(converted, item)
		case "input_image":
			converted = append(converted, part)
		}
	}
	return converted, nil
}

func isEmptyContent(content any) bool {
	switch value := content.(type) {
	case nil:
		return true
	case string:
		return value == ""
	case []any:
		return len(value) == 0
	default:
		return false
	}
}

func contentAsString(content any) string {
	if text, ok := content.(string); ok {
		return text
	}
	encoded, _ := json.Marshal(content)
	return string(encoded)
}

func convertTools(tools []any) []any {
	converted := make([]any, 0, len(tools))
	for _, rawTool := range tools {
		tool, _ := rawTool.(map[string]any)
		if tool["type"] != "function" {
			converted = append(converted, tool)
			continue
		}
		function, _ := tool["function"].(map[string]any)
		flat := map[string]any{"type": "function"}
		copyIfPresent(flat, function, "name", "description", "parameters", "strict")
		converted = append(converted, flat)
	}
	return converted
}

func convertToolChoice(choice any) any {
	choiceMap, ok := choice.(map[string]any)
	if !ok {
		return choice
	}
	if choiceMap["type"] != "function" {
		return choice
	}
	function, _ := choiceMap["function"].(map[string]any)
	return map[string]any{"type": "function", "name": function["name"]}
}

func convertResponseFormat(format map[string]any) map[string]any {
	if format["type"] != "json_schema" {
		return format
	}
	nested, _ := format["json_schema"].(map[string]any)
	flat := map[string]any{"type": "json_schema"}
	copyIfPresent(flat, nested, "name", "description", "schema", "strict")
	return flat
}

type responsesEnvelope struct {
	Object string `json:"object"`
	Status string `json:"status"`
	Error  *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error"`
	Output []struct {
		Type      string `json:"type"`
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
		Content   []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
		InputDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	} `json:"usage"`
}

// ParseResponsesTerminalEnvelope parses a response.completed envelope. Stream
// consumers use it because compatible gateways may omit final output text that
// was already delivered through output_text.delta events.
func ParseResponsesTerminalEnvelope(body []byte) (*types.ChatResponse, error) {
	var envelope responsesEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, &responsesJSONDecodeError{err: err}
	}
	if strings.EqualFold(strings.TrimSpace(envelope.Object), "chat.completion") {
		return nil, ErrResponsesEndpointReturnedChatCompletion
	}
	if envelope.Error != nil && strings.TrimSpace(envelope.Error.Message) != "" {
		return nil, fmt.Errorf("Responses API error: %s", strings.TrimSpace(envelope.Error.Message))
	}
	if envelope.Status != "completed" {
		return nil, fmt.Errorf("Responses API ended with status %s", envelope.Status)
	}

	var content strings.Builder
	toolCalls := make([]types.LLMToolCall, 0)
	for _, output := range envelope.Output {
		switch output.Type {
		case "message":
			for _, part := range output.Content {
				if part.Type == "output_text" {
					content.WriteString(part.Text)
				}
			}
		case "function_call":
			if strings.TrimSpace(output.CallID) == "" || strings.TrimSpace(output.Name) == "" {
				return nil, fmt.Errorf("Responses API function call requires call_id and name")
			}
			toolCalls = append(toolCalls, types.LLMToolCall{
				ID:   output.CallID,
				Type: "function",
				Function: types.FunctionCall{
					Name:      output.Name,
					Arguments: output.Arguments,
				},
			})
		}
	}
	finishReason := "stop"
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
	}
	usage := types.TokenUsage{
		PromptTokens:     envelope.Usage.InputTokens,
		CompletionTokens: envelope.Usage.OutputTokens,
		TotalTokens:      envelope.Usage.TotalTokens,
	}
	usage.SetPromptCacheUsage(envelope.Usage.InputDetails.CachedTokens, 0, 0, true)
	return &types.ChatResponse{
		Content:      content.String(),
		ToolCalls:    toolCalls,
		FinishReason: finishReason,
		Usage:        usage,
	}, nil
}

// ParseResponsesResponse returns WeKnora's existing ChatResponse shape so the
// rest of the application remains protocol-agnostic. Unlike the terminal-only
// parser, synchronous responses must contain text or a function call.
func ParseResponsesResponse(body []byte) (*types.ChatResponse, error) {
	response, err := ParseResponsesTerminalEnvelope(body)
	if err != nil {
		return nil, err
	}
	if response.Content == "" && len(response.ToolCalls) == 0 {
		return nil, fmt.Errorf("Responses API returned no output text or function call")
	}
	return response, nil
}

// ParseResponsesHTTPResponse decodes a successful synchronous /responses HTTP
// exchange. Some compatible gateways return a valid Responses event stream
// even when stream=false, so response representation is negotiated from the
// HTTP metadata and a conservative body sniff rather than from model names.
func ParseResponsesHTTPResponse(status int, contentType string, body []byte) (*types.ChatResponse, ResponsesResponseFacts, error) {
	facts := ResponsesResponseFacts{
		BodyFormat: ResponsesBodyFormatUnknown, StatusCode: status,
		ContentType: strings.TrimSpace(contentType), Bytes: len(body),
	}
	mediaType, _, _ := mime.ParseMediaType(contentType)
	trimmed := bytes.TrimSpace(body)
	switch {
	case strings.EqualFold(mediaType, "text/event-stream"):
		facts.BodyFormat = ResponsesBodyFormatSSE
	case looksLikeResponsesSSE(trimmed):
		facts.BodyFormat = ResponsesBodyFormatSSE
	case strings.EqualFold(mediaType, "application/json"), strings.HasSuffix(strings.ToLower(mediaType), "+json"),
		len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '['):
		facts.BodyFormat = ResponsesBodyFormatJSON
	}

	var response *types.ChatResponse
	var err error
	switch facts.BodyFormat {
	case ResponsesBodyFormatJSON:
		response, err = ParseResponsesResponse(body)
	case ResponsesBodyFormatSSE:
		response, err = parseResponsesSSE(body)
	default:
		return nil, facts, &ResponsesDecodeError{status, facts.ContentType, len(body), facts.BodyFormat}
	}
	if err != nil {
		if facts.BodyFormat == ResponsesBodyFormatSSE &&
			(errors.Is(err, errInvalidResponsesSSEEnvelope) || errors.Is(err, errReadResponsesSSE)) {
			return nil, facts, &ResponsesDecodeError{status, facts.ContentType, len(body), facts.BodyFormat}
		}
		if facts.BodyFormat == ResponsesBodyFormatJSON && !errors.Is(err, ErrResponsesEndpointReturnedChatCompletion) {
			var decodeErr *responsesJSONDecodeError
			if errors.As(err, &decodeErr) {
				return nil, facts, &ResponsesDecodeError{status, facts.ContentType, len(body), facts.BodyFormat}
			}
		}
		return nil, facts, err
	}
	return response, facts, nil
}

type ResponsesStreamEvent struct {
	Type     string          `json:"type"`
	Delta    string          `json:"delta"`
	Item     json.RawMessage `json:"item"`
	Response json.RawMessage `json:"response"`
	Error    *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type ResponsesStreamUpdate struct {
	AnswerDelta   string
	ThinkingDelta string
	Completed     *types.ChatResponse
}

type ResponsesStreamReducer struct {
	emitted   strings.Builder
	toolCalls []types.LLMToolCall
	seenTools map[string]struct{}
}

func NewResponsesStreamReducer() *ResponsesStreamReducer {
	return &ResponsesStreamReducer{seenTools: make(map[string]struct{})}
}

func DecodeResponsesStreamEvent(data []byte) (ResponsesStreamEvent, error) {
	var event ResponsesStreamEvent
	if err := json.Unmarshal(data, &event); err != nil || (event.Type != "error" && !strings.HasPrefix(event.Type, "response.")) {
		return ResponsesStreamEvent{}, errInvalidResponsesSSEEnvelope
	}
	return event, nil
}

func (r *ResponsesStreamReducer) Apply(event ResponsesStreamEvent) (ResponsesStreamUpdate, error) {
	var update ResponsesStreamUpdate
	switch event.Type {
	case "response.output_text.delta":
		r.emitted.WriteString(event.Delta)
		update.AnswerDelta = event.Delta
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		update.ThinkingDelta = event.Delta
	case "response.output_item.done":
		if toolCall, ok := parseResponsesSSEToolCall(event.Item); ok {
			key := toolCall.ID + "\x00" + toolCall.Function.Name
			if _, exists := r.seenTools[key]; !exists {
				r.seenTools[key] = struct{}{}
				r.toolCalls = append(r.toolCalls, toolCall)
			}
		}
	case "response.failed", "response.incomplete", "error":
		if event.Error != nil && strings.TrimSpace(event.Error.Message) != "" {
			return update, errors.New("Responses stream failed: " + strings.TrimSpace(event.Error.Message))
		}
		var failed struct {
			Status string `json:"status"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(event.Response, &failed)
		if failed.Error != nil && strings.TrimSpace(failed.Error.Message) != "" {
			return update, errors.New("Responses stream failed: " + strings.TrimSpace(failed.Error.Message))
		}
		if failed.Status != "" {
			return update, errors.New("Responses stream ended with status " + failed.Status)
		}
		return update, errors.New("Responses stream failed")
	case "response.completed":
		finalResponse, err := ParseResponsesTerminalEnvelope(event.Response)
		if err != nil {
			return update, err
		}
		if len(finalResponse.ToolCalls) == 0 && len(r.toolCalls) > 0 {
			finalResponse.ToolCalls = r.toolCalls
			finalResponse.FinishReason = "tool_calls"
		}
		streamed := r.emitted.String()
		switch {
		case finalResponse.Content == "":
			finalResponse.Content = streamed
		case streamed == "":
			update.AnswerDelta = finalResponse.Content
			r.emitted.WriteString(finalResponse.Content)
		case finalResponse.Content == streamed:
		case strings.HasPrefix(finalResponse.Content, streamed):
			update.AnswerDelta = strings.TrimPrefix(finalResponse.Content, streamed)
			r.emitted.WriteString(update.AnswerDelta)
		default:
			return update, errors.New("Responses stream completed content does not match received deltas")
		}
		if finalResponse.Content == "" && len(finalResponse.ToolCalls) == 0 {
			return update, errors.New("Responses stream completed without output text or function call")
		}
		update.Completed = finalResponse
	}
	return update, nil
}

func looksLikeResponsesSSE(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	dataEvents, err := responsesSSEData(body)
	if err != nil {
		return false
	}
	for _, data := range dataEvents {
		event, err := DecodeResponsesStreamEvent(data)
		if err == nil && (strings.HasPrefix(event.Type, "response.") || event.Type == "error") {
			return true
		}
	}
	return false
}

func parseResponsesSSE(body []byte) (*types.ChatResponse, error) {
	dataEvents, err := responsesSSEData(body)
	if err != nil {
		return nil, errReadResponsesSSE
	}
	reducer := NewResponsesStreamReducer()
	validEnvelope := false
	for _, data := range dataEvents {
		if bytes.Equal(data, []byte("[DONE]")) {
			break
		}
		event, err := DecodeResponsesStreamEvent(data)
		if err != nil {
			return nil, errInvalidResponsesSSEEnvelope
		}
		validEnvelope = true
		update, err := reducer.Apply(event)
		if err != nil {
			return nil, err
		}
		if update.Completed != nil {
			return update.Completed, nil
		}
	}
	if !validEnvelope {
		return nil, errInvalidResponsesSSEEnvelope
	}
	return nil, errors.New("Responses SSE closed before response.completed")
}

func responsesSSEData(body []byte) ([][]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 4096), 4*1024*1024)
	var dataLines []string
	var events [][]byte
	flush := func() {
		if len(dataLines) == 0 {
			return
		}
		events = append(events, []byte(strings.Join(dataLines, "\n")))
		dataLines = nil
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			flush()
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
	}
	flush()
	return events, scanner.Err()
}

func parseResponsesSSEToolCall(raw json.RawMessage) (types.LLMToolCall, bool) {
	var item struct {
		Type      string `json:"type"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &item) != nil || item.Type != "function_call" ||
		strings.TrimSpace(item.CallID) == "" || strings.TrimSpace(item.Name) == "" {
		return types.LLMToolCall{}, false
	}
	return types.LLMToolCall{ID: item.CallID, Type: "function", Function: types.FunctionCall{
		Name: item.Name, Arguments: item.Arguments,
	}}, true
}
