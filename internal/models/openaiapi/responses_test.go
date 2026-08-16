package openaiapi

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResponsesHTTPResponseAcceptsSSEForSyncRequest(t *testing.T) {
	body := []byte("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello world\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n")

	response, facts, err := ParseResponsesHTTPResponse(200, "text/event-stream; charset=utf-8", body)
	require.NoError(t, err)
	require.Equal(t, "hello world", response.Content)
	require.Equal(t, 5, response.Usage.TotalTokens)
	require.Equal(t, ResponsesBodyFormatSSE, facts.BodyFormat)
}

func TestParseResponsesHTTPResponseAcceptsDeltasWhenCompletedOutputIsEmpty(t *testing.T) {
	body := []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"delta only\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"total_tokens\":7}}}\n\n")
	response, _, err := ParseResponsesHTTPResponse(200, "application/octet-stream", body)
	require.NoError(t, err)
	require.Equal(t, "delta only", response.Content)
	require.Equal(t, 7, response.Usage.TotalTokens)
}

func TestParseResponsesHTTPResponseRejectsIncompleteAndFailedSSE(t *testing.T) {
	for _, body := range []string{
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
		"data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n",
	} {
		_, _, err := ParseResponsesHTTPResponse(200, "text/event-stream", []byte(body))
		require.Error(t, err)
	}
}

func TestParseResponsesHTTPResponseDecodeErrorIsTypedAndSafe(t *testing.T) {
	secret := "secret-model-output-token"
	for _, tc := range []struct{ contentType, body string }{
		{"text/plain", "error: " + secret},
		{"text/html", "<html>" + secret + "</html>"},
		{"application/json", "{broken " + secret},
		{"text/event-stream", "event: response.completed\ndata: not-json " + secret + "\n\n"},
		{"", "event: not-an-sse-envelope " + secret},
	} {
		_, facts, err := ParseResponsesHTTPResponse(200, tc.contentType, []byte(tc.body))
		var decodeErr *ResponsesDecodeError
		require.ErrorAs(t, err, &decodeErr)
		require.NotContains(t, err.Error(), secret)
		require.Contains(t, err.Error(), "status=200")
		require.Contains(t, err.Error(), "bytes=")
		require.Equal(t, 200, facts.StatusCode)
	}
}

func TestParseResponsesHTTPResponseJSONPathUnchanged(t *testing.T) {
	response, facts, err := ParseResponsesHTTPResponse(200, "application/json", []byte(`{
		"status":"completed",
		"output":[{"type":"message","content":[{"type":"output_text","text":"json"}]}]
	}`))
	require.NoError(t, err)
	require.Equal(t, "json", response.Content)
	require.Equal(t, ResponsesBodyFormatJSON, facts.BodyFormat)
}

func TestParseResponsesResponseClassifiesChatCompletionMismatch(t *testing.T) {
	_, err := ParseResponsesResponse([]byte(`{"object":"chat.completion","choices":[{"message":{"content":"chat"}}]}`))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrResponsesEndpointReturnedChatCompletion))
}

func TestBuildResponsesRequestMapsVisionReasoningAndStructuredOutput(t *testing.T) {
	request, err := BuildResponsesRequest(map[string]any{
		"model":                 "test-model",
		"max_completion_tokens": 4096,
		"reasoning_effort":      "high",
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "describe"},
					map[string]any{
						"type": "image_url",
						"image_url": map[string]any{
							"url":    "data:image/png;base64,AA==",
							"detail": "auto",
						},
					},
				},
			},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "answer",
				"strict": true,
				"schema": map[string]any{"type": "object"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, false, request["store"])
	require.EqualValues(t, 4096, request["max_output_tokens"])
	require.Equal(t, map[string]any{"effort": "high"}, request["reasoning"])

	input := request["input"].([]any)
	require.Len(t, input, 1)
	content := input[0].(map[string]any)["content"].([]any)
	require.Equal(t, map[string]any{"type": "input_text", "text": "describe"}, content[0])
	require.Equal(t, "input_image", content[1].(map[string]any)["type"])
	require.Equal(t, "data:image/png;base64,AA==", content[1].(map[string]any)["image_url"])

	text := request["text"].(map[string]any)
	format := text["format"].(map[string]any)
	require.Equal(t, "json_schema", format["type"])
	require.Equal(t, "answer", format["name"])
}

func TestBuildResponsesRequestCanOmitUnsupportedMaxOutputTokens(t *testing.T) {
	request, err := BuildResponsesRequestWithOptions(map[string]any{
		"model":                 "gpt-test",
		"messages":              []any{map[string]any{"role": "user", "content": "hello"}},
		"max_completion_tokens": 4096,
	}, ResponsesRequestOptions{OmitMaxOutputTokens: true})
	require.NoError(t, err)
	require.NotContains(t, request, "max_output_tokens")
}

func TestBuildResponsesRequestReportsWhetherMaxOutputTokensWasGenerated(t *testing.T) {
	withField, facts, err := BuildResponsesRequestWithOptionsAndFacts(map[string]any{
		"model":                 "gpt-test",
		"messages":              []any{map[string]any{"role": "user", "content": "hello"}},
		"max_completion_tokens": 4096,
	}, ResponsesRequestOptions{})
	require.NoError(t, err)
	require.Contains(t, withField, "max_output_tokens")
	require.True(t, facts.HasMaxOutputTokens)

	withoutSourceField, facts, err := BuildResponsesRequestWithOptionsAndFacts(map[string]any{
		"model":    "gpt-test",
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	}, ResponsesRequestOptions{})
	require.NoError(t, err)
	require.NotContains(t, withoutSourceField, "max_output_tokens")
	require.False(t, facts.HasMaxOutputTokens)

	omitted, facts, err := BuildResponsesRequestWithOptionsAndFacts(map[string]any{
		"model":      "gpt-test",
		"messages":   []any{map[string]any{"role": "user", "content": "hello"}},
		"max_tokens": 2048,
	}, ResponsesRequestOptions{OmitMaxOutputTokens: true})
	require.NoError(t, err)
	require.NotContains(t, omitted, "max_output_tokens")
	require.False(t, facts.HasMaxOutputTokens)
}

func TestParseResponsesResponseRequiresCompletedStatus(t *testing.T) {
	_, err := ParseResponsesResponse([]byte(`{
		"status":"incomplete",
		"output":[{"type":"message","content":[{"type":"output_text","text":"partial"}]}]
	}`))
	require.ErrorContains(t, err, "status incomplete")
}

func TestParseResponsesTerminalEnvelopeAllowsEmptyOutput(t *testing.T) {
	response, err := ParseResponsesTerminalEnvelope([]byte(`{
		"status":"completed",
		"output":[],
		"usage":{"input_tokens":2,"output_tokens":2,"total_tokens":4}
	}`))
	require.NoError(t, err)
	require.Empty(t, response.Content)
	require.Empty(t, response.ToolCalls)
	require.Equal(t, "stop", response.FinishReason)
	require.Equal(t, 4, response.Usage.TotalTokens)

	_, err = ParseResponsesResponse([]byte(`{"status":"completed","output":[]}`))
	require.ErrorContains(t, err, "no output text or function call")
}

func TestParseResponsesTerminalEnvelopeRejectsToolWithoutIdentity(t *testing.T) {
	for _, body := range []string{
		`{"status":"completed","output":[{"type":"function_call","name":"wiki_read_page","arguments":"{}"}]}`,
		`{"status":"completed","output":[{"type":"function_call","call_id":"call_1","arguments":"{}"}]}`,
	} {
		_, err := ParseResponsesTerminalEnvelope([]byte(body))
		require.ErrorContains(t, err, "function call requires call_id and name")
	}
}
