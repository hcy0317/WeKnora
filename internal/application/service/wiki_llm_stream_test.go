package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
	"github.com/Tencent/WeKnora/internal/types"
)

type blockingWikiChat struct {
	streamCalls int
}

func (m *blockingWikiChat) Chat(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (*types.ChatResponse, error) {
	return nil, errors.New("unexpected non-streaming Wiki call")
}

func (m *blockingWikiChat) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.streamCalls++
	return make(chan types.StreamResponse), nil
}

func (m *blockingWikiChat) GetModelName() string { return "blocking-model" }
func (m *blockingWikiChat) GetModelID() string   { return "blocking-model-id" }

type scriptedWikiChat struct {
	modelName   string
	chatCalls   int
	streamCalls int
	chatResp    *types.ChatResponse
	chatErr     error
	streamErr   error
	events      []types.StreamResponse
	streamSteps [][]types.StreamResponse
	streamOpts  *chat.ChatOptions
}

func (m *scriptedWikiChat) Chat(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.chatCalls++
	return m.chatResp, m.chatErr
}

func (m *scriptedWikiChat) ChatStream(
	_ context.Context,
	_ []chat.Message,
	opts *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.streamCalls++
	m.streamOpts = opts
	if m.streamErr != nil {
		return nil, m.streamErr
	}
	events := m.events
	if len(m.streamSteps) >= m.streamCalls {
		events = m.streamSteps[m.streamCalls-1]
	}
	ch := make(chan types.StreamResponse, len(events))
	for _, event := range events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func (m *scriptedWikiChat) GetModelName() string { return m.modelName }
func (m *scriptedWikiChat) GetModelID() string   { return "scripted-wiki" }

func TestCallWikiLLMStreamsLongFormForAnyModelAndAggregatesOnlyAnswers(t *testing.T) {
	usage := &types.TokenUsage{PromptTokens: 10, CompletionTokens: 4, TotalTokens: 14}
	model := &scriptedWikiChat{
		modelName: "any-chat-model",
		events: []types.StreamResponse{
			{ResponseType: types.ResponseTypeThinking, Content: "private reasoning"},
			{ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: hello\n\n"},
			{ResponseType: types.ResponseTypeAnswer, Content: "world", Done: true, FinishReason: "stop", Usage: usage},
		},
	}

	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_summary",
	)
	if err != nil {
		t.Fatalf("callWikiLLM() error = %v", err)
	}
	if model.chatCalls != 0 || model.streamCalls != 1 {
		t.Fatalf("unexpected call routing: chat=%d stream=%d", model.chatCalls, model.streamCalls)
	}
	if response.Content != "SUMMARY: hello\n\nworld" {
		t.Fatalf("content = %q", response.Content)
	}
	if response.FinishReason != "stop" || response.Usage.TotalTokens != 14 {
		t.Fatalf("metadata not preserved: %#v", response)
	}
}

func TestCallWikiLLMStreamsLargeOutputWikiPurposes(t *testing.T) {
	validContent := map[string]string{
		"wiki_summary":           "SUMMARY: valid\n\nMarkdown body",
		"wiki_page_modify":       "SUMMARY: valid\n\nMarkdown body",
		"wiki_candidate_slug":    `{"entities":[],"concepts":[]}`,
		"wiki_knowledge_extract": `{"entities":[],"concepts":[]}`,
		"wiki_chunk_citation":    `{"citations":{},"new_slugs":[]}`,
		"wiki_taxonomy_plan":     `{"assignments":[]}`,
		"wiki_deduplication":     `{"merges":{}}`,
	}
	for purpose, content := range validContent {
		t.Run(purpose, func(t *testing.T) {
			model := &scriptedWikiChat{
				modelName: "provider-independent-model",
				events: []types.StreamResponse{{
					ResponseType: types.ResponseTypeAnswer,
					Content:      content,
					Done:         true,
					FinishReason: "stop",
				}},
			}
			if _, err := callWikiLLM(context.Background(), model, nil, &chat.ChatOptions{}, purpose); err != nil {
				t.Fatalf("callWikiLLM() error = %v", err)
			}
			if model.chatCalls != 0 || model.streamCalls != 1 {
				t.Fatalf("unexpected call routing: chat=%d stream=%d", model.chatCalls, model.streamCalls)
			}
		})
	}
}

func TestCallWikiLLMKeepsShortAndUnknownWikiPurposesNonStreaming(t *testing.T) {
	purposes := []string{
		"wiki_index_intro",
		"wiki_generation",
	}
	for _, purpose := range purposes {
		t.Run(purpose, func(t *testing.T) {
			model := &scriptedWikiChat{
				modelName: "provider-independent-model",
				chatResp:  &types.ChatResponse{Content: `{"ok":true}`},
			}
			response, err := callWikiLLM(
				context.Background(), model, nil, &chat.ChatOptions{}, purpose,
			)
			if err != nil {
				t.Fatalf("callWikiLLM() error = %v", err)
			}
			if model.chatCalls != 1 || model.streamCalls != 0 {
				t.Fatalf("unexpected call routing: chat=%d stream=%d", model.chatCalls, model.streamCalls)
			}
			if response.Content != `{"ok":true}` {
				t.Fatalf("content = %q", response.Content)
			}
		})
	}
}

func TestCallWikiLLMAggregatesStructuredJSONAcrossAnswerEvents(t *testing.T) {
	for _, tc := range []struct {
		purpose string
		parts   []string
		want    string
	}{
		{
			purpose: "wiki_candidate_slug",
			parts:   []string{`{"entities":[`, `{"name":"A","slug":"entity/a","aliases":[],"description":"A","details":"A"}],"concepts":[]}`},
			want:    `{"entities":[{"name":"A","slug":"entity/a","aliases":[],"description":"A","details":"A"}],"concepts":[]}`,
		},
		{
			purpose: "wiki_knowledge_extract",
			parts:   []string{`{"entities":[],`, `"concepts":[]}`},
			want:    `{"entities":[],"concepts":[]}`,
		},
		{
			purpose: "wiki_chunk_citation",
			parts:   []string{`{"citations":{"entity/a":[`, `"c001"]},"new_slugs":[]}`},
			want:    `{"citations":{"entity/a":["c001"]},"new_slugs":[]}`,
		},
		{
			purpose: "wiki_taxonomy_plan",
			parts:   []string{`{"assignments":[{"slug":"entity/a",`, `"path":["People"]}]}`},
			want:    `{"assignments":[{"slug":"entity/a","path":["People"]}]}`,
		},
	} {
		t.Run(tc.purpose, func(t *testing.T) {
			events := make([]types.StreamResponse, 0, len(tc.parts))
			for i, part := range tc.parts {
				event := types.StreamResponse{
					ResponseType: types.ResponseTypeAnswer,
					Content:      part,
				}
				if i == len(tc.parts)-1 {
					event.Done = true
					event.FinishReason = "stop"
				}
				events = append(events, event)
			}
			model := &scriptedWikiChat{events: events}
			response, err := callWikiLLM(
				context.Background(), model, nil, &chat.ChatOptions{}, tc.purpose,
			)
			if err != nil {
				t.Fatalf("callWikiLLM() error = %v", err)
			}
			if response == nil {
				t.Fatal("callWikiLLM() returned a nil response")
			}
			if response.Content != tc.want {
				t.Fatalf("content = %q, want %q", response.Content, tc.want)
			}
		})
	}
}

func TestCallWikiLLMRejectsInvalidStructuredJSON(t *testing.T) {
	for _, purpose := range []string{
		"wiki_candidate_slug",
		"wiki_knowledge_extract",
		"wiki_chunk_citation",
		"wiki_taxonomy_plan",
		"wiki_deduplication",
	} {
		t.Run(purpose, func(t *testing.T) {
			model := &scriptedWikiChat{events: []types.StreamResponse{{
				ResponseType: types.ResponseTypeAnswer,
				Content:      `{"incomplete":`,
				Done:         true,
				FinishReason: "stop",
			}}}
			response, err := callWikiLLM(
				context.Background(), model, nil, &chat.ChatOptions{}, purpose,
			)
			if err == nil || response != nil || !strings.Contains(err.Error(), "content validation failed") {
				t.Fatalf("invalid structured output must fail atomically: response=%#v err=%v", response, err)
			}
		})
	}
}

func TestCallWikiLLMRejectsJSONMissingPurposeContract(t *testing.T) {
	for _, tc := range []struct {
		purpose string
		content string
	}{
		{purpose: "wiki_candidate_slug", content: `{}`},
		{purpose: "wiki_knowledge_extract", content: `{"entities":{}}`},
		{purpose: "wiki_chunk_citation", content: `{"citations":[]}`},
		{purpose: "wiki_taxonomy_plan", content: `{"assignments":{}}`},
		{purpose: "wiki_deduplication", content: `{"merges":[]}`},
	} {
		t.Run(tc.purpose, func(t *testing.T) {
			model := &scriptedWikiChat{events: []types.StreamResponse{{
				ResponseType: types.ResponseTypeAnswer,
				Content:      tc.content,
				Done:         true,
				FinishReason: "stop",
			}}}
			response, err := callWikiLLM(context.Background(), model, nil, &chat.ChatOptions{}, tc.purpose)
			if err == nil || response != nil || !strings.Contains(err.Error(), "content validation failed") {
				t.Fatalf("contract-invalid JSON must fail atomically: content=%q response=%#v err=%v", tc.content, response, err)
			}
		})
	}
}

func TestCallWikiLLMValidatesFencedStructuredJSONLikeExistingConsumers(t *testing.T) {
	content := "```json\n{\"entities\":[],\"concepts\":[]}\n```"
	want := `{"entities":[],"concepts":[]}`
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer,
		Content:      content,
		Done:         true,
		FinishReason: "stop",
	}}}
	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_candidate_slug",
	)
	if err != nil {
		t.Fatalf("callWikiLLM() error = %v", err)
	}
	if response == nil || response.Content != want {
		t.Fatalf("response = %#v, want normalized JSON %q", response, want)
	}
}

func TestCallWikiLLMNormalizesPrefacedStructuredJSON(t *testing.T) {
	want := `{"entities":[],"concepts":[]}`
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer,
		Content:      "我会先按科研关联筛选候选实体。\n" + want,
		Done:         true,
		FinishReason: "stop",
	}}}

	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_candidate_slug",
	)
	if err != nil {
		t.Fatalf("callWikiLLM() error = %v", err)
	}
	if response == nil || response.Content != want {
		t.Fatalf("response = %#v, want normalized JSON %q", response, want)
	}
}

func TestGenerateWithTemplateEnablesJSONModeForStructuredWikiPurposes(t *testing.T) {
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer,
		Content:      `{"entities":[],"concepts":[]}`,
		Done:         true,
		FinishReason: "stop",
	}}}

	_, err := (&wikiIngestService{}).generateWithTemplate(
		context.Background(), model, agent.WikiCandidateSlugPrompt, map[string]string{
			"Content": "source", "Language": "Chinese",
		},
	)
	if err != nil {
		t.Fatalf("generateWithTemplate() error = %v", err)
	}
	if model.streamOpts == nil || !json.Valid(model.streamOpts.Format) {
		t.Fatalf("structured Wiki call format = %s, want valid JSON schema", model.streamOpts.Format)
	}
	format := string(model.streamOpts.Format)
	if !strings.Contains(format, `"entities"`) || !strings.Contains(format, `"concepts"`) {
		t.Fatalf("structured Wiki schema = %s, want entities and concepts", format)
	}
}

func TestCallWikiLLMDiscardsPartialStreamOnProviderError(t *testing.T) {
	model := &scriptedWikiChat{
		modelName: "any-chat-model",
		events: []types.StreamResponse{
			{ResponseType: types.ResponseTypeAnswer, Content: "partial"},
			{ResponseType: types.ResponseTypeError, Content: "API request failed with status 502: upstream request failed", Done: true},
		},
	}

	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_page_modify",
	)
	if err == nil || response != nil {
		t.Fatalf("partial stream must fail atomically: response=%#v err=%v", response, err)
	}
	if !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("error lost provider detail: %v", err)
	}
}

func TestCallWikiLLMRejectsStreamWithoutSuccessfulFinish(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []types.StreamResponse
		want   string
	}{
		{
			name:   "closed before done",
			events: []types.StreamResponse{{ResponseType: types.ResponseTypeAnswer, Content: "partial"}},
			want:   "before completion",
		},
		{
			name: "stop without done",
			events: []types.StreamResponse{{
				ResponseType: types.ResponseTypeAnswer,
				Content:      "SUMMARY: complete\n\nbody",
				FinishReason: "stop",
			}},
			want: "before completion",
		},
		{
			name: "length finish",
			events: []types.StreamResponse{{
				ResponseType: types.ResponseTypeAnswer,
				Content:      "truncated",
				Done:         true,
				FinishReason: "length",
			}},
			want: "finish reason",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &scriptedWikiChat{modelName: "any-chat-model", events: tc.events}
			response, err := callWikiLLM(
				context.Background(), model, nil, &chat.ChatOptions{}, "wiki_summary",
			)
			if err == nil || response != nil {
				t.Fatalf("incomplete stream must fail: response=%#v err=%v", response, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want substring %q", err, tc.want)
			}
		})
	}
}

func TestCallWikiLLMRejectsCompletedButInvalidContent(t *testing.T) {
	for _, purpose := range []string{"wiki_summary", "wiki_page_modify"} {
		for _, content := range []string{"# heading without a stable fallback", "SUMMARY: headline only"} {
			model := &scriptedWikiChat{events: []types.StreamResponse{{
				ResponseType: types.ResponseTypeAnswer,
				Content:      content,
				Done:         true,
				FinishReason: "stop",
			}}}
			response, err := callWikiLLM(context.Background(), model, nil, &chat.ChatOptions{}, purpose)
			if err == nil || response != nil || !strings.Contains(err.Error(), "content validation failed") {
				t.Fatalf("invalid %s content must fail atomically: content=%q response=%#v err=%v", purpose, content, response, err)
			}
		}
	}
}

func TestNormalizeWikiStreamContentRepairsMissingSummaryDeterministically(t *testing.T) {
	for _, tc := range []struct {
		name            string
		content         string
		existingSummary string
		pageTitle       string
		wantSummary     string
	}{
		{
			name:            "existing summary wins",
			content:         "# Generated heading\n\nA fresh body paragraph.",
			existingSummary: "Stable existing summary",
			pageTitle:       "Stable title",
			wantSummary:     "Stable existing summary",
		},
		{
			name:        "first prose paragraph skips heading list and table separator",
			content:     "# Generated heading\n\n- list item\n\n| --- | --- |\n\nThe first meaningful paragraph.\nIt continues here.\n\nLater text.",
			pageTitle:   "Stable title",
			wantSummary: "The first meaningful paragraph. It continues here.",
		},
		{
			name:        "stable page title is final local fallback",
			content:     "# Generated heading\n\n- list item",
			pageTitle:   "Stable title",
			wantSummary: "Stable title",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeWikiStreamContent(
				"wiki_page_modify", tc.content, tc.existingSummary, tc.pageTitle,
			)
			if err != nil {
				t.Fatalf("normalizeWikiStreamContent() error = %v", err)
			}
			summary, body := splitSummaryLine(got)
			if summary != tc.wantSummary || strings.TrimSpace(body) != strings.TrimSpace(tc.content) {
				t.Fatalf("normalized summary/body = %q/%q, want %q/%q", summary, body, tc.wantSummary, tc.content)
			}
		})
	}
}

func TestNormalizeWikiStreamContentFailsClosedWithoutAnyStableSummary(t *testing.T) {
	_, err := normalizeWikiStreamContent("wiki_summary", "# Heading only", "", "")
	if err == nil || wikiGenerationErrorClassOf(err) != WikiGenerationErrorDeterministicOutput {
		t.Fatalf("missing all deterministic fallbacks must fail as deterministic_output: %v", err)
	}
}

func TestGenerateWithTemplateRepairsSummaryWithoutAdditionalModelCall(t *testing.T) {
	model := &scriptedWikiChat{events: []types.StreamResponse{{
		ResponseType: types.ResponseTypeAnswer,
		Content:      "# Generated heading\n\nA usable body paragraph.",
		Done:         true,
		FinishReason: "stop",
	}}}
	content, err := (&wikiIngestService{}).generateWithTemplate(
		context.Background(), model, agent.WikiSummaryPrompt, map[string]string{
			"Content": "source", "Language": "Chinese", "PageTitle": "Stable document title",
		},
	)
	if err != nil {
		t.Fatalf("generateWithTemplate() error = %v", err)
	}
	if model.streamCalls != 1 {
		t.Fatalf("local summary repair used %d model calls, want exactly 1", model.streamCalls)
	}
	if summary, _ := splitSummaryLine(content); summary != "A usable body paragraph." {
		t.Fatalf("repaired summary = %q", summary)
	}
}

func TestGenerateWithTemplateRetriesInterruptedStreamWithinThreeAttempts(t *testing.T) {
	if wikiLLMMaxAttempts != 3 {
		t.Fatalf("wikiLLMMaxAttempts = %d, want 3", wikiLLMMaxAttempts)
	}
	model := &scriptedWikiChat{
		modelName: "any-chat-model",
		streamSteps: [][]types.StreamResponse{
			{
				{ResponseType: types.ResponseTypeAnswer, Content: "discard-first"},
				{ResponseType: types.ResponseTypeError, Content: "Upstream HTTP/2 stream failed", Data: map[string]interface{}{"http_status": 503}},
			},
			{
				{ResponseType: types.ResponseTypeAnswer, Content: "discard-second"},
				{ResponseType: types.ResponseTypeError, Content: "upstream HTTP/2 stream failed", Data: map[string]interface{}{"http_status": 503}},
			},
			{{ResponseType: types.ResponseTypeAnswer, Content: "SUMMARY: recovered\n\ncomplete body", Done: true, FinishReason: "stop"}},
		},
	}
	content, err := (&wikiIngestService{}).generateWithTemplate(
		context.Background(), model, agent.WikiSummaryPrompt, map[string]string{
			"Content": "source", "Language": "Chinese",
		},
	)
	if err != nil {
		t.Fatalf("generateWithTemplate() error = %v", err)
	}
	if model.streamCalls != 3 {
		t.Fatalf("stream calls = %d, want 3", model.streamCalls)
	}
	if content != "SUMMARY: recovered\n\ncomplete body" {
		t.Fatalf("partial attempts leaked into result: %q", content)
	}
}

func TestGenerateWithTemplateDoesNotRetryContractValidationFailure(t *testing.T) {
	model := &scriptedWikiChat{streamSteps: [][]types.StreamResponse{
		{{ResponseType: types.ResponseTypeAnswer, Content: `{}`, Done: true, FinishReason: "stop"}},
		{{ResponseType: types.ResponseTypeAnswer, Content: `{"entities":[],"concepts":[]}`, Done: true, FinishReason: "stop"}},
	}}
	content, err := (&wikiIngestService{}).generateWithTemplate(
		context.Background(), model, agent.WikiCandidateSlugPrompt, map[string]string{
			"Content": "source", "Language": "Chinese",
		},
	)
	if err == nil || content != "" {
		t.Fatalf("validation failure must fail atomically: content=%q err=%v", content, err)
	}
	if model.streamCalls != 1 {
		t.Fatalf("validation failure calls = %d, want 1", model.streamCalls)
	}
}

func TestWikiLLMProductionBudgets(t *testing.T) {
	if wikiLLMMaxAttempts != 3 {
		t.Fatalf("wikiLLMMaxAttempts = %d, want 3", wikiLLMMaxAttempts)
	}
	if wikiLLMAttemptTimeout != 30*time.Minute {
		t.Fatalf("wikiLLMAttemptTimeout = %s, want 30m", wikiLLMAttemptTimeout)
	}
	if WikiIngestTaskTimeout != 90*time.Minute {
		t.Fatalf("WikiIngestTaskTimeout = %s, want 90m", WikiIngestTaskTimeout)
	}
}

func TestGenerateWithTemplateRetriesWikiAttemptTimeoutWithoutInheritingTaskDeadline(t *testing.T) {
	oldTimeout := wikiLLMAttemptTimeout
	oldBackoff := wikiLLMBackoffBase
	wikiLLMAttemptTimeout = 15 * time.Millisecond
	wikiLLMBackoffBase = time.Millisecond
	t.Cleanup(func() {
		wikiLLMAttemptTimeout = oldTimeout
		wikiLLMBackoffBase = oldBackoff
	})

	parentCtx, cancel := context.WithTimeout(context.Background(), WikiIngestTaskTimeout)
	defer cancel()
	model := &blockingWikiChat{}
	started := time.Now()
	content, err := (&wikiIngestService{}).generateWithTemplate(
		parentCtx, model, agent.WikiSummaryPrompt, map[string]string{
			"Content": "source", "Language": "Chinese",
		},
	)
	if err == nil || content != "" {
		t.Fatalf("timed-out Wiki call must fail atomically: content=%q err=%v", content, err)
	}
	if model.streamCalls != wikiLLMMaxAttempts {
		t.Fatalf("stream calls = %d, want %d", model.streamCalls, wikiLLMMaxAttempts)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "attempt timed out") {
		t.Fatalf("error = %q, want Wiki attempt timeout detail", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("child Wiki timeout did not override the outer task deadline: %s", elapsed)
	}
	if parentCtx.Err() != nil {
		t.Fatalf("per-attempt timeout must not cancel the parent task: %v", parentCtx.Err())
	}
}

func TestCallWikiLLMMarksHTTP2StreamCreationInterruptionTransient(t *testing.T) {
	wantErr := openaiapi.NewProtocolHTTPError(
		openaiapi.ProtocolResponses, http.StatusServiceUnavailable, "Upstream HTTP/2 stream failed",
	)
	model := &scriptedWikiChat{modelName: "any-chat-model", streamErr: wantErr}

	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_summary",
	)
	if response != nil || !errors.Is(err, wantErr) {
		t.Fatalf("response=%#v err=%v, want wrapped %v", response, err, wantErr)
	}
	if !isTransientLLMError(context.Background(), err) {
		t.Fatalf("explicit HTTP/2 stream interruption must be retryable: %v", err)
	}
}

func TestCallWikiLLMReturnsStreamCreationError(t *testing.T) {
	wantErr := openaiapi.NewProtocolHTTPError(
		openaiapi.ProtocolResponses, http.StatusServiceUnavailable, "unavailable",
	)
	model := &scriptedWikiChat{modelName: "any-chat-model", streamErr: wantErr}

	response, err := callWikiLLM(
		context.Background(), model, nil, &chat.ChatOptions{}, "wiki_summary",
	)
	if response != nil || !errors.Is(err, wantErr) {
		t.Fatalf("response=%#v err=%v, want %v", response, err, wantErr)
	}
}

func TestIsTransientLLMErrorRejectsAmbiguousHTTP200UpstreamEnvelope(t *testing.T) {
	for _, message := range []string{
		"wiki LLM stream failed: API stream error: Upstream request failed (type=upstream_error)",
		"wiki LLM stream failed: API stream error: gateway failed (code=bad_gateway)",
		"wiki LLM stream closed before completion",
		"wiki LLM stream failed: Responses stream failed: stream_read_error",
	} {
		if isTransientLLMError(context.Background(), errors.New(message)) {
			t.Fatalf("HTTP-200 upstream text must fail closed without typed transport evidence: %q", message)
		}
	}
}
