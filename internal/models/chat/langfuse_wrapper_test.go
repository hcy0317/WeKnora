package chat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
)

type langfuseStreamTestChat struct {
	events    []types.StreamResponse
	nilStream bool
}

func (f *langfuseStreamTestChat) GetModelName() string { return "test-model" }
func (f *langfuseStreamTestChat) GetModelID() string   { return "test-model-id" }
func (f *langfuseStreamTestChat) Chat(
	context.Context, []Message, *ChatOptions,
) (*types.ChatResponse, error) {
	return &types.ChatResponse{}, nil
}
func (f *langfuseStreamTestChat) ChatStream(
	context.Context, []Message, *ChatOptions,
) (<-chan types.StreamResponse, error) {
	if f.nilStream {
		return nil, nil
	}
	ch := make(chan types.StreamResponse, len(f.events))
	for _, event := range f.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func installLangfuseUsageRecorder(t *testing.T) chan types.KnowledgeGenerationUsage {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	mgr, err := langfuse.Init(langfuse.Config{
		Enabled:        true,
		Host:           server.URL,
		PublicKey:      "pk-test",
		SecretKey:      "sk-test",
		FlushAt:        1,
		FlushInterval:  time.Second,
		QueueSize:      32,
		RequestTimeout: time.Second,
		SampleRate:     1,
	})
	if err != nil {
		server.Close()
		t.Fatalf("init Langfuse test manager: %v", err)
	}
	recorded := make(chan types.KnowledgeGenerationUsage, 2)
	mgr.SetKnowledgeUsageRecorder(func(_ context.Context, usage types.KnowledgeGenerationUsage) {
		recorded <- usage
	})
	t.Cleanup(func() {
		_ = mgr.Shutdown(context.Background())
		server.Close()
		_, _ = langfuse.Init(langfuse.Config{})
	})
	return recorded
}

func TestBuildLangfuseGenerationOutput(t *testing.T) {
	toolCalls := []types.LLMToolCall{{ID: "call_1", Type: "function"}}

	got := buildLangfuseGenerationOutput("", "", "tool_calls", toolCalls)
	want := map[string]interface{}{
		"content":       "",
		"tool_calls":    toolCalls,
		"finish_reason": "tool_calls",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("output without reasoning = %#v; want %#v", got, want)
	}

	got = buildLangfuseGenerationOutput("answer", "thinking", "stop", nil)
	want = map[string]interface{}{
		"content":           "answer",
		"tool_calls":        []types.LLMToolCall(nil),
		"finish_reason":     "stop",
		"reasoning_content": "thinking",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("output with reasoning = %#v; want %#v", got, want)
	}
}

func TestSnapshotLangfuseToolCallsKeepsModelArguments(t *testing.T) {
	providerCalls := []types.LLMToolCall{{
		ID:       "call_1",
		Function: types.FunctionCall{Name: "wiki_read_page", Arguments: `{"slugs":["res://0001"]}`},
	}}
	snapshot := snapshotLangfuseToolCalls(providerCalls)
	providerCalls[0].Function.Arguments = `{"slugs":["summary/uuid"]}`

	if got := snapshot[0].Function.Arguments; got != `{"slugs":["res://0001"]}` {
		t.Fatalf("Langfuse snapshot was mutated to %s", got)
	}
}

func TestBuildLangfuseMessagesReasoningContent(t *testing.T) {
	msgs := buildLangfuseMessages([]Message{
		{Role: "assistant", ReasoningContent: "chain of thought", ToolCalls: []ToolCall{{ID: "tc1"}}},
	})
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d; want 1", len(msgs))
	}
	if msgs[0]["reasoning_content"] != "chain of thought" {
		t.Fatalf("reasoning_content = %v; want chain of thought", msgs[0]["reasoning_content"])
	}
}

func TestConvertUsageIncludesPromptCacheCounters(t *testing.T) {
	got := convertUsage(&types.TokenUsage{
		PromptTokens: 1000, CompletionTokens: 50, TotalTokens: 1050,
		CacheReadTokens: 800, CacheWriteTokens: 100, CacheMissTokens: 200,
	})
	if got == nil {
		t.Fatal("convertUsage returned nil")
	}
	if got.CacheRead != 800 || got.CacheWrite != 100 || got.CacheMiss != 200 {
		t.Fatalf("cache usage = read:%d write:%d miss:%d", got.CacheRead, got.CacheWrite, got.CacheMiss)
	}
}

func TestLangfuseChatStreamRecordsProviderErrorAsFailed(t *testing.T) {
	recorded := installLangfuseUsageRecorder(t)
	ctx := langfuse.WithKnowledgeTraceContext(context.Background(), langfuse.KnowledgeTraceContext{
		KnowledgeID: "knowledge-1",
		Attempt:     1,
		Stage:       "postprocess.wiki.extract",
		TaskType:    types.TypeWikiIngest,
	})
	wrapped := &langfuseChat{inner: &langfuseStreamTestChat{events: []types.StreamResponse{
		{ResponseType: types.ResponseTypeAnswer, Content: "partial"},
		{ResponseType: types.ResponseTypeError, Content: "Upstream HTTP/2 stream failed", Done: true},
	}}}

	stream, err := wrapped.ChatStream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("ChatStream returned setup error: %v", err)
	}
	for range stream {
	}
	started := <-recorded
	if started.Status != types.SpanStatusRunning {
		t.Fatalf("generation start status = %q; want running", started.Status)
	}

	select {
	case usage := <-recorded:
		if usage.Name != "chat.response.stream" {
			t.Fatalf("generation name = %q; want endpoint-neutral chat.response.stream", usage.Name)
		}
		if usage.Status != types.SpanStatusFailed {
			t.Fatalf("status = %q; want failed", usage.Status)
		}
		if usage.ErrorMessage != "Upstream HTTP/2 stream failed" {
			t.Fatalf("error = %q; want provider stream error", usage.ErrorMessage)
		}
		if usage.UsageAvailable {
			t.Fatal("failed stream without provider usage must remain unavailable")
		}
		if got := usage.Output["content"]; got != "partial" {
			t.Fatalf("diagnostic partial output = %#v; want partial", got)
		}
	case <-time.After(time.Second):
		t.Fatal("knowledge usage was not recorded")
	}
}

func TestLangfuseChatStreamNilChannelRecordsFailureWithoutOutput(t *testing.T) {
	recorded := installLangfuseUsageRecorder(t)
	ctx := langfuse.WithKnowledgeTraceContext(context.Background(), langfuse.KnowledgeTraceContext{
		KnowledgeID: "knowledge-nil-stream",
		Attempt:     1,
		Stage:       "postprocess.wiki.extract",
		TaskType:    types.TypeWikiIngest,
	})
	wrapped := &langfuseChat{inner: &langfuseStreamTestChat{nilStream: true}}

	stream, err := wrapped.ChatStream(ctx, nil, nil)
	if err == nil {
		t.Fatal("ChatStream returned nil error for a nil provider stream")
	}
	if err.Error() != "provider returned a nil stream channel" {
		t.Fatalf("ChatStream error = %q; want nil stream diagnostic", err)
	}
	if stream != nil {
		t.Fatal("nil provider stream should remain nil")
	}
	<-recorded // generation start

	select {
	case usage := <-recorded:
		if usage.Status != types.SpanStatusFailed {
			t.Fatalf("status = %q; want failed", usage.Status)
		}
		if usage.ErrorMessage != err.Error() {
			t.Fatalf("observation error = %q; want returned error %q", usage.ErrorMessage, err)
		}
		if usage.Output != nil {
			t.Fatalf("output = %#v; want nil", usage.Output)
		}
		if usage.UsageAvailable {
			t.Fatal("nil stream must not synthesize token usage")
		}
	case <-time.After(time.Second):
		t.Fatal("knowledge usage was not recorded")
	}
}

func TestLangfuseChatStreamCloseWithoutSuccessfulDoneRecordsFailureWithoutOutput(t *testing.T) {
	recorded := installLangfuseUsageRecorder(t)
	ctx := langfuse.WithKnowledgeTraceContext(context.Background(), langfuse.KnowledgeTraceContext{
		KnowledgeID: "knowledge-bare-close",
		Attempt:     1,
		Stage:       "postprocess.wiki.extract",
		TaskType:    types.TypeWikiIngest,
	})
	wrapped := &langfuseChat{inner: &langfuseStreamTestChat{}}

	stream, err := wrapped.ChatStream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("ChatStream returned setup error: %v", err)
	}
	for range stream {
	}
	<-recorded // generation start

	select {
	case usage := <-recorded:
		if usage.Status != types.SpanStatusFailed {
			t.Fatalf("status = %q; want failed", usage.Status)
		}
		if usage.ErrorMessage != "provider stream closed without a successful terminal event" {
			t.Fatalf("error = %q; want missing terminal diagnostic", usage.ErrorMessage)
		}
		if usage.Output != nil {
			t.Fatalf("output = %#v; want nil instead of an empty response shell", usage.Output)
		}
		if usage.UsageAvailable {
			t.Fatal("closed stream without usage must remain unavailable")
		}
	case <-time.After(time.Second):
		t.Fatal("knowledge usage was not recorded")
	}
}

func TestLangfuseChatStreamErrorWithoutPartialRecordsFailureWithoutOutput(t *testing.T) {
	recorded := installLangfuseUsageRecorder(t)
	ctx := langfuse.WithKnowledgeTraceContext(context.Background(), langfuse.KnowledgeTraceContext{
		KnowledgeID: "knowledge-error-only",
		Attempt:     1,
		Stage:       "postprocess.wiki.extract",
		TaskType:    types.TypeWikiIngest,
	})
	wrapped := &langfuseChat{inner: &langfuseStreamTestChat{events: []types.StreamResponse{
		{ResponseType: types.ResponseTypeError, Content: "provider failed before output", Done: true},
	}}}

	stream, err := wrapped.ChatStream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("ChatStream returned setup error: %v", err)
	}
	for range stream {
	}
	<-recorded // generation start

	select {
	case usage := <-recorded:
		if usage.Status != types.SpanStatusFailed {
			t.Fatalf("status = %q; want failed", usage.Status)
		}
		if usage.ErrorMessage != "provider failed before output" {
			t.Fatalf("error = %q; want provider diagnostic", usage.ErrorMessage)
		}
		if usage.Output != nil {
			t.Fatalf("output = %#v; want nil instead of an empty response shell", usage.Output)
		}
		if usage.UsageAvailable {
			t.Fatal("failed stream without provider usage must remain unavailable")
		}
	case <-time.After(time.Second):
		t.Fatal("knowledge usage was not recorded")
	}
}

func TestLangfuseChatStreamSuccessWithoutUsageStaysDoneAndUnavailable(t *testing.T) {
	recorded := installLangfuseUsageRecorder(t)
	ctx := langfuse.WithKnowledgeTraceContext(context.Background(), langfuse.KnowledgeTraceContext{
		KnowledgeID: "knowledge-2",
		Attempt:     1,
		Stage:       "postprocess.wiki.summary",
		TaskType:    types.TypeWikiIngest,
	})
	wrapped := &langfuseChat{inner: &langfuseStreamTestChat{events: []types.StreamResponse{
		{ResponseType: types.ResponseTypeAnswer, Content: "complete", FinishReason: "stop", Done: true},
	}}}

	stream, err := wrapped.ChatStream(ctx, nil, nil)
	if err != nil {
		t.Fatalf("ChatStream returned setup error: %v", err)
	}
	for range stream {
	}
	started := <-recorded
	if started.Status != types.SpanStatusRunning {
		t.Fatalf("generation start status = %q; want running", started.Status)
	}

	select {
	case usage := <-recorded:
		if usage.Name != "chat.response.stream" {
			t.Fatalf("generation name = %q; want endpoint-neutral chat.response.stream", usage.Name)
		}
		if usage.Status != types.SpanStatusDone {
			t.Fatalf("status = %q; want done", usage.Status)
		}
		if usage.ErrorMessage != "" {
			t.Fatalf("unexpected error: %q", usage.ErrorMessage)
		}
		if usage.UsageAvailable {
			t.Fatal("successful stream without provider usage must remain unavailable")
		}
	case <-time.After(time.Second):
		t.Fatal("knowledge usage was not recorded")
	}
}
