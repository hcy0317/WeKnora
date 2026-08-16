package chat

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// sendStreamResponse prevents a cancelled consumer from permanently blocking
// a provider or observability forwarding goroutine on an unbuffered channel.
func sendStreamResponse(
	ctx context.Context,
	ch chan<- types.StreamResponse,
	response types.StreamResponse,
) bool {
	select {
	case ch <- response:
		return true
	case <-ctx.Done():
		return false
	}
}

// thinkingEmitter owns the "reasoning then answer" hand-off that every
// streaming Chat implementation shares: thinking chunks are forwarded as they
// arrive, and exactly one thinking-done marker is emitted before the first
// answer token (or when the stream ends without one). Centralizing the
// bookkeeping keeps the OpenAI-compatible and Ollama stream loops in sync.
type thinkingEmitter struct {
	active bool
}

// emit forwards a reasoning chunk and records that a thinking-done marker is
// still owed.
func (e *thinkingEmitter) emit(ctx context.Context, ch chan types.StreamResponse, content string) bool {
	if !sendStreamResponse(ctx, ch, types.StreamResponse{
		ResponseType: types.ResponseTypeThinking,
		Content:      content,
		Done:         false,
	}) {
		return false
	}
	e.active = true
	return true
}

// finish emits the single thinking-done marker if one is owed. Safe to call
// multiple times; only the first call after an emit sends anything.
func (e *thinkingEmitter) finish(ctx context.Context, ch chan types.StreamResponse) bool {
	if !e.active {
		return true
	}
	if !sendStreamResponse(ctx, ch, types.StreamResponse{
		ResponseType: types.ResponseTypeThinking,
		Done:         true,
	}) {
		return false
	}
	e.active = false
	return true
}
