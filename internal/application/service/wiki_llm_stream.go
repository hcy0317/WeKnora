package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

type wikiStreamOutputContract uint8

const (
	wikiStreamOutputNone wikiStreamOutputContract = iota
	wikiStreamOutputSummaryMarkdown
	wikiStreamOutputJSON
)

func wikiStreamContract(purpose string) wikiStreamOutputContract {
	switch strings.TrimSpace(purpose) {
	case "wiki_summary", "wiki_page_modify":
		return wikiStreamOutputSummaryMarkdown
	case "wiki_candidate_slug", "wiki_knowledge_extract", "wiki_chunk_citation", "wiki_taxonomy_plan", "wiki_deduplication":
		return wikiStreamOutputJSON
	default:
		return wikiStreamOutputNone
	}
}

func shouldStreamWikiLLM(purpose string) bool {
	return wikiStreamContract(purpose) != wikiStreamOutputNone
}

// callWikiLLM streams Wiki stages whose output can grow with the source
// document or ingest batch. The stream is accumulated entirely in memory and
// returned only after Done, finish_reason=stop, and output-contract validation,
// so callers keep the same atomic persistence boundary as non-streaming Chat.
// Short index outputs and unknown prompt purposes remain non-streaming.
func callWikiLLM(
	ctx context.Context,
	chatModel chat.Chat,
	messages []chat.Message,
	opts *chat.ChatOptions,
	purpose string,
) (*types.ChatResponse, error) {
	if !shouldStreamWikiLLM(purpose) {
		return chatModel.Chat(ctx, messages, opts)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := chatModel.ChatStream(streamCtx, messages, opts)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, fmt.Errorf("wiki LLM stream is nil")
	}

	var (
		content      strings.Builder
		finishReason string
		completed    bool
		usage        types.TokenUsage
	)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if !completed {
					return nil, fmt.Errorf("wiki LLM stream closed before completion")
				}
				if finishReason != "stop" {
					return nil, fmt.Errorf("wiki LLM stream ended with finish reason %q", finishReason)
				}
				if content.Len() == 0 {
					return nil, fmt.Errorf("wiki LLM stream completed without answer content")
				}
				if err := validateWikiStreamContent(purpose, content.String()); err != nil {
					return nil, err
				}
				return &types.ChatResponse{
					Content:      content.String(),
					FinishReason: finishReason,
					Usage:        usage,
				}, nil
			}

			if event.ResponseType == types.ResponseTypeError {
				detail := strings.TrimSpace(event.Content)
				if detail == "" {
					detail = "provider returned an empty stream error"
				}
				return nil, fmt.Errorf("wiki LLM stream failed: %s", detail)
			}
			if len(event.ToolCalls) > 0 || event.ResponseType == types.ResponseTypeToolCall {
				return nil, fmt.Errorf("wiki LLM stream returned an unexpected tool call")
			}
			if event.ResponseType == types.ResponseTypeAnswer && event.Content != "" {
				content.WriteString(event.Content)
			}
			if event.Usage != nil {
				usage = *event.Usage
			}
			if event.FinishReason != "" {
				finishReason = event.FinishReason
				if finishReason != "stop" {
					return nil, fmt.Errorf("wiki LLM stream ended with finish reason %q", finishReason)
				}
			}
			if event.Done && finishReason == "stop" {
				completed = true
			}
		}
	}
}

func validateWikiStreamContent(purpose, content string) error {
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("wiki LLM stream completed without answer content")
	}
	switch wikiStreamContract(purpose) {
	case wikiStreamOutputNone:
		return nil
	case wikiStreamOutputSummaryMarkdown:
		summary, body := splitSummaryLine(content)
		if strings.TrimSpace(summary) == "" {
			return fmt.Errorf("wiki LLM stream content validation failed: missing SUMMARY line")
		}
		if strings.TrimSpace(body) == "" {
			return fmt.Errorf("wiki LLM stream content validation failed: missing Markdown body")
		}
		return nil
	case wikiStreamOutputJSON:
		// Existing Wiki consumers tolerate fenced JSON and sanitized literal
		// control characters through cleanLLMJSON; validate against that same
		// normalized representation, while returning the original content for
		// the caller's unchanged parsing path.
		cleaned := cleanLLMJSON(content)
		if !json.Valid([]byte(cleaned)) {
			return fmt.Errorf("wiki LLM stream content validation failed: invalid JSON")
		}
		if err := validateWikiJSONContract(purpose, []byte(cleaned)); err != nil {
			return fmt.Errorf("wiki LLM stream content validation failed: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("wiki LLM stream content validation failed: unknown output contract")
	}
}

func validateWikiJSONContract(purpose string, content []byte) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(content, &root); err != nil || root == nil {
		return fmt.Errorf("expected JSON object")
	}
	require := func(key string, target any) error {
		raw, ok := root[key]
		if !ok || string(raw) == "null" {
			return fmt.Errorf("missing required %q field", key)
		}
		if err := json.Unmarshal(raw, target); err != nil {
			return fmt.Errorf("invalid %q field: %w", key, err)
		}
		return nil
	}

	switch purpose {
	case "wiki_candidate_slug", "wiki_knowledge_extract":
		var entities, concepts []extractedItem
		if err := require("entities", &entities); err != nil {
			return err
		}
		if err := require("concepts", &concepts); err != nil {
			return err
		}
		for _, item := range append(entities, concepts...) {
			if strings.TrimSpace(item.Name) == "" || strings.TrimSpace(item.Slug) == "" ||
				strings.TrimSpace(item.Description) == "" || strings.TrimSpace(item.Details) == "" || item.Aliases == nil {
				return fmt.Errorf("entity/concept item is missing required fields")
			}
		}
		return nil
	case "wiki_chunk_citation":
		var citations map[string][]string
		var newSlugs []newSlugFromCitation
		if err := require("citations", &citations); err != nil {
			return err
		}
		if err := require("new_slugs", &newSlugs); err != nil {
			return err
		}
		for _, item := range newSlugs {
			if (item.Type != "entity" && item.Type != "concept") || strings.TrimSpace(item.Name) == "" ||
				strings.TrimSpace(item.Slug) == "" || strings.TrimSpace(item.Description) == "" ||
				strings.TrimSpace(item.Details) == "" || item.Aliases == nil || item.SourceChunks == nil {
				return fmt.Errorf("new_slugs item is missing required fields")
			}
		}
		return nil
	case "wiki_taxonomy_plan":
		var assignments []struct {
			Slug string   `json:"slug"`
			Path []string `json:"path"`
		}
		if err := require("assignments", &assignments); err != nil {
			return err
		}
		for _, assignment := range assignments {
			if strings.TrimSpace(assignment.Slug) == "" || assignment.Path == nil {
				return fmt.Errorf("assignment is missing required fields")
			}
		}
		return nil
	case "wiki_deduplication":
		var merges map[string]string
		return require("merges", &merges)
	default:
		return fmt.Errorf("unknown JSON output contract")
	}
}
