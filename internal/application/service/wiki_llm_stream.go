package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/openaiapi"
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
	return callWikiLLMWithFallbacks(ctx, chatModel, messages, opts, purpose, "", "")
}

func callWikiLLMWithFallbacks(
	ctx context.Context,
	chatModel chat.Chat,
	messages []chat.Message,
	opts *chat.ChatOptions,
	purpose string,
	existingSummary string,
	pageTitle string,
) (*types.ChatResponse, error) {
	if !shouldStreamWikiLLM(purpose) {
		response, err := chatModel.Chat(ctx, messages, opts)
		if err != nil {
			return nil, classifyWikiGenerationError(ctx, err)
		}
		return response, nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := chatModel.ChatStream(streamCtx, messages, opts)
	if err != nil {
		return nil, classifyWikiGenerationError(ctx, err)
	}
	if stream == nil {
		return nil, newWikiGenerationError(
			WikiGenerationErrorDeterministicOutput,
			fmt.Errorf("wiki LLM stream is nil"),
		)
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
			return nil, newWikiGenerationError(WikiGenerationErrorCancelled, ctx.Err())
		case event, ok := <-stream:
			if !ok {
				if !completed {
					return nil, newWikiGenerationError(
						WikiGenerationErrorDeterministicOutput,
						fmt.Errorf("wiki LLM stream closed before completion"),
					)
				}
				if finishReason != "stop" {
					return nil, newWikiGenerationError(
						WikiGenerationErrorDeterministicOutput,
						fmt.Errorf("wiki LLM stream ended with finish reason %q", finishReason),
					)
				}
				if content.Len() == 0 {
					return nil, newWikiGenerationError(
						WikiGenerationErrorDeterministicOutput,
						fmt.Errorf("wiki LLM stream completed without answer content"),
					)
				}
				normalizedContent, err := normalizeWikiStreamContent(
					purpose, content.String(), existingSummary, pageTitle,
				)
				if err != nil {
					return nil, err
				}
				return &types.ChatResponse{
					Content:      normalizedContent,
					FinishReason: finishReason,
					Usage:        usage,
				}, nil
			}

			if event.ResponseType == types.ResponseTypeError {
				detail := strings.TrimSpace(event.Content)
				if detail == "" {
					detail = "provider returned an empty stream error"
				}
				streamErr := fmt.Errorf("wiki LLM stream failed: %s", detail)
				if _, ok := types.StreamErrorDetailsFromData(event.Data); ok {
					streamErr = newWikiProviderStreamError(streamErr, event.Data)
				} else if status, ok := wikiStreamHTTPStatus(event.Data); ok {
					streamErr = openaiapi.NewProtocolHTTPError(openaiapi.ProtocolResponses, status, detail)
				}
				return nil, classifyWikiGenerationError(ctx, streamErr)
			}
			if len(event.ToolCalls) > 0 || event.ResponseType == types.ResponseTypeToolCall {
				return nil, newWikiGenerationError(
					WikiGenerationErrorDeterministicOutput,
					fmt.Errorf("wiki LLM stream returned an unexpected tool call"),
				)
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
					return nil, newWikiGenerationError(
						WikiGenerationErrorDeterministicOutput,
						fmt.Errorf("wiki LLM stream ended with finish reason %q", finishReason),
					)
				}
			}
			if event.Done && finishReason == "stop" {
				completed = true
			}
		}
	}
}

func wikiStreamHTTPStatus(data map[string]interface{}) (int, bool) {
	if data == nil {
		return 0, false
	}
	value, ok := data["http_status"]
	if !ok {
		return 0, false
	}
	switch status := value.(type) {
	case int:
		return status, status > 0
	case int64:
		return int(status), status > 0
	case float64:
		return int(status), status > 0 && status == float64(int(status))
	case json.Number:
		parsed, err := strconv.Atoi(status.String())
		return parsed, err == nil && parsed > 0
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(status))
		return parsed, err == nil && parsed > 0
	default:
		return 0, false
	}
}

func normalizeWikiStreamContent(purpose, content, existingSummary, pageTitle string) (string, error) {
	if strings.TrimSpace(content) == "" {
		return "", newWikiGenerationError(
			WikiGenerationErrorDeterministicOutput,
			fmt.Errorf("wiki LLM stream completed without answer content"),
		)
	}
	switch wikiStreamContract(purpose) {
	case wikiStreamOutputNone:
		return content, nil
	case wikiStreamOutputSummaryMarkdown:
		summary, body := splitSummaryLine(content)
		if strings.TrimSpace(summary) != "" {
			if strings.TrimSpace(body) == "" {
				return "", newWikiGenerationError(
					WikiGenerationErrorDeterministicOutput,
					fmt.Errorf("wiki LLM stream content validation failed: missing Markdown body"),
				)
			}
			return content, nil
		}
		body = strings.TrimSpace(content)
		fallback := strings.TrimSpace(existingSummary)
		if fallback == "" {
			fallback = firstMeaningfulWikiParagraph(body)
		}
		if fallback == "" {
			fallback = strings.TrimSpace(pageTitle)
		}
		if fallback == "" || !hasUsableWikiMarkdownBody(body) {
			return "", newWikiGenerationError(
				WikiGenerationErrorDeterministicOutput,
				fmt.Errorf("wiki LLM stream content validation failed: missing SUMMARY line and stable fallback"),
			)
		}
		return "SUMMARY: " + fallback + "\n\n" + body, nil
	case wikiStreamOutputJSON:
		normalized, err := normalizeWikiJSONContent(purpose, content)
		if err != nil {
			return "", newWikiGenerationError(
				WikiGenerationErrorDeterministicOutput,
				fmt.Errorf("wiki LLM stream content validation failed: %w", err),
			)
		}
		return normalized, nil
	default:
		return "", newWikiGenerationError(
			WikiGenerationErrorDeterministicOutput,
			fmt.Errorf("wiki LLM stream content validation failed: unknown output contract"),
		)
	}
}

func firstMeaningfulWikiParagraph(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	inFence := false
	paragraph := make([]string, 0, 2)
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if line == "" {
			if len(paragraph) > 0 {
				return strings.Join(paragraph, " ")
			}
			continue
		}
		if isWikiNonProseLine(line) {
			if len(paragraph) > 0 {
				return strings.Join(paragraph, " ")
			}
			continue
		}
		paragraph = append(paragraph, line)
	}
	return strings.Join(paragraph, " ")
}

func hasUsableWikiMarkdownBody(content string) bool {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return false
	}
	if firstMeaningfulWikiParagraph(trimmed) != "" {
		return true
	}
	for _, rawLine := range strings.Split(strings.ReplaceAll(trimmed, "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(rawLine)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") || strings.HasPrefix(line, "+ ") {
			return true
		}
	}
	return false
}

func isWikiNonProseLine(line string) bool {
	if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ">") ||
		strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") || strings.HasPrefix(line, "+ ") {
		return true
	}
	if len(line) >= 3 && line[0] >= '0' && line[0] <= '9' {
		if dot := strings.IndexByte(line, '.'); dot > 0 && dot < 5 {
			return true
		}
	}
	if strings.Count(line, "|") >= 2 {
		return true
	}
	withoutSeparators := strings.NewReplacer("-", "", ":", "", "|", "", " ", "").Replace(line)
	return withoutSeparators == "" && strings.Contains(line, "-")
}

func normalizeWikiJSONContent(purpose, content string) (string, error) {
	cleaned := cleanLLMJSON(content)
	if json.Valid([]byte(cleaned)) {
		if err := validateWikiJSONContract(purpose, []byte(cleaned)); err != nil {
			return "", err
		}
		return cleaned, nil
	}

	var contractErr error
	for start := 0; start < len(content); start++ {
		if content[start] != '{' {
			continue
		}
		end, ok := findWikiJSONObjectEnd(content, start)
		if !ok {
			continue
		}
		candidate := sanitizeJSONString(strings.TrimSpace(content[start : end+1]))
		if !json.Valid([]byte(candidate)) {
			continue
		}
		if err := validateWikiJSONContract(purpose, []byte(candidate)); err != nil {
			if contractErr == nil {
				contractErr = err
			}
			continue
		}
		return candidate, nil
	}
	if contractErr != nil {
		return "", contractErr
	}
	return "", fmt.Errorf("invalid JSON")
}

func findWikiJSONObjectEnd(content string, start int) (int, bool) {
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(content); i++ {
		ch := content[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch ch {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, true
			}
			if depth < 0 {
				return 0, false
			}
		}
	}
	return 0, false
}

func wikiJSONOutputSchema(purpose string) json.RawMessage {
	switch purpose {
	case "wiki_candidate_slug", "wiki_knowledge_extract":
		return json.RawMessage(`{"type":"object","required":["entities","concepts"],"properties":{"entities":{"type":"array"},"concepts":{"type":"array"}}}`)
	case "wiki_chunk_citation":
		return json.RawMessage(`{"type":"object","required":["citations","new_slugs"],"properties":{"citations":{"type":"object"},"new_slugs":{"type":"array"}}}`)
	case "wiki_taxonomy_plan":
		return json.RawMessage(`{"type":"object","required":["assignments"],"properties":{"assignments":{"type":"array"}}}`)
	case "wiki_deduplication":
		return json.RawMessage(`{"type":"object","required":["merges"],"properties":{"merges":{"type":"object"}}}`)
	default:
		return nil
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
