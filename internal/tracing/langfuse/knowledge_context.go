package langfuse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeTraceContext is attached by the asynq middleware when a task
// payload belongs to one permanent knowledge document. Model wrappers inherit
// it through context.Context, which makes every Generation independently
// searchable by document and processing stage in Langfuse.
type KnowledgeTraceContext struct {
	KnowledgeID string
	Attempt     int
	Stage       string
	TaskType    string
}

type knowledgeTraceContextKey struct{}

func withKnowledgeTraceContext(ctx context.Context, info KnowledgeTraceContext) context.Context {
	if info.KnowledgeID == "" {
		return ctx
	}
	return context.WithValue(ctx, knowledgeTraceContextKey{}, info)
}

// WithKnowledgeTraceContext scopes model generations to one document when a
// batch worker resolves the document only after dequeuing (Wiki ingest is the
// main example). It is observability-only and never changes task behavior.
func WithKnowledgeTraceContext(ctx context.Context, info KnowledgeTraceContext) context.Context {
	return withKnowledgeTraceContext(ctx, info)
}

// WithKnowledgeGenerationStage narrows an existing document-scoped context to
// the concrete processing subspan that owns the next model call. Correlation
// fields remain unchanged; only Stage is replaced. It is intentionally a no-op
// without an existing knowledge context so observability cannot invent a
// document association for KB-global Wiki work.
func WithKnowledgeGenerationStage(ctx context.Context, stage string) context.Context {
	info, ok := knowledgeTraceContextFromContext(ctx)
	if !ok || strings.TrimSpace(stage) == "" {
		return ctx
	}
	info.Stage = strings.TrimSpace(stage)
	return withKnowledgeTraceContext(ctx, info)
}

func knowledgeTraceContextFromContext(ctx context.Context) (KnowledgeTraceContext, bool) {
	if ctx == nil {
		return KnowledgeTraceContext{}, false
	}
	info, ok := ctx.Value(knowledgeTraceContextKey{}).(KnowledgeTraceContext)
	return info, ok && info.KnowledgeID != ""
}

// peekKnowledgeTraceContext extracts the small set of common correlation
// fields shared by document-processing payloads. Unknown payload fields are
// intentionally ignored so observability can never reject a real task.
func peekKnowledgeTraceContext(payload []byte, taskType string) KnowledgeTraceContext {
	if len(payload) == 0 {
		return KnowledgeTraceContext{}
	}
	var raw struct {
		KnowledgeID string `json:"knowledge_id"`
		Attempt     int    `json:"attempt"`
		BatchIndex  int    `json:"batch_index"`
		ChunkIndex  int    `json:"chunk_index"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil || raw.KnowledgeID == "" {
		return KnowledgeTraceContext{}
	}
	return KnowledgeTraceContext{
		KnowledgeID: raw.KnowledgeID,
		Attempt:     raw.Attempt,
		Stage:       stageForTask(taskType, raw.BatchIndex, raw.ChunkIndex),
		TaskType:    taskType,
	}
}

func stageForTask(taskType string, batchIndex, chunkIndex int) string {
	switch taskType {
	case types.TypeQuestionGeneration:
		return fmt.Sprintf("postprocess.question.batch[%d]", batchIndex)
	case types.TypeSummaryGeneration:
		return "postprocess.summary"
	case types.TypeChunkExtract:
		return fmt.Sprintf("postprocess.graph.chunk[%d]", chunkIndex)
	case types.TypeImageMultimodal:
		return "multimodal"
	case types.TypeWikiIngest:
		return "postprocess.wiki"
	case types.TypeDataTableSummary:
		return "docreader.datatable"
	case types.TypeKnowledgePostProcess:
		return "postprocess"
	default:
		// document:process and manual:process execute several stages in one
		// handler. The concrete model type/purpose resolves their stage later.
		return ""
	}
}

func resolveGenerationStage(info KnowledgeTraceContext, generationName, purpose string) string {
	if info.Stage != "" && info.Stage != "postprocess.wiki" {
		return info.Stage
	}
	if info.Stage == "postprocess.wiki" {
		switch purpose {
		case "wiki_candidate_slug", "wiki_knowledge_extract", "wiki_deduplication":
			return "postprocess.wiki.extract"
		case "wiki_summary":
			return "postprocess.wiki.summary"
		case "wiki_chunk_citation":
			return "postprocess.wiki.classify"
		case "wiki_page_modify":
			// The exact postprocess.wiki.page[slug] is supplied by the reduce
			// lane through WithKnowledgeGenerationStage. This fallback keeps
			// metadata below the structural parent if an older caller omitted it.
			return "postprocess.wiki.page"
		}
	}
	switch {
	case strings.HasPrefix(generationName, "embedding."):
		return types.StageEmbedding
	case strings.HasPrefix(generationName, "vlm."):
		return types.StageMultimodal
	case strings.HasPrefix(generationName, "asr."):
		return types.StageDocReader
	case purpose == "document_summary":
		return "postprocess.summary"
	case purpose == "question_generation":
		return "postprocess.question"
	case strings.Contains(purpose, "wiki"):
		return "postprocess.wiki"
	case strings.Contains(purpose, "graph") || strings.Contains(purpose, "entity_extract"):
		return "postprocess.graph"
	default:
		return "unattributed"
	}
}

func modelTypeFromGenerationName(name string) string {
	prefix, _, _ := strings.Cut(name, ".")
	if prefix == "chat" || prefix == "embedding" || prefix == "rerank" || prefix == "vlm" || prefix == "asr" {
		return prefix
	}
	return "model"
}

func generationUsageEstimated(modelType string) bool {
	switch modelType {
	case "embedding", "rerank", "vlm":
		return true
	default:
		return false
	}
}

func enrichGenerationMetadata(
	metadata map[string]interface{}, info KnowledgeTraceContext, generationName, purpose string,
) map[string]interface{} {
	out := make(map[string]interface{}, len(metadata)+4)
	for key, value := range metadata {
		out[key] = value
	}
	if info.KnowledgeID == "" {
		return out
	}
	out["knowledge_id"] = info.KnowledgeID
	if info.Attempt > 0 {
		out["knowledge_attempt"] = info.Attempt
	}
	out["processing_stage"] = resolveGenerationStage(info, generationName, purpose)
	if info.TaskType != "" {
		out["task_type"] = info.TaskType
	}
	return out
}

func knowledgeTraceMetadata(info KnowledgeTraceContext) map[string]interface{} {
	if info.KnowledgeID == "" {
		return nil
	}
	out := map[string]interface{}{
		"knowledge_id": info.KnowledgeID,
		"task_type":    info.TaskType,
	}
	if info.Attempt > 0 {
		out["knowledge_attempt"] = info.Attempt
	}
	if info.Stage != "" {
		out["processing_stage"] = info.Stage
	}
	return out
}
