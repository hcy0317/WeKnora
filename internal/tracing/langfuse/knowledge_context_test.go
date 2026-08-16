package langfuse

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestResolveGenerationStageRoutesWikiPurposesBelowParent(t *testing.T) {
	info := KnowledgeTraceContext{
		KnowledgeID: "knowledge-1",
		Attempt:     1,
		Stage:       "postprocess.wiki",
		TaskType:    types.TypeWikiIngest,
	}
	tests := map[string]string{
		"wiki_candidate_slug":    "postprocess.wiki.extract",
		"wiki_knowledge_extract": "postprocess.wiki.extract",
		"wiki_deduplication":     "postprocess.wiki.extract",
		"wiki_summary":           "postprocess.wiki.summary",
		"wiki_chunk_citation":    "postprocess.wiki.classify",
	}
	for purpose, want := range tests {
		t.Run(purpose, func(t *testing.T) {
			if got := resolveGenerationStage(info, "chat.completion.stream", purpose); got != want {
				t.Fatalf("stage = %q; want %q", got, want)
			}
		})
	}
}

func TestWithKnowledgeGenerationStagePreservesCorrelationAndTargetsExactWikiPage(t *testing.T) {
	ctx := WithKnowledgeTraceContext(context.Background(), KnowledgeTraceContext{
		KnowledgeID: "knowledge-1",
		Attempt:     2,
		Stage:       "postprocess.wiki",
		TaskType:    types.TypeWikiIngest,
	})
	ctx = WithKnowledgeGenerationStage(ctx, "postprocess.wiki.page[entity/example]")

	info, ok := knowledgeTraceContextFromContext(ctx)
	if !ok {
		t.Fatal("knowledge trace context was lost")
	}
	if info.KnowledgeID != "knowledge-1" || info.Attempt != 2 || info.TaskType != types.TypeWikiIngest {
		t.Fatalf("correlation changed: %+v", info)
	}
	if got := resolveGenerationStage(info, "chat.completion.stream", "wiki_page_modify"); got != "postprocess.wiki.page[entity/example]" {
		t.Fatalf("stage = %q; want exact page subspan", got)
	}
}
