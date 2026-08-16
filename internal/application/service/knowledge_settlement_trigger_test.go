package service

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
)

type failingFinalizeKnowledgeRepo struct {
	interfaces.KnowledgeRepository
}

type promotedFinalizeKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	promoted bool
}

func (r promotedFinalizeKnowledgeRepo) FinalizeSubtask(context.Context, string) (int, bool, error) {
	return 0, r.promoted, nil
}

func (r promotedFinalizeKnowledgeRepo) FinalizeSubtaskForAttempt(context.Context, string, int) (int, bool, error) {
	return 0, r.promoted, nil
}

type retryingSettlementSpanRepo struct {
	repository.KnowledgeSpanRepository
	failures int
	calls    int
}

func (r *retryingSettlementSpanRepo) SettleProcessingOutcome(context.Context, string, int) error {
	r.calls++
	if r.calls <= r.failures {
		return errors.New("transient settlement failure")
	}
	return nil
}

func (failingFinalizeKnowledgeRepo) FinalizeSubtask(context.Context, string) (int, bool, error) {
	return 0, false, errors.New("observer counter unavailable")
}

func (failingFinalizeKnowledgeRepo) FinalizeSubtaskForAttempt(context.Context, string, int) (int, bool, error) {
	return 0, false, errors.New("observer counter unavailable")
}

func TestSpanTrackerSettlementRetriesTransientRepositoryErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		failures int
		settle   func(SpanTracker)
	}{
		{name: "question first attempt fails", failures: 1, settle: func(tracker SpanTracker) {
			tracker.SettleQuestionGroup(context.Background(), "kid-retry", 1)
		}},
		{name: "post first two attempts fail", failures: 2, settle: func(tracker SpanTracker) {
			tracker.SettlePostProcessTree(context.Background(), "kid-retry", 1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &retryingSettlementSpanRepo{failures: test.failures}
			test.settle(NewSpanTracker(repo, nil))
			assert.Equal(t, test.failures+1, repo.calls)
		})
	}
}

func TestFinalizeSubtaskDetachedCounterFailureStillRequestsSettlement(t *testing.T) {
	shouldSettle := finalizeSubtaskDetached(
		context.Background(), failingFinalizeKnowledgeRepo{}, "kid-counter-error", 7, "summary",
		nil, false, true,
	)
	assert.True(t, shouldSettle,
		"a terminal child must request authoritative reduction even when the observer counter write fails")
}

func TestFinalizeSubtaskDetachedIgnoresLegacyPromotionResult(t *testing.T) {
	for _, promoted := range []bool{false, true} {
		t.Run(map[bool]string{false: "not promoted", true: "promoted"}[promoted], func(t *testing.T) {
			assert.True(t, finalizeSubtaskDetached(
				context.Background(), promotedFinalizeKnowledgeRepo{promoted: promoted},
				"kid-promotion-independent", 7, "terminal", nil, false, true,
			))
		})
	}
}

func TestTerminalCallsiteMatrixRequestsAuthoritativeReducer(t *testing.T) {
	for _, test := range []struct {
		file    string
		anchors []string
	}{
		{file: "extract.go", anchors: []string{"graph_chunk[%d]", "SettlePostProcessTree(ctx, p.KnowledgeID, p.Attempt)"}},
		{file: "knowledge_process.go", anchors: []string{
			`payload.KnowledgeID, payload.Attempt, "summary"`,
			`payload.KnowledgeID, payload.Attempt, "question_legacy"`,
			`question_batch[%d]`, "SettleQuestionGroup(terminalCtx", "SettlePostProcessTree(ctx, payload.KnowledgeID",
		}},
		{file: "wiki_ingest.go", anchors: []string{
			"s.finalizeWikiSubtask(ctx,", "settler.SettleWikiPendingOpStrict(",
		}},
		{file: "knowledge_post_process.go", anchors: []string{
			"Every fan-out terminal transition runs the repository reducer",
			"SettlePostProcessTree(ctx, payload.KnowledgeID, attempt)",
		}},
	} {
		t.Run(test.file, func(t *testing.T) {
			source, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			text := string(source)
			for _, anchor := range test.anchors {
				assert.True(t, strings.Contains(text, anchor), "missing terminal settlement anchor %q", anchor)
			}
			assert.NotContains(t, text, "promotedByShortfallRelease")
		})
	}
}
