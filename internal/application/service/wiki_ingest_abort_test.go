package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wikiAbortPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	interfaces.TaskPendingOpsClaimLease
	failCount int
	order     []string
}

func (r *wikiAbortPendingRepo) IncrClaimFailCount(
	_ context.Context, _ int64, _ types.TaskClaimOwner,
) (int, error) {
	r.order = append(r.order, "increment")
	return r.failCount, nil
}

func (r *wikiAbortPendingRepo) ReleaseClaims(
	_ context.Context, _ []int64, _ types.TaskClaimOwner,
) error {
	r.order = append(r.order, "release")
	return nil
}

type wikiAbortSpanTracker struct {
	noopSpanTracker
	span       *Span
	latest     int
	order      *[]string
	settled    int
	deadLetter *types.TaskDeadLetter
}

func (t *wikiAbortSpanTracker) LookupSpanByNameStrict(
	_ context.Context, _ string, _ int, _ string,
) (*Span, error) {
	*t.order = append(*t.order, "lookup")
	return t.span, nil
}

func (t *wikiAbortSpanTracker) LatestAttemptStrict(context.Context, string) (int, error) {
	return t.latest, nil
}

func (t *wikiAbortSpanTracker) FailSpan(
	_ context.Context, span *Span, _, _ string, _ error,
) {
	*t.order = append(*t.order, "fail")
	span.Status = types.SpanStatusFailed
}

func (t *wikiAbortSpanTracker) SettleWikiPendingOpStrict(
	_ context.Context,
	_ string,
	_ int,
	_ []int64,
	deadLetter *types.TaskDeadLetter,
	_ *types.TaskClaimOwner,
) error {
	*t.order = append(*t.order, "settle")
	t.settled++
	t.deadLetter = deadLetter
	return nil
}

func newWikiAbortFixture(status string, failCount int) (
	*wikiIngestService, *wikiAbortPendingRepo, *wikiAbortSpanTracker, WikiPendingOp,
) {
	owner := &types.TaskClaimOwner{Token: "claim-token", TaskID: "task-id"}
	repo := &wikiAbortPendingRepo{failCount: failCount}
	tracker := &wikiAbortSpanTracker{
		span: &Span{
			KnowledgeID: "knowledge-1",
			Attempt:     4,
			SpanID:      "wiki-span",
			Name:        "postprocess.wiki",
			Kind:        types.SpanKindSubSpan,
			Status:      status,
			StartedAt:   time.Now(),
		},
		latest: 4,
		order:  &repo.order,
	}
	op := WikiPendingOp{
		Op:          WikiOpIngest,
		KnowledgeID: "knowledge-1",
		Attempt:     4,
		dbID:        91,
		dbIDs:       []int64{91},
		claimOwner:  owner,
	}
	return &wikiIngestService{pendingRepo: repo, spanTracker: tracker}, repo, tracker, op
}

func TestSettleAbortedWikiBatchFailsAndRequeuesRunningOwner(t *testing.T) {
	svc, repo, tracker, op := newWikiAbortFixture(types.SpanStatusRunning, 2)

	err := svc.settleAbortedWikiBatch(
		context.Background(), WikiIngestPayload{KnowledgeBaseID: "kb-1"},
		[]WikiPendingOp{op}, context.DeadlineExceeded,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"lookup", "fail", "increment", "release"}, repo.order)
	assert.Equal(t, types.SpanStatusFailed, tracker.span.Status)
	assert.Zero(t, tracker.settled)
}

func TestSettleAbortedWikiBatchAcknowledgesCompletedOwnerWithoutFailure(t *testing.T) {
	svc, repo, tracker, op := newWikiAbortFixture(types.SpanStatusDone, 0)

	err := svc.settleAbortedWikiBatch(
		context.Background(), WikiIngestPayload{KnowledgeBaseID: "kb-1"},
		[]WikiPendingOp{op}, context.Canceled,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"lookup", "settle"}, repo.order)
	assert.Equal(t, 1, tracker.settled)
	assert.Nil(t, tracker.deadLetter)
}

func TestSettleAbortedWikiBatchDeadLettersExhaustedOwner(t *testing.T) {
	svc, repo, tracker, op := newWikiAbortFixture(
		types.SpanStatusRunning, wikiMaxFailRetries+1,
	)

	err := svc.settleAbortedWikiBatch(
		context.Background(), WikiIngestPayload{TenantID: 7, KnowledgeBaseID: "kb-1"},
		[]WikiPendingOp{op}, context.DeadlineExceeded,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"lookup", "fail", "increment", "settle"}, repo.order)
	assert.Equal(t, 1, tracker.settled)
	require.NotNil(t, tracker.deadLetter)
	assert.Equal(t, op.KnowledgeID, tracker.deadLetter.RelatedID)
}

func TestWikiLLMAttemptBudgetReservesDurableTail(t *testing.T) {
	ctx, cancel := context.WithDeadline(
		context.Background(), time.Now().Add(wikiTaskSettlementReserve+time.Minute),
	)
	defer cancel()

	budget, err := wikiLLMAttemptBudget(ctx, wikiLLMMaxAttempts)

	require.NoError(t, err)
	assert.InDelta(t, (20 * time.Second).Seconds(), budget.Seconds(), 1)
}
