package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpanTracker_OpenAttempt_CancelsOpenSpansFromOlderAttempts(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	root1, attempt1, err := tracker.OpenAttempt(ctx, "kid-stale-wiki", "trace-1")
	require.NoError(t, err)
	require.NotNil(t, root1)
	post := tracker.BeginStage(ctx, "kid-stale-wiki", attempt1, types.StagePostProcess, nil)
	require.NotNil(t, post)
	tracker.EndSpan(ctx, post, nil)

	wiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	require.NotNil(t, wiki)
	extract := tracker.BeginSubSpan(ctx, wiki, "postprocess.wiki.extract", types.SpanKindSubSpan, nil)
	require.NotNil(t, extract)

	_, attempt2, err := tracker.OpenAttempt(ctx, "kid-stale-wiki", "trace-2")
	require.NoError(t, err)
	require.Equal(t, 2, attempt2)

	type row struct {
		Name, Status, ErrorCode string
		FinishedAt              *time.Time
	}
	var rows []row
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("name, status, error_code, finished_at").
		Where("knowledge_id = ? AND attempt = ? AND span_id IN ?",
			"kid-stale-wiki", attempt1, []string{wiki.SpanID, extract.SpanID}).
		Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	for _, got := range rows {
		assert.Equal(t, types.SpanStatusCancelled, got.Status, got.Name)
		assert.Equal(t, "ATTEMPT_SUPERSEDED", got.ErrorCode, got.Name)
		assert.NotNil(t, got.FinishedAt, got.Name)
	}
}

func TestSpanTracker_EndSpan_PersistsAfterTaskContextCancellation(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid-cancelled-context", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-cancelled-context", attempt, types.StagePostProcess, nil)
	require.NotNil(t, post)
	wiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	require.NotNil(t, wiki)

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	tracker.EndSpan(cancelledCtx, wiki, types.JSONMap{"pages_written": 3})

	var got types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			"kid-cancelled-context", attempt, wiki.SpanID).
		First(&got).Error)
	assert.Equal(t, types.SpanStatusDone, got.Status)
	assert.NotNil(t, got.FinishedAt)
	assert.EqualValues(t, 3, got.Output["pages_written"])
}

func TestSpanTracker_FailSpan_PersistsAfterTaskContextCancellation(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-wiki-fail-cancelled", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-wiki-fail-cancelled", attempt, types.StagePostProcess, nil)
	wiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	extract := tracker.BeginSubSpan(ctx, wiki, "postprocess.wiki.extract", types.SpanKindSubSpan, nil)

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	tracker.FailSpan(cancelledCtx, extract, "EXTRACT_FAILED", "upstream stream failed", errors.New("boom"))

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").Where("span_id = ?", extract.SpanID).First(&row).Error)
	assert.Equal(t, types.SpanStatusFailed, row.Status)
	assert.Equal(t, "EXTRACT_FAILED", row.ErrorCode)
	assert.NotNil(t, row.FinishedAt)
}

func TestSpanTracker_SkipSpan_PersistsAfterTaskContextCancellation(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-wiki-skip-cancelled", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-wiki-skip-cancelled", attempt, types.StagePostProcess, nil)
	wiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	tracker.SkipSpan(cancelledCtx, wiki, "no_chunks")

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").Where("span_id = ?", wiki.SpanID).First(&row).Error)
	assert.Equal(t, types.SpanStatusSkipped, row.Status)
	assert.Equal(t, "no_chunks", row.ErrorMessage)
	assert.NotNil(t, row.FinishedAt)
}
