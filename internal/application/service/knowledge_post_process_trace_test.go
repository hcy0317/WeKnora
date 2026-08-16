package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type questionBatchSelectiveEnqueuer struct {
	failBatch int
}

func (q *questionBatchSelectiveEnqueuer) Enqueue(
	task *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	var payload types.QuestionGenerationPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return nil, err
	}
	if payload.BatchIndex == q.failBatch {
		return nil, errors.New("queue unavailable")
	}
	return &asynq.TaskInfo{ID: "queued", Type: task.Type()}, nil
}

func TestFinishRunningMultimodalStage_PreservesSkippedStage(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid-multimodal-disabled", "")
	require.NoError(t, err)
	mm := tracker.BeginStage(ctx, "kid-multimodal-disabled", attempt, types.StageMultimodal, nil)
	require.NotNil(t, mm)
	tracker.SkipSpan(ctx, mm, "skipped")

	service := &KnowledgePostProcessService{spanTracker: tracker}
	service.finishRunningMultimodalStage(ctx, "kid-multimodal-disabled", attempt)

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?",
			"kid-multimodal-disabled", attempt, types.StageMultimodal).
		First(&row).Error)
	assert.Equal(t, types.SpanStatusSkipped, row.Status)
	assert.Equal(t, int64(0), row.DurationMs)
}

func TestFinishRunningMultimodalStage_CompletesRunningStage(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid-multimodal-enabled", "")
	require.NoError(t, err)
	mm := tracker.BeginStage(ctx, "kid-multimodal-enabled", attempt, types.StageMultimodal, nil)
	require.NotNil(t, mm)

	service := &KnowledgePostProcessService{spanTracker: tracker}
	service.finishRunningMultimodalStage(ctx, "kid-multimodal-enabled", attempt)

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?",
			"kid-multimodal-enabled", attempt, types.StageMultimodal).
		First(&row).Error)
	assert.Equal(t, types.SpanStatusDone, row.Status)
	assert.NotNil(t, row.FinishedAt)
	assert.GreaterOrEqual(t, row.DurationMs, int64(0))
}

func TestEnqueueQuestionGenerationTasks_PartialFailureFailsFastWithoutOpenOwners(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, parse_status, pending_subtasks_count) VALUES (?, ?, ?)`,
		"kid-question-enqueue", types.ParseStatusFinalizing, 2).Error)
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-enqueue", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-enqueue", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	service := &KnowledgePostProcessService{
		spanTracker:  tracker,
		taskEnqueuer: &questionBatchSelectiveEnqueuer{failBatch: 1},
	}
	chunks := make([]*types.Chunk, questionGenChunkBatchSize+1)
	for i := range chunks {
		chunks[i] = &types.Chunk{ID: string(rune('a' + i))}
	}

	enqueued, enqueueErr := service.enqueueQuestionGenerationTasks(ctx, types.KnowledgePostProcessPayload{
		TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "kid-question-enqueue",
	}, types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3}, attempt, chunks, group)

	require.NoError(t, enqueueErr)
	assert.Equal(t, 1, enqueued)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusCancelled)
	var openOwners int64
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND status IN ?", "kid-question-enqueue", attempt,
			[]string{types.SpanStatusPending, types.SpanStatusRunning}).
		Count(&openOwners).Error)
	assert.Zero(t, openOwners, "terminal enqueue failure must not strand an accepted sibling owner")
	var parseStatus string
	require.NoError(t, db.Table("knowledges").Select("parse_status").
		Where("id = ?", "kid-question-enqueue").Scan(&parseStatus).Error)
	assert.Equal(t, types.ParseStatusFailed, parseStatus)
}

func TestEnqueueQuestionGenerationTasks_AllFailuresDoNotLeaveParentRunning(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-enqueue-all", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-enqueue-all", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 1})
	service := &KnowledgePostProcessService{spanTracker: tracker}
	chunks := []*types.Chunk{{ID: "chunk-1"}}

	enqueued, enqueueErr := service.enqueueQuestionGenerationTasks(ctx, types.KnowledgePostProcessPayload{
		TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "kid-question-enqueue-all",
	}, types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3}, attempt, chunks, group)

	require.NoError(t, enqueueErr)
	assert.Zero(t, enqueued)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusFailed)
}
