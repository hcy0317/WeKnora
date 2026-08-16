package container

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type spanRetryRecoveryEnqueuer struct {
	mu       sync.Mutex
	tasks    []*asynq.Task
	conflict bool
	failures int
}

func (q *spanRetryRecoveryEnqueuer) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	if q.failures > 0 {
		q.failures--
		return nil, errors.New("queue unavailable")
	}
	if q.conflict {
		return nil, asynq.ErrTaskIDConflict
	}
	return &asynq.TaskInfo{ID: "queued", Queue: types.QueueSummary}, nil
}

func (q *spanRetryRecoveryEnqueuer) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func setupSpanRetryRecoveryDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}, &types.KnowledgeProcessingSpan{}))
	return db
}

func seedSpanRetryRecoveryOutbox(
	t *testing.T, db *gorm.DB, targetName, taskID, status string, input types.JSONMap,
) {
	t.Helper()
	payload, err := json.Marshal(types.KnowledgeSpanRetryOutboxPayload{
		TaskID: taskID, KnowledgeID: "kid", Attempt: 5,
		SpanID: "span", TargetName: targetName, TenantID: 7, KnowledgeBaseID: "kb", Input: input,
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.TaskPendingOp{
		TenantID: 7, TaskType: types.KnowledgeSpanRetryOutboxTaskType,
		Scope: types.KnowledgeSpanRetryOutboxScope, ScopeID: "kid",
		Op: types.KnowledgeSpanRetryOutboxOp, DedupKey: taskID, Payload: payload,
	}).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: "kid", Attempt: 5, SpanID: "span", ParentSpanID: "post",
		Name: targetName, Kind: types.SpanKindSubSpan, Status: status, Input: input,
	}).Error)
}

func TestRecoverPendingSpanRetriesReplaysCommittedOutbox(t *testing.T) {
	db := setupSpanRetryRecoveryDB(t)
	seedSpanRetryRecoveryOutbox(t, db, "postprocess.summary",
		"knowledge-fanout:kid:5:summary", types.SpanStatusPending, nil)

	queue := &spanRetryRecoveryEnqueuer{conflict: true}
	recoverPendingSpanRetries(db, queue)
	recoverPendingSpanRetries(db, queue)
	require.Equal(t, 1, queue.count(), "a committed row must be replayed after process restart")
	var count int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&count).Error)
	require.Zero(t, count, "duplicate TaskID proves publication and consumes the outbox")
}

func TestRecoverPendingSpanRetriesReplaysExactQuestionBatch(t *testing.T) {
	db := setupSpanRetryRecoveryDB(t)
	input := types.JSONMap{
		"batch_index": 3, "chunk_ids": []string{"chunk-31", "chunk-32"},
		"prev_chunk_id": "chunk-30", "next_chunk_id": "chunk-33",
		"question_count": 4, "language": "zh-CN",
	}
	seedSpanRetryRecoveryOutbox(t, db, "postprocess.question.batch[3]",
		"knowledge-fanout:kid:5:question:3", types.SpanStatusPending, input)

	queue := &spanRetryRecoveryEnqueuer{}
	recoverPendingSpanRetries(db, queue)

	require.Equal(t, 1, queue.count())
	require.Equal(t, types.TypeQuestionGeneration, queue.tasks[0].Type())
	var payload types.QuestionGenerationPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.Equal(t, 5, payload.Attempt)
	require.Equal(t, 3, payload.BatchIndex)
	require.Equal(t, []string{"chunk-31", "chunk-32"}, payload.ChunkIDs)
	require.Equal(t, "chunk-30", payload.PrevChunkID)
	require.Equal(t, "chunk-33", payload.NextChunkID)
	require.Equal(t, 4, payload.QuestionCount)
	require.Equal(t, "zh-CN", payload.Language)
	var count int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestPendingSpanRetryRecoveryRunnerRetriesWithoutRestartAndStops(t *testing.T) {
	db := setupSpanRetryRecoveryDB(t)
	seedSpanRetryRecoveryOutbox(t, db, "postprocess.summary",
		"knowledge-fanout:kid:5:summary", types.SpanStatusPending, nil)
	queue := &spanRetryRecoveryEnqueuer{failures: 1}
	runner := newPendingSpanRetryRecoveryRunner(db, queue, 10*time.Millisecond)
	runner.Start()
	t.Cleanup(runner.Stop)

	require.Eventually(t, func() bool {
		var count int64
		return db.Model(&types.TaskPendingOp{}).Count(&count).Error == nil && count == 0
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, queue.count(), 2, "periodic recovery must retry the failed startup publish")
}

func TestRecoverPendingSpanRetriesConsumesResidualOutboxWithoutRepublish(t *testing.T) {
	targets := []struct {
		name   string
		taskID string
		input  types.JSONMap
	}{
		{name: "postprocess.summary", taskID: "knowledge-fanout:kid:5:summary"},
		{name: "postprocess.wiki", taskID: "knowledge-fanout:kid:5:wiki"},
		{name: "postprocess.graph.chunk[3]", taskID: "knowledge-fanout:kid:5:graph:3",
			input: types.JSONMap{"chunk_id": "chunk", "model_id": "model", "chunk_index": 3}},
	}
	statuses := []string{types.SpanStatusRunning, types.SpanStatusDone, types.SpanStatusFailed,
		types.SpanStatusSkipped, types.SpanStatusCancelled}
	for _, target := range targets {
		for _, status := range statuses {
			t.Run(target.name+"/"+status, func(t *testing.T) {
				db := setupSpanRetryRecoveryDB(t)
				seedSpanRetryRecoveryOutbox(t, db, target.name, target.taskID, status, target.input)
				queue := &spanRetryRecoveryEnqueuer{}
				recoverPendingSpanRetries(db, queue)
				require.Zero(t, queue.count())
				var count int64
				require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&count).Error)
				require.Zero(t, count)
			})
		}
	}
}

func TestRecoverPendingSpanRetriesPreservesMissingAndUnknownTargets(t *testing.T) {
	for _, mode := range []string{"missing", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			db := setupSpanRetryRecoveryDB(t)
			seedSpanRetryRecoveryOutbox(t, db, "postprocess.summary",
				"knowledge-fanout:kid:5:summary", "mystery", nil)
			if mode == "missing" {
				require.NoError(t, db.Where("knowledge_id = ?", "kid").Delete(&types.KnowledgeProcessingSpan{}).Error)
			}
			queue := &spanRetryRecoveryEnqueuer{}
			recoverPendingSpanRetries(db, queue)
			require.Zero(t, queue.count())
			var count int64
			require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&count).Error)
			require.EqualValues(t, 1, count)
		})
	}
}
