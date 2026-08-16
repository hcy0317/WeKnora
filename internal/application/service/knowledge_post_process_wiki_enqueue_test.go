package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wikiEnqueueFailureKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge        *types.Knowledge
	expectedSubtasks int
}

func (r *wikiEnqueueFailureKnowledgeRepo) GetKnowledgeByIDOnly(
	context.Context,
	string,
) (*types.Knowledge, error) {
	return r.knowledge, nil
}

func (r *wikiEnqueueFailureKnowledgeRepo) SetFinalizing(
	_ context.Context,
	_ string,
	expectedSubtasks int,
) (bool, error) {
	r.expectedSubtasks = expectedSubtasks
	r.knowledge.ParseStatus = types.ParseStatusFinalizing
	return true, nil
}

func (r *wikiEnqueueFailureKnowledgeRepo) UpdateKnowledgeColumn(
	context.Context,
	string,
	string,
	interface{},
) error {
	return nil
}

func (r *wikiEnqueueFailureKnowledgeRepo) FinalizeSubtask(
	context.Context,
	string,
) (int, bool, error) {
	if r.expectedSubtasks > 0 {
		r.expectedSubtasks--
	}
	return r.expectedSubtasks, false, nil
}

func (r *wikiEnqueueFailureKnowledgeRepo) FinalizeSubtaskForAttempt(
	_ context.Context,
	_ string,
	_ int,
) (int, bool, error) {
	if r.expectedSubtasks > 0 {
		r.expectedSubtasks--
	}
	return r.expectedSubtasks, false, nil
}

type wikiEnqueueFailureKBService struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *wikiEnqueueFailureKBService) GetKnowledgeBaseByIDOnly(
	context.Context,
	string,
) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

type wikiEnqueueFailureChunkService struct {
	interfaces.ChunkService
	chunks []*types.Chunk
}

func (s *wikiEnqueueFailureChunkService) ListChunksByKnowledgeID(
	context.Context,
	string,
) ([]*types.Chunk, error) {
	return s.chunks, nil
}

type wikiEnqueueFailureTaskQueue struct {
	interfaces.TaskEnqueuer
	taskTypes        []string
	questionPayloads []types.QuestionGenerationPayload
	graphPayloads    []types.ExtractChunkPayload
	wikiErr          error
	summaryErr       error
	fastTracker      SpanTracker
	fastKnowledgeID  string
	fastAttempt      int
}

func (q *wikiEnqueueFailureTaskQueue) Enqueue(
	task *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	q.taskTypes = append(q.taskTypes, task.Type())
	switch task.Type() {
	case types.TypeQuestionGeneration:
		var payload types.QuestionGenerationPayload
		if err := json.Unmarshal(task.Payload(), &payload); err == nil {
			q.questionPayloads = append(q.questionPayloads, payload)
		}
	case types.TypeChunkExtract:
		var payload types.ExtractChunkPayload
		if err := json.Unmarshal(task.Payload(), &payload); err == nil {
			q.graphPayloads = append(q.graphPayloads, payload)
		}
	}
	if task.Type() == types.TypeSummaryGeneration && q.summaryErr != nil {
		return nil, q.summaryErr
	}
	if q.fastTracker != nil {
		name := ""
		switch task.Type() {
		case types.TypeSummaryGeneration:
			name = "postprocess.summary"
		case types.TypeWikiIngest:
			name = "postprocess.wiki"
		}
		if name != "" {
			parent := q.fastTracker.LookupStage(context.Background(), q.fastKnowledgeID, q.fastAttempt, types.StagePostProcess)
			span := q.fastTracker.BeginSubSpan(context.Background(), parent, name, types.SpanKindSubSpan,
				types.JSONMap{"worker_input": true})
			q.fastTracker.EndSpan(context.Background(), span, nil)
		}
	}
	if task.Type() == types.TypeWikiIngest {
		return nil, q.wikiErr
	}
	return &asynq.TaskInfo{ID: "queued", Type: task.Type()}, nil
}

type wikiEnqueueFailurePendingRepo struct {
	interfaces.TaskPendingOpsRepository
	seedErr       error
	seedCalls     int
	seededOp      *types.TaskPendingOp
	knowledgeRepo *wikiEnqueueFailureKnowledgeRepo
}

func (r *wikiEnqueueFailurePendingRepo) SeedKnowledgeFinalizingWithPendingOp(
	_ context.Context,
	_ string,
	expectedSubtasks int,
	op *types.TaskPendingOp,
) (bool, error) {
	r.seedCalls++
	if r.seedErr != nil {
		return false, r.seedErr
	}
	r.seededOp = op
	r.knowledgeRepo.expectedSubtasks = expectedSubtasks
	r.knowledgeRepo.knowledge.ParseStatus = types.ParseStatusFinalizing
	return true, nil
}

func newWikiEnqueueTestService(
	t *testing.T,
	knowledgeID string,
	pendingRepo *wikiEnqueueFailurePendingRepo,
	queue *wikiEnqueueFailureTaskQueue,
) (*KnowledgePostProcessService, *wikiEnqueueFailureKnowledgeRepo) {
	repo := &wikiEnqueueFailureKnowledgeRepo{
		knowledge: &types.Knowledge{
			ID:          knowledgeID,
			ParseStatus: types.ParseStatusProcessing,
		},
	}
	pendingRepo.knowledgeRepo = repo
	tracker, _ := setupSpanTrackerTest(t)
	_, _, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	return &KnowledgePostProcessService{
		knowledgeRepo: repo,
		kbService: &wikiEnqueueFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-wiki",
			IndexingStrategy: types.IndexingStrategy{
				WikiEnabled: true,
			},
		}},
		chunkService: &wikiEnqueueFailureChunkService{chunks: []*types.Chunk{
			{ID: "chunk-1", ChunkType: types.ChunkTypeText},
		}},
		taskEnqueuer: queue,
		pendingRepo:  pendingRepo,
		spanTracker:  tracker,
	}, repo
}

func newWikiEnqueuePostProcessTask(t *testing.T, knowledgeID string) *asynq.Task {
	return newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, 0)
}

func newWikiEnqueuePostProcessTaskForAttempt(t *testing.T, knowledgeID string, attempt int) *asynq.Task {
	t.Helper()
	payload, err := json.Marshal(types.KnowledgePostProcessPayload{
		TenantID:        7,
		KnowledgeID:     knowledgeID,
		KnowledgeBaseID: "kb-wiki",
		Attempt:         attempt,
	})
	require.NoError(t, err)
	return asynq.NewTask(types.TypeKnowledgePostProcess, payload)
}

func TestKnowledgePostProcessFanOutKeepsParentSpansRunning(t *testing.T) {
	const knowledgeID = "knowledge-postprocess-parent-waits"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker

	err = service.Handle(context.Background(), newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt))

	require.NoError(t, err)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusRunning)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusRunning)
	var post types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, attempt, types.StagePostProcess).
		First(&post).Error)
	assert.ElementsMatch(t, []any{"postprocess.summary", "postprocess.wiki"}, post.Input["expected_branches"])
}

func TestKnowledgePostProcessAtomicallySeedsWikiSlot(t *testing.T) {
	const knowledgeID = "knowledge-wiki-enqueue-failure"
	tests := []struct {
		name       string
		pendingErr error
		wantTasks  []string
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "pending op persistence fails",
			pendingErr: errors.New("postgres unavailable"),
			wantStatus: types.ParseStatusProcessing,
			wantErr:    true,
		},
		{
			name:       "pending op and trigger succeed",
			wantTasks:  []string{types.TypeSummaryGeneration, types.TypeWikiIngest},
			wantStatus: types.ParseStatusFinalizing,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pendingRepo := &wikiEnqueueFailurePendingRepo{seedErr: test.pendingErr}
			queue := &wikiEnqueueFailureTaskQueue{}
			service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)

			err := service.Handle(context.Background(), newWikiEnqueuePostProcessTask(t, knowledgeID))

			if test.wantErr {
				require.ErrorIs(t, err, test.pendingErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, 2, repo.expectedSubtasks, "summary and wiki each seed one slot")
				require.NotNil(t, pendingRepo.seededOp)
				assert.Equal(t, knowledgeID, pendingRepo.seededOp.DedupKey)
			}
			assert.Equal(t, test.wantStatus, repo.knowledge.ParseStatus)
			assert.Equal(t, test.wantTasks, queue.taskTypes)
			assert.Equal(t, 1, pendingRepo.seedCalls)
		})
	}
}

func TestKnowledgePostProcessRetriesWikiTriggerWithoutDoubleAccounting(t *testing.T) {
	const knowledgeID = "knowledge-wiki-trigger-retry"
	wikiErr := errors.New("redis unavailable")
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{wikiErr: wikiErr}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	task := newWikiEnqueuePostProcessTask(t, knowledgeID)

	err := service.Handle(context.Background(), task)

	require.ErrorIs(t, err, wikiErr)
	assert.Equal(t, 2, repo.expectedSubtasks, "summary and wiki each seed one slot")
	assert.Equal(t, 1, pendingRepo.seedCalls)
	assert.Equal(t, []string{types.TypeSummaryGeneration, types.TypeWikiIngest}, queue.taskTypes)

	queue.wikiErr = nil
	err = service.Handle(context.Background(), task)

	require.NoError(t, err)
	assert.Equal(t, 1, pendingRepo.seedCalls, "retry must not append another pending op")
	assert.Equal(t,
		[]string{types.TypeSummaryGeneration, types.TypeWikiIngest, types.TypeSummaryGeneration, types.TypeWikiIngest},
		queue.taskTypes,
		"incomplete recovery republishes deterministic IDs; Redis rejects an already queued summary",
	)
}

func TestKnowledgePostProcessFinalWikiTriggerFailureSettlesParentsFailed(t *testing.T) {
	const knowledgeID = "knowledge-wiki-trigger-retry-fails"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{wikiErr: errors.New("redis unavailable")}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	task := newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)

	finalCtx := types.WithTaskRetryMetadata(context.Background(), 3, 3)
	require.Error(t, service.Handle(finalCtx, task))
	summary := tracker.LookupSpanByName(context.Background(), knowledgeID, attempt, "postprocess.summary")
	require.NotNil(t, summary)
	tracker.EndSpan(context.Background(), summary, nil)
	tracker.SettlePostProcessTree(context.Background(), knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusFailed)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusFailed)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "postprocess.wiki",
		types.SpanKindSubSpan, types.SpanStatusFailed)
}

func TestKnowledgePostProcessDoesNotPublishFanoutWhenFailedChildCannotPersist(t *testing.T) {
	const knowledgeID = "knowledge-enqueue-failure-not-persisted"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, nil))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_enqueue_failure_span BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'subspan' BEGIN SELECT RAISE(ABORT, 'injected child persistence failure'); END;`).Error)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{summaryErr: errors.New("summary queue unavailable")}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker

	err = service.Handle(context.Background(), newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt))
	require.ErrorContains(t, err, "persist queued postprocess child postprocess.summary")
	var post types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, attempt, types.StagePostProcess).
		Take(&post).Error)
	assert.False(t, post.Input["fanout_complete"].(bool),
		"fanout completion must not be published when the failed child is not durable")
	require.NoError(t, db.Exec(`DROP TRIGGER reject_enqueue_failure_span`).Error)
	queue.summaryErr = nil
	queue.fastTracker = tracker
	queue.fastKnowledgeID = knowledgeID
	queue.fastAttempt = attempt
	require.NoError(t, service.Handle(context.Background(),
		newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)))
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
}

func TestKnowledgePostProcessFinalizingReentryRecoversPartialFanout(t *testing.T) {
	const knowledgeID = "knowledge-partial-fanout-recovery"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, nil))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_wiki_pending_span BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.name = 'postprocess.wiki' BEGIN SELECT RAISE(ABORT, 'injected wiki pending failure'); END;`).Error)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	task := newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)

	require.ErrorContains(t, service.Handle(context.Background(), task), "persist queued postprocess child postprocess.wiki")
	assert.Equal(t, []string{types.TypeSummaryGeneration}, queue.taskTypes)
	summary := tracker.BeginSubSpan(context.Background(),
		tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess),
		"postprocess.summary", types.SpanKindSubSpan, types.JSONMap{"recovered_worker": true})
	require.NotNil(t, summary)
	tracker.EndSpan(context.Background(), summary, nil)
	require.NoError(t, db.Exec(`DROP TRIGGER reject_wiki_pending_span`).Error)
	queue.fastTracker = tracker
	queue.fastKnowledgeID = knowledgeID
	queue.fastAttempt = attempt

	require.NoError(t, service.Handle(context.Background(), task))
	assert.Equal(t, []string{types.TypeSummaryGeneration, types.TypeWikiIngest}, queue.taskTypes,
		"completed summary must not be republished during finalizing recovery")
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
	var summaryRows int64
	require.NoError(t, db.Table("knowledge_processing_spans").Where(
		"knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, attempt, "postprocess.summary").Count(&summaryRows).Error)
	assert.Equal(t, int64(1), summaryRows)
}

func TestKnowledgePostProcessFastWorkerClaimsPendingBeforeFanoutCompletes(t *testing.T) {
	const knowledgeID = "knowledge-fast-worker"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{
		fastTracker: tracker, fastKnowledgeID: knowledgeID, fastAttempt: attempt,
	}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker

	require.NoError(t, service.Handle(context.Background(),
		newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)))
	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name IN ?", knowledgeID, attempt,
		[]string{"postprocess.summary", "postprocess.wiki"}).Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	for _, row := range rows {
		assert.Equal(t, types.SpanStatusDone, row.Status)
		assert.NotEqual(t, "TASK_SUPERSEDED", row.ErrorCode)
	}
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
}

func TestKnowledgePostProcessIgnoresSupersededAttemptBeforeFanOut(t *testing.T) {
	const knowledgeID = "knowledge-postprocess-superseded"
	tracker, db := setupSpanTrackerTest(t)
	_, oldAttempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	_, latestAttempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.Greater(t, latestAttempt, oldAttempt)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker

	err = service.Handle(context.Background(), newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, oldAttempt))

	require.NoError(t, err)
	assert.Equal(t, types.ParseStatusProcessing, repo.knowledge.ParseStatus)
	assert.Zero(t, pendingRepo.seedCalls)
	assert.Empty(t, queue.taskTypes)
	var postCount int64
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, oldAttempt, types.StagePostProcess).
		Count(&postCount).Error)
	assert.Zero(t, postCount, "superseded task must not write a postprocess span")
}

func TestKnowledgePostProcessFinalizingRecoveryDoesNotOverrideCancellation(t *testing.T) {
	const knowledgeID = "knowledge-recovery-cancelled"
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	repo.knowledge.ParseStatus = types.ParseStatusCancelled

	require.NoError(t, service.Handle(context.Background(), newWikiEnqueuePostProcessTask(t, knowledgeID)))
	assert.Empty(t, queue.taskTypes)
	assert.Zero(t, pendingRepo.seedCalls)
	assert.Equal(t, types.ParseStatusCancelled, repo.knowledge.ParseStatus)
}

func TestKnowledgePostProcessFinalizingRecoveryKeepsPersistedWikiBranchAfterConfigDisabled(t *testing.T) {
	const knowledgeID = "knowledge-recovery-wiki-enabled-plan"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, nil))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_recovery_wiki_child BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'subspan' BEGIN SELECT RAISE(ABORT, 'stop after fanout plan seed'); END;`).Error)
	t.Cleanup(func() { _ = db.Exec(`DROP TRIGGER IF EXISTS reject_recovery_wiki_child`).Error })

	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	task := newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)

	require.ErrorContains(t, service.Handle(context.Background(), task), "persist queued postprocess child")
	require.Equal(t, 1, pendingRepo.seedCalls)
	require.NotNil(t, pendingRepo.seededOp, "Wiki ownership must be durable before finalizing recovery")
	before := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, before)
	assert.ElementsMatch(t, []any{"postprocess.summary", "postprocess.wiki"}, before.Input["expected_branches"])

	require.NoError(t, db.Exec(`DROP TRIGGER reject_recovery_wiki_child`).Error)
	service.kbService.(*wikiEnqueueFailureKBService).kb.IndexingStrategy.WikiEnabled = false
	service.chunkService.(*wikiEnqueueFailureChunkService).chunks = nil
	queue.taskTypes = nil

	require.NoError(t, service.Handle(context.Background(), task))
	assert.Equal(t, []string{types.TypeSummaryGeneration, types.TypeWikiIngest}, queue.taskTypes,
		"recovery must publish the persisted Wiki branch even after current config/chunks disable it")
	assert.Equal(t, 1, pendingRepo.seedCalls, "recovery must reuse the already durable Wiki op")
	after := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, after)
	assert.ElementsMatch(t, []any{"postprocess.summary", "postprocess.wiki"}, after.Input["expected_branches"])
}

func TestKnowledgePostProcessFinalizingRecoveryDoesNotAddWikiAfterConfigEnabled(t *testing.T) {
	const knowledgeID = "knowledge-recovery-wiki-disabled-plan"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, nil))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_recovery_summary_child BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'subspan' BEGIN SELECT RAISE(ABORT, 'stop after fanout plan seed'); END;`).Error)
	t.Cleanup(func() { _ = db.Exec(`DROP TRIGGER IF EXISTS reject_recovery_summary_child`).Error })

	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, _ := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	service.kbService.(*wikiEnqueueFailureKBService).kb.IndexingStrategy.WikiEnabled = false
	task := newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)

	require.ErrorContains(t, service.Handle(context.Background(), task), "persist queued postprocess child")
	assert.Zero(t, pendingRepo.seedCalls)
	before := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, before)
	assert.ElementsMatch(t, []any{"postprocess.summary"}, before.Input["expected_branches"])

	require.NoError(t, db.Exec(`DROP TRIGGER reject_recovery_summary_child`).Error)
	service.kbService.(*wikiEnqueueFailureKBService).kb.IndexingStrategy.WikiEnabled = true
	queue.taskTypes = nil

	require.NoError(t, service.Handle(context.Background(), task))
	assert.Equal(t, []string{types.TypeSummaryGeneration}, queue.taskTypes,
		"recovery must not invent a Wiki branch with no atomically persisted pending op")
	assert.Zero(t, pendingRepo.seedCalls)
	after := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, after)
	assert.ElementsMatch(t, []any{"postprocess.summary"}, after.Input["expected_branches"])
	assert.Nil(t, tracker.LookupSpanByName(context.Background(), knowledgeID, attempt, "postprocess.wiki"))
}

func TestKnowledgePostProcessFinalizingRecoveryUsesPersistedQuestionAndGraphInputs(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	const knowledgeID = "knowledge-recovery-question-graph-plan"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, nil))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_recovery_fanout_child BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'subspan' BEGIN SELECT RAISE(ABORT, 'stop after fanout plan seed'); END;`).Error)
	t.Cleanup(func() { _ = db.Exec(`DROP TRIGGER IF EXISTS reject_recovery_fanout_child`).Error })

	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	kb := service.kbService.(*wikiEnqueueFailureKBService).kb
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true, GraphEnabled: true}
	kb.ExtractConfig = &types.ExtractConfig{Enabled: true}
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 5}
	kb.SummaryModelID = "summary-model-original"
	chunkService := service.chunkService.(*wikiEnqueueFailureChunkService)
	chunkService.chunks = make([]*types.Chunk, questionGenChunkBatchSize+1)
	for i := range chunkService.chunks {
		chunkService.chunks[i] = &types.Chunk{
			ID: fmt.Sprintf("original-chunk-%02d", i), ChunkType: types.ChunkTypeText, StartAt: i,
		}
	}
	task := newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)

	require.ErrorContains(t, service.Handle(context.Background(), task), "persist queued postprocess child")
	assert.Equal(t, 1+2+questionGenChunkBatchSize+1, repo.expectedSubtasks)
	before := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, before)
	require.Len(t, before.Input["expected_branches"], 1+1+questionGenChunkBatchSize+1)

	require.NoError(t, db.Exec(`DROP TRIGGER reject_recovery_fanout_child`).Error)
	kb.IndexingStrategy = types.IndexingStrategy{}
	kb.ExtractConfig = nil
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: false, QuestionCount: 1}
	kb.SummaryModelID = "summary-model-drifted"
	chunkService.chunks = []*types.Chunk{{ID: "drifted-chunk", ChunkType: types.ChunkTypeText}}
	queue.taskTypes = nil

	require.NoError(t, service.Handle(context.Background(), task))
	require.Len(t, queue.questionPayloads, 2, "persisted question batch count must survive config/chunk drift")
	require.Len(t, queue.graphPayloads, questionGenChunkBatchSize+1,
		"persisted graph chunk count must survive config/chunk drift")
	assert.Equal(t, 5, queue.questionPayloads[0].QuestionCount)
	assert.Equal(t, "original-chunk-00", queue.questionPayloads[0].ChunkIDs[0])
	assert.Equal(t, fmt.Sprintf("original-chunk-%02d", questionGenChunkBatchSize),
		queue.questionPayloads[1].ChunkIDs[0])
	for i, payload := range queue.graphPayloads {
		assert.Equal(t, fmt.Sprintf("original-chunk-%02d", i), payload.ChunkID)
		assert.Equal(t, "summary-model-original", payload.ModelID)
		assert.Equal(t, i, payload.ChunkIndex)
	}
	after := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, after)
	assert.Equal(t, before.Input["expected_branches"], after.Input["expected_branches"],
		"recovery must not overwrite the authoritative plan with current config")
}

func TestKnowledgePostProcessFinalizingRecoveryUpgradesVersionlessPlanWithoutAddingBranches(t *testing.T) {
	const knowledgeID = "knowledge-recovery-versionless-plan"
	tracker, _ := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, types.JSONMap{
		"expected_branches":       []string{"postprocess.summary"},
		"expected_subtasks_count": 1,
		"question_batch_count":    0,
		"fanout_complete":         false,
	})
	require.NotNil(t, post)
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	repo.knowledge.ParseStatus = types.ParseStatusFinalizing
	repo.expectedSubtasks = 1
	// Current config enables Wiki, but the authoritative legacy shape does not.
	service.kbService.(*wikiEnqueueFailureKBService).kb.IndexingStrategy.WikiEnabled = true

	require.NoError(t, service.Handle(context.Background(),
		newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)))
	assert.Equal(t, []string{types.TypeSummaryGeneration}, queue.taskTypes)
	assert.Zero(t, pendingRepo.seedCalls)
	stored := tracker.LookupStage(context.Background(), knowledgeID, attempt, types.StagePostProcess)
	require.NotNil(t, stored)
	assert.ElementsMatch(t, []any{"postprocess.summary"}, stored.Input["expected_branches"])
	require.NotNil(t, stored.Input["fanout_plan"], "versionless shape should be upgraded after bounded input recovery")
}

func TestKnowledgePostProcessFinalizingRecoveryFailsClosedOnBranchlessVersionlessPlan(t *testing.T) {
	const knowledgeID = "knowledge-recovery-versionless-branchless"
	tracker, db := setupSpanTrackerTest(t)
	_, attempt, err := tracker.OpenAttempt(context.Background(), knowledgeID, "")
	require.NoError(t, err)
	require.NotNil(t, tracker.BeginStage(context.Background(), knowledgeID, attempt, types.StagePostProcess, types.JSONMap{
		"expected_branches":       []string{},
		"expected_subtasks_count": 0,
		"question_batch_count":    0,
		"fanout_complete":         false,
	}))
	pendingRepo := &wikiEnqueueFailurePendingRepo{}
	queue := &wikiEnqueueFailureTaskQueue{}
	service, repo := newWikiEnqueueTestService(t, knowledgeID, pendingRepo, queue)
	service.spanTracker = tracker
	repo.knowledge.ParseStatus = types.ParseStatusFinalizing

	require.NoError(t, service.Handle(context.Background(),
		newWikiEnqueuePostProcessTaskForAttempt(t, knowledgeID, attempt)))
	assert.Empty(t, queue.taskTypes)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusFailed)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusFailed)
}
