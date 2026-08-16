package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type reparseFailureKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge          *types.Knowledge
	afterOpenKnowledge *types.Knowledge
	lastUpdated        *types.Knowledge
	getCalls           int
	updateCalls        int
	columnUpdateCalls  int
}

func (r *reparseFailureKnowledgeRepo) GetKnowledgeByID(
	_ context.Context,
	_ uint64,
	_ string,
) (*types.Knowledge, error) {
	r.getCalls++
	if r.getCalls > 1 && r.afterOpenKnowledge != nil {
		return r.afterOpenKnowledge, nil
	}
	return r.knowledge, nil
}

func (r *reparseFailureKnowledgeRepo) UpdateKnowledge(
	_ context.Context,
	knowledge *types.Knowledge,
) error {
	r.updateCalls++
	copy := *knowledge
	r.lastUpdated = &copy
	return nil
}

func (r *reparseFailureKnowledgeRepo) UpdateKnowledgeColumn(
	_ context.Context,
	_ string,
	_ string,
	_ interface{},
) error {
	r.columnUpdateCalls++
	return nil
}

type reparseFailureKBService struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *reparseFailureKBService) GetKnowledgeBaseByID(
	_ context.Context,
	_ string,
) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

type failingReparseTaskEnqueuer struct {
	err error
}

func (e failingReparseTaskEnqueuer) Enqueue(
	_ *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	return nil, e.err
}

type countingReparseGraphRepository struct {
	interfaces.RetrieveGraphRepository
	deleteCalls int
}

func (r *countingReparseGraphRepository) DelGraph(context.Context, []types.NameSpace) error {
	r.deleteCalls++
	return nil
}

type finalizingAttemptTracker struct {
	noopSpanTracker
	finalizedStatus  string
	finalizedAttempt int
	onOpen           func()
	openCalls        int
	rootInput        types.JSONMap
	updateInputErr   error
}

func (t *finalizingAttemptTracker) LatestAttempt(context.Context, string) int {
	if checkpoint, err := types.DecodeReparseCleanupCheckpoint(t.rootInput); err == nil && checkpoint != nil {
		return checkpoint.Attempt
	}
	return 8
}

func (t *finalizingAttemptTracker) OpenAttempt(
	_ context.Context,
	knowledgeID string,
	_ string,
) (*Span, int, error) {
	t.openCalls++
	if t.onOpen != nil {
		t.onOpen()
	}
	return &Span{KnowledgeID: knowledgeID, Attempt: 8, SpanID: "attempt-8", Kind: types.SpanKindRoot}, 8, nil
}

func (t *finalizingAttemptTracker) UpdateSpanInput(_ context.Context, span *Span, input types.JSONMap) error {
	if t.updateInputErr != nil {
		return t.updateInputErr
	}
	t.rootInput = input
	if span != nil {
		span.Input = input
	}
	return nil
}

func (t *finalizingAttemptTracker) LookupAttemptRoot(_ context.Context, knowledgeID string, attempt int) (*Span, error) {
	if attempt <= 0 {
		return nil, nil
	}
	return &Span{
		KnowledgeID: knowledgeID, Attempt: attempt, SpanID: "attempt-root",
		Name: "knowledge_processing", Kind: types.SpanKindRoot,
		Status: types.SpanStatusRunning, Input: t.rootInput,
	}, nil
}

func (t *finalizingAttemptTracker) FinalizeAttempt(
	_ context.Context,
	_ string,
	attempt int,
	status string,
	_ types.JSONMap,
	_ string,
	_ string,
) {
	t.finalizedAttempt = attempt
	t.finalizedStatus = status
}

type statusRecordingReparseRepo struct {
	interfaces.KnowledgeRepository
	knowledge *types.Knowledge
	statuses  []string
}

type cancellationAwareReparseRepo struct {
	statusRecordingReparseRepo
	canceledWrites int
}

func (r *cancellationAwareReparseRepo) UpdateKnowledge(ctx context.Context, knowledge *types.Knowledge) error {
	if err := ctx.Err(); err != nil {
		r.canceledWrites++
		return err
	}
	return r.statusRecordingReparseRepo.UpdateKnowledge(ctx, knowledge)
}

func (r *cancellationAwareReparseRepo) UpdateKnowledgeColumn(
	ctx context.Context,
	knowledgeID string,
	column string,
	value interface{},
) error {
	if err := ctx.Err(); err != nil {
		r.canceledWrites++
		return err
	}
	return r.statusRecordingReparseRepo.UpdateKnowledgeColumn(ctx, knowledgeID, column, value)
}

func (r *statusRecordingReparseRepo) GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error) {
	return r.knowledge, nil
}

func (r *statusRecordingReparseRepo) UpdateKnowledge(_ context.Context, knowledge *types.Knowledge) error {
	r.statuses = append(r.statuses, knowledge.ParseStatus)
	return nil
}

func (r *statusRecordingReparseRepo) UpdateKnowledgeColumn(context.Context, string, string, interface{}) error {
	return nil
}

type capturingReparseTaskEnqueuer struct {
	task *asynq.Task
}

func (e *capturingReparseTaskEnqueuer) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	e.task = task
	return &asynq.TaskInfo{ID: "reparse-document-8", Queue: types.QueueDefault}, nil
}

func TestReparseKnowledgeEnqueuesCleanupInsteadOfDeletingInRequest(t *testing.T) {
	knowledge := &types.Knowledge{
		ID:               "knowledge-reparse",
		TenantID:         7,
		KnowledgeBaseID:  "kb-1",
		Type:             "file",
		FilePath:         "resource://document",
		FileName:         "document.doc",
		ParseStatus:      types.ParseStatusCancelled,
		EnableStatus:     "enabled",
		EmbeddingModelID: "old-model",
		StorageSize:      4096,
	}
	repo := &statusRecordingReparseRepo{knowledge: knowledge}
	tracker := &finalizingAttemptTracker{}
	chunks := newAttemptChunkService()
	graph := &countingReparseGraphRepository{}
	queue := &capturingReparseTaskEnqueuer{}
	pending := &attemptPendingRepo{}
	svc := &knowledgeService{
		repo: repo,
		kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", EmbeddingModelID: "new-model",
			IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
		}},
		chunkService:    chunks,
		graphEngine:     graph,
		task:            queue,
		taskPendingRepo: pending,
		spanTracker:     tracker,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, nil)

	require.NoError(t, err)
	require.Same(t, knowledge, got)
	require.Equal(t, []string{types.ParseStatusPending}, repo.statuses)
	require.Zero(t, chunks.repo.imageInfoCalls)
	require.Zero(t, chunks.deleteCalls)
	require.Zero(t, graph.deleteCalls)
	require.Zero(t, pending.deleteCalls)
	require.NotNil(t, queue.task)
	require.Equal(t, types.TypeDocumentProcess, queue.task.Type())

	var payload types.DocumentProcessPayload
	require.NoError(t, json.Unmarshal(queue.task.Payload(), &payload))
	require.Equal(t, 8, payload.Attempt)
	require.True(t, payload.NeedCleanup)
	require.Equal(t, "old-model", knowledge.EmbeddingModelID,
		"the shared row must keep identifying the old vectors until cleanup commits")
	checkpoint, err := types.DecodeReparseCleanupCheckpoint(tracker.rootInput)
	require.NoError(t, err)
	require.Equal(t, &types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 8, Phase: types.ReparseCleanupPending,
		SourceEmbeddingModelID: "old-model", TargetEmbeddingModelID: "new-model",
		SourceEffectiveEngines: append([]types.RetrieverEngineParams(nil), types.GetDefaultRetrieverEngines()...),
		TargetEffectiveEngines: append([]types.RetrieverEngineParams(nil), types.GetDefaultRetrieverEngines()...),
		KnowledgeType:          "file", WikiCleanupRequired: true,
	}, checkpoint)
}

func TestReparseKnowledgeCheckpointFailureStopsBeforeResetAndEnqueue(t *testing.T) {
	checkpointErr := errors.New("span input unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-checkpoint-failure", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: "file", FilePath: "resource://document", FileName: "document.doc",
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", EmbeddingModelID: "old-model",
	}
	repo := &statusRecordingReparseRepo{knowledge: knowledge}
	queue := &capturingReparseTaskEnqueuer{}
	tracker := &finalizingAttemptTracker{updateInputErr: checkpointErr}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", EmbeddingModelID: "new-model",
		}}, task: queue, spanTracker: tracker,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, nil)

	require.ErrorIs(t, err, checkpointErr)
	require.Same(t, knowledge, got)
	require.Empty(t, repo.statuses, "checkpoint durability is required before shared state changes")
	require.Nil(t, queue.task)
	require.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
	require.Equal(t, "old-model", knowledge.EmbeddingModelID)
	require.Equal(t, 8, tracker.finalizedAttempt)
	require.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestReparseKnowledgeAcceptedAttemptSurvivesRequestCancellation(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "knowledge-disconnected", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: "file", FilePath: "resource://document", FileName: "document.doc",
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	repo := &cancellationAwareReparseRepo{
		statusRecordingReparseRepo: statusRecordingReparseRepo{knowledge: knowledge},
	}
	queue := &capturingReparseTaskEnqueuer{}
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	tracker := &finalizingAttemptTracker{onOpen: cancelRequest}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		task: queue, spanTracker: tracker,
	}
	requestCtx = context.WithValue(requestCtx, types.TenantIDContextKey, uint64(7))
	requestCtx = context.WithValue(requestCtx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(requestCtx, knowledge.ID, nil)

	require.NoError(t, err)
	require.Same(t, knowledge, got)
	require.Zero(t, repo.canceledWrites)
	require.Equal(t, []string{types.ParseStatusPending}, repo.statuses)
	require.NotNil(t, queue.task)
	var payload types.DocumentProcessPayload
	require.NoError(t, json.Unmarshal(queue.task.Payload(), &payload))
	require.Equal(t, 8, payload.Attempt)
	require.True(t, payload.NeedCleanup)
}

func TestReparseKnowledgeSubmissionFailureClosesFreshAttempt(t *testing.T) {
	enqueueErr := errors.New("queue unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-enqueue-failure", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: "file", FilePath: "resource://document", FileName: "document.doc",
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	repo := &statusRecordingReparseRepo{knowledge: knowledge}
	tracker := &finalizingAttemptTracker{}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		task: failingReparseTaskEnqueuer{err: enqueueErr}, spanTracker: tracker,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, nil)

	require.Error(t, err)
	require.Same(t, knowledge, got)
	require.Equal(t, []string{types.ParseStatusPending, types.ParseStatusFailed}, repo.statuses)
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Equal(t, "Failed to submit reparse task", knowledge.ErrorMessage)
	require.Equal(t, 8, tracker.finalizedAttempt)
	require.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestManualReparsePayloadCarriesSubmissionAttempt(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "knowledge-manual-attempt", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: types.KnowledgeTypeManual, ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	require.NoError(t, knowledge.SetManualMetadata(
		types.NewManualKnowledgeMetadata("# content", types.ManualKnowledgeStatusPublish, 1),
	))
	repo := &statusRecordingReparseRepo{knowledge: knowledge}
	tracker := &finalizingAttemptTracker{}
	queue := &capturingReparseTaskEnqueuer{}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		task: queue, spanTracker: tracker,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, nil)

	require.NoError(t, err)
	require.Same(t, knowledge, got)
	require.NotNil(t, queue.task)
	require.Equal(t, types.TypeManualProcess, queue.task.Type())
	var payload types.ManualProcessPayload
	require.NoError(t, json.Unmarshal(queue.task.Payload(), &payload))
	require.Equal(t, 8, payload.Attempt)
	require.True(t, payload.NeedCleanup)
}

func TestReparseKnowledgeManualEnqueueFailureIsVisible(t *testing.T) {
	enqueueErr := errors.New("queue unavailable")
	knowledge := &types.Knowledge{
		ID:              "knowledge-1",
		TenantID:        7,
		KnowledgeBaseID: "kb-1",
		Type:            types.KnowledgeTypeManual,
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
	}
	require.NoError(t, knowledge.SetManualMetadata(
		types.NewManualKnowledgeMetadata("# content", types.ManualKnowledgeStatusPublish, 1),
	))
	repo := &reparseFailureKnowledgeRepo{knowledge: knowledge}
	svc := &knowledgeService{
		repo:        repo,
		kbService:   &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		task:        failingReparseTaskEnqueuer{err: enqueueErr},
		spanTracker: &attemptTestTracker{},
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))

	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, nil)

	require.Error(t, err)
	require.Same(t, knowledge, got)
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Equal(t, "disabled", knowledge.EnableStatus)
	require.Equal(t, "Failed to submit reparse task", knowledge.ErrorMessage)
	require.GreaterOrEqual(t, repo.updateCalls, 2, "pending and failed states must both be persisted")
}

func TestReparseKnowledgeReloadsRowAfterAttemptAcceptance(t *testing.T) {
	stale := &types.Knowledge{
		ID: "knowledge-stale-row", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: "file", FilePath: "resource://document", FileName: "document.doc",
		EmbeddingModelID: "model-a", StorageSize: 4096,
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	fresh := *stale
	fresh.EmbeddingModelID = "model-b"
	fresh.StorageSize = 0
	repo := &reparseFailureKnowledgeRepo{knowledge: stale, afterOpenKnowledge: &fresh}
	queue := &capturingReparseTaskEnqueuer{}
	tracker := &finalizingAttemptTracker{}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", EmbeddingModelID: "model-c",
		}},
		task: queue, spanTracker: tracker,
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.ReparseKnowledge(ctx, stale.ID, nil)

	require.NoError(t, err)
	require.Same(t, &fresh, got)
	require.NotNil(t, repo.lastUpdated)
	require.Equal(t, "model-b", repo.lastUpdated.EmbeddingModelID,
		"submission must not roll back the model published by the previous worker")
	require.Zero(t, repo.lastUpdated.StorageSize,
		"submission must not resurrect storage already settled by the previous worker")
	require.NotNil(t, queue.task)
}

func TestUpdateManualKnowledgeReappliesIntentToFreshAcceptedRow(t *testing.T) {
	stale := &types.Knowledge{
		ID: "manual-stale-row", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: types.KnowledgeTypeManual, Title: "old title",
		EmbeddingModelID: "model-a", StorageSize: 8192,
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	require.NoError(t, stale.SetManualMetadata(
		types.NewManualKnowledgeMetadata("old content", types.ManualKnowledgeStatusPublish, 2),
	))
	fresh := *stale
	fresh.EmbeddingModelID = "model-b"
	fresh.StorageSize = 0
	repo := &reparseFailureKnowledgeRepo{knowledge: stale, afterOpenKnowledge: &fresh}
	queue := &capturingReparseTaskEnqueuer{}
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", EmbeddingModelID: "model-c",
		}},
		task: queue, spanTracker: &finalizingAttemptTracker{},
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	got, err := svc.UpdateManualKnowledge(ctx, stale.ID, &types.ManualKnowledgePayload{
		Title: "new title", Content: "new content", Status: types.ManualKnowledgeStatusPublish,
	})

	require.NoError(t, err)
	require.Same(t, &fresh, got)
	require.NotNil(t, repo.lastUpdated)
	require.Equal(t, "model-b", repo.lastUpdated.EmbeddingModelID)
	require.Zero(t, repo.lastUpdated.StorageSize)
	meta, err := repo.lastUpdated.ManualMetadata()
	require.NoError(t, err)
	require.Equal(t, "new content", meta.Content)
	require.Equal(t, 3, meta.Version)
	require.Equal(t, "new title", repo.lastUpdated.Title)
	require.NotNil(t, queue.task)
}

func TestRunKnowledgeListReparseSubmissionsReportsPartialFailure(t *testing.T) {
	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")
	var attempted []string

	outcome, err := runKnowledgeListReparseSubmissions(
		[]string{"ok-1", "bad-1", "ok-2", "bad-2"},
		func(id string) error {
			attempted = append(attempted, id)
			switch id {
			case "bad-1":
				return firstErr
			case "bad-2":
				return secondErr
			default:
				return nil
			}
		},
	)

	require.Equal(t, []string{"ok-1", "bad-1", "ok-2", "bad-2"}, attempted)
	require.Equal(t, knowledgeListReparseOutcome{Submitted: 2, Failed: 2}, outcome)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.ErrorContains(t, err, "knowledge bad-1")
	require.ErrorContains(t, err, "knowledge bad-2")
}

func TestRunKnowledgeListReparseSubmissionsSucceeds(t *testing.T) {
	outcome, err := runKnowledgeListReparseSubmissions(
		[]string{"knowledge-1", "knowledge-2"},
		func(string) error { return nil },
	)

	require.NoError(t, err)
	require.Equal(t, knowledgeListReparseOutcome{Submitted: 2}, outcome)
}
