package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type attemptTestTracker struct {
	noopSpanTracker
	err   error
	calls int
}

func (t *attemptTestTracker) OpenAttempt(_ context.Context, knowledgeID, _ string) (*Span, int, error) {
	t.calls++
	if t.err != nil {
		return nil, 0, t.err
	}
	return &Span{KnowledgeID: knowledgeID, Attempt: 1, SpanID: "test-root", Kind: types.SpanKindRoot}, 1, nil
}

func (t *attemptTestTracker) LatestAttempt(context.Context, string) int {
	if t.err != nil || t.calls == 0 {
		return 0
	}
	return 1
}

type attemptTenantRepo struct{ interfaces.TenantRepository }

func (attemptTenantRepo) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return &types.Tenant{}, nil
}

type failingAttemptTenantRepo struct {
	interfaces.TenantRepository
	err error
}

func (r failingAttemptTenantRepo) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return nil, r.err
}

type attemptKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge   *types.Knowledge
	updateCalls int
	onUpdate    func()
}

type routeSnapshotAttemptRepo struct {
	*attemptKnowledgeRepo
	current      bool
	err          error
	calls        int
	knowledgeID  string
	attempt      int
	lastSnapshot types.KnowledgeIndexRouteSnapshot
}

type routeAwareFailingModelService struct {
	interfaces.ModelService
	repo *routeSnapshotAttemptRepo
	err  error
}

func (s routeAwareFailingModelService) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	if s.repo == nil || s.repo.calls == 0 {
		return nil, errors.New("embedding model loaded before accepted index route was durable")
	}
	return nil, s.err
}

func (r *routeSnapshotAttemptRepo) SaveKnowledgeIndexRouteSnapshot(
	_ context.Context,
	knowledgeID string,
	attempt int,
	snapshot types.KnowledgeIndexRouteSnapshot,
) (bool, error) {
	r.calls++
	r.knowledgeID = knowledgeID
	r.attempt = attempt
	r.lastSnapshot = snapshot
	return r.current, r.err
}

type attemptLatestTracker struct {
	noopSpanTracker
	latest int
}

func (t *attemptLatestTracker) LatestAttempt(context.Context, string) int { return t.latest }

type rejectingAttemptKBService struct {
	interfaces.KnowledgeBaseService
	calls int
}

func (s *rejectingAttemptKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	s.calls++
	return nil, errors.New("knowledge base lookup must not run for a superseded attempt")
}

type attemptFailingGraphRepository struct {
	interfaces.RetrieveGraphRepository
	err         error
	deleteCalls int
}

func (r *attemptFailingGraphRepository) DelGraph(context.Context, []types.NameSpace) error {
	r.deleteCalls++
	return r.err
}

type attemptPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	deleteCalls int
	err         error
}

type attemptCleanupRepo struct {
	interfaces.ChunkRepository
	imageInfoCalls int
	imageInfos     []interfaces.ChunkImageInfo
	err            error
}

type cleanupVectorEngine struct {
	interfaces.RetrieveEngineService
	engineType  types.RetrieverEngineType
	deleteCalls int
}

func (e *cleanupVectorEngine) EngineType() types.RetrieverEngineType { return e.engineType }
func (e *cleanupVectorEngine) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType}
}
func (e *cleanupVectorEngine) DeleteByKnowledgeIDList(
	context.Context, []string, int, string,
) error {
	e.deleteCalls++
	return nil
}

type cleanupVectorRegistry struct {
	interfaces.RetrieveEngineRegistry
	engines map[types.RetrieverEngineType]interfaces.RetrieveEngineService
}

func (r cleanupVectorRegistry) GetRetrieveEngineService(
	engineType types.RetrieverEngineType,
) (interfaces.RetrieveEngineService, error) {
	engine := r.engines[engineType]
	if engine == nil {
		return nil, errors.New("engine not registered")
	}
	return engine, nil
}

func (r cleanupVectorRegistry) GetAllRetrieveEngineServices() []interfaces.RetrieveEngineService {
	services := make([]interfaces.RetrieveEngineService, 0, len(r.engines))
	for _, engine := range r.engines {
		services = append(services, engine)
	}
	return services
}

func (r *attemptCleanupRepo) ListImageInfoByKnowledgeIDs(context.Context, uint64, []string) ([]interfaces.ChunkImageInfo, error) {
	r.imageInfoCalls++
	return r.imageInfos, r.err
}

type attemptChunkService struct {
	interfaces.ChunkService
	repo        *attemptCleanupRepo
	deleteCalls int
	deleteErr   error
	createCalls int
	createErr   error
}

func (s *attemptChunkService) GetRepository() interfaces.ChunkRepository { return s.repo }

func (s *attemptChunkService) DeleteChunksByKnowledgeID(context.Context, string) error {
	s.deleteCalls++
	return s.deleteErr
}

func (s *attemptChunkService) CreateChunks(context.Context, []*types.Chunk) error {
	s.createCalls++
	return s.createErr
}

func newAttemptChunkService() *attemptChunkService {
	return &attemptChunkService{repo: &attemptCleanupRepo{}}
}

func (r *attemptPendingRepo) DeleteByDedupKey(context.Context, string, string, string, string, string) error {
	r.deleteCalls++
	return r.err
}

func (r *attemptKnowledgeRepo) GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error) {
	return r.knowledge, nil
}

func (r *attemptKnowledgeRepo) UpdateKnowledge(context.Context, *types.Knowledge) error {
	r.updateCalls++
	if r.onUpdate != nil {
		r.onUpdate()
	}
	return nil
}

type latestFinalizingTracker struct {
	noopSpanTracker
	latest           int
	finalizedAttempt int
	finalizedStatus  string
}

type routeHistoryTracker struct {
	noopSpanTracker
	roots map[int]*Span
	calls []int
}

func (t *routeHistoryTracker) LookupAttemptRoot(
	_ context.Context, _ string, attempt int,
) (*Span, error) {
	t.calls = append(t.calls, attempt)
	return t.roots[attempt], nil
}

func (t *latestFinalizingTracker) LatestAttempt(context.Context, string) int { return t.latest }

func (t *latestFinalizingTracker) FinalizeAttempt(
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

func reparseCleanupInput(t *testing.T, checkpoint types.ReparseCleanupCheckpoint) types.JSONMap {
	t.Helper()
	input, err := types.PutReparseCleanupCheckpoint(nil, checkpoint)
	require.NoError(t, err)
	return input
}

func TestInitialCreateOpenAttemptFailureDoesNotEnqueue(t *testing.T) {
	openErr := errors.New("span database unavailable")
	repo := &createKnowledgeFileRepoStub{}
	task := &createKnowledgeTaskEnqueuerStub{}
	tracker := &attemptTestTracker{err: openErr}
	svc := &knowledgeService{
		repo: repo, kbService: &createKnowledgeFileKBServiceStub{kb: &types.KnowledgeBase{ID: "kb-1"}},
		fileSvc: &createKnowledgeFileServiceStub{}, task: task, spanTracker: tracker,
	}

	knowledge, err := svc.CreateKnowledgeFromFile(
		newCreateKnowledgeFileContext(), "kb-1",
		newMultipartFileHeader(t, "doc.txt", "hello"), nil, nil, "", nil, "", nil,
	)
	require.ErrorIs(t, err, openErr)
	assert.Nil(t, knowledge)
	assert.Equal(t, 1, tracker.calls)
	assert.Zero(t, task.calls, "attempt failure must stop before enqueue")
}

func TestInitialAttemptGatePropagatesFailureForEveryCreateVariant(t *testing.T) {
	for _, variant := range []string{"file", "url", "file_url", "passage"} {
		t.Run(variant, func(t *testing.T) {
			openErr := errors.New("span database unavailable")
			tracker := &attemptTestTracker{err: openErr}
			svc := &knowledgeService{spanTracker: tracker}
			ctx, attempt, err := svc.beginInitialKnowledgeAttempt(context.Background(), "knowledge-"+variant)
			require.ErrorIs(t, err, openErr)
			assert.Zero(t, attempt)
			assert.Zero(t, attemptFromCtx(ctx))
			assert.Equal(t, 1, tracker.calls)
		})
	}
}

func TestInitialCreateVariantsStopBeforeEnqueueWhenAttemptFails(t *testing.T) {
	tests := []struct {
		name string
		run  func(*knowledgeService) (*types.Knowledge, error)
	}{
		{name: "url", run: func(s *knowledgeService) (*types.Knowledge, error) {
			return s.CreateKnowledgeFromURL(newCreateKnowledgeFileContext(), "kb-1",
				"https://example.com/article", "", "", nil, "article", nil, "", nil)
		}},
		{name: "file_url", run: func(s *knowledgeService) (*types.Knowledge, error) {
			return s.CreateKnowledgeFromURL(newCreateKnowledgeFileContext(), "kb-1",
				"https://example.com/document.pdf", "document.pdf", "pdf", nil, "document", nil, "", nil)
		}},
		{name: "passage", run: func(s *knowledgeService) (*types.Knowledge, error) {
			return s.CreateKnowledgeFromPassage(newCreateKnowledgeFileContext(), "kb-1", []string{"safe passage"}, "")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			openErr := errors.New("span database unavailable")
			task := &createKnowledgeTaskEnqueuerStub{}
			tracker := &attemptTestTracker{err: openErr}
			svc := &knowledgeService{
				repo:      &createKnowledgeFileRepoStub{},
				kbService: &createKnowledgeFileKBServiceStub{kb: &types.KnowledgeBase{ID: "kb-1"}},
				fileSvc:   &createKnowledgeFileServiceStub{}, task: task, spanTracker: tracker,
			}
			knowledge, err := tt.run(svc)
			require.ErrorIs(t, err, openErr)
			assert.Nil(t, knowledge)
			assert.Equal(t, 1, tracker.calls)
			assert.Zero(t, task.calls)
		})
	}
}

func TestReparseOpenAttemptFailureDoesNotResetOrEnqueue(t *testing.T) {
	openErr := errors.New("span database unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-reparse", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: "file", FilePath: "resource://document", FileName: "document.doc",
		ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
	}
	repo := &reparseFailureKnowledgeRepo{knowledge: knowledge}
	task := &createKnowledgeTaskEnqueuerStub{}
	pending := &attemptPendingRepo{}
	chunks := newAttemptChunkService()
	svc := &knowledgeService{
		repo: repo, kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
		}},
		task: task, taskPendingRepo: pending, chunkService: chunks, spanTracker: &attemptTestTracker{err: openErr},
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{})

	graphEnabled := true
	got, err := svc.ReparseKnowledge(ctx, knowledge.ID, &types.KnowledgeProcessOverrides{
		GraphEnabled: &graphEnabled,
	})
	require.ErrorIs(t, err, openErr)
	assert.Nil(t, got)
	assert.Zero(t, repo.updateCalls)
	assert.Zero(t, repo.columnUpdateCalls, "attempt failure must not persist reparse overrides")
	assert.Zero(t, task.calls)
	assert.Zero(t, pending.deleteCalls, "attempt failure must stop before prepareWiki")
	assert.Zero(t, chunks.repo.imageInfoCalls, "attempt failure must stop before cleanup reads")
	assert.Zero(t, chunks.deleteCalls, "attempt failure must stop before cleanup writes")
	assert.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
}

func TestManualOpenAttemptFailureDoesNotResetOrProcess(t *testing.T) {
	openErr := errors.New("span database unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-manual", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: types.KnowledgeTypeManual, ParseStatus: types.ParseStatusPending,
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:    &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		chunkService: chunks,
		spanTracker:  &attemptTestTracker{err: openErr},
	}
	payload, err := json.Marshal(types.ManualProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1", Content: "content", NeedCleanup: true,
	})
	require.NoError(t, err)

	err = svc.ProcessManualUpdate(context.Background(), asynq.NewTask(types.TypeManualProcess, payload))
	require.ErrorIs(t, err, openErr)
	assert.Zero(t, repo.updateCalls)
	assert.Zero(t, chunks.repo.imageInfoCalls)
	assert.Zero(t, chunks.deleteCalls)
	assert.Equal(t, types.ParseStatusPending, knowledge.ParseStatus)
}

func TestLegacyDocumentOpenAttemptFailureDoesNotResetOrProcess(t *testing.T) {
	openErr := errors.New("span database unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-legacy", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(),
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:    &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		chunkService: chunks,
		spanTracker:  &attemptTestTracker{err: openErr},
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1", Attempt: 0,
	})
	require.NoError(t, err)

	err = svc.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, payload))
	require.ErrorIs(t, err, openErr)
	assert.Zero(t, repo.updateCalls)
	assert.Zero(t, chunks.repo.imageInfoCalls)
	assert.Zero(t, chunks.deleteCalls)
	assert.Equal(t, types.ParseStatusPending, knowledge.ParseStatus)
}

func TestProcessDocumentSupersededAttemptDoesNotMutateOrCleanup(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "knowledge-stale", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(),
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	kbService := &rejectingAttemptKBService{}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{}, kbService: kbService,
		chunkService: chunks, spanTracker: &attemptLatestTracker{latest: 2},
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1", Attempt: 1, NeedCleanup: true,
	})
	require.NoError(t, err)

	err = svc.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, payload))

	require.NoError(t, err)
	assert.Zero(t, kbService.calls)
	assert.Zero(t, repo.updateCalls)
	assert.Zero(t, chunks.repo.imageInfoCalls)
	assert.Zero(t, chunks.deleteCalls)
	assert.Equal(t, types.ParseStatusPending, knowledge.ParseStatus)
}

func TestProcessDocumentTerminalValidationFailureIsAttemptAware(t *testing.T) {
	tracker := &latestFinalizingTracker{latest: 1}
	knowledge := &types.Knowledge{
		ID: "knowledge-terminal-validation", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(),
	}
	baseRepo := &attemptKnowledgeRepo{knowledge: knowledge}
	repo := &routeSnapshotAttemptRepo{attemptKnowledgeRepo: baseRepo, current: true}
	vectorStoreID := "vector-store-a"
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", EmbeddingModelID: "embedding-model-a", VectorStoreID: &vectorStoreID,
			IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
		}},
		spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		FilePath: "resource://image", FileType: "png", Attempt: 1,
	})
	require.NoError(t, err)

	err = svc.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, payload))

	require.ErrorIs(t, err, asynq.SkipRetry)
	require.ErrorIs(t, err, ErrImageNotParse)
	assert.Equal(t, 2, repo.updateCalls, "processing and terminal failure must both use the attempt-aware writer")
	require.Equal(t, 1, repo.calls, "the accepted route must be durable before any parser or index write")
	require.Equal(t, knowledge.ID, repo.knowledgeID)
	require.Equal(t, 1, repo.attempt)
	require.Equal(t, "embedding-model-a", repo.lastSnapshot.EmbeddingModelID)
	require.Equal(t, &vectorStoreID, repo.lastSnapshot.VectorStoreID)
	assert.Equal(t, 1, tracker.finalizedAttempt)
	assert.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestProcessDocumentTerminalValidationDoesNotOverwriteNewAttempt(t *testing.T) {
	tracker := &latestFinalizingTracker{latest: 1}
	knowledge := &types.Knowledge{
		ID: "knowledge-stale-validation", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(),
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	repo.onUpdate = func() {
		if repo.updateCalls == 1 {
			tracker.latest = 2
		}
	}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:   &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		FilePath: "resource://image", FileType: "png", Attempt: 1,
	})
	require.NoError(t, err)

	err = svc.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, payload))

	require.NoError(t, err)
	assert.Equal(t, 1, repo.updateCalls,
		"a terminal branch from the old worker must not persist after a newer attempt opens")
}

func TestProcessDocumentTenantLookupFailureUsesThreeRetriesThenFailsAttempt(t *testing.T) {
	wantErr := errors.New("tenant database unavailable")
	tracker := &latestFinalizingTracker{latest: 6}
	svc := &knowledgeService{
		tenantRepo:  failingAttemptTenantRepo{err: wantErr},
		spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: "knowledge-tenant-retry", KnowledgeBaseID: "kb-1", Attempt: 6,
	})
	require.NoError(t, err)
	task := asynq.NewTask(types.TypeDocumentProcess, payload)

	err = svc.ProcessDocument(types.WithTaskRetryMetadata(context.Background(), 0, 3), task)
	require.ErrorIs(t, err, wantErr)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	require.Zero(t, tracker.finalizedAttempt)

	err = svc.ProcessDocument(types.WithTaskRetryMetadata(context.Background(), 3, 3), task)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, 6, tracker.finalizedAttempt)
	require.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestProcessChunksCreateFailureRetriesBeforeFailingAttempt(t *testing.T) {
	wantErr := errors.New("chunk database unavailable")
	tracker := &latestFinalizingTracker{latest: 5}
	knowledge := &types.Knowledge{
		ID: "knowledge-chunk-retry", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusProcessing,
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	chunks.createErr = wantErr
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{}, chunkService: chunks,
		graphEngine: &attemptFailingGraphRepository{}, spanTracker: tracker,
	}
	kb := &types.KnowledgeBase{
		ID: "kb-1", IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
	}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
	ctx = withAttempt(ctx, 5)

	err := svc.processChunks(types.WithTaskRetryMetadata(ctx, 0, 3), kb, knowledge,
		[]types.ParsedChunk{{Content: "content", Seq: 0}})
	require.ErrorIs(t, err, wantErr)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	require.Zero(t, repo.updateCalls)
	require.Zero(t, tracker.finalizedAttempt)

	err = svc.processChunks(types.WithTaskRetryMetadata(ctx, 3, 3), kb, knowledge,
		[]types.ParsedChunk{{Content: "content", Seq: 0}})
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, 1, repo.updateCalls)
	require.Equal(t, 5, tracker.finalizedAttempt)
	require.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestProcessChunksFreezesAcceptedRouteBeforeLoadingEmbeddingModel(t *testing.T) {
	wantErr := errors.New("embedding model temporarily unavailable")
	tracker := &latestFinalizingTracker{latest: 4}
	knowledge := &types.Knowledge{
		ID: "knowledge-route-before-index", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusProcessing,
	}
	baseRepo := &attemptKnowledgeRepo{knowledge: knowledge}
	repo := &routeSnapshotAttemptRepo{attemptKnowledgeRepo: baseRepo, current: true}
	vectorStoreID := "vector-store-a"
	kb := &types.KnowledgeBase{
		ID: "kb-1", EmbeddingModelID: "embedding-model-a", VectorStoreID: &vectorStoreID,
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	svc := &knowledgeService{
		repo: repo, spanTracker: tracker,
		modelService: routeAwareFailingModelService{repo: repo, err: wantErr},
	}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
	ctx = withAttempt(types.WithTaskRetryMetadata(ctx, 0, 3), 4)

	err := svc.processChunks(ctx, kb, knowledge, []types.ParsedChunk{{Content: "content"}})

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, repo.calls)
	require.Equal(t, "embedding-model-a", repo.lastSnapshot.EmbeddingModelID)
	require.Equal(t, &vectorStoreID, repo.lastSnapshot.VectorStoreID)
}

func TestProcessDocumentDeferredCleanupFailureIsVisible(t *testing.T) {
	cleanupErr := errors.New("graph cleanup unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-cleanup", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(), FilePath: "resource://image", FileType: "png",
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	graph := &attemptFailingGraphRepository{err: cleanupErr}
	tracker := &finalizingAttemptTracker{rootInput: reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 8, Phase: types.ReparseCleanupPrepared,
		KnowledgeType: "file", WikiPendingIngestScrubbed: true,
		VectorsDeleted: true, ChunksDeleted: true, ImagesDeleted: true,
	})}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:    &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		chunkService: chunks, graphEngine: graph, spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		FilePath: knowledge.FilePath, FileType: knowledge.FileType, Attempt: 8, NeedCleanup: true,
	})
	require.NoError(t, err)

	retryCtx := types.WithTaskRetryMetadata(context.Background(), 0, 3)
	err = svc.ProcessDocument(retryCtx, asynq.NewTask(types.TypeDocumentProcess, payload))

	require.ErrorIs(t, err, cleanupErr)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	assert.Equal(t, 1, graph.deleteCalls)
	assert.NotEqual(t, types.ParseStatusFailed, knowledge.ParseStatus)
	assert.Zero(t, tracker.finalizedAttempt, "intermediate cleanup failure must keep the attempt retryable")

	finalCtx := types.WithTaskRetryMetadata(context.Background(), 3, 3)
	err = svc.ProcessDocument(finalCtx, asynq.NewTask(types.TypeDocumentProcess, payload))
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorIs(t, err, asynq.SkipRetry)
	assert.Equal(t, 2, graph.deleteCalls)
	assert.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	assert.Contains(t, knowledge.ErrorMessage, "failed to cleanup old resources")
	assert.Equal(t, 8, tracker.finalizedAttempt)
	assert.Equal(t, types.SpanStatusFailed, tracker.finalizedStatus)
}

func TestProcessManualUpdateReusesPayloadAttemptOnCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("graph cleanup unavailable")
	knowledge := &types.Knowledge{
		ID: "manual-cleanup", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: types.KnowledgeTypeManual, ParseStatus: types.ParseStatusPending,
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	graph := &attemptFailingGraphRepository{err: cleanupErr}
	tracker := &finalizingAttemptTracker{rootInput: reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 4, Phase: types.ReparseCleanupPrepared,
		KnowledgeType: types.KnowledgeTypeManual, WikiPendingIngestScrubbed: true,
		VectorsDeleted: true, ChunksDeleted: true, ImagesDeleted: true,
	})}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:    &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		chunkService: chunks, graphEngine: graph, spanTracker: tracker,
	}
	payload, err := json.Marshal(types.ManualProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		Content: "content", NeedCleanup: true, Attempt: 4,
	})
	require.NoError(t, err)

	err = svc.ProcessManualUpdate(types.WithTaskRetryMetadata(context.Background(), 0, 3),
		asynq.NewTask(types.TypeManualProcess, payload))

	require.ErrorIs(t, err, cleanupErr)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	assert.Zero(t, tracker.openCalls)
	assert.Zero(t, tracker.finalizedAttempt)
}

func TestWikiScrubFailureStopsDestructiveCleanupAndRemainsRetryable(t *testing.T) {
	scrubErr := errors.New("pending wiki store unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-wiki-scrub", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(), FilePath: "resource://doc", FileType: "doc",
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	graph := &attemptFailingGraphRepository{}
	pending := &attemptPendingRepo{err: scrubErr}
	tracker := &finalizingAttemptTracker{rootInput: reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 8, Phase: types.ReparseCleanupPending, KnowledgeType: "file",
		WikiCleanupRequired: true,
	})}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService: &reparseFailureKBService{kb: &types.KnowledgeBase{
			ID: "kb-1", IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
		}},
		chunkService: chunks, graphEngine: graph, taskPendingRepo: pending, spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		FilePath: knowledge.FilePath, FileType: knowledge.FileType, Attempt: 8, NeedCleanup: true,
	})
	require.NoError(t, err)

	err = svc.ProcessDocument(types.WithTaskRetryMetadata(context.Background(), 0, 3),
		asynq.NewTask(types.TypeDocumentProcess, payload))

	require.ErrorIs(t, err, scrubErr)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	assert.Equal(t, 1, pending.deleteCalls)
	assert.Zero(t, chunks.repo.imageInfoCalls)
	assert.Zero(t, chunks.deleteCalls)
	assert.Zero(t, graph.deleteCalls)
	assert.Zero(t, tracker.finalizedAttempt)
}

func TestDeleteReparseVectorsUsesSubmittedEffectiveEngineSnapshot(t *testing.T) {
	oldEngine := &cleanupVectorEngine{engineType: types.PostgresRetrieverEngineType}
	newEngine := &cleanupVectorEngine{engineType: types.QdrantRetrieverEngineType}
	svc := &knowledgeService{retrieveEngine: cleanupVectorRegistry{engines: map[types.RetrieverEngineType]interfaces.RetrieveEngineService{
		types.PostgresRetrieverEngineType: oldEngine,
		types.QdrantRetrieverEngineType:   newEngine,
	}}}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})
	checkpoint := &types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 4, Phase: types.ReparseCleanupPrepared,
		SourceEmbeddingModelID: "old-model", EmbeddingDimensions: 1536, KnowledgeType: "file",
		SourceEffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	}

	err := svc.deleteReparseVectors(ctx, &types.Knowledge{ID: "knowledge-1"}, checkpoint)
	require.NoError(t, err)
	require.Equal(t, 1, oldEngine.deleteCalls)
	require.Zero(t, newEngine.deleteCalls,
		"cleanup must not follow a tenant default changed after the attempt was submitted")
}

func TestApplyReparseExecutionSnapshotRejectsMutableModelAndEngineDrift(t *testing.T) {
	tracker := &finalizingAttemptTracker{rootInput: reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 4, Phase: types.ReparseCleanupCompleted,
		TargetEmbeddingModelID: "accepted-model",
		TargetEffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	})}
	svc := &knowledgeService{spanTracker: tracker}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})
	kb := &types.KnowledgeBase{ID: "kb-1", EmbeddingModelID: "later-model"}
	knowledge := &types.Knowledge{ID: "knowledge-1", EmbeddingModelID: "old-model"}

	snapshotCtx, snapshotKB, err := svc.applyReparseExecutionSnapshot(ctx, kb, knowledge, 4)
	require.NoError(t, err)
	require.Equal(t, "accepted-model", snapshotKB.EmbeddingModelID)
	require.Equal(t, "accepted-model", knowledge.EmbeddingModelID)
	tenantSnapshot, ok := types.TenantInfoFromContext(snapshotCtx)
	require.True(t, ok)
	require.Equal(t, types.PostgresRetrieverEngineType,
		tenantSnapshot.GetEffectiveEngines()[0].RetrieverEngineType)
	require.Equal(t, "later-model", kb.EmbeddingModelID, "the mutable KB object must not be modified in place")
}

func TestNewReparseCleanupCheckpointUsesPreviousAttemptIndexRoute(t *testing.T) {
	previousInput, err := types.PutKnowledgeIndexRouteSnapshot(nil, types.KnowledgeIndexRouteSnapshot{
		Version: 1,
		EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	})
	require.NoError(t, err)
	tracker := &finalizingAttemptTracker{rootInput: previousInput}
	svc := &knowledgeService{spanTracker: tracker}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})

	checkpoint, err := svc.newReparseCleanupCheckpoint(
		ctx,
		&types.Knowledge{ID: "knowledge-1", EmbeddingModelID: "old-model", Type: "file"},
		&types.KnowledgeBase{ID: "kb-1", EmbeddingModelID: "new-model"},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, types.PostgresRetrieverEngineType,
		checkpoint.SourceEffectiveEngines[0].RetrieverEngineType)
	require.Equal(t, types.QdrantRetrieverEngineType,
		checkpoint.TargetEffectiveEngines[0].RetrieverEngineType)
}

func TestNewReparseCleanupCheckpointLegacyFallbackCoversAllRegisteredDefaultEngines(t *testing.T) {
	postgres := &cleanupVectorEngine{engineType: types.PostgresRetrieverEngineType}
	qdrant := &cleanupVectorEngine{engineType: types.QdrantRetrieverEngineType}
	svc := &knowledgeService{
		spanTracker: &finalizingAttemptTracker{},
		retrieveEngine: cleanupVectorRegistry{engines: map[types.RetrieverEngineType]interfaces.RetrieveEngineService{
			types.PostgresRetrieverEngineType: postgres,
			types.QdrantRetrieverEngineType:   qdrant,
		}},
	}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})

	checkpoint, err := svc.newReparseCleanupCheckpoint(
		ctx,
		&types.Knowledge{ID: "legacy-knowledge", EmbeddingModelID: "old-model", Type: "file"},
		&types.KnowledgeBase{ID: "kb-1", EmbeddingModelID: "new-model"},
		1,
	)
	require.NoError(t, err)
	require.Len(t, checkpoint.TargetEffectiveEngines, 1)
	require.Len(t, checkpoint.SourceEffectiveEngines, 2)
	require.Equal(t, types.PostgresRetrieverEngineType,
		checkpoint.SourceEffectiveEngines[0].RetrieverEngineType)
	require.Equal(t, types.QdrantRetrieverEngineType,
		checkpoint.SourceEffectiveEngines[1].RetrieverEngineType)
}

func TestNewReparseCleanupCheckpointWalksBackToNewestPublishedIndexRoute(t *testing.T) {
	oldRoute, err := types.PutKnowledgeIndexRouteSnapshot(nil, types.KnowledgeIndexRouteSnapshot{
		Version: 1,
		EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	})
	require.NoError(t, err)
	tracker := &routeHistoryTracker{roots: map[int]*Span{
		3: {Attempt: 3, Input: nil},
		2: {Attempt: 2, Input: oldRoute},
	}}
	svc := &knowledgeService{spanTracker: tracker}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})

	checkpoint, err := svc.newReparseCleanupCheckpoint(
		ctx,
		&types.Knowledge{ID: "knowledge-history", EmbeddingModelID: "old-model", Type: "file"},
		&types.KnowledgeBase{ID: "kb-1", EmbeddingModelID: "new-model"},
		4,
	)
	require.NoError(t, err)
	require.Equal(t, []int{3, 2}, tracker.calls)
	require.Len(t, checkpoint.SourceEffectiveEngines, 1)
	require.Equal(t, types.PostgresRetrieverEngineType,
		checkpoint.SourceEffectiveEngines[0].RetrieverEngineType)
}

func TestNewReparseCleanupCheckpointUsesCompletedPreviousTargetBeforeRoutePublication(t *testing.T) {
	previousInput := reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: types.ReparseCleanupVersion, Attempt: 3, Phase: types.ReparseCleanupCompleted,
		SourceEmbeddingModelID: "model-a", TargetEmbeddingModelID: "model-b",
		EmbeddingDimensions: 1536,
		TargetEffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.QdrantRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	})
	tracker := &routeHistoryTracker{roots: map[int]*Span{
		3: {Attempt: 3, Input: previousInput},
	}}
	svc := &knowledgeService{spanTracker: tracker}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}}},
	})

	checkpoint, err := svc.newReparseCleanupCheckpoint(
		ctx,
		&types.Knowledge{ID: "knowledge-inflight", EmbeddingModelID: "model-a", Type: "file"},
		&types.KnowledgeBase{ID: "kb-1", EmbeddingModelID: "model-c"},
		4,
	)
	require.NoError(t, err)
	require.Equal(t, "model-b", checkpoint.SourceEmbeddingModelID)
	require.Len(t, checkpoint.SourceEffectiveEngines, 1)
	require.Equal(t, types.QdrantRetrieverEngineType,
		checkpoint.SourceEffectiveEngines[0].RetrieverEngineType)
}

func TestReparseCleanupPersistsImageManifestAndSkipsCompletedStepsOnRetry(t *testing.T) {
	graphErr := errors.New("graph cleanup unavailable")
	knowledge := &types.Knowledge{
		ID: "knowledge-image-manifest", TenantID: 7, KnowledgeBaseID: "kb-1",
		ParseStatus: types.ParseStatusPending, CreatedAt: time.Now(), FilePath: "resource://doc", FileType: "doc",
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	chunks.repo.imageInfos = []interfaces.ChunkImageInfo{{
		KnowledgeID: knowledge.ID,
		ImageInfo:   `[{"url":"local://persisted-image"}]`,
	}}
	graph := &attemptFailingGraphRepository{err: graphErr}
	files := &createKnowledgeFileServiceStub{}
	tracker := &finalizingAttemptTracker{rootInput: reparseCleanupInput(t, types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: 8, Phase: types.ReparseCleanupPending, KnowledgeType: "file",
	})}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{},
		kbService:    &reparseFailureKBService{kb: &types.KnowledgeBase{ID: "kb-1"}},
		chunkService: chunks, graphEngine: graph, fileSvc: files, spanTracker: tracker,
	}
	payload, err := json.Marshal(types.DocumentProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		FilePath: knowledge.FilePath, FileType: knowledge.FileType, Attempt: 8, NeedCleanup: true,
	})
	require.NoError(t, err)
	task := asynq.NewTask(types.TypeDocumentProcess, payload)

	err = svc.ProcessDocument(types.WithTaskRetryMetadata(context.Background(), 0, 3), task)
	require.ErrorIs(t, err, graphErr)
	checkpoint, decodeErr := types.DecodeReparseCleanupCheckpoint(tracker.rootInput)
	require.NoError(t, decodeErr)
	require.Equal(t, []string{"local://persisted-image"}, checkpoint.ImageURLs)
	require.True(t, checkpoint.ChunksDeleted)
	require.True(t, checkpoint.ImagesDeleted)
	require.False(t, checkpoint.GraphDeleted)
	require.Equal(t, 1, chunks.repo.imageInfoCalls)
	require.Equal(t, 1, chunks.deleteCalls)
	require.Equal(t, 1, files.deleteCalls)

	err = svc.ProcessDocument(types.WithTaskRetryMetadata(context.Background(), 1, 3), task)
	require.ErrorIs(t, err, graphErr)
	require.Equal(t, 1, chunks.repo.imageInfoCalls, "retry must use the root-span manifest")
	require.Equal(t, 1, chunks.deleteCalls, "successful cleanup steps must not repeat")
	require.Equal(t, 1, files.deleteCalls, "successful image cleanup must not repeat")
	require.Equal(t, 2, graph.deleteCalls)
}

func TestProcessManualUpdateSupersededAttemptDoesNotMutateOrCleanup(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "manual-stale", TenantID: 7, KnowledgeBaseID: "kb-1",
		Type: types.KnowledgeTypeManual, ParseStatus: types.ParseStatusPending,
	}
	repo := &attemptKnowledgeRepo{knowledge: knowledge}
	chunks := newAttemptChunkService()
	kbService := &rejectingAttemptKBService{}
	svc := &knowledgeService{
		repo: repo, tenantRepo: attemptTenantRepo{}, kbService: kbService,
		chunkService: chunks, spanTracker: &attemptLatestTracker{latest: 2},
	}
	payload, err := json.Marshal(types.ManualProcessPayload{
		TenantID: 7, KnowledgeID: knowledge.ID, KnowledgeBaseID: "kb-1",
		Content: "content", NeedCleanup: true, Attempt: 1,
	})
	require.NoError(t, err)

	err = svc.ProcessManualUpdate(context.Background(), asynq.NewTask(types.TypeManualProcess, payload))

	require.NoError(t, err)
	assert.Zero(t, kbService.calls)
	assert.Zero(t, repo.updateCalls)
	assert.Zero(t, chunks.repo.imageInfoCalls)
	assert.Zero(t, chunks.deleteCalls)
	assert.Equal(t, types.ParseStatusPending, knowledge.ParseStatus)
}
