package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type kbTaskCancelCall struct {
	kbID          string
	knowledgeIDs  []string
	dataSourceIDs []string
}

type recordingKBTaskInspector struct {
	repo                 *kbDeleteKBRepo
	calls                []kbTaskCancelCall
	cancelErr            error
	sawSoftDeletedRecord bool
}

func (r *recordingKBTaskInspector) CancelTasksForKnowledge(
	context.Context,
	string,
) (int, int, error) {
	return 0, 0, nil
}

func (r *recordingKBTaskInspector) HasQueuedTasksForKnowledge(context.Context, string) (bool, error) {
	return false, nil
}

func (r *recordingKBTaskInspector) QueueStats(context.Context) ([]types.QueueStat, bool, error) {
	return nil, true, nil
}

func (r *recordingKBTaskInspector) WorkerServerStats(context.Context) ([]types.WorkerServerStat, bool, error) {
	return nil, true, nil
}

func (r *recordingKBTaskInspector) CancelTasksForKnowledgeBase(
	_ context.Context,
	kbID string,
	knowledgeIDs []string,
	dataSourceIDs []string,
) (int, int, error) {
	r.calls = append(r.calls, kbTaskCancelCall{
		kbID:          kbID,
		knowledgeIDs:  append([]string(nil), knowledgeIDs...),
		dataSourceIDs: append([]string(nil), dataSourceIDs...),
	})
	if r.repo != nil && r.repo.deletedID == kbID {
		r.sawSoftDeletedRecord = true
	}
	return 0, 0, r.cancelErr
}

var (
	_ interfaces.TaskInspector              = (*recordingKBTaskInspector)(nil)
	_ interfaces.KnowledgeBaseTaskCanceller = (*recordingKBTaskInspector)(nil)
)

type recordingKBDeleteEnqueuer struct {
	calls         int
	task          *asynq.Task
	opts          []asynq.Option
	info          *asynq.TaskInfo
	err           error
	returnNilInfo bool
}

type recordingKBPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	scopeIDs  []string
	deleteErr error
}

func (r *recordingKBPendingRepo) DeleteByScope(_ context.Context, scope, scopeID string) error {
	if scope == types.TaskScopeKnowledgeBase {
		r.scopeIDs = append(r.scopeIDs, scopeID)
	}
	return r.deleteErr
}

func (r *recordingKBDeleteEnqueuer) Enqueue(
	task *asynq.Task,
	opts ...asynq.Option,
) (*asynq.TaskInfo, error) {
	r.calls++
	r.task = task
	r.opts = append([]asynq.Option(nil), opts...)
	if r.err != nil || r.info != nil || r.returnNilInfo {
		return r.info, r.err
	}
	return &asynq.TaskInfo{ID: "kb-delete-task"}, nil
}

func TestDeleteKnowledgeBaseRetainsPreparedOutboxWhenPublishFails(t *testing.T) {
	for _, test := range []struct {
		name     string
		enqueuer *recordingKBDeleteEnqueuer
		wantErr  string
	}{
		{name: "enqueue error", enqueuer: &recordingKBDeleteEnqueuer{err: errors.New("queue offline")}, wantErr: "queue offline"},
		{name: "nil info", enqueuer: &recordingKBDeleteEnqueuer{returnNilInfo: true}, wantErr: "no task info"},
	} {
		t.Run(test.name, func(t *testing.T) {
			const kbID = "kb-durable-publish"
			repo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
			repo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}
			svc := &knowledgeBaseService{repo: repo, asynqClient: test.enqueuer}

			err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)

			require.ErrorContains(t, err, test.wantErr)
			require.NotNil(t, repo.prepared, "durable outbox must commit before immediate publish")
			assert.Equal(t, types.TaskScopeKnowledgeBaseDeletion, repo.prepared.Scope)
			assert.Equal(t, "kb-delete:1:"+kbID, repo.prepared.DedupKey)
			assert.Equal(t, kbID, repo.deletedID)
		})
	}
}

func optionValue(opts []asynq.Option, optionType asynq.OptionType) any {
	for _, opt := range opts {
		if opt.Type() == optionType {
			return opt.Value()
		}
	}
	return nil
}

func TestDeleteKnowledgeBaseForwardsDataSourceTaskScope(t *testing.T) {
	const kbID = "kb-with-datasource"
	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}
	inspector := &recordingKBTaskInspector{repo: kbRepo}
	enqueuer := &recordingKBDeleteEnqueuer{}
	dsRepo := newKBDeleteDSRepo(kbID, &types.DataSource{ID: "datasource-1", KnowledgeBaseID: kbID})
	svc := &knowledgeBaseService{
		repo:          kbRepo,
		asynqClient:   enqueuer,
		taskInspector: inspector,
		dsRepo:        dsRepo,
	}

	err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	assert.Empty(t, inspector.calls[0].dataSourceIDs)
	assert.Equal(t, []string{"datasource-1"}, inspector.calls[1].dataSourceIDs)
	require.NotNil(t, enqueuer.task)
	assert.Equal(t, "kb-delete:1:"+kbID, optionValue(enqueuer.opts, asynq.TaskIDOpt))
	assert.Equal(t, 3, optionValue(enqueuer.opts, asynq.MaxRetryOpt))
	var payload types.KBDeletePayload
	require.NoError(t, json.Unmarshal(enqueuer.task.Payload(), &payload))
	assert.Equal(t, []string{"datasource-1"}, payload.DataSourceIDs)
}

func TestDeleteKnowledgeBaseCancelsQueuedTasksBestEffort(t *testing.T) {
	tests := []struct {
		name       string
		cancelErr  error
		pendingErr error
	}{
		{name: "success"},
		{name: "inspector failure", cancelErr: errors.New("redis unavailable")},
		{name: "durable queue failure", pendingErr: errors.New("database unavailable")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const kbID = "kb-task-cleanup"
			kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
			kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}
			inspector := &recordingKBTaskInspector{repo: kbRepo, cancelErr: tt.cancelErr}
			pendingRepo := &recordingKBPendingRepo{deleteErr: tt.pendingErr}
			enqueuer := &recordingKBDeleteEnqueuer{}
			svc := &knowledgeBaseService{
				repo:            kbRepo,
				asynqClient:     enqueuer,
				taskInspector:   inspector,
				taskPendingRepo: pendingRepo,
			}

			err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)

			require.NoError(t, err)
			require.Len(t, inspector.calls, 1)
			assert.Equal(t, kbID, inspector.calls[0].kbID)
			assert.Empty(t, inspector.calls[0].knowledgeIDs)
			assert.True(t, inspector.sawSoftDeletedRecord)
			assert.Equal(t, []string{kbID}, pendingRepo.scopeIDs)
			assert.Equal(t, 1, enqueuer.calls)
		})
	}
}

type emptyKBKnowledgeRepo struct {
	interfaces.KnowledgeRepository
}

func (emptyKBKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	uint64,
	string,
) ([]*types.Knowledge, error) {
	return nil, nil
}

func TestProcessKBDeleteRepeatsQueueCleanup(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	pendingRepo := &recordingKBPendingRepo{}
	svc := &knowledgeBaseService{
		kgRepo:          emptyKBKnowledgeRepo{},
		taskInspector:   inspector,
		taskPendingRepo: pendingRepo,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb-race"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	for _, call := range inspector.calls {
		assert.Equal(t, "kb-race", call.kbID)
		assert.Empty(t, call.knowledgeIDs)
	}
	assert.Equal(t, []string{"kb-race", "kb-race"}, pendingRepo.scopeIDs)
}

type populatedKBKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	items []*types.Knowledge
}

func (r populatedKBKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	uint64,
	string,
) ([]*types.Knowledge, error) {
	return r.items, nil
}

func (populatedKBKnowledgeRepo) DeleteKnowledgeList(context.Context, uint64, []string) error {
	return nil
}

type kbCleanupChunkRepo struct {
	interfaces.ChunkRepository
}

func (kbCleanupChunkRepo) ListImageInfoByKnowledgeIDs(
	context.Context,
	uint64,
	[]string,
) ([]interfaces.ChunkImageInfo, error) {
	return nil, nil
}

func (kbCleanupChunkRepo) DeleteChunksByKnowledgeID(context.Context, uint64, string) error {
	return nil
}

type kbCleanupModelService struct {
	interfaces.ModelService
	err error
}

func (s kbCleanupModelService) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return kbCleanupEmbedder{}, s.err
}

type kbDeleteFinalizerRepo struct {
	interfaces.KnowledgeBaseRepository
	finalizeCalls   int
	tenantID        uint64
	kbID            string
	storeID         string
	authorizeCalls  int
	authorizeErr    error
	expectedPayload *types.KBDeletePayload
}

func (r *kbDeleteFinalizerRepo) AuthorizeKnowledgeBaseDeletion(
	_ context.Context, _ uint64, _ string, _ string, payload *types.KBDeletePayload,
) error {
	r.authorizeCalls++
	if r.expectedPayload != nil && !reflect.DeepEqual(*r.expectedPayload, *payload) {
		return errors.New("executing payload does not match durable snapshot")
	}
	return r.authorizeErr
}

func (r *kbDeleteFinalizerRepo) FinalizeKnowledgeBaseDeletion(
	_ context.Context, tenantID uint64, kbID, storeID string,
) error {
	r.finalizeCalls++
	r.tenantID, r.kbID, r.storeID = tenantID, kbID, storeID
	return nil
}

type kbDeleteOutboxAcker struct {
	interfaces.TaskPendingOpsRepository
	err   error
	calls int
}

func (a *kbDeleteOutboxAcker) AckKnowledgeBaseDeletion(
	context.Context, uint64, string, string,
) error {
	a.calls++
	return a.err
}

type kbDeleteVectorEngine struct {
	interfaces.RetrieveEngineService
	err               error
	sourceDeleteCalls *int
}

func (kbDeleteVectorEngine) EngineType() types.RetrieverEngineType {
	return types.RetrieverEngineType("kb-delete-test")
}
func (kbDeleteVectorEngine) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType}
}
func (e kbDeleteVectorEngine) DeleteByKnowledgeIDList(
	context.Context, []string, int, string,
) error {
	return e.err
}
func (e kbDeleteVectorEngine) DeleteBySourceIDList(
	context.Context, []string, int, string,
) error {
	if e.sourceDeleteCalls != nil {
		(*e.sourceDeleteCalls)++
	}
	return e.err
}

type kbDeleteManifestRepo struct {
	interfaces.QuestionGenerationManifestRepository
	mu        sync.Mutex
	manifests []*types.QuestionGenerationManifest
	deletes   int
}

func (r *kbDeleteManifestRepo) ListQuestionGenerationManifestsByKnowledgeBase(
	context.Context, uint64, string,
) ([]*types.QuestionGenerationManifest, error) {
	return r.manifests, nil
}
func (r *kbDeleteManifestRepo) WithQuestionGenerationGuard(
	ctx context.Context, _ types.QuestionGenerationManifestKey, fn func(context.Context) error,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(ctx)
}
func (r *kbDeleteManifestRepo) DeleteQuestionGenerationManifest(
	context.Context, types.QuestionGenerationManifestKey,
) error {
	r.deletes++
	return nil
}

type kbDeleteRegistry struct {
	interfaces.RetrieveEngineRegistry
	engine interfaces.RetrieveEngineService
}

func (r kbDeleteRegistry) GetRetrieveEngineService(types.RetrieverEngineType) (
	interfaces.RetrieveEngineService, error,
) {
	return r.engine, nil
}

type kbCleanupEmbedder struct{}

func (kbCleanupEmbedder) Embed(context.Context, string) ([]float32, error) { return nil, nil }
func (kbCleanupEmbedder) BatchEmbed(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (kbCleanupEmbedder) GetModelName() string { return "test" }
func (kbCleanupEmbedder) GetDimensions() int   { return 1 }
func (kbCleanupEmbedder) GetModelID() string   { return "test" }
func (kbCleanupEmbedder) BatchEmbedWithPool(
	context.Context,
	embedding.Embedder,
	[]string,
) ([][]float32, error) {
	return nil, nil
}

func TestProcessKBDeleteCollectsKnowledgeIDsForEveryScrub(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	svc := &knowledgeBaseService{
		kgRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
			{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
			{ID: "knowledge-2", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
		}},
		chunkRepo:     kbCleanupChunkRepo{},
		modelService:  kbCleanupModelService{},
		taskInspector: inspector,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb-1"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	for _, call := range inspector.calls {
		assert.Equal(t, []string{"knowledge-1", "knowledge-2"}, call.knowledgeIDs)
	}
}

func TestProcessKBDeleteModelFailureDoesNotDeleteRowsOrFinalize(t *testing.T) {
	knowledgeRepo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	finalizer := &kbDeleteFinalizerRepo{}
	svc := &knowledgeBaseService{
		repo: finalizer, kgRepo: knowledgeRepo, chunkRepo: kbCleanupChunkRepo{},
		modelService: kbCleanupModelService{err: errors.New("model unavailable")},
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb-1"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))
	require.ErrorContains(t, err, "model unavailable")
	assert.Equal(t, 0, knowledgeRepo.deleteCalls)
	assert.Equal(t, 0, finalizer.finalizeCalls)
}

func TestProcessKBDeleteVectorFailureDoesNotDeleteRowsOrFinalize(t *testing.T) {
	knowledgeRepo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	finalizer := &kbDeleteFinalizerRepo{}
	engineType := types.RetrieverEngineType("kb-delete-test")
	svc := &knowledgeBaseService{
		repo: finalizer, kgRepo: knowledgeRepo, chunkRepo: kbCleanupChunkRepo{},
		modelService:   kbCleanupModelService{},
		retrieveEngine: kbDeleteRegistry{engine: kbDeleteVectorEngine{err: errors.New("vector delete failed")}},
	}
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID: 1, KnowledgeBaseID: "kb-1",
		EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: engineType, RetrieverType: types.VectorRetrieverType,
		}},
	})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))
	require.ErrorContains(t, err, "vector delete failed")
	assert.Equal(t, 0, knowledgeRepo.deleteCalls)
	assert.Equal(t, 0, finalizer.finalizeCalls)
}

func TestProcessKBDeleteEmptyKnowledgeListFinalizesCrashRetryResidue(t *testing.T) {
	finalizer := &kbDeleteFinalizerRepo{}
	acker := &kbDeleteOutboxAcker{}
	svc := &knowledgeBaseService{
		repo: finalizer, kgRepo: populatedKBKnowledgeRepo{}, taskPendingRepo: acker,
	}
	storeID := "store-A"
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID: 7, KnowledgeBaseID: "kb-1", VectorStoreID: &storeID,
	})
	require.NoError(t, err)

	require.NoError(t, svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload)))
	assert.Equal(t, 1, finalizer.finalizeCalls)
	assert.Equal(t, uint64(7), finalizer.tenantID)
	assert.Equal(t, "kb-1", finalizer.kbID)
	assert.Equal(t, "store-A", finalizer.storeID)
	assert.Equal(t, 1, acker.calls)
}

func TestProcessKBDeleteAckFailureRetriesAfterFinalization(t *testing.T) {
	finalizer := &kbDeleteFinalizerRepo{}
	acker := &kbDeleteOutboxAcker{err: errors.New("ack database unavailable")}
	svc := &knowledgeBaseService{
		repo: finalizer, kgRepo: populatedKBKnowledgeRepo{}, taskPendingRepo: acker,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 7, KnowledgeBaseID: "kb-ack"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.ErrorContains(t, err, "ack database unavailable")
	assert.Equal(t, 1, finalizer.finalizeCalls)
	assert.Equal(t, 1, acker.calls)
}

func TestDrainQuestionGenerationManifestsWaitsForPublisherAndHandlesStaleListedRow(t *testing.T) {
	manifest := &types.QuestionGenerationManifest{
		TenantID: 1, KnowledgeBaseID: "kb-1", KnowledgeID: "knowledge-1", ChunkID: "chunk-1",
		ContentRevision: 1, BatchIndex: 0, VectorStoreID: "",
		EmbeddingDimension: 1, KnowledgeType: "document",
		EffectiveEngines: types.JSON(`[{"retriever_engine_type":"kb-delete-test","retriever_type":"vector"}]`),
		DesiredSourceIDs: types.JSON(`["desired"]`), AbandonedSourceIDs: types.JSON(`["abandoned"]`),
	}
	manifestRepo := &kbDeleteManifestRepo{manifests: []*types.QuestionGenerationManifest{manifest}}
	entered, release := make(chan struct{}), make(chan struct{})
	go func() {
		_ = manifestRepo.WithQuestionGenerationGuard(context.Background(), manifest.Key(), func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	sourceDeletes := 0
	svc := &knowledgeBaseService{
		questionManifestRepo: manifestRepo,
		retrieveEngine:       kbDeleteRegistry{engine: kbDeleteVectorEngine{sourceDeleteCalls: &sourceDeletes}},
	}
	done := make(chan error, 1)
	go func() { done <- svc.drainQuestionGenerationManifests(context.Background(), 1, "kb-1") }()
	select {
	case err := <-done:
		t.Fatalf("cleanup escaped publisher guard: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, 1, sourceDeletes)
	assert.Equal(t, 1, manifestRepo.deletes, "stale listed delete must remain idempotent")
}

// kbDeleteDeferredRegistry reports a retryable engine-resolution failure from
// the rebuild path, matching what GetOrLoadByStoreID does when the caller
// goes away or the store engine cannot be produced yet.
type kbDeleteDeferredRegistry struct {
	err error
}

func (kbDeleteDeferredRegistry) Register(interfaces.RetrieveEngineService) error { return nil }
func (kbDeleteDeferredRegistry) GetRetrieveEngineService(types.RetrieverEngineType) (
	interfaces.RetrieveEngineService, error,
) {
	return nil, nil
}
func (kbDeleteDeferredRegistry) GetAllRetrieveEngineServices() []interfaces.RetrieveEngineService {
	return nil
}
func (kbDeleteDeferredRegistry) GetByStoreID(string) (interfaces.RetrieveEngineService, error) {
	return nil, errors.New("store not in registry")
}
func (r kbDeleteDeferredRegistry) GetOrLoadByStoreID(
	context.Context, uint64, string,
) (interfaces.RetrieveEngineService, error) {
	return nil, r.err
}

type kbDeleteOwnership struct {
	owned map[string]uint64
}

func (o *kbDeleteOwnership) StoreOwnedBy(_ context.Context, storeID string, tenantID uint64) (bool, error) {
	owner, ok := o.owned[storeID]
	return ok && owner == tenantID, nil
}

type kbDeleteTrackingKnowledgeRepo struct {
	populatedKBKnowledgeRepo
	deleteCalls int
}

func (r *kbDeleteTrackingKnowledgeRepo) DeleteKnowledgeList(context.Context, uint64, []string) error {
	r.deleteCalls++
	return nil
}

func TestProcessKBDeleteEngineResolutionFailureRetries(t *testing.T) {
	const storeID = "00000000-0000-0000-0000-0000000000dd"
	storeIDPtr := storeID
	repo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	svc := &knowledgeBaseService{
		kgRepo:         repo,
		chunkRepo:      kbCleanupChunkRepo{},
		modelService:   kbCleanupModelService{},
		retrieveEngine: kbDeleteDeferredRegistry{err: context.Canceled},
		ownership:      &kbDeleteOwnership{owned: map[string]uint64{storeID: 1}},
	}
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		VectorStoreID:   &storeIDPtr,
	})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, repo.deleteCalls, "knowledge rows must not be deleted when engine resolution is deferred")
}

func TestProcessKBDeleteUnavailableStoreRetries(t *testing.T) {
	const storeID = "00000000-0000-0000-0000-0000000000ee"
	storeIDPtr := storeID
	repo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	svc := &knowledgeBaseService{
		kgRepo:         repo,
		chunkRepo:      kbCleanupChunkRepo{},
		modelService:   kbCleanupModelService{},
		retrieveEngine: kbDeleteDeferredRegistry{err: retriever.ErrVectorStoreUnavailable},
		ownership:      &kbDeleteOwnership{owned: map[string]uint64{storeID: 1}},
	}
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		VectorStoreID:   &storeIDPtr,
	})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.ErrorIs(t, err, retriever.ErrVectorStoreUnavailable)
	assert.Equal(t, 0, repo.deleteCalls, "knowledge rows must not be deleted when engine resolution is deferred")
}

func TestCancelTasksForKnowledgeBaseForwardsKnowledgeIDs(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	svc := &knowledgeBaseService{taskInspector: inspector}

	svc.cancelTasksForKnowledgeBase(
		context.Background(),
		"kb-1",
		[]string{"knowledge-1", "knowledge-2"},
		[]string{"datasource-1"},
	)

	require.Len(t, inspector.calls, 1)
	assert.Equal(t, "kb-1", inspector.calls[0].kbID)
	assert.Equal(t, []string{"knowledge-1", "knowledge-2"}, inspector.calls[0].knowledgeIDs)
	assert.Equal(t, []string{"datasource-1"}, inspector.calls[0].dataSourceIDs)
}
