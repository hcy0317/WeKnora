package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type kbDeleteDSRepo struct {
	mu        sync.Mutex
	byKB      map[string][]*types.DataSource
	deleted   map[string]bool
	deleteIDs []string
	findErr   error
}

func newKBDeleteDSRepo(kbID string, ds ...*types.DataSource) *kbDeleteDSRepo {
	r := &kbDeleteDSRepo{
		byKB:    map[string][]*types.DataSource{kbID: ds},
		deleted: map[string]bool{},
	}
	return r
}

func (r *kbDeleteDSRepo) Create(_ context.Context, _ *types.DataSource) error { return nil }
func (r *kbDeleteDSRepo) FindByID(_ context.Context, id string) (*types.DataSource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleted[id] {
		return nil, errors.New("data source not found")
	}
	for _, list := range r.byKB {
		for _, ds := range list {
			if ds.ID == id {
				return ds, nil
			}
		}
	}
	return nil, errors.New("data source not found")
}
func (r *kbDeleteDSRepo) FindByKnowledgeBase(_ context.Context, kbID string) ([]*types.DataSource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.findErr != nil {
		return nil, r.findErr
	}
	var active []*types.DataSource
	for _, ds := range r.byKB[kbID] {
		if !r.deleted[ds.ID] {
			active = append(active, ds)
		}
	}
	return active, nil
}
func (r *kbDeleteDSRepo) Update(_ context.Context, _ *types.DataSource) error { return nil }
func (r *kbDeleteDSRepo) UpdateSyncState(_ context.Context, _ *types.DataSource) error {
	return nil
}
func (r *kbDeleteDSRepo) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted[id] = true
	r.deleteIDs = append(r.deleteIDs, id)
	return nil
}
func (r *kbDeleteDSRepo) FindActive(_ context.Context) ([]*types.DataSource, error) {
	return nil, nil
}

var _ interfaces.DataSourceRepository = (*kbDeleteDSRepo)(nil)

type kbDeleteSyncLogRepo struct {
	mu       sync.Mutex
	canceled []string
}

func (r *kbDeleteSyncLogRepo) Create(_ context.Context, _ *types.SyncLog) error { return nil }
func (r *kbDeleteSyncLogRepo) FindByID(_ context.Context, _ string) (*types.SyncLog, error) {
	return nil, errors.New("not found")
}
func (r *kbDeleteSyncLogRepo) FindByDataSource(_ context.Context, _ string, _, _ int) ([]*types.SyncLog, error) {
	return nil, nil
}
func (r *kbDeleteSyncLogRepo) FindLatest(_ context.Context, _ string) (*types.SyncLog, error) {
	return nil, nil
}
func (r *kbDeleteSyncLogRepo) HasRunningSync(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (r *kbDeleteSyncLogRepo) Update(_ context.Context, _ *types.SyncLog) error { return nil }
func (r *kbDeleteSyncLogRepo) UpdateResult(_ context.Context, _ *types.SyncLog) error {
	return nil
}
func (r *kbDeleteSyncLogRepo) CancelPendingByDataSource(_ context.Context, dsID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.canceled = append(r.canceled, dsID)
	return nil
}
func (r *kbDeleteSyncLogRepo) CleanupOldLogs(_ context.Context, _ int) error { return nil }

var _ interfaces.SyncLogRepository = (*kbDeleteSyncLogRepo)(nil)

type kbDeleteKBRepo struct {
	fakeKBRepo
	deletedID  string
	prepared   *types.TaskPendingOp
	prepareErr error
}

func (r *kbDeleteKBRepo) DeleteKnowledgeBase(_ context.Context, id string) error {
	r.deletedID = id
	delete(r.rows, id)
	return nil
}

func (r *kbDeleteKBRepo) PrepareKnowledgeBaseDeletion(
	ctx context.Context, _ uint64, id string, op *types.TaskPendingOp,
) error {
	if r.prepareErr != nil {
		return r.prepareErr
	}
	copy := *op
	copy.Payload = append([]byte(nil), op.Payload...)
	r.prepared = &copy
	return r.DeleteKnowledgeBase(ctx, id)
}

type kbDeleteTaskEnqueuer struct{}

func (kbDeleteTaskEnqueuer) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	return &asynq.TaskInfo{ID: "kb-delete-task"}, nil
}

type kbDeleteShareRepo struct {
	interfaces.KBShareRepository
	calls int
	err   error
}

func (r *kbDeleteShareRepo) DeleteByKnowledgeBaseID(context.Context, string) error {
	r.calls++
	return r.err
}

func TestDeleteDataSourcesForKnowledgeBase(t *testing.T) {
	const kbID = "kb-1"
	dsRepo := newKBDeleteDSRepo(kbID,
		&types.DataSource{ID: "ds-1", KnowledgeBaseID: kbID, Status: types.DataSourceStatusActive, SyncSchedule: "0 0 * * * *"},
		&types.DataSource{ID: "ds-2", KnowledgeBaseID: kbID, Status: types.DataSourceStatusActive},
	)
	syncLogRepo := &kbDeleteSyncLogRepo{}
	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}

	scheduler := datasource.NewScheduler(dsRepo, syncLogRepo, kbDeleteTaskEnqueuer{})
	require.NoError(t, scheduler.AddOrUpdate(dsRepo.byKB[kbID][0]))

	svc := &knowledgeBaseService{
		dsRepo:      dsRepo,
		syncLogRepo: syncLogRepo,
		dsScheduler: scheduler,
	}

	svc.deleteDataSourcesForKnowledgeBase(ctxWithTenant(1), kbID)

	assert.ElementsMatch(t, []string{"ds-1", "ds-2"}, dsRepo.deleteIDs)
	assert.ElementsMatch(t, []string{"ds-1", "ds-2"}, syncLogRepo.canceled)
	assert.Equal(t, 0, scheduler.EntryCount())
}

func TestDeleteKnowledgeBaseCleansUpDataSources(t *testing.T) {
	const kbID = "kb-1"
	dsRepo := newKBDeleteDSRepo(kbID,
		&types.DataSource{ID: "ds-1", KnowledgeBaseID: kbID, Status: types.DataSourceStatusActive, SyncSchedule: "0 0 * * * *"},
	)
	syncLogRepo := &kbDeleteSyncLogRepo{}
	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}

	scheduler := datasource.NewScheduler(dsRepo, syncLogRepo, kbDeleteTaskEnqueuer{})
	require.NoError(t, scheduler.AddOrUpdate(dsRepo.byKB[kbID][0]))

	svc := &knowledgeBaseService{
		repo:        kbRepo,
		shareRepo:   nil,
		asynqClient: kbDeleteTaskEnqueuer{},
		dsRepo:      dsRepo,
		syncLogRepo: syncLogRepo,
		dsScheduler: scheduler,
	}

	ctx := ctxWithTenantStorage(1, "local")
	err := svc.DeleteKnowledgeBase(ctx, kbID)
	require.NoError(t, err)

	assert.Equal(t, kbID, kbRepo.deletedID)
	assert.Equal(t, []string{"ds-1"}, dsRepo.deleteIDs)
	assert.Equal(t, []string{"ds-1"}, syncLogRepo.canceled)
	assert.Equal(t, 0, scheduler.EntryCount())
}

func TestDeleteDataSourcesForKnowledgeBaseContinuesOnDeleteError(t *testing.T) {
	const kbID = "kb-2"
	dsRepo := &deleteErrDSRepo{
		kbDeleteDSRepo: *newKBDeleteDSRepo(kbID, &types.DataSource{ID: "ds-bad", KnowledgeBaseID: kbID}),
		deleteErr:      errors.New("db unavailable"),
	}

	svc := &knowledgeBaseService{
		dsRepo:      dsRepo,
		syncLogRepo: &kbDeleteSyncLogRepo{},
	}

	svc.deleteDataSourcesForKnowledgeBase(context.Background(), kbID)
	assert.Empty(t, dsRepo.deleteIDs)
}

func TestDeleteKnowledgeBaseContinuesWhenDataSourceCleanupFails(t *testing.T) {
	const kbID = "kb-2"
	dsRepo := &deleteErrDSRepo{
		kbDeleteDSRepo: *newKBDeleteDSRepo(kbID, &types.DataSource{ID: "ds-bad", KnowledgeBaseID: kbID}),
		deleteErr:      errors.New("db unavailable"),
	}

	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}

	svc := &knowledgeBaseService{
		repo:        kbRepo,
		asynqClient: kbDeleteTaskEnqueuer{},
		dsRepo:      dsRepo,
		syncLogRepo: &kbDeleteSyncLogRepo{},
	}

	err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)
	require.NoError(t, err)
	assert.Equal(t, kbID, kbRepo.deletedID)
}

func TestProcessKBDeleteFailsClosedOnDataSourceRepositoryErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		repo interfaces.DataSourceRepository
		want string
	}{
		{
			name: "find",
			repo: &kbDeleteDSRepo{byKB: map[string][]*types.DataSource{}, deleted: map[string]bool{}, findErr: errors.New("find failed")},
			want: "find failed",
		},
		{
			name: "delete",
			repo: &deleteErrDSRepo{
				kbDeleteDSRepo: *newKBDeleteDSRepo("kb-worker", &types.DataSource{ID: "ds-1", KnowledgeBaseID: "kb-worker"}),
				deleteErr:      errors.New("delete failed"),
			},
			want: "delete failed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc := &knowledgeBaseService{dsRepo: test.repo, kgRepo: emptyKBKnowledgeRepo{}}
			payload, err := json.Marshal(types.KBDeletePayload{TenantID: 7, KnowledgeBaseID: "kb-worker"})
			require.NoError(t, err)

			err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestProcessKBDeleteReplaysRequestSideShareAndDataSourceCleanup(t *testing.T) {
	const kbID = "kb-crash-after-prepare"
	dsRepo := newKBDeleteDSRepo(kbID, &types.DataSource{ID: "ds-residue", KnowledgeBaseID: kbID})
	shareRepo := &kbDeleteShareRepo{}
	finalizer := &kbDeleteFinalizerRepo{}
	acker := &kbDeleteOutboxAcker{}
	svc := &knowledgeBaseService{
		repo: finalizer, kgRepo: emptyKBKnowledgeRepo{}, taskPendingRepo: acker,
		shareRepo: shareRepo, dsRepo: dsRepo,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 7, KnowledgeBaseID: kbID})
	require.NoError(t, err)

	require.NoError(t, svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload)))

	assert.Equal(t, 1, shareRepo.calls)
	assert.Equal(t, []string{"ds-residue"}, dsRepo.deleteIDs)
	assert.Equal(t, 1, finalizer.finalizeCalls)
	assert.Equal(t, 1, acker.calls)
}

func TestProcessKBDeleteAuthorizationFailureHasNoDestructiveSideEffects(t *testing.T) {
	const kbID = "kb-active-or-forged"
	canonical := types.KBDeletePayload{TenantID: 7, KnowledgeBaseID: kbID}
	storeID := "forged-store"
	for _, altered := range []types.KBDeletePayload{
		{TenantID: 7, KnowledgeBaseID: kbID, VectorStoreID: &storeID},
		{TenantID: 7, KnowledgeBaseID: kbID, DataSourceIDs: []string{"forged-ds"}},
		{TenantID: 7, KnowledgeBaseID: kbID, EffectiveEngines: []types.RetrieverEngineParams{{RetrieverEngineType: "forged", RetrieverType: types.VectorRetrieverType}}},
	} {
		dsRepo := newKBDeleteDSRepo(kbID, &types.DataSource{ID: "ds-protected", KnowledgeBaseID: kbID})
		shareRepo := &kbDeleteShareRepo{}
		finalizer := &kbDeleteFinalizerRepo{expectedPayload: &canonical}
		knowledgeRepo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
			{ID: "knowledge-protected", KnowledgeBaseID: kbID, EmbeddingModelID: "model"},
		}}}
		svc := &knowledgeBaseService{
			repo: finalizer, kgRepo: knowledgeRepo, shareRepo: shareRepo, dsRepo: dsRepo,
		}
		payload, err := json.Marshal(altered)
		require.NoError(t, err)

		err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

		require.ErrorContains(t, err, "authorize durable KB deletion")
		assert.Equal(t, 1, finalizer.authorizeCalls)
		assert.Zero(t, shareRepo.calls)
		assert.Empty(t, dsRepo.deleteIDs)
		assert.Zero(t, knowledgeRepo.deleteCalls)
		assert.Zero(t, finalizer.finalizeCalls)
	}
}

// deleteErrDSRepo injects a delete failure for testing best-effort cleanup.
type deleteErrDSRepo struct {
	kbDeleteDSRepo
	deleteErr error
}

func (r *deleteErrDSRepo) Delete(_ context.Context, id string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	return r.kbDeleteDSRepo.Delete(context.Background(), id)
}
