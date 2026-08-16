package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupReparseCleanupRepo(t *testing.T) (*knowledgeRepository, *gorm.DB, string) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared&_busy_timeout=5000", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Tenant{}, &types.KnowledgeProcessingSpan{}))
	knowledge := &types.Knowledge{
		ID: "knowledge-1", TenantID: 7, KnowledgeBaseID: "kb-1", Type: "file",
		ParseStatus: types.ParseStatusPending, StorageSize: 4096, EmbeddingModelID: "old-model",
	}
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "tenant", StorageUsed: 10000}).Error)
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: knowledge.ID, Attempt: 1, SpanID: "root-1", Kind: types.SpanKindRoot,
		Name: "knowledge_processing", Status: types.SpanStatusRunning,
	}).Error)
	return &knowledgeRepository{db: db}, db, knowledge.ID
}

func completedCleanupCheckpoint(attempt int) types.ReparseCleanupCheckpoint {
	return types.ReparseCleanupCheckpoint{
		Version: 1, Attempt: attempt, Phase: types.ReparseCleanupPrepared,
		SourceEmbeddingModelID: "old-model", TargetEmbeddingModelID: "new-model",
		KnowledgeType: "file", EmbeddingDimensions: 1536,
		WikiPendingIngestScrubbed: true, VectorsDeleted: true, ChunksDeleted: true,
		ImagesDeleted: true, GraphDeleted: true,
	}
}

func TestCompleteReparseCleanupAdjustsStorageAndTargetModelExactlyOnce(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	requireCurrent, err := repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, completedCleanupCheckpoint(1))
	require.NoError(t, err)
	require.True(t, requireCurrent)

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, completeErr := repo.CompleteReparseCleanup(ctx, knowledgeID, 1)
			errs <- completeErr
		}()
	}
	wg.Wait()
	close(errs)
	for completeErr := range errs {
		require.NoError(t, completeErr)
	}

	var knowledge types.Knowledge
	require.NoError(t, db.First(&knowledge, "id = ?", knowledgeID).Error)
	require.Zero(t, knowledge.StorageSize)
	require.Equal(t, "new-model", knowledge.EmbeddingModelID)
	checkpoint, err := repo.GetReparseCleanupCheckpoint(ctx, knowledgeID, 1)
	require.NoError(t, err)
	require.Equal(t, types.ReparseCleanupCompleted, checkpoint.Phase)
	var tenant types.Tenant
	require.NoError(t, db.First(&tenant, 7).Error)
	require.EqualValues(t, 5904, tenant.StorageUsed)
}

func TestCompleteReparseCleanupRollsBackCheckpointModelAndStorageTogether(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	current, err := repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, completedCleanupCheckpoint(1))
	require.NoError(t, err)
	require.True(t, current)

	injectedErr := errors.New("injected tenant accounting failure")
	callbackName := "test:fail-reparse-tenant-accounting:" + t.Name()
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "tenants" {
			tx.AddError(injectedErr)
		}
	}))
	t.Cleanup(func() {
		require.NoError(t, db.Callback().Update().Remove(callbackName))
	})

	current, err = repo.CompleteReparseCleanup(ctx, knowledgeID, 1)
	require.ErrorIs(t, err, injectedErr)
	require.True(t, current)

	var knowledge types.Knowledge
	require.NoError(t, db.First(&knowledge, "id = ?", knowledgeID).Error)
	require.EqualValues(t, 4096, knowledge.StorageSize)
	require.Equal(t, "old-model", knowledge.EmbeddingModelID)
	checkpoint, loadErr := repo.GetReparseCleanupCheckpoint(ctx, knowledgeID, 1)
	require.NoError(t, loadErr)
	require.Equal(t, types.ReparseCleanupPrepared, checkpoint.Phase)
	var tenant types.Tenant
	require.NoError(t, db.First(&tenant, 7).Error)
	require.EqualValues(t, 10000, tenant.StorageUsed)
}

func TestFinalizeIndexedKnowledgeForAttemptAccountsStorageOnceAndRejectsStaleWorker(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	current, err := repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, completedCleanupCheckpoint(1))
	require.NoError(t, err)
	require.True(t, current)
	current, err = repo.CompleteReparseCleanup(ctx, knowledgeID, 1)
	require.NoError(t, err)
	require.True(t, current)

	var indexed types.Knowledge
	require.NoError(t, db.First(&indexed, "id = ?", knowledgeID).Error)
	indexed.ParseStatus = types.ParseStatusProcessing
	indexed.StorageSize = 2048
	for i := 0; i < 2; i++ {
		current, err = repo.FinalizeIndexedKnowledgeForAttempt(ctx, &indexed, 1)
		require.NoError(t, err)
		require.True(t, current)
	}
	var tenant types.Tenant
	require.NoError(t, db.First(&tenant, 7).Error)
	require.EqualValues(t, 7952, tenant.StorageUsed)

	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", knowledgeID, 1).
		Update("status", types.SpanStatusCancelled).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, Attempt: 2, SpanID: "root-2", Kind: types.SpanKindRoot,
		Name: "knowledge_processing", Status: types.SpanStatusRunning,
	}).Error)
	indexed.StorageSize = 4096
	indexed.ErrorMessage = "stale worker"
	current, err = repo.FinalizeIndexedKnowledgeForAttempt(ctx, &indexed, 1)
	require.NoError(t, err)
	require.False(t, current)

	var stored types.Knowledge
	require.NoError(t, db.First(&stored, "id = ?", knowledgeID).Error)
	require.EqualValues(t, 2048, stored.StorageSize)
	require.NotEqual(t, "stale worker", stored.ErrorMessage)
	require.NoError(t, db.First(&tenant, 7).Error)
	require.EqualValues(t, 7952, tenant.StorageUsed)
}

func TestReparseCleanupCheckpointsRemainAttemptScoped(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	cp1 := completedCleanupCheckpoint(1)
	cp1.Phase = types.ReparseCleanupPending
	cp1.EmbeddingDimensions = 0
	cp1.VectorsDeleted = false
	cp1.ChunksDeleted = false
	cp1.ImagesDeleted = false
	cp1.GraphDeleted = false
	requireCurrent, err := repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, cp1)
	require.NoError(t, err)
	require.True(t, requireCurrent)

	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", knowledgeID, 1).
		Updates(map[string]any{"status": types.SpanStatusCancelled, "error_code": "ATTEMPT_SUPERSEDED"}).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, Attempt: 2, SpanID: "root-2", Kind: types.SpanKindRoot,
		Name: "knowledge_processing", Status: types.SpanStatusRunning,
	}).Error)
	cp2 := cp1
	cp2.Attempt = 2
	cp2.SourceEmbeddingModelID = "newer-source"
	requireCurrent, err = repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 2, cp2)
	require.NoError(t, err)
	require.True(t, requireCurrent)

	got1, err := repo.GetReparseCleanupCheckpoint(ctx, knowledgeID, 1)
	require.NoError(t, err)
	require.Equal(t, "old-model", got1.SourceEmbeddingModelID)
	got2, err := repo.GetReparseCleanupCheckpoint(ctx, knowledgeID, 2)
	require.NoError(t, err)
	require.Equal(t, "newer-source", got2.SourceEmbeddingModelID)

	current, err := repo.CompleteReparseCleanup(ctx, knowledgeID, 1)
	require.NoError(t, err)
	require.False(t, current, "superseded attempt must not settle shared accounting")
}

func TestSaveKnowledgeIndexRouteSnapshotIsCurrentAndImmutable(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	snapshot := types.KnowledgeIndexRouteSnapshot{
		Version:          1,
		EmbeddingModelID: "embedding-model-a",
		EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: types.PostgresRetrieverEngineType,
			RetrieverType:       types.VectorRetrieverType,
		}},
	}

	current, err := repo.SaveKnowledgeIndexRouteSnapshot(ctx, knowledgeID, 1, snapshot)
	require.NoError(t, err)
	require.True(t, current)
	var firstRoot types.KnowledgeProcessingSpan
	require.NoError(t, db.Where(
		"knowledge_id = ? AND attempt = ? AND kind = ?",
		knowledgeID, 1, types.SpanKindRoot,
	).Take(&firstRoot).Error)
	stored, err := types.DecodeKnowledgeIndexRouteSnapshot(firstRoot.Input)
	require.NoError(t, err)
	require.Equal(t, &snapshot, stored)

	changedModel := snapshot
	changedModel.EmbeddingModelID = "embedding-model-b"
	current, err = repo.SaveKnowledgeIndexRouteSnapshot(ctx, knowledgeID, 1, changedModel)
	require.ErrorContains(t, err, "immutable embedding model")
	require.True(t, current)

	changedRoute := snapshot
	changedRoute.EffectiveEngines = []types.RetrieverEngineParams{{
		RetrieverEngineType: types.QdrantRetrieverEngineType,
		RetrieverType:       types.VectorRetrieverType,
	}}
	current, err = repo.SaveKnowledgeIndexRouteSnapshot(ctx, knowledgeID, 1, changedRoute)
	require.ErrorContains(t, err, "immutable route")
	require.True(t, current)

	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", knowledgeID, 1).
		Updates(map[string]any{
			"status":     types.SpanStatusCancelled,
			"error_code": "ATTEMPT_SUPERSEDED",
		}).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, Attempt: 2, SpanID: "root-2", Kind: types.SpanKindRoot,
		Name: "knowledge_processing", Status: types.SpanStatusRunning,
	}).Error)
	current, err = repo.SaveKnowledgeIndexRouteSnapshot(ctx, knowledgeID, 1, snapshot)
	require.NoError(t, err)
	require.False(t, current, "superseded attempt must not rewrite its accepted route")
}

func TestSaveReparseCleanupCheckpointMergesOutOfOrderDeliveryMonotonically(t *testing.T) {
	repo, _, knowledgeID := setupReparseCleanupRepo(t)
	ctx := context.Background()
	pending := completedCleanupCheckpoint(1)
	pending.Phase = types.ReparseCleanupPending
	pending.EmbeddingDimensions = 0
	pending.VectorsDeleted = false
	pending.ChunksDeleted = false
	pending.ImagesDeleted = false
	pending.GraphDeleted = false

	current, err := repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, pending)
	require.NoError(t, err)
	require.True(t, current)
	advanced := pending
	advanced.Phase = types.ReparseCleanupPrepared
	advanced.EmbeddingDimensions = 1536
	advanced.ImageURLs = []string{"local://image-1"}
	advanced.VectorsDeleted = true
	advanced.ImagesDeleted = true
	current, err = repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, advanced)
	require.NoError(t, err)
	require.True(t, current)

	stale := advanced
	stale.VectorsDeleted = false
	stale.ImagesDeleted = false
	current, err = repo.SaveReparseCleanupCheckpoint(ctx, knowledgeID, 1, stale)
	require.NoError(t, err)
	require.True(t, current)

	got, err := repo.GetReparseCleanupCheckpoint(ctx, knowledgeID, 1)
	require.NoError(t, err)
	require.True(t, got.VectorsDeleted)
	require.True(t, got.ImagesDeleted)
	require.Equal(t, types.ReparseCleanupPrepared, got.Phase)
	require.Equal(t, []string{"local://image-1"}, got.ImageURLs)
}

func TestUpdateKnowledgeForAttemptRejectsSupersededSubmission(t *testing.T) {
	repo, db, knowledgeID := setupReparseCleanupRepo(t)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", knowledgeID, 1).
		Update("status", types.SpanStatusCancelled).Error)
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, Attempt: 2, SpanID: "root-2", Kind: types.SpanKindRoot,
		Name: "knowledge_processing", Status: types.SpanStatusRunning,
	}).Error)
	var knowledge types.Knowledge
	require.NoError(t, db.First(&knowledge, "id = ?", knowledgeID).Error)
	knowledge.ParseStatus = types.ParseStatusFailed
	knowledge.ErrorMessage = "stale submission failure"

	current, err := repo.UpdateKnowledgeForAttempt(context.Background(), &knowledge, 1, false)
	require.NoError(t, err)
	require.False(t, current)
	var stored types.Knowledge
	require.NoError(t, db.First(&stored, "id = ?", knowledgeID).Error)
	require.NotEqual(t, types.ParseStatusFailed, stored.ParseStatus)
}
