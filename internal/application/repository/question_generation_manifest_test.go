package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupQuestionGenerationManifestRepo(t *testing.T) (*questionGenerationManifestRepository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared&_busy_timeout=5000"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.QuestionGenerationManifest{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "test"}).Error)
	return &questionGenerationManifestRepository{db: db}, db
}

func manifestJSON(t *testing.T, value any) types.JSON {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return types.JSON(data)
}

func manifestCandidate(t *testing.T, question string) *types.QuestionGenerationManifest {
	t.Helper()
	return &types.QuestionGenerationManifest{
		ID: uuid.NewString(), TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", ChunkID: "chunk",
		ContentRevision: 3, BatchIndex: 5, Attempt: 9,
		IdentityVersion: 1, GenerationKey: "knowledge:chunk:3:5", TaskID: "task-9",
		VectorStoreID: "store", EmbeddingModelID: "embedding", EmbeddingDimension: 1536,
		KnowledgeType:      "document",
		EffectiveEngines:   manifestJSON(t, []types.RetrieverEngineParams{}),
		State:              types.QuestionGenerationManifestPrepared,
		Questions:          manifestJSON(t, []types.GeneratedQuestion{{ID: "qid", Question: question}}),
		IndexEntries:       manifestJSON(t, []*types.IndexInfo{{Content: question, SourceID: "desired"}}),
		DesiredSourceIDs:   manifestJSON(t, []string{"desired"}),
		AbandonedSourceIDs: manifestJSON(t, []string{"old"}),
	}
}

func TestQuestionGenerationManifestRepositoryGetOrCreateKeepsCanonicalWinner(t *testing.T) {
	repo, _ := setupQuestionGenerationManifestRepo(t)
	first, created, err := repo.GetOrCreateQuestionGenerationManifest(context.Background(), manifestCandidate(t, "first"))
	require.NoError(t, err)
	require.True(t, created)

	second, created, err := repo.GetOrCreateQuestionGenerationManifest(context.Background(), manifestCandidate(t, "second"))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.ID, second.ID)
	require.JSONEq(t, string(first.Questions), string(second.Questions))
	require.Contains(t, string(second.Questions), "first")
}

func TestQuestionGenerationManifestRepositoryRejectsExistingAndNewAfterKnowledgeBaseSoftDelete(t *testing.T) {
	repo, db := setupQuestionGenerationManifestRepo(t)
	existing := manifestCandidate(t, "existing")
	_, _, err := repo.GetOrCreateQuestionGenerationManifest(context.Background(), existing)
	require.NoError(t, err)
	require.NoError(t, db.Where("tenant_id = ? AND id = ?", 7, "kb").Delete(&types.KnowledgeBase{}).Error)

	_, _, err = repo.GetOrCreateQuestionGenerationManifest(context.Background(), existing)
	require.ErrorContains(t, err, "deleted or unavailable")
	newCandidate := manifestCandidate(t, "new")
	newCandidate.ChunkID = "chunk-new"
	newCandidate.GenerationKey = "knowledge:chunk-new:3:5"
	_, _, err = repo.GetOrCreateQuestionGenerationManifest(context.Background(), newCandidate)
	require.ErrorContains(t, err, "deleted or unavailable")
}

func TestQuestionGenerationManifestRepositoryConcurrentCreateHasOneCanonicalSet(t *testing.T) {
	repo, db := setupQuestionGenerationManifestRepo(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)

	const workers = 8
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			manifest, _, createErr := repo.GetOrCreateQuestionGenerationManifest(
				context.Background(), manifestCandidate(t, string(rune('a'+index))))
			if createErr != nil {
				errs <- createErr
				return
			}
			ids <- manifest.ID
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var winner string
	for id := range ids {
		if winner == "" {
			winner = id
		}
		require.Equal(t, winner, id)
	}
	var count int64
	require.NoError(t, db.Model(&types.QuestionGenerationManifest{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestQuestionGenerationManifestRepositoryAbortAndDeleteAreKeyScoped(t *testing.T) {
	repo, _ := setupQuestionGenerationManifestRepo(t)
	manifest, _, err := repo.GetOrCreateQuestionGenerationManifest(context.Background(), manifestCandidate(t, "first"))
	require.NoError(t, err)
	transitioned, err := repo.TransitionQuestionGenerationManifest(context.Background(), manifest.Key(),
		types.QuestionGenerationManifestPrepared, types.QuestionGenerationManifestIndexing)
	require.NoError(t, err)
	require.True(t, transitioned)
	transitioned, err = repo.TransitionQuestionGenerationManifest(context.Background(), manifest.Key(),
		types.QuestionGenerationManifestPrepared, types.QuestionGenerationManifestAbortCleanup)
	require.NoError(t, err)
	require.False(t, transitioned, "stale state transition must not overwrite the current state")
	transitioned, err = repo.TransitionQuestionGenerationManifest(context.Background(), manifest.Key(),
		types.QuestionGenerationManifestIndexing, types.QuestionGenerationManifestAbortCleanup)
	require.NoError(t, err)
	require.True(t, transitioned)
	loaded, err := repo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	require.NoError(t, err)
	require.Equal(t, types.QuestionGenerationManifestAbortCleanup, loaded.State)
	require.NoError(t, repo.DeleteQuestionGenerationManifest(context.Background(), manifest.Key()))
	_, err = repo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestQuestionGenerationManifestRepositorySQLiteGuardSerializesWithoutWrappingTransaction(t *testing.T) {
	repo, _ := setupQuestionGenerationManifestRepo(t)
	candidate := manifestCandidate(t, "rollback")
	err := repo.WithQuestionGenerationGuard(context.Background(), candidate.Key(), func(guardedCtx context.Context) error {
		_, _, createErr := repo.GetOrCreateQuestionGenerationManifest(guardedCtx, candidate)
		require.NoError(t, createErr)
		return errors.New("abort guarded publication")
	})
	require.ErrorContains(t, err, "abort guarded publication")
	stored, err := repo.GetQuestionGenerationManifest(context.Background(), candidate.Key())
	require.NoError(t, err)
	require.Equal(t, candidate.ID, stored.ID)
}
