package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestQuestionGenerationManifestRepository_PostgresKnowledgeBaseShareLockFencesSoftDelete(t *testing.T) {
	dsn := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if dsn == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	otherDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.QuestionGenerationManifest{}))
	kbID := uuid.NewString()
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: kbID, TenantID: 7, Name: "guard"}).Error)
	t.Cleanup(func() {
		_ = db.Unscoped().Where("tenant_id = ? AND id = ?", 7, kbID).Delete(&types.KnowledgeBase{}).Error
	})
	repo := &questionGenerationManifestRepository{db: db}
	candidate := manifestCandidate(t, "guarded")
	candidate.KnowledgeBaseID = kbID
	candidate.ChunkID = uuid.NewString()
	candidate.GenerationKey = uuid.NewString()
	entered, release := make(chan struct{}), make(chan struct{})
	callbackName := "test:block_after_kb_share_lock:" + kbID
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "knowledge_bases" {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
		}
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(callbackName) })
	createDone := make(chan error, 1)
	go func() {
		_, _, createErr := repo.GetOrCreateQuestionGenerationManifest(context.Background(), candidate)
		createDone <- createErr
	}()
	<-entered
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- otherDB.Where("tenant_id = ? AND id = ?", 7, kbID).Delete(&types.KnowledgeBase{}).Error
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("soft delete escaped manifest KB share lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-createDone)
	require.NoError(t, <-deleteDone)
	manifests, err := repo.ListQuestionGenerationManifestsByKnowledgeBase(context.Background(), 7, kbID)
	require.NoError(t, err)
	require.Len(t, manifests, 1)

	newCandidate := manifestCandidate(t, "rejected")
	newCandidate.KnowledgeBaseID = kbID
	newCandidate.ChunkID = uuid.NewString()
	newCandidate.GenerationKey = uuid.NewString()
	_, _, err = repo.GetOrCreateQuestionGenerationManifest(context.Background(), newCandidate)
	require.ErrorContains(t, err, "deleted or unavailable")
}
