package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupPostgresKnowledgeBaseDeletionRepo(t *testing.T) (*gorm.DB, uint64) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if dsn == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	baseSQL, err := base.DB()
	require.NoError(t, err)
	schema := fmt.Sprintf("g004_kbdelete_%d", time.Now().UnixNano())
	require.True(t, strings.HasPrefix(schema, "g004_kbdelete_"))
	require.NoError(t, base.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = base.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		_ = baseSQL.Close()
	})
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	db, err := gorm.Open(postgres.Open(dsn+separator+"search_path="+schema), &gorm.Config{})
	require.NoError(t, err)
	testSQL, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = testSQL.Close() })
	require.NoError(t, db.AutoMigrate(
		&types.Tenant{},
		&types.KnowledgeBase{},
		&types.Knowledge{},
		&types.TaskPendingOp{},
		&types.QuestionGenerationManifest{},
	))
	tenantID := uint64(900_000_000 + os.Getpid())
	require.NoError(t, db.Create(&types.Tenant{
		ID: tenantID, Name: "g004-postgres", StorageUsed: 100,
	}).Error)
	return db, tenantID
}

func TestKnowledgeBaseDeletionRepository_PostgresPrepareAuthorizeFinalizeAndAck(t *testing.T) {
	db, tenantID := setupPostgresKnowledgeBaseDeletionRepo(t)
	kbID := uuid.NewString()
	storeID := uuid.NewString()
	require.NoError(t, db.Create(&types.KnowledgeBase{
		ID: kbID, TenantID: tenantID, Name: "delete-me", VectorStoreID: &storeID,
	}).Error)
	payload := types.KBDeletePayload{
		TenantID: tenantID, KnowledgeBaseID: kbID, DataSourceIDs: []string{"ds-1"},
		EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType,
		}},
		VectorStoreID: &storeID,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	dedupKey := "kb-delete:" + strconv.FormatUint(tenantID, 10) + ":" + kbID
	op := &types.TaskPendingOp{
		TenantID: tenantID, TaskType: types.TypeKBDelete,
		Scope: types.TaskScopeKnowledgeBaseDeletion, ScopeID: kbID,
		Op: "delete", DedupKey: dedupKey, Payload: payloadBytes,
	}
	kbRepo := &knowledgeBaseRepository{db: db}
	require.NoError(t, kbRepo.PrepareKnowledgeBaseDeletion(t.Context(), tenantID, kbID, op))
	require.NoError(t, kbRepo.AuthorizeKnowledgeBaseDeletion(t.Context(), tenantID, kbID, dedupKey, &payload))

	altered := payload
	altered.DataSourceIDs = []string{"forged"}
	require.ErrorContains(t,
		kbRepo.AuthorizeKnowledgeBaseDeletion(t.Context(), tenantID, kbID, dedupKey, &altered),
		"does not match durable snapshot",
	)

	manifest := manifestCandidate(t, "pending cleanup")
	manifest.TenantID = tenantID
	manifest.KnowledgeBaseID = kbID
	manifest.KnowledgeID = uuid.NewString()
	manifest.ChunkID = uuid.NewString()
	manifest.GenerationKey = uuid.NewString()
	require.NoError(t, db.Create(manifest).Error)
	require.ErrorContains(t,
		kbRepo.FinalizeKnowledgeBaseDeletion(t.Context(), tenantID, kbID, storeID),
		"manifest",
	)
	require.NoError(t, db.Delete(manifest).Error)
	require.NoError(t, kbRepo.FinalizeKnowledgeBaseDeletion(t.Context(), tenantID, kbID, storeID))

	var stored types.KnowledgeBase
	require.NoError(t, db.Unscoped().Where("tenant_id = ? AND id = ?", tenantID, kbID).First(&stored).Error)
	require.Nil(t, stored.VectorStoreID)

	queueRepo := &taskPendingOpsRepository{db: db}
	require.NoError(t, queueRepo.AckKnowledgeBaseDeletion(t.Context(), tenantID, kbID, dedupKey))
	require.ErrorContains(t,
		queueRepo.AckKnowledgeBaseDeletion(t.Context(), tenantID, kbID, dedupKey),
		"expected 1 outbox row",
	)
}

func TestKnowledgeBaseDeletionRepository_PostgresKnowledgeDeleteAndQuotaAreAtomic(t *testing.T) {
	db, tenantID := setupPostgresKnowledgeBaseDeletionRepo(t)
	kbID := uuid.NewString()
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: kbID, TenantID: tenantID, Name: "quota"}).Error)
	knowledge := []*types.Knowledge{
		{ID: uuid.NewString(), TenantID: tenantID, KnowledgeBaseID: kbID, Type: "file", StorageSize: 30},
		{ID: uuid.NewString(), TenantID: tenantID, KnowledgeBaseID: kbID, Type: "file", StorageSize: 20},
	}
	require.NoError(t, db.Create(&knowledge).Error)
	ids := []string{knowledge[0].ID, knowledge[1].ID}
	repo := &knowledgeRepository{db: db}
	require.NoError(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), tenantID, ids))
	require.NoError(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), tenantID, ids))

	var tenant types.Tenant
	require.NoError(t, db.Where("id = ?", tenantID).First(&tenant).Error)
	require.EqualValues(t, 50, tenant.StorageUsed)
	var active int64
	require.NoError(t, db.Model(&types.Knowledge{}).Where("tenant_id = ? AND id IN ?", tenantID, ids).Count(&active).Error)
	require.Zero(t, active)

	rollbackKnowledge := &types.Knowledge{
		ID: uuid.NewString(), TenantID: tenantID, KnowledgeBaseID: kbID, Type: "file", StorageSize: 10,
	}
	require.NoError(t, db.Create(rollbackKnowledge).Error)
	callback := "test:fail-postgres-quota-update:" + rollbackKnowledge.ID
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "tenants" {
			tx.AddError(errors.New("injected quota update failure"))
		}
	}))
	require.ErrorContains(t,
		repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), tenantID, []string{rollbackKnowledge.ID}),
		"injected quota update failure",
	)
	require.NoError(t, db.Callback().Update().Remove(callback))
	require.NoError(t, db.Model(&types.Knowledge{}).
		Where("tenant_id = ? AND id = ?", tenantID, rollbackKnowledge.ID).Count(&active).Error)
	require.EqualValues(t, 1, active, "quota failure must roll back knowledge deletion")
	require.NoError(t, db.Where("id = ?", tenantID).First(&tenant).Error)
	require.EqualValues(t, 50, tenant.StorageUsed)
}
