package repository

import (
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDeleteKnowledgeListAndAdjustStorageIsAtomicAndReplaySafe(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}))
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "tenant", StorageUsed: 100}).Error)
	items := []*types.Knowledge{
		{ID: "knowledge-a", TenantID: 7, KnowledgeBaseID: "kb", StorageSize: 20},
		{ID: "knowledge-b", TenantID: 7, KnowledgeBaseID: "kb", StorageSize: 10},
	}
	require.NoError(t, db.Create(items).Error)
	repo := &knowledgeRepository{db: db}

	callback := "test:fail-knowledge-delete"
	require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register(callback, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "knowledges" {
			tx.AddError(errors.New("injected knowledge delete failure"))
		}
	}))
	require.ErrorContains(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), 7,
		[]string{"knowledge-a", "knowledge-b"}), "injected")
	require.NoError(t, db.Callback().Delete().Remove(callback))
	var tenant types.Tenant
	require.NoError(t, db.First(&tenant, 7).Error)
	require.Equal(t, int64(100), tenant.StorageUsed, "delete failure must not decrement quota")

	require.NoError(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), 7,
		[]string{"knowledge-a", "knowledge-b"}))
	require.NoError(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), 7,
		[]string{"knowledge-a", "knowledge-b"}), "replay must be idempotent")
	require.NoError(t, db.First(&tenant, 7).Error)
	require.Equal(t, int64(70), tenant.StorageUsed)

	orphan := &types.Knowledge{ID: "knowledge-orphan", TenantID: 8, KnowledgeBaseID: "kb", StorageSize: 5}
	require.NoError(t, db.Create(orphan).Error)
	require.ErrorContains(t, repo.DeleteKnowledgeListAndAdjustStorage(t.Context(), 8,
		[]string{orphan.ID}), "tenant 8 not found")
	var remaining int64
	require.NoError(t, db.Model(&types.Knowledge{}).Where("tenant_id = ? AND id = ?", 8, orphan.ID).Count(&remaining).Error)
	require.Equal(t, int64(1), remaining, "missing tenant must roll back knowledge deletion")
}
