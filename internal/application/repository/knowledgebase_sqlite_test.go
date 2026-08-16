package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// knowledgeBasesTestDDL mirrors the `knowledge_bases` section of
// migrations/sqlite/000000_init.up.sql. We inline the DDL here instead of
// using GORM AutoMigrate because KnowledgeBase carries fields tagged with
// `type:jsonb`, which AutoMigrate does not map cleanly onto SQLite.
const knowledgeBasesTestDDL = `
CREATE TABLE IF NOT EXISTS knowledge_bases (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    tenant_id INTEGER NOT NULL,
    type VARCHAR(32) NOT NULL DEFAULT 'document',
    chunking_config TEXT NOT NULL DEFAULT '{}',
    image_processing_config TEXT NOT NULL DEFAULT '{}',
    embedding_model_id VARCHAR(64) NOT NULL,
    summary_model_id VARCHAR(64) NOT NULL,
    cos_config TEXT NOT NULL DEFAULT '{}',
    storage_provider_config TEXT DEFAULT NULL,
    vlm_config TEXT NOT NULL DEFAULT '{}',
    extract_config TEXT NULL DEFAULT NULL,
    faq_config TEXT,
    question_generation_config TEXT NULL,
    auto_tag_config TEXT NULL,
    is_temporary BOOLEAN NOT NULL DEFAULT 0,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    pinned_at DATETIME NULL,
    asr_config TEXT,
    vector_store_id VARCHAR(36),
    storage_backend_id VARCHAR(36),
    wiki_config TEXT,
    indexing_strategy TEXT,
    creator_id VARCHAR(36),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME
);
`

// setupKBTestDB creates an in-memory SQLite database containing the
// knowledge_bases table with the vector_store_id column included, so that
// tests can exercise the GORM behavior of the VectorStoreID field.
func setupKBTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(knowledgeBasesTestDDL).Error)
	return db
}

// makeKB builds a minimal KnowledgeBase suitable for insert. Only the columns
// required by the schema above and any field we explicitly care about are set;
// the rest are left at their Go zero values and serialize to "{}" via their
// custom JSON valuers.
func makeKB(vectorStoreID *string) *types.KnowledgeBase {
	return &types.KnowledgeBase{
		ID:               uuid.New().String(),
		Name:             "test-kb",
		TenantID:         1,
		EmbeddingModelID: "test-embed",
		SummaryModelID:   "test-summary",
		VectorStoreID:    vectorStoreID,
	}
}

func kbStrPtr(s string) *string { return &s }

// reloadKB fetches the KnowledgeBase back from the database using its ID.
func reloadKB(t *testing.T, db *gorm.DB, id string) types.KnowledgeBase {
	t.Helper()
	var got types.KnowledgeBase
	require.NoError(t, db.First(&got, "id = ?", id).Error)
	return got
}

// TestKnowledgeBase_VectorStoreID_Save_Immutable verifies that db.Save(kb)
// — which WeKnora's UpdateKnowledgeBase repository path uses today — does
// NOT overwrite the vector_store_id column, thanks to the GORM
// `<-:create` tag. This is the primary automated defense for the H1 review
// finding (silent overwrite of the binding via full-struct UPDATE).
func TestKnowledgeBase_VectorStoreID_Save_Immutable(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(original).Error)

	// Attempt to overwrite through the canonical Save() path.
	mutated := *original
	mutated.Name = "updated-name"
	mutated.VectorStoreID = kbStrPtr("store-B")
	require.NoError(t, db.Save(&mutated).Error)

	reloaded := reloadKB(t, db, original.ID)

	// Non-immutable fields must update.
	assert.Equal(t, "updated-name", reloaded.Name, "Name must update through Save")

	// Immutable field must NOT update.
	require.NotNil(t, reloaded.VectorStoreID, "VectorStoreID must remain set to the original value")
	assert.Equal(t, "store-A", *reloaded.VectorStoreID,
		"db.Save must not overwrite vector_store_id (enforced by GORM `<-:create` tag)")
}

// TestKnowledgeBase_VectorStoreID_Updates_Immutable verifies that
// db.Updates(struct{...}) also honors `<-:create` and silently ignores the
// vector_store_id column.
func TestKnowledgeBase_VectorStoreID_Updates_Immutable(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(original).Error)

	// Partial struct update that includes vector_store_id must not take effect.
	err := db.Model(&types.KnowledgeBase{}).
		Where("id = ?", original.ID).
		Updates(types.KnowledgeBase{
			Name:          "updated-name",
			VectorStoreID: kbStrPtr("store-B"),
		}).Error
	require.NoError(t, err)

	reloaded := reloadKB(t, db, original.ID)

	assert.Equal(t, "updated-name", reloaded.Name)
	require.NotNil(t, reloaded.VectorStoreID)
	assert.Equal(t, "store-A", *reloaded.VectorStoreID,
		"db.Updates must not overwrite vector_store_id (enforced by GORM `<-:create` tag)")
}

// TestKnowledgeBase_VectorStoreID_Select_StillImmutable verifies that even
// an explicit Select("vector_store_id").Updates(...) does NOT bypass the
// `<-:create` tag. GORM's `<-:create` is strict: it withholds write
// permission from every UPDATE path, including selective updates. This is
// a stronger guarantee than a typical "column is protected unless you
// opt in" scheme.
//
// The implication for Phase 4 (cross-store migration) is that ORM-level
// updates cannot move bindings. Migration code must use raw SQL (db.Exec)
// — see TestKnowledgeBase_VectorStoreID_RawSQL_EscapeHatch below.
func TestKnowledgeBase_VectorStoreID_Select_StillImmutable(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(original).Error)

	err := db.Model(&types.KnowledgeBase{}).
		Where("id = ?", original.ID).
		Select("vector_store_id").
		Updates(map[string]interface{}{"vector_store_id": "store-B"}).Error
	require.NoError(t, err)

	reloaded := reloadKB(t, db, original.ID)
	require.NotNil(t, reloaded.VectorStoreID)
	assert.Equal(t, "store-A", *reloaded.VectorStoreID,
		"Select(\"vector_store_id\").Updates must NOT bypass <-:create; ORM is fully locked")
}

// TestKnowledgeBase_VectorStoreID_RawSQL_EscapeHatch documents that raw
// SQL UPDATE bypasses the GORM tag system entirely, as expected. Phase 4
// cross-store migration is the only sanctioned caller of this path, and
// it must do so through a dedicated, narrowly-scoped repository method
// rather than through general Save/Updates/Select flows.
func TestKnowledgeBase_VectorStoreID_RawSQL_EscapeHatch(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(original).Error)

	require.NoError(t,
		db.Exec("UPDATE knowledge_bases SET vector_store_id = ? WHERE id = ?",
			"store-B", original.ID).Error)

	reloaded := reloadKB(t, db, original.ID)
	require.NotNil(t, reloaded.VectorStoreID)
	assert.Equal(t, "store-B", *reloaded.VectorStoreID,
		"raw SQL UPDATE bypasses GORM tags; Phase 4 migration must use this path through a dedicated method")
}

// TestKnowledgeBase_VectorStoreID_Roundtrip_Nil verifies that a nil
// VectorStoreID is persisted as SQL NULL and loaded back as nil.
func TestKnowledgeBase_VectorStoreID_Roundtrip_Nil(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(nil)
	require.NoError(t, db.Create(original).Error)

	reloaded := reloadKB(t, db, original.ID)
	assert.Nil(t, reloaded.VectorStoreID,
		"nil VectorStoreID must round-trip through the database as nil")

	// Extra assurance: column is actually NULL, not an empty string.
	var nullCount int64
	require.NoError(t,
		db.Table("knowledge_bases").
			Where("id = ? AND vector_store_id IS NULL", original.ID).
			Count(&nullCount).Error)
	assert.Equal(t, int64(1), nullCount, "nil must be stored as SQL NULL")
}

// TestKnowledgeBase_VectorStoreID_Roundtrip_Value verifies that a non-nil
// VectorStoreID is persisted and loaded back with the same value.
func TestKnowledgeBase_VectorStoreID_Roundtrip_Value(t *testing.T) {
	db := setupKBTestDB(t)

	original := makeKB(kbStrPtr("store-uuid-42"))
	require.NoError(t, db.Create(original).Error)

	reloaded := reloadKB(t, db, original.ID)
	require.NotNil(t, reloaded.VectorStoreID)
	assert.Equal(t, "store-uuid-42", *reloaded.VectorStoreID)
}

// TestCountByVectorStoreID covers the binding-count helper used by the
// VectorStore delete guard. Verifies tenant scoping, the GORM auto soft-delete
// filter (no explicit `deleted_at IS NULL` literal in the query), and the
// edge case where a row stores an empty-string vector_store_id (SQLite
// treats "" and NULL differently — we want both excluded from a non-empty
// storeID lookup).
func TestCountByVectorStoreID(t *testing.T) {
	db := setupKBTestDB(t)
	repo := &knowledgeBaseRepository{db: db}
	ctx := t.Context()

	// Tenant 1: two active rows bound to store-A, one row bound to store-B.
	kbA1 := makeKB(kbStrPtr("store-A"))
	kbA2 := makeKB(kbStrPtr("store-A"))
	kbB := makeKB(kbStrPtr("store-B"))
	require.NoError(t, db.Create(kbA1).Error)
	require.NoError(t, db.Create(kbA2).Error)
	require.NoError(t, db.Create(kbB).Error)

	// Tenant 2: one row bound to store-A (must not count for tenant 1).
	kbCross := makeKB(kbStrPtr("store-A"))
	kbCross.TenantID = 2
	require.NoError(t, db.Create(kbCross).Error)

	// Soft-deleted rows retain their binding until async deletion finalizes.
	kbDeleted := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(kbDeleted).Error)
	require.NoError(t, db.Delete(kbDeleted).Error)

	// Row with empty-string vector_store_id (regression — sqlite quirk).
	kbEmpty := makeKB(kbStrPtr(""))
	require.NoError(t, db.Create(kbEmpty).Error)

	t.Run("nil db handle uses default", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, nil, 1, "store-A")
		require.NoError(t, err)
		assert.Equal(t, int64(3), count, "soft-deleted KB bindings must block store deletion")
	})

	t.Run("explicit db handle works the same", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, db, 1, "store-A")
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("different store id", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, nil, 1, "store-B")
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("unknown store id returns zero", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, nil, 1, "store-XYZ")
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("different tenant", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, nil, 2, "store-A")
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("non-empty lookup does not match empty-string rows", func(t *testing.T) {
		count, err := repo.CountByVectorStoreID(ctx, nil, 1, "")
		require.NoError(t, err)
		// Only the empty-string-vsid row matches "" exactly (SQLite quirk —
		// "" is not NULL). The non-empty rows do not.
		assert.Equal(t, int64(1), count, "empty-string lookup matches only the empty row")
	})

	t.Run("shared tx handle returns same result twice", func(t *testing.T) {
		err := db.Transaction(func(tx *gorm.DB) error {
			c1, err := repo.CountByVectorStoreID(ctx, tx, 1, "store-A")
			if err != nil {
				return err
			}
			c2, err := repo.CountByVectorStoreID(ctx, tx, 1, "store-A")
			if err != nil {
				return err
			}
			assert.Equal(t, c1, c2, "two reads within the same tx must agree")
			assert.Equal(t, int64(3), c1)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestFinalizeKnowledgeBaseDeletionClearsOnlyMatchingSoftDeletedBindingAndManifests(t *testing.T) {
	db := setupKBTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.QuestionGenerationManifest{}))
	repo := &knowledgeBaseRepository{db: db}
	kb := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(kb).Error)
	require.NoError(t, db.Delete(kb).Error)
	other := makeKB(kbStrPtr("store-A"))
	other.TenantID = 2
	require.NoError(t, db.Create(other).Error)
	require.NoError(t, db.Delete(other).Error)
	manifest := &types.QuestionGenerationManifest{
		ID: "manifest-1", TenantID: kb.TenantID, KnowledgeBaseID: kb.ID,
		KnowledgeID: "knowledge-1", ChunkID: "chunk-1", ContentRevision: 1,
		BatchIndex: 0, IdentityVersion: 1, GenerationKey: "generation-1",
		EmbeddingDimension: 1, State: types.QuestionGenerationManifestIndexing,
		EffectiveEngines: types.JSON("[]"), Questions: types.JSON("[]"),
		IndexEntries: types.JSON("[]"), DesiredSourceIDs: types.JSON("[]"),
		AbandonedSourceIDs: types.JSON("[]"),
	}
	require.NoError(t, db.Create(manifest).Error)

	require.Error(t, repo.FinalizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, "store-forged"))
	require.Error(t, repo.FinalizeKnowledgeBaseDeletion(t.Context(), 2, kb.ID, "store-A"))
	require.ErrorContains(t, repo.FinalizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, "store-A"), "manifest")
	require.NoError(t, db.Delete(manifest).Error)
	require.NoError(t, repo.FinalizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, "store-A"))
	require.NoError(t, repo.FinalizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, "store-A"), "retry must be idempotent")

	var stored types.KnowledgeBase
	require.NoError(t, db.Unscoped().Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID).First(&stored).Error)
	require.Nil(t, stored.VectorStoreID)
	var otherStored types.KnowledgeBase
	require.NoError(t, db.Unscoped().Where("tenant_id = ? AND id = ?", other.TenantID, other.ID).First(&otherStored).Error)
	require.Equal(t, "store-A", *otherStored.VectorStoreID)
}

func TestPrepareKnowledgeBaseDeletionIsAtomicAndIdentityScoped(t *testing.T) {
	db := setupKBTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}))
	repo := &knowledgeBaseRepository{db: db}
	kb := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(kb).Error)
	payload := json.RawMessage(`{"tenant_id":1,"knowledge_base_id":"` + kb.ID + `"}`)
	valid := func() *types.TaskPendingOp {
		return &types.TaskPendingOp{
			TenantID: kb.TenantID, TaskType: types.TypeKBDelete,
			Scope: types.TaskScopeKnowledgeBaseDeletion, ScopeID: kb.ID,
			Op: "delete", DedupKey: "kb-delete:1:" + kb.ID, Payload: payload,
		}
	}
	for _, forged := range []*types.TaskPendingOp{
		func() *types.TaskPendingOp { op := valid(); op.TenantID = 2; return op }(),
		func() *types.TaskPendingOp { op := valid(); op.ScopeID = "other-kb"; return op }(),
		func() *types.TaskPendingOp { op := valid(); op.DedupKey = ""; return op }(),
		func() *types.TaskPendingOp { op := valid(); op.DedupKey = "kb-delete:1:forged"; return op }(),
		func() *types.TaskPendingOp { op := valid(); op.Payload = nil; return op }(),
		func() *types.TaskPendingOp {
			op := valid()
			op.Payload = json.RawMessage(`{"tenant_id":1,"knowledge_base_id":"forged"}`)
			return op
		}(),
	} {
		require.Error(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, forged))
	}
	var outboxRows int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&outboxRows).Error)
	require.Zero(t, outboxRows)

	forgedExisting := valid()
	forgedExisting.Payload = json.RawMessage(`{"tenant_id":1,"knowledge_base_id":"forged"}`)
	require.NoError(t, db.Create(forgedExisting).Error)
	require.ErrorContains(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, valid()), "existing outbox payload mismatch")
	require.NoError(t, db.Where("scope = ? AND scope_id = ?", types.TaskScopeKnowledgeBaseDeletion, kb.ID).Delete(&types.TaskPendingOp{}).Error)
	var active int64
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID).Count(&active).Error)
	require.Equal(t, int64(1), active, "mismatched residue must not soft-delete KB")

	require.NoError(t, db.Create(valid()).Error)
	require.NoError(t, db.Create(valid()).Error)
	require.ErrorContains(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, valid()), "duplicate outbox rows")
	require.NoError(t, db.Where("scope = ? AND scope_id = ?", types.TaskScopeKnowledgeBaseDeletion, kb.ID).Delete(&types.TaskPendingOp{}).Error)
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID).Count(&active).Error)
	require.Equal(t, int64(1), active, "duplicate residue must not soft-delete KB")

	callback := "test:fail-kb-delete-outbox"
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "task_pending_ops" {
			tx.AddError(errors.New("injected outbox insert failure"))
		}
	}))
	require.ErrorContains(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, valid()), "injected")
	require.NoError(t, db.Callback().Create().Remove(callback))
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID).Count(&active).Error)
	require.Equal(t, int64(1), active, "outbox failure must roll back soft delete")

	require.NoError(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, valid()))
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("scope = ? AND scope_id = ?",
		types.TaskScopeKnowledgeBaseDeletion, kb.ID).Count(&active).Error)
	require.Equal(t, int64(1), active)
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID).Count(&active).Error)
	require.Zero(t, active)
}

func TestAuthorizeKnowledgeBaseDeletionRequiresSoftDeleteAndExactOutbox(t *testing.T) {
	db := setupKBTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}))
	repo := &knowledgeBaseRepository{db: db}
	kb := makeKB(kbStrPtr("store-A"))
	require.NoError(t, db.Create(kb).Error)
	dedup := fmt.Sprintf("kb-delete:%d:%s", kb.TenantID, kb.ID)
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID})
	require.NoError(t, err)
	op := &types.TaskPendingOp{
		TenantID: kb.TenantID, TaskType: types.TypeKBDelete,
		Scope: types.TaskScopeKnowledgeBaseDeletion, ScopeID: kb.ID,
		Op: "delete", DedupKey: dedup, Payload: payload,
	}

	executing := &types.KBDeletePayload{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID}
	require.ErrorContains(t, repo.AuthorizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, dedup, executing), "not soft-deleted")
	require.NoError(t, repo.PrepareKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, op))
	require.NoError(t, repo.AuthorizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, dedup, executing))
	storeID := "forged-store"
	for _, altered := range []*types.KBDeletePayload{
		{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, VectorStoreID: &storeID},
		{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, DataSourceIDs: []string{"forged-ds"}},
		{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, EffectiveEngines: []types.RetrieverEngineParams{{
			RetrieverEngineType: "forged", RetrieverType: types.VectorRetrieverType,
		}}},
	} {
		require.ErrorContains(t,
			repo.AuthorizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, dedup, altered),
			"does not match durable snapshot")
	}

	forged, err := json.Marshal(types.KBDeletePayload{TenantID: kb.TenantID + 1, KnowledgeBaseID: kb.ID})
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("dedup_key = ?", dedup).Update("payload", forged).Error)
	require.ErrorContains(t, repo.AuthorizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, dedup, executing), "payload identity")
	require.NoError(t, db.Where("dedup_key = ?", dedup).Delete(&types.TaskPendingOp{}).Error)
	require.ErrorContains(t, repo.AuthorizeKnowledgeBaseDeletion(t.Context(), kb.TenantID, kb.ID, dedup, executing), "expected one outbox row")
}
