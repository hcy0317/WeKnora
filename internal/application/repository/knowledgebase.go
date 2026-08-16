package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrKnowledgeBaseNotFound = errors.New("knowledge base not found")

// knowledgeBaseRepository implements the KnowledgeBaseRepository interface
type knowledgeBaseRepository struct {
	db *gorm.DB
}

// NewKnowledgeBaseRepository creates a new knowledge base repository
func NewKnowledgeBaseRepository(db *gorm.DB) interfaces.KnowledgeBaseRepository {
	return &knowledgeBaseRepository{db: db}
}

// CreateKnowledgeBase creates a new knowledge base
func (r *knowledgeBaseRepository) CreateKnowledgeBase(ctx context.Context, kb *types.KnowledgeBase) error {
	return r.db.WithContext(ctx).Create(kb).Error
}

// GetKnowledgeBaseByID gets a knowledge base by id (no tenant scope; caller must enforce isolation where needed)
func (r *knowledgeBaseRepository) GetKnowledgeBaseByID(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	var kb types.KnowledgeBase
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&kb).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrKnowledgeBaseNotFound
		}
		return nil, err
	}
	return &kb, nil
}

// GetKnowledgeBaseByIDAndTenant gets a knowledge base by id only if it belongs to the given tenant (enforces tenant isolation)
func (r *knowledgeBaseRepository) GetKnowledgeBaseByIDAndTenant(ctx context.Context, id string, tenantID uint64) (*types.KnowledgeBase, error) {
	var kb types.KnowledgeBase
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", id, tenantID).First(&kb).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrKnowledgeBaseNotFound
		}
		return nil, err
	}
	return &kb, nil
}

// GetKnowledgeBaseByIDs gets knowledge bases by multiple ids
func (r *knowledgeBaseRepository) GetKnowledgeBaseByIDs(ctx context.Context, ids []string) ([]*types.KnowledgeBase, error) {
	if len(ids) == 0 {
		return []*types.KnowledgeBase{}, nil
	}
	var kbs []*types.KnowledgeBase
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&kbs).Error; err != nil {
		return nil, err
	}
	return kbs, nil
}

// ListKnowledgeBases lists all knowledge bases
func (r *knowledgeBaseRepository) ListKnowledgeBases(ctx context.Context) ([]*types.KnowledgeBase, error) {
	var kbs []*types.KnowledgeBase
	if err := r.db.WithContext(ctx).Find(&kbs).Error; err != nil {
		return nil, err
	}
	return kbs, nil
}

// ListKnowledgeBasesByTenantID lists all knowledge bases by tenant id.
//
// Ordering used to also include `is_pinned DESC, pinned_at DESC` so the
// repository would return tenant-wide pinned rows first. That column is
// no longer the source of truth (see migration 000050) — pin state is
// now per (user, kb) and applied by the service layer after enrichment.
// We keep `created_at DESC` here so callers that don't enrich (chat
// pipeline, agent editor, IM commands) still get a stable ordering.
func (r *knowledgeBaseRepository) ListKnowledgeBasesByTenantID(
	ctx context.Context, tenantID uint64,
) ([]*types.KnowledgeBase, error) {
	var kbs []*types.KnowledgeBase
	if err := r.db.WithContext(ctx).Where("tenant_id = ? AND is_temporary = ?", tenantID, false).
		Order("created_at DESC").Find(&kbs).Error; err != nil {
		return nil, err
	}
	return kbs, nil
}

// userKBPinRow mirrors the user_kb_pins table. Kept local to the
// repository because it never escapes the package; callers see the
// higher-level map[kb_id]pinned_at returned by ListUserKBPinIDs.
type userKBPinRow struct {
	TenantID uint64    `gorm:"column:tenant_id"`
	UserID   string    `gorm:"column:user_id"`
	KBID     string    `gorm:"column:kb_id"`
	PinnedAt time.Time `gorm:"column:pinned_at"`
}

func (userKBPinRow) TableName() string { return "user_kb_pins" }

// SetUserKBPin upserts (pinned=true) or deletes (pinned=false) the row
// for the given (tenant, user, kb) triple. The returned pinned_at is
// nil when pinned=false; otherwise it carries the timestamp written
// to the row (either the existing one if the row already existed, or
// the current time on insert) so the caller can stamp the response
// without a follow-up SELECT.
func (r *knowledgeBaseRepository) SetUserKBPin(
	ctx context.Context, tenantID uint64, userID string, kbID string, pinned bool,
) (*time.Time, error) {
	if userID == "" {
		return nil, errors.New("user_kb_pins: empty user_id")
	}
	if !pinned {
		err := r.db.WithContext(ctx).
			Where("tenant_id = ? AND user_id = ? AND kb_id = ?", tenantID, userID, kbID).
			Delete(&userKBPinRow{}).Error
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Upsert with idempotent INSERT … ON CONFLICT DO NOTHING. We then
	// SELECT to learn whether an existing row's pinned_at survived (so
	// repeated calls return a stable timestamp instead of bumping it).
	row := userKBPinRow{
		TenantID: tenantID,
		UserID:   userID,
		KBID:     kbID,
		PinnedAt: time.Now(),
	}
	if err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND user_id = ? AND kb_id = ?", tenantID, userID, kbID).
		Attrs(userKBPinRow{PinnedAt: row.PinnedAt}).
		FirstOrCreate(&row).Error; err != nil {
		return nil, err
	}
	pa := row.PinnedAt
	return &pa, nil
}

// ListUserKBPinIDs returns every KB id this user has personally pinned
// in this tenant, mapped to its pinned_at. Returns an empty map (not
// nil) when there are no pins, so callers can do `len(m) == 0` checks
// without a nil guard.
func (r *knowledgeBaseRepository) ListUserKBPinIDs(
	ctx context.Context, tenantID uint64, userID string,
) (map[string]time.Time, error) {
	out := make(map[string]time.Time)
	if userID == "" {
		return out, nil
	}
	var rows []userKBPinRow
	if err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND user_id = ?", tenantID, userID).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		out[row.KBID] = row.PinnedAt
	}
	return out, nil
}

// UpdateKnowledgeBase updates a knowledge base
func (r *knowledgeBaseRepository) UpdateKnowledgeBase(ctx context.Context, kb *types.KnowledgeBase) error {
	return r.db.WithContext(ctx).Save(kb).Error
}

// DeleteKnowledgeBase deletes a knowledge base
func (r *knowledgeBaseRepository) DeleteKnowledgeBase(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&types.KnowledgeBase{}).Error
}

// CountByVectorStoreID counts all knowledge bases that retain a binding to the
// given vector store within a tenant scope, including soft-deleted rows whose
// asynchronous cleanup has not finalized yet.
//
// Pass db == nil to use the repository's default db handle; pass a *gorm.DB
// bound to a transaction (e.g., from db.Transaction) to share the same
// write-lock context as the caller. Query column order matches the
// composite index idx_knowledge_bases_tenant_vector_store(tenant_id,
// vector_store_id).
func (r *knowledgeBaseRepository) CountByVectorStoreID(
	ctx context.Context, db *gorm.DB, tenantID uint64, storeID string,
) (int64, error) {
	if db == nil {
		db = r.db
	}
	var count int64
	err := db.WithContext(ctx).
		Unscoped().
		Model(&types.KnowledgeBase{}).
		Where("tenant_id = ? AND vector_store_id = ?", tenantID, storeID).
		Count(&count).Error
	return count, err
}

func (r *knowledgeBaseRepository) FinalizeKnowledgeBaseDeletion(
	ctx context.Context, tenantID uint64, knowledgeBaseID, expectedVectorStoreID string,
) error {
	if tenantID == 0 || knowledgeBaseID == "" {
		return errors.New("knowledge base deletion finalizer: tenant and knowledge base are required")
	}
	return dbWithContext(ctx, r.db).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		query := tx.Unscoped().Where("tenant_id = ? AND id = ?", tenantID, knowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.First(&kb).Error; err != nil {
			return fmt.Errorf("load knowledge base deletion target: %w", err)
		}
		if !kb.DeletedAt.Valid {
			return errors.New("knowledge base deletion finalizer: target is not soft-deleted")
		}
		if kb.VectorStoreID != nil && *kb.VectorStoreID != expectedVectorStoreID {
			return fmt.Errorf(
				"knowledge base deletion finalizer: vector store identity mismatch: expected %q, found %q",
				expectedVectorStoreID, *kb.VectorStoreID,
			)
		}
		var manifests int64
		if err := tx.Model(&types.QuestionGenerationManifest{}).
			Where("tenant_id = ? AND knowledge_base_id = ?", tenantID, knowledgeBaseID).
			Count(&manifests).Error; err != nil {
			return fmt.Errorf("count knowledge base question manifests: %w", err)
		}
		if manifests != 0 {
			return fmt.Errorf("knowledge base deletion finalizer: %d question manifest(s) remain", manifests)
		}
		if kb.VectorStoreID == nil {
			return nil
		}
		result := tx.Exec(
			"UPDATE knowledge_bases SET vector_store_id = NULL WHERE tenant_id = ? AND id = ?",
			tenantID, knowledgeBaseID,
		)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("knowledge base deletion finalizer: binding changed concurrently")
		}
		return nil
	})
}

func (r *knowledgeBaseRepository) PrepareKnowledgeBaseDeletion(
	ctx context.Context, tenantID uint64, knowledgeBaseID string, op *types.TaskPendingOp,
) error {
	if tenantID == 0 || knowledgeBaseID == "" || op == nil {
		return errors.New("knowledge base deletion preparer: complete identity and outbox are required")
	}
	if op.TenantID != tenantID || op.TaskType != types.TypeKBDelete ||
		op.Scope != types.TaskScopeKnowledgeBaseDeletion || op.ScopeID != knowledgeBaseID ||
		op.DedupKey != fmt.Sprintf("kb-delete:%d:%s", tenantID, knowledgeBaseID) || op.Op != "delete" {
		return errors.New("knowledge base deletion preparer: outbox identity mismatch")
	}
	var payload types.KBDeletePayload
	if err := json.Unmarshal(op.Payload, &payload); err != nil ||
		payload.TenantID != tenantID || payload.KnowledgeBaseID != knowledgeBaseID {
		return errors.New("knowledge base deletion preparer: payload identity mismatch")
	}
	return dbWithContext(ctx, r.db).Transaction(func(tx *gorm.DB) error {
		query := tx.Where("tenant_id = ? AND id = ?", tenantID, knowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var kb types.KnowledgeBase
		if err := query.First(&kb).Error; err != nil {
			return fmt.Errorf("load active knowledge base for deletion: %w", err)
		}
		var existing []types.TaskPendingOp
		if err := tx.Model(&types.TaskPendingOp{}).Where(
			"tenant_id = ? AND task_type = ? AND scope = ? AND scope_id = ? AND op = ? AND dedup_key = ?",
			op.TenantID, op.TaskType, op.Scope, op.ScopeID, op.Op, op.DedupKey,
		).Find(&existing).Error; err != nil {
			return err
		}
		switch len(existing) {
		case 0:
			if len(op.Payload) == 0 {
				return errors.New("knowledge base deletion preparer: payload is required")
			}
			if op.EnqueuedAt.IsZero() {
				op.EnqueuedAt = time.Now()
			}
			if err := tx.Omit("ClaimedAt", "ClaimToken", "ClaimedByTaskID", "ClaimHeartbeatAt").Create(op).Error; err != nil {
				return err
			}
		case 1:
			var stored types.KBDeletePayload
			if err := json.Unmarshal(existing[0].Payload, &stored); err != nil || !reflect.DeepEqual(stored, payload) {
				return errors.New("knowledge base deletion preparer: existing outbox payload mismatch")
			}
		default:
			return fmt.Errorf("knowledge base deletion preparer: duplicate outbox rows: %d", len(existing))
		}
		return tx.Where("tenant_id = ? AND id = ?", tenantID, knowledgeBaseID).Delete(&types.KnowledgeBase{}).Error
	})
}

func (r *knowledgeBaseRepository) AuthorizeKnowledgeBaseDeletion(
	ctx context.Context, tenantID uint64, knowledgeBaseID, dedupKey string,
	executing *types.KBDeletePayload,
) error {
	if executing == nil || tenantID == 0 || knowledgeBaseID == "" ||
		dedupKey != fmt.Sprintf("kb-delete:%d:%s", tenantID, knowledgeBaseID) {
		return errors.New("knowledge base deletion authorizer: identity mismatch")
	}
	return dbWithContext(ctx, r.db).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		query := tx.Unscoped().Where("tenant_id = ? AND id = ?", tenantID, knowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if err := query.First(&kb).Error; err != nil {
			return fmt.Errorf("load knowledge base deletion authorization target: %w", err)
		}
		if !kb.DeletedAt.Valid {
			return errors.New("knowledge base deletion authorizer: target is not soft-deleted")
		}
		var rows []types.TaskPendingOp
		if err := tx.Where(
			"tenant_id = ? AND task_type = ? AND scope = ? AND scope_id = ? AND op = ? AND dedup_key = ?",
			tenantID, types.TypeKBDelete, types.TaskScopeKnowledgeBaseDeletion,
			knowledgeBaseID, "delete", dedupKey,
		).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("knowledge base deletion authorizer: expected one outbox row, found %d", len(rows))
		}
		var payload types.KBDeletePayload
		if err := json.Unmarshal(rows[0].Payload, &payload); err != nil ||
			payload.TenantID != tenantID || payload.KnowledgeBaseID != knowledgeBaseID {
			return errors.New("knowledge base deletion authorizer: payload identity mismatch")
		}
		if !reflect.DeepEqual(payload, *executing) {
			return errors.New("knowledge base deletion authorizer: executing payload does not match durable snapshot")
		}
		return nil
	})
}

// CountByModelID counts active knowledge bases that reference modelID in any
// model-binding column (scalar fields or JSON config blobs).
func (r *knowledgeBaseRepository) CountByModelID(
	ctx context.Context, tenantID uint64, modelID string,
) (int64, error) {
	var count int64
	query := r.db.WithContext(ctx).
		Model(&types.KnowledgeBase{}).
		Where("tenant_id = ?", tenantID)
	query = scopeKnowledgeBasesByModelID(query, modelID)
	err := query.Count(&count).Error
	return count, err
}
