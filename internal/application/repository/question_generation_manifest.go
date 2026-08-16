package repository

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type questionGenerationManifestRepository struct {
	db       *gorm.DB
	createMu sync.Mutex
	guardMu  sync.Mutex
}

func NewQuestionGenerationManifestRepository(db *gorm.DB) interfaces.QuestionGenerationManifestRepository {
	return &questionGenerationManifestRepository{db: db}
}

func (r *questionGenerationManifestRepository) WithQuestionGenerationGuard(
	ctx context.Context, key types.QuestionGenerationManifestKey, fn func(context.Context) error,
) error {
	if fn == nil {
		return errors.New("question generation guard callback is required")
	}
	if r.db.Dialector.Name() != "postgres" {
		r.guardMu.Lock()
		defer r.guardMu.Unlock()
		return fn(ctx)
	}
	guard := func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", questionGenerationGuardID(key)).Error; err != nil {
				return fmt.Errorf("acquire question generation manifest guard: %w", err)
			}
		}
		return fn(withGuardedDB(ctx, tx))
	}
	if tx, ok := guardedDBFromContext(ctx); ok {
		return guard(tx.WithContext(ctx))
	}
	return dbWithContext(ctx, r.db).Transaction(guard)
}

func questionGenerationGuardID(key types.QuestionGenerationManifestKey) int64 {
	hash := fnv.New64a()
	_, _ = fmt.Fprintf(
		hash, "%d\x00%s\x00%s\x00%d\x00%d",
		key.TenantID, key.KnowledgeID, key.ChunkID, key.ContentRevision, key.BatchIndex,
	)
	return int64(hash.Sum64())
}

func questionGenerationManifestScope(
	db *gorm.DB, key types.QuestionGenerationManifestKey,
) *gorm.DB {
	return db.Where(
		"tenant_id = ? AND knowledge_id = ? AND chunk_id = ? AND content_revision = ? AND batch_index = ?",
		key.TenantID, key.KnowledgeID, key.ChunkID, key.ContentRevision, key.BatchIndex,
	)
}

func (r *questionGenerationManifestRepository) GetOrCreateQuestionGenerationManifest(
	ctx context.Context, candidate *types.QuestionGenerationManifest,
) (*types.QuestionGenerationManifest, bool, error) {
	if r.db.Dialector.Name() == "sqlite" {
		r.createMu.Lock()
		defer r.createMu.Unlock()
	}
	var manifest *types.QuestionGenerationManifest
	var created bool
	err := dbWithContext(ctx, r.db).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		query := tx.Where("tenant_id = ? AND id = ?", candidate.TenantID, candidate.KnowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if err := query.First(&kb).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("question generation manifest: knowledge base is deleted or unavailable")
			}
			return err
		}
		result := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"}, {Name: "knowledge_id"}, {Name: "chunk_id"},
				{Name: "content_revision"}, {Name: "batch_index"},
			},
			DoNothing: true,
		}).Create(candidate)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected == 1
		var canonical types.QuestionGenerationManifest
		if err := questionGenerationManifestScope(tx, candidate.Key()).Take(&canonical).Error; err != nil {
			return err
		}
		manifest = &canonical
		return nil
	})
	return manifest, created, err
}

func (r *questionGenerationManifestRepository) ListQuestionGenerationManifestsByKnowledgeBase(
	ctx context.Context, tenantID uint64, knowledgeBaseID string,
) ([]*types.QuestionGenerationManifest, error) {
	var manifests []*types.QuestionGenerationManifest
	err := dbWithContext(ctx, r.db).
		Where("tenant_id = ? AND knowledge_base_id = ?", tenantID, knowledgeBaseID).
		Order("knowledge_id, chunk_id, content_revision, batch_index").
		Find(&manifests).Error
	return manifests, err
}

func (r *questionGenerationManifestRepository) GetQuestionGenerationManifest(
	ctx context.Context, key types.QuestionGenerationManifestKey,
) (*types.QuestionGenerationManifest, error) {
	var manifest types.QuestionGenerationManifest
	err := questionGenerationManifestScope(dbWithContext(ctx, r.db), key).Take(&manifest).Error
	return &manifest, err
}

func (r *questionGenerationManifestRepository) TransitionQuestionGenerationManifest(
	ctx context.Context, key types.QuestionGenerationManifestKey,
	from, to types.QuestionGenerationManifestState,
) (bool, error) {
	if !validQuestionGenerationManifestTransition(from, to) {
		return false, fmt.Errorf("invalid question generation manifest transition %s -> %s", from, to)
	}
	result := questionGenerationManifestScope(dbWithContext(ctx, r.db).Model(&types.QuestionGenerationManifest{}), key).
		Where("state = ?", from).Update("state", to)
	return result.RowsAffected == 1, result.Error
}

func validQuestionGenerationManifestTransition(from, to types.QuestionGenerationManifestState) bool {
	switch from {
	case types.QuestionGenerationManifestPrepared:
		return to == types.QuestionGenerationManifestIndexing || to == types.QuestionGenerationManifestAbortCleanup
	case types.QuestionGenerationManifestIndexing:
		return to == types.QuestionGenerationManifestIndexed || to == types.QuestionGenerationManifestAbortCleanup
	case types.QuestionGenerationManifestIndexed:
		return to == types.QuestionGenerationManifestPublished || to == types.QuestionGenerationManifestAbortCleanup
	default:
		return false
	}
}

func (r *questionGenerationManifestRepository) DeleteQuestionGenerationManifest(
	ctx context.Context, key types.QuestionGenerationManifestKey,
) error {
	return questionGenerationManifestScope(dbWithContext(ctx, r.db), key).
		Delete(&types.QuestionGenerationManifest{}).Error
}
