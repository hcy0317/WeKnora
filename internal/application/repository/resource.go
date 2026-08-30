package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type resourceRepository struct{ db *gorm.DB }

// NewResourceRepository creates the persistence adapter for resource metadata.
func NewResourceRepository(db *gorm.DB) interfaces.ResourceRepository {
	return &resourceRepository{db: db}
}

func (r *resourceRepository) Create(ctx context.Context, resource *types.StoredResource) error {
	return r.db.WithContext(ctx).Create(resource).Error
}

func (r *resourceRepository) GetByID(ctx context.Context, id string) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).Where("id = ? AND state = ?", id, types.ResourceStateActive).First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) GetByHandle(ctx context.Context, handle string) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).
		Where("handle = ? AND state = ?", handle, types.ResourceStateActive).
		First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) GetByHandleIncludingDeleted(
	ctx context.Context, handle string,
) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).Unscoped().Where("handle = ?", handle).First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) GetByTenantLocation(
	ctx context.Context,
	tenantID uint64,
	locationHash string,
) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).
		Where(
			"tenant_id = ? AND location_hash = ? AND state = ?",
			tenantID,
			locationHash,
			types.ResourceStateActive,
		).
		First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func nextResourceDeletionClaim(resource *types.StoredResource) time.Time {
	claim := time.Now().UTC().Truncate(time.Microsecond)
	if resource != nil && !claim.After(resource.UpdatedAt) {
		claim = resource.UpdatedAt.UTC().Truncate(time.Microsecond).Add(time.Microsecond)
	}
	return claim
}

func (r *resourceRepository) MarkDeletedIfUnbound(ctx context.Context, id string) (time.Time, error) {
	var claim time.Time
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var resource types.StoredResource
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", id).First(&resource).Error; err != nil {
			return err
		}
		if resource.State == types.ResourceStateDeleted {
			return nil
		}
		var count int64
		if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", id).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return nil
		}
		claim = nextResourceDeletionClaim(&resource)
		updates := map[string]any{
			"state": types.ResourceStateDeleted, "deleted_at": claim, "updated_at": claim,
		}
		if err := tx.Model(&types.StoredResource{}).Where("id = ? AND state = ?", id, types.ResourceStateActive).
			Updates(updates).Error; err != nil {
			return err
		}
		return nil
	})
	return claim, err
}

func (r *resourceRepository) ValidateDeletionClaim(
	ctx context.Context, id string, claim time.Time,
) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Unscoped().Model(&types.StoredResource{}).
		Where("id = ? AND state = ? AND deleted_at = ? AND updated_at = ?",
			id, types.ResourceStateDeleted, claim, claim).
		Count(&count).Error
	return count == 1, err
}

func (r *resourceRepository) RestoreActiveIfClaim(
	ctx context.Context, id string, claim time.Time,
) (bool, error) {
	result := r.db.WithContext(ctx).Unscoped().Model(&types.StoredResource{}).
		Where(`id = ? AND state = ? AND deleted_at = ? AND updated_at = ?
			AND NOT EXISTS (SELECT 1 FROM resource_bindings WHERE resource_id = resources.id)`,
			id, types.ResourceStateDeleted, claim, claim).
		Updates(map[string]any{"state": types.ResourceStateActive, "deleted_at": nil, "updated_at": time.Now()})
	return result.RowsAffected == 1, result.Error
}

func (r *resourceRepository) CreateBindingIfActive(
	ctx context.Context, binding *types.ResourceBinding,
) (bool, error) {
	if binding.ID == "" {
		binding.ID = uuid.NewString()
	}
	if binding.CreatedAt.IsZero() {
		binding.CreatedAt = time.Now()
	}
	active := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var resource types.StoredResource
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND state = ?", binding.ResourceID, types.ResourceStateActive).
			First(&resource).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		active = true
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(binding).Error
	})
	return active, err
}

// DeleteBinding removes one owner's claim on a resource. Deleting a claim that
// was never recorded is not an error: callers release optimistically, from a
// content scan that cannot know which references were bound.
func (r *resourceRepository) ReleaseBindingAndMarkIfUnbound(
	ctx context.Context, resourceID, ownerType, ownerID string,
) (remaining int64, deletionClaim time.Time, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var resource types.StoredResource
		if lockErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND state = ?", resourceID, types.ResourceStateActive).
			First(&resource).Error; lockErr != nil {
			return lockErr
		}
		if deleteErr := tx.Where(
			"resource_id = ? AND owner_type = ? AND owner_id = ?", resourceID, ownerType, ownerID,
		).Delete(&types.ResourceBinding{}).Error; deleteErr != nil {
			return deleteErr
		}
		if countErr := tx.Model(&types.ResourceBinding{}).
			Where("resource_id = ?", resourceID).Count(&remaining).Error; countErr != nil {
			return countErr
		}
		if remaining != 0 {
			return nil
		}
		deletionClaim = nextResourceDeletionClaim(&resource)
		updates := map[string]any{
			"state": types.ResourceStateDeleted, "deleted_at": deletionClaim, "updated_at": deletionClaim,
		}
		if updateErr := tx.Model(&types.StoredResource{}).Where("id = ?", resourceID).
			Updates(updates).Error; updateErr != nil {
			return updateErr
		}
		return nil
	})
	return remaining, deletionClaim, err
}

func (r *resourceRepository) CreateGrant(ctx context.Context, grant *types.ResourceAccessGrant) error {
	return r.db.WithContext(ctx).Create(grant).Error
}

func (r *resourceRepository) GetValidGrant(
	ctx context.Context,
	tokenHash string,
	now time.Time,
) (*types.ResourceAccessGrant, error) {
	var grant types.ResourceAccessGrant
	err := r.db.WithContext(ctx).
		Where("token_hash = ? AND revoked_at IS NULL AND expires_at > ?", tokenHash, now).
		First(&grant).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &grant, err
}

// DeleteExpiredGrants drops grants that are past their expiry. A revoked grant
// is kept until then on purpose: it is the tombstone that stops a token derived
// for the same resource and window from re-creating the row and reviving access.
func (r *resourceRepository) DeleteExpiredGrants(ctx context.Context, before time.Time) error {
	return r.db.WithContext(ctx).
		Where("expires_at <= ?", before).
		Delete(&types.ResourceAccessGrant{}).Error
}
