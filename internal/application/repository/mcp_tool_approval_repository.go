package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MCPToolApprovalRepository implements interfaces.MCPToolApprovalRepository.
type MCPToolApprovalRepository struct {
	db *gorm.DB
}

// NewMCPToolApprovalRepository creates a repository backed by GORM.
func NewMCPToolApprovalRepository(db *gorm.DB) interfaces.MCPToolApprovalRepository {
	return &MCPToolApprovalRepository{db: db}
}

// ListByService returns all stored approval rows for an MCP service (may be empty).
func (r *MCPToolApprovalRepository) ListByService(ctx context.Context, tenantID uint64, serviceID string) ([]*types.MCPToolApproval, error) {
	var rows []*types.MCPToolApproval
	err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND service_id = ?", tenantID, serviceID).
		Order("tool_name ASC").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("list mcp tool approvals: %w", err)
	}
	return rows, nil
}

// IsRequired returns true when a row exists with require_approval = true.
func (r *MCPToolApprovalRepository) IsRequired(ctx context.Context, tenantID uint64, serviceID, toolName string) (bool, error) {
	var row types.MCPToolApproval
	err := r.db.WithContext(ctx).
		Select("require_approval").
		Where("tenant_id = ? AND service_id = ? AND tool_name = ?", tenantID, serviceID, toolName).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get mcp tool approval: %w", err)
	}
	return row.RequireApproval, nil
}

// IsEnabled returns true when no policy row exists, preserving the historic
// default; a stored row can explicitly disable an exposed tool.
func (r *MCPToolApprovalRepository) IsEnabled(
	ctx context.Context, tenantID uint64, serviceID, toolName string,
) (bool, error) {
	var row types.MCPToolApproval
	err := r.db.WithContext(ctx).
		Select("enabled").
		Where("tenant_id = ? AND service_id = ? AND tool_name = ?", tenantID, serviceID, toolName).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get mcp tool enabled state: %w", err)
	}
	return row.Enabled, nil
}

// UpsertPolicy creates or patches only the policy columns supplied by the
// caller. Concurrent edits to approval and enabled remain independent.
func (r *MCPToolApprovalRepository) UpsertPolicy(
	ctx context.Context, tenantID uint64, serviceID, toolName string, patch types.MCPToolPolicyPatch,
) error {
	if serviceID == "" || toolName == "" {
		return errors.New("service_id and tool_name are required")
	}
	if patch.RequireApproval == nil && patch.Enabled == nil {
		return errors.New("require_approval or enabled is required")
	}

	now := time.Now()
	requireApproval := false
	if patch.RequireApproval != nil {
		requireApproval = *patch.RequireApproval
	}
	enabled := true
	if patch.Enabled != nil {
		enabled = *patch.Enabled
	}
	updates := map[string]interface{}{"updated_at": now}
	if patch.RequireApproval != nil {
		updates["require_approval"] = *patch.RequireApproval
	}
	if patch.Enabled != nil {
		updates["enabled"] = *patch.Enabled
	}
	row := map[string]interface{}{
		"id": uuid.New().String(), "tenant_id": tenantID, "service_id": serviceID,
		"tool_name": toolName, "require_approval": requireApproval, "enabled": enabled,
		"created_at": now, "updated_at": now,
	}
	// Create via a map so enabled=false is persisted; GORM struct inserts can
	// omit bool zero values when the model field has a default tag.
	err := r.db.WithContext(ctx).Model(&types.MCPToolApproval{}).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tenant_id"}, {Name: "service_id"}, {Name: "tool_name"},
		},
		DoUpdates: clause.Assignments(updates),
	}).Create(row).Error
	if err != nil {
		return fmt.Errorf("upsert mcp tool policy: %w", err)
	}
	return nil
}

// Upsert creates or updates the approval flag for a tool atomically.
// Uses ON CONFLICT against the (tenant_id, service_id, tool_name) unique index
// so concurrent writers don't race the prior SELECT-then-INSERT path into
// duplicate-key 500s.
func (r *MCPToolApprovalRepository) Upsert(ctx context.Context, row *types.MCPToolApproval) error {
	if row == nil {
		return errors.New("row is nil")
	}
	if row.ID == "" {
		row.ID = uuid.New().String()
	}
	now := time.Now()
	row.UpdatedAt = now
	if row.CreatedAt.IsZero() {
		row.CreatedAt = now
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tenant_id"},
			{Name: "service_id"},
			{Name: "tool_name"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"require_approval": row.RequireApproval,
			"updated_at":       now,
		}),
	}).Create(row).Error
	if err != nil {
		return fmt.Errorf("upsert mcp tool approval: %w", err)
	}
	return nil
}
