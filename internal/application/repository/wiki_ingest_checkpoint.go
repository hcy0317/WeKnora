package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func wikiCheckpointJSONEqual(left, right []byte) bool {
	left = bytes.TrimSpace(left)
	right = bytes.TrimSpace(right)
	if bytes.Equal(left, right) {
		return true
	}
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validateWikiIngestWorkUnit(unit *types.WikiIngestWorkUnit) error {
	if unit == nil || strings.TrimSpace(unit.WorkID) == "" || unit.TenantID == 0 ||
		strings.TrimSpace(unit.KnowledgeBaseID) == "" || strings.TrimSpace(unit.KnowledgeID) == "" ||
		strings.TrimSpace(unit.SourceRevisionDigest) == "" || strings.TrimSpace(unit.SourceDocumentKey) == "" ||
		strings.TrimSpace(unit.GenerationContractKey) == "" || strings.TrimSpace(unit.RuntimeSnapshotKey) == "" {
		return errors.New("prepare wiki work unit: complete identity is required")
	}
	return nil
}

func sameWikiIngestWorkIdentity(left, right *types.WikiIngestWorkUnit) bool {
	return left != nil && right != nil && left.WorkID == right.WorkID &&
		left.TenantID == right.TenantID && left.KnowledgeBaseID == right.KnowledgeBaseID &&
		left.KnowledgeID == right.KnowledgeID && left.SourceRevisionDigest == right.SourceRevisionDigest &&
		left.SourceDocumentKey == right.SourceDocumentKey &&
		left.GenerationContractKey == right.GenerationContractKey &&
		left.RuntimeSnapshotKey == right.RuntimeSnapshotKey
}

func (r *wikiPageRepository) PrepareWikiIngestWorkUnit(
	ctx context.Context, unit *types.WikiIngestWorkUnit,
) (*types.WikiIngestWorkUnit, error) {
	if err := validateWikiIngestWorkUnit(unit); err != nil {
		return nil, err
	}
	if unit.State == "" {
		unit.State = types.WikiIngestWorkUnitPrepared
	}
	if len(unit.MappedOutput) == 0 {
		unit.MappedOutput = types.JSON([]byte(`{}`))
	}
	var stored types.WikiIngestWorkUnit
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", unit.KnowledgeID).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&types.WikiIngestWorkUnit{}).
			Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND work_id <> ? AND state IN ?",
				unit.TenantID, unit.KnowledgeBaseID, unit.KnowledgeID, unit.WorkID,
				[]types.WikiIngestWorkUnitState{types.WikiIngestWorkUnitPrepared, types.WikiIngestWorkUnitMapped}).
			Update("state", types.WikiIngestWorkUnitAbandoned).Error; err != nil {
			return fmt.Errorf("abandon stale wiki work units: %w", err)
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(unit).Error; err != nil {
			return err
		}
		return tx.Where("work_id = ?", unit.WorkID).First(&stored).Error
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

// PrepareAndBindWikiIngestWorkUnit atomically pins the exact source Wiki span
// to one attempt-independent work unit. A copied binding on a partial retry
// wins over current model/config settings; source/title drift never does.
func (r *wikiPageRepository) PrepareAndBindWikiIngestWorkUnit(
	ctx context.Context, binding types.WikiIngestWorkBinding, unit *types.WikiIngestWorkUnit,
) (*types.WikiIngestWorkUnit, error) {
	if err := validateWikiIngestWorkUnit(unit); err != nil {
		return nil, err
	}
	if binding.KnowledgeID == "" || binding.Attempt <= 0 || binding.SpanID == "" ||
		binding.KnowledgeID != unit.KnowledgeID || binding.SourceRevisionDigest != unit.SourceRevisionDigest ||
		binding.SourceDocumentKey != unit.SourceDocumentKey {
		return nil, errors.New("bind wiki work unit: exact span and source identity are required")
	}
	if unit.State == "" {
		unit.State = types.WikiIngestWorkUnitPrepared
	}
	if len(unit.MappedOutput) == 0 {
		unit.MappedOutput = types.JSON([]byte(`{}`))
	}

	var stored types.WikiIngestWorkUnit
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", unit.KnowledgeID).Error; err != nil {
				return err
			}
		}
		var span types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			binding.KnowledgeID, binding.Attempt, binding.SpanID).First(&span).Error; err != nil {
			return fmt.Errorf("load wiki owner span: %w", err)
		}

		var pinned types.WikiIngestWorkBinding
		if raw, ok := span.Input[types.WikiIngestWorkBindingInputKey]; ok && raw != nil {
			encoded, err := json.Marshal(raw)
			if err != nil || json.Unmarshal(encoded, &pinned) != nil || pinned.WorkID == "" {
				return errors.New("bind wiki work unit: invalid existing binding")
			}
			if err := tx.Where("work_id = ?", pinned.WorkID).First(&stored).Error; err != nil {
				return fmt.Errorf("load bound wiki work unit: %w", err)
			}
			if stored.TenantID != unit.TenantID || stored.KnowledgeBaseID != unit.KnowledgeBaseID ||
				stored.KnowledgeID != unit.KnowledgeID || stored.SourceRevisionDigest != pinned.SourceRevisionDigest ||
				stored.SourceDocumentKey != pinned.SourceDocumentKey ||
				(pinned.GenerationContractKey != "" && stored.GenerationContractKey != pinned.GenerationContractKey) ||
				(pinned.RuntimeSnapshotKey != "" && stored.RuntimeSnapshotKey != pinned.RuntimeSnapshotKey) {
				return errors.New("bind wiki work unit: bound work unit identity differs")
			}
			sameSource := pinned.SourceRevisionDigest == binding.SourceRevisionDigest &&
				pinned.SourceDocumentKey == binding.SourceDocumentKey
			if sameSource && stored.State == types.WikiIngestWorkUnitMapped {
				return nil
			}
			if sameSource && stored.State == types.WikiIngestWorkUnitPrepared &&
				stored.GenerationContractKey == unit.GenerationContractKey &&
				stored.RuntimeSnapshotKey == unit.RuntimeSnapshotKey {
				if stored.WorkID != unit.WorkID {
					return errors.New("bind wiki work unit: prepared work id differs for identical identity")
				}
				return nil
			}
			if stored.State != types.WikiIngestWorkUnitPrepared && stored.State != types.WikiIngestWorkUnitMapped {
				return fmt.Errorf("bind wiki work unit: bound state %s is not reusable", stored.State)
			}
			if err := tx.Model(&types.WikiIngestWorkUnit{}).
				Where("work_id = ? AND state = ?", pinned.WorkID, stored.State).
				Update("state", types.WikiIngestWorkUnitAbandoned).Error; err != nil {
				return fmt.Errorf("abandon drifted wiki work unit: %w", err)
			}
			stored = types.WikiIngestWorkUnit{}
		} else {
			var legacy []types.WikiIngestWorkUnit
			if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND source_revision_digest = ? AND source_document_key = ? AND state = ?",
				unit.TenantID, unit.KnowledgeBaseID, unit.KnowledgeID, unit.SourceRevisionDigest,
				unit.SourceDocumentKey, types.WikiIngestWorkUnitMapped).Limit(2).Find(&legacy).Error; err != nil {
				return err
			}
			if len(legacy) > 1 {
				return errors.New("bind wiki work unit: ambiguous legacy mapped checkpoints")
			}
			if len(legacy) == 1 {
				stored = legacy[0]
			}
		}

		if stored.WorkID == "" {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(unit).Error; err != nil {
				return err
			}
			if err := tx.Where("work_id = ?", unit.WorkID).First(&stored).Error; err != nil {
				return err
			}
			if !sameWikiIngestWorkIdentity(&stored, unit) {
				return errors.New("bind wiki work unit: candidate work unit identity differs")
			}
		}
		binding.WorkID = stored.WorkID
		binding.GenerationContractKey = stored.GenerationContractKey
		binding.RuntimeSnapshotKey = stored.RuntimeSnapshotKey
		if span.Input == nil {
			span.Input = types.JSONMap{}
		}
		span.Input[types.WikiIngestWorkBindingInputKey] = binding
		return tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("id = ?", span.ID).Update("input", span.Input).Error
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

func (r *wikiPageRepository) MarkWikiIngestWorkUnitMapped(
	ctx context.Context, workID string, output types.JSON,
) error {
	if strings.TrimSpace(workID) == "" || len(output) == 0 {
		return errors.New("map wiki work unit: work id and output are required")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var unit types.WikiIngestWorkUnit
		if err := tx.Where("work_id = ?", workID).First(&unit).Error; err != nil {
			return err
		}
		switch unit.State {
		case types.WikiIngestWorkUnitPrepared:
			return tx.Model(&unit).Updates(map[string]any{
				"state": types.WikiIngestWorkUnitMapped, "mapped_output": output,
			}).Error
		case types.WikiIngestWorkUnitMapped:
			if wikiCheckpointJSONEqual(unit.MappedOutput, output) {
				return nil
			}
			return errors.New("map wiki work unit: canonical output already differs")
		default:
			return fmt.Errorf("map wiki work unit: state %s is not writable", unit.State)
		}
	})
}

func (r *wikiPageRepository) PrepareWikiTaxonomyPlan(
	ctx context.Context, plan *types.WikiTaxonomyPlan,
) (*types.WikiTaxonomyPlan, error) {
	if plan == nil || plan.PlanID == "" || plan.TenantID == 0 || plan.KnowledgeBaseID == "" ||
		plan.WorkSetDigest == "" || plan.MissingSetDigest == "" || plan.FolderBaseDigest == "" || plan.ContractKey == "" {
		return nil, errors.New("prepare wiki taxonomy plan: complete identity is required")
	}
	if plan.State == "" {
		plan.State = types.WikiTaxonomyPlanPrepared
	}
	if len(plan.ResolvedOutput) == 0 {
		plan.ResolvedOutput = types.JSON([]byte(`{}`))
	}
	var stored types.WikiTaxonomyPlan
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		stableLock := fmt.Sprintf("wiki-taxonomy:%d:%s:%s:%s:%s", plan.TenantID, plan.KnowledgeBaseID,
			plan.WorkSetDigest, plan.MissingSetDigest, plan.ContractKey)
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", stableLock).Error; err != nil {
				return err
			}
		}
		var mapped []types.WikiTaxonomyPlan
		if err := tx.Where(
			"tenant_id = ? AND knowledge_base_id = ? AND work_set_digest = ? AND missing_set_digest = ? AND contract_key = ? AND state = ?",
			plan.TenantID, plan.KnowledgeBaseID, plan.WorkSetDigest, plan.MissingSetDigest,
			plan.ContractKey, types.WikiTaxonomyPlanMapped,
		).Order("created_at ASC").Limit(2).Find(&mapped).Error; err != nil {
			return err
		}
		if len(mapped) > 1 {
			return errors.New("prepare wiki taxonomy plan: ambiguous mapped checkpoints")
		}
		if len(mapped) == 1 {
			stored = mapped[0]
			return nil
		}
		if err := tx.Model(&types.WikiTaxonomyPlan{}).
			Where("tenant_id = ? AND knowledge_base_id = ? AND work_set_digest = ? AND missing_set_digest = ? AND contract_key = ? AND plan_id <> ? AND state = ?",
				plan.TenantID, plan.KnowledgeBaseID, plan.WorkSetDigest, plan.MissingSetDigest,
				plan.ContractKey, plan.PlanID, types.WikiTaxonomyPlanPrepared).
			Update("state", types.WikiTaxonomyPlanAbandoned).Error; err != nil {
			return fmt.Errorf("abandon stale taxonomy plan: %w", err)
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(plan).Error; err != nil {
			return err
		}
		if err := tx.Where("plan_id = ?", plan.PlanID).First(&stored).Error; err != nil {
			return err
		}
		if stored.TenantID != plan.TenantID || stored.KnowledgeBaseID != plan.KnowledgeBaseID ||
			stored.WorkSetDigest != plan.WorkSetDigest || stored.MissingSetDigest != plan.MissingSetDigest ||
			stored.FolderBaseDigest != plan.FolderBaseDigest || stored.ContractKey != plan.ContractKey {
			return errors.New("prepare wiki taxonomy plan: existing plan identity differs")
		}
		if stored.State == types.WikiTaxonomyPlanAbandoned {
			result := tx.Model(&types.WikiTaxonomyPlan{}).
				Where("plan_id = ? AND state = ?", stored.PlanID, types.WikiTaxonomyPlanAbandoned).
				Update("state", types.WikiTaxonomyPlanPrepared)
			if result.Error != nil {
				return fmt.Errorf("revive matching taxonomy plan: %w", result.Error)
			}
			if result.RowsAffected != 1 {
				return errors.New("revive matching taxonomy plan: state changed concurrently")
			}
			stored.State = types.WikiTaxonomyPlanPrepared
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

func (r *wikiPageRepository) FindMappedWikiTaxonomyPlan(
	ctx context.Context, tenantID uint64, knowledgeBaseID, workSetDigest, missingSetDigest, contractKey string,
) (*types.WikiTaxonomyPlan, error) {
	if tenantID == 0 || knowledgeBaseID == "" || workSetDigest == "" || missingSetDigest == "" || contractKey == "" {
		return nil, errors.New("find mapped wiki taxonomy plan: stable identity is required")
	}
	var plans []types.WikiTaxonomyPlan
	if err := r.db.WithContext(ctx).Where(
		"tenant_id = ? AND knowledge_base_id = ? AND work_set_digest = ? AND missing_set_digest = ? AND contract_key = ? AND state = ?",
		tenantID, knowledgeBaseID, workSetDigest, missingSetDigest, contractKey, types.WikiTaxonomyPlanMapped,
	).Order("created_at ASC").Limit(2).Find(&plans).Error; err != nil {
		return nil, err
	}
	if len(plans) > 1 {
		return nil, errors.New("find mapped wiki taxonomy plan: ambiguous mapped checkpoints")
	}
	if len(plans) == 0 {
		return nil, nil
	}
	return &plans[0], nil
}

func (r *wikiPageRepository) SaveWikiTaxonomyPlanProgress(
	ctx context.Context, planID string, expected, output types.JSON,
) error {
	if planID == "" || len(expected) == 0 || len(output) == 0 {
		return errors.New("save wiki taxonomy progress: plan id, expected and output are required")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var plan types.WikiTaxonomyPlan
		query := tx.Where("plan_id = ?", planID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.First(&plan).Error; err != nil {
			return err
		}
		if plan.State != types.WikiTaxonomyPlanPrepared {
			return fmt.Errorf("save wiki taxonomy progress: plan state is %s", plan.State)
		}
		if wikiCheckpointJSONEqual(plan.ResolvedOutput, output) {
			return nil
		}
		if !wikiCheckpointJSONEqual(plan.ResolvedOutput, expected) {
			return errors.New("save wiki taxonomy progress: checkpoint changed concurrently")
		}
		return tx.Model(&plan).Update("resolved_output", output).Error
	})
}

func (r *wikiPageRepository) MarkWikiTaxonomyPlanMapped(
	ctx context.Context, planID string, output types.JSON,
) error {
	if planID == "" || len(output) == 0 {
		return errors.New("map wiki taxonomy plan: plan id and output are required")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var plan types.WikiTaxonomyPlan
		if err := tx.Where("plan_id = ?", planID).First(&plan).Error; err != nil {
			return err
		}
		switch plan.State {
		case types.WikiTaxonomyPlanPrepared:
			return tx.Model(&plan).Updates(map[string]any{"state": types.WikiTaxonomyPlanMapped, "resolved_output": output}).Error
		case types.WikiTaxonomyPlanMapped:
			if wikiCheckpointJSONEqual(plan.ResolvedOutput, output) {
				return nil
			}
			return errors.New("map wiki taxonomy plan: canonical output already differs")
		default:
			return fmt.Errorf("map wiki taxonomy plan: state %s is not writable", plan.State)
		}
	})
}

func (r *wikiPageRepository) PrepareWikiSlugApplication(
	ctx context.Context, application *types.WikiSlugApplication,
) (*types.WikiSlugApplication, error) {
	if application == nil || strings.TrimSpace(application.PlanID) == "" || application.TenantID == 0 ||
		strings.TrimSpace(application.KnowledgeBaseID) == "" || strings.TrimSpace(application.Slug) == "" ||
		strings.TrimSpace(application.ContributionKey) == "" || strings.TrimSpace(application.ExpectedPageHash) == "" ||
		strings.TrimSpace(application.OperationDigest) == "" {
		return nil, errors.New("prepare wiki slug application: complete identity is required")
	}
	if application.State == "" {
		application.State = types.WikiSlugApplicationPrepared
	}
	var stored types.WikiSlugApplication
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			lockKey := application.KnowledgeBaseID + ":" + application.Slug
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockKey).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&types.WikiSlugApplication{}).
			Where("tenant_id = ? AND knowledge_base_id = ? AND slug = ? AND contribution_key = ? AND plan_id <> ? AND state IN ?",
				application.TenantID, application.KnowledgeBaseID, application.Slug,
				application.ContributionKey, application.PlanID,
				[]types.WikiSlugApplicationState{types.WikiSlugApplicationPrepared, types.WikiSlugApplicationApplying}).
			Update("state", types.WikiSlugApplicationAbandoned).Error; err != nil {
			return fmt.Errorf("abandon stale wiki slug application: %w", err)
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(application).Error; err != nil {
			return err
		}
		return tx.Where("plan_id = ?", application.PlanID).First(&stored).Error
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

func (r *wikiPageRepository) MarkWikiSlugApplicationApplying(
	ctx context.Context, planID, generatedOutput string,
) error {
	if strings.TrimSpace(planID) == "" || strings.TrimSpace(generatedOutput) == "" {
		return errors.New("apply wiki slug plan: plan id and generated output are required")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var application types.WikiSlugApplication
		if err := tx.Where("plan_id = ?", planID).First(&application).Error; err != nil {
			return err
		}
		switch application.State {
		case types.WikiSlugApplicationPrepared:
			return tx.Model(&application).Updates(map[string]any{
				"state": types.WikiSlugApplicationApplying, "generated_output": generatedOutput,
			}).Error
		case types.WikiSlugApplicationApplying:
			if application.GeneratedOutput == generatedOutput {
				return nil
			}
			return errors.New("apply wiki slug plan: generated output already differs")
		default:
			return fmt.Errorf("apply wiki slug plan: state %s is not writable", application.State)
		}
	})
}

func (r *wikiPageRepository) FindWikiSlugApplication(
	ctx context.Context, tenantID uint64, knowledgeBaseID, slug, contributionKey string,
) (*types.WikiSlugApplication, error) {
	var application types.WikiSlugApplication
	err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND knowledge_base_id = ? AND slug = ? AND contribution_key = ? AND state IN ?",
			tenantID, knowledgeBaseID, slug, contributionKey,
			[]types.WikiSlugApplicationState{types.WikiSlugApplicationApplying, types.WikiSlugApplicationPublished}).
		Order("updated_at DESC").First(&application).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &application, nil
}

func (r *wikiPageRepository) ListWikiSlugContributionMarkers(
	ctx context.Context, workIDs []string,
) ([]types.WikiSlugContributionMarker, error) {
	if len(workIDs) == 0 {
		return nil, nil
	}
	var markers []types.WikiSlugContributionMarker
	err := r.db.WithContext(ctx).Where("work_id IN ?", workIDs).Find(&markers).Error
	return markers, err
}

func applyWikiSlugApplicationTransition(tx *gorm.DB, ctx context.Context) error {
	transition, ok := types.WikiSlugApplicationTransitionFromContext(ctx)
	if !ok {
		return nil
	}
	if transition.State != types.WikiSlugApplicationApplying && transition.State != types.WikiSlugApplicationPublished {
		return fmt.Errorf("wiki slug application transition: invalid state %s", transition.State)
	}
	var application types.WikiSlugApplication
	if err := tx.Where("plan_id = ?", transition.PlanID).First(&application).Error; err != nil {
		return err
	}
	if transition.State == types.WikiSlugApplicationApplying && application.State != types.WikiSlugApplicationApplying {
		return fmt.Errorf("wiki slug application %s is not applying", transition.PlanID)
	}
	if transition.State == types.WikiSlugApplicationPublished &&
		application.State != types.WikiSlugApplicationApplying && application.State != types.WikiSlugApplicationPublished {
		return fmt.Errorf("wiki slug application %s cannot publish from %s", transition.PlanID, application.State)
	}
	for i := range transition.Markers {
		marker := transition.Markers[i]
		marker.PlanID = transition.PlanID
		marker.State = transition.State
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "work_id"}, {Name: "slug"}, {Name: "operation_digest"}},
			DoUpdates: clause.Assignments(map[string]any{"plan_id": transition.PlanID, "state": transition.State}),
		}).Create(&marker).Error; err != nil {
			return err
		}
	}
	return tx.Model(&application).Update("state", transition.State).Error
}
