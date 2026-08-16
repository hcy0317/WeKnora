package container

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// pendingWikiScope is the minimum routing information needed to recreate an
// ephemeral wiki trigger from the durable task_pending_ops queue.
type pendingWikiScope struct {
	TenantID uint64 `gorm:"column:tenant_id"`
	TaskType string `gorm:"column:task_type"`
	ScopeID  string `gorm:"column:scope_id"`
}

// wikiRuntimeTaskLister is the narrow read-only queue surface startup recovery
// needs. The Redis-backed TaskInspector implements it; Lite mode deliberately
// does not because it never stamps claimed_at.
type wikiRuntimeTaskLister interface {
	ListRuntimeTasks(
		ctx context.Context,
		queue string,
		state types.RuntimeTaskState,
		cursor string,
		pageSize int,
	) (types.RuntimeTaskPage, bool, error)
}

type wikiRuntimeRecoveryInspector interface {
	wikiRuntimeTaskLister
	GetRuntimeTask(
		ctx context.Context,
		queue, taskID string,
	) (*types.RuntimeTaskInfo, bool, error)
	WithProcessingOwnerRecoveryLease(
		ctx context.Context,
		ref types.ProcessingOwnerRef,
		owner types.TaskClaimOwner,
		ttl time.Duration,
		fn func() error,
	) (supported bool, acquired bool, err error)
}

type wikiRuntimeTaskKey struct {
	TenantID        uint64
	KnowledgeBaseID string
}

type wikiRuntimeTaskEvidence struct {
	byScope map[wikiRuntimeTaskKey]struct{}
	byID    map[string]wikiRuntimeTaskKey
}

type wikiLegacyClaimCandidate struct {
	ID               int64
	TenantID         uint64
	ScopeID          string
	DedupKey         string
	Payload          []byte
	ClaimedAt        *time.Time
	ClaimToken       string
	ClaimedByTaskID  string
	ClaimHeartbeatAt *time.Time
}

var errWikiRecoveryInspectorUnsupported = errors.New("Wiki recovery inspector does not support exact owner recovery")

const wikiRecoveryOwnerLeaseTTL = 30 * time.Second

var wikiRecoveryLiveStates = []types.RuntimeTaskState{
	types.RuntimeTaskActive,
	types.RuntimeTaskPending,
	types.RuntimeTaskScheduled,
	types.RuntimeTaskRetry,
}

// recoverPendingWikiTasks recreates one trigger per pending wiki queue lane.
//
// task_pending_ops is deliberately durable, but the trigger that wakes its
// consumer is not durable in Lite mode (SyncTaskExecutor is process-local) and
// may also be absent after an interrupted Redis enqueue. Running this after all
// handlers are registered closes that gap. Duplicate triggers are harmless:
// ingest claims/peeks disjoint rows and finalize coalesces its pending lane.
func recoverPendingWikiTasks(db *gorm.DB, task interfaces.TaskEnqueuer) {
	recoverPendingWikiTasksWithInspector(db, task, nil)
}

func recoverPendingWikiTasksWithInspector(
	db *gorm.DB,
	task interfaces.TaskEnqueuer,
	inspector interfaces.TaskInspector,
) {
	if db == nil || task == nil {
		return
	}
	ctx := context.Background()
	const activeKnowledgeBase = `EXISTS (
		SELECT 1 FROM knowledge_bases kb
		WHERE kb.id = task_pending_ops.scope_id
			AND kb.tenant_id = task_pending_ops.tenant_id
			AND kb.deleted_at IS NULL
	)`
	wikiTaskTypes := []string{types.TypeWikiIngest, types.TypeWikiFinalize}

	// Complete the read-only Redis inventory before any startup repair write.
	// If one live state cannot be inspected, the absence of a task is unknown,
	// so every durable claim must remain untouched.
	if err := releaseOrphanedWikiClaims(ctx, db, inspector); err != nil {
		logger.Warnf(ctx, "[WikiRecovery] preserving claims after startup inspection failure: %v", err)
		return
	}

	// Durable rows for a deleted/missing KB must not recreate ephemeral
	// triggers at startup. Fail closed if this cleanup cannot be verified.
	cleanup := db.WithContext(ctx).
		Where("scope = ? AND task_type IN ?", types.TaskScopeKnowledgeBase, wikiTaskTypes).
		Where("NOT " + activeKnowledgeBase).
		Delete(&types.TaskPendingOp{})
	if cleanup.Error != nil {
		logger.Warnf(ctx, "[WikiRecovery] failed to clear deleted KB queues: %v", cleanup.Error)
		return
	}
	if cleanup.RowsAffected > 0 {
		logger.Infof(ctx, "[WikiRecovery] removed %d pending row(s) for deleted knowledge bases", cleanup.RowsAffected)
	}

	var scopes []pendingWikiScope
	if err := db.WithContext(ctx).
		Model(&types.TaskPendingOp{}).
		Distinct("tenant_id", "task_type", "scope_id").
		Where("scope = ? AND task_type IN ?", types.TaskScopeKnowledgeBase, wikiTaskTypes).
		Where(activeKnowledgeBase).
		Find(&scopes).Error; err != nil {
		logger.Warnf(ctx, "[WikiRecovery] failed to list pending queues: %v", err)
		return
	}

	recovered := 0
	for _, scope := range scopes {
		if scope.ScopeID == "" {
			continue
		}
		payload, err := json.Marshal(service.WikiIngestPayload{
			TenantID:        scope.TenantID,
			KnowledgeBaseID: scope.ScopeID,
		})
		if err != nil {
			logger.Warnf(ctx, "[WikiRecovery] marshal trigger for KB %s failed: %v", scope.ScopeID, err)
			continue
		}
		opts := []asynq.Option{
			asynq.Queue(types.QueueWiki),
			asynq.MaxRetry(10), // keep aligned with the wiki ingest retry policy
			asynq.Timeout(service.WikiIngestTaskTimeout),
		}
		if scope.TaskType == types.TypeWikiFinalize {
			opts[2] = asynq.Timeout(30 * time.Minute)
			// Match scheduleFinalize so simultaneous replica startups collapse
			// into the same per-KB finalize trigger.
			opts = append(opts, asynq.TaskID("wiki-finalize-"+scope.ScopeID))
		}
		trigger := asynq.NewTask(scope.TaskType, payload, opts...)
		if _, err := task.Enqueue(trigger); err != nil {
			if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
				recovered++
				continue
			}
			logger.Warnf(ctx, "[WikiRecovery] enqueue %s trigger for KB %s failed: %v",
				scope.TaskType, scope.ScopeID, err)
			continue
		}
		recovered++
	}
	if recovered > 0 {
		logger.Infof(ctx, "[WikiRecovery] recreated %d trigger(s) from durable pending queues", recovered)
	}
}

func releaseOrphanedWikiClaims(
	ctx context.Context,
	db *gorm.DB,
	inspector interfaces.TaskInspector,
) error {
	runtimeInspector, ok := inspector.(wikiRuntimeRecoveryInspector)
	if !ok || runtimeInspector == nil {
		return nil
	}

	evidence, supported, err := inspectLiveWikiTasks(ctx, runtimeInspector)
	if err != nil {
		return err
	}
	if !supported {
		return nil
	}

	staleBefore := time.Now().Add(-(service.WikiIngestTaskTimeout + 15*time.Minute))
	var candidates []wikiLegacyClaimCandidate
	if err := db.WithContext(ctx).
		Model(&types.TaskPendingOp{}).
		Select("id", "tenant_id", "scope_id", "dedup_key", "payload", "claimed_at", "claim_token", "claimed_by_task_id", "claim_heartbeat_at").
		Where("task_type = ? AND scope = ?", types.TypeWikiIngest, types.TaskScopeKnowledgeBase).
		Where(`claimed_at IS NOT NULL AND (
			(claimed_by_task_id IS NOT NULL AND claimed_by_task_id <> '') OR
			(claim_token IS NULL AND COALESCE(claim_heartbeat_at, claimed_at) < ?)
		)`, staleBefore).
		Order("id ASC").
		Find(&candidates).Error; err != nil {
		return fmt.Errorf("list stale legacy Wiki claims: %w", err)
	}

	released := int64(0)
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for i := range candidates {
			candidate := &candidates[i]
			if candidate.ClaimedByTaskID == "" || candidate.ClaimedAt == nil {
				continue
			}
			var op service.WikiPendingOp
			if err := json.Unmarshal(candidate.Payload, &op); err != nil ||
				op.KnowledgeID == "" || op.KnowledgeID != candidate.DedupKey || op.Attempt <= 0 {
				continue
			}
			key := wikiRuntimeTaskKey{TenantID: candidate.TenantID, KnowledgeBaseID: candidate.ScopeID}
			if ownerKey, exists := evidence.byID[candidate.ClaimedByTaskID]; exists {
				if ownerKey != key {
					return fmt.Errorf("Wiki task %s inventory identity does not match its durable claim", candidate.ClaimedByTaskID)
				}
				continue
			}

			ref := types.ProcessingOwnerRef{
				TenantID: candidate.TenantID, KnowledgeID: op.KnowledgeID,
				Attempt: op.Attempt, Name: "postprocess.wiki",
			}
			recoveryOwner := types.TaskClaimOwner{
				Token: uuid.NewString(), TaskID: fmt.Sprintf("wiki-startup-recovery-%d", candidate.ID),
			}
			leaseSupported, acquired, leaseErr := runtimeInspector.WithProcessingOwnerRecoveryLease(
				ctx, ref, recoveryOwner, wikiRecoveryOwnerLeaseTTL,
				func() error {
					task, exactSupported, exactErr := runtimeInspector.GetRuntimeTask(
						ctx, types.QueueWiki, candidate.ClaimedByTaskID,
					)
					if exactErr != nil {
						return fmt.Errorf("inspect exact Wiki task %s: %w", candidate.ClaimedByTaskID, exactErr)
					}
					if !exactSupported {
						return errWikiRecoveryInspectorUnsupported
					}
					if task != nil {
						if task.ID != candidate.ClaimedByTaskID || task.Queue != types.QueueWiki ||
							task.Type != types.TypeWikiIngest || task.TenantID != candidate.TenantID ||
							task.KnowledgeBaseID != candidate.ScopeID || !task.State.Valid() {
							return fmt.Errorf("exact Wiki task %s has incomplete or inconsistent identity", candidate.ClaimedByTaskID)
						}
						if wikiRecoveryStateIsLive(task.State) {
							return nil
						}
					}

					query := tx.Model(&types.TaskPendingOp{}).
						Where("id = ?", candidate.ID).
						Where("claimed_by_task_id = ? AND claimed_at = ?", candidate.ClaimedByTaskID, candidate.ClaimedAt).
						Where("claim_token IS NOT DISTINCT FROM ?", nullIfEmpty(candidate.ClaimToken))
					if candidate.ClaimHeartbeatAt == nil {
						query = query.Where("claim_heartbeat_at IS NULL")
					} else {
						query = query.Where("claim_heartbeat_at = ?", candidate.ClaimHeartbeatAt)
					}
					result := query.Updates(map[string]any{
						"claimed_at": nil, "claim_token": nil,
						"claimed_by_task_id": nil, "claim_heartbeat_at": nil,
					})
					if result.Error != nil {
						return fmt.Errorf("release stale legacy Wiki claim %d: %w", candidate.ID, result.Error)
					}
					released += result.RowsAffected
					return nil
				},
			)
			if leaseErr != nil {
				return leaseErr
			}
			if !leaseSupported {
				return errWikiRecoveryInspectorUnsupported
			}
			if !acquired {
				continue
			}
		}
		return nil
	})
	if errors.Is(err, errWikiRecoveryInspectorUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}
	if released > 0 {
		logger.Infof(ctx, "[WikiRecovery] released %d orphaned Wiki claim(s) for immediate retry", released)
	}
	return nil
}

func nullIfEmpty(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func wikiRecoveryStateIsLive(state types.RuntimeTaskState) bool {
	for _, liveState := range wikiRecoveryLiveStates {
		if state == liveState {
			return true
		}
	}
	return false
}

func inspectLiveWikiTasks(
	ctx context.Context,
	lister wikiRuntimeTaskLister,
) (wikiRuntimeTaskEvidence, bool, error) {
	evidence := wikiRuntimeTaskEvidence{
		byScope: make(map[wikiRuntimeTaskKey]struct{}),
		byID:    make(map[string]wikiRuntimeTaskKey),
	}
	for _, state := range wikiRecoveryLiveStates {
		cursor := ""
		for {
			page, supported, err := lister.ListRuntimeTasks(ctx, types.QueueWiki, state, cursor, 100)
			if err != nil {
				return evidence, true, fmt.Errorf("inspect Wiki %s tasks: %w", state, err)
			}
			if !supported {
				return evidence, false, nil
			}
			for _, task := range page.Tasks {
				if task.Type != types.TypeWikiIngest {
					continue
				}
				if task.ID == "" || task.TenantID == 0 || task.KnowledgeBaseID == "" || task.State != state {
					return evidence, true, fmt.Errorf("Wiki %s task has incomplete or inconsistent identity", state)
				}
				key := wikiRuntimeTaskKey{TenantID: task.TenantID, KnowledgeBaseID: task.KnowledgeBaseID}
				evidence.byScope[key] = struct{}{}
				evidence.byID[task.ID] = key
			}
			if !page.HasMore {
				break
			}
			if page.NextCursor == "" || page.NextCursor == cursor {
				return evidence, true, fmt.Errorf("Wiki %s task pagination stalled", state)
			}
			cursor = page.NextCursor
		}
	}
	return evidence, true, nil
}
