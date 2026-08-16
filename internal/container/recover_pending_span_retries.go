package container

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

const pendingSpanRetryRecoveryInterval = time.Minute

type pendingSpanRetryRecoveryRunner struct {
	db       *gorm.DB
	task     interfaces.TaskEnqueuer
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func newPendingSpanRetryRecoveryRunner(
	db *gorm.DB, task interfaces.TaskEnqueuer, interval time.Duration,
) *pendingSpanRetryRecoveryRunner {
	if interval <= 0 {
		interval = pendingSpanRetryRecoveryInterval
	}
	return &pendingSpanRetryRecoveryRunner{db: db, task: task, interval: interval,
		stop: make(chan struct{}), done: make(chan struct{})}
}

func (r *pendingSpanRetryRecoveryRunner) Start() {
	recoverPendingSpanRetries(r.db, r.task)
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				recoverPendingSpanRetries(r.db, r.task)
			case <-r.stop:
				return
			}
		}
	}()
}

func (r *pendingSpanRetryRecoveryRunner) Stop() {
	r.once.Do(func() { close(r.stop) })
	<-r.done
}

func startPendingSpanRetryRecovery(
	db *gorm.DB, task interfaces.TaskEnqueuer, cleaner interfaces.ResourceCleaner,
) {
	runner := newPendingSpanRetryRecoveryRunner(db, task, pendingSpanRetryRecoveryInterval)
	runner.Start()
	cleaner.RegisterWithName("PendingSpanRetryRecovery", func() error {
		runner.Stop()
		return nil
	})
}

// recoverPendingSpanRetries closes the DB-commit -> queue-publish crash window
// for isolated postprocess repairs. The rows carry deterministic task IDs, so
// replay after a publish-before-delete crash is safe: Asynq reports a duplicate
// and the durable row is then consumed.
func recoverPendingSpanRetries(db *gorm.DB, task interfaces.TaskEnqueuer) {
	if db == nil || task == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var rows []*types.TaskPendingOp
	if err := db.WithContext(ctx).
		Where("task_type = ? AND scope = ? AND op = ?",
			types.KnowledgeSpanRetryOutboxTaskType, types.KnowledgeSpanRetryOutboxScope,
			types.KnowledgeSpanRetryOutboxOp).
		Order("id ASC").Find(&rows).Error; err != nil {
		logger.Warnf(ctx, "[SpanRetryRecovery] list durable dispatch rows failed: %v", err)
		return
	}

	recovered := 0
	for _, row := range rows {
		prepared, err := service.DecodeFailedSpanRetryOutbox(row)
		if err != nil {
			logger.Warnf(ctx, "[SpanRetryRecovery] invalid outbox row id=%d: %v", row.ID, err)
			continue
		}
		err = service.WithFailedSpanRetryDispatchGuard(prepared.TaskID, func() error {
			// Re-read both records after acquiring the per-task guard. The
			// request path may have published and acknowledged this snapshot
			// while recovery was waiting.
			var currentOp types.TaskPendingOp
			result := db.WithContext(ctx).Where("id = ? AND task_type = ? AND dedup_key = ?",
				row.ID, types.KnowledgeSpanRetryOutboxTaskType, prepared.TaskID).Take(&currentOp)
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			if result.Error != nil {
				return result.Error
			}
			currentPrepared, decodeErr := service.DecodeFailedSpanRetryOutbox(&currentOp)
			if decodeErr != nil {
				return decodeErr
			}
			var target types.KnowledgeProcessingSpan
			targetResult := db.WithContext(ctx).Where(
				"knowledge_id = ? AND attempt = ? AND span_id = ?",
				currentPrepared.KnowledgeID, currentPrepared.Attempt, currentPrepared.SpanID,
			).Take(&target)
			if errors.Is(targetResult.Error, gorm.ErrRecordNotFound) {
				logger.Warnf(ctx, "[SpanRetryRecovery] target missing for task %s; preserving outbox",
					currentPrepared.TaskID)
				return nil
			}
			if targetResult.Error != nil {
				return targetResult.Error
			}
			switch target.Status {
			case types.SpanStatusPending:
				if enqueueErr := service.EnqueueFailedSpanRetry(ctx, task, currentPrepared); enqueueErr != nil {
					return enqueueErr
				}
			case types.SpanStatusRunning, types.SpanStatusDone, types.SpanStatusFailed,
				types.SpanStatusSkipped, types.SpanStatusCancelled:
				// Publication already happened or the exact target is terminal.
				// The residual durable row is only stale acknowledgement state.
			default:
				logger.Warnf(ctx, "[SpanRetryRecovery] target task %s has unknown status %q; preserving outbox",
					currentPrepared.TaskID, target.Status)
				return nil
			}
			deleted := db.WithContext(ctx).Where("id = ? AND task_type = ? AND dedup_key = ?",
				currentOp.ID, types.KnowledgeSpanRetryOutboxTaskType, currentPrepared.TaskID).
				Delete(&types.TaskPendingOp{})
			if deleted.Error != nil {
				return deleted.Error
			}
			service.AcknowledgeFailedSpanRetryPublication(currentPrepared.TaskID)
			if deleted.RowsAffected == 1 {
				recovered++
			}
			return nil
		})
		if err != nil {
			logger.Warnf(ctx, "[SpanRetryRecovery] dispatch task %s failed: %v", prepared.TaskID, err)
		}
	}
	if recovered > 0 {
		logger.Infof(ctx, "[SpanRetryRecovery] replayed %d durable retry task(s)", recovered)
	}
}
