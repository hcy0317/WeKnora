package container

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

const pendingKBDeletionRecoveryInterval = time.Minute

type pendingKBDeletionRecoveryRunner struct {
	db         *gorm.DB
	task       interfaces.TaskEnqueuer
	inspector  interfaces.TaskInspector
	interval   time.Duration
	stop, done chan struct{}
	once       sync.Once
}

func startPendingKBDeletionRecovery(
	db *gorm.DB, task interfaces.TaskEnqueuer, inspector interfaces.TaskInspector,
	cleaner interfaces.ResourceCleaner,
) {
	runner := &pendingKBDeletionRecoveryRunner{
		db: db, task: task, inspector: inspector, interval: pendingKBDeletionRecoveryInterval,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	runner.run()
	go func() {
		defer close(runner.done)
		ticker := time.NewTicker(runner.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runner.run()
			case <-runner.stop:
				return
			}
		}
	}()
	cleaner.RegisterWithName("PendingKBDeletionRecovery", func() error {
		runner.once.Do(func() { close(runner.stop) })
		<-runner.done
		return nil
	})
}

func (r *pendingKBDeletionRecoveryRunner) run() {
	if r.db == nil || r.task == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var rows []*types.TaskPendingOp
	if err := r.db.WithContext(ctx).Where(
		"task_type = ? AND scope = ? AND op = ?",
		types.TypeKBDelete, types.TaskScopeKnowledgeBaseDeletion, "delete",
	).Order("id ASC").Find(&rows).Error; err != nil {
		logger.Warnf(ctx, "[KBDeleteRecovery] list outbox failed: %v", err)
		return
	}
	runtimeInspector, inspectable := r.inspector.(interfaces.RuntimeTaskInspector)
	for _, row := range rows {
		var payload types.KBDeletePayload
		if err := json.Unmarshal(row.Payload, &payload); err != nil ||
			row.TenantID == 0 || row.ScopeID == "" || row.DedupKey == "" ||
			payload.TenantID != row.TenantID || payload.KnowledgeBaseID != row.ScopeID ||
			row.DedupKey != fmt.Sprintf("kb-delete:%d:%s", row.TenantID, row.ScopeID) {
			logger.Warnf(ctx, "[KBDeleteRecovery] refusing forged outbox row id=%d", row.ID)
			continue
		}
		if inspectable {
			runtimeTask, supported, err := runtimeInspector.GetRuntimeTask(ctx, types.QueueMaintenance, row.DedupKey)
			if err != nil {
				logger.Warnf(ctx, "[KBDeleteRecovery] inspect %s failed: %v", row.DedupKey, err)
				continue
			}
			if supported && runtimeTask != nil {
				switch runtimeTask.State {
				case types.RuntimeTaskArchived:
					if _, err := runtimeInspector.RunRuntimeTask(ctx, types.QueueMaintenance, row.DedupKey); err != nil {
						logger.Warnf(ctx, "[KBDeleteRecovery] revive %s failed: %v", row.DedupKey, err)
					}
				case types.RuntimeTaskPending, types.RuntimeTaskActive,
					types.RuntimeTaskScheduled, types.RuntimeTaskRetry, types.RuntimeTaskCompleted:
				}
				continue
			}
		}
		task := asynq.NewTask(types.TypeKBDelete, row.Payload)
		if _, err := r.task.Enqueue(task,
			asynq.Queue(types.QueueMaintenance), asynq.MaxRetry(3), asynq.Timeout(2*time.Hour),
			asynq.Retention(24*time.Hour), asynq.TaskID(row.DedupKey)); err != nil &&
			!errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
			logger.Warnf(ctx, "[KBDeleteRecovery] enqueue %s failed: %v", row.DedupKey, err)
		}
	}
}
