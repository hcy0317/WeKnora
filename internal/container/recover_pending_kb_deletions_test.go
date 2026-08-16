package container

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type kbDeletionRecoveryInspector struct {
	interfaces.TaskInspector
	interfaces.RuntimeTaskInspector
	task      *types.RuntimeTaskInfo
	supported bool
	runCalls  int
}

func (i *kbDeletionRecoveryInspector) GetRuntimeTask(
	context.Context, string, string,
) (*types.RuntimeTaskInfo, bool, error) {
	return i.task, i.supported, nil
}
func (i *kbDeletionRecoveryInspector) RunRuntimeTask(context.Context, string, string) (bool, error) {
	i.runCalls++
	return true, nil
}

func TestPendingKBDeletionRecoveryRuntimeStates(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       *types.RuntimeTaskInfo
		wantEnqueue int
		wantRun     int
	}{
		{name: "missing enqueues", wantEnqueue: 1},
		{name: "active no duplicate", state: &types.RuntimeTaskInfo{State: types.RuntimeTaskActive}},
		{name: "archived requeues same task", state: &types.RuntimeTaskInfo{State: types.RuntimeTaskArchived}, wantRun: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := setupResetPendingDB(t)
			payload, err := json.Marshal(types.KBDeletePayload{TenantID: 7, KnowledgeBaseID: "kb-1"})
			require.NoError(t, err)
			require.NoError(t, db.Exec(`INSERT INTO task_pending_ops
				(tenant_id, task_type, scope, scope_id, op, dedup_key, payload)
				VALUES (?, ?, ?, ?, ?, ?, ?)`, 7, types.TypeKBDelete,
				types.TaskScopeKnowledgeBaseDeletion, "kb-1", "delete", "kb-delete:7:kb-1", payload).Error)
			recorder := &recordingTaskEnqueuer{}
			inspector := &kbDeletionRecoveryInspector{task: test.state, supported: true}
			runner := &pendingKBDeletionRecoveryRunner{db: db, task: recorder, inspector: inspector}
			runner.run()
			require.Len(t, recorder.tasks, test.wantEnqueue)
			if test.wantEnqueue == 1 {
				var taskID any
				for _, opt := range recorder.opts[0] {
					if opt.Type() == asynq.TaskIDOpt {
						taskID = opt.Value()
					}
				}
				require.Equal(t, "kb-delete:7:kb-1", taskID)
			}
			require.Equal(t, test.wantRun, inspector.runCalls)
			var rows int64
			require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&rows).Error)
			require.Equal(t, int64(1), rows, "recovery must retain outbox until worker ack")
		})
	}
}

func TestPendingKBDeletionRecoveryRejectsForgedPayload(t *testing.T) {
	db := setupResetPendingDB(t)
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 8, KnowledgeBaseID: "kb-forged"})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO task_pending_ops
		(tenant_id, task_type, scope, scope_id, op, dedup_key, payload)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, 7, types.TypeKBDelete,
		types.TaskScopeKnowledgeBaseDeletion, "kb-1", "delete", "kb-delete:7:kb-1", payload).Error)
	recorder := &recordingTaskEnqueuer{}
	(&pendingKBDeletionRecoveryRunner{db: db, task: recorder}).run()
	require.Empty(t, recorder.tasks)
}
