package container

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type wikiRecoveryTaskInspector struct {
	interfaces.TaskInspector
	tasks          map[types.RuntimeTaskState][]types.RuntimeTaskInfo
	exactTasks     map[string]*types.RuntimeTaskInfo
	supported      bool
	errors         map[types.RuntimeTaskState]error
	exactErrors    map[string]error
	exactHooks     map[string]func()
	onList         map[types.RuntimeTaskState]func()
	calls          []types.RuntimeTaskState
	exactTaskCalls []string
	leaseMu        sync.Mutex
	leaseCalls     []types.ProcessingOwnerRef
}

func (i *wikiRecoveryTaskInspector) ListRuntimeTasks(
	_ context.Context,
	queue string,
	state types.RuntimeTaskState,
	_ string,
	_ int,
) (types.RuntimeTaskPage, bool, error) {
	i.calls = append(i.calls, state)
	if hook := i.onList[state]; hook != nil {
		hook()
	}
	if queue != types.QueueWiki {
		return types.RuntimeTaskPage{}, i.supported, errors.New("unexpected queue")
	}
	if err := i.errors[state]; err != nil {
		return types.RuntimeTaskPage{}, i.supported, err
	}
	return types.RuntimeTaskPage{Tasks: append([]types.RuntimeTaskInfo(nil), i.tasks[state]...)}, i.supported, nil
}

func (i *wikiRecoveryTaskInspector) GetRuntimeTask(
	_ context.Context,
	queue, taskID string,
) (*types.RuntimeTaskInfo, bool, error) {
	i.exactTaskCalls = append(i.exactTaskCalls, taskID)
	if queue != types.QueueWiki {
		return nil, i.supported, errors.New("unexpected queue")
	}
	if err := i.exactErrors[taskID]; err != nil {
		return nil, i.supported, err
	}
	if hook := i.exactHooks[taskID]; hook != nil {
		hook()
	}
	if task := i.exactTasks[taskID]; task != nil {
		copy := *task
		return &copy, i.supported, nil
	}
	for _, tasks := range i.tasks {
		for index := range tasks {
			if tasks[index].ID == taskID {
				copy := tasks[index]
				return &copy, i.supported, nil
			}
		}
	}
	return nil, i.supported, nil
}

func (i *wikiRecoveryTaskInspector) WithProcessingOwnerRecoveryLease(
	_ context.Context,
	ref types.ProcessingOwnerRef,
	_ types.TaskClaimOwner,
	_ time.Duration,
	fn func() error,
) (bool, bool, error) {
	if !i.supported {
		return false, false, nil
	}
	i.leaseMu.Lock()
	defer i.leaseMu.Unlock()
	i.leaseCalls = append(i.leaseCalls, ref)
	return true, true, fn()
}

func setupWikiRecoveryDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := setupResetPendingDB(t)
	for _, statement := range []string{
		`ALTER TABLE task_pending_ops ADD COLUMN claim_token VARCHAR(64)`,
		`ALTER TABLE task_pending_ops ADD COLUMN claimed_by_task_id VARCHAR(255)`,
		`ALTER TABLE task_pending_ops ADD COLUMN claim_heartbeat_at DATETIME`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	return db
}

func insertWikiRecoveryKB(t *testing.T, db *gorm.DB, tenantID uint64, kbID string) {
	t.Helper()
	require.NoError(t, db.Exec(
		`INSERT INTO knowledge_bases (id, tenant_id, deleted_at) VALUES (?, ?, NULL)`,
		kbID, tenantID,
	).Error)
}

func insertWikiRecoveryClaim(
	t *testing.T,
	db *gorm.DB,
	tenantID uint64,
	kbID, dedupKey string,
	claimedAt time.Time,
	claimToken, claimedByTaskID string,
) int64 {
	t.Helper()
	payload, err := json.Marshal(service.WikiPendingOp{
		Op: "ingest", KnowledgeID: dedupKey, Attempt: 1,
	})
	require.NoError(t, err)
	result := db.Exec(
		`INSERT INTO task_pending_ops
		 (tenant_id, task_type, scope, scope_id, op, dedup_key, payload,
		  claimed_at, claim_token, claimed_by_task_id, claim_heartbeat_at)
		 VALUES (?, ?, ?, ?, 'ingest', ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?)`,
		tenantID, types.TypeWikiIngest, types.TaskScopeKnowledgeBase, kbID,
		dedupKey, payload, claimedAt, claimToken, claimedByTaskID, claimedAt,
	)
	require.NoError(t, result.Error)
	return result.RowsAffected
}

type wikiRecoveryClaimSnapshot struct {
	ClaimedAt        *time.Time
	ClaimToken       *string
	ClaimedByTaskID  *string
	ClaimHeartbeatAt *time.Time
}

func loadWikiRecoveryClaim(t *testing.T, db *gorm.DB, kbID string) wikiRecoveryClaimSnapshot {
	t.Helper()
	var snapshot wikiRecoveryClaimSnapshot
	require.NoError(t, db.Raw(
		`SELECT claimed_at, claim_token, claimed_by_task_id, claim_heartbeat_at
		 FROM task_pending_ops WHERE scope_id = ?`, kbID,
	).Row().Scan(
		&snapshot.ClaimedAt,
		&snapshot.ClaimToken,
		&snapshot.ClaimedByTaskID,
		&snapshot.ClaimHeartbeatAt,
	))
	return snapshot
}

func TestRecoverPendingWikiTasks_ReleasesTokenOwnedClaimWhenExactTaskIsGone(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-successor")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-successor", "k-successor", stale, "successor-token", "successor-task")

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
	})

	claim := loadWikiRecoveryClaim(t, db, "kb-successor")
	assert.Nil(t, claim.ClaimedAt)
	assert.Nil(t, claim.ClaimToken)
	assert.Nil(t, claim.ClaimedByTaskID)
	assert.Nil(t, claim.ClaimHeartbeatAt)
}

func TestRecoverPendingWikiTasks_KeepsTokenOwnedClaimWhenExactTaskExists(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-successor")
	insertWikiRecoveryClaim(t, db, 7, "kb-successor", "k-successor", time.Now(), "successor-token", "successor-task")
	task := types.RuntimeTaskInfo{
		ID: "successor-task", Queue: types.QueueWiki, Type: types.TypeWikiIngest,
		State: types.RuntimeTaskActive, TenantID: 7, KnowledgeBaseID: "kb-successor",
	}

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
		exactTasks: map[string]*types.RuntimeTaskInfo{
			"successor-task": &task,
		},
	})

	claim := loadWikiRecoveryClaim(t, db, "kb-successor")
	require.NotNil(t, claim.ClaimedAt)
	require.NotNil(t, claim.ClaimToken)
	assert.Equal(t, "successor-token", *claim.ClaimToken)
}

func TestRecoverPendingWikiTasks_ReleasesOnlyStaleTokenlessClaimWithoutRuntimeTask(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-orphaned")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-orphaned", "k-orphaned", stale, "", "legacy-owner")
	inspector := &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
	}

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, inspector)

	claim := loadWikiRecoveryClaim(t, db, "kb-orphaned")
	assert.Nil(t, claim.ClaimedAt)
	assert.Nil(t, claim.ClaimToken)
	assert.Nil(t, claim.ClaimedByTaskID)
	assert.Nil(t, claim.ClaimHeartbeatAt)
	assert.Equal(t, []types.RuntimeTaskState{
		types.RuntimeTaskActive,
		types.RuntimeTaskPending,
		types.RuntimeTaskScheduled,
		types.RuntimeTaskRetry,
	}, inspector.calls)
}

func TestRecoverPendingWikiTasks_KeepsFreshTokenlessClaim(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-fresh")
	insertWikiRecoveryClaim(t, db, 7, "kb-fresh", "k-fresh", time.Now().Add(-time.Minute), "", "")

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
	})

	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-fresh").ClaimedAt)
}

func TestRecoverPendingWikiTasks_KeepsLegacyClaimWhenExactRuntimeTaskExists(t *testing.T) {
	for _, state := range []types.RuntimeTaskState{
		types.RuntimeTaskActive,
		types.RuntimeTaskPending,
		types.RuntimeTaskScheduled,
		types.RuntimeTaskRetry,
	} {
		t.Run(string(state), func(t *testing.T) {
			db := setupWikiRecoveryDB(t)
			insertWikiRecoveryKB(t, db, 7, "kb-live")
			stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
			insertWikiRecoveryClaim(t, db, 7, "kb-live", "k-live", stale, "", "live-task")
			inspector := &wikiRecoveryTaskInspector{
				supported: true,
				tasks: map[types.RuntimeTaskState][]types.RuntimeTaskInfo{
					state: {{
						ID:              "live-task",
						Queue:           types.QueueWiki,
						Type:            types.TypeWikiIngest,
						State:           state,
						TenantID:        7,
						KnowledgeBaseID: "kb-live",
					}},
				},
			}

			recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, inspector)

			assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-live").ClaimedAt)
		})
	}
}

func TestRecoverPendingWikiTasks_DoesNotTreatDifferentTaskIDAsOwnerEvidence(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-reused-scope")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-reused-scope", "k-old", stale, "", "old-task")

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks: map[types.RuntimeTaskState][]types.RuntimeTaskInfo{
			types.RuntimeTaskPending: {{
				ID:              "new-task",
				Queue:           types.QueueWiki,
				Type:            types.TypeWikiIngest,
				State:           types.RuntimeTaskPending,
				TenantID:        7,
				KnowledgeBaseID: "kb-reused-scope",
			}},
		},
	})

	assert.Nil(t, loadWikiRecoveryClaim(t, db, "kb-reused-scope").ClaimedAt)
}

func TestRecoverPendingWikiTasks_ExactProbeClosesPendingToActiveScanWindow(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-scan-window")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-scan-window", "k-window", stale, "", "moving-task")
	active := types.RuntimeTaskInfo{
		ID: "moving-task", Queue: types.QueueWiki, Type: types.TypeWikiIngest,
		State: types.RuntimeTaskActive, TenantID: 7, KnowledgeBaseID: "kb-scan-window",
	}
	inspector := &wikiRecoveryTaskInspector{
		supported:  true,
		tasks:      map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
		exactTasks: map[string]*types.RuntimeTaskInfo{"moving-task": &active},
	}

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, inspector)

	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-scan-window").ClaimedAt)
	assert.Equal(t, []string{"moving-task"}, inspector.exactTaskCalls)
}

func TestRecoverPendingWikiTasks_ExactProbeErrorPreservesEveryClaim(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-exact-first")
	insertWikiRecoveryKB(t, db, 7, "kb-exact-error")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-exact-first", "k-first", stale, "", "missing-task")
	insertWikiRecoveryClaim(t, db, 7, "kb-exact-error", "k-error", stale, "", "error-task")
	inspector := &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
		exactErrors: map[string]error{
			"error-task": errors.New("exact Redis lookup failed"),
		},
	}

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, inspector)

	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-exact-first").ClaimedAt)
	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-exact-error").ClaimedAt)
}

func TestRecoverPendingWikiTasks_ClaimWithoutExactTaskIDFailsClosed(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-no-exact-id")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-no-exact-id", "k-no-id", stale, "", "")

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
	})

	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-no-exact-id").ClaimedAt)
}

func TestRecoverPendingWikiTasks_HoldsLogicalOwnerLeaseFromExactProbeThroughCAS(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-lease")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	payload := `{"op":"ingest","knowledge_id":"knowledge-lease","attempt":3}`
	insertWikiRecoveryClaim(t, db, 7, "kb-lease", "knowledge-lease", stale, "", "missing-task")
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("scope_id = ?", "kb-lease").
		Update("payload", payload).Error)
	exactStarted := make(chan struct{})
	allowExactReturn := make(chan struct{})
	var once sync.Once
	inspector := &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
		exactHooks: map[string]func(){
			"missing-task": func() {
				once.Do(func() { close(exactStarted) })
				<-allowExactReturn
			},
		},
	}
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, inspector)
	}()

	select {
	case <-exactStarted:
	case <-time.After(time.Second):
		t.Fatal("startup recovery never reached exact probe while holding the logical owner lease")
	}
	workerEntered := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_, acquired, err := inspector.WithProcessingOwnerRecoveryLease(
			context.Background(),
			types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "knowledge-lease", Attempt: 3, Name: "postprocess.wiki"},
			types.TaskClaimOwner{Token: "worker-token", TaskID: "worker-task"},
			time.Minute,
			func() error {
				close(workerEntered)
				return nil
			},
		)
		require.NoError(t, err)
		require.True(t, acquired)
	}()
	select {
	case <-workerEntered:
		t.Fatal("worker acquired the logical owner lease before startup CAS released it")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowExactReturn)
	select {
	case <-recoveryDone:
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not finish after exact probe returned")
	}
	select {
	case <-workerEntered:
	case <-time.After(time.Second):
		t.Fatal("worker did not acquire the logical owner lease after startup released it")
	}
	<-workerDone
	assert.Nil(t, loadWikiRecoveryClaim(t, db, "kb-lease").ClaimedAt)
}

func TestRecoverPendingWikiTasks_InspectorErrorPreservesEveryClaim(t *testing.T) {
	db := setupWikiRecoveryDB(t)
	insertWikiRecoveryKB(t, db, 7, "kb-inspection-error")
	stale := time.Now().Add(-(service.WikiIngestTaskTimeout + 30*time.Minute))
	insertWikiRecoveryClaim(t, db, 7, "kb-inspection-error", "k-error", stale, "", "")
	insertWikiRecoveryClaim(t, db, 8, "kb-deleted", "k-deleted", stale, "", "")

	recoverPendingWikiTasksWithInspector(db, &recordingTaskEnqueuer{}, &wikiRecoveryTaskInspector{
		supported: true,
		tasks:     map[types.RuntimeTaskState][]types.RuntimeTaskInfo{},
		errors: map[types.RuntimeTaskState]error{
			types.RuntimeTaskRetry: errors.New("redis unavailable"),
		},
	})

	assert.NotNil(t, loadWikiRecoveryClaim(t, db, "kb-inspection-error").ClaimedAt)
	var count int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&count).Error)
	assert.Equal(t, int64(2), count, "Redis inspection failure must prevent every startup recovery write")
}
