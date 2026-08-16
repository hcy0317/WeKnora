package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type mutableLatestAttemptTracker struct {
	SpanTracker
	mu      sync.RWMutex
	attempt int
	err     error
}

type attemptCommitGuardTestTracker struct {
	SpanTracker
	guardErr   error
	guardCalls atomic.Int32
}

func (t *attemptCommitGuardTestTracker) WithAttemptCommitGuard(
	ctx context.Context, _ string, _ int, fn func(context.Context) error,
) error {
	t.guardCalls.Add(1)
	if t.guardErr != nil {
		return t.guardErr
	}
	return fn(ctx)
}

func (t *mutableLatestAttemptTracker) LatestAttemptStrict(context.Context, string) (int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.attempt, t.err
}

func (t *mutableLatestAttemptTracker) setAttempt(attempt int) {
	t.mu.Lock()
	t.attempt = attempt
	t.mu.Unlock()
}

func TestProcessingOwnerLeaseLostRenewalCancelsOwnerContext(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	oldInterval := processingOwnerLeaseRenewInterval
	processingOwnerLeaseRenewInterval = 5 * time.Millisecond
	t.Cleanup(func() { processingOwnerLeaseRenewInterval = oldInterval })

	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k1", Attempt: 3, Name: "postprocess.wiki"}
	owner := types.TaskClaimOwner{Token: "token-old", TaskID: "task-old"}
	lease, acquired, err := tryAcquireProcessingOwnerLease(context.Background(), client, ref, owner, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, client.Set(context.Background(), processingOwnerLeaseKey(ref),
		`{"token":"token-new","task_id":"task-new"}`, time.Minute).Err())

	select {
	case <-lease.Context().Done():
		require.Error(t, lease.Err())
	case <-time.After(time.Second):
		t.Fatal("lease loss did not cancel the owner context")
	}

	require.Error(t, lease.Release(context.Background()))
	value, err := client.Get(context.Background(), processingOwnerLeaseKey(ref)).Result()
	require.NoError(t, err)
	require.Contains(t, value, "token-new")
}

func TestProcessingWorkerLeaseAcquisitionFailsClosed(t *testing.T) {
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-worker", Attempt: 2, Name: "postprocess.summary"}
	owner := types.TaskClaimOwner{Token: "worker-token", TaskID: "worker-task"}

	lease, err := acquireProcessingWorkerLease(context.Background(), nil, ref, owner)
	require.Error(t, err)
	require.Nil(t, lease)
}

func TestProcessingWorkerLeaseCoordinatesSameLogicalOwner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-worker", Attempt: 2, Name: "postprocess.question.batch[0]"}

	first, err := acquireProcessingWorkerLease(context.Background(), client, ref,
		types.TaskClaimOwner{Token: "first-token", TaskID: "first-task"})
	require.NoError(t, err)
	t.Cleanup(func() { first.Release() })

	second, err := acquireProcessingWorkerLease(context.Background(), client, ref,
		types.TaskClaimOwner{Token: "second-token", TaskID: "second-task"})
	require.Error(t, err)
	require.Nil(t, second)
}

func TestProcessingWorkerLeaseConcurrentAcquisitionHasSingleOwner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-race", Attempt: 2, Name: "postprocess.summary"}

	var winners atomic.Int32
	var leasesMu sync.Mutex
	var leases []*processingWorkerLease
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			lease, err := acquireProcessingWorkerLease(context.Background(), client, ref,
				types.TaskClaimOwner{Token: fmt.Sprintf("token-%d", i), TaskID: fmt.Sprintf("task-%d", i)})
			if err != nil {
				return
			}
			winners.Add(1)
			leasesMu.Lock()
			leases = append(leases, lease)
			leasesMu.Unlock()
		}(i)
	}
	workers.Wait()
	require.Equal(t, int32(1), winners.Load())
	for _, lease := range leases {
		lease.Release()
	}
}

func TestProcessingWorkerLeaseRenewalLossCancelsWorkContext(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	oldInterval := processingOwnerLeaseRenewInterval
	processingOwnerLeaseRenewInterval = 5 * time.Millisecond
	t.Cleanup(func() { processingOwnerLeaseRenewInterval = oldInterval })
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-cancel", Attempt: 2, Name: "postprocess.summary"}

	lease, err := acquireProcessingWorkerLease(context.Background(), client, ref,
		types.TaskClaimOwner{Token: "old-token", TaskID: "old-task"})
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	require.NoError(t, client.Set(context.Background(), processingOwnerLeaseKey(ref),
		`{"token":"new-token","task_id":"new-task"}`, time.Minute).Err())

	select {
	case <-lease.Context().Done():
		require.Error(t, lease.CommitFence())
	case <-time.After(time.Second):
		t.Fatal("lease renewal loss did not cancel worker context")
	}
}

func TestProcessingWorkerLeaseFencesLateCompletion(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-worker", Attempt: 2, Name: "postprocess.graph.chunk[4]"}
	owner := types.TaskClaimOwner{Token: "old-token", TaskID: "old-task"}

	lease, err := acquireProcessingWorkerLease(context.Background(), client, ref, owner)
	require.NoError(t, err)
	t.Cleanup(func() { lease.Release() })
	require.NoError(t, lease.Check(context.Background()))

	require.NoError(t, client.Set(context.Background(), processingOwnerLeaseKey(ref),
		`{"token":"new-token","task_id":"new-task"}`, time.Minute).Err())
	require.Error(t, lease.Check(context.Background()))
}

func TestProcessingWorkerLeaseCommitFenceFailsClosedOnRedisReadError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ref := types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "k-read", Attempt: 2, Name: "postprocess.summary"}

	lease, err := acquireProcessingWorkerLease(context.Background(), client, ref,
		types.TaskClaimOwner{Token: "read-token", TaskID: "read-task"})
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	server.SetError("read failed")

	require.ErrorContains(t, lease.CommitFence(), "check processing worker lease")
	server.SetError("")
}

func TestProcessingWorkerLeaseCommitFenceRejectsSupersededAttemptForEveryRepairOwner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	for _, name := range []string{
		"postprocess.summary",
		"postprocess.question.batch[3]",
		"postprocess.graph.chunk[2]",
	} {
		t.Run(name, func(t *testing.T) {
			tracker := &mutableLatestAttemptTracker{attempt: 4}
			ctx, lease, err := acquireTaskProcessingWorkerLease(context.Background(), client,
				types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "kid-superseded", Attempt: 4, Name: name},
				"fallback-task", tracker)
			require.NoError(t, err)
			require.NotNil(t, ctx)
			t.Cleanup(lease.Release)
			require.NoError(t, lease.CommitFence())

			tracker.setAttempt(5)
			require.ErrorContains(t, lease.CommitFence(), "superseded")
		})
	}
}

func TestProcessingWorkerLeaseCommitWithFenceRunsWriteOnlyInsideBothGuards(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	tracker := &attemptCommitGuardTestTracker{}
	ctx, lease, err := acquireTaskProcessingWorkerLease(context.Background(), client,
		types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "kid-commit", Attempt: 4, Name: "postprocess.summary"},
		"commit-task", tracker)
	require.NoError(t, err)
	t.Cleanup(lease.Release)

	var writes atomic.Int32
	require.NoError(t, lease.CommitWithFence(ctx, func(context.Context) error {
		writes.Add(1)
		return nil
	}))
	require.Equal(t, int32(1), writes.Load())
	require.Equal(t, int32(1), tracker.guardCalls.Load())

	tracker.guardErr = errors.New("superseded by attempt 5")
	require.ErrorContains(t, lease.CommitWithFence(ctx, func(context.Context) error {
		writes.Add(1)
		return nil
	}), "superseded")
	require.Equal(t, int32(1), writes.Load(), "repository guard rejection must skip the write callback")
}

func TestProcessingWorkerLeaseCommitWithFenceRechecksRedisOwnerInsideAttemptGuard(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	tracker := &attemptCommitGuardTestTracker{}
	ref := types.ProcessingOwnerRef{
		TenantID: 7, KnowledgeID: "kid-owner-changed", Attempt: 4, Name: "postprocess.question.batch[2]",
	}
	ctx, lease, err := acquireTaskProcessingWorkerLease(
		context.Background(), client, ref, "owner-task", tracker)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	require.NoError(t, client.Set(context.Background(), processingOwnerLeaseKey(ref),
		`{"token":"replacement","task_id":"replacement-task"}`, time.Minute).Err())

	var writes atomic.Int32
	err = lease.CommitWithFence(ctx, func(context.Context) error {
		writes.Add(1)
		return nil
	})
	require.ErrorContains(t, err, "ownership changed")
	require.Equal(t, int32(1), tracker.guardCalls.Load(), "Redis ownership must be checked after entering the attempt guard")
	require.Zero(t, writes.Load(), "lost Redis ownership must skip the write callback")
}

func TestInspectProcessingOwnerLeaseFailsClosedOnRedisError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	server.Close()
	_, err := inspectProcessingOwnerLease(context.Background(), client, types.ProcessingOwnerRef{
		TenantID: 7, KnowledgeID: "k1", Attempt: 3, Name: "postprocess.wiki",
	})
	require.Error(t, err)
}

func TestWikiOwnerGuardCancelsWhenDurableClaimOwnershipChanges(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}))

	oldInterval := processingOwnerLeaseRenewInterval
	processingOwnerLeaseRenewInterval = 5 * time.Millisecond
	t.Cleanup(func() { processingOwnerLeaseRenewInterval = oldInterval })
	owner := types.TaskClaimOwner{Token: "claim-old", TaskID: "task-old"}
	now := time.Now()
	op := &types.TaskPendingOp{
		TenantID: 7, TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
		ScopeID: "kb-1", Op: WikiOpIngest, DedupKey: "k1", Payload: []byte(`{}`),
		ClaimedAt: &now, ClaimHeartbeatAt: &now,
		ClaimToken: owner.Token, ClaimedByTaskID: owner.TaskID,
	}
	require.NoError(t, db.Create(op).Error)
	svc := &wikiIngestService{pendingRepo: apprepo.NewTaskPendingOpsRepository(db), redisClient: client}
	guard, err := svc.acquireWikiOwnerGuard(context.Background(), 7,
		[]WikiPendingOp{{KnowledgeID: "k1", Attempt: 3}}, []int64{op.ID}, owner)
	require.NoError(t, err)
	t.Cleanup(guard.Release)

	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", op.ID).Updates(map[string]any{
		"claim_token": "claim-new", "claimed_by_task_id": "task-new",
	}).Error)
	select {
	case <-guard.ctx.Done():
		require.Error(t, guard.Err())
	case <-time.After(time.Second):
		t.Fatal("durable claim ownership loss did not cancel Wiki owner guard")
	}
}

func TestWikiOwnerGuardStopClaimRenewalIsConcurrentAndFinal(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}))

	oldInterval := processingOwnerLeaseRenewInterval
	processingOwnerLeaseRenewInterval = 5 * time.Millisecond
	t.Cleanup(func() { processingOwnerLeaseRenewInterval = oldInterval })
	owner := types.TaskClaimOwner{Token: "claim-stop", TaskID: "task-stop"}
	now := time.Now()
	op := &types.TaskPendingOp{
		TenantID: 7, TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
		ScopeID: "kb-stop", Op: WikiOpIngest, DedupKey: "k-stop", Payload: []byte(`{}`),
		ClaimedAt: &now, ClaimHeartbeatAt: &now,
		ClaimToken: owner.Token, ClaimedByTaskID: owner.TaskID,
	}
	require.NoError(t, db.Create(op).Error)
	svc := &wikiIngestService{pendingRepo: apprepo.NewTaskPendingOpsRepository(db), redisClient: client}
	guard, err := svc.acquireWikiOwnerGuard(context.Background(), 7,
		[]WikiPendingOp{{KnowledgeID: "k-stop", Attempt: 1}}, []int64{op.ID}, owner)
	require.NoError(t, err)
	t.Cleanup(guard.Release)

	var stops sync.WaitGroup
	for range 16 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			guard.StopClaimRenewal()
		}()
	}
	stops.Wait()
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", op.ID).Updates(map[string]any{
		"claim_token": "claim-successor", "claimed_by_task_id": "task-successor",
	}).Error)
	time.Sleep(4 * processingOwnerLeaseRenewInterval)
	select {
	case <-guard.ctx.Done():
		t.Fatalf("stopped durable renewal must not race into owner failure: %v", guard.Err())
	default:
	}
}
