package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type retryPreparingTracker struct {
	SpanTracker
	mu                 sync.Mutex
	prepared           *types.KnowledgeSpanRetryPreparation
	preparedList       []*types.KnowledgeSpanRetryPreparation
	candidates         []types.KnowledgeProcessingSpan
	candidateSpanID    string
	multiRequest       types.KnowledgeSpanMultiRetryRequest
	err                error
	failed             bool
	settled            bool
	compensated        bool
	compensationCount  int
	compensateErr      error
	compensationCtxErr error
	exactSpan          *types.KnowledgeProcessingSpan
	exactErr           error
	snapshot           *types.KnowledgeSpanRetryTargetSnapshot
	snapshots          map[string]*types.KnowledgeSpanRetryTargetSnapshot
	inspectErr         error
	inspectErrAfter    int
	inspectCalls       int
	pending            *retryPendingRepo
}

func (t *retryPreparingTracker) InspectSpanRetryTarget(
	_ context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryTargetSnapshot, error) {
	t.mu.Lock()
	t.inspectCalls++
	inspectCalls := t.inspectCalls
	t.mu.Unlock()
	if t.inspectErr != nil && (t.inspectErrAfter == 0 || inspectCalls >= t.inspectErrAfter) {
		return nil, t.inspectErr
	}
	if t.snapshots != nil {
		return t.snapshots[request.SpanID], nil
	}
	if t.snapshot == nil && t.prepared != nil {
		return &types.KnowledgeSpanRetryTargetSnapshot{
			Source: types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
				SpanID: request.SpanID, ParentSpanID: "post", Name: t.prepared.Name,
				Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
			Parent: types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
				SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage},
			LatestRoot: types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
				SpanID: "root", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed},
			LatestOwnerSpanID: request.SpanID,
		}, nil
	}
	return t.snapshot, nil
}

type retryRuntimeInspector struct {
	interfaces.TaskInspector
	task             *types.RuntimeTaskInfo
	supported        bool
	unsupportedAfter int
	calls            int
	err              error
}

type retryClaimPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	interfaces.TaskPendingOpsClaimLease
	claim *types.TaskPendingOpClaimSnapshot
	err   error
}

func (r *retryClaimPendingRepo) InspectClaim(
	_ context.Context, _, _, _, _ string,
) (*types.TaskPendingOpClaimSnapshot, error) {
	return r.claim, r.err
}

func (i *retryRuntimeInspector) GetRuntimeTask(
	_ context.Context, _, _ string,
) (*types.RuntimeTaskInfo, bool, error) {
	i.calls++
	if i.unsupportedAfter > 0 && i.calls >= i.unsupportedAfter {
		return nil, false, nil
	}
	return i.task, i.supported, i.err
}

func stalledRetrySnapshot(status string, updatedAt time.Time) *types.KnowledgeSpanRetryTargetSnapshot {
	return &types.KnowledgeSpanRetryTargetSnapshot{
		Source: types.KnowledgeProcessingSpan{KnowledgeID: "kid", Attempt: 4,
			SpanID: "source", ParentSpanID: "post", Name: "postprocess.summary",
			Kind: types.SpanKindSubSpan, Status: status, UpdatedAt: updatedAt},
		Parent: types.KnowledgeProcessingSpan{KnowledgeID: "kid", Attempt: 4,
			SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage},
		LatestRoot: types.KnowledgeProcessingSpan{KnowledgeID: "kid", Attempt: 4,
			SpanID: "root", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		LatestOwnerSpanID: "source", TenantID: 7, KnowledgeBaseID: "kb",
	}
}

func stalledRetryRedis(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestEvaluateKnowledgeSpanRetryRequiresExactRuntimeAbsence(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	tracker := &retryPreparingTracker{snapshot: stalledRetrySnapshot(types.SpanStatusRunning, stale)}
	for _, tt := range []struct {
		name                  string
		inspector             *retryRuntimeInspector
		wantState, wantReason string
		wantAllowed           bool
	}{
		{name: "pending task is active", inspector: &retryRuntimeInspector{supported: true,
			task: &types.RuntimeTaskInfo{State: types.RuntimeTaskPending}},
			wantState: types.KnowledgeSpanRetryStateActive, wantReason: "runtime_task_live"},
		{name: "inspector error is unknown", inspector: &retryRuntimeInspector{supported: true,
			err: errors.New("redis unavailable")},
			wantState: types.KnowledgeSpanRetryStateUnknown, wantReason: "runtime_inspection_failed"},
		{name: "known absence is stalled once the owner lease is absent", inspector: &retryRuntimeInspector{supported: true},
			wantState: types.KnowledgeSpanRetryStateStalled, wantAllowed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc := &knowledgeService{spanTracker: tracker, taskInspector: tt.inspector,
				redisClient: stalledRetryRedis(t)}
			action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
				types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"})
			if tt.inspector.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantState, action.State)
			require.Equal(t, tt.wantReason, action.Reason)
			require.Equal(t, tt.wantAllowed, action.Allowed)
			if tt.wantAllowed {
				require.NotNil(t, fence)
				require.Equal(t, "knowledge-fanout:kid:4:summary", fence.TaskID)
			}
		})
	}
}

func TestEvaluateKnowledgeSpanRetryAuthorizesStalledQuestionAndGraphOwners(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	for _, target := range []struct {
		name, queue, taskID string
		input               types.JSONMap
	}{
		{name: "postprocess.question.batch[3]", queue: types.QueueQuestion,
			taskID: "knowledge-fanout:kid:4:question:3", input: types.JSONMap{
				"batch_index": 3, "chunk_ids": []string{"chunk-1", "chunk-2"},
				"prev_chunk_id": "chunk-0", "next_chunk_id": "chunk-3", "question_count": 4,
			}},
		{name: "postprocess.graph.chunk[2]", queue: types.QueueGraph,
			taskID: "knowledge-fanout:kid:4:graph:2", input: types.JSONMap{
				"chunk_id": "chunk-2", "chunk_index": 2, "model_id": "graph-model",
			}},
	} {
		t.Run(target.name, func(t *testing.T) {
			snapshot := stalledRetrySnapshot(types.SpanStatusRunning, stale)
			snapshot.Source.Name = target.name
			snapshot.Source.Input = target.input
			svc := &knowledgeService{spanTracker: trackerWithSnapshot(snapshot),
				taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}

			action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
				types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"})

			require.NoError(t, err)
			require.True(t, action.Allowed)
			require.Equal(t, types.KnowledgeSpanRetryStateStalled, action.State)
			require.NotNil(t, fence)
			require.Equal(t, target.queue, fence.Queue)
			require.Equal(t, target.taskID, fence.TaskID)
		})
	}
}

func TestEvaluateKnowledgeSpanRetryFreshHeartbeatCannotBeStalled(t *testing.T) {
	tracker := &retryPreparingTracker{snapshot: stalledRetrySnapshot(types.SpanStatusRunning, time.Now())}
	svc := &knowledgeService{spanTracker: tracker,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}
	action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"})
	require.NoError(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateActive, action.State)
	require.Equal(t, "heartbeat_fresh", action.Reason)
	require.Nil(t, fence)
}

func TestAuthorizeFailedSpanRetryMutationRecheckReadFailureIsServiceUnavailable(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	tracker := &retryPreparingTracker{
		snapshot:        stalledRetrySnapshot(types.SpanStatusRunning, stale),
		inspectErr:      errors.New("database unavailable"),
		inspectErrAfter: 2,
	}
	svc := &knowledgeService{spanTracker: tracker,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}
	fence, lease, err := svc.authorizeFailedSpanRetryMutationWithTopology(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"}, nil, false)

	require.Error(t, err)
	require.Nil(t, fence)
	require.Nil(t, lease)
	appErr, ok := werrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 503, appErr.HTTPCode)
	require.Equal(t, 2, tracker.inspectCalls)
}

func TestAuthorizeFailedSpanRetryMutationRecheckUnknownWithoutErrorIsServiceUnavailable(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	client := stalledRetryRedis(t)
	inspector := &retryRuntimeInspector{supported: true, unsupportedAfter: 2}
	svc := &knowledgeService{
		spanTracker:   trackerWithSnapshot(stalledRetrySnapshot(types.SpanStatusRunning, stale)),
		taskInspector: inspector,
		redisClient:   client,
	}

	fence, lease, err := svc.authorizeFailedSpanRetryMutationWithTopology(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"}, nil, false)

	require.Error(t, err)
	require.Nil(t, fence)
	require.Nil(t, lease)
	appErr, ok := werrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 503, appErr.HTTPCode)
	require.Equal(t, 2, inspector.calls)
	owner, inspectErr := inspectProcessingOwnerLease(context.Background(), client, types.ProcessingOwnerRef{
		TenantID: 7, KnowledgeID: "kid", Attempt: 4, Name: "postprocess.summary",
	})
	require.NoError(t, inspectErr)
	require.False(t, owner.Active, "indeterminate recheck must release the recovery lease")
}

func TestEvaluateKnowledgeSpanRetryRejectsActiveDirectSibling(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	summary := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: "kid", Attempt: 4,
		SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed}
	graph := types.KnowledgeProcessingSpan{ID: 3, KnowledgeID: "kid", Attempt: 4,
		SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
		Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}}
	tracker := &retryPreparingTracker{
		candidates: []types.KnowledgeProcessingSpan{post, summary, graph},
		snapshots: map[string]*types.KnowledgeSpanRetryTargetSnapshot{
			"summary": {Source: summary, Parent: post,
				LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
					Status: types.SpanStatusFailed},
				LatestOwnerSpanID: "summary", TenantID: 7, KnowledgeBaseID: "kb"},
			"graph": {Source: graph, Parent: post,
				LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
					Status: types.SpanStatusRunning},
				LatestOwnerSpanID: "graph", TenantID: 7, KnowledgeBaseID: "kb"},
		},
	}
	svc := &knowledgeService{spanTracker: tracker, redisClient: stalledRetryRedis(t)}
	action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "summary"})

	require.NoError(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateActive, action.State)
	require.Equal(t, "active_sibling", action.Reason)
	require.Nil(t, fence)
}

func TestEvaluateKnowledgeSpanRetryRejectsActiveQuestionBatchSibling(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	question := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: "kid", Attempt: 4,
		SpanID: "question", ParentSpanID: "post", Name: "postprocess.question",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning}
	failed := types.KnowledgeProcessingSpan{ID: 3, KnowledgeID: "kid", Attempt: 4,
		SpanID: "batch-3", ParentSpanID: "question", Name: "postprocess.question.batch[3]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
		Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk-3"}, "question_count": 1}}
	active := types.KnowledgeProcessingSpan{ID: 4, KnowledgeID: "kid", Attempt: 4,
		SpanID: "batch-4", ParentSpanID: "question", Name: "postprocess.question.batch[4]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusPending,
		Input: types.JSONMap{"batch_index": 4, "chunk_ids": []string{"chunk-4"}, "question_count": 1}}
	tracker := &retryPreparingTracker{
		candidates: []types.KnowledgeProcessingSpan{post, question, failed, active},
		snapshots: map[string]*types.KnowledgeSpanRetryTargetSnapshot{
			"batch-3": {Source: failed, Parent: post,
				LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
					Status: types.SpanStatusFailed},
				LatestOwnerSpanID: "batch-3", TenantID: 7, KnowledgeBaseID: "kb"},
			"batch-4": {Source: active, Parent: post,
				LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
					Status: types.SpanStatusRunning},
				LatestOwnerSpanID: "batch-4", TenantID: 7, KnowledgeBaseID: "kb"},
		},
	}
	svc := &knowledgeService{spanTracker: tracker, redisClient: stalledRetryRedis(t)}
	action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "batch-3"})

	require.NoError(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateActive, action.State)
	require.Equal(t, "active_sibling", action.Reason)
	require.Nil(t, fence)
}

func TestEvaluateKnowledgeSpanRetryWikiRequiresStaleExactClaimOwner(t *testing.T) {
	stale := time.Now().Add(-wikiClaimStaleAfter - time.Minute)
	snapshot := stalledRetrySnapshot(types.SpanStatusRunning, stale)
	snapshot.Source.Name = "postprocess.wiki"
	claim := &types.TaskPendingOpClaimSnapshot{Found: true, Consistent: true, RowIDs: []int64{9},
		ClaimToken: "claim-token", ClaimedByTaskID: "wiki-delivery", HeartbeatAt: &stale}
	svc := &knowledgeService{
		spanTracker:     trackerWithSnapshot(snapshot),
		taskInspector:   &retryRuntimeInspector{supported: true},
		taskPendingRepo: &retryClaimPendingRepo{claim: claim},
		redisClient:     stalledRetryRedis(t),
	}
	action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"})
	require.NoError(t, err)
	require.True(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateStalled, action.State)
	require.Equal(t, "wiki-delivery", fence.TaskID)
	require.Equal(t, "claim-token", fence.ClaimToken)
	require.Equal(t, []int64{9}, fence.PendingOpIDs)
}

func TestEvaluateKnowledgeSpanRetryActiveOwnerLeaseFailsClosed(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	snapshot := stalledRetrySnapshot(types.SpanStatusRunning, stale)
	client := stalledRetryRedis(t)
	lease, acquired, err := tryAcquireProcessingOwnerLease(context.Background(), client,
		types.ProcessingOwnerRef{TenantID: 7, KnowledgeID: "kid", Attempt: 4, Name: "postprocess.summary"},
		types.TaskClaimOwner{Token: "worker-token", TaskID: "summary-delivery"}, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	svc := &knowledgeService{spanTracker: trackerWithSnapshot(snapshot), redisClient: client,
		taskInspector: &retryRuntimeInspector{supported: true}}
	action, fence, err := svc.EvaluateKnowledgeSpanRetry(context.Background(),
		types.KnowledgeSpanRetryRequest{KnowledgeID: "kid", Attempt: 4, SpanID: "source"})
	require.NoError(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateActive, action.State)
	require.Equal(t, "owner_lease_active", action.Reason)
	require.Nil(t, fence)
}

func trackerWithSnapshot(snapshot *types.KnowledgeSpanRetryTargetSnapshot) *retryPreparingTracker {
	return &retryPreparingTracker{snapshot: snapshot}
}

func (t *retryPreparingTracker) PrepareFailedSpanRetry(
	_ context.Context, _ types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	return t.prepared, t.err
}

func (t *retryPreparingTracker) PrepareFailedSpanRetries(
	_ context.Context, request types.KnowledgeSpanMultiRetryRequest,
) ([]*types.KnowledgeSpanRetryPreparation, error) {
	t.mu.Lock()
	t.multiRequest = request
	t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	if t.preparedList != nil {
		return t.preparedList, nil
	}
	if t.prepared == nil {
		return nil, nil
	}
	return []*types.KnowledgeSpanRetryPreparation{t.prepared}, nil
}

func (t *retryPreparingTracker) ListFailedSpanRetryCandidates(
	_ context.Context, knowledgeID string, attempt int,
) ([]types.KnowledgeProcessingSpan, error) {
	if t.candidates != nil {
		return append([]types.KnowledgeProcessingSpan(nil), t.candidates...), t.inspectErr
	}
	var source types.KnowledgeProcessingSpan
	switch {
	case t.snapshot != nil:
		source = t.snapshot.Source
	case t.prepared != nil:
		source = types.KnowledgeProcessingSpan{KnowledgeID: knowledgeID, Attempt: attempt,
			SpanID: t.candidateSpanID, ParentSpanID: "post", Name: t.prepared.Name,
			Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: t.prepared.Input}
	default:
		return nil, t.inspectErr
	}
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: knowledgeID, Attempt: attempt,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed}
	if isQuestionBatchRetryOwner(source.Name) {
		question := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: knowledgeID, Attempt: attempt,
			SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
			Kind: types.SpanKindSubSpan, Status: source.Status}
		source.ID = 3
		source.ParentSpanID = question.SpanID
		return []types.KnowledgeProcessingSpan{post, question, source}, t.inspectErr
	}
	source.ID = 2
	source.ParentSpanID = post.SpanID
	return []types.KnowledgeProcessingSpan{post, source}, t.inspectErr
}

func (t *retryPreparingTracker) LookupSpanByName(_ context.Context, knowledgeID string, attempt int, name string) *Span {
	return &Span{KnowledgeID: knowledgeID, Attempt: attempt, SpanID: "target", Name: name, Kind: types.SpanKindSubSpan}
}

func (t *retryPreparingTracker) FailSpan(_ context.Context, _ *Span, _, _ string, _ error) {
	t.failed = true
}

func (t *retryPreparingTracker) SettlePostProcessTree(_ context.Context, _ string, _ int) {
	t.settled = true
}

func (t *retryPreparingTracker) FailPreparedSpanRetry(
	ctx context.Context, prepared *types.KnowledgeSpanRetryPreparation, errorCode, errorMessage string,
) error {
	t.compensated = true
	t.compensationCount++
	t.compensationCtxErr = ctx.Err()
	if t.compensateErr == nil {
		t.exactSpan = &types.KnowledgeProcessingSpan{KnowledgeID: prepared.KnowledgeID,
			Attempt: prepared.Attempt, SpanID: prepared.SpanID, Name: prepared.Name,
			Status: types.SpanStatusFailed, ErrorCode: errorCode, ErrorMessage: errorMessage}
		if t.pending != nil {
			t.pending.rows = nil
		}
	}
	return t.compensateErr
}

func (t *retryPreparingTracker) GetPreparedSpanRetry(
	_ context.Context, knowledgeID string, attempt int, spanID string,
) (*types.KnowledgeProcessingSpan, error) {
	if t.exactErr != nil {
		return nil, t.exactErr
	}
	if t.exactSpan != nil {
		copy := *t.exactSpan
		return &copy, nil
	}
	if t.prepared == nil {
		for _, prepared := range t.preparedList {
			if prepared != nil && prepared.KnowledgeID == knowledgeID && prepared.Attempt == attempt && prepared.SpanID == spanID {
				return &types.KnowledgeProcessingSpan{KnowledgeID: knowledgeID, Attempt: attempt,
					SpanID: spanID, Name: prepared.Name, Status: prepared.Status}, nil
			}
		}
		return nil, nil
	}
	return &types.KnowledgeProcessingSpan{KnowledgeID: knowledgeID, Attempt: attempt,
		SpanID: spanID, Name: t.prepared.Name, Status: t.prepared.Status,
		ErrorCode: t.prepared.ErrorCode, ErrorMessage: t.prepared.ErrorMessage}, nil
}

type retryPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	interfaces.TaskPendingOpsClaimLease
	rows         []*types.TaskPendingOp
	peekErr      error
	deleteErr    error
	deleted      bool
	deleteCtxErr error
	claim        *types.TaskPendingOpClaimSnapshot
	claimErr     error
}

func (r *retryPendingRepo) InspectClaim(
	_ context.Context, _, _, _, _ string,
) (*types.TaskPendingOpClaimSnapshot, error) {
	return r.claim, r.claimErr
}

func (r *retryPendingRepo) PeekBatch(
	ctx context.Context, _, _, _ string, _ int,
) ([]*types.TaskPendingOp, error) {
	if r.peekErr != nil {
		return nil, r.peekErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.rows, nil
}

func (r *retryPendingRepo) DeleteByDedupKey(
	ctx context.Context, _, _, _, dedupKey, _ string,
) error {
	r.deleteCtxErr = ctx.Err()
	if r.deleteErr != nil {
		return r.deleteErr
	}
	r.deleted = true
	remaining := r.rows[:0]
	for _, row := range r.rows {
		if row == nil || row.DedupKey != dedupKey {
			remaining = append(remaining, row)
		}
	}
	r.rows = remaining
	return nil
}

func retryOutbox(t *testing.T, prepared *types.KnowledgeSpanRetryPreparation) *types.TaskPendingOp {
	t.Helper()
	payload, err := json.Marshal(types.KnowledgeSpanRetryOutboxPayload{
		TaskID: prepared.TaskID, KnowledgeID: prepared.KnowledgeID, Attempt: prepared.Attempt,
		SpanID: prepared.SpanID, TargetName: prepared.Name, TenantID: prepared.TenantID,
		KnowledgeBaseID: prepared.KnowledgeBaseID, Language: prepared.Language, Input: prepared.Input,
	})
	require.NoError(t, err)
	return &types.TaskPendingOp{TaskType: types.KnowledgeSpanRetryOutboxTaskType,
		Scope: types.KnowledgeSpanRetryOutboxScope, ScopeID: prepared.KnowledgeID,
		Op: types.KnowledgeSpanRetryOutboxOp, DedupKey: prepared.TaskID, Payload: payload}
}

type retryCaptureEnqueuer struct {
	tasks   []*asynq.Task
	taskIDs []string
}

type blockingRetryCaptureEnqueuer struct {
	retryCaptureEnqueuer
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (q *blockingRetryCaptureEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	q.once.Do(func() {
		close(q.entered)
		<-q.release
	})
	return q.retryCaptureEnqueuer.Enqueue(task, opts...)
}

func (q *retryCaptureEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	q.tasks = append(q.tasks, task)
	for _, option := range opts {
		if option.Type() == asynq.TaskIDOpt {
			q.taskIDs = append(q.taskIDs, option.Value().(string))
		}
	}
	return &asynq.TaskInfo{ID: "queued", Queue: "summary"}, nil
}

type repositoryBackedQuestionRetryFixture struct {
	db       *gorm.DB
	svc      *knowledgeService
	queue    *retryCaptureEnqueuer
	request  types.KnowledgeSpanRetryRequest
	sourceID string
}

func setupRepositoryBackedQuestionRetry(
	t *testing.T, knowledgeID, sourceStatus string,
) *repositoryBackedQuestionRetryFixture {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(
		&types.Knowledge{}, &types.KnowledgeProcessingSpan{}, &types.TaskPendingOp{},
	))
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX retry_question_span_identity
		ON knowledge_processing_spans (knowledge_id, attempt, span_id);
		CREATE UNIQUE INDEX retry_question_root_attempt
		ON knowledge_processing_spans (knowledge_id, attempt) WHERE kind = 'root';
		CREATE TABLE retry_question_outbox_audit (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			dedup_key TEXT NOT NULL,
			payload BLOB NOT NULL
		);
		CREATE TRIGGER audit_question_retry_outbox_insert AFTER INSERT ON task_pending_ops
		WHEN NEW.task_type = 'knowledge:span_retry_dispatch'
		BEGIN
			INSERT INTO retry_question_outbox_audit (dedup_key, payload)
			VALUES (NEW.dedup_key, NEW.payload);
		END;`).Error)

	rootStatus := types.SpanStatusFailed
	parentStatus := types.SpanStatusFailed
	parseStatus := types.ParseStatusFailed
	if sourceStatus == types.SpanStatusPending || sourceStatus == types.SpanStatusRunning {
		rootStatus = types.SpanStatusRunning
		parentStatus = types.SpanStatusRunning
		parseStatus = types.ParseStatusFinalizing
	}
	require.NoError(t, db.Create(&types.Knowledge{
		ID: knowledgeID, TenantID: 7, KnowledgeBaseID: "kb-" + knowledgeID,
		ParseStatus: parseStatus,
	}).Error)

	spanRepo := repository.NewKnowledgeSpanRepository(db)
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute).UTC().Truncate(time.Millisecond)
	finished := stale.Add(time.Second)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root-old", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: rootStatus, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old",
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: parentStatus,
			Input:     types.JSONMap{"expected_branches": []string{"postprocess.question"}, "fanout_complete": true},
			StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-group-superseded", ParentSpanID: "post-old",
			Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			StartedAt: &stale, FinishedAt: &finished, UpdatedAt: finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-superseded-source", ParentSpanID: "question-group-superseded",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			Input:     types.JSONMap{"batch_index": 3, "chunk_ids": []string{"obsolete-chunk"}, "question_count": 4},
			StartedAt: &stale, FinishedAt: &finished, UpdatedAt: finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-group-current", ParentSpanID: "post-old",
			Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: parentStatus,
			Input: types.JSONMap{"batch_count": 4}, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-2-done", ParentSpanID: "question-group-current",
			Name: "postprocess.question.batch[2]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone,
			Input:     types.JSONMap{"batch_index": 2, "chunk_ids": []string{"chunk-21"}, "question_count": 4},
			StartedAt: &stale, FinishedAt: &finished, UpdatedAt: finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-3-source", ParentSpanID: "question-group-current",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: sourceStatus,
			Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk-31", "chunk-32"},
				"prev_chunk_id": "chunk-30", "next_chunk_id": "chunk-33", "question_count": 4,
				"language": "zh-CN"},
			StartedAt: &stale, UpdatedAt: stale},
	}
	for _, row := range rows {
		if row.FinishedAt == nil && row.Status != types.SpanStatusPending && row.Status != types.SpanStatusRunning {
			row.FinishedAt = &finished
			row.UpdatedAt = finished
		}
		require.NoError(t, spanRepo.Upsert(context.Background(), row))
	}

	queue := &retryCaptureEnqueuer{}
	pending := repository.NewTaskPendingOpsRepository(db)
	return &repositoryBackedQuestionRetryFixture{
		db: db, queue: queue, sourceID: "question-batch-3-source",
		svc: &knowledgeService{spanTracker: NewSpanTracker(spanRepo, nil), task: queue, taskPendingRepo: pending},
		request: types.KnowledgeSpanRetryRequest{
			KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-3-source",
			ClientRequestID: "retry-" + knowledgeID, Language: "zh-CN",
		},
	}
}

func assertExactRepositoryBackedQuestionRetry(
	t *testing.T, fixture *repositoryBackedQuestionRetryFixture,
	prepared *types.KnowledgeSpanRetryPreparation,
) {
	t.Helper()
	require.Equal(t, "postprocess.question.batch[3]", prepared.Name)
	require.Equal(t, 5, prepared.Attempt)
	require.Equal(t, "knowledge-fanout:"+fixture.request.KnowledgeID+":5:question:3", prepared.TaskID)
	require.Len(t, fixture.queue.tasks, 1)
	require.Equal(t, types.TypeQuestionGeneration, fixture.queue.tasks[0].Type())
	require.Equal(t, []string{prepared.TaskID}, fixture.queue.taskIDs)
	var taskPayload types.QuestionGenerationPayload
	require.NoError(t, json.Unmarshal(fixture.queue.tasks[0].Payload(), &taskPayload))
	require.Equal(t, 3, taskPayload.BatchIndex)
	require.Equal(t, []string{"chunk-31", "chunk-32"}, taskPayload.ChunkIDs)

	var retryRows []types.KnowledgeProcessingSpan
	require.NoError(t, fixture.db.Where("knowledge_id = ? AND attempt = ?",
		fixture.request.KnowledgeID, prepared.Attempt).Order("id ASC").Find(&retryRows).Error)
	questionTargets := make([]types.KnowledgeProcessingSpan, 0)
	var post types.KnowledgeProcessingSpan
	for _, row := range retryRows {
		if row.Name == types.StagePostProcess {
			post = row
		}
		if isQuestionBatchRetryOwner(row.Name) {
			questionTargets = append(questionTargets, row)
		}
	}
	require.Len(t, questionTargets, 1, "the repair attempt must not replay the whole question group")
	require.Equal(t, "postprocess.question.batch[3]", questionTargets[0].Name)
	require.Equal(t, prepared.SpanID, questionTargets[0].SpanID)
	require.Equal(t, []any{"postprocess.question"}, post.Input["expected_branches"])
	require.Equal(t, []any{"postprocess.question.batch[3]"}, post.Input["expected_question_children"])
	require.EqualValues(t, 1, post.Input["expected_subtasks_count"])

	var audit struct {
		DedupKey string
		Payload  []byte
	}
	require.NoError(t, fixture.db.Table("retry_question_outbox_audit").Take(&audit).Error)
	require.Equal(t, prepared.TaskID, audit.DedupKey)
	var outbox types.KnowledgeSpanRetryOutboxPayload
	require.NoError(t, json.Unmarshal(audit.Payload, &outbox))
	require.Equal(t, prepared.TaskID, outbox.TaskID)
	require.Equal(t, prepared.SpanID, outbox.SpanID)
	require.Equal(t, "postprocess.question.batch[3]", outbox.TargetName)
	require.EqualValues(t, 3, outbox.Input["batch_index"])
	var remainingOutboxes int64
	require.NoError(t, fixture.db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType,
			fixture.request.KnowledgeID).Count(&remainingOutboxes).Error)
	require.Zero(t, remainingOutboxes, "successful exact dispatch must acknowledge its durable outbox")
}

type retryFailingEnqueuer struct{}

func (retryFailingEnqueuer) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	return nil, errors.New("queue unavailable")
}

type retryBlockingFailEnqueuer struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	calls   int
}

func (q *retryBlockingFailEnqueuer) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.calls++
	q.once.Do(func() { close(q.started) })
	<-q.release
	return nil, errors.New("queue unavailable")
}

func TestRetryFailedKnowledgeSpanPublishesSummaryForPreparedAttempt(t *testing.T) {
	queue := &retryCaptureEnqueuer{}
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", Attempt: 5, SpanID: "new-span", Name: "postprocess.summary",
		TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending, DispatchRequired: true,
		TenantID: 7, KnowledgeBaseID: "kb", Language: "zh-CN",
	}
	tracker := &retryPreparingTracker{prepared: prepared, candidateSpanID: "old"}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "old", ClientRequestID: "request-1",
	})
	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Len(t, queue.tasks, 1)
	require.Equal(t, types.TypeSummaryGeneration, queue.tasks[0].Type())
	var payload types.SummaryGenerationPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.Equal(t, 5, payload.Attempt)
	require.Equal(t, "kid", payload.KnowledgeID)
	require.Equal(t, "knowledge-fanout:kid:5:summary", got.TaskID)
	require.Equal(t, []string{got.TaskID}, queue.taskIDs)
	require.True(t, pending.deleted)
}

func TestRetryFailedKnowledgeSpanRepositoryBackedReplayIsCanonical(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&types.Knowledge{}, &types.KnowledgeProcessingSpan{}, &types.TaskPendingOp{},
	))
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX retry_span_identity
		ON knowledge_processing_spans (knowledge_id, attempt, span_id);
		CREATE UNIQUE INDEX retry_root_attempt
		ON knowledge_processing_spans (knowledge_id, attempt) WHERE kind = 'root';`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE retry_outbox_insert_audit (id INTEGER PRIMARY KEY AUTOINCREMENT);
		CREATE TRIGGER audit_retry_outbox_insert AFTER INSERT ON task_pending_ops
		WHEN NEW.task_type = 'knowledge:span_retry_dispatch'
		BEGIN INSERT INTO retry_outbox_insert_audit (id) VALUES (NULL); END;`).Error)

	knowledge := &types.Knowledge{
		ID: "kid-service-replay", TenantID: 7, KnowledgeBaseID: "kb-service-replay",
		ParseStatus: types.ParseStatusFailed, SummaryStatus: types.SummaryStatusFailed,
	}
	require.NoError(t, db.Create(knowledge).Error)
	spanRepo := repository.NewKnowledgeSpanRepository(db)
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledge.ID, Attempt: 4, SpanID: "root-old", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledge.ID, Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old",
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledge.ID, Attempt: 4, SpanID: "summary-old", ParentSpanID: "post-old",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
	} {
		require.NoError(t, spanRepo.Upsert(context.Background(), row))
	}

	queue := &retryCaptureEnqueuer{}
	pending := repository.NewTaskPendingOpsRepository(db)
	svc := &knowledgeService{
		spanTracker: NewSpanTracker(spanRepo, nil), task: queue, taskPendingRepo: pending,
	}
	request := types.KnowledgeSpanRetryRequest{
		KnowledgeID: knowledge.ID, Attempt: 4, SpanID: "summary-old",
		ClientRequestID: "same-service-request", Language: "zh-CN",
	}
	first, err := svc.RetryFailedKnowledgeSpan(context.Background(), request)
	require.NoError(t, err)
	second, err := svc.RetryFailedKnowledgeSpan(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, first.Attempt, second.Attempt)
	require.Equal(t, first.SpanID, second.SpanID)
	require.Equal(t, first.TaskID, second.TaskID)
	require.Len(t, queue.tasks, 1, "idempotent replay must not publish a second task")

	var roots, targets, currentOutboxes, insertedOutboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND kind = ?", knowledge.ID, first.Attempt, types.SpanKindRoot).
		Count(&roots).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", knowledge.ID, first.Attempt, first.SpanID).
		Count(&targets).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, knowledge.ID).
		Count(&currentOutboxes).Error)
	require.NoError(t, db.Table("retry_outbox_insert_audit").Count(&insertedOutboxes).Error)
	require.EqualValues(t, 1, roots)
	require.EqualValues(t, 1, targets)
	require.Zero(t, currentOutboxes, "successful publication must acknowledge the sole durable outbox")
	require.EqualValues(t, 1, insertedOutboxes, "exact replay must create only one durable outbox")
}

type repositoryBackedMultiRetryFixture struct {
	db      *gorm.DB
	svc     *knowledgeService
	queue   *retryCaptureEnqueuer
	request types.KnowledgeSpanAggregateRetryRequest
}

func setupRepositoryBackedMultiRetry(t *testing.T, knowledgeID, clientRequestID string) *repositoryBackedMultiRetryFixture {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&types.Knowledge{}, &types.KnowledgeProcessingSpan{}, &types.TaskPendingOp{},
	))
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX retry_multi_span_identity
		ON knowledge_processing_spans (knowledge_id, attempt, span_id);
		CREATE UNIQUE INDEX retry_multi_root_attempt
		ON knowledge_processing_spans (knowledge_id, attempt) WHERE kind = 'root';
		CREATE TABLE retry_multi_outbox_audit (id INTEGER PRIMARY KEY AUTOINCREMENT);
		CREATE TRIGGER audit_multi_retry_outbox_insert AFTER INSERT ON task_pending_ops
		WHEN NEW.task_type = 'knowledge:span_retry_dispatch'
		BEGIN INSERT INTO retry_multi_outbox_audit (id) VALUES (NULL); END;`).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: knowledgeID, TenantID: 7,
		KnowledgeBaseID: "kb-" + knowledgeID, ParseStatus: types.ParseStatusFailed,
		SummaryStatus: types.SummaryStatusFailed}).Error)
	spanRepo := repository.NewKnowledgeSpanRepository(db)
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root-old", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old",
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "summary-old", ParentSpanID: "post-old",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "wiki-old", ParentSpanID: "post-old",
			Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
	} {
		require.NoError(t, spanRepo.Upsert(context.Background(), row))
	}
	queue := &retryCaptureEnqueuer{}
	pending := repository.NewTaskPendingOpsRepository(db)
	return &repositoryBackedMultiRetryFixture{db: db, queue: queue,
		svc: &knowledgeService{spanTracker: NewSpanTracker(spanRepo, nil), task: queue, taskPendingRepo: pending},
		request: types.KnowledgeSpanAggregateRetryRequest{KnowledgeID: knowledgeID, Attempt: 4,
			ClientRequestID: clientRequestID, Language: "zh-CN"}}
}

func TestRetryFailedKnowledgeSpansRepositoryBackedReplayIsCanonical(t *testing.T) {
	fixture := setupRepositoryBackedMultiRetry(t, "kid-aggregate-replay", "same-aggregate-request")
	first, err := fixture.svc.RetryFailedKnowledgeSpans(context.Background(), fixture.request)
	require.NoError(t, err)
	second, err := fixture.svc.RetryFailedKnowledgeSpans(context.Background(), fixture.request)
	require.NoError(t, err)
	require.Equal(t, first.Attempt, second.Attempt)
	require.Equal(t, first.Targets, second.Targets)
	require.Len(t, first.Targets, 2)
	require.Len(t, fixture.queue.tasks, 2, "aggregate replay must not republish either worker")
	var currentOutboxes, insertedOutboxes, roots int64
	require.NoError(t, fixture.db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType,
			fixture.request.KnowledgeID).Count(&currentOutboxes).Error)
	require.NoError(t, fixture.db.Table("retry_multi_outbox_audit").Count(&insertedOutboxes).Error)
	require.NoError(t, fixture.db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND kind = ?", fixture.request.KnowledgeID,
			first.Attempt, types.SpanKindRoot).Count(&roots).Error)
	require.Zero(t, currentOutboxes)
	require.EqualValues(t, 2, insertedOutboxes)
	require.EqualValues(t, 1, roots)
}

func TestRetryFailedKnowledgeSpanRequestKindCannotCrossEndpoints(t *testing.T) {
	t.Run("row_then_aggregate", func(t *testing.T) {
		fixture := setupRepositoryBackedMultiRetry(t, "kid-row-then-aggregate", "shared-request-key")
		rowRequest := types.KnowledgeSpanRetryRequest{KnowledgeID: fixture.request.KnowledgeID,
			Attempt: 4, SpanID: "summary-old", ClientRequestID: fixture.request.ClientRequestID, Language: "zh-CN"}
		_, err := fixture.svc.RetryFailedKnowledgeSpan(context.Background(), rowRequest)
		require.NoError(t, err)
		_, err = fixture.svc.RetryFailedKnowledgeSpans(context.Background(), fixture.request)
		require.Error(t, err)
		appErr, ok := werrors.IsAppError(err)
		require.True(t, ok)
		require.Equal(t, 409, appErr.HTTPCode)
		require.Len(t, fixture.queue.tasks, 1)
		var inserted int64
		require.NoError(t, fixture.db.Table("retry_multi_outbox_audit").Count(&inserted).Error)
		require.EqualValues(t, 1, inserted)
	})

	t.Run("aggregate_then_row", func(t *testing.T) {
		fixture := setupRepositoryBackedMultiRetry(t, "kid-aggregate-then-row", "shared-request-key")
		_, err := fixture.svc.RetryFailedKnowledgeSpans(context.Background(), fixture.request)
		require.NoError(t, err)
		rowRequest := types.KnowledgeSpanRetryRequest{KnowledgeID: fixture.request.KnowledgeID,
			Attempt: 4, SpanID: "summary-old", ClientRequestID: fixture.request.ClientRequestID, Language: "zh-CN"}
		_, err = fixture.svc.RetryFailedKnowledgeSpan(context.Background(), rowRequest)
		require.Error(t, err)
		appErr, ok := werrors.IsAppError(err)
		require.True(t, ok)
		require.Equal(t, 409, appErr.HTTPCode)
		require.Len(t, fixture.queue.tasks, 2)
		var inserted int64
		require.NoError(t, fixture.db.Table("retry_multi_outbox_audit").Count(&inserted).Error)
		require.EqualValues(t, 2, inserted)
	})
}

func TestRetryFailedKnowledgeSpanRepositoryBackedFailedQuestionBatchIsExact(t *testing.T) {
	fixture := setupRepositoryBackedQuestionRetry(t, "kid-question-failed-real", types.SpanStatusFailed)

	action, fence, err := fixture.svc.EvaluateKnowledgeSpanRetry(context.Background(), fixture.request)
	require.NoError(t, err)
	require.True(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateFailed, action.State)
	require.Equal(t, "postprocess.question.batch[3]", action.Target)
	require.Nil(t, fence)

	prepared, err := fixture.svc.RetryFailedKnowledgeSpan(context.Background(), fixture.request)
	require.NoError(t, err)
	assertExactRepositoryBackedQuestionRetry(t, fixture, prepared)
}

func TestRetryFailedKnowledgeSpanRepositoryBackedStalledQuestionBatchIsExact(t *testing.T) {
	fixture := setupRepositoryBackedQuestionRetry(t, "kid-question-stalled-real", types.SpanStatusRunning)
	fixture.svc.redisClient = stalledRetryRedis(t)
	fixture.svc.taskInspector = &retryRuntimeInspector{supported: true}

	action, fence, err := fixture.svc.EvaluateKnowledgeSpanRetry(context.Background(), fixture.request)
	require.NoError(t, err)
	require.True(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateStalled, action.State)
	require.Equal(t, "postprocess.question.batch[3]", action.Target)
	require.NotNil(t, fence)
	require.Equal(t, types.QueueQuestion, fence.Queue)
	require.Equal(t, "knowledge-fanout:"+fixture.request.KnowledgeID+":4:question:3", fence.TaskID)

	prepared, err := fixture.svc.RetryFailedKnowledgeSpan(context.Background(), fixture.request)
	require.NoError(t, err)
	assertExactRepositoryBackedQuestionRetry(t, fixture, prepared)
}

func TestEvaluateKnowledgeSpanRetryRepositoryBackedQuestionBatchRejectsSupersededGroup(t *testing.T) {
	fixture := setupRepositoryBackedQuestionRetry(t, "kid-question-superseded-real", types.SpanStatusFailed)
	request := fixture.request
	request.SpanID = "question-batch-superseded-source"

	action, fence, err := fixture.svc.EvaluateKnowledgeSpanRetry(context.Background(), request)

	require.Error(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateUnknown, action.State)
	require.Equal(t, "liveness_read_failed", action.Reason)
	require.Nil(t, fence)
	var rootAttempts int64
	require.NoError(t, fixture.db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND kind = ?", request.KnowledgeID, types.SpanKindRoot).
		Count(&rootAttempts).Error)
	require.EqualValues(t, 1, rootAttempts, "rejected preflight must not seed a repair attempt")
}

func TestRetryFailedKnowledgeSpanPublishesExactQuestionBatch(t *testing.T) {
	queue := &retryCaptureEnqueuer{}
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid-question", Attempt: 5, SpanID: "new-question-span",
		Name: "postprocess.question.batch[3]", TaskID: "knowledge-fanout:kid-question:5:question:3",
		Status: types.SpanStatusPending, DispatchRequired: true,
		TenantID: 7, KnowledgeBaseID: "kb", Language: "zh-CN",
		Input: types.JSONMap{
			"batch_index": 3, "chunk_ids": []string{"chunk-31", "chunk-32"},
			"prev_chunk_id": "chunk-30", "next_chunk_id": "chunk-33", "question_count": 4,
		},
	}
	tracker := &retryPreparingTracker{prepared: prepared, candidateSpanID: "old-question"}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-question", Attempt: 4, SpanID: "old-question", ClientRequestID: "retry-question-3",
	})

	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Len(t, queue.tasks, 1)
	require.Equal(t, types.TypeQuestionGeneration, queue.tasks[0].Type())
	require.Equal(t, []string{prepared.TaskID}, queue.taskIDs)
	var payload types.QuestionGenerationPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.Equal(t, prepared.Attempt, payload.Attempt)
	require.Equal(t, 3, payload.BatchIndex)
	require.Equal(t, []string{"chunk-31", "chunk-32"}, payload.ChunkIDs)
	require.Equal(t, "chunk-30", payload.PrevChunkID)
	require.Equal(t, "chunk-33", payload.NextChunkID)
	require.Equal(t, 4, payload.QuestionCount)
	require.Equal(t, "zh-CN", payload.Language)
	require.True(t, pending.deleted)
}

func TestRetryFailedKnowledgeSpanRechecksStalledWikiUnderRecoveryLease(t *testing.T) {
	stale := time.Now().Add(-wikiClaimStaleAfter - time.Minute)
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", Attempt: 5, SpanID: "new-span", Name: "postprocess.wiki",
		TaskID: "knowledge-fanout:kid:5:wiki", Status: types.SpanStatusPending, DispatchRequired: true,
		TenantID: 7, KnowledgeBaseID: "kb",
	}
	snapshot := stalledRetrySnapshot(types.SpanStatusRunning, stale)
	snapshot.Source.Name = "postprocess.wiki"
	tracker := &retryPreparingTracker{prepared: prepared, snapshot: snapshot}
	pending := &retryPendingRepo{
		rows: []*types.TaskPendingOp{retryOutbox(t, prepared)},
		claim: &types.TaskPendingOpClaimSnapshot{Found: true, Consistent: true, RowIDs: []int64{9},
			ClaimToken: "old-owner", ClaimedByTaskID: "wiki-delivery", HeartbeatAt: &stale},
	}
	queue := &retryCaptureEnqueuer{}
	client := stalledRetryRedis(t)
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: client}

	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "source", ClientRequestID: "retry-stalled-wiki",
	})
	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Len(t, queue.tasks, 1)
	owner, err := inspectProcessingOwnerLease(context.Background(), client, types.ProcessingOwnerRef{
		TenantID: 7, KnowledgeID: "kid", Attempt: 4, Name: "postprocess.wiki",
	})
	require.NoError(t, err)
	require.False(t, owner.Active, "recovery owner lease must be released after dispatch")
}

func TestRetryFailedKnowledgeSpanRechecksStalledNonWikiOwnerUnderRecoveryLease(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	for _, target := range []struct {
		knowledgeID, name, taskID, taskType string
		input                               types.JSONMap
	}{
		{knowledgeID: "kid-stalled-summary", name: "postprocess.summary",
			taskID: "knowledge-fanout:kid-stalled-summary:5:summary", taskType: types.TypeSummaryGeneration},
		{knowledgeID: "kid-stalled-question", name: "postprocess.question.batch[3]",
			taskID: "knowledge-fanout:kid-stalled-question:5:question:3", taskType: types.TypeQuestionGeneration,
			input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk-31"},
				"prev_chunk_id": "chunk-30", "next_chunk_id": "chunk-32", "question_count": 4,
				"language": "zh-CN"}},
		{knowledgeID: "kid-stalled-graph", name: "postprocess.graph.chunk[2]",
			taskID: "knowledge-fanout:kid-stalled-graph:5:graph:2", taskType: types.TypeChunkExtract,
			input: types.JSONMap{"chunk_id": "chunk-2", "chunk_index": 2, "model_id": "graph-model"}},
	} {
		t.Run(target.name, func(t *testing.T) {
			prepared := &types.KnowledgeSpanRetryPreparation{
				KnowledgeID: target.knowledgeID, Attempt: 5, SpanID: "new-span", Name: target.name,
				TaskID: target.taskID, Status: types.SpanStatusPending, DispatchRequired: true,
				TenantID: 7, KnowledgeBaseID: "kb", Language: "zh-CN", Input: target.input,
			}
			snapshot := stalledRetrySnapshot(types.SpanStatusRunning, stale)
			snapshot.Source.KnowledgeID = target.knowledgeID
			snapshot.Source.Name = target.name
			snapshot.Source.Input = target.input
			snapshot.Parent.KnowledgeID = target.knowledgeID
			snapshot.LatestRoot.KnowledgeID = target.knowledgeID
			tracker := &retryPreparingTracker{prepared: prepared, snapshot: snapshot}
			pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
			queue := &retryCaptureEnqueuer{}
			client := stalledRetryRedis(t)
			svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending,
				taskInspector: &retryRuntimeInspector{supported: true}, redisClient: client}

			got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
				KnowledgeID: target.knowledgeID, Attempt: 4, SpanID: "source",
				ClientRequestID: "retry-stalled-owner", Language: "zh-CN",
			})

			require.NoError(t, err)
			require.Equal(t, prepared, got)
			require.Len(t, queue.tasks, 1)
			require.Equal(t, target.taskType, queue.tasks[0].Type())
			owner, err := inspectProcessingOwnerLease(context.Background(), client, types.ProcessingOwnerRef{
				TenantID: 7, KnowledgeID: target.knowledgeID, Attempt: 4, Name: target.name,
			})
			require.NoError(t, err)
			require.False(t, owner.Active, "recovery owner lease must be released after dispatch")
		})
	}
}

func TestRetryFailedKnowledgeSpanGraphTaskIDMatchesPublishedTask(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	queue := &retryCaptureEnqueuer{}
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", Attempt: 5, SpanID: "new-span", Name: "postprocess.graph.chunk[3]",
		TaskID: "knowledge-fanout:kid:5:graph:3", Status: types.SpanStatusPending, DispatchRequired: true,
		TenantID: 7, KnowledgeBaseID: "kb",
		Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model-1"},
	}
	tracker := &retryPreparingTracker{prepared: prepared, candidateSpanID: "old"}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "old", ClientRequestID: "request-graph",
	})
	require.NoError(t, err)
	require.Equal(t, "knowledge-fanout:kid:5:graph:3", got.TaskID)
	require.Equal(t, []string{got.TaskID}, queue.taskIDs)
	require.Len(t, queue.tasks, 1)
	require.Equal(t, types.TypeChunkExtract, queue.tasks[0].Type())
}

func TestRetryFailedKnowledgeSpanMapsUnknownOwnerToBadRequest(t *testing.T) {
	snapshot := stalledRetrySnapshot(types.SpanStatusFailed, time.Now())
	snapshot.Source.Name = "postprocess.unknown"
	snapshot.Source.SpanID = "old"
	snapshot.LatestRoot.Status = types.SpanStatusFailed
	svc := &knowledgeService{spanTracker: &retryPreparingTracker{
		snapshot: snapshot, err: repository.ErrKnowledgeSpanRetryUnsupported,
	}}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "old", ClientRequestID: "request-1",
	})
	require.Error(t, err)
	appErr, ok := err.(interface{ Error() string })
	require.True(t, ok)
	require.Contains(t, appErr.Error(), "cannot be retried independently")
}

func TestRetryFailedKnowledgeSpanPreparationFailureMapsToServiceUnavailable(t *testing.T) {
	tracker := &retryPreparingTracker{prepared: &types.KnowledgeSpanRetryPreparation{
		Name: "postprocess.summary",
	}, candidateSpanID: "source", err: errors.New("database unavailable")}
	svc := &knowledgeService{spanTracker: tracker}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "source", ClientRequestID: "request",
	})
	require.Error(t, err)
	appErr, ok := werrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 503, appErr.HTTPCode)
	require.NotContains(t, appErr.Message, "database unavailable")
}

func TestRetryFailedKnowledgeSpanEnqueueFailureTerminatesRepairAttempt(t *testing.T) {
	tracker := &retryPreparingTracker{candidateSpanID: "old", prepared: &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", Attempt: 5, SpanID: "new-span", Name: "postprocess.summary",
		TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending, DispatchRequired: true,
		TenantID: 7, KnowledgeBaseID: "kb",
	}}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, tracker.prepared)}}
	svc := &knowledgeService{spanTracker: tracker, task: retryFailingEnqueuer{}, taskPendingRepo: pending}

	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "old", ClientRequestID: "request-1",
	})
	require.Error(t, err)
	require.True(t, tracker.compensated)
	require.NoError(t, tracker.compensationCtxErr)
}

func TestRetryFailedKnowledgeSpanNilPreparationFailsClosed(t *testing.T) {
	svc := &knowledgeService{spanTracker: &retryPreparingTracker{}}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestRetryFailedKnowledgeSpanCanceledRequestUsesDetachedCompensation(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: true, TenantID: 7, KnowledgeBaseID: "kb"}
	tracker := &retryPreparingTracker{prepared: prepared}
	pending := &retryPendingRepo{peekErr: context.Canceled}
	svc := &knowledgeService{spanTracker: tracker, task: &retryCaptureEnqueuer{}, taskPendingRepo: pending}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.RetryFailedKnowledgeSpan(ctx, types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.True(t, tracker.compensated)
	require.NoError(t, tracker.compensationCtxErr)
}

func TestRetryFailedKnowledgeSpanCompensationFailureIsExplicit(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: true, TenantID: 7, KnowledgeBaseID: "kb"}
	tracker := &retryPreparingTracker{prepared: prepared, compensateErr: errors.New("rollback unavailable")}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	svc := &knowledgeService{spanTracker: tracker, task: retryFailingEnqueuer{}, taskPendingRepo: pending}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rollback unavailable")
	require.False(t, pending.deleted)
}

func TestRetryFailedKnowledgeSpanPublishedButAckFailureLeavesRecoverableOutbox(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: true, TenantID: 7, KnowledgeBaseID: "kb"}
	tracker := &retryPreparingTracker{prepared: prepared}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}, deleteErr: errors.New("db unavailable")}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "acknowledgement failed")
	require.False(t, tracker.compensated)
	require.NoError(t, pending.deleteCtxErr)
	require.Len(t, queue.tasks, 1)

	pending.deleteErr = nil
	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Len(t, queue.tasks, 1, "an in-process published marker must prevent Lite duplicate enqueue")
	require.True(t, pending.deleted)
	require.False(t, tracker.compensated)
}

func TestRetryFailedKnowledgeSpanIdempotentFailedPreparationDoesNotReturnAccepted(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusFailed, ErrorMessage: "queue unavailable"}
	svc := &knowledgeService{spanTracker: &retryPreparingTracker{prepared: prepared}}
	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "queue unavailable")
}

func TestRetryFailedKnowledgeSpanIdempotentPendingWithoutOutboxReturnsOriginalAccepted(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: false}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: &retryPreparingTracker{prepared: prepared},
		task: queue, taskPendingRepo: &retryPendingRepo{}}
	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Empty(t, queue.tasks, "an acknowledged idempotent retry must not be published again")
}

func TestRetryFailedKnowledgeSpanConcurrentPendingPublishesOnce(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: true, TenantID: 7, KnowledgeBaseID: "kb"}
	queue := &retryCaptureEnqueuer{}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	svc := &knowledgeService{spanTracker: &retryPreparingTracker{prepared: prepared},
		task: queue, taskPendingRepo: pending}

	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
			results <- err
		}()
	}
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Len(t, queue.tasks, 1, "the process guard must close the Lite pending-window duplicate")
}

func TestRetryFailedKnowledgeSpanConcurrentFailedPublisherDoesNotReturnStaleAccepted(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid-ab", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid-ab:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: true, TenantID: 7, KnowledgeBaseID: "kb"}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	tracker := &retryPreparingTracker{prepared: prepared, pending: pending}
	queue := &retryBlockingFailEnqueuer{started: make(chan struct{}), release: make(chan struct{})}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	results := make(chan error, 2)
	go func() {
		_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
		results <- err
	}()
	<-queue.started
	go func() {
		_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
		results <- err
	}()
	close(queue.release)
	firstErr, secondErr := <-results, <-results
	require.Error(t, firstErr)
	require.Error(t, secondErr)
	require.Contains(t, firstErr.Error()+secondErr.Error(), "queue unavailable")
	require.Equal(t, 1, queue.calls, "B must not republish after A terminalizes the exact target")
	require.Equal(t, 1, tracker.compensationCount, "compensation stays inside A's dispatch guard")
}

func TestRetryFailedKnowledgeSpanMissingOutboxReadFailureIsIndeterminateWithoutCompensation(t *testing.T) {
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: "kid-read", Attempt: 5,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid-read:5:summary",
		Status: types.SpanStatusPending, DispatchRequired: false}
	tracker := &retryPreparingTracker{prepared: prepared, exactErr: errors.New("database unavailable")}
	svc := &knowledgeService{spanTracker: tracker, task: &retryCaptureEnqueuer{},
		taskPendingRepo: &retryPendingRepo{}}
	_, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "indeterminate")
	require.False(t, tracker.compensated)
}

func TestRetryFailedKnowledgeSpansSelectsFourLatestOwnersIntoOneAttempt(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed}
	questionGroup := types.KnowledgeProcessingSpan{ID: 5, KnowledgeID: "kid", Attempt: 4,
		SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed}
	owners := []types.KnowledgeProcessingSpan{
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "summary-old", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
		{ID: 4, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
		{ID: 6, KnowledgeID: "kid", Attempt: 4, SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{ID: 7, KnowledgeID: "kid", Attempt: 4, SpanID: "question", ParentSpanID: "question-group", Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk"}, "question_count": 1}},
		{ID: 8, KnowledgeID: "kid", Attempt: 4, SpanID: "done", ParentSpanID: "post", Name: "postprocess.graph.chunk[4]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{ID: 9, KnowledgeID: "kid", Attempt: 4, SpanID: "diagnostic", ParentSpanID: "post", Name: "postprocess.wiki.page[x]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
	}
	prepared := []*types.KnowledgeSpanRetryPreparation{
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "graph", Attempt: 5, SpanID: "new-graph", Name: "postprocess.graph.chunk[3]", TaskID: "knowledge-fanout:kid:5:graph:3", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb", Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "question", Attempt: 5, SpanID: "new-question", Name: "postprocess.question.batch[3]", TaskID: "knowledge-fanout:kid:5:question:3", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb", Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk"}, "question_count": 1}},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb"},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "wiki", Attempt: 5, SpanID: "new-wiki", Name: "postprocess.wiki", TaskID: "knowledge-fanout:kid:5:wiki", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb"},
	}
	snapshots := make(map[string]*types.KnowledgeSpanRetryTargetSnapshot)
	for i := range owners {
		owner := owners[i]
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{
			Source: owner, Parent: post,
			LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusFailed},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb",
		}
	}
	candidateRows := []types.KnowledgeProcessingSpan{post, questionGroup}
	candidateRows = append(candidateRows, owners...)
	tracker := &retryPreparingTracker{candidates: candidateRows,
		preparedList: prepared, snapshots: snapshots}
	pending := &retryPendingRepo{}
	for _, item := range prepared {
		pending.rows = append(pending.rows, retryOutbox(t, item))
	}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	result, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "aggregate-1",
	})

	require.NoError(t, err)
	require.Equal(t, 5, result.Attempt)
	require.Len(t, result.Targets, 4)
	require.Len(t, tracker.multiRequest.Targets, 4)
	require.Len(t, queue.tasks, 4)
	for _, target := range tracker.multiRequest.Targets {
		require.NotEqual(t, "summary-old", target.SpanID)
		require.NotEqual(t, "done", target.SpanID)
		require.NotEqual(t, "diagnostic", target.SpanID)
	}
	replayed, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "aggregate-1",
	})
	require.NoError(t, err)
	require.Equal(t, result, replayed)
	require.Len(t, queue.tasks, 4, "same client id must not republish acknowledged deterministic tasks")
}

type retryFailOnCallEnqueuer struct {
	retryCaptureEnqueuer
	failAt int
}

func (q *retryFailOnCallEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	if len(q.tasks)+1 == q.failAt {
		return nil, errors.New("queue unavailable")
	}
	return q.retryCaptureEnqueuer.Enqueue(task, opts...)
}

func TestRetryFailedKnowledgeSpansPartialPublishFailureRemainsRecoverable(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed}
	owners := []types.KnowledgeProcessingSpan{
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
	}
	prepared := []*types.KnowledgeSpanRetryPreparation{
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb"},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "wiki", Attempt: 5, SpanID: "new-wiki", Name: "postprocess.wiki", TaskID: "knowledge-fanout:kid:5:wiki", Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb"},
	}
	snapshots := map[string]*types.KnowledgeSpanRetryTargetSnapshot{}
	for i := range owners {
		owner := owners[i]
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{Source: owner, Parent: post,
			LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusFailed},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb"}
	}
	tracker := &retryPreparingTracker{candidates: append([]types.KnowledgeProcessingSpan{post}, owners...),
		preparedList: prepared, snapshots: snapshots}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared[0]), retryOutbox(t, prepared[1])}}
	queue := &retryFailOnCallEnqueuer{failAt: 2}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}

	_, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "aggregate-partial",
	})

	require.Error(t, err)
	appErr, ok := werrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 503, appErr.HTTPCode)
	require.Len(t, queue.tasks, 1)
	require.NotEmpty(t, pending.rows, "unpublished durable targets must remain recoverable")
	require.True(t, tracker.compensated)
}

func TestRetryFailedKnowledgeSpansConcurrentDoubleClickPublishesOnce(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed}
	summary := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: "kid", Attempt: 4,
		SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed}
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "summary", Attempt: 5,
		SpanID: "new-summary", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
		Status: types.SpanStatusPending, TenantID: 7, KnowledgeBaseID: "kb",
	}
	tracker := &retryPreparingTracker{
		candidates:   []types.KnowledgeProcessingSpan{post, summary},
		preparedList: []*types.KnowledgeSpanRetryPreparation{prepared},
		snapshots: map[string]*types.KnowledgeSpanRetryTargetSnapshot{"summary": {
			Source: summary, Parent: post,
			LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusFailed},
			LatestOwnerSpanID: "summary", TenantID: 7, KnowledgeBaseID: "kb",
		}},
	}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending}
	request := types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "same-double-click",
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := svc.RetryFailedKnowledgeSpans(context.Background(), request)
			errs <- err
		}()
	}
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Len(t, queue.tasks, 1)
}

func TestRetryFailedKnowledgeSpansEmptyAndCandidateReadFailureFailClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code int
	}{
		{name: "empty", code: 409},
		{name: "read failure", err: errors.New("database unavailable"), code: 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracker := &retryPreparingTracker{inspectErr: test.err}
			svc := &knowledgeService{spanTracker: tracker}
			_, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
				KnowledgeID: "kid", Attempt: 4, ClientRequestID: "aggregate",
			})
			require.Error(t, err)
			appErr, ok := werrors.IsAppError(err)
			require.True(t, ok)
			require.Equal(t, test.code, appErr.HTTPCode)
		})
	}
}

func TestRetryFailedKnowledgeSpansRejectsUnselectedActiveSibling(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	summary := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: "kid", Attempt: 4,
		SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed}
	graph := types.KnowledgeProcessingSpan{ID: 3, KnowledgeID: "kid", Attempt: 4,
		SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning}
	tracker := &retryPreparingTracker{
		candidates: []types.KnowledgeProcessingSpan{post, summary, graph},
		snapshots: map[string]*types.KnowledgeSpanRetryTargetSnapshot{
			"summary": {Source: summary, Parent: post,
				LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusFailed},
				LatestOwnerSpanID: "summary", TenantID: 7, KnowledgeBaseID: "kb"},
			"graph": {Source: graph, Parent: post,
				LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
				LatestOwnerSpanID: "graph", TenantID: 7, KnowledgeBaseID: "kb"},
		},
		preparedList: []*types.KnowledgeSpanRetryPreparation{{KnowledgeID: "kid", SourceAttempt: 4,
			SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary", Name: "postprocess.summary",
			TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending}},
	}
	svc := &knowledgeService{spanTracker: tracker, redisClient: stalledRetryRedis(t)}
	_, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "active-sibling",
	})

	require.Error(t, err)
	appErr, ok := werrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 409, appErr.HTTPCode)
	require.Empty(t, tracker.multiRequest.Targets, "active sibling must block before preparation")
}

func runFourOwnerAggregateRetry(t *testing.T, summaryStatus string) (*retryPreparingTracker, *retryCaptureEnqueuer) {
	t.Helper()
	t.Setenv("NEO4J_ENABLE", "true")
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	questionGroup := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: "kid", Attempt: 4,
		SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale}
	owners := []types.KnowledgeProcessingSpan{
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: summaryStatus, UpdatedAt: stale},
		{ID: 4, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki", ParentSpanID: "post",
			Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale},
		{ID: 5, KnowledgeID: "kid", Attempt: 4, SpanID: "graph", ParentSpanID: "post",
			Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			UpdatedAt: stale, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{ID: 6, KnowledgeID: "kid", Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			UpdatedAt: stale, Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk"}, "question_count": 1}},
		{ID: 7, KnowledgeID: "kid", Attempt: 4, SpanID: "done-graph", ParentSpanID: "post",
			Name: "postprocess.graph.chunk[4]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
	}
	snapshots := make(map[string]*types.KnowledgeSpanRetryTargetSnapshot, 4)
	for i := 0; i < 4; i++ {
		owner := owners[i]
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{Source: owner, Parent: post,
			LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb"}
	}
	prepared := []*types.KnowledgeSpanRetryPreparation{
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary",
			Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "wiki", Attempt: 5, SpanID: "new-wiki",
			Name: "postprocess.wiki", TaskID: "knowledge-fanout:kid:5:wiki", Status: types.SpanStatusPending},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "graph", Attempt: 5, SpanID: "new-graph",
			Name: "postprocess.graph.chunk[3]", TaskID: "knowledge-fanout:kid:5:graph:3", Status: types.SpanStatusPending,
			Input: owners[2].Input},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "question", Attempt: 5, SpanID: "new-question",
			Name: "postprocess.question.batch[3]", TaskID: "knowledge-fanout:kid:5:question:3", Status: types.SpanStatusPending,
			Input: owners[3].Input},
	}
	tracker := &retryPreparingTracker{candidates: append([]types.KnowledgeProcessingSpan{post, questionGroup}, owners...),
		preparedList: prepared, snapshots: snapshots}
	pending := &retryPendingRepo{}
	for _, item := range prepared {
		pending.rows = append(pending.rows, retryOutbox(t, item))
	}
	claim := &types.TaskPendingOpClaimSnapshot{Found: true, Consistent: true, RowIDs: []int64{9},
		ClaimToken: "claim", ClaimedByTaskID: "wiki-delivery", HeartbeatAt: &stale}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: &retryClaimPendingRepo{
		TaskPendingOpsRepository: pending, claim: claim}, taskInspector: &retryRuntimeInspector{supported: true},
		redisClient: stalledRetryRedis(t)}
	result, err := svc.RetryFailedKnowledgeSpans(context.Background(), types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: "kid", Attempt: 4, ClientRequestID: "four-owner-retry",
	})
	require.NoError(t, err)
	require.Equal(t, 5, result.Attempt)
	require.Len(t, result.Targets, 4)
	require.Len(t, tracker.multiRequest.Targets, 4)
	require.Len(t, queue.tasks, 4)
	for _, target := range tracker.multiRequest.Targets {
		require.NotEqual(t, "done-graph", target.SpanID)
	}
	return tracker, queue
}

func TestRetryFailedKnowledgeSpansAllStalledOwnersShareOneAttempt(t *testing.T) {
	runFourOwnerAggregateRetry(t, types.SpanStatusRunning)
}

func TestRetryFailedKnowledgeSpansIncludesFailedAndAllStalledOwners(t *testing.T) {
	runFourOwnerAggregateRetry(t, types.SpanStatusFailed)
}

func TestEvaluateKnowledgeSpanAggregateRetrySkipsTerminalNonFailedOwners(t *testing.T) {
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid-terminal-fanout", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusDone}
	questionGroup := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: post.KnowledgeID, Attempt: 4,
		SpanID: "question-group", ParentSpanID: post.SpanID, Name: "postprocess.question",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone}
	candidates := []types.KnowledgeProcessingSpan{post, questionGroup}
	for i := 0; i < 4_500; i++ {
		candidates = append(candidates, types.KnowledgeProcessingSpan{
			ID: int64(i + 3), KnowledgeID: post.KnowledgeID, Attempt: 4,
			SpanID: fmt.Sprintf("graph-%d", i), ParentSpanID: post.SpanID,
			Name: fmt.Sprintf("postprocess.graph.chunk[%d]", i), Kind: types.SpanKindSubSpan,
			Status: types.SpanStatusDone,
		})
	}
	for i := 0; i < 250; i++ {
		candidates = append(candidates, types.KnowledgeProcessingSpan{
			ID: int64(i + 4_503), KnowledgeID: post.KnowledgeID, Attempt: 4,
			SpanID: fmt.Sprintf("question-%d", i), ParentSpanID: questionGroup.SpanID,
			Name: fmt.Sprintf("postprocess.question.batch[%d]", i), Kind: types.SpanKindSubSpan,
			Status: types.SpanStatusCancelled,
		})
	}

	tracker := &retryPreparingTracker{candidates: candidates}
	svc := &knowledgeService{spanTracker: tracker}
	action, err := svc.EvaluateKnowledgeSpanAggregateRetry(context.Background(),
		types.KnowledgeSpanAggregateRetryRequest{KnowledgeID: post.KnowledgeID, Attempt: 4})

	require.NoError(t, err)
	require.False(t, action.Allowed)
	require.Equal(t, "no_retryable_targets", action.Reason)
	require.Zero(t, tracker.inspectCalls, "terminal non-failed owners must not trigger per-owner liveness reads")
}

func TestRetryFailedKnowledgeSpansCarriesUnreplayableStalledQuestion(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid-legacy-question", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	questionGroup := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: post.KnowledgeID, Attempt: 4,
		SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale}
	graph := types.KnowledgeProcessingSpan{ID: 3, KnowledgeID: post.KnowledgeID, Attempt: 4,
		SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale,
		Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}}
	legacyQuestion := types.KnowledgeProcessingSpan{ID: 4, KnowledgeID: post.KnowledgeID, Attempt: 4,
		SpanID: "question", ParentSpanID: "question-group", Name: "postprocess.question.batch[3]",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale,
		Input: types.JSONMap{"batch_index": 3, "chunks": 20, "question_count": 4}}
	snapshots := map[string]*types.KnowledgeSpanRetryTargetSnapshot{}
	for _, owner := range []types.KnowledgeProcessingSpan{graph, legacyQuestion} {
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{Source: owner, Parent: post,
			LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
				Status: types.SpanStatusRunning},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb"}
	}
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: post.KnowledgeID, SourceAttempt: 4,
		SourceSpanID: "graph", Attempt: 5, SpanID: "new-graph", Name: graph.Name,
		TaskID: "knowledge-fanout:" + post.KnowledgeID + ":5:graph:3", Status: types.SpanStatusPending,
		TenantID: 7, KnowledgeBaseID: "kb", Input: graph.Input}
	tracker := &retryPreparingTracker{
		candidates:   []types.KnowledgeProcessingSpan{post, questionGroup, graph, legacyQuestion},
		preparedList: []*types.KnowledgeSpanRetryPreparation{prepared}, snapshots: snapshots,
	}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}

	action, err := svc.EvaluateKnowledgeSpanAggregateRetry(context.Background(),
		types.KnowledgeSpanAggregateRetryRequest{KnowledgeID: post.KnowledgeID, Attempt: 4})
	require.NoError(t, err)
	require.True(t, action.Allowed)
	require.Len(t, action.Targets, 1)
	require.Equal(t, "graph", action.Targets[0].SourceSpanID)

	result, err := svc.RetryFailedKnowledgeSpans(context.Background(),
		types.KnowledgeSpanAggregateRetryRequest{KnowledgeID: post.KnowledgeID, Attempt: 4,
			ClientRequestID: "legacy-question-repair"})
	require.NoError(t, err)
	require.Len(t, result.Targets, 1)
	require.Len(t, tracker.multiRequest.Targets, 1)
	require.Equal(t, "graph", tracker.multiRequest.Targets[0].SpanID)
	require.Len(t, tracker.multiRequest.CarryoverFences, 1)
	require.Equal(t, "question", tracker.multiRequest.CarryoverFences[0].SourceSpanID)
	require.Len(t, queue.tasks, 1)
}

func TestRetryFailedKnowledgeSpanExecutesClickedAndCarriesOtherStalledOwners(t *testing.T) {
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid-row-closure", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	questionGroup := types.KnowledgeProcessingSpan{ID: 2, KnowledgeID: post.KnowledgeID, Attempt: 4,
		SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale}
	owners := []types.KnowledgeProcessingSpan{
		{ID: 3, KnowledgeID: post.KnowledgeID, Attempt: 4, SpanID: "summary", ParentSpanID: "post",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale},
		{ID: 4, KnowledgeID: post.KnowledgeID, Attempt: 4, SpanID: "graph", ParentSpanID: "post",
			Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			UpdatedAt: stale, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{ID: 5, KnowledgeID: post.KnowledgeID, Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			UpdatedAt: stale, Input: types.JSONMap{"batch_index": 3, "chunk_ids": []string{"chunk-3"}, "question_count": 1}},
	}
	snapshots := make(map[string]*types.KnowledgeSpanRetryTargetSnapshot, len(owners))
	for _, owner := range owners {
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{Source: owner, Parent: post,
			LatestRoot: types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot,
				Status: types.SpanStatusRunning},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb"}
	}
	prepared := &types.KnowledgeSpanRetryPreparation{KnowledgeID: post.KnowledgeID, SourceAttempt: 4,
		SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary", Name: "postprocess.summary",
		TaskID: "knowledge-fanout:" + post.KnowledgeID + ":5:summary", Status: types.SpanStatusPending,
		TenantID: 7, KnowledgeBaseID: "kb"}
	tracker := &retryPreparingTracker{candidates: append([]types.KnowledgeProcessingSpan{post, questionGroup}, owners...),
		preparedList: []*types.KnowledgeSpanRetryPreparation{prepared}, snapshots: snapshots}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared)}}
	queue := &retryCaptureEnqueuer{}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}

	got, err := svc.RetryFailedKnowledgeSpan(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: post.KnowledgeID, Attempt: 4, SpanID: "summary", ClientRequestID: "row-closure"})
	require.NoError(t, err)
	require.Equal(t, prepared, got)
	require.Equal(t, "row", tracker.multiRequest.RequestKind)
	require.Len(t, tracker.multiRequest.Targets, 1)
	require.Equal(t, "summary", tracker.multiRequest.Targets[0].SpanID)
	require.Len(t, tracker.multiRequest.CarryoverFences, 2)
	require.ElementsMatch(t, []string{"graph", "question"}, []string{
		tracker.multiRequest.CarryoverFences[0].SourceSpanID,
		tracker.multiRequest.CarryoverFences[1].SourceSpanID,
	})
	require.Len(t, queue.tasks, 1)
}

func TestRetryFailedKnowledgeSpansConcurrentStalledOwnersHaveWinnerWithoutLeaseSplit(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	stale := time.Now().Add(-stalledSpanRetryHeartbeatTimeout - time.Minute)
	post := types.KnowledgeProcessingSpan{ID: 1, KnowledgeID: "kid", Attempt: 4,
		SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning}
	owners := []types.KnowledgeProcessingSpan{
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: stale},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "graph", ParentSpanID: "post",
			Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			UpdatedAt: stale, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
	}
	snapshots := make(map[string]*types.KnowledgeSpanRetryTargetSnapshot, 2)
	for _, owner := range owners {
		snapshots[owner.SpanID] = &types.KnowledgeSpanRetryTargetSnapshot{Source: owner, Parent: post,
			LatestRoot:        types.KnowledgeProcessingSpan{Attempt: 4, Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
			LatestOwnerSpanID: owner.SpanID, TenantID: 7, KnowledgeBaseID: "kb"}
	}
	prepared := []*types.KnowledgeSpanRetryPreparation{
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "summary", Attempt: 5, SpanID: "new-summary",
			Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary", Status: types.SpanStatusPending},
		{KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "graph", Attempt: 5, SpanID: "new-graph",
			Name: "postprocess.graph.chunk[3]", TaskID: "knowledge-fanout:kid:5:graph:3", Status: types.SpanStatusPending,
			Input: owners[1].Input},
	}
	tracker := &retryPreparingTracker{candidates: append([]types.KnowledgeProcessingSpan{post}, owners...),
		preparedList: prepared, snapshots: snapshots}
	pending := &retryPendingRepo{rows: []*types.TaskPendingOp{retryOutbox(t, prepared[0]), retryOutbox(t, prepared[1])}}
	queue := &blockingRetryCaptureEnqueuer{entered: make(chan struct{}), release: make(chan struct{})}
	svc := &knowledgeService{spanTracker: tracker, task: queue, taskPendingRepo: pending,
		taskInspector: &retryRuntimeInspector{supported: true}, redisClient: stalledRetryRedis(t)}
	request := types.KnowledgeSpanAggregateRetryRequest{KnowledgeID: "kid", Attempt: 4,
		ClientRequestID: "stalled-double-click"}
	start := make(chan struct{})
	type aggregateCall struct {
		result *types.KnowledgeSpanAggregateRetryResult
		err    error
	}
	calls := make(chan aggregateCall, 2)
	for range 2 {
		go func() {
			<-start
			result, err := svc.RetryFailedKnowledgeSpans(context.Background(), request)
			calls <- aggregateCall{result: result, err: err}
		}()
	}
	close(start)
	<-queue.entered
	loser := <-calls
	require.Error(t, loser.err)
	appErr, ok := werrors.IsAppError(loser.err)
	require.True(t, ok)
	require.Equal(t, 409, appErr.HTTPCode)
	close(queue.release)
	winner := <-calls
	require.NoError(t, winner.err)
	require.Equal(t, 5, winner.result.Attempt)
	require.Len(t, winner.result.Targets, 2)
	require.Len(t, tracker.multiRequest.Targets, 2)
	require.Len(t, queue.tasks, 2, "the canonical two-target repair publishes once")
}
