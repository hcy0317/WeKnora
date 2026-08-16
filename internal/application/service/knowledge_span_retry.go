package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

type failedSpanRetryPreparer interface {
	PrepareFailedSpanRetry(context.Context, types.KnowledgeSpanRetryRequest) (*types.KnowledgeSpanRetryPreparation, error)
}

type failedSpanMultiRetryPreparer interface {
	PrepareFailedSpanRetries(context.Context, types.KnowledgeSpanMultiRetryRequest) ([]*types.KnowledgeSpanRetryPreparation, error)
}

type stalledSpanRetryTargetReader interface {
	InspectSpanRetryTarget(context.Context, types.KnowledgeSpanRetryRequest) (*types.KnowledgeSpanRetryTargetSnapshot, error)
}

type stalledSpanRetryRuntimeInspector interface {
	GetRuntimeTask(context.Context, string, string) (*types.RuntimeTaskInfo, bool, error)
}

type failedSpanRetryCompensator interface {
	FailPreparedSpanRetry(context.Context, *types.KnowledgeSpanRetryPreparation, string, string) error
}

type failedSpanRetryExactReader interface {
	GetPreparedSpanRetry(context.Context, string, int, string) (*types.KnowledgeProcessingSpan, error)
}

type failedSpanRetryCandidateReader interface {
	ListFailedSpanRetryCandidates(context.Context, string, int) ([]types.KnowledgeProcessingSpan, error)
}

type existingFailedSpanRetryPlanReader interface {
	FindExistingFailedSpanRetryPlan(context.Context, string, int, string, string) ([]*types.KnowledgeSpanRetryPreparation, error)
}

const (
	failedSpanRetryCompensationTimeout = 10 * time.Second
	failedSpanRetryTaskRetention       = 24 * time.Hour
	stalledSpanRetryHeartbeatTimeout   = WikiIngestTaskTimeout + 15*time.Minute
	stalledSpanRecoveryLeaseTTL        = 90 * time.Second
)

func retryOwnerQueue(name string) (string, error) {
	switch {
	case name == "postprocess.summary":
		return types.QueueSummary, nil
	case isQuestionBatchRetryOwner(name):
		return types.QueueQuestion, nil
	case name == "postprocess.wiki":
		return types.QueueWiki, nil
	case strings.HasPrefix(name, "postprocess.graph.chunk[") && strings.HasSuffix(name, "]"):
		return types.QueueGraph, nil
	default:
		return "", repository.ErrKnowledgeSpanRetryUnsupported
	}
}

func retryOwnerNameSupportedForService(source *types.KnowledgeProcessingSpan) bool {
	if source == nil || source.Kind != types.SpanKindSubSpan {
		return false
	}
	return source.Name == "postprocess.summary" || isQuestionBatchRetryOwner(source.Name) ||
		source.Name == "postprocess.wiki" ||
		(strings.HasPrefix(source.Name, "postprocess.graph.chunk[") && strings.HasSuffix(source.Name, "]"))
}

func questionBatchRetryIndex(name string) (int, bool) {
	const prefix = "postprocess.question.batch["
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, "]") {
		return 0, false
	}
	indexText := strings.TrimSuffix(strings.TrimPrefix(name, prefix), "]")
	index, err := strconv.Atoi(indexText)
	if err != nil || index < 0 || name != fmt.Sprintf("%s%d]", prefix, index) {
		return 0, false
	}
	return index, true
}

func isQuestionBatchRetryOwner(name string) bool {
	_, ok := questionBatchRetryIndex(name)
	return ok
}

func retryOwnerReplayable(source *types.KnowledgeProcessingSpan) bool {
	if !retryOwnerNameSupportedForService(source) {
		return false
	}
	switch {
	case source.Name == "postprocess.summary", source.Name == "postprocess.wiki":
		return true
	case isQuestionBatchRetryOwner(source.Name):
		nameIndex, _ := questionBatchRetryIndex(source.Name)
		batchIndex, err := retryInputInt(source.Input, "batch_index")
		if err != nil || batchIndex != nameIndex {
			return false
		}
		chunkIDs, err := retryInputStrings(source.Input, "chunk_ids")
		if err != nil || len(chunkIDs) == 0 {
			return false
		}
		questionCount, err := retryInputInt(source.Input, "question_count")
		return err == nil && questionCount > 0
	case strings.HasPrefix(source.Name, "postprocess.graph.chunk["):
		chunkID := strings.TrimSpace(fmt.Sprint(source.Input["chunk_id"]))
		modelID := strings.TrimSpace(fmt.Sprint(source.Input["model_id"]))
		chunkIndex, err := retryInputInt(source.Input, "chunk_index")
		nameIndexText := strings.TrimSuffix(strings.TrimPrefix(source.Name,
			"postprocess.graph.chunk["), "]")
		nameIndex, parseErr := strconv.Atoi(nameIndexText)
		return err == nil && parseErr == nil && chunkIndex == nameIndex &&
			chunkID != "" && chunkID != "<nil>" && modelID != "" && modelID != "<nil>"
	default:
		return false
	}
}

func terminalRetrySpanStatus(status string) bool {
	switch status {
	case types.SpanStatusDone, types.SpanStatusFailed, types.SpanStatusSkipped, types.SpanStatusCancelled:
		return true
	default:
		return false
	}
}

type failedSpanRetryTopology struct {
	latestTargets    map[string]*types.KnowledgeProcessingSpan
	latestDirect     map[string]*types.KnowledgeProcessingSpan
	latestQuestion   map[string]*types.KnowledgeProcessingSpan
	questionParentID string
}

func buildFailedSpanRetryTopology(rows []types.KnowledgeProcessingSpan, attempt int) failedSpanRetryTopology {
	topology := failedSpanRetryTopology{
		latestTargets:  make(map[string]*types.KnowledgeProcessingSpan),
		latestDirect:   make(map[string]*types.KnowledgeProcessingSpan),
		latestQuestion: make(map[string]*types.KnowledgeProcessingSpan),
	}
	latestPostID := ""
	var latestPostRowID int64
	for i := range rows {
		row := &rows[i]
		if row.Attempt == attempt && row.Kind == types.SpanKindStage && row.Name == types.StagePostProcess &&
			row.ID > latestPostRowID {
			latestPostID, latestPostRowID = row.SpanID, row.ID
		}
	}
	if latestPostID == "" {
		return topology
	}
	var latestQuestionParentRowID int64
	for i := range rows {
		row := &rows[i]
		if row.Attempt != attempt || row.Kind != types.SpanKindSubSpan || row.ParentSpanID != latestPostID {
			continue
		}
		if row.Name == "postprocess.question" && row.ID > latestQuestionParentRowID {
			topology.questionParentID, latestQuestionParentRowID = row.SpanID, row.ID
		}
		if row.Name == "postprocess.summary" || row.Name == "postprocess.wiki" ||
			strings.HasPrefix(row.Name, "postprocess.graph.chunk[") || row.Name == "postprocess.question" {
			if prior := topology.latestDirect[row.Name]; prior == nil || row.ID > prior.ID {
				topology.latestDirect[row.Name] = row
			}
		}
	}
	for name, row := range topology.latestDirect {
		if name != "postprocess.question" && retryOwnerNameSupportedForService(row) {
			topology.latestTargets[name] = row
		}
	}
	if topology.questionParentID == "" {
		return topology
	}
	for i := range rows {
		row := &rows[i]
		if row.Attempt != attempt || row.Kind != types.SpanKindSubSpan ||
			row.ParentSpanID != topology.questionParentID || !isQuestionBatchRetryOwner(row.Name) {
			continue
		}
		if prior := topology.latestQuestion[row.Name]; prior == nil || row.ID > prior.ID {
			topology.latestQuestion[row.Name] = row
			topology.latestTargets[row.Name] = row
		}
	}
	return topology
}

func failedSpanRetryTopologyBlock(
	topology failedSpanRetryTopology, selectedSpanIDs map[string]struct{},
) (string, bool) {
	questionSelected := false
	for _, row := range topology.latestQuestion {
		if _, selected := selectedSpanIDs[row.SpanID]; selected {
			questionSelected = true
			break
		}
	}
	check := func(row *types.KnowledgeProcessingSpan, selected bool) (string, bool) {
		if row == nil || terminalRetrySpanStatus(row.Status) {
			return "", false
		}
		if selected && (row.Status == types.SpanStatusPending || row.Status == types.SpanStatusRunning) {
			return "", false
		}
		if row.Status == types.SpanStatusPending || row.Status == types.SpanStatusRunning {
			return types.KnowledgeSpanRetryStateActive, true
		}
		return types.KnowledgeSpanRetryStateUnknown, true
	}
	for name, row := range topology.latestDirect {
		_, selected := selectedSpanIDs[row.SpanID]
		if name == "postprocess.question" {
			selected = questionSelected
		}
		if state, blocked := check(row, selected); blocked {
			return state, true
		}
	}
	for _, row := range topology.latestQuestion {
		_, selected := selectedSpanIDs[row.SpanID]
		if state, blocked := check(row, selected); blocked {
			return state, true
		}
	}
	return "", false
}

func liveRuntimeRetryTask(state types.RuntimeTaskState) bool {
	switch state {
	case types.RuntimeTaskPending, types.RuntimeTaskActive,
		types.RuntimeTaskScheduled, types.RuntimeTaskRetry:
		return true
	default:
		return false
	}
}

// EvaluateKnowledgeSpanRetry is the server-authoritative read-only retry gate.
// Time is only one input: stalled additionally requires exact DB identity and
// an inspectable absence of the deterministic runtime task. Wiki durable claim
// ownership is added by the claim-fencing capability before Wiki can be
// authorized. Summary, question and graph owners use the same exact runtime
// absence plus attempt-scoped owner lease protocol and fail closed whenever
// either probe is unavailable.
func (s *knowledgeService) EvaluateKnowledgeSpanRetry(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryAction, *types.KnowledgeSpanRetryStallFence, error) {
	exactAction, exactFence, exactErr := s.evaluateKnowledgeSpanRetry(ctx, request, nil, nil, false)
	if exactErr != nil || exactAction == nil || !exactAction.Allowed {
		return exactAction, exactFence, exactErr
	}
	plan, blocked, planErr := s.planFailedSpanAggregateRetry(ctx, types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
	})
	if planErr != nil {
		return &types.KnowledgeSpanRetryAction{State: types.KnowledgeSpanRetryStateUnknown,
			Reason: "liveness_read_failed"}, nil, planErr
	}
	if blocked != "" && blocked != "no_retryable_targets" {
		reason := "active_sibling"
		if blocked == types.KnowledgeSpanRetryStateUnknown {
			reason = "liveness_unavailable"
		}
		return &types.KnowledgeSpanRetryAction{State: blocked, Reason: reason}, nil, nil
	}
	if plan != nil {
		for _, candidate := range plan.planned {
			if candidate.row.SpanID == request.SpanID {
				return exactAction, exactFence, nil
			}
		}
	}
	return &types.KnowledgeSpanRetryAction{State: exactAction.State,
		Target: exactAction.Target, Reason: "active_sibling"}, nil, nil
}

func (s *knowledgeService) evaluateKnowledgeSpanRetry(
	ctx context.Context,
	request types.KnowledgeSpanRetryRequest,
	expectedRecoveryOwner *types.TaskClaimOwner,
	topologySelection map[string]struct{},
	allowFailedDuringActiveAttempt bool,
) (*types.KnowledgeSpanRetryAction, *types.KnowledgeSpanRetryStallFence, error) {
	action := &types.KnowledgeSpanRetryAction{Allowed: false, State: types.KnowledgeSpanRetryStateUnknown}
	reader, ok := s.tracker().(stalledSpanRetryTargetReader)
	if !ok {
		action.Reason = "liveness_unavailable"
		return action, nil, nil
	}
	snapshot, err := reader.InspectSpanRetryTarget(ctx, request)
	if err != nil {
		action.Reason = "liveness_read_failed"
		return action, nil, fmt.Errorf("inspect retry owner: %w", err)
	}
	if snapshot == nil {
		action.Reason = "target_not_found"
		return action, nil, nil
	}
	source := &snapshot.Source
	if snapshot.Parent.Kind != types.SpanKindStage || snapshot.Parent.Name != types.StagePostProcess ||
		!retryOwnerNameSupportedForService(source) {
		action.Reason = "unsupported_target"
		return action, nil, nil
	}
	if snapshot.ExistingRetry && source.Status == types.SpanStatusFailed {
		action.Allowed = true
		action.State = types.KnowledgeSpanRetryStateFailed
		action.Target = source.Name
		action.Reason = "idempotent_replay"
		return action, nil, nil
	}
	if snapshot.LatestRoot.Attempt != request.Attempt || snapshot.LatestOwnerSpanID != source.SpanID {
		action.Reason = "superseded_retry"
		return action, nil, nil
	}
	if topologySelection != nil {
		candidateReader, ok := s.tracker().(failedSpanRetryCandidateReader)
		if !ok {
			action.Reason = "liveness_unavailable"
			return action, nil, fmt.Errorf("load retry topology: candidate reader unavailable")
		}
		rows, err := candidateReader.ListFailedSpanRetryCandidates(ctx, request.KnowledgeID, request.Attempt)
		if err != nil {
			action.Reason = "liveness_read_failed"
			return action, nil, fmt.Errorf("load retry topology: %w", err)
		}
		if state, blocked := failedSpanRetryTopologyBlock(
			buildFailedSpanRetryTopology(rows, request.Attempt), topologySelection,
		); blocked {
			action.State = state
			action.Reason = "active_sibling"
			return action, nil, nil
		}
	}
	if source.Status == types.SpanStatusFailed &&
		(terminalRetrySpanStatus(snapshot.LatestRoot.Status) || allowFailedDuringActiveAttempt) {
		action.Allowed = true
		action.State = types.KnowledgeSpanRetryStateFailed
		action.Target = source.Name
		return action, nil, nil
	}
	if source.Status != types.SpanStatusPending && source.Status != types.SpanStatusRunning {
		action.Reason = "owner_not_retryable"
		return action, nil, nil
	}
	ownerRef := types.ProcessingOwnerRef{TenantID: snapshot.TenantID, KnowledgeID: request.KnowledgeID,
		Attempt: request.Attempt, Name: source.Name}
	ownerLease, err := inspectProcessingOwnerLease(ctx, s.redisClient, ownerRef)
	if err != nil {
		action.Reason = "owner_lease_inspection_failed"
		return action, nil, fmt.Errorf("inspect processing owner lease: %w", err)
	}
	if ownerLease != nil && ownerLease.Active {
		if expectedRecoveryOwner == nil || ownerLease.Owner != *expectedRecoveryOwner {
			action.State = types.KnowledgeSpanRetryStateActive
			action.Reason = "owner_lease_active"
			return action, nil, nil
		}
	}
	if source.UpdatedAt.IsZero() || source.UpdatedAt.After(time.Now().Add(-stalledSpanRetryHeartbeatTimeout)) {
		action.State = types.KnowledgeSpanRetryStateActive
		action.Reason = "heartbeat_fresh"
		return action, nil, nil
	}
	queue, err := retryOwnerQueue(source.Name)
	if err != nil {
		action.Reason = "unsupported_target"
		return action, nil, nil
	}
	taskID, err := failedSpanRepairTaskID(&types.KnowledgeSpanRetryPreparation{
		KnowledgeID: request.KnowledgeID, Attempt: request.Attempt, Name: source.Name, Input: source.Input,
	})
	if err != nil {
		action.Reason = "task_identity_invalid"
		return action, nil, nil
	}
	var claim *types.TaskPendingOpClaimSnapshot
	if source.Name == "postprocess.wiki" {
		claimLease, ok := s.taskPendingRepo.(interfaces.TaskPendingOpsClaimLease)
		if !ok || claimLease == nil {
			action.Reason = "durable_claim_inspection_unavailable"
			return action, nil, nil
		}
		claim, err = claimLease.InspectClaim(ctx, types.TypeWikiIngest,
			types.TaskScopeKnowledgeBase, snapshot.KnowledgeBaseID, request.KnowledgeID)
		if err != nil {
			action.Reason = "durable_claim_inspection_failed"
			return action, nil, fmt.Errorf("inspect exact wiki claim: %w", err)
		}
		if claim == nil || !claim.Found || !claim.Consistent || claim.ClaimToken == "" ||
			claim.ClaimedByTaskID == "" || claim.HeartbeatAt == nil {
			action.Reason = "durable_claim_identity_unknown"
			return action, nil, nil
		}
		if claim.HeartbeatAt.After(time.Now().Add(-wikiClaimStaleAfter)) {
			action.State = types.KnowledgeSpanRetryStateActive
			action.Reason = "durable_claim_heartbeat_fresh"
			return action, nil, nil
		}
		taskID = claim.ClaimedByTaskID
	}
	inspector, ok := s.taskInspector.(stalledSpanRetryRuntimeInspector)
	if !ok || inspector == nil {
		action.Reason = "runtime_inspection_unavailable"
		return action, nil, nil
	}
	task, supported, err := inspector.GetRuntimeTask(ctx, queue, taskID)
	if err != nil {
		action.Reason = "runtime_inspection_failed"
		return action, nil, fmt.Errorf("inspect exact retry task: %w", err)
	}
	if !supported {
		action.Reason = "runtime_inspection_unavailable"
		return action, nil, nil
	}
	if task != nil && liveRuntimeRetryTask(task.State) {
		action.State = types.KnowledgeSpanRetryStateActive
		action.Reason = "runtime_task_live"
		return action, nil, nil
	}
	if task != nil && task.State == types.RuntimeTaskCompleted {
		action.Reason = "runtime_task_completed_pending_settlement"
		return action, nil, nil
	}
	fence := &types.KnowledgeSpanRetryStallFence{
		KnowledgeID: request.KnowledgeID, TenantID: snapshot.TenantID, OwnerName: source.Name,
		SourceAttempt: request.Attempt,
		SourceSpanID:  source.SpanID, SourceUpdatedAt: source.UpdatedAt,
		LatestRootAttempt: snapshot.LatestRoot.Attempt, LastHeartbeatAt: source.UpdatedAt,
		TaskID: taskID, Queue: queue,
	}
	if claim != nil {
		fence.PendingOpIDs = append([]int64(nil), claim.RowIDs...)
		fence.ClaimToken = claim.ClaimToken
		fence.ClaimedByTaskID = claim.ClaimedByTaskID
		fence.ClaimHeartbeatAt = *claim.HeartbeatAt
		fence.LastHeartbeatAt = *claim.HeartbeatAt
	}
	action.Allowed = true
	action.State = types.KnowledgeSpanRetryStateStalled
	action.Target = source.Name
	return action, fence, nil
}

type failedSpanRetryDispatchGuardEntry struct {
	mu   sync.Mutex
	refs int
}

var failedSpanRetryDispatchGuards = struct {
	sync.Mutex
	entries map[string]*failedSpanRetryDispatchGuardEntry
}{entries: make(map[string]*failedSpanRetryDispatchGuardEntry)}

var failedSpanRetryPublished sync.Map

type failedSpanRetryDispatchState uint8

const (
	failedSpanRetryDispatchAcked failedSpanRetryDispatchState = iota
	failedSpanRetryDispatchNeedsCompensation
	failedSpanRetryDispatchPublishedUnacked
	failedSpanRetryDispatchPreviouslyFailed
	failedSpanRetryDispatchIndeterminate
)

type failedSpanRetryDispatchResult struct {
	state   failedSpanRetryDispatchState
	err     error
	message string
}

// WithFailedSpanRetryDispatchGuard serializes publication of one deterministic
// retry task inside a process. Redis replicas remain protected by TaskID plus
// Retention; this guard supplies the equivalent pending-window protection for
// Lite mode and for request/recovery races in one process.
func WithFailedSpanRetryDispatchGuard(taskID string, fn func() error) error {
	if taskID == "" || fn == nil {
		return errors.New("retry dispatch guard requires task id and callback")
	}
	failedSpanRetryDispatchGuards.Lock()
	entry := failedSpanRetryDispatchGuards.entries[taskID]
	if entry == nil {
		entry = &failedSpanRetryDispatchGuardEntry{}
		failedSpanRetryDispatchGuards.entries[taskID] = entry
	}
	entry.refs++
	failedSpanRetryDispatchGuards.Unlock()

	entry.mu.Lock()
	defer func() {
		entry.mu.Unlock()
		failedSpanRetryDispatchGuards.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(failedSpanRetryDispatchGuards.entries, taskID)
		}
		failedSpanRetryDispatchGuards.Unlock()
	}()
	return fn()
}

func retryInputInt(input types.JSONMap, key string) (int, error) {
	value, ok := input[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int64:
		return int(typed), nil
	case float64:
		return int(typed), nil
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		return parsed, err
	default:
		parsed, err := strconv.Atoi(fmt.Sprint(value))
		return parsed, err
	}
}

func retryInputStrings(input types.JSONMap, key string) ([]string, error) {
	value, ok := input[key]
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}
	var values []string
	switch typed := value.(type) {
	case []string:
		values = append(values, typed...)
	case []any:
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text == "" || text == "<nil>" {
				return nil, fmt.Errorf("invalid %s entry", key)
			}
			values = append(values, text)
		}
	default:
		return nil, fmt.Errorf("invalid %s", key)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("empty %s", key)
	}
	return values, nil
}

func retryInputOptionalString(input types.JSONMap, key string) string {
	value, ok := input[key]
	if !ok || value == nil {
		return ""
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "<nil>" {
		return ""
	}
	return text
}

func mapFailedSpanRetryError(err error) error {
	switch {
	case errors.Is(err, repository.ErrKnowledgeSpanRetryNotFound):
		return werrors.NewNotFoundError("Failed processing item not found")
	case errors.Is(err, repository.ErrKnowledgeSpanRetryNotLatest):
		return werrors.NewConflictError("Only a failed item from the latest attempt can be retried")
	case errors.Is(err, repository.ErrKnowledgeSpanRetryNotTerminal):
		return werrors.NewConflictError("The selected item or attempt is not in a retryable failed state")
	case errors.Is(err, repository.ErrKnowledgeSpanRetryUnsupported):
		return werrors.NewBadRequestError("This processing item cannot be retried independently")
	default:
		return werrors.NewServiceUnavailableError("Processing item retry could not be prepared")
	}
}

func releaseRetryRecoveryLease(lease *processingOwnerLease) {
	if lease == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = lease.Release(releaseCtx)
}

func (s *knowledgeService) authorizeFailedSpanRetryMutation(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryStallFence, *processingOwnerLease, error) {
	return s.authorizeFailedSpanRetryMutationWithTopology(ctx, request,
		map[string]struct{}{request.SpanID: {}}, false)
}

func (s *knowledgeService) failedSpanRetryPlanMiss(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) error {
	action, _, err := s.evaluateKnowledgeSpanRetry(ctx, request, nil, nil, false)
	if err != nil || action == nil {
		return werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
	}
	switch action.Reason {
	case "target_not_found":
		return werrors.NewNotFoundError("Failed processing item not found")
	case "unsupported_target":
		return werrors.NewBadRequestError("This processing item cannot be retried independently")
	}
	if action.State == types.KnowledgeSpanRetryStateUnknown {
		return werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
	}
	return werrors.NewConflictError("The selected processing item is not safely retryable")
}

func (s *knowledgeService) authorizeFailedSpanRetryMutationWithTopology(
	ctx context.Context, request types.KnowledgeSpanRetryRequest, topologySelection map[string]struct{},
	allowFailedDuringActiveAttempt bool,
) (*types.KnowledgeSpanRetryStallFence, *processingOwnerLease, error) {
	action, fence, evaluationErr := s.evaluateKnowledgeSpanRetry(
		ctx, request, nil, topologySelection, allowFailedDuringActiveAttempt)
	if evaluationErr != nil || action == nil {
		return nil, nil, werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
	}
	if !action.Allowed {
		switch action.Reason {
		case "target_not_found":
			return nil, nil, werrors.NewNotFoundError("Failed processing item not found")
		case "unsupported_target":
			return nil, nil, werrors.NewBadRequestError("This processing item cannot be retried independently")
		}
		if action.State == types.KnowledgeSpanRetryStateUnknown {
			return nil, nil, werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
		}
		return nil, nil, werrors.NewConflictError("The selected processing item is active or not safely retryable")
	}
	if fence == nil {
		return nil, nil, nil
	}
	owner := types.TaskClaimOwner{Token: uuid.NewString(), TaskID: "span-recovery:" + uuid.NewString()}
	lease, acquired, err := tryAcquireProcessingOwnerLease(ctx, s.redisClient, types.ProcessingOwnerRef{
		TenantID: fence.TenantID, KnowledgeID: fence.KnowledgeID,
		Attempt: fence.SourceAttempt, Name: fence.OwnerName,
	}, owner, stalledSpanRecoveryLeaseTTL)
	if err != nil {
		return nil, nil, werrors.NewServiceUnavailableError("Processing item owner lease could not be acquired")
	}
	if !acquired {
		return nil, nil, werrors.NewConflictError("The selected processing item has an active owner")
	}
	action, fence, evaluationErr = s.evaluateKnowledgeSpanRetry(
		lease.Context(), request, &owner, topologySelection, allowFailedDuringActiveAttempt)
	if evaluationErr != nil || action == nil {
		releaseRetryRecoveryLease(lease)
		return nil, nil, werrors.NewServiceUnavailableError("Processing item liveness could not be rechecked")
	}
	if action.State == types.KnowledgeSpanRetryStateUnknown {
		releaseRetryRecoveryLease(lease)
		return nil, nil, werrors.NewServiceUnavailableError("Processing item liveness could not be rechecked")
	}
	if !action.Allowed || fence == nil {
		releaseRetryRecoveryLease(lease)
		return nil, nil, werrors.NewConflictError("Processing item liveness changed during recovery")
	}
	return fence, lease, nil
}

// RetryFailedKnowledgeSpan commits a partial-repair attempt before publishing
// its single worker. A publish failure is made terminal immediately so the new
// attempt cannot strand the document in finalizing.
func (s *knowledgeService) RetryFailedKnowledgeSpan(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	if reader, ok := s.tracker().(existingFailedSpanRetryPlanReader); ok {
		existing, readErr := reader.FindExistingFailedSpanRetryPlan(
			ctx, request.KnowledgeID, request.Attempt, request.ClientRequestID, "row")
		if readErr != nil {
			if errors.Is(readErr, repository.ErrKnowledgeSpanRetryNotLatest) {
				return nil, werrors.NewConflictError("Client request id belongs to a different retry operation")
			}
			return nil, werrors.NewServiceUnavailableError("Existing processing item retry could not be validated")
		}
		if len(existing) > 0 {
			if len(existing) != 1 || existing[0] == nil || existing[0].SourceSpanID != request.SpanID {
				return nil, werrors.NewConflictError("Client request id belongs to a different processing item retry")
			}
			if err := s.publishPreparedFailedSpanRetry(ctx, existing[0]); err != nil {
				return nil, err
			}
			return existing[0], nil
		}
	}
	plan, blocked, err := s.planFailedSpanAggregateRetry(ctx, types.KnowledgeSpanAggregateRetryRequest{
		KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
		ClientRequestID: request.ClientRequestID, Language: request.Language,
	})
	if err != nil || blocked == types.KnowledgeSpanRetryStateUnknown {
		return nil, werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
	}
	if blocked == types.KnowledgeSpanRetryStateActive {
		return nil, werrors.NewConflictError("An unselected processing item is still active")
	}
	if plan == nil {
		return nil, s.failedSpanRetryPlanMiss(ctx, request)
	}
	clicked := false
	for _, candidate := range plan.planned {
		if candidate.row.SpanID == request.SpanID {
			clicked = true
			break
		}
	}
	if !clicked {
		return nil, s.failedSpanRetryPlanMiss(ctx, request)
	}
	leases := make([]*processingOwnerLease, 0, len(plan.planned))
	defer func() {
		for _, lease := range leases {
			releaseRetryRecoveryLease(lease)
		}
	}()
	carryoverFences := make([]*types.KnowledgeSpanRetryStallFence, 0, len(plan.planned)-1)
	var executionFence *types.KnowledgeSpanRetryStallFence
	for _, candidate := range plan.planned {
		if candidate.state != types.KnowledgeSpanRetryStateStalled {
			continue
		}
		single := types.KnowledgeSpanRetryRequest{KnowledgeID: request.KnowledgeID,
			Attempt: request.Attempt, SpanID: candidate.row.SpanID,
			ClientRequestID: request.ClientRequestID, Language: request.Language}
		fence, lease, authErr := s.authorizeFailedSpanRetryMutationWithTopology(
			ctx, single, plan.selectedSpanIDs, true)
		if authErr != nil {
			return nil, authErr
		}
		if lease != nil {
			leases = append(leases, lease)
		}
		if candidate.row.SpanID == request.SpanID {
			executionFence = fence
		} else {
			carryoverFences = append(carryoverFences, fence)
		}
	}
	preparer, ok := s.tracker().(failedSpanMultiRetryPreparer)
	if !ok {
		return nil, werrors.NewServiceUnavailableError("Processing item retry is unavailable")
	}
	preparations, err := preparer.PrepareFailedSpanRetries(ctx, types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
		ClientRequestID: request.ClientRequestID, Language: request.Language, RequestKind: "row",
		Targets:         []types.KnowledgeSpanMultiRetryTarget{{SpanID: request.SpanID, StallFence: executionFence}},
		CarryoverFences: carryoverFences,
	})
	if err != nil {
		return nil, mapFailedSpanRetryError(err)
	}
	if len(preparations) != 1 || preparations[0] == nil {
		return nil, werrors.NewInternalServerError("Processing item retry returned an invalid preparation set")
	}
	prepared := preparations[0]
	if err := s.publishPreparedFailedSpanRetry(ctx, prepared); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *knowledgeService) publishPreparedFailedSpanRetry(
	ctx context.Context, prepared *types.KnowledgeSpanRetryPreparation,
) error {
	if prepared == nil {
		return werrors.NewInternalServerError("Processing item retry returned an invalid preparation")
	}
	expectedTaskID, err := failedSpanRepairTaskID(prepared)
	if err != nil {
		return werrors.NewInternalServerError("Stored processing item retry payload is invalid")
	}
	if prepared.TaskID == "" || prepared.TaskID != expectedTaskID {
		return werrors.NewInternalServerError("Stored processing item retry task identity is invalid")
	}
	switch prepared.Status {
	case types.SpanStatusDone, types.SpanStatusRunning:
		return nil
	case types.SpanStatusFailed:
		message := strings.TrimSpace(prepared.ErrorMessage)
		if message == "" {
			message = "Previous processing item retry could not be published"
		}
		return werrors.NewServiceUnavailableError(message)
	case types.SpanStatusPending:
		// Even when preparation observed an acknowledged/missing outbox, enter
		// the guard and re-read the exact target. A concurrent failed publisher
		// may have terminalized it after this request loaded its stale pending
		// preparation.
	default:
		return werrors.NewConflictError("The processing item retry is no longer publishable")
	}

	err = WithFailedSpanRetryDispatchGuard(prepared.TaskID, func() error {
		result := s.dispatchFailedSpanRetryOutboxGuarded(ctx, prepared)
		switch result.state {
		case failedSpanRetryDispatchAcked:
			return nil
		case failedSpanRetryDispatchPreviouslyFailed:
			message := strings.TrimSpace(result.message)
			if message == "" {
				message = "Previous processing item retry could not be published"
			}
			return werrors.NewServiceUnavailableError(message)
		case failedSpanRetryDispatchPublishedUnacked:
			return werrors.NewServiceUnavailableError(
				"Processing item retry was published but durable acknowledgement failed")
		case failedSpanRetryDispatchIndeterminate:
			return werrors.NewServiceUnavailableError(
				fmt.Sprintf("Processing item retry dispatch state is indeterminate: %v", result.err))
		case failedSpanRetryDispatchNeedsCompensation:
			compensator, ok := s.tracker().(failedSpanRetryCompensator)
			if !ok {
				return werrors.NewServiceUnavailableError(
					"Failed to publish processing item retry; durable recovery remains pending")
			}
			compensationCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx), failedSpanRetryCompensationTimeout)
			defer cancel()
			if compensationErr := compensator.FailPreparedSpanRetry(compensationCtx, prepared,
				"PARTIAL_REPAIR_ENQUEUE_FAILED", result.err.Error()); compensationErr != nil {
				return werrors.NewServiceUnavailableError(
					fmt.Sprintf("Failed to publish processing item retry and record terminal failure: %v", compensationErr))
			}
			return werrors.NewServiceUnavailableError("Failed to enqueue processing item retry")
		default:
			return werrors.NewServiceUnavailableError("Processing item retry dispatch state is invalid")
		}
	})
	if err != nil {
		return err
	}
	return nil
}

type plannedAggregateRetry struct {
	row   *types.KnowledgeProcessingSpan
	state string
	fence *types.KnowledgeSpanRetryStallFence
}

type failedSpanAggregateRetryPlan struct {
	topology        failedSpanRetryTopology
	planned         []plannedAggregateRetry
	carryover       []plannedAggregateRetry
	selectedSpanIDs map[string]struct{}
	states          map[string]string
}

func (s *knowledgeService) planFailedSpanAggregateRetry(
	ctx context.Context, request types.KnowledgeSpanAggregateRetryRequest,
) (*failedSpanAggregateRetryPlan, string, error) {
	reader, ok := s.tracker().(failedSpanRetryCandidateReader)
	if !ok {
		return nil, types.KnowledgeSpanRetryStateUnknown, errors.New("retry candidates are unavailable")
	}
	rows, err := reader.ListFailedSpanRetryCandidates(ctx, request.KnowledgeID, request.Attempt)
	if err != nil {
		return nil, types.KnowledgeSpanRetryStateUnknown, err
	}
	topology := buildFailedSpanRetryTopology(rows, request.Attempt)
	latest := topology.latestTargets
	names := make([]string, 0, len(latest))
	for name := range latest {
		names = append(names, name)
	}
	sort.Strings(names)
	planned := make([]plannedAggregateRetry, 0, len(names))
	carryover := make([]plannedAggregateRetry, 0)
	selectedSpanIDs := make(map[string]struct{}, len(names))
	states := make(map[string]string, len(names))
	for _, name := range names {
		row := latest[name]
		single := types.KnowledgeSpanRetryRequest{
			KnowledgeID: request.KnowledgeID, Attempt: request.Attempt, SpanID: row.SpanID,
			ClientRequestID: request.ClientRequestID, Language: request.Language,
		}
		action, fence, evaluationErr := s.evaluateKnowledgeSpanRetry(ctx, single, nil, nil, true)
		if evaluationErr != nil {
			return nil, types.KnowledgeSpanRetryStateUnknown, evaluationErr
		}
		if action == nil || !action.Allowed {
			if terminalRetrySpanStatus(row.Status) {
				continue
			}
			if action == nil || action.State == types.KnowledgeSpanRetryStateUnknown {
				return nil, types.KnowledgeSpanRetryStateUnknown, nil
			}
			continue
		}
		if !retryOwnerReplayable(row) {
			// Legacy workers did not persist the full question batch payload.
			// A proven-stalled owner can still be terminalized under its lease
			// fence, but it must never be presented or published as replayable.
			if action.State == types.KnowledgeSpanRetryStateStalled && fence != nil {
				carryover = append(carryover, plannedAggregateRetry{row: row, state: action.State, fence: fence})
				selectedSpanIDs[row.SpanID] = struct{}{}
			}
			continue
		}
		planned = append(planned, plannedAggregateRetry{row: row, state: action.State, fence: fence})
		selectedSpanIDs[row.SpanID] = struct{}{}
		states[row.SpanID] = action.State
	}
	if state, blocked := failedSpanRetryTopologyBlock(topology, selectedSpanIDs); blocked {
		return nil, state, nil
	}
	if len(planned) == 0 {
		return nil, "no_retryable_targets", nil
	}
	return &failedSpanAggregateRetryPlan{topology: topology, planned: planned, carryover: carryover,
		selectedSpanIDs: selectedSpanIDs, states: states}, "", nil
}

// EvaluateKnowledgeSpanAggregateRetry is the read-only shared planner used by
// GET projection and POST mutation. It evaluates the entire latest topology as
// one selection, so independently stalled siblings may be retried together
// while any unselected active/unknown owner fails the aggregate closed.
func (s *knowledgeService) EvaluateKnowledgeSpanAggregateRetry(
	ctx context.Context, request types.KnowledgeSpanAggregateRetryRequest,
) (*types.KnowledgeSpanAggregateRetryAction, error) {
	action := &types.KnowledgeSpanAggregateRetryAction{}
	plan, blocked, err := s.planFailedSpanAggregateRetry(ctx, request)
	if err != nil {
		action.Reason = "liveness_read_failed"
		return action, err
	}
	if blocked != "" {
		if blocked == types.KnowledgeSpanRetryStateActive {
			action.Reason = "active_sibling"
		} else if blocked == "no_retryable_targets" {
			action.Reason = "no_retryable_targets"
		} else {
			action.Reason = "liveness_unavailable"
		}
		return action, nil
	}
	action.Allowed = true
	action.Targets = make([]types.KnowledgeSpanAggregateRetryTarget, 0, len(plan.planned))
	for _, candidate := range plan.planned {
		target := types.KnowledgeSpanAggregateRetryTarget{SourceSpanID: candidate.row.SpanID,
			Name: candidate.row.Name, State: candidate.state}
		action.Targets = append(action.Targets, target)
		switch {
		case candidate.row.Name == "postprocess.summary":
			action.Counts.Summary++
		case candidate.row.Name == "postprocess.wiki":
			action.Counts.Wiki++
		case isQuestionBatchRetryOwner(candidate.row.Name):
			action.Counts.Question++
		case strings.HasPrefix(candidate.row.Name, "postprocess.graph.chunk["):
			action.Counts.Graph++
		}
	}
	return action, nil
}

// RetryFailedKnowledgeSpans creates one partial-repair attempt containing the
// exact latest failed/stalled owners selected by the shared planner.
func (s *knowledgeService) RetryFailedKnowledgeSpans(
	ctx context.Context, request types.KnowledgeSpanAggregateRetryRequest,
) (*types.KnowledgeSpanAggregateRetryResult, error) {
	if reader, ok := s.tracker().(existingFailedSpanRetryPlanReader); ok {
		existing, readErr := reader.FindExistingFailedSpanRetryPlan(
			ctx, request.KnowledgeID, request.Attempt, request.ClientRequestID, "aggregate")
		if readErr != nil {
			if errors.Is(readErr, repository.ErrKnowledgeSpanRetryNotLatest) {
				return nil, werrors.NewConflictError("Client request id belongs to a different retry operation")
			}
			return nil, werrors.NewServiceUnavailableError("Existing processing item retry could not be validated")
		}
		if len(existing) > 0 {
			result := &types.KnowledgeSpanAggregateRetryResult{KnowledgeID: request.KnowledgeID,
				SourceAttempt: request.Attempt, ClientRequestID: request.ClientRequestID,
				Targets: make([]types.KnowledgeSpanAggregateRetryTarget, 0, len(existing))}
			for _, prepared := range existing {
				if prepared == nil || prepared.SourceAttempt != request.Attempt || prepared.SourceSpanID == "" {
					return nil, werrors.NewServiceUnavailableError("Existing processing item retry is invalid")
				}
				if result.Attempt == 0 {
					result.Attempt = prepared.Attempt
				} else if result.Attempt != prepared.Attempt {
					return nil, werrors.NewServiceUnavailableError("Existing processing item retry is split across attempts")
				}
				if err := s.publishPreparedFailedSpanRetry(ctx, prepared); err != nil {
					return nil, err
				}
				state := retryInputOptionalString(prepared.Input, "source_retry_state")
				if state != types.KnowledgeSpanRetryStateStalled {
					state = types.KnowledgeSpanRetryStateFailed
				}
				result.Targets = append(result.Targets, types.KnowledgeSpanAggregateRetryTarget{
					SourceSpanID: prepared.SourceSpanID, Name: prepared.Name, State: state,
					NewSpanID: prepared.SpanID, TaskID: prepared.TaskID})
			}
			return result, nil
		}
	}
	plan, blocked, err := s.planFailedSpanAggregateRetry(ctx, request)
	if err != nil || blocked == types.KnowledgeSpanRetryStateUnknown {
		return nil, werrors.NewServiceUnavailableError("Processing item liveness could not be verified")
	}
	if blocked == types.KnowledgeSpanRetryStateActive {
		return nil, werrors.NewConflictError("An unselected processing item is still active")
	}
	if plan == nil {
		return nil, werrors.NewConflictError("No failed or stalled processing items are currently retryable")
	}
	reader, ok := s.tracker().(failedSpanRetryCandidateReader)
	if !ok {
		return nil, werrors.NewServiceUnavailableError("Processing item retry candidates are unavailable")
	}
	leases := make([]*processingOwnerLease, 0, len(plan.planned)+len(plan.carryover))
	defer func() {
		for _, lease := range leases {
			releaseRetryRecoveryLease(lease)
		}
	}()
	targets := make([]types.KnowledgeSpanMultiRetryTarget, 0, len(plan.planned))
	carryoverFences := make([]*types.KnowledgeSpanRetryStallFence, 0, len(plan.carryover))
	leaseCandidates := make([]plannedAggregateRetry, 0, len(plan.planned)+len(plan.carryover))
	leaseCandidates = append(leaseCandidates, plan.planned...)
	leaseCandidates = append(leaseCandidates, plan.carryover...)
	sort.Slice(leaseCandidates, func(i, j int) bool {
		if leaseCandidates[i].row.Name == leaseCandidates[j].row.Name {
			return leaseCandidates[i].row.SpanID < leaseCandidates[j].row.SpanID
		}
		return leaseCandidates[i].row.Name < leaseCandidates[j].row.Name
	})
	for _, candidate := range leaseCandidates {
		single := types.KnowledgeSpanRetryRequest{
			KnowledgeID: request.KnowledgeID, Attempt: request.Attempt, SpanID: candidate.row.SpanID,
			ClientRequestID: request.ClientRequestID, Language: request.Language,
		}
		fence, lease, authErr := s.authorizeFailedSpanRetryMutationWithTopology(
			ctx, single, plan.selectedSpanIDs, true)
		if authErr != nil {
			// Every contender acquires the same sorted first stalled owner.
			// A loser must abort immediately rather than skip ahead and split
			// the remaining owner leases with the winner.
			return nil, authErr
		}
		if lease != nil {
			leases = append(leases, lease)
		}
		if !retryOwnerReplayable(candidate.row) {
			if fence == nil {
				return nil, werrors.NewServiceUnavailableError("Unreplayable stalled item could not be fenced")
			}
			carryoverFences = append(carryoverFences, fence)
			continue
		}
		targets = append(targets, types.KnowledgeSpanMultiRetryTarget{
			SpanID: candidate.row.SpanID, StallFence: fence,
		})
	}
	latestRows, err := reader.ListFailedSpanRetryCandidates(ctx, request.KnowledgeID, request.Attempt)
	if err != nil {
		return nil, werrors.NewServiceUnavailableError("Processing item retry topology could not be rechecked")
	}
	if state, blocked := failedSpanRetryTopologyBlock(
		buildFailedSpanRetryTopology(latestRows, request.Attempt), plan.selectedSpanIDs,
	); blocked {
		if state == types.KnowledgeSpanRetryStateUnknown {
			return nil, werrors.NewServiceUnavailableError("Processing item sibling liveness could not be verified")
		}
		return nil, werrors.NewConflictError("An unselected processing item is still active")
	}
	preparer, ok := s.tracker().(failedSpanMultiRetryPreparer)
	if !ok {
		return nil, werrors.NewServiceUnavailableError("Processing item retry is unavailable")
	}
	preparations, err := preparer.PrepareFailedSpanRetries(ctx, types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: request.KnowledgeID, Attempt: request.Attempt,
		ClientRequestID: request.ClientRequestID, Language: request.Language,
		RequestKind: "aggregate", Targets: targets, CarryoverFences: carryoverFences,
	})
	if err != nil {
		return nil, mapFailedSpanRetryError(err)
	}
	if len(preparations) != len(targets) {
		return nil, werrors.NewInternalServerError("Processing item retry returned an invalid preparation set")
	}
	result := &types.KnowledgeSpanAggregateRetryResult{
		KnowledgeID: request.KnowledgeID, SourceAttempt: request.Attempt,
		ClientRequestID: request.ClientRequestID,
		Targets:         make([]types.KnowledgeSpanAggregateRetryTarget, 0, len(preparations)),
	}
	for _, prepared := range preparations {
		if prepared == nil || prepared.SourceSpanID == "" {
			return nil, werrors.NewInternalServerError("Processing item retry returned an invalid preparation")
		}
		if result.Attempt == 0 {
			result.Attempt = prepared.Attempt
		} else if result.Attempt != prepared.Attempt {
			return nil, werrors.NewInternalServerError("Processing item retry split across attempts")
		}
		if err := s.publishPreparedFailedSpanRetry(ctx, prepared); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, types.KnowledgeSpanAggregateRetryTarget{
			SourceSpanID: prepared.SourceSpanID, Name: prepared.Name, State: plan.states[prepared.SourceSpanID],
			NewSpanID: prepared.SpanID, TaskID: prepared.TaskID,
		})
	}
	return result, nil
}

func (s *knowledgeService) dispatchFailedSpanRetryOutboxGuarded(
	ctx context.Context, prepared *types.KnowledgeSpanRetryPreparation,
) failedSpanRetryDispatchResult {
	if s.taskPendingRepo == nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchNeedsCompensation,
			err: errors.New("retry dispatch outbox repository is not configured")}
	}
	rows, err := s.taskPendingRepo.PeekBatch(ctx, types.KnowledgeSpanRetryOutboxTaskType,
		types.KnowledgeSpanRetryOutboxScope, prepared.KnowledgeID, 1000)
	if err != nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchNeedsCompensation,
			err: fmt.Errorf("load retry dispatch outbox: %w", err)}
	}
	var matched *types.TaskPendingOp
	for _, row := range rows {
		if row != nil && row.Op == types.KnowledgeSpanRetryOutboxOp && row.DedupKey == prepared.TaskID {
			matched = row
			break
		}
	}
	if matched == nil {
		return s.resolveMissingFailedSpanRetryOutbox(ctx, prepared)
	}
	outboxPrepared, err := DecodeFailedSpanRetryOutbox(matched)
	if err != nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchNeedsCompensation, err: err}
	}
	if outboxPrepared.KnowledgeID != prepared.KnowledgeID || outboxPrepared.Attempt != prepared.Attempt ||
		outboxPrepared.SpanID != prepared.SpanID || outboxPrepared.Name != prepared.Name ||
		outboxPrepared.TaskID != prepared.TaskID {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchNeedsCompensation,
			err: errors.New("retry dispatch outbox does not match committed preparation")}
	}
	if err := EnqueueFailedSpanRetry(ctx, s.task, outboxPrepared); err != nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchNeedsCompensation, err: err}
	}
	deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failedSpanRetryCompensationTimeout)
	defer cancel()
	if err := s.taskPendingRepo.DeleteByDedupKey(deleteCtx,
		types.KnowledgeSpanRetryOutboxTaskType, types.KnowledgeSpanRetryOutboxScope,
		prepared.KnowledgeID, prepared.TaskID, types.KnowledgeSpanRetryOutboxOp); err != nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchPublishedUnacked,
			err: fmt.Errorf("acknowledge retry dispatch outbox: %w", err)}
	}
	AcknowledgeFailedSpanRetryPublication(prepared.TaskID)
	return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchAcked}
}

func (s *knowledgeService) resolveMissingFailedSpanRetryOutbox(
	ctx context.Context, prepared *types.KnowledgeSpanRetryPreparation,
) failedSpanRetryDispatchResult {
	reader, ok := s.tracker().(failedSpanRetryExactReader)
	if !ok {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchIndeterminate,
			err: errors.New("exact retry target reader is unavailable")}
	}
	span, err := reader.GetPreparedSpanRetry(ctx, prepared.KnowledgeID, prepared.Attempt, prepared.SpanID)
	if err != nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchIndeterminate,
			err: fmt.Errorf("read exact retry target: %w", err)}
	}
	if span == nil {
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchIndeterminate,
			err: errors.New("exact retry target is missing")}
	}
	switch span.Status {
	case types.SpanStatusFailed:
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchPreviouslyFailed,
			message: span.ErrorMessage}
	case types.SpanStatusPending, types.SpanStatusRunning, types.SpanStatusDone:
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchAcked}
	default:
		return failedSpanRetryDispatchResult{state: failedSpanRetryDispatchIndeterminate,
			err: fmt.Errorf("exact retry target has non-acknowledged status %q", span.Status)}
	}
}

// DecodeFailedSpanRetryOutbox validates one durable retry-dispatch row and
// reconstructs the exact worker preparation committed with the new attempt.
func DecodeFailedSpanRetryOutbox(op *types.TaskPendingOp) (*types.KnowledgeSpanRetryPreparation, error) {
	if op == nil || op.TaskType != types.KnowledgeSpanRetryOutboxTaskType ||
		op.Scope != types.KnowledgeSpanRetryOutboxScope || op.ScopeID == "" ||
		op.Op != types.KnowledgeSpanRetryOutboxOp || op.DedupKey == "" {
		return nil, errors.New("invalid retry dispatch outbox row")
	}
	var payload types.KnowledgeSpanRetryOutboxPayload
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode retry dispatch outbox: %w", err)
	}
	prepared := &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: payload.KnowledgeID, Attempt: payload.Attempt, SpanID: payload.SpanID,
		Name: payload.TargetName, TaskID: payload.TaskID, Status: types.SpanStatusPending,
		TenantID: payload.TenantID, KnowledgeBaseID: payload.KnowledgeBaseID,
		Language: payload.Language, Input: payload.Input,
	}
	expectedTaskID, err := failedSpanRepairTaskID(prepared)
	if err != nil || prepared.KnowledgeID != op.ScopeID || prepared.TaskID != op.DedupKey ||
		prepared.TaskID != expectedTaskID || prepared.Attempt <= 0 || prepared.SpanID == "" {
		return nil, errors.New("retry dispatch outbox payload is invalid")
	}
	return prepared, nil
}

func failedSpanRepairTaskID(prepared *types.KnowledgeSpanRetryPreparation) (string, error) {
	if prepared == nil {
		return "", errors.New("retry preparation is nil")
	}
	switch {
	case prepared.Name == "postprocess.summary":
		return fmt.Sprintf("knowledge-fanout:%s:%d:summary", prepared.KnowledgeID, prepared.Attempt), nil
	case isQuestionBatchRetryOwner(prepared.Name):
		nameIndex, _ := questionBatchRetryIndex(prepared.Name)
		batchIndex, err := retryInputInt(prepared.Input, "batch_index")
		if err != nil || batchIndex != nameIndex {
			return "", fmt.Errorf("invalid question retry batch index")
		}
		return fmt.Sprintf("knowledge-fanout:%s:%d:question:%d",
			prepared.KnowledgeID, prepared.Attempt, batchIndex), nil
	case prepared.Name == "postprocess.wiki":
		return fmt.Sprintf("knowledge-fanout:%s:%d:wiki", prepared.KnowledgeID, prepared.Attempt), nil
	case strings.HasPrefix(prepared.Name, "postprocess.graph.chunk["):
		chunkIndex, err := retryInputInt(prepared.Input, "chunk_index")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("knowledge-fanout:%s:%d:graph:%d", prepared.KnowledgeID, prepared.Attempt, chunkIndex), nil
	default:
		return "", repository.ErrKnowledgeSpanRetryUnsupported
	}
}

// EnqueueFailedSpanRetry publishes a validated durable outbox payload. It is
// exported so startup recovery can replay committed rows after a process exit.
func EnqueueFailedSpanRetry(
	ctx context.Context, taskEnqueuer interfaces.TaskEnqueuer,
	prepared *types.KnowledgeSpanRetryPreparation,
) error {
	if taskEnqueuer == nil || prepared == nil {
		return errors.New("retry task enqueuer and preparation are required")
	}
	if _, published := failedSpanRetryPublished.Load(prepared.TaskID); published {
		return nil
	}
	language := strings.TrimSpace(prepared.Language)
	if language == "" {
		language = retryInputOptionalString(prepared.Input, "language")
	}
	if language == "" {
		language = types.LanguageFromContextOrDefault(ctx)
	}
	switch {
	case prepared.Name == "postprocess.summary":
		taskID := prepared.TaskID
		payload := types.SummaryGenerationPayload{
			TenantID: prepared.TenantID, KnowledgeBaseID: prepared.KnowledgeBaseID,
			KnowledgeID: prepared.KnowledgeID, Language: language, Attempt: prepared.Attempt,
		}
		langfuse.InjectTracing(ctx, &payload)
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		task := asynq.NewTask(types.TypeSummaryGeneration, payloadBytes,
			asynq.Queue(types.QueueSummary), asynq.MaxRetry(3), asynq.Timeout(30*time.Minute),
			asynq.Retention(failedSpanRetryTaskRetention))
		_, err = taskEnqueuer.Enqueue(task, asynq.TaskID(taskID))
		err = ignoreRetryTaskConflict(err)
		if err == nil {
			failedSpanRetryPublished.Store(prepared.TaskID, struct{}{})
		}
		return err

	case isQuestionBatchRetryOwner(prepared.Name):
		nameIndex, _ := questionBatchRetryIndex(prepared.Name)
		batchIndex, err := retryInputInt(prepared.Input, "batch_index")
		if err != nil || batchIndex != nameIndex {
			return errors.New("invalid question retry payload: batch index does not match target")
		}
		chunkIDs, err := retryInputStrings(prepared.Input, "chunk_ids")
		if err != nil {
			return fmt.Errorf("invalid question retry payload: %w", err)
		}
		questionCount, err := retryInputInt(prepared.Input, "question_count")
		if err != nil || questionCount <= 0 {
			return errors.New("invalid question retry payload: positive question_count is required")
		}
		payload := types.QuestionGenerationPayload{
			TenantID: prepared.TenantID, KnowledgeBaseID: prepared.KnowledgeBaseID,
			KnowledgeID: prepared.KnowledgeID, QuestionCount: questionCount,
			Language: language, Attempt: prepared.Attempt, ChunkIDs: chunkIDs,
			BatchIndex:  batchIndex,
			PrevChunkID: retryInputOptionalString(prepared.Input, "prev_chunk_id"),
			NextChunkID: retryInputOptionalString(prepared.Input, "next_chunk_id"),
		}
		langfuse.InjectTracing(ctx, &payload)
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		task := asynq.NewTask(types.TypeQuestionGeneration, payloadBytes,
			asynq.Queue(types.QueueQuestion), asynq.MaxRetry(3), asynq.Timeout(30*time.Minute),
			asynq.Retention(failedSpanRetryTaskRetention))
		_, err = taskEnqueuer.Enqueue(task, asynq.TaskID(prepared.TaskID))
		err = ignoreRetryTaskConflict(err)
		if err == nil {
			failedSpanRetryPublished.Store(prepared.TaskID, struct{}{})
		}
		return err

	case strings.HasPrefix(prepared.Name, "postprocess.graph.chunk["):
		chunkID := strings.TrimSpace(fmt.Sprint(prepared.Input["chunk_id"]))
		modelID := strings.TrimSpace(fmt.Sprint(prepared.Input["model_id"]))
		chunkIndex, err := retryInputInt(prepared.Input, "chunk_index")
		if err != nil {
			return fmt.Errorf("invalid graph retry payload: %w", err)
		}
		if chunkID == "" || modelID == "" {
			return errors.New("invalid graph retry payload: chunk_id and model_id are required")
		}
		if strings.ToLower(os.Getenv("NEO4J_ENABLE")) != "true" {
			return errors.New("graph extraction task was not enqueued")
		}
		payload := types.ExtractChunkPayload{TenantID: prepared.TenantID, ChunkID: chunkID,
			ModelID: modelID, KnowledgeID: prepared.KnowledgeID, Attempt: prepared.Attempt,
			ChunkIndex: chunkIndex}
		langfuse.InjectTracing(ctx, &payload)
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		task := asynq.NewTask(types.TypeChunkExtract, payloadBytes,
			asynq.Queue(types.QueueGraph), asynq.MaxRetry(3), asynq.Timeout(30*time.Minute),
			asynq.Retention(failedSpanRetryTaskRetention))
		_, err = taskEnqueuer.Enqueue(task, asynq.TaskID(prepared.TaskID))
		err = ignoreRetryTaskConflict(err)
		if err == nil {
			failedSpanRetryPublished.Store(prepared.TaskID, struct{}{})
		}
		return err

	case prepared.Name == "postprocess.wiki":
		taskID := prepared.TaskID
		payload := WikiIngestPayload{TenantID: prepared.TenantID,
			KnowledgeBaseID: prepared.KnowledgeBaseID, Language: language}
		langfuse.InjectTracing(ctx, &payload)
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		task := asynq.NewTask(types.TypeWikiIngest, payloadBytes,
			asynq.Queue(types.QueueWiki), asynq.MaxRetry(wikiIngestMaxRetry),
			asynq.Timeout(WikiIngestTaskTimeout), asynq.ProcessIn(wikiIngestDelay),
			asynq.Retention(failedSpanRetryTaskRetention))
		_, err = taskEnqueuer.Enqueue(task, asynq.TaskID(taskID))
		err = ignoreRetryTaskConflict(err)
		if err == nil {
			failedSpanRetryPublished.Store(prepared.TaskID, struct{}{})
		}
		return err
	default:
		return repository.ErrKnowledgeSpanRetryUnsupported
	}
}

// AcknowledgeFailedSpanRetryPublication releases the Lite-mode publication
// marker after the durable outbox row has been consumed. If acknowledgement
// fails, callers deliberately keep both row and marker so a later cycle retries
// only the DB delete instead of publishing the in-memory task twice.
func AcknowledgeFailedSpanRetryPublication(taskID string) {
	failedSpanRetryPublished.Delete(taskID)
}

func ignoreRetryTaskConflict(err error) error {
	if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
		return nil
	}
	return err
}
