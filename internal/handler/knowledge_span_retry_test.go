package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type retryHandlerKnowledgeService struct {
	interfaces.KnowledgeService
	request            types.KnowledgeSpanRetryRequest
	result             *types.KnowledgeSpanRetryPreparation
	aggregateResult    *types.KnowledgeSpanAggregateRetryResult
	aggregateRequest   types.KnowledgeSpanAggregateRetryRequest
	aggregateErr       error
	action             *types.KnowledgeSpanRetryAction
	actionErr          error
	aggregateAction    *types.KnowledgeSpanAggregateRetryAction
	aggregateActionErr error
	evaluationCalls    int
}

func (s *retryHandlerKnowledgeService) RetryFailedKnowledgeSpans(
	_ context.Context, request types.KnowledgeSpanAggregateRetryRequest,
) (*types.KnowledgeSpanAggregateRetryResult, error) {
	s.aggregateRequest = request
	return s.aggregateResult, s.aggregateErr
}

func (s *retryHandlerKnowledgeService) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	return &types.Knowledge{ID: id, TenantID: 7, KnowledgeBaseID: "kb"}, nil
}

func (s *retryHandlerKnowledgeService) RetryFailedKnowledgeSpan(
	_ context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	s.request = request
	return s.result, nil
}

func (s *retryHandlerKnowledgeService) EvaluateKnowledgeSpanRetry(
	_ context.Context, _ types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryAction, *types.KnowledgeSpanRetryStallFence, error) {
	s.evaluationCalls++
	return s.action, nil, s.actionErr
}

func (s *retryHandlerKnowledgeService) EvaluateKnowledgeSpanAggregateRetry(
	_ context.Context, _ types.KnowledgeSpanAggregateRetryRequest,
) (*types.KnowledgeSpanAggregateRetryAction, error) {
	return s.aggregateAction, s.aggregateActionErr
}

func TestRetryKnowledgeSpanReturnsAcceptedPartialRepair(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &retryHandlerKnowledgeService{result: &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: "kid", SourceAttempt: 4, SourceSpanID: "failed-span",
		ClientRequestID: "request-1", Attempt: 5, SpanID: "new-span",
		Name: "postprocess.wiki", TaskID: "knowledge-fanout:kid:5:wiki", Status: types.SpanStatusPending,
	}}
	h := &KnowledgeHandler{kgService: svc}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/knowledge/kid/attempts/4/spans/failed-span/retry",
		bytes.NewBufferString(`{"client_request_id":"request-1"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: "kid"}, {Key: "attempt", Value: "4"}, {Key: "span_id", Value: "failed-span"}}
	c.Set(types.TenantIDContextKey.String(), uint64(7))

	h.RetryKnowledgeSpan(c)

	require.Empty(t, c.Errors)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "kid", svc.request.KnowledgeID)
	require.Equal(t, 4, svc.request.Attempt)
	require.Equal(t, "failed-span", svc.request.SpanID)
	require.Equal(t, "request-1", svc.request.ClientRequestID)
	var response struct {
		Success bool                                `json:"success"`
		Data    types.KnowledgeSpanRetryPreparation `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.True(t, response.Success)
	require.Equal(t, 5, response.Data.Attempt)
	require.Equal(t, "new-span", response.Data.SpanID)
	require.Equal(t, "postprocess.wiki", response.Data.Name)
	require.Equal(t, "knowledge-fanout:kid:5:wiki", response.Data.TaskID)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &wire))
	data := wire["data"].(map[string]any)
	require.EqualValues(t, 5, data["new_attempt"])
	require.Equal(t, "new-span", data["new_span_id"])
	require.Equal(t, "postprocess.wiki", data["target_name"])
	require.NotContains(t, data, "attempt")
	require.NotContains(t, data, "span_id")
}

func TestRetryKnowledgeSpansReturnsAcceptedAggregatePartialRepair(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &retryHandlerKnowledgeService{aggregateResult: &types.KnowledgeSpanAggregateRetryResult{
		KnowledgeID: "kid", SourceAttempt: 4, ClientRequestID: "aggregate-1", Attempt: 5,
		Targets: []types.KnowledgeSpanAggregateRetryTarget{{
			SourceSpanID: "question", Name: "postprocess.question.batch[3]", State: "failed",
			NewSpanID: "new-question", TaskID: "knowledge-fanout:kid:5:question:3",
		}},
	}}
	h := &KnowledgeHandler{kgService: svc}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/knowledge/kid/attempts/4/retry-failed",
		bytes.NewBufferString(`{"client_request_id":"aggregate-1"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: "kid"}, {Key: "attempt", Value: "4"}}
	c.Set(types.TenantIDContextKey.String(), uint64(7))

	h.RetryKnowledgeSpans(c)

	require.Empty(t, c.Errors)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "aggregate-1", svc.aggregateRequest.ClientRequestID)
	var response struct {
		Success bool                                    `json:"success"`
		Data    types.KnowledgeSpanAggregateRetryResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.True(t, response.Success)
	require.Equal(t, 5, response.Data.Attempt)
	require.Len(t, response.Data.Targets, 1)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &wire))
	target := wire["data"].(map[string]any)["targets"].([]any)[0].(map[string]any)
	require.Equal(t, "question", target["source_span_id"])
	require.Equal(t, "postprocess.question.batch[3]", target["target_name"])
	require.Equal(t, "failed", target["state"])
	require.Equal(t, "new-question", target["new_span_id"])
	require.Equal(t, "knowledge-fanout:kid:5:question:3", target["task_id"])
}

func TestRetryKnowledgeSpansPreservesSafeConflictAndUnavailableErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code int
	}{
		{name: "conflict", err: apperrors.NewConflictError("state changed"), code: http.StatusConflict},
		{name: "unavailable", err: apperrors.NewServiceUnavailableError("queue unavailable"), code: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &KnowledgeHandler{kgService: &retryHandlerKnowledgeService{aggregateErr: test.err}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"client_request_id":"request"}`))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Params = gin.Params{{Key: "id", Value: "kid"}, {Key: "attempt", Value: "4"}}
			c.Set(types.TenantIDContextKey.String(), uint64(7))

			h.RetryKnowledgeSpans(c)

			require.Len(t, c.Errors, 1)
			appErr, ok := apperrors.IsAppError(c.Errors[0].Err)
			require.True(t, ok)
			require.Equal(t, test.code, appErr.HTTPCode)
		})
	}
}

func TestAggregateKnowledgeSpanRetryActionWireContract(t *testing.T) {
	tree := &types.SpanTreeNode{KnowledgeProcessingSpan: types.KnowledgeProcessingSpan{
		Attempt: 4, SpanID: "root", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed,
	}}
	post := &types.SpanTreeNode{KnowledgeProcessingSpan: types.KnowledgeProcessingSpan{
		Attempt: 4, SpanID: "post", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed,
	}}
	tree.Children = []*types.SpanTreeNode{post}
	for i, target := range []struct{ id, name, state string }{
		{"summary", "postprocess.summary", "failed"},
		{"wiki", "postprocess.wiki", "stalled"},
		{"graph", "postprocess.graph.chunk[3]", "failed"},
		{"question", "postprocess.question.batch[3]", "failed"},
	} {
		node := &types.SpanTreeNode{KnowledgeProcessingSpan: types.KnowledgeProcessingSpan{
			ID: int64(i + 1), Attempt: 4, SpanID: target.id, Name: target.name, Kind: types.SpanKindSubSpan,
			Status: types.SpanStatusFailed, RetryAction: &types.KnowledgeSpanRetryAction{Allowed: true, State: target.state, Target: target.name},
		}}
		if target.id == "question" {
			post.Children = append(post.Children, &types.SpanTreeNode{KnowledgeProcessingSpan: types.KnowledgeProcessingSpan{
				Attempt: 4, SpanID: "question-group", Name: "postprocess.question", Kind: types.SpanKindSubSpan,
				Status: types.SpanStatusFailed,
			}, Children: []*types.SpanTreeNode{node}})
		} else {
			post.Children = append(post.Children, node)
		}
	}
	action := aggregateKnowledgeSpanRetryAction(tree, 4, 4)
	wire, err := json.Marshal(action)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(wire, &decoded))
	require.Equal(t, true, decoded["allowed"])
	counts := decoded["counts"].(map[string]any)
	require.EqualValues(t, 1, counts["summary"])
	require.EqualValues(t, 1, counts["wiki"])
	require.EqualValues(t, 1, counts["graph"])
	require.EqualValues(t, 1, counts["question"])
	targets := decoded["targets"].([]any)
	require.Len(t, targets, 4)
	for _, item := range targets {
		target := item.(map[string]any)
		require.Contains(t, target, "source_span_id")
		require.Contains(t, target, "target_name")
		require.Contains(t, target, "state")
		require.NotContains(t, target, "new_span_id")
		require.NotContains(t, target, "task_id")
	}
}

func TestAnnotateKnowledgeSpanRetryActionsAuthorizesOnlyLatestFailedOwner(t *testing.T) {
	now := time.Now()
	finished := now.Add(time.Second)
	rows := []types.KnowledgeProcessingSpan{
		{ID: 1, KnowledgeID: "kid", Attempt: 4, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki-old", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 4, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki-new", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 5, KnowledgeID: "kid", Attempt: 4, SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 6, KnowledgeID: "kid", Attempt: 4, SpanID: "question", ParentSpanID: "question-group", Name: "postprocess.question.batch[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
	}
	tree, _, _ := buildSpanTree("kid", 4, rows, types.ParseStatusFailed)
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleContributor)
	annotateKnowledgeSpanRetryActions(ctx, nil, tree, 4, 4, rows, true, nil)

	var post *types.SpanTreeNode
	for _, child := range tree.Children {
		if child.Name == types.StagePostProcess {
			post = child
			break
		}
	}
	require.NotNil(t, post)
	actions := map[string]*types.KnowledgeSpanRetryAction{}
	var collect func(*types.SpanTreeNode)
	collect = func(node *types.SpanTreeNode) {
		actions[node.SpanID] = node.RetryAction
		for _, child := range node.Children {
			collect(child)
		}
	}
	collect(post)
	require.False(t, actions["wiki-old"].Allowed)
	require.Equal(t, "superseded_retry", actions["wiki-old"].Reason)
	require.True(t, actions["wiki-new"].Allowed)
	require.Equal(t, "postprocess.wiki", actions["wiki-new"].Target)
	require.True(t, actions["question"].Allowed)
	require.Equal(t, "postprocess.question.batch[0]", actions["question"].Target)

	viewerTree, _, _ := buildSpanTree("kid", 4, rows, types.ParseStatusFailed)
	annotateKnowledgeSpanRetryActions(context.Background(), nil, viewerTree, 4, 4, rows, false, nil)
	viewerAction := aggregateKnowledgeSpanRetryAction(viewerTree, 4, 4)
	require.False(t, viewerAction.Allowed)
	require.Equal(t, "insufficient_permission", viewerAction.Reason)
}

func TestAnnotateKnowledgeSpanRetryActionsUsesAuthoritativeStalledState(t *testing.T) {
	now := time.Now()
	rows := []types.KnowledgeProcessingSpan{
		{ID: 1, KnowledgeID: "kid", Attempt: 4, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: now},
	}
	tree, _, _ := buildSpanTree("kid", 4, rows, types.ParseStatusFinalizing)
	svc := &retryHandlerKnowledgeService{action: &types.KnowledgeSpanRetryAction{
		Allowed: true, Target: "postprocess.summary", State: types.KnowledgeSpanRetryStateStalled,
	}}
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleContributor)
	annotateKnowledgeSpanRetryActions(ctx, svc, tree, 4, 4, rows, true, nil)

	var got *types.KnowledgeSpanRetryAction
	for _, stage := range tree.Children {
		for _, child := range stage.Children {
			if child.SpanID == "summary" {
				got = child.RetryAction
			}
		}
	}
	require.NotNil(t, got)
	require.True(t, got.Allowed)
	require.Equal(t, types.KnowledgeSpanRetryStateStalled, got.State)
}

func TestKnowledgeSpanRetryProjectionRequiresEffectiveEditorPermission(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now()
	finished := now.Add(time.Second)
	rows := []types.KnowledgeProcessingSpan{
		{ID: 1, KnowledgeID: "kid", Attempt: 4, SpanID: "root", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "post", ParentSpanID: "root",
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			StartedAt: &now, FinishedAt: &finished},
	}
	for _, test := range []struct {
		name       string
		role       types.TenantRole
		permission types.OrgMemberRole
		apiScope   *types.TenantAPIKeyScope
		want       bool
	}{
		{name: "shared viewer", role: types.TenantRoleContributor, permission: types.OrgRoleViewer},
		{name: "shared editor", role: types.TenantRoleContributor, permission: types.OrgRoleEditor, want: true},
		{name: "creator tenant viewer", role: types.TenantRoleViewer, permission: types.OrgRoleAdmin},
		{name: "ingest api key", role: types.TenantRoleViewer, permission: types.OrgRoleEditor,
			apiScope: &types.TenantAPIKeyScope{Capabilities: types.StringArray{string(types.APIKeyCapabilityIngest)}}, want: true},
		{name: "retrieve-only api key", role: types.TenantRoleViewer, permission: types.OrgRoleEditor,
			apiScope: &types.TenantAPIKeyScope{Capabilities: types.StringArray{string(types.APIKeyCapabilityRetrieve)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, test.role)
			if test.apiScope != nil {
				ctx = types.WithTenantAPIKeyScope(ctx, *test.apiScope)
			}
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			c.Set(middleware.KBAccessContextKey, &middleware.KBAccess{Permission: test.permission})
			tree, _, _ := buildSpanTree("kid", 4, rows, types.ParseStatusFailed)
			eligible := knowledgeSpanRetryMutationEligible(c)
			annotateKnowledgeSpanRetryActions(ctx, nil, tree, 4, 4, rows, eligible, nil)
			action := aggregateKnowledgeSpanRetryAction(tree, 4, 4)
			require.Equal(t, test.want, eligible)
			require.Equal(t, test.want, action.Allowed)
			if !test.want {
				require.Equal(t, "insufficient_permission", action.Reason)
			}
		})
	}
}

func TestKnowledgeSpanRetryProjectionUsesOneSharedTopologyPlan(t *testing.T) {
	now := time.Now()
	rows := []types.KnowledgeProcessingSpan{
		{ID: 1, KnowledgeID: "kid", Attempt: 4, SpanID: "root", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 2, KnowledgeID: "kid", Attempt: 4, SpanID: "post", ParentSpanID: "root",
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 3, KnowledgeID: "kid", Attempt: 4, SpanID: "summary", ParentSpanID: "post",
			Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, UpdatedAt: now},
		{ID: 4, KnowledgeID: "kid", Attempt: 4, SpanID: "wiki", ParentSpanID: "post",
			Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 5, KnowledgeID: "kid", Attempt: 4, SpanID: "graph", ParentSpanID: "post",
			Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 6, KnowledgeID: "kid", Attempt: 4, SpanID: "question-group", ParentSpanID: "post",
			Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: now},
		{ID: 7, KnowledgeID: "kid", Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, UpdatedAt: now},
	}
	projection := &types.KnowledgeSpanAggregateRetryAction{Allowed: true,
		Counts: types.KnowledgeSpanAggregateRetryCounts{Summary: 1, Wiki: 1, Graph: 1, Question: 1},
		Targets: []types.KnowledgeSpanAggregateRetryTarget{
			{SourceSpanID: "summary", Name: "postprocess.summary", State: types.KnowledgeSpanRetryStateFailed},
			{SourceSpanID: "wiki", Name: "postprocess.wiki", State: types.KnowledgeSpanRetryStateStalled},
			{SourceSpanID: "graph", Name: "postprocess.graph.chunk[3]", State: types.KnowledgeSpanRetryStateStalled},
			{SourceSpanID: "question", Name: "postprocess.question.batch[3]", State: types.KnowledgeSpanRetryStateStalled},
		}}
	for _, test := range []struct {
		name       string
		projection *types.KnowledgeSpanAggregateRetryAction
		want       int
		wantReason string
	}{
		{name: "failed and stalled selection", projection: projection, want: 4},
		{name: "active sibling blocks whole projection",
			projection: &types.KnowledgeSpanAggregateRetryAction{Reason: "active_sibling"}, wantReason: "active_sibling"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tree, _, _ := buildSpanTree("kid", 4, rows, types.ParseStatusFinalizing)
			svc := &retryHandlerKnowledgeService{action: &types.KnowledgeSpanRetryAction{Allowed: true}}
			annotateKnowledgeSpanRetryActions(context.Background(), svc, tree, 4, 4, rows, true, test.projection)
			if test.projection.Allowed {
				derived := aggregateKnowledgeSpanRetryAction(tree, 4, 4)
				require.Equal(t, test.want, len(derived.Targets))
			}
			require.Equal(t, test.want, len(test.projection.Targets))
			require.Equal(t, test.wantReason, test.projection.Reason)
			require.Zero(t, svc.evaluationCalls, "one shared projection must avoid per-row liveness replanning")
		})
	}
}

func TestRetryKnowledgeSpanRejectsMissingClientRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &KnowledgeHandler{kgService: &retryHandlerKnowledgeService{}}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: "kid"}, {Key: "attempt", Value: "4"}, {Key: "span_id", Value: "failed-span"}}

	h.RetryKnowledgeSpan(c)

	require.Len(t, c.Errors, 1)
}
