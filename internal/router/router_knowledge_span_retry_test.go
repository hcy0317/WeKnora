package router

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type retryRouteKnowledgeService struct {
	interfaces.KnowledgeService
	knowledge *types.Knowledge
	calls     int
	rowCalls  int
}

func (s *retryRouteKnowledgeService) RetryFailedKnowledgeSpan(
	_ context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	s.rowCalls++
	return &types.KnowledgeSpanRetryPreparation{
		KnowledgeID: request.KnowledgeID, SourceAttempt: request.Attempt, SourceSpanID: request.SpanID,
		ClientRequestID: request.ClientRequestID, Attempt: request.Attempt + 1,
		SpanID: "new-span", Name: "postprocess.summary", TaskID: "knowledge-fanout:kid:5:summary",
	}, nil
}

type retryRouteKBService struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *retryRouteKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func (s *retryRouteKnowledgeService) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.knowledge, nil
}

func (s *retryRouteKnowledgeService) GetOwningKBCreatorID(context.Context, string) (string, error) {
	return "editor-user", nil
}

func (s *retryRouteKnowledgeService) RetryFailedKnowledgeSpans(
	_ context.Context, request types.KnowledgeSpanAggregateRetryRequest,
) (*types.KnowledgeSpanAggregateRetryResult, error) {
	s.calls++
	return &types.KnowledgeSpanAggregateRetryResult{
		KnowledgeID: request.KnowledgeID, SourceAttempt: request.Attempt,
		ClientRequestID: request.ClientRequestID, Attempt: request.Attempt + 1,
	}, nil
}

func newKnowledgeRetryRouteEngine(t *testing.T, role types.TenantRole, userID string, shared bool) (*gin.Engine, *retryRouteKnowledgeService) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	enabled := true
	tenantID := uint64(1)
	creatorID := "editor-user"
	var share interfaces.KBShareService
	if shared {
		tenantID = 2
		creatorID = "source-owner"
		share = &downloadKBShareStub{permission: types.OrgRoleEditor, source: 2}
	}
	kg := &retryRouteKnowledgeService{knowledge: &types.Knowledge{ID: "kid", TenantID: tenantID, KnowledgeBaseID: "kb"}}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: tenantID, CreatorID: creatorID}
	kbSvc := &retryRouteKBService{kb: kb}
	cfg := &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enabled}}
	h := handler.NewKnowledgeHandler(cfg, kg, kbSvc, share, nil, nil, nil)
	guards := newRBACGuards(cfg, nil, nil, h, nil, nil, kbSvc, kg, nil, share, nil)
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(1))
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		ctx = context.WithValue(ctx, types.UserIDContextKey, userID)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Set(types.UserIDContextKey.String(), userID)
		c.Next()
	})
	RegisterKnowledgeRoutes(r.Group("/api/v1"), h, guards)
	return r, kg
}

func TestKnowledgeAggregateRetryPermissionMatrix(t *testing.T) {
	for _, test := range []struct {
		name   string
		role   types.TenantRole
		userID string
		status int
		calls  int
		shared bool
	}{
		{name: "viewer forbidden", role: types.TenantRoleViewer, userID: "editor-user", status: http.StatusForbidden},
		{name: "editor owner accepted", role: types.TenantRoleContributor, userID: "editor-user", status: http.StatusAccepted, calls: 1},
		{name: "shared editor accepted", role: types.TenantRoleContributor, userID: "org-editor", status: http.StatusAccepted, calls: 1, shared: true},
		{name: "admin accepted", role: types.TenantRoleAdmin, userID: "admin-user", status: http.StatusAccepted, calls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, service := newKnowledgeRetryRouteEngine(t, test.role, test.userID, test.shared)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/knowledge/kid/attempts/4/retry-failed",
				bytes.NewBufferString(`{"client_request_id":"request-1"}`))
			request.Header.Set("Content-Type", "application/json")

			engine.ServeHTTP(recorder, request)

			require.Equal(t, test.status, recorder.Code, "body=%s", recorder.Body.String())
			require.Equal(t, test.calls, service.calls)
		})
	}
}

func TestKnowledgeRowRetryPermissionMatrix(t *testing.T) {
	for _, test := range []struct {
		name   string
		role   types.TenantRole
		userID string
		status int
		calls  int
		shared bool
	}{
		{name: "viewer forbidden", role: types.TenantRoleViewer, userID: "editor-user", status: http.StatusForbidden},
		{name: "editor owner accepted", role: types.TenantRoleContributor, userID: "editor-user", status: http.StatusAccepted, calls: 1},
		{name: "shared editor accepted", role: types.TenantRoleContributor, userID: "org-editor", status: http.StatusAccepted, calls: 1, shared: true},
		{name: "admin accepted", role: types.TenantRoleAdmin, userID: "admin-user", status: http.StatusAccepted, calls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, service := newKnowledgeRetryRouteEngine(t, test.role, test.userID, test.shared)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost,
				"/api/v1/knowledge/kid/attempts/4/spans/source/retry",
				bytes.NewBufferString(`{"client_request_id":"request-1"}`))
			request.Header.Set("Content-Type", "application/json")

			engine.ServeHTTP(recorder, request)

			require.Equal(t, test.status, recorder.Code, "body=%s", recorder.Body.String())
			require.Equal(t, test.calls, service.rowCalls)
		})
	}
}
