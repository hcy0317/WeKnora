package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestListParserEnginesReportsManagedStoppedPaddleAsStandbyWithoutProbe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("WEKNORA_ENGINE_GATEWAY_URL", "http://engine-gateway:18084")
	fake := &fakeEngineLifecycleClient{
		config: testEngineLifecycleConfig(),
		statuses: map[lifecycle.Group]lifecycle.GroupSnapshot{
			lifecycle.GroupPaddleOCR: {Group: lifecycle.GroupPaddleOCR, State: lifecycle.StateStopped},
		},
	}
	probeCalls := 0
	handler := &SystemHandler{
		engineLifecycleClient: fake,
		paddleOCRVLProbe: func(context.Context, string) (bool, string) {
			probeCalls++
			return false, "unexpected probe"
		},
	}

	for i := 0; i < 10; i++ {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Set(types.TenantInfoContextKey.String(), &types.Tenant{ParserEngineConfig: &types.ParserEngineConfig{
			PaddleOCRVLEndpoint: "http://paddleocr-vl:8080",
		}})
		ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/system/parser-engines", nil)
		handler.ListParserEngines(ctx)

		require.Equal(t, http.StatusOK, recorder.Code)
		engine := parserEngineFromResponse(t, recorder, docparser.PaddleOCRVLEngineName)
		require.True(t, engine.Available)
		require.Equal(t, types.ParserEngineStateStandby, engine.State)
	}
	require.Zero(t, probeCalls)
}

func TestCheckParserEnginesActivelyProbesManagedPaddleWithinStartupBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("WEKNORA_ENGINE_GATEWAY_URL", "http://engine-gateway:18084")
	config := testEngineLifecycleConfig()
	startupSeconds := 2
	group := config.Groups[lifecycle.GroupPaddleOCR]
	group.StartupTimeoutSeconds = &startupSeconds
	config.Groups[lifecycle.GroupPaddleOCR] = group

	probeCalls := 0
	var remaining time.Duration
	handler := &SystemHandler{
		engineLifecycleClient: &fakeEngineLifecycleClient{config: config},
		paddleOCRVLProbe: func(ctx context.Context, endpoint string) (bool, string) {
			probeCalls++
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			remaining = time.Until(deadline)
			require.Equal(t, "http://engine-gateway:18084/paddleocr", endpoint)
			return true, ""
		},
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/system/parser-engines/check", strings.NewReader(`{
		"paddleocr_vl_endpoint":"http://paddleocr-vl:8080"
	}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	handler.CheckParserEngines(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 1, probeCalls)
	require.Greater(t, remaining, 6*time.Second)
	require.LessOrEqual(t, remaining, 7*time.Second)
	engine := parserEngineFromResponse(t, recorder, docparser.PaddleOCRVLEngineName)
	require.True(t, engine.Available)
	require.Equal(t, types.ParserEngineStateReady, engine.State)
}

func parserEngineFromResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	name string,
) types.ParserEngineInfo {
	t.Helper()
	var response struct {
		Data []types.ParserEngineInfo `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	for _, engine := range response.Data {
		if engine.Name == name {
			return engine
		}
	}
	t.Fatalf("parser engine %q not found in %s", name, recorder.Body.String())
	return types.ParserEngineInfo{}
}
