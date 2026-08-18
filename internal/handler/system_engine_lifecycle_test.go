package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/engine/adminclient"
	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type fakeEngineLifecycleClient struct {
	config           *lifecycle.Config
	statuses         map[lifecycle.Group]lifecycle.GroupSnapshot
	updated          *lifecycle.Config
	updateErr        error
	expectedRevision uint64
	candidate        lifecycle.Config
}

func (f *fakeEngineLifecycleClient) GetConfig(context.Context) (*lifecycle.Config, error) {
	return f.config, nil
}

func (f *fakeEngineLifecycleClient) UpdateConfig(
	_ context.Context,
	expectedRevision uint64,
	candidate lifecycle.Config,
) (*lifecycle.Config, error) {
	f.expectedRevision = expectedRevision
	f.candidate = candidate
	return f.updated, f.updateErr
}

func TestUpdateEngineLifecycleOverlaysOnlyEditablePolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	current := testEngineLifecycleConfig()
	updated := *current
	updated.Revision = 8
	fake := &fakeEngineLifecycleClient{config: current, updated: &updated}
	audit := &capturingAuditService{}
	handler := &SystemHandler{engineLifecycleClient: fake, auditSvc: audit}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/api/v1/system/admin/engine-lifecycle", strings.NewReader(`{
		"defaults":{"idle_minutes":15,"startup_timeout_seconds":90},
		"groups":{
			"paddleocr":{"mode":"always_on"},
			"asr":{"mode":"on_demand","idle_minutes":2},
			"reranker":{"mode":"on_demand","startup_timeout_seconds":180}
		}
	}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("If-Match", `"7"`)
	handler.UpdateEngineLifecycle(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, `"8"`, recorder.Header().Get("ETag"))
	require.Equal(t, uint64(7), fake.expectedRevision)
	require.Equal(t, 15, fake.candidate.Defaults.IdleMinutes)
	require.Equal(t, 90, fake.candidate.Defaults.StartupTimeoutSeconds)
	require.Equal(t, current.Defaults.FailureCooldownMinutes, fake.candidate.Defaults.FailureCooldownMinutes)
	require.Equal(t, current.Catalog, fake.candidate.Catalog)
	require.Equal(t, current.Controller, fake.candidate.Controller)
	require.Equal(t, lifecycle.ModeAlwaysOn, fake.candidate.Groups[lifecycle.GroupPaddleOCR].Mode)
	require.Equal(t, 2, *fake.candidate.Groups[lifecycle.GroupASR].IdleMinutes)
	require.Len(t, audit.entries, 1)
	require.Equal(t, "engine_lifecycle_config", audit.entries[0].TargetType)
	require.NotContains(t, string(audit.entries[0].Details), "docker_executable")
	require.NotContains(t, string(audit.entries[0].Details), "containers")
}

func TestUpdateEngineLifecycleReturnsLatestRevisionOnConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fake := &fakeEngineLifecycleClient{
		config: testEngineLifecycleConfig(),
		updateErr: &adminclient.HTTPError{
			StatusCode: http.StatusConflict,
			Message:    "revision changed",
			Revision:   9,
		},
	}
	handler := &SystemHandler{engineLifecycleClient: fake}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/api/v1/system/admin/engine-lifecycle", strings.NewReader(`{
		"defaults":{"idle_minutes":10,"startup_timeout_seconds":120},
		"groups":{
			"paddleocr":{"mode":"on_demand"},
			"asr":{"mode":"on_demand"},
			"reranker":{"mode":"on_demand"}
		}
	}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("If-Match", `"7"`)
	handler.UpdateEngineLifecycle(ctx)

	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Equal(t, `"9"`, recorder.Header().Get("ETag"))
	require.Contains(t, recorder.Body.String(), `"revision":9`)
}

func TestUpdateEngineLifecycleRejectsReadonlyOrUnknownFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fake := &fakeEngineLifecycleClient{}
	handler := &SystemHandler{engineLifecycleClient: fake}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/api/v1/system/admin/engine-lifecycle", strings.NewReader(`{
		"defaults":{"idle_minutes":10,"startup_timeout_seconds":120},
		"groups":{},
		"catalog":{"docker_host":"npipe:////./pipe/dockerDesktopLinuxEngine"}
	}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("If-Match", `"7"`)
	handler.UpdateEngineLifecycle(ctx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "unknown field")
}

func (f *fakeEngineLifecycleClient) GetGroupStatus(
	_ context.Context,
	group lifecycle.Group,
) (lifecycle.GroupSnapshot, error) {
	return f.statuses[group], nil
}

func TestGetEngineLifecycleReturnsOnlyEditablePolicyAndRuntimeStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &SystemHandler{engineLifecycleClient: &fakeEngineLifecycleClient{
		config: testEngineLifecycleConfig(),
		statuses: map[lifecycle.Group]lifecycle.GroupSnapshot{
			lifecycle.GroupPaddleOCR: {Group: lifecycle.GroupPaddleOCR, State: lifecycle.StateStopped},
			lifecycle.GroupASR:       {Group: lifecycle.GroupASR, State: lifecycle.StateBusy, Active: 1},
			lifecycle.GroupReranker:  {Group: lifecycle.GroupReranker, State: lifecycle.StateReady},
		},
	}}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/system/admin/engine-lifecycle", nil)
	handler.GetEngineLifecycle(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, `"7"`, recorder.Header().Get("ETag"))
	var response map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, true, response["controller_online"])
	require.NotContains(t, recorder.Body.String(), "docker_executable")
	require.NotContains(t, recorder.Body.String(), "containers")
	require.Contains(t, recorder.Body.String(), `"state":"busy"`)
}

func testEngineLifecycleConfig() *lifecycle.Config {
	idle := 4
	return &lifecycle.Config{
		SchemaVersion: lifecycle.CurrentSchemaVersion,
		Revision:      7,
		Defaults: lifecycle.DefaultsConfig{
			IdleMinutes:            10,
			StartupTimeoutSeconds:  120,
			FailureCooldownMinutes: 5,
		},
		Groups: map[lifecycle.Group]lifecycle.GroupConfig{
			lifecycle.GroupPaddleOCR: {Mode: lifecycle.ModeOnDemand},
			lifecycle.GroupASR:       {Mode: lifecycle.ModeAlwaysOn, IdleMinutes: &idle},
			lifecycle.GroupReranker:  {Mode: lifecycle.ModeOnDemand},
		},
		Catalog: lifecycle.CatalogConfig{
			DockerHost: "npipe:////./pipe/dockerDesktopLinuxEngine",
			Groups: map[lifecycle.Group]lifecycle.CatalogGroup{
				lifecycle.GroupPaddleOCR: {
					Paths: []string{"/layout-parsing"},
					Backends: []lifecycle.CatalogBackend{{
						ID: "paddle", Upstream: "http://paddleocr-vl:8080",
						Containers: []string{"WeKnora-paddleocr-vlm-server", "WeKnora-paddleocr-vl"},
					}},
				},
				lifecycle.GroupASR: {
					Paths: []string{"/v1/audio/transcriptions"},
					Backends: []lifecycle.CatalogBackend{{
						ID: "speaches", Upstream: "http://speaches:8000",
						Containers: []string{"WeKnora-speaches"},
					}},
				},
				lifecycle.GroupReranker: {
					Paths: []string{"/rerank"},
					Backends: []lifecycle.CatalogBackend{{
						ID: "reranker", Upstream: "http://qwen-reranker:8000",
						Containers: []string{"WeKnora-qwen-reranker"},
					}},
				},
			},
		},
		Controller: lifecycle.ControllerConfig{
			ListenAddress:        ":18443",
			DockerExecutable:     `C:\Program Files\Docker\Docker\resources\bin\docker.exe`,
			ObserveOnly:          true,
			OwnerMutex:           `Global\WeKnoraEngineDockerOwner`,
			SweepIntervalSeconds: 5,
			TLS: lifecycle.TLSFilesConfig{
				Certificate: `C:\ProgramData\WeKnora\engine-controller\tls\server.crt`,
				PrivateKey:  `C:\ProgramData\WeKnora\engine-controller\tls\server.key`,
				ClientCA:    `C:\ProgramData\WeKnora\engine-controller\tls\ca.crt`,
			},
		},
	}
}
