package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

type fakeLeaseClient struct {
	mu           sync.Mutex
	group        lifecycle.Group
	request      lifecycle.AcquireRequest
	lease        lifecycle.Lease
	acquireErr   error
	reconcileErr error
	released     []string
	reconciles   []lifecycle.GatewayReconcile
	acquire      <-chan struct{}
}

func (c *fakeLeaseClient) Acquire(
	ctx context.Context,
	group lifecycle.Group,
	request lifecycle.AcquireRequest,
) (lifecycle.Lease, error) {
	if c.acquire != nil {
		select {
		case <-c.acquire:
		case <-ctx.Done():
			return lifecycle.Lease{}, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.group = group
	c.request = request
	if c.acquireErr != nil {
		return lifecycle.Lease{}, c.acquireErr
	}
	return c.lease, nil
}

func TestGatewayFailsOpenWithHealthyLastValidatedBackendOnlyWhenControllerIsUnreachable(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			response.WriteHeader(http.StatusOK)
			return
		}
		require.Equal(t, "/v1/audio/transcriptions", request.URL.Path)
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	leaseClient := &fakeLeaseClient{lease: lifecycle.Lease{
		ID:              "lease-signed",
		Group:           lifecycle.GroupASR,
		Backend:         lifecycle.Backend{ID: "asr-cpu", URL: backend.URL},
		ControllerEpoch: 7,
	}}
	gateway, err := New(Config{
		LeaseClient: leaseClient,
		Routes:      DefaultRoutes(),
		AllowedBackends: map[lifecycle.Group]map[string]struct{}{
			lifecycle.GroupASR: {backend.URL: {}},
		},
	})
	require.NoError(t, err)

	warmResponse := httptest.NewRecorder()
	warmRequest := httptest.NewRequest(http.MethodPost, "/asr/v1/audio/transcriptions", strings.NewReader("audio"))
	warmRequest.Header.Set("X-Request-ID", "request-warm")
	gateway.ServeHTTP(warmResponse, warmRequest)
	require.Equal(t, http.StatusNoContent, warmResponse.Code)

	leaseClient.mu.Lock()
	leaseClient.acquireErr = ErrControllerUnreachable
	leaseClient.mu.Unlock()
	partitionResponse := httptest.NewRecorder()
	partitionRequest := httptest.NewRequest(http.MethodPost, "/asr/v1/audio/transcriptions", strings.NewReader("audio"))
	partitionRequest.Header.Set("X-Request-ID", "request-shadow")
	gateway.ServeHTTP(partitionResponse, partitionRequest)

	require.Equal(t, http.StatusNoContent, partitionResponse.Code)
	require.Equal(t, "true", partitionResponse.Header().Get("X-WeKnora-Engine-Controller-Degraded"))
	statusResponse := httptest.NewRecorder()
	gateway.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/v1/requests/request-shadow", nil))
	require.Equal(t, http.StatusOK, statusResponse.Code)
	var status RequestStatus
	require.NoError(t, json.Unmarshal(statusResponse.Body.Bytes(), &status))
	require.True(t, status.ControllerDegraded)
}

func TestGatewayDoesNotFailOpenForControllerApplicationError(t *testing.T) {
	t.Parallel()

	leaseClient := &fakeLeaseClient{
		lease: lifecycle.Lease{
			ID:      "lease-signed",
			Group:   lifecycle.GroupASR,
			Backend: lifecycle.Backend{ID: "asr-cpu", URL: "http://speaches:8000"},
		},
		acquireErr: errors.New("controller rejected acquire"),
	}
	gateway, err := New(Config{
		LeaseClient:     leaseClient,
		Routes:          DefaultRoutes(),
		AllowedBackends: DefaultAllowedBackends(),
	})
	require.NoError(t, err)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/asr/v1/audio/transcriptions", strings.NewReader("audio"))
	gateway.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}

func TestGatewayReconcilesActiveShadowLeaseAfterControllerRecovers(t *testing.T) {
	t.Parallel()

	finishProxy := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			response.WriteHeader(http.StatusOK)
			return
		}
		if request.Header.Get("X-Test-Block") == "true" {
			<-finishProxy
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	leaseClient := &fakeLeaseClient{lease: lifecycle.Lease{
		ID:              "lease-signed",
		Group:           lifecycle.GroupASR,
		Backend:         lifecycle.Backend{ID: "asr-cpu", URL: backend.URL},
		ControllerEpoch: 11,
	}}
	gateway, err := New(Config{
		LeaseClient:   leaseClient,
		GatewayID:     "gateway-reconcile",
		Routes:        DefaultRoutes(),
		RenewInterval: 10 * time.Millisecond,
		AllowedBackends: map[lifecycle.Group]map[string]struct{}{
			lifecycle.GroupASR: {backend.URL: {}},
		},
	})
	require.NoError(t, err)

	warmResponse := httptest.NewRecorder()
	gateway.ServeHTTP(warmResponse, httptest.NewRequest(http.MethodPost, "/asr/v1/audio/transcriptions", strings.NewReader("warm")))
	require.Equal(t, http.StatusNoContent, warmResponse.Code)

	leaseClient.mu.Lock()
	leaseClient.acquireErr = ErrControllerUnreachable
	leaseClient.reconcileErr = ErrControllerUnreachable
	leaseClient.mu.Unlock()
	proxyDone := make(chan struct{})
	go func() {
		defer close(proxyDone)
		request := httptest.NewRequest(
			http.MethodPost,
			"/asr/v1/audio/transcriptions",
			strings.NewReader("shadow"),
		)
		request.Header.Set("X-Test-Block", "true")
		gateway.ServeHTTP(httptest.NewRecorder(), request)
	}()

	require.Eventually(t, func() bool {
		leaseClient.mu.Lock()
		defer leaseClient.mu.Unlock()
		leaseClient.reconcileErr = nil
		return len(leaseClient.reconciles) > 0 && len(leaseClient.reconciles[len(leaseClient.reconciles)-1].ShadowLeases) == 1
	}, time.Second, 10*time.Millisecond)
	leaseClient.mu.Lock()
	reconcile := leaseClient.reconciles[len(leaseClient.reconciles)-1]
	leaseClient.mu.Unlock()
	require.Equal(t, "gateway-reconcile", reconcile.GatewayID)
	require.Equal(t, lifecycle.GroupASR, reconcile.ShadowLeases[0].Group)
	require.Equal(t, uint64(11), reconcile.ShadowLeases[0].ControllerEpoch)

	close(finishProxy)
	select {
	case <-proxyDone:
	case <-time.After(time.Second):
		t.Fatal("shadow proxy did not complete")
	}
}

func TestGatewayPublishesWaitingAndCompletedRequestStatus(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	allowAcquire := make(chan struct{})
	leaseClient := &fakeLeaseClient{
		acquire: allowAcquire,
		lease: lifecycle.Lease{
			ID:        "lease-status",
			Group:     lifecycle.GroupASR,
			Backend:   lifecycle.Backend{ID: "asr-cpu", URL: backend.URL},
			ColdStart: true,
		},
	}
	gateway, err := New(Config{
		LeaseClient: leaseClient,
		Routes:      DefaultRoutes(),
		AllowedBackends: map[lifecycle.Group]map[string]struct{}{
			lifecycle.GroupASR: {backend.URL: {}},
		},
	})
	require.NoError(t, err)

	proxyResponse := httptest.NewRecorder()
	proxyDone := make(chan struct{})
	go func() {
		defer close(proxyDone)
		request := httptest.NewRequest(http.MethodPost, "/asr/v1/audio/transcriptions", strings.NewReader("audio"))
		request.Header.Set("X-Request-ID", "request-status")
		gateway.ServeHTTP(proxyResponse, request)
	}()

	require.Eventually(t, func() bool {
		statusResponse := httptest.NewRecorder()
		gateway.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/v1/requests/request-status", nil))
		if statusResponse.Code != http.StatusOK {
			return false
		}
		var status RequestStatus
		require.NoError(t, json.Unmarshal(statusResponse.Body.Bytes(), &status))
		return status.State == RequestWaiting && status.Phase == "acquiring_lease" && status.ElapsedMS >= 0
	}, time.Second, 10*time.Millisecond)

	close(allowAcquire)
	select {
	case <-proxyDone:
	case <-time.After(time.Second):
		t.Fatal("proxy request did not complete")
	}
	require.Equal(t, http.StatusNoContent, proxyResponse.Code)

	statusResponse := httptest.NewRecorder()
	gateway.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/v1/requests/request-status", nil))
	require.Equal(t, http.StatusOK, statusResponse.Code)
	var status RequestStatus
	require.NoError(t, json.Unmarshal(statusResponse.Body.Bytes(), &status))
	require.Equal(t, RequestCompleted, status.State)
	require.Equal(t, "completed", status.Phase)
	require.True(t, status.ColdStart)
	require.Equal(t, "asr-cpu", status.BackendID)
}

func (c *fakeLeaseClient) Renew(context.Context, string) error { return nil }

func (c *fakeLeaseClient) Release(_ context.Context, leaseID string, _ lifecycle.ReleaseReason) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.released = append(c.released, leaseID)
	return nil
}

func (c *fakeLeaseClient) Reconcile(_ context.Context, reconcile lifecycle.GatewayReconcile) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reconcileErr != nil {
		return c.reconcileErr
	}
	c.reconciles = append(c.reconciles, reconcile)
	return nil
}

func TestGatewayAcquiresStreamsAndReleasesPaddleLease(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/layout-parsing", request.URL.Path)
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"markdown":"ok","received":` + string(body) + `}`))
	}))
	t.Cleanup(backend.Close)
	leaseClient := &fakeLeaseClient{lease: lifecycle.Lease{
		ID:        "lease-1",
		Group:     lifecycle.GroupPaddleOCR,
		Backend:   lifecycle.Backend{ID: "paddle-gpu", URL: backend.URL},
		ColdStart: true,
	}}
	gateway, err := New(Config{
		LeaseClient: leaseClient,
		Routes:      DefaultRoutes(),
		AllowedBackends: map[lifecycle.Group]map[string]struct{}{
			lifecycle.GroupPaddleOCR: {backend.URL: {}},
		},
	})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/paddleocr/layout-parsing", strings.NewReader(`{"file":"base64"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", "request-paddle")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"markdown":"ok","received":{"file":"base64"}}`, response.Body.String())
	require.Equal(t, "true", response.Header().Get("X-WeKnora-Engine-Cold-Start"))
	require.Equal(t, lifecycle.GroupPaddleOCR, leaseClient.group)
	require.Equal(t, "request-paddle", leaseClient.request.RequestID)
	require.Equal(t, []string{"lease-1"}, leaseClient.released)
}
