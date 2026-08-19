package hostcontroller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

type readyRuntime struct{}

func (readyRuntime) Start(context.Context, lifecycle.Group) (lifecycle.Backend, error) {
	return lifecycle.Backend{ID: "speaches-cpu", URL: "http://speaches:8000"}, nil
}

func (readyRuntime) Stop(context.Context, lifecycle.Group) error { return nil }

func TestAPIEnforcesClientCertificateCapabilitiesForLeaseAcquire(t *testing.T) {
	t.Parallel()

	coordinator, err := lifecycle.NewCoordinator(apiTestConfig(), readyRuntime{})
	require.NoError(t, err)
	api := NewAPI(coordinator, nil)

	backendRequest := newMTLSRequest(
		http.MethodPost,
		"/v1/groups/asr/leases",
		`{"request_id":"request-1","gateway_id":"gateway-1","purpose":"transcribe"}`,
		ClientCNBackend,
	)
	backendResponse := httptest.NewRecorder()
	api.ServeHTTP(backendResponse, backendRequest)
	require.Equal(t, http.StatusForbidden, backendResponse.Code)

	gatewayRequest := newMTLSRequest(
		http.MethodPost,
		"/v1/groups/asr/leases",
		`{"request_id":"request-1","gateway_id":"gateway-1","purpose":"transcribe"}`,
		ClientCNGateway,
	)
	gatewayResponse := httptest.NewRecorder()
	api.ServeHTTP(gatewayResponse, gatewayRequest)
	require.Equal(t, http.StatusCreated, gatewayResponse.Code)
	require.Contains(t, gatewayResponse.Body.String(), `"group":"asr"`)
	require.Contains(t, gatewayResponse.Body.String(), `"backend":{"id":"speaches-cpu"`)
}

func TestMutualTLSConfigRequiresTrustedClientCertificate(t *testing.T) {
	t.Parallel()

	clientCAs := x509.NewCertPool()
	serverCertificate := tls.Certificate{Certificate: [][]byte{{1, 2, 3}}}
	config, err := NewMutualTLSConfig(serverCertificate, clientCAs)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), config.MinVersion)
	require.Equal(t, tls.RequireAndVerifyClientCert, config.ClientAuth)
	require.Same(t, clientCAs, config.ClientCAs)
	require.Len(t, config.Certificates, 1)
}

func TestAPIUsesETagCASForBackendConfigWrites(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
schema_version: 1
revision: 1
defaults:
  idle_minutes: 10
  startup_timeout_seconds: 120
  failure_cooldown_minutes: 5
groups:
  paddleocr:
    mode: on_demand
  asr:
    mode: on_demand
  reranker:
    mode: on_demand
`), 0o600))
	store := lifecycle.NewConfigStore(path)
	coordinator, err := lifecycle.NewCoordinator(apiTestConfig(), readyRuntime{})
	require.NoError(t, err)
	api := NewAPI(coordinator, store)

	getRequest := newMTLSRequest(http.MethodGet, "/v1/config", "", ClientCNBackend)
	getResponse := httptest.NewRecorder()
	api.ServeHTTP(getResponse, getRequest)
	require.Equal(t, http.StatusOK, getResponse.Code)
	require.Equal(t, `"1"`, getResponse.Header().Get("ETag"))

	candidate, err := store.Load()
	require.NoError(t, err)
	idleMinutes := 3
	asr := candidate.Groups[lifecycle.GroupASR]
	asr.IdleMinutes = &idleMinutes
	candidate.Groups[lifecycle.GroupASR] = asr
	body, err := json.Marshal(candidate)
	require.NoError(t, err)

	putRequest := newMTLSRequest(http.MethodPut, "/v1/config", string(body), ClientCNBackend)
	putRequest.Header.Set("If-Match", `"1"`)
	putResponse := httptest.NewRecorder()
	api.ServeHTTP(putResponse, putRequest)
	require.Equal(t, http.StatusOK, putResponse.Code)
	require.Equal(t, `"2"`, putResponse.Header().Get("ETag"))

	staleRequest := newMTLSRequest(http.MethodPut, "/v1/config", string(body), ClientCNBackend)
	staleRequest.Header.Set("If-Match", `"1"`)
	staleResponse := httptest.NewRecorder()
	api.ServeHTTP(staleResponse, staleRequest)
	require.Equal(t, http.StatusConflict, staleResponse.Code)
	require.Equal(t, `"2"`, staleResponse.Header().Get("ETag"))
}

func TestAPIGatewayRenewsAndReleasesLeaseIdempotently(t *testing.T) {
	t.Parallel()

	coordinator, err := lifecycle.NewCoordinator(apiTestConfig(), readyRuntime{})
	require.NoError(t, err)
	api := NewAPI(coordinator, nil)

	acquireRequest := newMTLSRequest(
		http.MethodPost,
		"/v1/groups/asr/leases",
		`{"request_id":"request-lease","gateway_id":"gateway-1","purpose":"transcribe"}`,
		ClientCNGateway,
	)
	acquireResponse := httptest.NewRecorder()
	api.ServeHTTP(acquireResponse, acquireRequest)
	require.Equal(t, http.StatusCreated, acquireResponse.Code)
	var lease lifecycle.Lease
	require.NoError(t, json.Unmarshal(acquireResponse.Body.Bytes(), &lease))
	require.NotEmpty(t, lease.ID)

	renewRequest := newMTLSRequest(http.MethodPost, "/v1/leases/"+lease.ID+"/renew", "", ClientCNGateway)
	renewResponse := httptest.NewRecorder()
	api.ServeHTTP(renewResponse, renewRequest)
	require.Equal(t, http.StatusNoContent, renewResponse.Code)

	for range 2 {
		releaseRequest := newMTLSRequest(http.MethodDelete, "/v1/leases/"+lease.ID, "", ClientCNGateway)
		releaseResponse := httptest.NewRecorder()
		api.ServeHTTP(releaseResponse, releaseRequest)
		require.Equal(t, http.StatusNoContent, releaseResponse.Code)
	}

	statusRequest := newMTLSRequest(http.MethodGet, "/v1/groups/asr", "", ClientCNBackend)
	statusResponse := httptest.NewRecorder()
	api.ServeHTTP(statusResponse, statusRequest)
	require.Equal(t, http.StatusOK, statusResponse.Code)
	require.Contains(t, statusResponse.Body.String(), `"state":"ready"`)
	require.Contains(t, statusResponse.Body.String(), `"active":0`)
}

func newMTLSRequest(method, target, body, commonName string) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{
			Subject: pkix.Name{CommonName: commonName},
		}},
	}
	return request
}

func apiTestConfig() lifecycle.Config {
	return lifecycle.Config{
		SchemaVersion: lifecycle.CurrentSchemaVersion,
		Revision:      1,
		Defaults: lifecycle.DefaultsConfig{
			IdleMinutes:            10,
			StartupTimeoutSeconds:  120,
			FailureCooldownMinutes: 5,
		},
		Groups: map[lifecycle.Group]lifecycle.GroupConfig{
			lifecycle.GroupPaddleOCR: {Mode: lifecycle.ModeOnDemand},
			lifecycle.GroupASR:       {Mode: lifecycle.ModeOnDemand},
			lifecycle.GroupReranker:  {Mode: lifecycle.ModeOnDemand},
		},
	}
}
