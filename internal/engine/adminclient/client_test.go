package adminclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

type readyRuntime struct{}

func (readyRuntime) Start(context.Context, lifecycle.Group) (lifecycle.Backend, error) {
	return lifecycle.Backend{ID: "backend", URL: "http://backend:8000"}, nil
}

func (readyRuntime) Stop(context.Context, lifecycle.Group) error { return nil }

func TestClientUsesBackendMTLSIdentityAndConfigETagCAS(t *testing.T) {
	t.Parallel()

	bundle, err := hostcontroller.BootstrapCertificateBundle(t.TempDir())
	require.NoError(t, err)
	store := lifecycle.NewConfigStore(writeTestConfig(t))
	coordinator, err := lifecycle.NewCoordinator(testConfig(), readyRuntime{})
	require.NoError(t, err)

	serverCertificate, err := tls.LoadX509KeyPair(bundle.ServerCertificate, bundle.ServerPrivateKey)
	require.NoError(t, err)
	clientCAs := x509.NewCertPool()
	caPEM, err := os.ReadFile(bundle.CACertificate)
	require.NoError(t, err)
	require.True(t, clientCAs.AppendCertsFromPEM(caPEM))
	serverTLS, err := hostcontroller.NewMutualTLSConfig(serverCertificate, clientCAs)
	require.NoError(t, err)

	server := httptest.NewUnstartedServer(hostcontroller.NewAPI(coordinator, store))
	server.TLS = serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)

	client, err := New(Config{
		BaseURL:         server.URL,
		CAFile:          bundle.CACertificate,
		CertificateFile: bundle.BackendCertificate,
		PrivateKeyFile:  bundle.BackendPrivateKey,
		Timeout:         5 * time.Second,
	})
	require.NoError(t, err)

	current, err := client.GetConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), current.Revision)

	idle := 3
	asr := current.Groups[lifecycle.GroupASR]
	asr.IdleMinutes = &idle
	current.Groups[lifecycle.GroupASR] = asr
	updated, err := client.UpdateConfig(context.Background(), 1, *current)
	require.NoError(t, err)
	require.Equal(t, uint64(2), updated.Revision)

	_, err = client.UpdateConfig(context.Background(), 1, *current)
	var responseError *HTTPError
	require.ErrorAs(t, err, &responseError)
	require.Equal(t, http.StatusConflict, responseError.StatusCode)
	require.Equal(t, uint64(2), responseError.Revision)
}

func TestClientRejectsControllerOutsideFixedLocalHosts(t *testing.T) {
	t.Parallel()

	_, err := New(Config{BaseURL: "https://example.com:18443"})
	require.ErrorContains(t, err, "local controller host")
}

func writeTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte(`schema_version: 1
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
`), 0o600)
	require.NoError(t, err)
	return path
}

func testConfig() lifecycle.Config {
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
