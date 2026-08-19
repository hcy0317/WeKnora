package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/engine/hostcontroller"
	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestControllerClientAcquiresLeaseOverMutualTLS(t *testing.T) {
	t.Parallel()

	bundle, err := hostcontroller.BootstrapCertificateBundle(t.TempDir())
	require.NoError(t, err)
	tlsConfig, err := hostcontroller.LoadMutualTLSConfig(lifecycle.TLSFilesConfig{
		Certificate: bundle.ServerCertificate,
		PrivateKey:  bundle.ServerPrivateKey,
		ClientCA:    bundle.CACertificate,
	})
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.NotNil(t, request.TLS)
		require.NotEmpty(t, request.TLS.PeerCertificates)
		require.Equal(t, hostcontroller.ClientCNGateway, request.TLS.PeerCertificates[0].Subject.CommonName)
		switch request.URL.Path {
		case "/v1/groups/asr/leases":
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(lifecycle.Lease{
				ID:      "lease-mtls",
				Group:   lifecycle.GroupASR,
				Backend: lifecycle.Backend{ID: "speaches-cpu", URL: "http://speaches:8000"},
			})
		case "/v1/gateways/gateway-test/reconcile":
			var reconcile lifecycle.GatewayReconcile
			require.NoError(t, json.NewDecoder(request.Body).Decode(&reconcile))
			require.Equal(t, "gateway-test", reconcile.GatewayID)
			require.Equal(t, []string{"lease-mtls"}, reconcile.ActiveLeaseIDs)
			response.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(response, request)
		}
	}))
	server.TLS = tlsConfig
	server.StartTLS()
	t.Cleanup(server.Close)

	client, err := NewControllerClient(ControllerClientConfig{
		BaseURL:    server.URL,
		CACertPath: bundle.CACertificate,
		CertPath:   bundle.GatewayCertificate,
		KeyPath:    bundle.GatewayPrivateKey,
		GatewayID:  "gateway-test",
	})
	require.NoError(t, err)
	lease, err := client.Acquire(context.Background(), lifecycle.GroupASR, lifecycle.AcquireRequest{
		RequestID: "request-mtls",
		GatewayID: "gateway-test",
		Purpose:   "transcribe",
	})
	require.NoError(t, err)
	require.Equal(t, "lease-mtls", lease.ID)
	require.NoError(t, client.Reconcile(context.Background(), lifecycle.GatewayReconcile{
		ActiveLeaseIDs: []string{lease.ID},
	}))
}
