package ollama

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/ollama/ollama/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOptionalOllamaService(t *testing.T, handler http.Handler) *OllamaService {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("OLLAMA_BASE_URL", server.URL)
	t.Setenv("OLLAMA_OPTIONAL", "true")
	service, err := GetOllamaService()
	require.NoError(t, err)
	return service
}

func TestOllamaEndpointLabelOmitsCredentialsPathAndQuery(t *testing.T) {
	parsed, err := url.Parse("https://user:secret@example.test:11434/private?token=secret")
	require.NoError(t, err)
	assert.Equal(t, "https://example.test:11434", ollamaEndpointLabel(parsed))
}

func TestInvalidOllamaURLDoesNotLeakRawConfiguration(t *testing.T) {
	t.Setenv("OLLAMA_BASE_URL", "https://user:password@example.test/private%zz?token=secret#fragment")

	_, err := GetOllamaService()

	require.EqualError(t, err, "invalid Ollama service URL")
	for _, secret := range []string{"user", "password", "private", "token", "secret", "fragment"} {
		assert.NotContains(t, err.Error(), secret)
	}
}

func TestOptionalOllamaRuntimeOperationsNeverMaskUnavailability(t *testing.T) {
	service := newOptionalOllamaService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))

	for _, tc := range []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "health", run: service.StartService},
		{name: "ensure model", run: func(ctx context.Context) error {
			return service.EnsureModelAvailable(ctx, "qwen3-embedding:0.6b")
		}},
		{name: "pull model", run: func(ctx context.Context) error {
			return service.PullModel(ctx, "qwen3-embedding:0.6b")
		}},
		{name: "chat", run: func(ctx context.Context) error {
			return service.Chat(ctx, &api.ChatRequest{Model: "minicpm-v:latest"}, func(api.ChatResponse) error { return nil })
		}},
		{name: "embeddings", run: func(ctx context.Context) error {
			_, err := service.Embeddings(ctx, &api.EmbedRequest{Model: "qwen3-embedding:0.6b"})
			return err
		}},
		{name: "generate", run: func(ctx context.Context) error {
			return service.Generate(ctx, &api.GenerateRequest{Model: "minicpm-v:latest"}, func(api.GenerateResponse) error { return nil })
		}},
		{name: "version", run: func(ctx context.Context) error {
			_, err := service.GetVersion(ctx)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(context.Background())
			require.Error(t, err, "optional configuration must not turn a runtime provider failure into success")
			assert.Contains(t, err.Error(), "ollama service unavailable")
			assert.False(t, service.IsAvailable())
		})
	}
}

func TestOptionalOllamaRecoversWhenServiceBecomesAvailable(t *testing.T) {
	var healthy atomic.Bool
	var probes atomic.Int32
	service := newOptionalOllamaService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			probes.Add(1)
			if !healthy.Load() {
				http.Error(w, "offline", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("Ollama is running"))
		case "/api/tags":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"models":[{"name":"qwen3-embedding:0.6b","model":"qwen3-embedding:0.6b"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))

	require.Error(t, service.StartService(context.Background()))
	require.False(t, service.IsAvailable())
	healthy.Store(true)

	require.NoError(t, service.EnsureModelAvailable(context.Background(), "qwen3-embedding:0.6b"))
	assert.True(t, service.IsAvailable())
	assert.GreaterOrEqual(t, probes.Load(), int32(2), "real work must re-probe after an optional startup failure")
}
