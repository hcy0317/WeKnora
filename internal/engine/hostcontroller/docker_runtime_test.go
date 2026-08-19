package hostcontroller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/stretchr/testify/require"
)

type catalogRunner struct {
	mu      sync.Mutex
	started map[string]bool
	calls   [][]string
}

type recordingActuationGate struct {
	err   error
	calls int
}

func (g *recordingActuationGate) WithOwnership(_ context.Context, action func() error) error {
	g.calls++
	if g.err != nil {
		return g.err
	}
	return action()
}

func (r *catalogRunner) Run(_ context.Context, arguments ...string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, append([]string(nil), arguments...))
	if len(arguments) == 2 && arguments[0] == "start" {
		r.started[arguments[1]] = true
		return arguments[1], nil
	}
	if len(arguments) == 4 && arguments[0] == "inspect" && arguments[1] == "--format" {
		container := arguments[3]
		if r.started[container] {
			return `{"Status":"running","Running":true,"Health":{"Status":"healthy"}}`, nil
		}
		return `{"Status":"exited","Running":false}`, nil
	}
	if len(arguments) == 4 && arguments[0] == "stop" && arguments[1] == "--time" {
		container := arguments[3]
		if !r.started[container] {
			return "", fmt.Errorf("container %s is not running", container)
		}
		r.started[container] = false
		return container, nil
	}
	return "", fmt.Errorf("unexpected docker arguments: %s", strings.Join(arguments, " "))
}

func (r *catalogRunner) snapshotCalls() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := make([][]string, len(r.calls))
	for index := range r.calls {
		calls[index] = append([]string(nil), r.calls[index]...)
	}
	return calls
}

func TestDockerRuntimeStartsPaddleCatalogInOrderAndWaitsForHealth(t *testing.T) {
	t.Parallel()

	runner := &catalogRunner{started: make(map[string]bool)}
	runtime, err := NewDockerRuntime(testCatalog(), runner, WithHealthPollInterval(time.Millisecond))
	require.NoError(t, err)

	backend, err := runtime.StartWithAdmission(context.Background(), lifecycle.StartRequest{
		Group:      lifecycle.GroupPaddleOCR,
		GPUAllowed: true,
	})
	require.NoError(t, err)
	require.Equal(t, "paddle-gpu", backend.ID)
	require.Equal(t, "http://paddleocr-vl:8080", backend.URL)

	calls := runner.snapshotCalls()
	require.Equal(t, []string{"start", "WeKnora-paddleocr-vlm-server"}, calls[0])
	require.Equal(t, "WeKnora-paddleocr-vlm-server", calls[1][3])
	require.Equal(t, []string{"start", "WeKnora-paddleocr-vl"}, calls[2])
	require.Equal(t, "WeKnora-paddleocr-vl", calls[3][3])
}

func TestDockerRuntimeObserveOnlyNeverActuatesContainers(t *testing.T) {
	t.Parallel()

	runner := &catalogRunner{started: map[string]bool{
		"WeKnora-speaches": true,
	}}
	runtime, err := NewDockerRuntime(
		testCatalog(),
		runner,
		WithObserveOnly(true),
		WithHealthPollInterval(time.Millisecond),
	)
	require.NoError(t, err)

	backend, err := runtime.Start(context.Background(), lifecycle.GroupASR)
	require.NoError(t, err)
	require.Equal(t, "speaches-cpu", backend.ID)
	for _, call := range runner.snapshotCalls() {
		require.NotEqual(t, "start", call[0])
		require.NotEqual(t, "stop", call[0])
	}
	require.ErrorIs(t, runtime.Stop(context.Background(), lifecycle.GroupASR), ErrObserveOnly)
}

func TestDockerRuntimeRequiresControllerOwnershipBeforeActuation(t *testing.T) {
	t.Parallel()

	runner := &catalogRunner{started: make(map[string]bool)}
	gate := &recordingActuationGate{err: ErrOwnerMismatch}
	runtime, err := NewDockerRuntime(
		testCatalog(),
		runner,
		WithActuationGate(gate),
		WithHealthPollInterval(time.Millisecond),
	)
	require.NoError(t, err)

	_, err = runtime.Start(context.Background(), lifecycle.GroupASR)
	require.ErrorIs(t, err, ErrOwnerMismatch)
	require.Equal(t, 1, gate.calls)
	require.Empty(t, runner.snapshotCalls())
}

func TestDockerRuntimeStopSkipsContainersThatAreAlreadyStopped(t *testing.T) {
	t.Parallel()

	runner := &catalogRunner{started: map[string]bool{
		"WeKnora-qwen-reranker-gpu": false,
		"WeKnora-qwen-reranker":     true,
	}}
	runtime, err := NewDockerRuntime(testCatalog(), runner)
	require.NoError(t, err)

	require.NoError(t, runtime.Stop(context.Background(), lifecycle.GroupReranker))
	calls := runner.snapshotCalls()
	require.Equal(t, [][]string{
		{"inspect", "--format", "{{json .State}}", "WeKnora-qwen-reranker"},
		{"stop", "--time", "20", "WeKnora-qwen-reranker"},
		{"inspect", "--format", "{{json .State}}", "WeKnora-qwen-reranker-gpu"},
	}, calls)
}

func testCatalog() lifecycle.CatalogConfig {
	return lifecycle.CatalogConfig{
		DockerHost: "npipe:////./pipe/dockerDesktopLinuxEngine",
		Groups: map[lifecycle.Group]lifecycle.CatalogGroup{
			lifecycle.GroupPaddleOCR: {
				Paths: []string{"/layout-parsing"},
				Backends: []lifecycle.CatalogBackend{{
					ID:         "paddle-gpu",
					Upstream:   "http://paddleocr-vl:8080",
					GPU:        true,
					Containers: []string{"WeKnora-paddleocr-vlm-server", "WeKnora-paddleocr-vl"},
				}},
			},
			lifecycle.GroupASR: {
				Paths: []string{"/v1/audio/transcriptions"},
				Backends: []lifecycle.CatalogBackend{{
					ID:         "speaches-cpu",
					Upstream:   "http://speaches:8000",
					Containers: []string{"WeKnora-speaches"},
				}},
			},
			lifecycle.GroupReranker: {
				Paths: []string{"/rerank"},
				Backends: []lifecycle.CatalogBackend{
					{
						ID:         "qwen-gpu",
						Upstream:   "http://qwen-reranker-gpu:8000",
						GPU:        true,
						Containers: []string{"WeKnora-qwen-reranker-gpu"},
					},
					{
						ID:         "jina-cpu",
						Upstream:   "http://qwen-reranker:8000",
						Containers: []string{"WeKnora-qwen-reranker"},
					},
				},
			},
		},
	}
}
