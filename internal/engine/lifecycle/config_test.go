package lifecycle

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeConfigResolvesDefaultAndGroupPolicies(t *testing.T) {
	t.Parallel()

	config, err := DecodeConfig(strings.NewReader(`
schema_version: 1
revision: 7
defaults:
  idle_minutes: 10
  startup_timeout_seconds: 120
  failure_cooldown_minutes: 5
groups:
  paddleocr:
    mode: on_demand
  asr:
    mode: always_on
    idle_minutes: 3
    startup_timeout_seconds: 45
  reranker:
    mode: on_demand
`))
	require.NoError(t, err)
	require.Equal(t, uint64(7), config.Revision)

	paddlePolicy, err := config.PolicyFor(GroupPaddleOCR)
	require.NoError(t, err)
	require.Equal(t, ModeOnDemand, paddlePolicy.Mode)
	require.Equal(t, 10*time.Minute, paddlePolicy.IdleTimeout)
	require.Equal(t, 120*time.Second, paddlePolicy.StartupTimeout)
	require.Equal(t, 5*time.Minute, paddlePolicy.FailureCooldown)

	asrPolicy, err := config.PolicyFor(GroupASR)
	require.NoError(t, err)
	require.Equal(t, ModeAlwaysOn, asrPolicy.Mode)
	require.Equal(t, 3*time.Minute, asrPolicy.IdleTimeout)
	require.Equal(t, 45*time.Second, asrPolicy.StartupTimeout)
	require.Equal(t, 5*time.Minute, asrPolicy.FailureCooldown)
}

func TestDecodeConfigRejectsUnknownGroup(t *testing.T) {
	t.Parallel()

	_, err := DecodeConfig(strings.NewReader(`
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
  arbitrary_container:
    mode: always_on
`))
	require.EqualError(t, err, `unknown engine group "arbitrary_container"`)
}

func TestDecodeConfigLoadsReadonlyCatalog(t *testing.T) {
	t.Parallel()

	config, err := DecodeConfig(strings.NewReader(`
schema_version: 1
revision: 4
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
catalog:
  docker_host: npipe:////./pipe/dockerDesktopLinuxEngine
  groups:
    paddleocr:
      paths: [/layout-parsing]
      backends:
        - id: paddle-gpu
          upstream: http://paddleocr-vl:8080
          containers: [WeKnora-paddleocr-vlm-server, WeKnora-paddleocr-vl]
    asr:
      paths: [/v1/audio/transcriptions]
      backends:
        - id: speaches-cpu
          upstream: http://speaches:8000
          containers: [WeKnora-speaches]
    reranker:
      paths: [/rerank]
      backends:
        - id: qwen-gpu
          upstream: http://qwen-reranker-gpu:8000
          gpu: true
          containers: [WeKnora-qwen-reranker-gpu]
        - id: jina-cpu
          upstream: http://qwen-reranker:8000
          containers: [WeKnora-qwen-reranker]
`))
	require.NoError(t, err)
	require.Equal(t, "npipe:////./pipe/dockerDesktopLinuxEngine", config.Catalog.DockerHost)
	require.Equal(t, []string{"/layout-parsing"}, config.Catalog.Groups[GroupPaddleOCR].Paths)
	require.Equal(t,
		[]string{"WeKnora-paddleocr-vlm-server", "WeKnora-paddleocr-vl"},
		config.Catalog.Groups[GroupPaddleOCR].Backends[0].Containers,
	)
	require.True(t, config.Catalog.Groups[GroupReranker].Backends[0].GPU)
}

func TestDecodeConfigLoadsReadonlyControllerSettings(t *testing.T) {
	t.Parallel()

	config, err := DecodeConfig(strings.NewReader(`
schema_version: 1
revision: 1
defaults:
  idle_minutes: 10
  startup_timeout_seconds: 120
  failure_cooldown_minutes: 5
groups:
  paddleocr:
    mode: always_on
  asr:
    mode: always_on
  reranker:
    mode: always_on
controller:
  listen_address: :18443
  docker_executable: C:\Program Files\Docker\Docker\resources\bin\docker.exe
  observe_only: true
  owner_mutex: Global\WeKnoraEngineDockerOwner
  sweep_interval_seconds: 5
  tls:
    certificate: C:\ProgramData\WeKnora\engine-controller\tls\server.crt
    private_key: C:\ProgramData\WeKnora\engine-controller\tls\server.key
    client_ca: C:\ProgramData\WeKnora\engine-controller\tls\ca.crt
`))
	require.NoError(t, err)
	require.Equal(t, ":18443", config.Controller.ListenAddress)
	require.True(t, config.Controller.ObserveOnly)
	require.Equal(t, `Global\WeKnoraEngineDockerOwner`, config.Controller.OwnerMutex)
	require.Equal(t, 5, config.Controller.SweepIntervalSeconds)
	require.Contains(t, config.Controller.TLS.Certificate, `engine-controller\tls\server.crt`)
}

func TestEngineControllerExampleConfigIsValidAndMigrationSafe(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "config", "engine-controller.example.yaml")
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	config, err := DecodeConfig(file)
	require.NoError(t, err)
	require.True(t, config.Controller.ObserveOnly)
	for _, group := range managedGroups {
		require.Equal(t, ModeAlwaysOn, config.Groups[group].Mode)
	}
}
