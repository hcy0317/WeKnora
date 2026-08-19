package lifecycle

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigStoreUsesRevisionCASAndKeepsPreviousVersion(t *testing.T) {
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
	store := NewConfigStore(path)

	candidate, err := store.Load()
	require.NoError(t, err)
	idleMinutes := 3
	asr := candidate.Groups[GroupASR]
	asr.IdleMinutes = &idleMinutes
	candidate.Groups[GroupASR] = asr

	updated, err := store.Update(1, *candidate)
	require.NoError(t, err)
	require.Equal(t, uint64(2), updated.Revision)
	policy, err := updated.PolicyFor(GroupASR)
	require.NoError(t, err)
	require.Equal(t, 3, int(policy.IdleTimeout.Minutes()))

	_, err = store.Update(1, *candidate)
	var conflict *RevisionConflictError
	require.True(t, errors.As(err, &conflict))
	require.Equal(t, uint64(1), conflict.Expected)
	require.Equal(t, uint64(2), conflict.Actual)

	current, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(2), current.Revision)
	previous, err := os.Open(path + ".previous")
	require.NoError(t, err)
	t.Cleanup(func() { _ = previous.Close() })
	previousConfig, err := DecodeConfig(previous)
	require.NoError(t, err)
	require.Equal(t, uint64(1), previousConfig.Revision)
}
