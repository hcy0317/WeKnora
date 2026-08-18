package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetControllerObserveOnlyUsesRevisionedAtomicConfigUpdate(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	example := filepath.Join(filepath.Dir(currentFile), "..", "..", "config", "engine-controller.example.yaml")
	contents, err := os.ReadFile(example)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, contents, 0o600))

	updated, err := setControllerObserveOnly(configPath, false)
	require.NoError(t, err)
	require.False(t, updated.Controller.ObserveOnly)
	require.Equal(t, uint64(2), updated.Revision)

	unchanged, err := setControllerObserveOnly(configPath, false)
	require.NoError(t, err)
	require.Equal(t, updated.Revision, unchanged.Revision)
	require.FileExists(t, configPath+".previous")
}
