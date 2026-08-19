package hostcontroller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnershipInterlockRejectsSecondOwner(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf(`Local\WeKnoraEngineLifecycleTest-%d`, os.Getpid())
	first, err := AcquireOwnership(name)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	second, err := AcquireOwnership(name)
	require.ErrorIs(t, err, ErrOwnerConflict)
	require.Nil(t, second)
}

func TestOwnerFileActuationGateChecksOwnerWhileHoldingMutex(t *testing.T) {
	t.Parallel()

	ownerPath := filepath.Join(t.TempDir(), "owner.txt")
	require.NoError(t, os.WriteFile(ownerPath, []byte("controller\n"), 0o600))
	gate, err := NewOwnerFileActuationGate(
		fmt.Sprintf(`Local\WeKnoraEngineLifecycleOwnerFileTest-%d`, os.Getpid()),
		ownerPath,
		"controller",
	)
	require.NoError(t, err)

	called := false
	require.NoError(t, gate.WithOwnership(context.Background(), func() error {
		called = true
		return nil
	}))
	require.True(t, called)

	require.NoError(t, os.WriteFile(ownerPath, []byte("legacy\n"), 0o600))
	called = false
	err = gate.WithOwnership(context.Background(), func() error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, ErrOwnerMismatch)
	require.False(t, called)
}
