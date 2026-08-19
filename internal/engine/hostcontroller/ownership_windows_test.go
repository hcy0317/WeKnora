//go:build windows

package hostcontroller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestOwnerFileActuationGateKeepsMutexOnOneOSThread(t *testing.T) {
	ownerPath := filepath.Join(t.TempDir(), "owner.txt")
	require.NoError(t, os.WriteFile(ownerPath, []byte("controller\n"), 0o600))
	gate, err := NewOwnerFileActuationGate(
		fmt.Sprintf(`Local\WeKnoraEngineLifecycleThreadAffinityTest-%d`, os.Getpid()),
		ownerPath,
		"controller",
	)
	require.NoError(t, err)

	var firstThread uint32
	require.NoError(t, gate.WithOwnership(context.Background(), func() error {
		firstThread = windows.GetCurrentThreadId()
		for range 100_000 {
			runtime.Gosched()
			if currentThread := windows.GetCurrentThreadId(); currentThread != firstThread {
				return fmt.Errorf("actuation moved from OS thread %d to %d", firstThread, currentThread)
			}
		}
		return nil
	}))
}
