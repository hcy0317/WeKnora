package hostcontroller

import (
	"fmt"
	"os"
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
