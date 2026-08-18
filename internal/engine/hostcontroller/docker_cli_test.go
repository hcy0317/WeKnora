package hostcontroller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordedProcess struct {
	executable string
	arguments  []string
	calls      int
}

func (p *recordedProcess) Execute(_ context.Context, executable string, arguments ...string) (string, error) {
	p.calls++
	p.executable = executable
	p.arguments = append([]string(nil), arguments...)
	return `{"Status":"running","Running":true,"Health":{"Status":"healthy"}}`, nil
}

func TestDockerCLIUsesFixedEndpointAndRejectsCommandsOutsideAllowlist(t *testing.T) {
	t.Parallel()

	process := &recordedProcess{}
	runner, err := NewDockerCLI(
		`C:\Program Files\Docker\Docker\resources\bin\docker.exe`,
		"npipe:////./pipe/dockerDesktopLinuxEngine",
		withProcessExecutor(process),
	)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), "rm", "--force", "WeKnora-speaches")
	require.ErrorContains(t, err, "not allowed")
	require.Zero(t, process.calls)

	_, err = runner.Run(context.Background(), "inspect", "--format", "{{json .State}}", "WeKnora-speaches")
	require.NoError(t, err)
	require.Equal(t, 1, process.calls)
	require.Equal(t, `C:\Program Files\Docker\Docker\resources\bin\docker.exe`, process.executable)
	require.Equal(t, []string{
		"-H", "npipe:////./pipe/dockerDesktopLinuxEngine",
		"inspect", "--format", "{{json .State}}", "WeKnora-speaches",
	}, process.arguments)
}
