package hostcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type maintenanceCoordinatorStub struct {
	calls []string
}

func (s *maintenanceCoordinatorStub) SweepGPUAdmission(context.Context) error {
	s.calls = append(s.calls, "gpu")
	return nil
}

func (s *maintenanceCoordinatorStub) EnsureAlwaysOn(context.Context) error {
	s.calls = append(s.calls, "always_on")
	return errors.New("start retry is cooling down")
}

func (s *maintenanceCoordinatorStub) SweepIdle(context.Context) error {
	s.calls = append(s.calls, "idle")
	return errors.New("stop retry is cooling down")
}

func TestMaintenanceSweepKeepsRunningAfterRecoverableLifecycleErrors(t *testing.T) {
	t.Parallel()

	coordinator := &maintenanceCoordinatorStub{}
	runMaintenanceSweep(context.Background(), coordinator)
	require.Equal(t, []string{"gpu", "always_on", "idle"}, coordinator.calls)
}
