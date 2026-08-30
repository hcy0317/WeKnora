package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestHoldSandboxTurnOpensAndClosesTheLease(t *testing.T) {
	holder := &turnLeaseManager{}
	svc := &sessionService{sandboxMgr: holder}

	release := svc.holdSandboxTurn(context.Background(), "session-a", "")
	require.Equal(t, 1, holder.begins)
	require.Zero(t, holder.ends)

	release()
	require.Equal(t, 1, holder.ends)
}

func TestHoldSandboxTurnUsesPinnedNamedConfig(t *testing.T) {
	pinner := NewSessionSandboxPinner(newPinTestDB(t))
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	_, err := pinner.Pin(ctx, "s-1", "cfg-pinned")
	require.NoError(t, err)
	deploymentDefault := &turnLeaseManager{}
	named := &turnLeaseManager{}
	svc := &sessionService{
		sandboxMgr: deploymentDefault, sandboxResolver: stubSandboxResolver{mgr: named},
		sandboxPinner: pinner,
	}

	release := svc.holdSandboxTurn(ctx, "s-1", "cfg-agent-now")
	require.Zero(t, deploymentDefault.begins)
	require.Equal(t, 1, named.begins)
	release()
	require.Equal(t, 1, named.ends)
}

func TestHoldSandboxTurnIsNoopWhenBeginFails(t *testing.T) {
	holder := &turnLeaseManager{beginErr: context.Canceled}
	svc := &sessionService{sandboxMgr: holder}

	release := svc.holdSandboxTurn(context.Background(), "session-a", "")
	require.Equal(t, 1, holder.begins)
	release()
	require.Zero(t, holder.ends)
}

type turnLeaseManager struct {
	stagingSandboxManager
	begins   int
	ends     int
	beginErr error
	endErr   error
}

func (m *turnLeaseManager) HoldSessionTurn(context.Context, string) (func() error, error) {
	m.begins++
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return func() error {
		m.ends++
		return m.endErr
	}, nil
}
