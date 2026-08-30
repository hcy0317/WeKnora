package router_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/router"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type syncKBDeleteRepo struct {
	interfaces.KnowledgeBaseRepository
	kb *types.KnowledgeBase
}

func (r *syncKBDeleteRepo) GetKnowledgeBaseByIDAndTenant(
	context.Context, string, uint64,
) (*types.KnowledgeBase, error) {
	return r.kb, nil
}

func (r *syncKBDeleteRepo) PrepareKnowledgeBaseDeletion(
	context.Context, uint64, string, *types.TaskPendingOp,
) error {
	return nil
}

func TestDeleteKnowledgeBaseLiteUsesStableTaskIDAcrossRealEnqueue(t *testing.T) {
	executor := router.NewSyncTaskExecutor()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var starts atomic.Int32
	executor.RegisterHandler(types.TypeKBDelete, func(context.Context, *asynq.Task) error {
		starts.Add(1)
		entered <- struct{}{}
		<-release
		return nil
	})
	repo := &syncKBDeleteRepo{kb: &types.KnowledgeBase{ID: "kb-lite", TenantID: 7}}
	svc := service.NewKnowledgeBaseService(
		repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		executor, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 7})

	require.NoError(t, svc.DeleteKnowledgeBase(ctx, "kb-lite"))
	<-entered
	require.NoError(t, svc.DeleteKnowledgeBase(ctx, "kb-lite"), "stable-ID conflict is an idempotent publish")
	require.Equal(t, int32(1), starts.Load(), "two request/recovery publishes may only start one active task")

	close(release)
	require.Eventually(t, func() bool {
		return svc.DeleteKnowledgeBase(ctx, "kb-lite") == nil && starts.Load() >= 2
	}, time.Second, 10*time.Millisecond, "terminal release must allow the same stable ID to awaken again")
}
