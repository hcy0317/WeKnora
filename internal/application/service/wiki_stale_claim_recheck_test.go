package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type staleClaimRecheckPendingRepo struct {
	interfaces.TaskPendingOpsRepository
}

func (*staleClaimRecheckPendingRepo) PendingCount(
	context.Context, string, string, string,
) (int64, error) {
	return 1, nil
}

func testStaleClaimRecheckSuccessor(
	t *testing.T, currentTaskID, expectedSuccessorID string,
) {
	t.Helper()
	redisServer := miniredis.RunT(t)
	redisOpt := asynq.RedisClientOpt{Addr: redisServer.Addr()}
	client := asynq.NewClient(redisOpt)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	inspector := asynq.NewInspector(redisOpt)
	t.Cleanup(func() { require.NoError(t, inspector.Close()) })

	payload := WikiIngestPayload{TenantID: 7, KnowledgeBaseID: "kb-stale-claim"}
	service := &wikiIngestService{
		pendingRepo: &staleClaimRecheckPendingRepo{},
		task:        client,
	}
	handled := make(chan bool, 1)
	mux := asynq.NewServeMux()
	mux.HandleFunc(types.TypeWikiIngest, func(ctx context.Context, _ *asynq.Task) error {
		id, ok := asynq.GetTaskID(ctx)
		handled <- ok && id == currentTaskID && service.scheduleStaleClaimRecheck(ctx, payload)
		return nil
	})
	server := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency:     1,
		Queues:          map[string]int{types.QueueWiki: 1},
		ShutdownTimeout: time.Second,
	})
	require.NoError(t, server.Start(mux))
	t.Cleanup(server.Shutdown)

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	_, err = client.Enqueue(
		asynq.NewTask(types.TypeWikiIngest, payloadBytes),
		asynq.Queue(types.QueueWiki),
		asynq.TaskID(currentTaskID),
	)
	require.NoError(t, err)

	select {
	case scheduled := <-handled:
		require.True(t, scheduled)
	case <-time.After(5 * time.Second):
		t.Fatal("current stale-claim recheck was not processed")
	}

	require.Eventually(t, func() bool {
		tasks, inspectErr := inspector.ListScheduledTasks(types.QueueWiki)
		return inspectErr == nil && len(tasks) == 1 && tasks[0].ID == expectedSuccessorID
	}, 5*time.Second, 20*time.Millisecond)
}

func TestStaleClaimRecheckAlternatesTaskIDWhenRearming(t *testing.T) {
	baseTaskID := "wiki-ingest-recheck-kb-stale-claim"
	for _, test := range []struct {
		name                string
		currentTaskID       string
		expectedSuccessorID string
	}{
		{name: "base to next", currentTaskID: baseTaskID, expectedSuccessorID: baseTaskID + "-next"},
		{name: "next to base", currentTaskID: baseTaskID + "-next", expectedSuccessorID: baseTaskID},
	} {
		t.Run(test.name, func(t *testing.T) {
			testStaleClaimRecheckSuccessor(t, test.currentTaskID, test.expectedSuccessorID)
		})
	}
}
