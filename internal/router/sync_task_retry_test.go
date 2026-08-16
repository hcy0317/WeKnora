package router

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestSyncTaskExecutorInjectsRetryMetadata(t *testing.T) {
	executor := NewSyncTaskExecutor()
	observed := make(chan [2]int, 1)
	executor.RegisterHandler("test:retry-metadata", func(ctx context.Context, _ *asynq.Task) error {
		retried, maxRetry, ok := types.TaskRetryMetadataFromContext(ctx)
		if !ok {
			observed <- [2]int{-1, -1}
			return nil
		}
		observed <- [2]int{retried, maxRetry}
		return nil
	})

	task := asynq.NewTask("test:retry-metadata", nil)
	if _, err := executor.Enqueue(task, asynq.MaxRetry(3)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case got := <-observed:
		if got != [2]int{0, 3} {
			t.Fatalf("retry metadata = %v, want [0 3]", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sync task")
	}
}

func TestSyncTaskExecutorHonorsStableTaskIDUntilTerminal(t *testing.T) {
	executor := NewSyncTaskExecutor()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	executor.RegisterHandler("stable", func(context.Context, *asynq.Task) error {
		entered <- struct{}{}
		<-release
		return nil
	})
	task := asynq.NewTask("stable", nil)
	first, err := executor.Enqueue(task, asynq.TaskID("stable-id"), asynq.MaxRetry(0))
	require.NoError(t, err)
	require.Equal(t, "stable-id", first.ID)
	<-entered
	_, err = executor.Enqueue(task, asynq.TaskID("stable-id"), asynq.MaxRetry(0))
	require.ErrorIs(t, err, asynq.ErrTaskIDConflict)
	close(release)
	require.Eventually(t, func() bool {
		_, retryErr := executor.Enqueue(task, asynq.TaskID("stable-id"), asynq.MaxRetry(0))
		return retryErr == nil
	}, time.Second, 10*time.Millisecond)
}
