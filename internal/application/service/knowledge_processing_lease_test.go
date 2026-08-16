package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestKnowledgeProcessingLeaseSerializesOldAndNewAttemptWorkers(t *testing.T) {
	svc := &knowledgeService{}
	_, releaseOld, err := svc.acquireKnowledgeProcessingLease(context.Background(), 7, "knowledge-1")
	require.NoError(t, err)

	acquiredNew := make(chan func(), 1)
	go func() {
		_, release, acquireErr := svc.acquireKnowledgeProcessingLease(context.Background(), 7, "knowledge-1")
		if acquireErr == nil {
			acquiredNew <- release
		}
	}()

	select {
	case release := <-acquiredNew:
		release()
		t.Fatal("new attempt worker entered while the old attempt still held the processing lease")
	case <-time.After(50 * time.Millisecond):
	}

	releaseOld()
	select {
	case release := <-acquiredNew:
		release()
	case <-time.After(time.Second):
		t.Fatal("new attempt worker did not enter after the old attempt released the lease")
	}
}

func TestRedisKnowledgeProcessingLeaseIsOwnerSafe(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	svc := &knowledgeService{redisClient: client}

	_, release, err := svc.acquireKnowledgeProcessingLease(context.Background(), 9, "knowledge-redis")
	require.NoError(t, err)
	key := knowledgeProcessingLeaseKey(9, "knowledge-redis")
	require.NoError(t, client.Set(context.Background(), key, "new-owner", time.Minute).Err())

	release()
	value, err := client.Get(context.Background(), key).Result()
	require.NoError(t, err)
	require.Equal(t, "new-owner", value, "an old delivery must not release a newer owner's lease")
}

func TestKnowledgeProcessingLeaseOutlivesOuterTaskAndSingleCallMargin(t *testing.T) {
	svc := &knowledgeService{config: &config.Config{KnowledgeBase: &config.KnowledgeBaseConfig{
		DocumentProcessTimeout: 90 * time.Minute,
	}}}
	require.Equal(t, 125*time.Minute, svc.knowledgeProcessingLeaseTTL())
}

type strictAttemptErrorTracker struct {
	noopSpanTracker
	err error
}

func (t strictAttemptErrorTracker) LatestAttemptStrict(context.Context, string) (int, error) {
	return 0, t.err
}

func TestAttemptSupersededStrictFailsClosedOnLookupError(t *testing.T) {
	wantErr := errors.New("span database unavailable")
	superseded, err := attemptSupersededStrict(
		context.Background(), strictAttemptErrorTracker{err: wantErr}, "knowledge-1", 3,
	)
	require.ErrorIs(t, err, wantErr)
	require.False(t, superseded)
}

func TestAttemptSupersededStrictRejectsTrackerWithoutReliableLatestAttempt(t *testing.T) {
	superseded, err := attemptSupersededStrict(
		context.Background(), noopSpanTracker{}, "knowledge-1", 3,
	)
	require.EqualError(t, err, "strict latest attempt lookup is unavailable")
	require.False(t, superseded)
}
