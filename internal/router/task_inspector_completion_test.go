package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseKnowledgeCompletionPayloadDistinguishesAttemptPresence(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantID   string
		want     completionAttemptPresence
		wantN    int
		wantOkay bool
	}{
		{name: "absent", payload: `{"knowledge_id":"kid"}`, wantID: "kid", want: completionAttemptAbsent, wantOkay: true},
		{name: "null", payload: `{"knowledge_id":"kid","attempt":null}`, wantID: "kid", want: completionAttemptNull, wantOkay: true},
		{name: "zero", payload: `{"knowledge_id":"kid","attempt":0}`, wantID: "kid", want: completionAttemptZero, wantOkay: true},
		{name: "positive", payload: `{"knowledge_id":"kid","attempt":2}`, wantID: "kid", want: completionAttemptPositive, wantN: 2, wantOkay: true},
		{name: "negative", payload: `{"knowledge_id":"kid","attempt":-1}`, wantID: "kid", wantOkay: false},
		{name: "malformed", payload: `{"knowledge_id":"kid","attempt":"two"}`, wantID: "kid", wantOkay: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			knowledgeID, attempt, presence, ok := parseKnowledgeCompletionPayload([]byte(tt.payload))
			assert.Equal(t, tt.wantOkay, ok)
			assert.Equal(t, tt.wantID, knowledgeID)
			assert.Equal(t, tt.wantN, attempt)
			assert.Equal(t, tt.want, presence)
		})
	}
}

func TestReconcileCompletedKnowledgeTasksDeletesOnlyCompletedAttemptArchives(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	asynqClient := asynq.NewClientFromRedisClient(client)
	t.Cleanup(func() { _ = asynqClient.Close() })
	inspector := asynq.NewInspectorFromRedisClient(client)
	t.Cleanup(func() { _ = inspector.Close() })
	sut := &asynqTaskInspector{inspector: inspector, redis: client}

	fixtures := []struct {
		id      string
		payload string
	}{
		{id: "same-1", payload: `{"knowledge_id":"kid","attempt":1}`},
		{id: "same-2", payload: `{"knowledge_id":"kid","attempt":2}`},
		{id: "newer-3", payload: `{"knowledge_id":"kid","attempt":3}`},
		{id: "legacy-absent", payload: `{"knowledge_id":"kid"}`},
		{id: "legacy-null", payload: `{"knowledge_id":"kid","attempt":null}`},
		{id: "legacy-zero", payload: `{"knowledge_id":"kid","attempt":0}`},
		{id: "other", payload: `{"knowledge_id":"other","attempt":1}`},
	}
	for _, fixture := range fixtures {
		info, err := asynqClient.Enqueue(
			asynq.NewTask(types.TypeKnowledgePostProcess, []byte(fixture.payload)),
			asynq.Queue(types.QueueDefault), asynq.TaskID(fixture.id),
		)
		require.NoError(t, err)
		require.NoError(t, inspector.ArchiveTask(types.QueueDefault, info.ID))
	}

	deleted, legacy, supported, err := sut.ReconcileCompletedKnowledgeTasks(context.Background(), "kid", 2)
	require.NoError(t, err)
	assert.True(t, supported)
	assert.Equal(t, 2, deleted)
	assert.Equal(t, 3, legacy)

	archived, err := inspector.ListArchivedTasks(types.QueueDefault)
	require.NoError(t, err)
	remaining := make(map[string]bool, len(archived))
	for _, task := range archived {
		remaining[task.ID] = true
	}
	assert.False(t, remaining["same-1"])
	assert.False(t, remaining["same-2"])
	for _, id := range []string{"newer-3", "legacy-absent", "legacy-null", "legacy-zero", "other"} {
		assert.Truef(t, remaining[id], "archive %s must be retained", id)
	}
}

func TestReconcileCompletedKnowledgeTasksPropagatesRedisFailure(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	inspector := asynq.NewInspectorFromRedisClient(client)
	sut := &asynqTaskInspector{inspector: inspector, redis: client}
	server.Close()
	t.Cleanup(func() { _ = inspector.Close(); _ = client.Close() })

	_, _, supported, err := sut.ReconcileCompletedKnowledgeTasks(context.Background(), "kid", 1)
	assert.True(t, supported)
	require.Error(t, err)
	assert.Contains(t, fmt.Sprint(err), "list archived")
}
