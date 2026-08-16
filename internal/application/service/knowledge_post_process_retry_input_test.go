package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type questionRetryInputCaptureEnqueuer struct {
	payloads []types.QuestionGenerationPayload
}

func (q *questionRetryInputCaptureEnqueuer) Enqueue(
	task *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	var payload types.QuestionGenerationPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return nil, err
	}
	q.payloads = append(q.payloads, payload)
	return &asynq.TaskInfo{ID: "queued", Type: task.Type()}, nil
}

type persistedQuestionRetryInput struct {
	PlanVersion   int      `json:"plan_version"`
	BatchIndex    int      `json:"batch_index"`
	ChunkIDs      []string `json:"chunk_ids"`
	PrevChunkID   string   `json:"prev_chunk_id"`
	NextChunkID   string   `json:"next_chunk_id"`
	QuestionCount int      `json:"question_count"`
	Language      string   `json:"language"`
}

func TestEnqueueQuestionGenerationPlan_PersistsExactSparseReplayInput(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-replay-input", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-replay-input", attempt, types.StagePostProcess, nil)
	require.NotNil(t, post)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	require.NotNil(t, group)

	batches := []postProcessQuestionBatchPlan{
		{BatchIndex: 3, ChunkIDs: []string{"chunk-60", "chunk-61"}, PrevChunkID: "chunk-59", NextChunkID: "chunk-62"},
		{BatchIndex: 7, ChunkIDs: []string{"chunk-140"}, PrevChunkID: "chunk-139", NextChunkID: ""},
	}
	queue := &questionRetryInputCaptureEnqueuer{}
	service := &KnowledgePostProcessService{spanTracker: tracker, taskEnqueuer: queue}

	enqueued, err := service.enqueueQuestionGenerationPlan(ctx, types.KnowledgePostProcessPayload{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "kid-question-replay-input", Language: "zh-CN",
	}, types.QuestionGenerationConfig{Enabled: true, QuestionCount: 12}, attempt, batches, group)
	require.NoError(t, err)
	require.Equal(t, 2, enqueued)
	require.Len(t, queue.payloads, 2)

	expected := []persistedQuestionRetryInput{
		{PlanVersion: postProcessFanoutPlanVersion, BatchIndex: 3, ChunkIDs: []string{"chunk-60", "chunk-61"}, PrevChunkID: "chunk-59", NextChunkID: "chunk-62", QuestionCount: 10, Language: "zh-CN"},
		{PlanVersion: postProcessFanoutPlanVersion, BatchIndex: 7, ChunkIDs: []string{"chunk-140"}, PrevChunkID: "chunk-139", NextChunkID: "", QuestionCount: 10, Language: "zh-CN"},
	}
	for i := range expected {
		require.Equal(t, expected[i].BatchIndex, queue.payloads[i].BatchIndex)
		require.Equal(t, expected[i].ChunkIDs, queue.payloads[i].ChunkIDs)
		require.Equal(t, expected[i].PrevChunkID, queue.payloads[i].PrevChunkID)
		require.Equal(t, expected[i].NextChunkID, queue.payloads[i].NextChunkID)
		require.Equal(t, expected[i].QuestionCount, queue.payloads[i].QuestionCount)
		require.Equal(t, expected[i].Language, queue.payloads[i].Language)
	}

	var spans []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where(
		"knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name LIKE ?",
		"kid-question-replay-input", attempt, group.SpanID, "postprocess.question.batch[%",
	).Order("name ASC").Find(&spans).Error)
	require.Len(t, spans, 2)
	for i, span := range spans {
		raw, err := json.Marshal(span.Input)
		require.NoError(t, err)
		var input persistedQuestionRetryInput
		require.NoError(t, json.Unmarshal(raw, &input))
		require.Equal(t, expected[i], input)
	}
}
