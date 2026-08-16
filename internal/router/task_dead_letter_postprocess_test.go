package router

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const deadLetterPostprocessTestDDL = `
CREATE TABLE knowledges (
    id VARCHAR(64) PRIMARY KEY,
    parse_status VARCHAR(32) NOT NULL,
    pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    processed_at DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE knowledge_processing_spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    knowledge_id VARCHAR(64) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    span_id VARCHAR(64) NOT NULL,
    parent_span_id VARCHAR(64),
    name VARCHAR(255) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL,
    input TEXT,
    output TEXT,
    metadata TEXT,
    error_code VARCHAR(64),
    error_message TEXT,
    error_detail TEXT,
    started_at DATETIME,
    finished_at DATETIME,
    duration_ms BIGINT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (knowledge_id, attempt, span_id)
);
`

func TestDeadLetterPostprocessOwnerIdentity(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
		payload  any
		want     string
		ok       bool
	}{
		{name: "summary", taskType: types.TypeSummaryGeneration,
			payload: types.SummaryGenerationPayload{KnowledgeID: "kid", Attempt: 3},
			want:    "postprocess.summary", ok: true},
		{name: "question batch", taskType: types.TypeQuestionGeneration,
			payload: types.QuestionGenerationPayload{KnowledgeID: "kid", Attempt: 3,
				ChunkIDs: []string{"chunk"}, BatchIndex: 4},
			want: "postprocess.question.batch[4]", ok: true},
		{name: "legacy question", taskType: types.TypeQuestionGeneration,
			payload: types.QuestionGenerationPayload{KnowledgeID: "kid", Attempt: 3},
			want:    "postprocess.question", ok: true},
		{name: "graph", taskType: types.TypeChunkExtract,
			payload: types.ExtractChunkPayload{KnowledgeID: "kid", Attempt: 3, ChunkIndex: 7},
			want:    "postprocess.graph.chunk[7]", ok: true},
		{name: "summary refresh is independent", taskType: types.TypeSummaryGeneration,
			payload: types.SummaryGenerationPayload{KnowledgeID: "kid", Attempt: 3, Refresh: true}},
		{name: "unrelated task", taskType: types.TypeDocumentProcess,
			payload: types.DocumentProcessPayload{KnowledgeID: "kid", Attempt: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(tt.payload)
			require.NoError(t, err)
			owner, ok := deadLetterPostprocessOwner(asynq.NewTask(tt.taskType, payload))
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, owner.Name)
			if ok {
				require.Equal(t, "kid", owner.KnowledgeID)
				require.Equal(t, 3, owner.Attempt)
			}
		})
	}
}

func TestDeadLetterPostprocessOwnerFinalizesKnowledge(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(deadLetterPostprocessTestDDL).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, parse_status, pending_subtasks_count) VALUES (?, ?, ?)`,
		"kid-finalizing", types.ParseStatusFinalizing, 1).Error)

	spanRepo := repository.NewKnowledgeSpanRepository(db)
	tracker := service.NewSpanTracker(spanRepo, nil)
	root, attempt, err := tracker.OpenAttempt(context.Background(), "kid-finalizing", "")
	require.NoError(t, err)
	require.NotNil(t, root)
	post := tracker.BeginStage(context.Background(), "kid-finalizing", attempt,
		types.StagePostProcess, types.JSONMap{
			"expected_branches": []string{"postprocess.summary"},
			"fanout_complete":   true,
		})
	require.NotNil(t, post)
	owner := tracker.BeginSubSpan(context.Background(), post, "postprocess.summary",
		types.SpanKindSubSpan, nil)
	require.NotNil(t, owner)

	payload, err := json.Marshal(types.SummaryGenerationPayload{
		KnowledgeID: "kid-finalizing", Attempt: attempt,
	})
	require.NoError(t, err)
	handled := failDeadLetterPostprocessOwner(context.Background(), tracker,
		asynq.NewTask(types.TypeSummaryGeneration, payload), errors.New("provider unavailable"))
	require.True(t, handled)

	var ownerStatus, ownerCode, knowledgeStatus string
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("status", "error_code").Where("span_id = ?", owner.SpanID).
		Row().Scan(&ownerStatus, &ownerCode))
	require.Equal(t, types.SpanStatusFailed, ownerStatus)
	require.Equal(t, "TASK_RETRIES_EXHAUSTED", ownerCode)
	require.NoError(t, db.Table("knowledges").Select("parse_status").
		Where("id = ?", "kid-finalizing").Row().Scan(&knowledgeStatus))
	require.Equal(t, types.ParseStatusFailed, knowledgeStatus,
		"the reducer must move the document out of finalizing")
}
