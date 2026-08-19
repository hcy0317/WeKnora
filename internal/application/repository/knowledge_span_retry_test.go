package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupKnowledgeSpanRetryRepo(t *testing.T) (KnowledgeSpanRepository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(spansTestDDL).Error)
	require.NoError(t, db.Exec(knowledgeCompletionOutboxTestDDL).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id VARCHAR(64) PRIMARY KEY, tenant_id INTEGER NOT NULL,
		knowledge_base_id VARCHAR(64) NOT NULL, parse_status VARCHAR(32) NOT NULL,
		pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
		summary_status VARCHAR(32) NOT NULL DEFAULT 'none', error_message TEXT NOT NULL DEFAULT '',
		processed_at DATETIME, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE task_pending_ops (
		id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id INTEGER NOT NULL, task_type VARCHAR(64) NOT NULL,
		scope VARCHAR(32) NOT NULL, scope_id VARCHAR(64) NOT NULL, op VARCHAR(32) NOT NULL,
		dedup_key VARCHAR(255) NOT NULL, payload BLOB NOT NULL, fail_count INTEGER NOT NULL DEFAULT 0,
		enqueued_at DATETIME DEFAULT CURRENT_TIMESTAMP, claimed_at DATETIME,
		claim_token VARCHAR(64), claimed_by_task_id VARCHAR(255), claim_heartbeat_at DATETIME
	)`).Error)
	return NewKnowledgeSpanRepository(db), db
}

func seedRetryAttempt(t *testing.T, repo KnowledgeSpanRepository, knowledgeID, targetName string) {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root-old", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "target-old", ParentSpanID: "post-old", Name: targetName, Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model-1"}, StartedAt: &now, FinishedAt: &finished},
	}
	for _, row := range rows {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
}

func seedMultiRetryAttempt(t *testing.T, repo KnowledgeSpanRepository, knowledgeID string) {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root-old", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "summary-old", ParentSpanID: "post-old", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "wiki-old", ParentSpanID: "post-old", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "graph-old", ParentSpanID: "post-old", Name: "postprocess.graph.chunk[7]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"chunk_id": "chunk-7", "chunk_index": 7, "model_id": "model-1"}, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-old", ParentSpanID: "post-old", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-1-old", ParentSpanID: "question-old", Name: "postprocess.question.batch[1]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"batch_index": 1, "chunk_ids": []any{"c1"}, "prev_chunk_id": "", "next_chunk_id": "c3", "question_count": 3, "language": "zh-CN"}, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-batch-3-old", ParentSpanID: "question-old", Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"batch_index": 3, "chunk_ids": []any{"c3"}, "prev_chunk_id": "c1", "next_chunk_id": "", "question_count": 3, "language": "zh-CN"}, StartedAt: &now, FinishedAt: &finished},
	}
	for _, row := range rows {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesSeedsOneCanonicalSparseAttempt(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-multi', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-multi")

	prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-multi", Attempt: 4, ClientRequestID: "request-multi",
		Targets: []types.KnowledgeSpanMultiRetryTarget{
			{SpanID: "question-batch-3-old"}, {SpanID: "graph-old"}, {SpanID: "summary-old"},
			{SpanID: "question-batch-3-old"}, // exact duplicate is idempotently deduped
		},
	})
	require.NoError(t, err)
	require.Len(t, prepared, 3)
	require.Equal(t, []string{"postprocess.summary", "postprocess.question.batch[3]", "postprocess.graph.chunk[7]"},
		[]string{prepared[0].Name, prepared[1].Name, prepared[2].Name})
	require.Equal(t, prepared[0].Attempt, prepared[2].Attempt)

	var roots, posts int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ? AND kind = ?", "kid-multi", 5, types.SpanKindRoot).Count(&roots).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-multi", 5, types.StagePostProcess).Count(&posts).Error)
	require.EqualValues(t, 1, roots)
	require.EqualValues(t, 1, posts)
	var post types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-multi", 5, types.StagePostProcess).Take(&post).Error)
	require.Equal(t, []any{"postprocess.summary", "postprocess.question", "postprocess.graph.chunk[7]"}, post.Input["expected_branches"])
	require.Equal(t, []any{"postprocess.question.batch[1]", "postprocess.question.batch[3]"},
		post.Input["expected_question_children"],
		"fresh settlement plan must preserve unresolved unselected question failures")
	var carried int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND input LIKE ?", "kid-multi", 5, "%carryover_source_span_id%").
		Count(&carried).Error)
	require.EqualValues(t, 1, carried,
		"unselected unresolved failures must be copied as terminal settlement evidence")

	var pending int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ? AND status = ?", "kid-multi", 5, types.SpanStatusPending).Count(&pending).Error)
	require.EqualValues(t, 3, pending, "only exact targets are pending; carried failures remain terminal")
	var knowledge struct{ PendingSubtasksCount int }
	require.NoError(t, db.Table("knowledges").Select("pending_subtasks_count").Where("id = ?", "kid-multi").Take(&knowledge).Error)
	require.Equal(t, 3, knowledge.PendingSubtasksCount)

	var outboxes []types.TaskPendingOp
	require.NoError(t, db.Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Order("dedup_key").Find(&outboxes).Error)
	require.Len(t, outboxes, 3)
	for i, outbox := range outboxes {
		var payload types.KnowledgeSpanRetryOutboxPayload
		require.NoError(t, json.Unmarshal(outbox.Payload, &payload))
		require.NotEmpty(t, payload.TaskID, "outbox %d", i)
		require.Equal(t, 5, payload.Attempt)
	}
	now := time.Now()
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status = ?", "kid-multi", 5, types.SpanStatusPending).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": now, "finished_at": now}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid-multi", 5))
	var settledPost types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-multi", 5, types.StagePostProcess).Take(&settledPost).Error)
	require.Equal(t, types.SpanStatusFailed, settledPost.Status,
		"carried unresolved failure must prevent a false completed repair attempt")
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesSingletonQuestionUsesCanonicalPlan(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-question-one', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-question-one")

	prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-question-one", Attempt: 4, ClientRequestID: "question-one",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "question-batch-3-old"}},
	})
	require.NoError(t, err)
	require.Len(t, prepared, 1)
	require.Equal(t, "postprocess.question.batch[3]", prepared[0].Name)
	var post, question types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-question-one", 5, types.StagePostProcess).Take(&post).Error)
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-question-one", 5, "postprocess.question").Take(&question).Error)
	require.Equal(t, []any{"postprocess.summary", "postprocess.question", "postprocess.graph.chunk[7]"},
		post.Input["expected_branches"])
	require.Equal(t, []any{"postprocess.question.batch[1]", "postprocess.question.batch[3]"},
		post.Input["expected_question_children"])
	require.Equal(t, []any{"postprocess.question.batch[1]", "postprocess.question.batch[3]"},
		question.Input["expected_question_children"])
	var pending, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status = ?", "kid-question-one", 5, types.SpanStatusPending).Count(&pending).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, "kid-question-one").Count(&outboxes).Error)
	require.EqualValues(t, 1, pending)
	require.EqualValues(t, 1, outboxes)
	var carryovers []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND input LIKE ?",
		"kid-question-one", 5, "%carryover_source_span_id%").Order("name").Find(&carryovers).Error)
	require.Len(t, carryovers, 3)
	require.ElementsMatch(t, []string{"postprocess.summary", "postprocess.question.batch[1]", "postprocess.graph.chunk[7]"},
		[]string{carryovers[0].Name, carryovers[1].Name, carryovers[2].Name})
	for _, carryover := range carryovers {
		require.Equal(t, types.SpanStatusFailed, carryover.Status)
		require.False(t, carryover.Input["retry_target"].(bool))
	}
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesRejectsActiveQuestionSiblingUnderLock(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-question-active-sibling', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-question-active-sibling")
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid-question-active-sibling", 4, "question-old").
		Update("status", types.SpanStatusRunning).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid-question-active-sibling", 4, "question-batch-1-old").
		Update("status", types.SpanStatusPending).Error)

	_, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-question-active-sibling", Attempt: 4, ClientRequestID: "question-active-sibling",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "question-batch-3-old"}},
	})

	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)
	var rows, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid-question-active-sibling", 5).Count(&rows).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("scope_id = ?", "kid-question-active-sibling").Count(&outboxes).Error)
	require.Zero(t, rows)
	require.Zero(t, outboxes)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesAllStalledAndMixedPlansAreExact(t *testing.T) {
	for _, test := range []struct {
		name, summaryStatus string
	}{
		{name: "all stalled", summaryStatus: types.SpanStatusRunning},
		{name: "failed summary plus stalled siblings", summaryStatus: types.SpanStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, db := setupKnowledgeSpanRetryRepo(t)
			knowledgeID := "kid-four-owner-" + strings.ReplaceAll(test.name, " ", "-")
			require.NoError(t, db.Exec(`INSERT INTO knowledges
				(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
				VALUES (?, 7, 'kb', 'finalizing', 4)`, knowledgeID).Error)
			stale := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
			finished := stale.Add(time.Minute)
			rows := []*types.KnowledgeProcessingSpan{
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root", Name: "knowledge_processing",
					Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess,
					Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary",
					Kind: types.SpanKindSubSpan, Status: test.summaryStatus, StartedAt: &stale, UpdatedAt: stale},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki",
					Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
					Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale,
					Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
					Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
					Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
					StartedAt: &stale, UpdatedAt: stale,
					Input: types.JSONMap{"batch_index": 3, "chunk_ids": []any{"chunk-3"}, "question_count": 1}},
				{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "done-graph", ParentSpanID: "post",
					Name: "postprocess.graph.chunk[4]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone,
					StartedAt: &stale, FinishedAt: &finished, UpdatedAt: finished,
					Input: types.JSONMap{"chunk_id": "chunk-4", "chunk_index": 4, "model_id": "model"}},
			}
			for _, row := range rows {
				require.NoError(t, repo.Upsert(context.Background(), row))
			}
			oldWiki := &types.TaskPendingOp{TenantID: 7, TaskType: types.TypeWikiIngest,
				Scope: types.TaskScopeKnowledgeBase, ScopeID: "kb", Op: "ingest", DedupKey: knowledgeID,
				Payload: []byte(`{"op":"ingest"}`), ClaimedAt: &stale, ClaimHeartbeatAt: &stale,
				ClaimToken: "wiki-claim", ClaimedByTaskID: "wiki-delivery"}
			require.NoError(t, db.Create(oldWiki).Error)
			fence := func(spanID, name, queue, taskID string) *types.KnowledgeSpanRetryStallFence {
				return &types.KnowledgeSpanRetryStallFence{KnowledgeID: knowledgeID, TenantID: 7,
					OwnerName: name, SourceAttempt: 4, SourceSpanID: spanID, SourceUpdatedAt: stale,
					LatestRootAttempt: 4, LastHeartbeatAt: stale, Queue: queue, TaskID: taskID}
			}
			targets := []types.KnowledgeSpanMultiRetryTarget{
				{SpanID: "summary"},
				{SpanID: "wiki", StallFence: fence("wiki", "postprocess.wiki", types.QueueWiki, "wiki-delivery")},
				{SpanID: "graph", StallFence: fence("graph", "postprocess.graph.chunk[3]", types.QueueGraph,
					"knowledge-fanout:"+knowledgeID+":4:graph:3")},
				{SpanID: "question", StallFence: fence("question", "postprocess.question.batch[3]", types.QueueQuestion,
					"knowledge-fanout:"+knowledgeID+":4:question:3")},
			}
			if test.summaryStatus == types.SpanStatusRunning {
				targets[0].StallFence = fence("summary", "postprocess.summary", types.QueueSummary,
					"knowledge-fanout:"+knowledgeID+":4:summary")
			}
			targets[1].StallFence.PendingOpIDs = []int64{oldWiki.ID}
			targets[1].StallFence.ClaimToken = oldWiki.ClaimToken
			targets[1].StallFence.ClaimedByTaskID = oldWiki.ClaimedByTaskID
			targets[1].StallFence.ClaimHeartbeatAt = stale

			prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
				KnowledgeID: knowledgeID, Attempt: 4, ClientRequestID: "four-owner", Targets: targets,
			})
			require.NoError(t, err)
			require.Len(t, prepared, 4)
			for _, item := range prepared {
				require.Equal(t, 5, item.Attempt)
			}
			var outboxes, carryovers, doneReruns int64
			require.NoError(t, db.Model(&types.TaskPendingOp{}).
				Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, knowledgeID).
				Count(&outboxes).Error)
			require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND attempt = ? AND input LIKE ?", knowledgeID, 5, "%carryover_source_span_id%").
				Count(&carryovers).Error)
			require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND attempt = ? AND input LIKE ?", knowledgeID, 5, "%done-graph%").
				Count(&doneReruns).Error)
			require.EqualValues(t, 4, outboxes)
			require.Zero(t, carryovers)
			require.Zero(t, doneReruns)
		})
	}
}

func TestKnowledgeSpanRepo_SingleRetryTerminalizesStalledSiblingsAsCarryover(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	const knowledgeID = "kid-row-stalled-closure"
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES (?, 7, 'kb', 'finalizing', 3)`, knowledgeID).Error)
	stale := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess,
			Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary",
			Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
			Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale,
			Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question",
			Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			StartedAt: &stale, UpdatedAt: stale,
			Input: types.JSONMap{"batch_index": 3, "chunk_ids": []any{"chunk-3"}, "question_count": 1}},
	}
	for _, row := range rows {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	fence := func(spanID, name, queue, taskID string) *types.KnowledgeSpanRetryStallFence {
		return &types.KnowledgeSpanRetryStallFence{KnowledgeID: knowledgeID, TenantID: 7,
			OwnerName: name, SourceAttempt: 4, SourceSpanID: spanID, SourceUpdatedAt: stale,
			LatestRootAttempt: 4, LastHeartbeatAt: stale, Queue: queue, TaskID: taskID}
	}
	prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: knowledgeID, Attempt: 4, ClientRequestID: "row-summary",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "summary", StallFence: fence(
			"summary", "postprocess.summary", types.QueueSummary, "knowledge-fanout:"+knowledgeID+":4:summary")}},
		CarryoverFences: []*types.KnowledgeSpanRetryStallFence{
			fence("graph", "postprocess.graph.chunk[3]", types.QueueGraph, "knowledge-fanout:"+knowledgeID+":4:graph:3"),
			fence("question", "postprocess.question.batch[3]", types.QueueQuestion, "knowledge-fanout:"+knowledgeID+":4:question:3"),
		},
	})
	require.NoError(t, err)
	require.Len(t, prepared, 1)
	require.Equal(t, "postprocess.summary", prepared[0].Name)
	var outboxes, pending, carryovers int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, knowledgeID).Count(&outboxes).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status = ?", knowledgeID, 5, types.SpanStatusPending).Count(&pending).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND input LIKE ?", knowledgeID, 5, "%carryover_source_span_id%").Count(&carryovers).Error)
	require.EqualValues(t, 1, outboxes)
	require.EqualValues(t, 1, pending)
	require.EqualValues(t, 2, carryovers)
	var repairPost, repairQuestion types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 5,
		types.StagePostProcess).Take(&repairPost).Error)
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 5,
		"postprocess.question").Take(&repairQuestion).Error)
	require.EqualValues(t, 1, repairPost.Input["expected_subtasks_count"])
	require.ElementsMatch(t, []any{"postprocess.summary", "postprocess.graph.chunk[3]", "postprocess.question"},
		repairPost.Input["expected_branches"])
	require.Equal(t, []any{"postprocess.question.batch[3]"}, repairPost.Input["expected_question_children"])
	require.Equal(t, []any{"postprocess.question.batch[3]"}, repairQuestion.Input["expected_question_children"])

	doneAt := time.Now()
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", knowledgeID, 5, prepared[0].SpanID).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": doneAt, "finished_at": doneAt}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), knowledgeID, 5))
	for _, name := range []string{"postprocess.question", types.StagePostProcess, "knowledge_processing"} {
		var settled types.KnowledgeProcessingSpan
		require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 5,
			name).Take(&settled).Error)
		require.Equal(t, types.SpanStatusFailed, settled.Status, name)
	}
	var carriedQuestion types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 5,
		"postprocess.question.batch[3]").Take(&carriedQuestion).Error)
	second, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: knowledgeID, Attempt: 5, ClientRequestID: "row-question",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: carriedQuestion.SpanID}},
	})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, "postprocess.question.batch[3]", second[0].Name)
	var secondOutboxes int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, knowledgeID).Count(&secondOutboxes).Error)
	require.EqualValues(t, 2, secondOutboxes, "the later row retry adds only its one exact outbox")
	var secondQuestion types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 6,
		"postprocess.question").Take(&secondQuestion).Error)
	require.Equal(t, []any{"postprocess.question.batch[3]"}, secondQuestion.Input["expected_question_children"])
}

func TestKnowledgeSpanRepo_CarryoverDoesNotRequireLegacyQuestionReplayPayload(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	const knowledgeID = "kid-legacy-question-carryover"
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES (?, 7, 'kb', 'finalizing', 2)`, knowledgeID).Error)
	stale := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "root", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess,
			Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "graph", ParentSpanID: "post", Name: "postprocess.graph.chunk[3]",
			Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale,
			Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model"}},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question-group", ParentSpanID: "post",
			Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			StartedAt: &stale, UpdatedAt: stale},
		{KnowledgeID: knowledgeID, Attempt: 4, SpanID: "question", ParentSpanID: "question-group",
			Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan,
			Status: types.SpanStatusRunning, StartedAt: &stale, UpdatedAt: stale,
			Input: types.JSONMap{"batch_index": 3, "chunks": 20, "question_count": 4}},
	}
	for _, row := range rows {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	fence := func(spanID, name, queue, taskID string) *types.KnowledgeSpanRetryStallFence {
		return &types.KnowledgeSpanRetryStallFence{KnowledgeID: knowledgeID, TenantID: 7,
			OwnerName: name, SourceAttempt: 4, SourceSpanID: spanID, SourceUpdatedAt: stale,
			LatestRootAttempt: 4, LastHeartbeatAt: stale, Queue: queue, TaskID: taskID}
	}
	prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: knowledgeID, Attempt: 4, ClientRequestID: "legacy-question",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "graph", StallFence: fence(
			"graph", "postprocess.graph.chunk[3]", types.QueueGraph,
			"knowledge-fanout:"+knowledgeID+":4:graph:3")}},
		CarryoverFences: []*types.KnowledgeSpanRetryStallFence{fence(
			"question", "postprocess.question.batch[3]", types.QueueQuestion,
			"knowledge-fanout:"+knowledgeID+":4:question:3")},
	})
	require.NoError(t, err)
	require.Len(t, prepared, 1)
	require.Equal(t, "postprocess.graph.chunk[3]", prepared[0].Name)
	var carried types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND input LIKE ?", knowledgeID, 5,
		"%carryover_source_span_id%").Take(&carried).Error)
	require.Equal(t, "postprocess.question.batch[3]", carried.Name)
	require.Equal(t, types.SpanStatusFailed, carried.Status)
}

func TestKnowledgeSpanRepo_SparseQuestionBatchThreeSuccessSettlesWholeRepair(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-question-sparse-success', 7, 'kb', 'failed')`).Error)
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	rows := []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-question-sparse-success", Attempt: 4, SpanID: "root-old", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-sparse-success", Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-sparse-success", Attempt: 4, SpanID: "question-old", ParentSpanID: "post-old", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-sparse-success", Attempt: 4, SpanID: "batch-3-old", ParentSpanID: "question-old", Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			Input: types.JSONMap{"batch_index": 3, "chunk_ids": []any{"chunk-31"}, "question_count": 3}, StartedAt: &now, FinishedAt: &finished},
	}
	for _, row := range rows {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}

	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-question-sparse-success", Attempt: 4, SpanID: "batch-3-old",
		ClientRequestID: "sparse-batch-3-success",
	})
	require.NoError(t, err)
	require.Equal(t, 5, prepared.Attempt)
	require.Equal(t, "postprocess.question.batch[3]", prepared.Name)
	var post, question types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid-question-sparse-success", 5, types.StagePostProcess).Take(&post).Error)
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid-question-sparse-success", 5, "postprocess.question").Take(&question).Error)
	require.Equal(t, []any{"postprocess.question.batch[3]"}, post.Input["expected_question_children"])
	require.Equal(t, []any{"postprocess.question.batch[3]"}, question.Input["expected_question_children"])

	doneAt := time.Now()
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			"kid-question-sparse-success", 5, prepared.SpanID).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": doneAt, "finished_at": doneAt}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid-question-sparse-success", 5))

	for _, name := range []string{"postprocess.question", types.StagePostProcess, "knowledge_processing"} {
		var span types.KnowledgeProcessingSpan
		require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
			"kid-question-sparse-success", 5, name).Take(&span).Error)
		require.Equal(t, types.SpanStatusDone, span.Status, name)
	}
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", "kid-question-sparse-success").Take(&knowledge).Error)
	require.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
	require.Zero(t, knowledge.PendingSubtasksCount)
	var source types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-question-sparse-success", 4, "batch-3-old").Take(&source).Error)
	require.Equal(t, types.SpanStatusFailed, source.Status, "source attempt history must remain immutable")
}

func TestKnowledgeSpanRepo_QuestionPublicationFailureFailsNewAttemptAndPreservesSource(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-question-publication-fail', 7, 'kb', 'failed')`).Error)
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-question-publication-fail", Attempt: 4, SpanID: "root-old", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-publication-fail", Attempt: 4, SpanID: "post-old", ParentSpanID: "root-old", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-publication-fail", Attempt: 4, SpanID: "question-old", ParentSpanID: "post-old", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid-question-publication-fail", Attempt: 4, SpanID: "batch-3-old", ParentSpanID: "question-old", Name: "postprocess.question.batch[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
			Input:     types.JSONMap{"batch_index": 3, "chunk_ids": []any{"chunk-31"}, "question_count": 3},
			ErrorCode: "QUESTION_FAILED", ErrorMessage: "old failure", StartedAt: &now, FinishedAt: &finished},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-question-publication-fail", Attempt: 4, SpanID: "batch-3-old",
		ClientRequestID: "publication-index-failure",
	})
	require.NoError(t, err)
	failureAt := time.Now()
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			"kid-question-publication-fail", prepared.Attempt, prepared.SpanID).
		Updates(map[string]any{
			"status": types.SpanStatusFailed, "error_code": "QUESTION_PUBLICATION_FAILED",
			"error_message": "partial backend failure", "started_at": failureAt, "finished_at": failureAt,
		}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid-question-publication-fail", prepared.Attempt))

	for _, name := range []string{"postprocess.question.batch[3]", "postprocess.question", types.StagePostProcess, "knowledge_processing"} {
		var span types.KnowledgeProcessingSpan
		require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
			"kid-question-publication-fail", prepared.Attempt, name).Take(&span).Error)
		require.Equal(t, types.SpanStatusFailed, span.Status, name)
	}
	var knowledgeStatus string
	require.NoError(t, db.Table("knowledges").Select("parse_status").
		Where("id = ?", "kid-question-publication-fail").Scan(&knowledgeStatus).Error)
	require.Equal(t, types.ParseStatusFailed, knowledgeStatus)
	var source types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-question-publication-fail", 4, "batch-3-old").Take(&source).Error)
	require.Equal(t, types.SpanStatusFailed, source.Status)
	require.Equal(t, "old failure", source.ErrorMessage)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesRejectsUnreplayableQuestionInput(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-question-invalid', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-question-invalid")
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			"kid-question-invalid", 4, "question-batch-3-old").
		Update("input", types.JSONMap{"batch_index": 3}).Error)

	_, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-question-invalid", Attempt: 4, ClientRequestID: "question-invalid",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "question-batch-3-old"}},
	})

	require.ErrorIs(t, err, ErrKnowledgeSpanRetryUnsupported)
	var freshRows, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid-question-invalid", 5).Count(&freshRows).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Count(&outboxes).Error)
	require.Zero(t, freshRows)
	require.Zero(t, outboxes)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesReplayAndConcurrentIdempotency(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-replay', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-replay")
	request := types.KnowledgeSpanMultiRetryRequest{KnowledgeID: "kid-replay", Attempt: 4, ClientRequestID: "same-request",
		Language: "zh-CN",
		Targets:  []types.KnowledgeSpanMultiRetryTarget{{SpanID: "summary-old"}, {SpanID: "graph-old"}}}

	results := make(chan []*types.KnowledgeSpanRetryPreparation, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, callErr := repo.PrepareFailedSpanRetries(context.Background(), request)
			results <- got
			errs <- callErr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for callErr := range errs {
		require.NoError(t, callErr)
	}
	var first []*types.KnowledgeSpanRetryPreparation
	for got := range results {
		require.Len(t, got, 2)
		if first == nil {
			first = got
		} else {
			require.Equal(t, first, got)
		}
	}
	reversed := request
	reversed.Language = "en-US"
	reversed.Targets = []types.KnowledgeSpanMultiRetryTarget{{SpanID: "graph-old"}, {SpanID: "summary-old"}}
	again, err := repo.PrepareFailedSpanRetries(context.Background(), reversed)
	require.NoError(t, err)
	require.Equal(t, first, again)
	for _, prepared := range again {
		require.Equal(t, "zh-CN", prepared.Language,
			"idempotent replay must use the persisted canonical dispatch language")
	}
	_, err = repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-replay", Attempt: 4, ClientRequestID: "same-request",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "summary-old"}, {SpanID: "question-batch-3-old"}},
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotLatest,
		"one deterministic root/client request cannot be replayed with a different exact target plan")
	var roots, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ? AND kind = ?", "kid-replay", 5, types.SpanKindRoot).Count(&roots).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Count(&outboxes).Error)
	require.EqualValues(t, 1, roots)
	require.EqualValues(t, 2, outboxes)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesCombinesStalledWikiAndFailedGraph(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-mixed', 7, 'kb', 'finalizing', 1)`).Error)
	heartbeat := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	finished := heartbeat.Add(time.Minute)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-mixed", Attempt: 4, SpanID: "root-mixed", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-mixed", Attempt: 4, SpanID: "post-mixed", ParentSpanID: "root-mixed", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-mixed", Attempt: 4, SpanID: "wiki-mixed", ParentSpanID: "post-mixed", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-mixed", Attempt: 4, SpanID: "graph-mixed", ParentSpanID: "post-mixed", Name: "postprocess.graph.chunk[7]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, Input: types.JSONMap{"chunk_id": "chunk-7", "chunk_index": 7, "model_id": "model-1"}, StartedAt: &heartbeat, FinishedAt: &finished},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	claimHeartbeat := heartbeat
	oldWikiOp := &types.TaskPendingOp{TenantID: 7, TaskType: types.TypeWikiIngest,
		Scope: types.TaskScopeKnowledgeBase, ScopeID: "kb", Op: "ingest", DedupKey: "kid-mixed",
		Payload:   []byte(`{"op":"ingest","knowledge_id":"kid-mixed","attempt":4}`),
		ClaimedAt: &claimHeartbeat, ClaimHeartbeatAt: &claimHeartbeat,
		ClaimToken: "wiki-claim", ClaimedByTaskID: "wiki-delivery"}
	require.NoError(t, db.Create(oldWikiOp).Error)
	var wikiSource types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-mixed", 4, "wiki-mixed").Take(&wikiSource).Error)

	prepared, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-mixed", Attempt: 4, ClientRequestID: "repair-mixed",
		Targets: []types.KnowledgeSpanMultiRetryTarget{
			{SpanID: "wiki-mixed", StallFence: &types.KnowledgeSpanRetryStallFence{
				KnowledgeID: "kid-mixed", TenantID: 7, OwnerName: "postprocess.wiki",
				SourceAttempt: 4, SourceSpanID: "wiki-mixed",
				SourceUpdatedAt: wikiSource.UpdatedAt, LatestRootAttempt: 4, LastHeartbeatAt: heartbeat,
				TaskID: "wiki-delivery", Queue: types.QueueWiki, PendingOpIDs: []int64{oldWikiOp.ID},
				ClaimToken: "wiki-claim", ClaimedByTaskID: "wiki-delivery", ClaimHeartbeatAt: claimHeartbeat,
			}},
			{SpanID: "graph-mixed"},
		},
	})
	require.NoError(t, err)
	require.Len(t, prepared, 2)
	require.Equal(t, []string{"postprocess.wiki", "postprocess.graph.chunk[7]"},
		[]string{prepared[0].Name, prepared[1].Name})

	var oldRoot, oldPost, oldWiki types.KnowledgeProcessingSpan
	for spanID, destination := range map[string]*types.KnowledgeProcessingSpan{
		"root-mixed": &oldRoot, "post-mixed": &oldPost, "wiki-mixed": &oldWiki,
	} {
		require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			"kid-mixed", 4, spanID).Take(destination).Error)
		require.Equal(t, types.SpanStatusFailed, destination.Status)
	}
	var roots, posts, pendingTargets, dispatchOutboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND kind = ?", "kid-mixed", 5, types.SpanKindRoot).Count(&roots).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-mixed", 5, types.StagePostProcess).Count(&posts).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status = ?", "kid-mixed", 5, types.SpanStatusPending).Count(&pendingTargets).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Count(&dispatchOutboxes).Error)
	require.EqualValues(t, 1, roots)
	require.EqualValues(t, 1, posts)
	require.EqualValues(t, 2, pendingTargets)
	require.EqualValues(t, 2, dispatchOutboxes)

	var wikiOps []types.TaskPendingOp
	require.NoError(t, db.Where("task_type = ? AND scope_id = ? AND dedup_key = ?",
		types.TypeWikiIngest, "kb", "kid-mixed").Find(&wikiOps).Error)
	require.Len(t, wikiOps, 1)
	require.NotEqual(t, oldWikiOp.ID, wikiOps[0].ID)
	var wikiPayload map[string]any
	require.NoError(t, json.Unmarshal(wikiOps[0].Payload, &wikiPayload))
	require.EqualValues(t, 5, wikiPayload["attempt"])
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetriesRejectsConflictingLogicalDuplicateAndRollsBack(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid-conflict', 7, 'kb', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-conflict")
	now := time.Now()
	require.NoError(t, repo.Upsert(context.Background(), &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-conflict", Attempt: 4, SpanID: "summary-new", ParentSpanID: "post-old",
		Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
		StartedAt: &now, FinishedAt: &now,
	}))

	_, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-conflict", Attempt: 4, ClientRequestID: "conflict",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "summary-old"}, {SpanID: "summary-new"}},
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotLatest)
	var rows, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ?", "kid-conflict", 5).Count(&rows).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&outboxes).Error)
	require.Zero(t, rows)
	require.Zero(t, outboxes)

	require.NoError(t, db.Exec("DROP TABLE task_pending_ops").Error)
	_, err = repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-conflict", Attempt: 4, ClientRequestID: "db-failure",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{SpanID: "summary-new"}, {SpanID: "graph-old"}},
	})
	require.Error(t, err)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ?", "kid-conflict", 5).Count(&rows).Error)
	require.Zero(t, rows, fmt.Sprintf("all attempt writes must roll back: %d", rows))
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetrySeedsPartialRepairAtomically(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.graph.chunk[3]")

	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-1",
	})
	require.NoError(t, err)
	require.Equal(t, 5, prepared.Attempt)
	require.True(t, prepared.DispatchRequired)
	require.Equal(t, "postprocess.graph.chunk[3]", prepared.Name)
	require.Equal(t, "chunk-3", prepared.Input["chunk_id"])

	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ?", "kid", 5).Order("id").Find(&rows).Error)
	require.Len(t, rows, 7)
	require.Equal(t, types.SpanStatusRunning, rows[0].Status)
	require.Equal(t, "partial_repair", rows[0].Input["attempt_kind"])
	for _, stage := range rows[1:5] {
		require.Equal(t, types.SpanStatusSkipped, stage.Status)
	}
	require.Equal(t, types.StagePostProcess, rows[5].Name)
	require.Equal(t, types.SpanStatusRunning, rows[5].Status)
	require.Equal(t, types.SpanStatusPending, rows[6].Status)

	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Select("parse_status", "pending_subtasks_count").
		Where("id = ?", "kid").Take(&knowledge).Error)
	require.Equal(t, types.ParseStatusFinalizing, knowledge.ParseStatus)
	require.Equal(t, 1, knowledge.PendingSubtasksCount)
	var outbox types.TaskPendingOp
	require.NoError(t, db.Where("task_type = ? AND scope_id = ? AND dedup_key = ?",
		types.KnowledgeSpanRetryOutboxTaskType, "kid", prepared.TaskID).Take(&outbox).Error)
	require.Contains(t, string(outbox.Payload), `"target_name":"postprocess.graph.chunk[3]"`)

	again, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-1",
	})
	require.NoError(t, err)
	require.Equal(t, prepared.Attempt, again.Attempt)
	require.True(t, again.DispatchRequired)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ?", "kid", 5).Count(&count).Error)
	require.EqualValues(t, 7, count)

	require.NoError(t, db.Where("task_type = ? AND dedup_key = ?",
		types.KnowledgeSpanRetryOutboxTaskType, prepared.TaskID).Delete(&types.TaskPendingOp{}).Error)
	acknowledged, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-1",
	})
	require.NoError(t, err)
	require.Equal(t, types.SpanStatusPending, acknowledged.Status)
	require.False(t, acknowledged.DispatchRequired,
		"pending plus absent deterministic outbox means publication was already acknowledged")
}

func TestKnowledgeSpanRepo_PrepareFailedWikiRetryReplacesClaimedPendingOp(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.wiki")
	require.NoError(t, repo.UpdateInput(context.Background(), "kid", 4, "target-old", types.JSONMap{
		types.WikiIngestWorkBindingInputKey: types.WikiIngestWorkBinding{
			WorkID: "work-bound", SourceRevisionDigest: "source", SourceDocumentKey: "title",
			GenerationContractKey: "contract", RuntimeSnapshotKey: "runtime",
		},
	}))
	require.NoError(t, db.Exec(`INSERT INTO task_pending_ops
		(tenant_id, task_type, scope, scope_id, op, dedup_key, payload, fail_count, claimed_at)
		VALUES (7, 'wiki:ingest', 'knowledge_base', 'kb', 'ingest', 'kid', '{}', 2, CURRENT_TIMESTAMP)`).Error)

	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-wiki",
	})
	require.NoError(t, err)
	require.Equal(t, 5, prepared.Attempt)
	require.Equal(t, "work-bound", retryWikiWorkID(prepared.Input))

	var ops []types.TaskPendingOp
	require.NoError(t, db.Where("dedup_key = ?", "kid").Find(&ops).Error)
	require.Len(t, ops, 1)
	require.Contains(t, string(ops[0].Payload), `"work_id":"work-bound"`)
	require.Nil(t, ops[0].ClaimedAt)
	require.Zero(t, ops[0].FailCount)
	require.Contains(t, string(ops[0].Payload), `"attempt":5`)
}

func TestKnowledgeSpanRepo_PrepareStalledSpanRetryAtomicallyClosesOrphan(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-stalled', 7, 'kb', 'finalizing', 1)`).Error)
	heartbeat := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-stalled", Attempt: 4, SpanID: "root-stalled", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-stalled", Attempt: 4, SpanID: "post-stalled", ParentSpanID: "root-stalled", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-stalled", Attempt: 4, SpanID: "target-stalled", ParentSpanID: "post-stalled", Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model-1"}, StartedAt: &heartbeat, UpdatedAt: heartbeat},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	var source types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-stalled", 4, "target-stalled").Take(&source).Error)

	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-stalled", Attempt: 4, SpanID: "target-stalled", ClientRequestID: "repair-stalled",
		StallFence: &types.KnowledgeSpanRetryStallFence{KnowledgeID: "kid-stalled", TenantID: 7,
			OwnerName: "postprocess.graph.chunk[3]", SourceAttempt: 4,
			SourceSpanID: "target-stalled", SourceUpdatedAt: source.UpdatedAt,
			LatestRootAttempt: 4, LastHeartbeatAt: source.UpdatedAt,
			TaskID: "knowledge-fanout:kid-stalled:4:graph:3", Queue: types.QueueGraph},
	})
	require.NoError(t, err)
	require.Equal(t, 5, prepared.Attempt)

	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-stalled", 4, "target-stalled").Take(&source).Error)
	require.Equal(t, types.SpanStatusFailed, source.Status)
	require.Equal(t, "ORPHANED_OWNER_RECOVERED", source.ErrorCode)
	require.NotNil(t, source.FinishedAt)
	require.WithinDuration(t, heartbeat, *source.FinishedAt, time.Second)
	var oldRoot types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-stalled", 4, "root-stalled").Take(&oldRoot).Error)
	require.Equal(t, types.SpanStatusFailed, oldRoot.Status)
}

func TestKnowledgeSpanRepo_PrepareStalledSpanRetryRejectsActiveDirectSiblingWithoutWrites(t *testing.T) {
	siblingNames := []string{
		"postprocess.summary",
		"postprocess.question",
		"postprocess.graph.chunk[4]",
		"postprocess.wiki",
	}
	for _, siblingName := range siblingNames {
		for _, siblingStatus := range []string{types.SpanStatusPending, types.SpanStatusRunning} {
			t.Run(siblingName+"/"+siblingStatus, func(t *testing.T) {
				repo, db := setupKnowledgeSpanRetryRepo(t)
				require.NoError(t, db.Exec(`INSERT INTO knowledges
					(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count, error_message)
					VALUES ('kid-active-sibling', 7, 'kb', 'finalizing', 2, 'original error')`).Error)
				heartbeat := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
				rows := []*types.KnowledgeProcessingSpan{
					{KnowledgeID: "kid-active-sibling", Attempt: 4, SpanID: "root-active-sibling", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
					{KnowledgeID: "kid-active-sibling", Attempt: 4, SpanID: "post-active-sibling", ParentSpanID: "root-active-sibling", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
					{KnowledgeID: "kid-active-sibling", Attempt: 4, SpanID: "target-active-sibling", ParentSpanID: "post-active-sibling", Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model-1"}, StartedAt: &heartbeat, UpdatedAt: heartbeat},
					{KnowledgeID: "kid-active-sibling", Attempt: 4, SpanID: "sibling-active", ParentSpanID: "post-active-sibling", Name: siblingName, Kind: types.SpanKindSubSpan, Status: siblingStatus, StartedAt: &heartbeat, UpdatedAt: heartbeat},
				}
				for _, row := range rows {
					require.NoError(t, repo.Upsert(context.Background(), row))
				}

				type knowledgeSnapshot struct {
					ParseStatus          string
					PendingSubtasksCount int
					ErrorMessage         string
				}
				var knowledgeBefore knowledgeSnapshot
				require.NoError(t, db.Table("knowledges").Select("parse_status", "pending_subtasks_count", "error_message").
					Where("id = ?", "kid-active-sibling").Take(&knowledgeBefore).Error)
				var sourceBefore, rootBefore, postBefore, siblingBefore types.KnowledgeProcessingSpan
				for spanID, destination := range map[string]*types.KnowledgeProcessingSpan{
					"target-active-sibling": &sourceBefore,
					"root-active-sibling":   &rootBefore,
					"post-active-sibling":   &postBefore,
					"sibling-active":        &siblingBefore,
				} {
					require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
						"kid-active-sibling", 4, spanID).Take(destination).Error)
				}

				_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
					KnowledgeID: "kid-active-sibling", Attempt: 4, SpanID: "target-active-sibling",
					ClientRequestID: "repair-with-active-sibling",
					StallFence: &types.KnowledgeSpanRetryStallFence{
						KnowledgeID: "kid-active-sibling", TenantID: 7,
						OwnerName: "postprocess.graph.chunk[3]", SourceAttempt: 4,
						SourceSpanID: "target-active-sibling", SourceUpdatedAt: sourceBefore.UpdatedAt,
						LatestRootAttempt: 4, LastHeartbeatAt: sourceBefore.UpdatedAt,
						TaskID: "knowledge-fanout:kid-active-sibling:4:graph:3", Queue: types.QueueGraph,
					},
				})
				require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)

				var knowledgeAfter knowledgeSnapshot
				require.NoError(t, db.Table("knowledges").Select("parse_status", "pending_subtasks_count", "error_message").
					Where("id = ?", "kid-active-sibling").Take(&knowledgeAfter).Error)
				require.Equal(t, knowledgeBefore, knowledgeAfter)
				for spanID, before := range map[string]types.KnowledgeProcessingSpan{
					"target-active-sibling": sourceBefore,
					"root-active-sibling":   rootBefore,
					"post-active-sibling":   postBefore,
					"sibling-active":        siblingBefore,
				} {
					var after types.KnowledgeProcessingSpan
					require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
						"kid-active-sibling", 4, spanID).Take(&after).Error)
					require.Equal(t, before, after)
				}
				var newAttemptCount, outboxCount int64
				require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
					Where("knowledge_id = ? AND attempt > ?", "kid-active-sibling", 4).Count(&newAttemptCount).Error)
				require.Zero(t, newAttemptCount)
				require.NoError(t, db.Model(&types.TaskPendingOp{}).Count(&outboxCount).Error)
				require.Zero(t, outboxCount)
			})
		}
	}
}

func TestKnowledgeSpanRepo_PrepareStalledSpanRetryRejectsChangedFenceWithoutWrites(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-race', 7, 'kb', 'finalizing', 1)`).Error)
	heartbeat := time.Now().Add(-2 * time.Hour)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-race", Attempt: 4, SpanID: "root-race", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat},
		{KnowledgeID: "kid-race", Attempt: 4, SpanID: "post-race", ParentSpanID: "root-race", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat},
		{KnowledgeID: "kid-race", Attempt: 4, SpanID: "target-race", ParentSpanID: "post-race", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &heartbeat},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-race", Attempt: 4, SpanID: "target-race", ClientRequestID: "repair-race",
		StallFence: &types.KnowledgeSpanRetryStallFence{KnowledgeID: "kid-race", TenantID: 7,
			OwnerName: "postprocess.summary", SourceAttempt: 4,
			SourceSpanID: "target-race", SourceUpdatedAt: heartbeat.Add(-time.Minute),
			LatestRootAttempt: 4, LastHeartbeatAt: heartbeat,
			TaskID: "knowledge-fanout:kid-race:4:summary", Queue: types.QueueSummary},
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid-race", 5).Count(&count).Error)
	require.Zero(t, count)
}

func TestKnowledgeSpanRepo_PrepareStalledSpanRetryRejectsForgedFenceIdentityWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*types.KnowledgeSpanRetryStallFence)
	}{
		{name: "tenant", mutate: func(fence *types.KnowledgeSpanRetryStallFence) { fence.TenantID = 99 }},
		{name: "owner", mutate: func(fence *types.KnowledgeSpanRetryStallFence) { fence.OwnerName = "postprocess.summary" }},
		{name: "queue", mutate: func(fence *types.KnowledgeSpanRetryStallFence) { fence.Queue = types.QueueSummary }},
		{name: "task id", mutate: func(fence *types.KnowledgeSpanRetryStallFence) { fence.TaskID = "forged-task" }},
		{name: "last heartbeat", mutate: func(fence *types.KnowledgeSpanRetryStallFence) {
			fence.LastHeartbeatAt = fence.SourceUpdatedAt.Add(-time.Minute)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, db := setupKnowledgeSpanRetryRepo(t)
			require.NoError(t, db.Exec(`INSERT INTO knowledges
				(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count, error_message)
				VALUES ('kid-forged-fence', 7, 'kb', 'finalizing', 1, 'original')`).Error)
			heartbeat := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
			for _, row := range []*types.KnowledgeProcessingSpan{
				{KnowledgeID: "kid-forged-fence", Attempt: 4, SpanID: "root-forged", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
				{KnowledgeID: "kid-forged-fence", Attempt: 4, SpanID: "post-forged", ParentSpanID: "root-forged", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
				{KnowledgeID: "kid-forged-fence", Attempt: 4, SpanID: "target-forged", ParentSpanID: "post-forged", Name: "postprocess.graph.chunk[3]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"chunk_id": "chunk-3", "chunk_index": 3, "model_id": "model-1"}, StartedAt: &heartbeat, UpdatedAt: heartbeat},
			} {
				require.NoError(t, repo.Upsert(context.Background(), row))
			}
			var sourceBefore types.KnowledgeProcessingSpan
			require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
				"kid-forged-fence", 4, "target-forged").Take(&sourceBefore).Error)

			fence := &types.KnowledgeSpanRetryStallFence{
				KnowledgeID: "kid-forged-fence", TenantID: 7,
				OwnerName: "postprocess.graph.chunk[3]", SourceAttempt: 4,
				SourceSpanID: "target-forged", SourceUpdatedAt: sourceBefore.UpdatedAt,
				LatestRootAttempt: 4, LastHeartbeatAt: heartbeat,
				TaskID: "knowledge-fanout:kid-forged-fence:4:graph:3", Queue: types.QueueGraph,
			}
			test.mutate(fence)
			_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
				KnowledgeID: "kid-forged-fence", Attempt: 4, SpanID: "target-forged",
				ClientRequestID: "forged-" + strings.ReplaceAll(test.name, " ", "-"), StallFence: fence,
			})
			require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)

			var sourceAfter types.KnowledgeProcessingSpan
			require.NoError(t, db.Where("id = ?", sourceBefore.ID).Take(&sourceAfter).Error)
			require.Equal(t, sourceBefore, sourceAfter)
			var freshRows, outboxes int64
			require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND attempt > ?", "kid-forged-fence", 4).Count(&freshRows).Error)
			require.NoError(t, db.Model(&types.TaskPendingOp{}).
				Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Count(&outboxes).Error)
			require.Zero(t, freshRows)
			require.Zero(t, outboxes)
		})
	}
}

func TestKnowledgeSpanRepo_PrepareStalledWikiRetryClaimTokenCAS(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-wiki-stalled', 7, 'kb', 'finalizing', 1)`).Error)
	heartbeat := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-wiki-stalled", Attempt: 4, SpanID: "root-wiki", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-wiki-stalled", Attempt: 4, SpanID: "post-wiki", ParentSpanID: "root-wiki", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
		{KnowledgeID: "kid-wiki-stalled", Attempt: 4, SpanID: "target-wiki", ParentSpanID: "post-wiki", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &heartbeat, UpdatedAt: heartbeat},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	claimHeartbeat := heartbeat
	op := &types.TaskPendingOp{TenantID: 7, TaskType: types.TypeWikiIngest,
		Scope: types.TaskScopeKnowledgeBase, ScopeID: "kb", Op: "ingest", DedupKey: "kid-wiki-stalled",
		Payload:   []byte(`{"op":"ingest","knowledge_id":"kid-wiki-stalled","attempt":4}`),
		ClaimedAt: &claimHeartbeat, ClaimHeartbeatAt: &claimHeartbeat,
		ClaimToken: "successor-token", ClaimedByTaskID: "wiki-delivery"}
	require.NoError(t, db.Create(op).Error)
	var source types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-wiki-stalled", 4, "target-wiki").Take(&source).Error)
	request := types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-wiki-stalled", Attempt: 4, SpanID: "target-wiki", ClientRequestID: "repair-wiki-stalled",
		StallFence: &types.KnowledgeSpanRetryStallFence{KnowledgeID: "kid-wiki-stalled", TenantID: 7,
			OwnerName: "postprocess.wiki", SourceAttempt: 4,
			SourceSpanID: "target-wiki", SourceUpdatedAt: source.UpdatedAt, LatestRootAttempt: 4,
			LastHeartbeatAt: claimHeartbeat, TaskID: "wiki-delivery", Queue: types.QueueWiki,
			PendingOpIDs: []int64{op.ID}, ClaimToken: "stale-token", ClaimedByTaskID: "wiki-delivery",
			ClaimHeartbeatAt: claimHeartbeat},
	}
	_, err := repo.PrepareFailedSpanRetry(context.Background(), request)
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)
	var sourceAfter types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("id = ?", source.ID).Take(&sourceAfter).Error)
	require.Equal(t, types.SpanStatusRunning, sourceAfter.Status)
	var attemptFiveCount int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid-wiki-stalled", 5).Count(&attemptFiveCount).Error)
	require.Zero(t, attemptFiveCount)
}

func TestKnowledgeSpanRepo_PrepareStalledWikiRetryRejectsForgedHeartbeatWithoutWrites(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-wiki-heartbeat', 7, 'kb', 'finalizing', 1)`).Error)
	updatedAt := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Millisecond)
	claimHeartbeat := updatedAt.Add(time.Hour)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-wiki-heartbeat", Attempt: 4, SpanID: "root-wiki-heartbeat", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &updatedAt, UpdatedAt: updatedAt},
		{KnowledgeID: "kid-wiki-heartbeat", Attempt: 4, SpanID: "post-wiki-heartbeat", ParentSpanID: "root-wiki-heartbeat", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &updatedAt, UpdatedAt: updatedAt},
		{KnowledgeID: "kid-wiki-heartbeat", Attempt: 4, SpanID: "target-wiki-heartbeat", ParentSpanID: "post-wiki-heartbeat", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &updatedAt, UpdatedAt: updatedAt},
	} {
		require.NoError(t, repo.Upsert(context.Background(), row))
	}
	op := &types.TaskPendingOp{TenantID: 7, TaskType: types.TypeWikiIngest,
		Scope: types.TaskScopeKnowledgeBase, ScopeID: "kb", Op: "ingest", DedupKey: "kid-wiki-heartbeat",
		Payload:   []byte(`{"op":"ingest","knowledge_id":"kid-wiki-heartbeat","attempt":4}`),
		ClaimedAt: &claimHeartbeat, ClaimHeartbeatAt: &claimHeartbeat,
		ClaimToken: "wiki-token", ClaimedByTaskID: "wiki-delivery"}
	require.NoError(t, db.Create(op).Error)
	var sourceBefore types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-wiki-heartbeat", 4, "target-wiki-heartbeat").Take(&sourceBefore).Error)

	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid-wiki-heartbeat", Attempt: 4, SpanID: "target-wiki-heartbeat",
		ClientRequestID: "forged-wiki-heartbeat",
		StallFence: &types.KnowledgeSpanRetryStallFence{
			KnowledgeID: "kid-wiki-heartbeat", TenantID: 7, OwnerName: "postprocess.wiki",
			SourceAttempt: 4, SourceSpanID: "target-wiki-heartbeat", SourceUpdatedAt: sourceBefore.UpdatedAt,
			LatestRootAttempt: 4, LastHeartbeatAt: claimHeartbeat.Add(-time.Minute),
			TaskID: "wiki-delivery", Queue: types.QueueWiki, PendingOpIDs: []int64{op.ID},
			ClaimToken: "wiki-token", ClaimedByTaskID: "wiki-delivery", ClaimHeartbeatAt: claimHeartbeat,
		},
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotTerminal)
	var sourceAfter types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("id = ?", sourceBefore.ID).Take(&sourceAfter).Error)
	require.Equal(t, sourceBefore, sourceAfter)
	var freshRows, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt > ?", "kid-wiki-heartbeat", 4).Count(&freshRows).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).Count(&outboxes).Error)
	require.Zero(t, freshRows)
	require.Zero(t, outboxes)
}

func TestKnowledgeSpanRepo_PrepareFailedWikiRetryRollsBackWholeAttemptOnQueueFailure(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.wiki")
	require.NoError(t, db.Exec("DROP TABLE task_pending_ops").Error)

	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-wiki",
	})
	require.Error(t, err)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid", 5).Count(&count).Error)
	require.Zero(t, count)
	var status string
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&status).Error)
	require.Equal(t, types.ParseStatusFailed, status)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetryRejectsUnsafeTargets(t *testing.T) {
	tests := []struct{ name, target string }{
		{name: "question batch", target: "postprocess.question.batch[0]"},
		{name: "wiki child", target: "postprocess.wiki.extract"},
		{name: "generation", target: "chat.response.stream"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, db := setupKnowledgeSpanRetryRepo(t)
			require.NoError(t, db.Exec(`INSERT INTO knowledges
				(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
			seedRetryAttempt(t, repo, "kid", tt.target)
			_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
				KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-unsafe",
			})
			require.ErrorIs(t, err, ErrKnowledgeSpanRetryUnsupported)
		})
	}
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetryRejectsSupersededLogicalOwner(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.wiki")
	finished := time.Now()
	require.NoError(t, repo.Upsert(context.Background(), &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-new", ParentSpanID: "post-old",
		Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
		StartedAt: &finished, FinishedAt: &finished,
	}))

	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-old",
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryNotLatest)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetryRejectsForgedOwnerParent(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.wiki")
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid", 4, "target-old").
		Update("parent_span_id", "root-old").Error)

	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-forged",
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryUnsupported)
}

func TestKnowledgeSpanRepo_PrepareFailedSpanRetryRejectsGraphIndexMismatch(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.graph.chunk[3]")
	require.NoError(t, repo.UpdateInput(context.Background(), "kid", 4, "target-old", types.JSONMap{
		"chunk_id": "chunk-3", "chunk_index": 2, "model_id": "model-1",
	}))

	_, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "request-mismatch",
	})
	require.ErrorIs(t, err, ErrKnowledgeSpanRetryUnsupported)
}

func TestKnowledgeSpanRepo_PartialRepairCarriesUnresolvedFailuresAcrossSequentialRetries(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.summary")
	now := time.Now()
	require.NoError(t, repo.Upsert(context.Background(), &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid", Attempt: 4, SpanID: "wiki-old", ParentSpanID: "post-old",
		Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
		ErrorCode: "WIKI_FAILED", ErrorMessage: "wiki failed", StartedAt: &now, FinishedAt: &now,
	}))

	first, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "repair-summary",
	})
	require.NoError(t, err)
	var carriedWiki types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid", 5, "postprocess.wiki").Take(&carriedWiki).Error)
	require.Equal(t, types.SpanStatusFailed, carriedWiki.Status)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid", 5, first.SpanID).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": now, "finished_at": now}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid", 5))
	var knowledgeStatus string
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&knowledgeStatus).Error)
	require.Equal(t, types.ParseStatusFailed, knowledgeStatus)

	second, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 5, SpanID: carriedWiki.SpanID, ClientRequestID: "repair-wiki",
	})
	require.NoError(t, err)
	require.Equal(t, 6, second.Attempt)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid", 6, second.SpanID).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": now, "finished_at": now}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid", 6))
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&knowledgeStatus).Error)
	require.Equal(t, types.ParseStatusCompleted, knowledgeStatus)

	var oldSummary types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid", 4, "target-old").Take(&oldSummary).Error)
	require.Equal(t, types.SpanStatusFailed, oldSummary.Status, "old history must remain immutable")
}

func TestKnowledgeSpanRepo_PartialRepairCarriesUnsupportedDirectFailure(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.summary")
	now := time.Now()
	require.NoError(t, repo.Upsert(context.Background(), &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid", Attempt: 4, SpanID: "question-old", ParentSpanID: "post-old",
		Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
		ErrorCode: "QUESTION_FAILED", ErrorMessage: "question failed", StartedAt: &now, FinishedAt: &now,
	}))

	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "repair-summary",
	})
	require.NoError(t, err)
	var question types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid", 5, "postprocess.question").Take(&question).Error)
	require.Equal(t, types.SpanStatusFailed, question.Status)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", "kid", 5, prepared.SpanID).
		Updates(map[string]any{"status": types.SpanStatusDone, "started_at": now, "finished_at": now}).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), "kid", 5))
	var status string
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&status).Error)
	require.Equal(t, types.ParseStatusFailed, status)
}

func TestKnowledgeSpanRepo_FailPreparedSpanRetryIsAtomic(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.summary")
	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "repair-summary",
	})
	require.NoError(t, err)
	require.NoError(t, repo.FailPreparedSpanRetry(context.Background(), prepared, "ENQUEUE_FAILED", "queue unavailable"))

	var target types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid", 5, prepared.SpanID).Take(&target).Error)
	require.Equal(t, types.SpanStatusFailed, target.Status)
	var count int64
	require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("dedup_key = ?", prepared.TaskID).Count(&count).Error)
	require.Zero(t, count)
	var status string
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&status).Error)
	require.Equal(t, types.ParseStatusFailed, status)
}

func TestKnowledgeSpanRepo_FailPreparedSpanRetryRollsBackWhenOutboxDeleteFails(t *testing.T) {
	repo, db := setupKnowledgeSpanRetryRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status) VALUES ('kid', 7, 'kb', 'failed')`).Error)
	seedRetryAttempt(t, repo, "kid", "postprocess.summary")
	prepared, err := repo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 4, SpanID: "target-old", ClientRequestID: "repair-summary",
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec("DROP TABLE task_pending_ops").Error)
	require.Error(t, repo.FailPreparedSpanRetry(context.Background(), prepared, "ENQUEUE_FAILED", "queue unavailable"))

	var target types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid", 5, prepared.SpanID).Take(&target).Error)
	require.Equal(t, types.SpanStatusPending, target.Status)
	var status string
	require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", "kid").Scan(&status).Error)
	require.Equal(t, types.ParseStatusFinalizing, status)
}
