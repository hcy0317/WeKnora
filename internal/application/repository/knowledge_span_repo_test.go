package repository

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// spansTestDDL mirrors migration 000053 for SQLite — same column order
// minus the JSONB type (SQLite stores JSON as TEXT, the JSONMap Scanner
// handles the round trip transparently). Inlined for the same reason
// knowledgebase_sqlite_test.go inlines its DDL: GORM AutoMigrate doesn't
// reproduce our PostgreSQL-flavoured schema cleanly.
const spansTestDDL = `
CREATE TABLE IF NOT EXISTS knowledge_processing_spans (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    knowledge_id    VARCHAR(64) NOT NULL,
    attempt         INTEGER     NOT NULL DEFAULT 1,
    span_id         VARCHAR(64) NOT NULL,
    parent_span_id  VARCHAR(64),
    name            VARCHAR(255) NOT NULL,
    kind            VARCHAR(16) NOT NULL,
    status          VARCHAR(16) NOT NULL,
    input           TEXT,
    output          TEXT,
    metadata        TEXT,
    error_code      VARCHAR(64),
    error_message   TEXT,
    error_detail    TEXT,
    started_at      DATETIME,
    finished_at     DATETIME,
    duration_ms     BIGINT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (knowledge_id, attempt, span_id)
);
`

const knowledgeSettlementTestDDL = `
CREATE TABLE IF NOT EXISTS knowledges (
    id                     VARCHAR(64) PRIMARY KEY,
    parse_status           VARCHAR(32) NOT NULL,
    pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
    error_message          TEXT NOT NULL DEFAULT '',
    processed_at           DATETIME,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);
`

const wikiPendingSettlementTestDDL = `
CREATE TABLE IF NOT EXISTS task_pending_ops (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id   BIGINT NOT NULL DEFAULT 0,
    task_type   VARCHAR(64) NOT NULL,
    scope       VARCHAR(32) NOT NULL,
    scope_id    VARCHAR(64) NOT NULL,
    op          VARCHAR(32) NOT NULL,
    dedup_key   VARCHAR(128) NOT NULL DEFAULT '',
    payload     TEXT NOT NULL DEFAULT '{}',
    fail_count  INTEGER NOT NULL DEFAULT 0,
    enqueued_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    claimed_at  DATETIME,
    claim_token VARCHAR(64),
    claimed_by_task_id VARCHAR(255),
    claim_heartbeat_at DATETIME
);
CREATE TABLE IF NOT EXISTS task_dead_letters (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id   BIGINT NOT NULL DEFAULT 0,
    task_type   VARCHAR(64) NOT NULL,
    scope       VARCHAR(32) NOT NULL,
    scope_id    VARCHAR(64) NOT NULL,
    related_id  VARCHAR(64) NOT NULL DEFAULT '',
    payload     TEXT NOT NULL,
    last_error  TEXT NOT NULL DEFAULT '',
    fail_count  INTEGER NOT NULL,
    failed_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);
`

func setupSpanTestRepo(t *testing.T) (KnowledgeSpanRepository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(spansTestDDL).Error)
	require.NoError(t, db.Exec(knowledgeSettlementTestDDL).Error)
	require.NoError(t, db.Exec(wikiPendingSettlementTestDDL).Error)
	return NewKnowledgeSpanRepository(db), db
}

func seedSettlementKnowledge(t *testing.T, db *gorm.DB, knowledgeID string, pending int) {
	t.Helper()
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, parse_status, pending_subtasks_count) VALUES (?, ?, ?)`,
		knowledgeID, types.ParseStatusFinalizing, pending).Error)
}

func seedSettlementSpan(t *testing.T, repo KnowledgeSpanRepository, row *types.KnowledgeProcessingSpan) {
	t.Helper()
	if row.StartedAt == nil {
		now := time.Now()
		row.StartedAt = &now
	}
	require.NoError(t, repo.Upsert(context.Background(), row))
}

func settlementStatus(t *testing.T, db *gorm.DB, knowledgeID, spanID string) string {
	t.Helper()
	var status string
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("status").Where("knowledge_id = ? AND span_id = ?", knowledgeID, spanID).
		Scan(&status).Error)
	return status
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeUsesStableLogicalRetryIdentity(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	ctx := context.Background()
	kid := "g005-retry-identity"
	seedSettlementKnowledge(t, db, kid, 99)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root-random", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "docreader", ParentSpanID: "root-random", Name: types.StageDocReader, Kind: types.SpanKindStage, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root-random", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches":       []any{"postprocess.summary", "postprocess.question", "postprocess.wiki", "postprocess.graph.chunk[0]"},
			"expected_subtasks_count": 4, "question_batch_count": 1, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "question-group", ParentSpanID: "post", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"batch_count": 1}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch-old-random", ParentSpanID: "question-group", Name: "postprocess.question.batch[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch-new-random", ParentSpanID: "question-group", Name: "postprocess.question.batch[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "graph-old-random", ParentSpanID: "post", Name: "postprocess.graph.chunk[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusCancelled},
		{KnowledgeID: kid, Attempt: 1, SpanID: "graph-new-random", ParentSpanID: "post", Name: "postprocess.graph.chunk[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
	} {
		seedSettlementSpan(t, repo, row)
	}

	require.NoError(t, repo.SettleProcessingOutcome(ctx, kid, 1))
	require.NoError(t, repo.SettleProcessingOutcome(ctx, kid, 1), "duplicate terminal delivery must be idempotent")

	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "question-group"))
	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "root-random"))
	assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "batch-old-random"), "retry history must remain queryable")
	assert.Equal(t, types.SpanStatusCancelled, settlementStatus(t, db, kid, "graph-old-random"), "cancelled retry history must remain queryable")
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
	assert.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
	assert.Zero(t, knowledge.PendingSubtasksCount)
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeRejectsMissingLogicalBatch(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	kid := "g005-missing-batch"
	seedSettlementKnowledge(t, db, kid, 2)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches": []any{"postprocess.question"}, "expected_subtasks_count": 2,
			"question_batch_count": 2, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "question", ParentSpanID: "post", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"batch_count": 2}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch-0", ParentSpanID: "question", Name: "postprocess.question.batch[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch-9", ParentSpanID: "question", Name: "postprocess.question.batch[9]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
	} {
		seedSettlementSpan(t, repo, row)
	}

	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
	assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "question"))
	assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "root"))
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
		ErrorMessage         string
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
	assert.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	assert.Zero(t, knowledge.PendingSubtasksCount)
	assert.Contains(t, knowledge.ErrorMessage, "batch[1]")
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeSettlesQuestionBeforeOtherBranches(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	kid := "g005-question-independent"
	seedSettlementKnowledge(t, db, kid, 2)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches":       []any{"postprocess.question", "postprocess.wiki"},
			"expected_subtasks_count": 2, "question_batch_count": 1, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "question", ParentSpanID: "post", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, Input: types.JSONMap{"batch_count": 1}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch-0", ParentSpanID: "question", Name: "postprocess.question.batch[0]", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 1, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning},
	} {
		seedSettlementSpan(t, repo, row)
	}

	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "question"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
	assert.Equal(t, types.ParseStatusFinalizing, knowledge.ParseStatus)
	assert.Equal(t, 1, knowledge.PendingSubtasksCount)
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeFailsFastOnlyForTerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name             string
		terminalFailure  bool
		wantKnowledge    string
		wantWiki         string
		wantPendingCount int
	}{
		{name: "retryable failure waits for sibling", wantKnowledge: types.ParseStatusFinalizing,
			wantWiki: types.SpanStatusRunning, wantPendingCount: 1},
		{name: "terminal failure cancels sibling", terminalFailure: true,
			wantKnowledge: types.ParseStatusFailed, wantWiki: types.SpanStatusCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, db := setupSpanTestRepo(t)
			kid := "g005-fail-fast-" + strings.ReplaceAll(tc.name, " ", "-")
			seedSettlementKnowledge(t, db, kid, 2)
			summaryInput := types.JSONMap{}
			if tc.terminalFailure {
				summaryInput["terminal_failure"] = true
			}
			for _, row := range []*types.KnowledgeProcessingSpan{
				{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
				{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
					"expected_branches": []any{"postprocess.summary", "postprocess.wiki"}, "fanout_complete": true,
				}},
				{KnowledgeID: kid, Attempt: 1, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan,
					Status: types.SpanStatusFailed, Input: summaryInput, ErrorCode: "SUMMARY_FAILED", ErrorMessage: "upstream failed"},
				{KnowledgeID: kid, Attempt: 1, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning},
			} {
				seedSettlementSpan(t, repo, row)
			}

			require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
			assert.Equal(t, tc.wantWiki, settlementStatus(t, db, kid, "wiki"))
			var knowledge struct {
				ParseStatus          string
				PendingSubtasksCount int
			}
			require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
			assert.Equal(t, tc.wantKnowledge, knowledge.ParseStatus)
			assert.Equal(t, tc.wantPendingCount, knowledge.PendingSubtasksCount)
			if tc.terminalFailure {
				assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "post"))
				assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "root"))
			} else {
				assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
				assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
			}
		})
	}
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeCancellationIsAuthoritative(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	kid := "g005-cancel-authoritative"
	seedSettlementKnowledge(t, db, kid, 1)
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).
		Update("parse_status", types.ParseStatusCancelled).Error)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches": []any{"postprocess.summary"}, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
	} {
		seedSettlementSpan(t, repo, row)
	}

	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
	assert.Equal(t, types.ParseStatusCancelled, knowledge.ParseStatus)
	assert.Equal(t, 1, knowledge.PendingSubtasksCount)
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeIgnoresLateOlderAttempt(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	kid := "g005-old-attempt"
	seedSettlementKnowledge(t, db, kid, 1)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root-1", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post-1", ParentSpanID: "root-1", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches": []any{"postprocess.summary"}, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "summary-1", ParentSpanID: "post-1", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		{KnowledgeID: kid, Attempt: 2, SpanID: "root-2", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
	} {
		seedSettlementSpan(t, repo, row)
	}

	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post-1"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root-1"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root-2"))
	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
	}
	require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
	assert.Equal(t, types.ParseStatusFinalizing, knowledge.ParseStatus)
	assert.Equal(t, 1, knowledge.PendingSubtasksCount)
}

func TestKnowledgeSpanRepo_SettleProcessingOutcomeRollsBackEveryWrite(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	kid := "g005-rollback"
	seedSettlementKnowledge(t, db, kid, 1)
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
			"expected_branches": []any{"postprocess.summary"}, "expected_subtasks_count": 1, "fanout_complete": true,
		}},
		{KnowledgeID: kid, Attempt: 1, SpanID: "summary", ParentSpanID: "post", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
	} {
		seedSettlementSpan(t, repo, row)
	}
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_g005_knowledge_settlement BEFORE UPDATE ON knowledges
		WHEN OLD.id = 'g005-rollback' BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;`).Error)

	err := repo.SettleProcessingOutcome(context.Background(), kid, 1)
	require.ErrorContains(t, err, "injected settlement failure")
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
	require.NoError(t, db.Exec(`DROP TRIGGER reject_g005_knowledge_settlement`).Error)
	require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "post"))
	assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "root"))
}

func TestKnowledgeSpanRepo_SettleWikiPendingOpDeletesQueueRowAtomically(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	seedWikiAttempt := func(t *testing.T, kid string) int64 {
		t.Helper()
		seedSettlementKnowledge(t, db, kid, 1)
		for _, row := range []*types.KnowledgeProcessingSpan{
			{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
			{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
				"expected_branches": []any{"postprocess.wiki"}, "expected_subtasks_count": 1, "fanout_complete": true,
			}},
			{KnowledgeID: kid, Attempt: 1, SpanID: "wiki", ParentSpanID: "post", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone},
		} {
			seedSettlementSpan(t, repo, row)
		}
		op := &types.TaskPendingOp{TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
			ScopeID: "kb-1", Op: "ingest", DedupKey: kid, Payload: []byte(`{"op":"ingest"}`)}
		require.NoError(t, db.Create(op).Error)
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", op.ID).Updates(map[string]any{
			"claim_token": nil, "claimed_by_task_id": nil, "claim_heartbeat_at": nil,
		}).Error)
		return op.ID
	}

	t.Run("success settles the tree and consumes the durable row", func(t *testing.T) {
		id := seedWikiAttempt(t, "wiki-atomic-success")
		require.NoError(t, repo.SettleWikiPendingOp(context.Background(), "wiki-atomic-success", 1, []int64{id}, nil, nil))
		assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, "wiki-atomic-success", "post"))
		assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, "wiki-atomic-success", "root"))
		var count int64
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Count(&count).Error)
		assert.Zero(t, count)
	})

	t.Run("stale owner cannot acknowledge successor claim", func(t *testing.T) {
		kid := "wiki-atomic-successor-owner"
		id := seedWikiAttempt(t, kid)
		now := time.Now()
		successor := types.TaskClaimOwner{Token: "successor-token", TaskID: "successor-task"}
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Updates(map[string]any{
			"claimed_at": now, "claim_heartbeat_at": now,
			"claim_token": successor.Token, "claimed_by_task_id": successor.TaskID,
		}).Error)

		err := repo.SettleWikiPendingOp(context.Background(), kid, 1, []int64{id}, nil,
			&types.TaskClaimOwner{Token: "old-token", TaskID: "old-task"})
		require.ErrorContains(t, err, "deleted 0 of 1")
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
		var stored types.TaskPendingOp
		require.NoError(t, db.First(&stored, id).Error)
		assert.Equal(t, successor.Token, stored.ClaimToken)
		assert.Equal(t, successor.TaskID, stored.ClaimedByTaskID)
	})

	for _, tc := range []struct {
		name    string
		updates map[string]any
	}{
		{name: "claim token", updates: map[string]any{"claim_token": "successor-token"}},
		{name: "task id", updates: map[string]any{"claimed_by_task_id": "successor-task"}},
		{name: "heartbeat", updates: map[string]any{"claim_heartbeat_at": time.Now()}},
	} {
		t.Run("nil owner cannot acknowledge successor "+tc.name, func(t *testing.T) {
			kid := "wiki-atomic-nil-owner-" + strings.ReplaceAll(tc.name, " ", "-")
			id := seedWikiAttempt(t, kid)
			require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Updates(tc.updates).Error)
			deadLetter := &types.TaskDeadLetter{
				TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
				ScopeID: "kb-1", RelatedID: kid, Payload: []byte(`{"op":"ingest"}`),
				LastError: "retry budget exhausted", FailCount: 4,
			}

			err := repo.SettleWikiPendingOp(context.Background(), kid, 1, []int64{id}, deadLetter, nil)
			require.ErrorContains(t, err, "deleted 0 of 1")
			assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
			assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
			var knowledge struct {
				ParseStatus          string
				PendingSubtasksCount int
				ProcessedAt          *time.Time
			}
			require.NoError(t, db.Table("knowledges").
				Select("parse_status", "pending_subtasks_count", "processed_at").
				Where("id = ?", kid).Take(&knowledge).Error)
			assert.Equal(t, types.ParseStatusFinalizing, knowledge.ParseStatus)
			assert.Equal(t, 1, knowledge.PendingSubtasksCount)
			assert.Nil(t, knowledge.ProcessedAt)
			var pendingCount, archiveCount int64
			require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Count(&pendingCount).Error)
			require.NoError(t, db.Model(&types.TaskDeadLetter{}).Where("related_id = ?", kid).Count(&archiveCount).Error)
			assert.EqualValues(t, 1, pendingCount)
			assert.Zero(t, archiveCount)
		})
	}

	t.Run("queue delete failure rolls the reducer back", func(t *testing.T) {
		id := seedWikiAttempt(t, "wiki-atomic-rollback")
		require.NoError(t, db.Exec(`CREATE TRIGGER reject_wiki_pending_delete BEFORE DELETE ON task_pending_ops
			WHEN OLD.id = `+fmt.Sprint(id)+` BEGIN SELECT RAISE(ABORT, 'injected wiki pending delete failure'); END;`).Error)
		err := repo.SettleWikiPendingOp(context.Background(), "wiki-atomic-rollback", 1, []int64{id}, nil, nil)
		require.ErrorContains(t, err, "injected wiki pending delete failure")
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, "wiki-atomic-rollback", "post"))
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, "wiki-atomic-rollback", "root"))
		var count int64
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Count(&count).Error)
		assert.EqualValues(t, 1, count)
	})

	t.Run("dead letter archive and queue acknowledgement share the transaction", func(t *testing.T) {
		id := seedWikiAttempt(t, "wiki-atomic-deadletter")
		deadLetter := &types.TaskDeadLetter{
			TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
			ScopeID: "kb-1", RelatedID: "wiki-atomic-deadletter", Payload: []byte(`{"op":"ingest"}`),
			LastError: "retry budget exhausted", FailCount: 4,
		}
		require.NoError(t, repo.SettleWikiPendingOp(
			context.Background(), "wiki-atomic-deadletter", 1, []int64{id}, deadLetter,
			nil,
		))
		var pendingCount, archiveCount int64
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Count(&pendingCount).Error)
		require.NoError(t, db.Model(&types.TaskDeadLetter{}).
			Where("related_id = ?", "wiki-atomic-deadletter").Count(&archiveCount).Error)
		assert.Zero(t, pendingCount)
		assert.EqualValues(t, 1, archiveCount)
	})

	t.Run("dead letter archive failure rolls back reducer and queue acknowledgement", func(t *testing.T) {
		id := seedWikiAttempt(t, "wiki-atomic-deadletter-rollback")
		require.NoError(t, db.Exec(`CREATE TRIGGER reject_wiki_dead_letter BEFORE INSERT ON task_dead_letters
			WHEN NEW.related_id = 'wiki-atomic-deadletter-rollback' BEGIN SELECT RAISE(ABORT, 'injected wiki dead letter failure'); END;`).Error)
		deadLetter := &types.TaskDeadLetter{
			TaskType: types.TypeWikiIngest, Scope: types.TaskScopeKnowledgeBase,
			ScopeID: "kb-1", RelatedID: "wiki-atomic-deadletter-rollback", Payload: []byte(`{"op":"ingest"}`),
			LastError: "retry budget exhausted", FailCount: 4,
		}
		err := repo.SettleWikiPendingOp(
			context.Background(), "wiki-atomic-deadletter-rollback", 1, []int64{id}, deadLetter,
			nil,
		)
		require.ErrorContains(t, err, "injected wiki dead letter failure")
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, "wiki-atomic-deadletter-rollback", "post"))
		assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, "wiki-atomic-deadletter-rollback", "root"))
		var pendingCount, archiveCount int64
		require.NoError(t, db.Model(&types.TaskPendingOp{}).Where("id = ?", id).Count(&pendingCount).Error)
		require.NoError(t, db.Model(&types.TaskDeadLetter{}).
			Where("related_id = ?", "wiki-atomic-deadletter-rollback").Count(&archiveCount).Error)
		assert.EqualValues(t, 1, pendingCount)
		assert.Zero(t, archiveCount)
	})
}

func TestKnowledgeSpanRepo_EnqueueFailurePropagatesSummaryAndGraph(t *testing.T) {
	for _, branch := range []string{"postprocess.summary", "postprocess.graph.chunk[0]"} {
		t.Run(branch, func(t *testing.T) {
			repo, db := setupSpanTestRepo(t)
			kid := "g005-enqueue-failure-" + strings.NewReplacer(".", "-", "[", "-", "]", "").Replace(branch)
			seedSettlementKnowledge(t, db, kid, 1)
			for _, row := range []*types.KnowledgeProcessingSpan{
				{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
				{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, Input: types.JSONMap{
					"expected_branches": []any{branch}, "fanout_complete": true,
				}},
				{KnowledgeID: kid, Attempt: 1, SpanID: "failed-child", ParentSpanID: "post", Name: branch, Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed, ErrorCode: "ENQUEUE_FAILED", ErrorMessage: "queue unavailable"},
			} {
				seedSettlementSpan(t, repo, row)
			}

			require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
			require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
			assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "failed-child"))
			assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "post"))
			assert.Equal(t, types.SpanStatusFailed, settlementStatus(t, db, kid, "root"))
			var knowledge struct {
				ParseStatus          string
				PendingSubtasksCount int
				ErrorMessage         string
			}
			require.NoError(t, db.Table("knowledges").Where("id = ?", kid).Take(&knowledge).Error)
			assert.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
			assert.Zero(t, knowledge.PendingSubtasksCount)
			assert.Contains(t, knowledge.ErrorMessage, branch)
		})
	}
}

func TestKnowledgeSpanRepo_ConcurrentLastCompletionAndDuplicateDeliveryMatrix(t *testing.T) {
	for _, branch := range []string{
		"postprocess.summary", "postprocess.question.batch[0]", "postprocess.wiki", "postprocess.graph.chunk[0]",
	} {
		t.Run(branch, func(t *testing.T) {
			repo, db := setupSpanTestRepo(t)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			sqlDB.SetMaxOpenConns(1)
			kid := "g005-concurrent-" + strings.NewReplacer(".", "-", "[", "-", "]", "").Replace(branch)
			seedSettlementKnowledge(t, db, kid, 1)
			expected := branch
			parentID := "post"
			rows := []*types.KnowledgeProcessingSpan{
				{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
				{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning},
			}
			if strings.HasPrefix(branch, "postprocess.question.batch[") {
				expected = "postprocess.question"
				parentID = "question"
				rows = append(rows, &types.KnowledgeProcessingSpan{
					KnowledgeID: kid, Attempt: 1, SpanID: "question", ParentSpanID: "post",
					Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
					Input: types.JSONMap{"batch_count": 1},
				})
				rows[1].Input = types.JSONMap{"expected_branches": []any{expected}, "question_batch_count": 1, "fanout_complete": true}
			} else {
				rows[1].Input = types.JSONMap{"expected_branches": []any{expected}, "fanout_complete": true}
			}
			rows = append(rows, &types.KnowledgeProcessingSpan{
				KnowledgeID: kid, Attempt: 1, SpanID: "last-child", ParentSpanID: parentID,
				Name: branch, Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
			})
			for _, row := range rows {
				seedSettlementSpan(t, repo, row)
			}

			start := make(chan struct{})
			errs := make(chan error, 4)
			var wg sync.WaitGroup
			for worker := 0; worker < 4; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					if worker == 0 {
						if err := db.Table("knowledge_processing_spans").Where("knowledge_id = ? AND span_id = ?", kid, "last-child").
							Update("status", types.SpanStatusDone).Error; err != nil {
							errs <- err
							return
						}
					}
					errs <- repo.SettleProcessingOutcome(context.Background(), kid, 1)
				}(worker)
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			require.NoError(t, repo.SettleProcessingOutcome(context.Background(), kid, 1))
			assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "post"))
			assert.Equal(t, types.SpanStatusDone, settlementStatus(t, db, kid, "root"))
			var parseStatus string
			require.NoError(t, db.Table("knowledges").Select("parse_status").Where("id = ?", kid).Scan(&parseStatus).Error)
			assert.Equal(t, types.ParseStatusCompleted, parseStatus)
		})
	}
}

func TestKnowledgeSpanRepo_TransactionalRollbackMatrix(t *testing.T) {
	for index, branch := range []string{
		"postprocess.summary", "postprocess.question.batch[0]", "postprocess.wiki", "postprocess.graph.chunk[0]",
	} {
		t.Run(branch, func(t *testing.T) {
			repo, db := setupSpanTestRepo(t)
			kid := fmt.Sprintf("g005-rollback-matrix-%d", index)
			seedSettlementKnowledge(t, db, kid, 1)
			expected := branch
			parentID := "post"
			rows := []*types.KnowledgeProcessingSpan{
				{KnowledgeID: kid, Attempt: 1, SpanID: "root", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
				{KnowledgeID: kid, Attempt: 1, SpanID: "post", ParentSpanID: "root", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusRunning},
			}
			if strings.HasPrefix(branch, "postprocess.question.batch[") {
				expected = "postprocess.question"
				parentID = "question"
				rows = append(rows, &types.KnowledgeProcessingSpan{
					KnowledgeID: kid, Attempt: 1, SpanID: "question", ParentSpanID: "post",
					Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
					Input: types.JSONMap{"batch_count": 1},
				})
				rows[1].Input = types.JSONMap{"expected_branches": []any{expected}, "question_batch_count": 1, "fanout_complete": true}
			} else {
				rows[1].Input = types.JSONMap{"expected_branches": []any{expected}, "fanout_complete": true}
			}
			rows = append(rows, &types.KnowledgeProcessingSpan{
				KnowledgeID: kid, Attempt: 1, SpanID: "terminal-child", ParentSpanID: parentID,
				Name: branch, Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone,
			})
			for _, row := range rows {
				seedSettlementSpan(t, repo, row)
			}
			trigger := fmt.Sprintf(`CREATE TRIGGER reject_g005_matrix_%d BEFORE UPDATE ON knowledges
				WHEN OLD.id = '%s' BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;`, index, kid)
			require.NoError(t, db.Exec(trigger).Error)

			err := repo.SettleProcessingOutcome(context.Background(), kid, 1)
			require.ErrorContains(t, err, "injected settlement failure")
			assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "post"))
			assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "root"))
			if parentID == "question" {
				assert.Equal(t, types.SpanStatusRunning, settlementStatus(t, db, kid, "question"))
			}
		})
	}
}

// TestKnowledgeSpanRepo_UpsertAndList covers the round-trip: a Begin
// followed by an End for the same (kid, attempt, span_id) updates the
// existing row in place, leaving exactly one row queryable by
// ListByAttempt with the latest state.
func TestKnowledgeSpanRepo_UpsertAndList(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	kid := "kid-1"
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: kid,
		Attempt:     1,
		SpanID:      "span-A",
		Name:        types.StageDocReader,
		Kind:        types.SpanKindStage,
		Status:      types.SpanStatusRunning,
		StartedAt:   &now,
	}
	require.NoError(t, repo.Upsert(ctx, row))

	// Second Upsert with same (kid, attempt, span_id) flips status and
	// sets finished_at — must overwrite, not insert a duplicate.
	finished := now.Add(2 * time.Second)
	row.Status = types.SpanStatusDone
	row.FinishedAt = &finished
	row.DurationMs = 2000
	require.NoError(t, repo.Upsert(ctx, row))

	rows, err := repo.ListByAttempt(ctx, kid, 1)
	require.NoError(t, err)
	require.Len(t, rows, 1, "Upsert must replace, not append")
	assert.Equal(t, types.SpanStatusDone, rows[0].Status)
	assert.Equal(t, int64(2000), rows[0].DurationMs)
}

func TestKnowledgeSpanRepo_CancelAllOpenSpansPersistsDuration(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	started := time.Now().Add(-2 * time.Second)
	require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "cancel-duration", Attempt: 1, SpanID: "running-child",
		Name: "postprocess.wiki", Kind: types.SpanKindSubSpan,
		Status: types.SpanStatusRunning, StartedAt: &started,
	}))

	affected, err := repo.CancelAllOpenSpans(ctx, "cancel-duration", 1, "USER_CANCELLED", "cancelled")
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
	rows, err := repo.ListByAttempt(ctx, "cancel-duration", 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusCancelled, rows[0].Status)
	assert.NotNil(t, rows[0].FinishedAt)
	assert.GreaterOrEqual(t, rows[0].DurationMs, int64(1900))
}

func TestKnowledgeSpanRepo_RejectsLateStageReentryAfterNewAttempt(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	knowledgeID := "late-stage-reentry"

	root1 := &types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, SpanID: "root-1", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	}
	attempt1, err := repo.OpenAttempt(ctx, root1)
	require.NoError(t, err)
	started := time.Now().Add(-time.Second)
	stage := &types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, Attempt: attempt1, SpanID: "stage-1",
		ParentSpanID: root1.SpanID, Name: types.StageDocReader, Kind: types.SpanKindStage,
		Status: types.SpanStatusRunning, StartedAt: &started,
	}
	accepted, err := repo.UpsertRunningStageIfCurrent(ctx, stage)
	require.NoError(t, err)
	require.True(t, accepted)

	_, err = repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID, SpanID: "root-2", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)

	lateStarted := time.Now()
	stage.Status = types.SpanStatusRunning
	stage.StartedAt = &lateStarted
	stage.FinishedAt = nil
	stage.DurationMs = 0
	accepted, err = repo.UpsertRunningStageIfCurrent(ctx, stage)
	require.NoError(t, err)
	assert.False(t, accepted, "an older attempt must not reopen a terminal stage")

	rows, err := repo.ListByAttempt(ctx, knowledgeID, attempt1)
	require.NoError(t, err)
	for _, row := range rows {
		if row.SpanID == stage.SpanID {
			assert.Equal(t, types.SpanStatusCancelled, row.Status)
			assert.NotNil(t, row.FinishedAt)
			assert.Greater(t, row.DurationMs, int64(0))
			return
		}
	}
	t.Fatal("stage row not found")
}

func TestKnowledgeSpanRepo_QueuePendingSpanGuardsKnowledgeAttemptAndParent(t *testing.T) {
	seedParent := func(t *testing.T, repo KnowledgeSpanRepository, knowledgeID string, attempt int, status string) string {
		t.Helper()
		rootID := fmt.Sprintf("root-%d", attempt)
		seedSettlementSpan(t, repo, &types.KnowledgeProcessingSpan{
			KnowledgeID: knowledgeID, Attempt: attempt, SpanID: rootID,
			Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
		})
		parentID := fmt.Sprintf("post-%d", attempt)
		seedSettlementSpan(t, repo, &types.KnowledgeProcessingSpan{
			KnowledgeID: knowledgeID, Attempt: attempt, SpanID: parentID, ParentSpanID: rootID,
			Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: status,
		})
		return parentID
	}
	queue := func(repo KnowledgeSpanRepository, knowledgeID string, attempt int, parentID, name string) (*types.KnowledgeProcessingSpan, error) {
		return repo.QueuePendingSpan(context.Background(), &types.KnowledgeProcessingSpan{
			KnowledgeID: knowledgeID, Attempt: attempt, SpanID: fmt.Sprintf("%s-%d", name, attempt),
			ParentSpanID: parentID, Name: name, Kind: types.SpanKindSubSpan,
		})
	}

	t.Run("finalizing allows one idempotent pending child", func(t *testing.T) {
		repo, db := setupSpanTestRepo(t)
		const knowledgeID = "queue-guard-finalizing"
		seedSettlementKnowledge(t, db, knowledgeID, 1)
		parentID := seedParent(t, repo, knowledgeID, 1, types.SpanStatusRunning)

		first, err := queue(repo, knowledgeID, 1, parentID, "postprocess.summary")
		require.NoError(t, err)
		require.NotNil(t, first)
		second, err := queue(repo, knowledgeID, 1, parentID, "postprocess.summary")
		require.NoError(t, err)
		require.NotNil(t, second)
		assert.Equal(t, first.SpanID, second.SpanID)
		var count int64
		require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, 1, "postprocess.summary").
			Count(&count).Error)
		assert.Equal(t, int64(1), count)
	})

	for _, parseStatus := range []string{types.ParseStatusCancelled, types.ParseStatusDeleting} {
		t.Run(parseStatus+" knowledge rejects with zero new rows", func(t *testing.T) {
			repo, db := setupSpanTestRepo(t)
			knowledgeID := "queue-guard-" + parseStatus
			seedSettlementKnowledge(t, db, knowledgeID, 1)
			parentID := seedParent(t, repo, knowledgeID, 1, types.SpanStatusRunning)
			require.NoError(t, db.Table("knowledges").Where("id = ?", knowledgeID).
				Update("parse_status", parseStatus).Error)

			queued, err := queue(repo, knowledgeID, 1, parentID, "postprocess.summary")
			require.ErrorContains(t, err, "status="+parseStatus)
			assert.Nil(t, queued)
			var count int64
			require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND name = ?", knowledgeID, "postprocess.summary").Count(&count).Error)
			assert.Zero(t, count)
		})
	}

	t.Run("superseded attempt rejects", func(t *testing.T) {
		repo, db := setupSpanTestRepo(t)
		const knowledgeID = "queue-guard-superseded"
		seedSettlementKnowledge(t, db, knowledgeID, 1)
		oldParentID := seedParent(t, repo, knowledgeID, 1, types.SpanStatusRunning)
		seedParent(t, repo, knowledgeID, 2, types.SpanStatusRunning)

		queued, err := queue(repo, knowledgeID, 1, oldParentID, "postprocess.summary")
		require.ErrorContains(t, err, "queued=1 latest=2")
		assert.Nil(t, queued)
	})

	t.Run("terminal parent rejects", func(t *testing.T) {
		repo, db := setupSpanTestRepo(t)
		const knowledgeID = "queue-guard-terminal-parent"
		seedSettlementKnowledge(t, db, knowledgeID, 1)
		parentID := seedParent(t, repo, knowledgeID, 1, types.SpanStatusDone)

		queued, err := queue(repo, knowledgeID, 1, parentID, "postprocess.summary")
		require.ErrorContains(t, err, "parent state: status=done")
		assert.Nil(t, queued)
	})
}

func TestKnowledgeSpanRepo_TerminalOutcomeIsMonotonic(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-terminal", Attempt: 1, SpanID: "generation-1",
		Name: "chat.response.stream", Kind: types.SpanKindGeneration,
		Status: types.SpanStatusRunning, StartedAt: &now,
	}
	require.NoError(t, repo.Upsert(ctx, row))

	cancelledAt := now.Add(time.Second)
	row.Status = types.SpanStatusCancelled
	row.FinishedAt = &cancelledAt
	row.ErrorCode = "TASK_CANCELLED"
	require.NoError(t, repo.Upsert(ctx, row))

	// A late progress update and a late successful close from the provider
	// must both be ignored after cancellation.
	row.Status = types.SpanStatusRunning
	row.FinishedAt = nil
	row.ErrorCode = ""
	require.NoError(t, repo.Upsert(ctx, row))
	row.Status = types.SpanStatusDone
	row.FinishedAt = &cancelledAt
	require.NoError(t, repo.Upsert(ctx, row))

	rows, err := repo.ListByAttempt(ctx, row.KnowledgeID, row.Attempt)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusCancelled, rows[0].Status)
	assert.Equal(t, "TASK_CANCELLED", rows[0].ErrorCode)
}

func TestKnowledgeSpanRepo_StageRetryMayReopenTerminalRow(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-stage-retry", Attempt: 1, SpanID: "docreader-stage",
		Name: types.StageDocReader, Kind: types.SpanKindStage,
		Status: types.SpanStatusFailed, StartedAt: &now,
	}
	require.NoError(t, repo.Upsert(ctx, row))
	row.Status = types.SpanStatusRunning
	row.ErrorCode = ""
	require.NoError(t, repo.Upsert(ctx, row))

	rows, err := repo.ListByAttempt(ctx, row.KnowledgeID, row.Attempt)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status)
}

func TestKnowledgeSpanRepo_OpenAttemptIsAtomic(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	kid := "kid-open-attempt"

	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "root-1", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusDone, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "open-old", Name: "child", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now},
	} {
		require.NoError(t, repo.Upsert(ctx, row))
	}

	root := &types.KnowledgeProcessingSpan{
		KnowledgeID: kid, SpanID: "root-2", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &now,
	}
	attempt, err := repo.OpenAttempt(ctx, root)
	require.NoError(t, err)
	assert.Equal(t, 2, attempt)
	assert.Equal(t, 2, root.Attempt)

	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 3)
	assert.Equal(t, types.SpanStatusDone, rows[0].Status, "old terminal history must not change")
	assert.Equal(t, types.SpanStatusCancelled, rows[1].Status)
	assert.Equal(t, "ATTEMPT_SUPERSEDED", rows[1].ErrorCode)
	assert.NotNil(t, rows[1].FinishedAt)
	assert.Greater(t, rows[1].DurationMs, int64(0), "superseded history must retain a frozen duration")
	assert.Equal(t, types.SpanStatusRunning, rows[2].Status)
}

func TestKnowledgeSpanRepo_OpenAttemptRollsBackWhenRootInsertFails(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	kid := "kid-open-rollback"
	require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: kid, Attempt: 1, SpanID: "open-old", Name: "child",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now,
	}))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_root_insert BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'root' BEGIN SELECT RAISE(ABORT, 'injected root failure'); END;`).Error)

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: kid, SpanID: "root-2", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &now,
	})
	require.ErrorContains(t, err, "injected root failure")
	assert.Zero(t, attempt)

	rows, listErr := repo.ListByAttempt(ctx, kid, 0)
	require.NoError(t, listErr)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status, "supersede update must roll back")
}

func TestKnowledgeSpanRepo_OpenAttemptRollsBackWhenSupersedeFails(t *testing.T) {
	repo, db := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	kid := "kid-cancel-rollback"
	require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: kid, Attempt: 1, SpanID: "open-old", Name: "child",
		Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now,
	}))
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_supersede BEFORE UPDATE ON knowledge_processing_spans
		WHEN OLD.knowledge_id = 'kid-cancel-rollback' BEGIN SELECT RAISE(ABORT, 'injected cancel failure'); END;`).Error)

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: kid, SpanID: "root-2", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &now,
	})
	require.ErrorContains(t, err, "injected cancel failure")
	assert.Zero(t, attempt)

	rows, listErr := repo.ListByAttempt(ctx, kid, 0)
	require.NoError(t, listErr)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status)
}

func TestKnowledgeSpanRepo_AttemptCommitGuardSerializesWithOpenAttemptSQLite(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	guard := repo.(*knowledgeSpanRepository)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-sqlite-commit-guard", SpanID: "root-1", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)
	require.Equal(t, 1, attempt)

	entered := make(chan struct{})
	release := make(chan struct{})
	guardResult := make(chan error, 1)
	go func() {
		guardResult <- guard.WithAttemptCommitGuard(ctx, "kid-sqlite-commit-guard", attempt, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	openStarted := make(chan struct{})
	openResult := make(chan error, 1)
	go func() {
		close(openStarted)
		_, openErr := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
			KnowledgeID: "kid-sqlite-commit-guard", SpanID: "root-2", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
		})
		openResult <- openErr
	}()
	<-openStarted
	select {
	case openErr := <-openResult:
		t.Fatalf("OpenAttempt crossed an in-flight guarded commit: %v", openErr)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-guardResult)
	require.NoError(t, <-openResult)

	callbackCalled := false
	err = guard.WithAttemptCommitGuard(ctx, "kid-sqlite-commit-guard", attempt, func(context.Context) error {
		callbackCalled = true
		return nil
	})
	require.ErrorContains(t, err, "superseded")
	require.False(t, callbackCalled, "a superseded attempt must never enter the durable write callback")
}

const spansPostgresTestDDL = `
CREATE TABLE knowledge_processing_spans (
    id BIGSERIAL PRIMARY KEY, knowledge_id VARCHAR(64) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1, span_id VARCHAR(64) NOT NULL,
    parent_span_id VARCHAR(64), name VARCHAR(255) NOT NULL,
    kind VARCHAR(16) NOT NULL, status VARCHAR(16) NOT NULL,
    input JSONB, output JSONB, metadata JSONB, error_code VARCHAR(64),
    error_message TEXT, error_detail TEXT, started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ, duration_ms BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (knowledge_id, attempt, span_id)
);
CREATE UNIQUE INDEX idx_knowledge_processing_spans_root_attempt_unique
ON knowledge_processing_spans (knowledge_id, attempt) WHERE kind = 'root';`

func setupPostgresSpanTestRepo(t *testing.T) (KnowledgeSpanRepository, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if dsn == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("WEKNORA_TEST_POSTGRES_DSN is required for PostgreSQL integration tests")
	}
	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	baseSQL, err := base.DB()
	require.NoError(t, err)
	schema := fmt.Sprintf("g004_%d", time.Now().UnixNano())
	require.True(t, strings.HasPrefix(schema, "g004_"))
	require.NoError(t, base.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = base.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		_ = baseSQL.Close()
	})
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	db, err := gorm.Open(postgres.Open(dsn+separator+"search_path="+schema), &gorm.Config{})
	require.NoError(t, err)
	testSQL, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = testSQL.Close() })
	require.NoError(t, db.Exec(spansPostgresTestDDL).Error)
	return NewKnowledgeSpanRepository(db), db
}

func TestKnowledgeSpanRepo_PostgresConflictUpdateHasQualifiedGuard(t *testing.T) {
	repo, db := setupPostgresSpanTestRepo(t)
	resultRepo := repo.(*knowledgeSpanRepository)
	ctx := context.Background()
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-conflict", Attempt: 1, SpanID: "generation",
		Name: "generation", Kind: types.SpanKindGeneration,
		Status: types.SpanStatusRunning, StartedAt: &now,
	}
	affected, err := resultRepo.upsertWithResult(ctx, row)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	row.Status = types.SpanStatusDone
	affected, err = resultRepo.upsertWithResult(ctx, row)
	require.NoError(t, err, "conflict update must not fail with SQLSTATE 42702")
	assert.Equal(t, int64(1), affected)

	var count int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND span_id = ?", row.KnowledgeID, row.SpanID).Count(&count).Error)
	assert.Equal(t, int64(1), count)
	var stored types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND span_id = ?", row.KnowledgeID, row.SpanID).Take(&stored).Error)
	assert.Equal(t, types.SpanStatusDone, stored.Status)

	row.Status = types.SpanStatusRunning
	affected, err = resultRepo.upsertWithResult(ctx, row)
	require.NoError(t, err)
	assert.Zero(t, affected, "terminal guard must report that no row was updated")
	require.NoError(t, db.Where("knowledge_id = ? AND span_id = ?", row.KnowledgeID, row.SpanID).Take(&stored).Error)
	assert.Equal(t, types.SpanStatusDone, stored.Status, "terminal guard must reject late updates")
}

func TestKnowledgeSpanRepo_PostgresConcurrentOpenAttemptIsUnique(t *testing.T) {
	repo, db := setupPostgresSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	const workers = 6
	var wg sync.WaitGroup
	attempts := make(chan int, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
				KnowledgeID: "kid-pg-concurrent", SpanID: fmt.Sprintf("root-%d", i),
				Name: "knowledge_processing", Kind: types.SpanKindRoot,
				Status: types.SpanStatusRunning, StartedAt: &now,
			})
			if err != nil {
				errs <- err
				return
			}
			attempts <- attempt
		}(i)
	}
	wg.Wait()
	close(errs)
	close(attempts)
	for err := range errs {
		require.NoError(t, err)
	}
	got := make([]int, 0, workers)
	for attempt := range attempts {
		got = append(got, attempt)
	}
	sort.Ints(got)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)

	var roots int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND kind = 'root'", "kid-pg-concurrent").Count(&roots).Error)
	assert.Equal(t, int64(workers), roots)
}

func TestKnowledgeSpanRepo_PostgresAttemptCommitGuardSerializesWithOpenAttempt(t *testing.T) {
	repo, _ := setupPostgresSpanTestRepo(t)
	guard := repo.(*knowledgeSpanRepository)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-commit-guard", SpanID: "root-guard-1", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	guardResult := make(chan error, 1)
	go func() {
		guardResult <- guard.WithAttemptCommitGuard(ctx, "kid-pg-commit-guard", attempt, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	openResult := make(chan error, 1)
	go func() {
		_, openErr := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
			KnowledgeID: "kid-pg-commit-guard", SpanID: "root-guard-2", Name: "knowledge_processing",
			Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
		})
		openResult <- openErr
	}()
	select {
	case openErr := <-openResult:
		t.Fatalf("OpenAttempt crossed an in-flight PostgreSQL guarded commit: %v", openErr)
	case <-time.After(250 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-guardResult)
	require.NoError(t, <-openResult)

	callbackCalled := false
	err = guard.WithAttemptCommitGuard(ctx, "kid-pg-commit-guard", attempt, func(context.Context) error {
		callbackCalled = true
		return nil
	})
	require.ErrorContains(t, err, "superseded")
	require.False(t, callbackCalled)
}

func TestKnowledgeSpanRepo_PostgresAttemptCommitGuardReusesTransactionOnSingleConnection(t *testing.T) {
	repo, db := setupPostgresSpanTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	knowledge := &types.Knowledge{
		ID: "kid-pg-single-connection", TenantID: 7, KnowledgeBaseID: "kb-pg",
		ParseStatus: types.ParseStatusFinalizing, SummaryStatus: types.SummaryStatusPending,
	}
	require.NoError(t, db.Create(knowledge).Error)
	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: knowledge.ID, SpanID: "root-single-connection", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	knowledgeRepo := NewKnowledgeRepository(db)
	chunkRepo := NewChunkRepository(db)
	chunk := &types.Chunk{
		ID: "chunk-pg-single-connection", TenantID: knowledge.TenantID,
		KnowledgeID: knowledge.ID, KnowledgeBaseID: knowledge.KnowledgeBaseID,
		Content: "guarded", ChunkType: types.ChunkTypeSummary, IsEnabled: true,
	}
	guard := repo.(*knowledgeSpanRepository)
	require.NoError(t, guard.WithAttemptCommitGuard(ctx, knowledge.ID, attempt,
		func(guardedCtx context.Context) error {
			if err := knowledgeRepo.UpdateKnowledgeColumn(
				guardedCtx, knowledge.ID, "summary_status", types.SummaryStatusCompleted,
			); err != nil {
				return err
			}
			return chunkRepo.CreateChunks(guardedCtx, []*types.Chunk{chunk})
		}))

	var storedKnowledge types.Knowledge
	require.NoError(t, db.Where("id = ?", knowledge.ID).Take(&storedKnowledge).Error)
	require.Equal(t, types.SummaryStatusCompleted, storedKnowledge.SummaryStatus)
	var storedChunk types.Chunk
	require.NoError(t, db.Where("id = ?", chunk.ID).Take(&storedChunk).Error)
	require.Equal(t, "guarded", storedChunk.Content)
}

func TestKnowledgeSpanRepo_PostgresRootInsertFailureRollsBackSupersede(t *testing.T) {
	repo, db := setupPostgresSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-insert-rollback", Attempt: 1, SpanID: "old-open",
		Name: "child", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now,
	}))
	require.NoError(t, db.Exec(`CREATE FUNCTION reject_root() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.kind = 'root' THEN RAISE EXCEPTION 'injected root failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reject_root BEFORE INSERT ON knowledge_processing_spans
		FOR EACH ROW EXECUTE FUNCTION reject_root();`).Error)

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-insert-rollback", SpanID: "new-root", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &now,
	})
	require.ErrorContains(t, err, "injected root failure")
	assert.Zero(t, attempt)
	rows, listErr := repo.ListByAttempt(ctx, "kid-pg-insert-rollback", 0)
	require.NoError(t, listErr)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status)
}

func TestKnowledgeSpanRepo_PostgresSupersedeFailureRollsBackRoot(t *testing.T) {
	repo, db := setupPostgresSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-update-rollback", Attempt: 1, SpanID: "old-open",
		Name: "child", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now,
	}))
	require.NoError(t, db.Exec(`CREATE FUNCTION reject_supersede() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected supersede failure'; END $$;
		CREATE TRIGGER reject_supersede BEFORE UPDATE ON knowledge_processing_spans
		FOR EACH ROW EXECUTE FUNCTION reject_supersede();`).Error)

	attempt, err := repo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-update-rollback", SpanID: "new-root", Name: "knowledge_processing",
		Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, StartedAt: &now,
	})
	require.ErrorContains(t, err, "injected supersede failure")
	assert.Zero(t, attempt)
	rows, listErr := repo.ListByAttempt(ctx, "kid-pg-update-rollback", 0)
	require.NoError(t, listErr)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status)
}

// TestKnowledgeSpanRepo_CancelDescendants verifies the cascade walk:
// failing a stage cancels every pending/running descendant in its
// subtree, while terminal states (done/skipped/failed) are left intact.
func TestKnowledgeSpanRepo_CancelDescendants(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	kid := "kid-3"
	now := time.Now()

	// Tree: chunking → embedding (running) → batch[0] (running)
	//                → multimodal (running) → image[0] (done)
	for _, r := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "chunking", Name: types.StageChunking, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "embedding", ParentSpanID: "chunking", Name: types.StageEmbedding, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "batch0", ParentSpanID: "embedding", Name: "embedding.batch[0]", Kind: types.SpanKindGeneration, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "multimodal", ParentSpanID: "chunking", Name: types.StageMultimodal, Kind: types.SpanKindStage, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "image0", ParentSpanID: "multimodal", Name: "multimodal.image[0]", Kind: types.SpanKindGeneration, Status: types.SpanStatusDone, StartedAt: &now},
	} {
		require.NoError(t, repo.Upsert(ctx, r))
	}

	affected, err := repo.CancelDescendants(ctx, kid, 1, "chunking", "test reason")
	require.NoError(t, err)
	// Expected cancellations: embedding, batch0, multimodal (3 rows).
	// The done image0 is terminal and left alone.
	assert.Equal(t, int64(3), affected, "must cancel exactly the 3 pending/running descendants")

	rows, err := repo.ListByAttempt(ctx, kid, 1)
	require.NoError(t, err)
	statusBy := map[string]string{}
	for _, r := range rows {
		statusBy[r.SpanID] = r.Status
	}
	assert.Equal(t, types.SpanStatusRunning, statusBy["chunking"], "the failed span itself stays untouched (FailSpan layer flips it)")
	assert.Equal(t, types.SpanStatusCancelled, statusBy["embedding"])
	assert.Equal(t, types.SpanStatusCancelled, statusBy["batch0"])
	assert.Equal(t, types.SpanStatusCancelled, statusBy["multimodal"])
	assert.Equal(t, types.SpanStatusDone, statusBy["image0"], "terminal states must not be touched")
}

func TestKnowledgeSpanRepo_CancelDescendantsTraversesTerminalParents(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	now := time.Now()
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid-terminal-parent", Attempt: 1, SpanID: "parent", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: "kid-terminal-parent", Attempt: 1, SpanID: "finished-child", ParentSpanID: "parent", Name: "postprocess.wiki.extract", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone, StartedAt: &now},
		{KnowledgeID: "kid-terminal-parent", Attempt: 1, SpanID: "running-grandchild", ParentSpanID: "finished-child", Name: "chat.response.stream", Kind: types.SpanKindGeneration, Status: types.SpanStatusRunning, StartedAt: &now},
	} {
		require.NoError(t, repo.Upsert(ctx, row))
	}

	affected, err := repo.CancelDescendants(ctx, "kid-terminal-parent", 1, "parent", "cancel subtree")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	rows, err := repo.ListByAttempt(ctx, "kid-terminal-parent", 1)
	require.NoError(t, err)
	statusBy := map[string]string{}
	for _, row := range rows {
		statusBy[row.SpanID] = row.Status
	}
	assert.Equal(t, types.SpanStatusDone, statusBy["finished-child"])
	assert.Equal(t, types.SpanStatusCancelled, statusBy["running-grandchild"])
}

func TestKnowledgeSpanRepo_CancelOpenSpansByName(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	kid := "kid-supersede"
	now := time.Now()

	for _, r := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: kid, Attempt: 1, SpanID: "sum-old", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "sum-done", Name: "postprocess.summary", Kind: types.SpanKindSubSpan, Status: types.SpanStatusDone, StartedAt: &now},
		{KnowledgeID: kid, Attempt: 1, SpanID: "q-old", Name: "postprocess.question", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning, StartedAt: &now},
	} {
		require.NoError(t, repo.Upsert(ctx, r))
	}

	affected, err := repo.CancelOpenSpansByName(ctx, kid, 1, "postprocess.summary", "TASK_SUPERSEDED", "retry")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	rows, err := repo.ListByAttempt(ctx, kid, 1)
	require.NoError(t, err)
	statusBy := map[string]string{}
	for _, r := range rows {
		statusBy[r.SpanID] = r.Status
	}
	assert.Equal(t, types.SpanStatusCancelled, statusBy["sum-old"])
	assert.Equal(t, types.SpanStatusDone, statusBy["sum-done"])
	assert.Equal(t, types.SpanStatusRunning, statusBy["q-old"])
}

// TestKnowledgeSpanRepo_ListAttemptIsolation guarantees that different
// attempts of the same knowledge stay queryable independently — the
// foundation for the "show history" UI navigation (?attempt=N).
func TestKnowledgeSpanRepo_ListAttemptIsolation(t *testing.T) {
	repo, _ := setupSpanTestRepo(t)
	ctx := context.Background()
	kid := "kid-history"
	now := time.Now()

	for _, attempt := range []int{1, 2} {
		require.NoError(t, repo.Upsert(ctx, &types.KnowledgeProcessingSpan{
			KnowledgeID: kid, Attempt: attempt, SpanID: "root",
			Name: "knowledge_processing", Kind: types.SpanKindRoot,
			Status: types.SpanStatusDone, StartedAt: &now,
		}))
	}
	a1, err := repo.ListByAttempt(ctx, kid, 1)
	require.NoError(t, err)
	require.Len(t, a1, 1)
	a2, err := repo.ListByAttempt(ctx, kid, 2)
	require.NoError(t, err)
	require.Len(t, a2, 1)

	all, err := repo.ListByAttempt(ctx, kid, 0)
	require.NoError(t, err)
	assert.Len(t, all, 2, "attempt=0 returns all attempts (used by housekeeping)")
}
