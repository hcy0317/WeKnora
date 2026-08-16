package repository

import (
	"context"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const knowledgeSpanRetryPostgresDDL = `
CREATE TABLE knowledges (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(64) NOT NULL,
    parse_status VARCHAR(32) NOT NULL,
    pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
    summary_status VARCHAR(32) NOT NULL DEFAULT 'none',
    error_message TEXT NOT NULL DEFAULT '',
    processed_at TIMESTAMPTZ,
	deleted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE task_pending_ops (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    task_type VARCHAR(64) NOT NULL,
    scope VARCHAR(32) NOT NULL,
    scope_id VARCHAR(64) NOT NULL,
    op VARCHAR(32) NOT NULL,
    dedup_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    fail_count INTEGER NOT NULL DEFAULT 0,
    enqueued_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    claimed_at TIMESTAMPTZ,
    claim_token VARCHAR(64),
    claimed_by_task_id VARCHAR(255),
    claim_heartbeat_at TIMESTAMPTZ
);`

func setupPostgresSpanRetryTestRepo(t *testing.T) (KnowledgeSpanRepository, *gorm.DB) {
	t.Helper()
	repo, db := setupPostgresSpanTestRepo(t)
	require.NoError(t, db.Exec(knowledgeSpanRetryPostgresDDL).Error)
	return repo, db
}

func TestKnowledgeSpanRepo_PostgresConcurrentMultiRetryIsCanonicalAndIdempotent(t *testing.T) {
	repo, db := setupPostgresSpanRetryTestRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status)
		VALUES ('kid-pg-multi-retry', 7, 'kb-pg', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-pg-multi-retry")

	requests := []types.KnowledgeSpanMultiRetryRequest{
		{
			KnowledgeID: "kid-pg-multi-retry", Attempt: 4, ClientRequestID: "same-pg-request",
			Language: "zh-CN",
			Targets: []types.KnowledgeSpanMultiRetryTarget{
				{SpanID: "summary-old"}, {SpanID: "question-batch-3-old"}, {SpanID: "graph-old"},
			},
		},
		{
			KnowledgeID: "kid-pg-multi-retry", Attempt: 4, ClientRequestID: "same-pg-request",
			Language: "zh-CN",
			Targets: []types.KnowledgeSpanMultiRetryTarget{
				{SpanID: "graph-old"}, {SpanID: "question-batch-3-old"}, {SpanID: "summary-old"},
			},
		},
	}
	type retryResult struct {
		prepared []*types.KnowledgeSpanRetryPreparation
		err      error
	}
	results := make(chan retryResult, len(requests))
	var wg sync.WaitGroup
	for _, request := range requests {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			prepared, err := repo.PrepareFailedSpanRetries(context.Background(), request)
			results <- retryResult{prepared: prepared, err: err}
		}()
	}
	wg.Wait()
	close(results)

	var canonical []*types.KnowledgeSpanRetryPreparation
	for result := range results {
		require.NoError(t, result.err)
		require.Len(t, result.prepared, 3)
		require.Equal(t, []string{
			"postprocess.summary",
			"postprocess.question.batch[3]",
			"postprocess.graph.chunk[7]",
		}, []string{result.prepared[0].Name, result.prepared[1].Name, result.prepared[2].Name})
		for _, prepared := range result.prepared {
			require.Equal(t, 5, prepared.Attempt)
		}
		if canonical == nil {
			canonical = result.prepared
		} else {
			require.Equal(t, canonical, result.prepared)
		}
	}
	replay := requests[0]
	replay.Language = ""
	replayed, err := repo.PrepareFailedSpanRetries(context.Background(), replay)
	require.NoError(t, err)
	require.Equal(t, canonical, replayed)
	for _, prepared := range replayed {
		require.Equal(t, "zh-CN", prepared.Language,
			"PostgreSQL idempotent replay must reconstruct the persisted canonical dispatch")
	}

	var roots, posts, targets, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND kind = ?", "kid-pg-multi-retry", 5, types.SpanKindRoot).
		Count(&roots).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid-pg-multi-retry", 5, types.StagePostProcess).
		Count(&posts).Error)
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status = ?", "kid-pg-multi-retry", 5, types.SpanStatusPending).
		Count(&targets).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ? AND scope_id = ?", types.KnowledgeSpanRetryOutboxTaskType, "kid-pg-multi-retry").
		Count(&outboxes).Error)
	require.EqualValues(t, 1, roots)
	require.EqualValues(t, 1, posts)
	require.EqualValues(t, 3, targets)
	require.EqualValues(t, 3, outboxes)
}

func TestKnowledgeSpanRepo_PostgresOutboxFailureRollsBackMultiRetry(t *testing.T) {
	repo, db := setupPostgresSpanRetryTestRepo(t)
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status)
		VALUES ('kid-pg-outbox-rollback', 7, 'kb-pg', 'failed')`).Error)
	seedMultiRetryAttempt(t, repo, "kid-pg-outbox-rollback")
	require.NoError(t, db.Exec(`
		CREATE FUNCTION reject_span_retry_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.task_type = 'knowledge:span_retry_dispatch' THEN
				RAISE EXCEPTION 'injected span retry outbox failure';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_span_retry_outbox
		BEFORE INSERT ON task_pending_ops
		FOR EACH ROW EXECUTE FUNCTION reject_span_retry_outbox();`).Error)

	_, err := repo.PrepareFailedSpanRetries(context.Background(), types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID: "kid-pg-outbox-rollback", Attempt: 4, ClientRequestID: "pg-outbox-failure",
		Targets: []types.KnowledgeSpanMultiRetryTarget{
			{SpanID: "summary-old"}, {SpanID: "graph-old"},
		},
	})
	require.ErrorContains(t, err, "injected span retry outbox failure")

	var freshRows, outboxes int64
	require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ?", "kid-pg-outbox-rollback", 5).
		Count(&freshRows).Error)
	require.NoError(t, db.Model(&types.TaskPendingOp{}).
		Where("task_type = ?", types.KnowledgeSpanRetryOutboxTaskType).
		Count(&outboxes).Error)
	require.Zero(t, freshRows)
	require.Zero(t, outboxes)

	var knowledge struct {
		ParseStatus          string
		PendingSubtasksCount int
		SummaryStatus        string
	}
	require.NoError(t, db.Table("knowledges").
		Select("parse_status", "pending_subtasks_count", "summary_status").
		Where("id = ?", "kid-pg-outbox-rollback").Take(&knowledge).Error)
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Zero(t, knowledge.PendingSubtasksCount)
	require.Equal(t, types.SummaryStatusNone, knowledge.SummaryStatus)
}

func TestKnowledgeRepo_PostgresFinalizeSubtaskForAttemptDoesNotDrainNewAttempt(t *testing.T) {
	spanRepo, db := setupPostgresSpanRetryTestRepo(t)
	knowledgeRepo := NewKnowledgeRepository(db)
	ctx := context.Background()
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id, tenant_id, knowledge_base_id, parse_status, pending_subtasks_count)
		VALUES ('kid-pg-finalize-fence', 7, 'kb-pg', 'finalizing', 1)`).Error)
	attempt1, err := spanRepo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-finalize-fence", SpanID: "pg-finalize-root-1",
		Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)
	attempt2, err := spanRepo.OpenAttempt(ctx, &types.KnowledgeProcessingSpan{
		KnowledgeID: "kid-pg-finalize-fence", SpanID: "pg-finalize-root-2",
		Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning,
	})
	require.NoError(t, err)
	require.Equal(t, attempt1+1, attempt2)

	_, _, err = knowledgeRepo.FinalizeSubtaskForAttempt(ctx, "kid-pg-finalize-fence", attempt1)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.Table("knowledges").Select("pending_subtasks_count").
		Where("id = ?", "kid-pg-finalize-fence").Scan(&count).Error)
	require.Equal(t, 1, count, "superseded PostgreSQL worker must perform zero counter writes")

	_, _, err = knowledgeRepo.FinalizeSubtaskForAttempt(ctx, "kid-pg-finalize-fence", attempt2)
	require.NoError(t, err)
	require.NoError(t, db.Table("knowledges").Select("pending_subtasks_count").
		Where("id = ?", "kid-pg-finalize-fence").Scan(&count).Error)
	require.Zero(t, count)
}
