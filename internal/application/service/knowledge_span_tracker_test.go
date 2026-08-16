package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// span tracker tests use a real GORM-backed repo against an in-memory
// SQLite DB. We do this instead of a stub repo because the cascade /
// LookupStage logic interacts non-trivially with the persistence layer
// (UPSERT, MAX(attempt), parent IN ...) — a stub would let regressions
// in those queries slip through.
//
// We DDL-define the spans table inline (same content as the repo test's
// spansTestDDL — kept duplicated rather than exported because a service
// test crossing into the repository test file's identifiers couples the
// two too tightly).
const spanTrackerTestDDL = `
CREATE TABLE IF NOT EXISTS knowledges (
    id                     VARCHAR(64) PRIMARY KEY,
    parse_status           VARCHAR(32) NOT NULL,
    pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
    error_message          TEXT NOT NULL DEFAULT '',
    processed_at           DATETIME,
    updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
);
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

func setupSpanTrackerTest(t *testing.T) (SpanTracker, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(spanTrackerTestDDL).Error)
	// Pass nil for the heartbeat db: these tests don't exercise heartbeat
	// side-effects (those are covered in the housekeeping suite). The
	// knowledges table remains present for settlement and queue-state guards.
	repo := repository.NewKnowledgeSpanRepository(db)
	return NewSpanTracker(repo, nil), db
}

func TestSpanTracker_QueuedChildClaimReusesSpanWithoutFalseCancellation(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	root, attempt, err := tracker.OpenAttempt(ctx, "kid-queued-claim", "")
	require.NoError(t, err)
	queued := tracker.QueueSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan,
		types.JSONMap{"queued_input": true})
	require.NotNil(t, queued)
	assert.Equal(t, types.SpanStatusPending, queued.Status)
	duplicate := tracker.QueueSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan, nil)
	require.NotNil(t, duplicate)
	assert.Equal(t, queued.SpanID, duplicate.SpanID, "duplicate enqueue must reuse the pending claim")

	running := tracker.BeginSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan,
		types.JSONMap{"worker_input": true})
	require.NotNil(t, running)
	assert.Equal(t, queued.SpanID, running.SpanID)
	assert.Equal(t, attempt, running.Attempt)
	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid-queued-claim", attempt, "postprocess.summary").Find(&rows).Error)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanStatusRunning, rows[0].Status)
	assert.Empty(t, rows[0].ErrorCode)
	assert.Equal(t, true, rows[0].Input["queued_input"])
	assert.Equal(t, true, rows[0].Input["worker_input"])
}

func TestSpanTracker_FailingPendingChildNormalizesStartedAt(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	root, attempt, err := tracker.OpenAttempt(ctx, "kid-pending-fail-time", "")
	require.NoError(t, err)
	queued := tracker.QueueSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan, nil)
	require.NotNil(t, queued)
	before := time.Now().Add(-time.Second)
	tracker.FailSpan(ctx, queued, "ENQUEUE_FAILED", "queue unavailable", errors.New("queue unavailable"))
	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-pending-fail-time", attempt, queued.SpanID).Take(&row).Error)
	require.NotNil(t, row.StartedAt)
	assert.True(t, row.StartedAt.After(before))
	assert.GreaterOrEqual(t, row.DurationMs, int64(0))
}

func TestSpanTracker_QueueSubSpanRejectsCancelledKnowledgeAndSupersededAttempt(t *testing.T) {
	t.Run("cancelled knowledge", func(t *testing.T) {
		tracker, db := setupSpanTrackerTest(t)
		ctx := context.Background()
		_, attempt, err := tracker.OpenAttempt(ctx, "kid-queue-cancelled", "")
		require.NoError(t, err)
		post := tracker.BeginStage(ctx, "kid-queue-cancelled", attempt, types.StagePostProcess, nil)
		require.NotNil(t, post)
		require.NoError(t, db.Exec(`INSERT INTO knowledges
			(id, parse_status, pending_subtasks_count) VALUES (?, ?, 1)`,
			"kid-queue-cancelled", types.ParseStatusCancelled).Error)

		assert.Nil(t, tracker.QueueSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil))
		var count int64
		require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND name = ?",
				"kid-queue-cancelled", attempt, "postprocess.summary").Count(&count).Error)
		assert.Zero(t, count)
	})

	t.Run("superseded attempt", func(t *testing.T) {
		tracker, db := setupSpanTrackerTest(t)
		ctx := context.Background()
		_, oldAttempt, err := tracker.OpenAttempt(ctx, "kid-queue-superseded", "")
		require.NoError(t, err)
		oldPost := tracker.BeginStage(ctx, "kid-queue-superseded", oldAttempt, types.StagePostProcess, nil)
		require.NotNil(t, oldPost)
		require.NoError(t, db.Exec(`INSERT INTO knowledges
			(id, parse_status, pending_subtasks_count) VALUES (?, ?, 1)`,
			"kid-queue-superseded", types.ParseStatusFinalizing).Error)
		_, latestAttempt, err := tracker.OpenAttempt(ctx, "kid-queue-superseded", "")
		require.NoError(t, err)
		require.Greater(t, latestAttempt, oldAttempt)

		assert.Nil(t, tracker.QueueSubSpan(ctx, oldPost, "postprocess.summary", types.SpanKindSubSpan, nil))
		var count int64
		require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND name = ?",
				"kid-queue-superseded", oldAttempt, "postprocess.summary").Count(&count).Error)
		assert.Zero(t, count)
	})
}

func TestSpanTracker_RealRetryPreservesFailedHistory(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	root, attempt, err := tracker.OpenAttempt(ctx, "kid-real-retry", "")
	require.NoError(t, err)
	first := tracker.BeginSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan, nil)
	require.NotNil(t, first)
	tracker.FailSpan(ctx, first, "SUMMARY_FAILED", "first delivery failed", errors.New("first delivery failed"))

	retry := tracker.BeginSubSpan(ctx, root, "postprocess.summary", types.SpanKindSubSpan, nil)
	require.NotNil(t, retry)
	assert.NotEqual(t, first.SpanID, retry.SpanID)
	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND name = ?",
		"kid-real-retry", attempt, "postprocess.summary").Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2)
	assert.Equal(t, types.SpanStatusFailed, rows[0].Status)
	assert.Equal(t, types.SpanStatusRunning, rows[1].Status)
}

// TestSpanTracker_OpenAttempt_AllocatesFreshNumbers covers the contract
// that drives reparse history: each OpenAttempt must hand out a strictly
// increasing attempt number per knowledge, and previous attempts'
// rows must remain queryable (via a separate ?attempt=N navigation).
func TestSpanTracker_OpenAttempt_AllocatesFreshNumbers(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	root1, n1, err := tracker.OpenAttempt(ctx, "kid", "trace-1")
	require.NoError(t, err)
	require.NotNil(t, root1)
	assert.Equal(t, 1, n1)

	root2, n2, err := tracker.OpenAttempt(ctx, "kid", "trace-2")
	require.NoError(t, err)
	require.NotNil(t, root2)
	assert.Equal(t, 2, n2, "second OpenAttempt must allocate attempt 2")
	assert.NotEqual(t, root1.SpanID, root2.SpanID, "each attempt has its own root span ID")

	// Both roots must persist — a reparse must NOT erase the previous
	// attempt's history.
	var count int64
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND kind = 'root'", "kid").
		Count(&count).Error)
	assert.Equal(t, int64(2), count, "previous attempt's root must remain after reparse")
}

func TestSpanTracker_OpenAttemptFailureDoesNotUpdateMemory(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_root_insert BEFORE INSERT ON knowledge_processing_spans
		WHEN NEW.kind = 'root' BEGIN SELECT RAISE(ABORT, 'injected root failure'); END;`).Error)

	root, attempt, err := tracker.OpenAttempt(context.Background(), "kid-open-failure", "trace")
	require.ErrorContains(t, err, "injected root failure")
	assert.Nil(t, root)
	assert.Zero(t, attempt)

	impl := tracker.(*spanTracker)
	impl.startsMu.Lock()
	defer impl.startsMu.Unlock()
	assert.Empty(t, impl.starts, "failed repository transaction must not populate in-memory start times")
}

// TestSpanTracker_FailSpan_CascadesDownstream verifies that failing a
// stage flips its dependent stages to "cancelled" so the UI shows a
// clear blast radius instead of orphan spinners. This is the central
// guarantee of the DAG model — without it, a Chunking failure leaves
// Embedding/Multimodal/PostProcess as pending forever.
func TestSpanTracker_FailSpan_CascadesDownstream(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)
	require.Equal(t, 1, attempt)

	// Begin every stage so the cascade has something to cancel.
	docreader := tracker.BeginStage(ctx, "kid", attempt, types.StageDocReader, nil)
	tracker.EndSpan(ctx, docreader, nil)
	chunking := tracker.BeginStage(ctx, "kid", attempt, types.StageChunking, nil)
	embedding := tracker.BeginStage(ctx, "kid", attempt, types.StageEmbedding, nil)
	multimodal := tracker.BeginStage(ctx, "kid", attempt, types.StageMultimodal, nil)
	postprocess := tracker.BeginStage(ctx, "kid", attempt, types.StagePostProcess, nil)

	// Fail Chunking. Embedding/Multimodal/PostProcess must cascade.
	tracker.FailSpan(ctx, chunking, "CHUNKING_FAILED", "synthetic", errors.New("boom"))

	statusBy := map[string]string{}
	type row struct {
		Name, Status string
	}
	var rows []row
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("name, status").
		Where("knowledge_id = ? AND attempt = ?", "kid", attempt).
		Find(&rows).Error)
	for _, r := range rows {
		statusBy[r.Name] = r.Status
	}

	assert.Equal(t, types.SpanStatusDone, statusBy[types.StageDocReader], "upstream stage stays done")
	assert.Equal(t, types.SpanStatusFailed, statusBy[types.StageChunking], "the failed stage itself stays failed")
	assert.Equal(t, types.SpanStatusCancelled, statusBy[types.StageEmbedding], "direct dependent must cascade")
	assert.Equal(t, types.SpanStatusCancelled, statusBy[types.StageMultimodal], "sibling dependent must cascade")
	assert.Equal(t, types.SpanStatusCancelled, statusBy[types.StagePostProcess], "transitive dependent must cascade")

	// Quiet the unused-variable check: embedding / multimodal /
	// postprocess pointers were used to seed the table; their state
	// is now in statusBy. Linter still wants them "consumed".
	_ = embedding
	_ = multimodal
	_ = postprocess
}

// TestSpanTracker_LookupStage_FindsAcrossProcesses simulates the
// cross-process bridge an asynq worker uses: the upstream pipeline
// creates the multimodal stage span, then a separate worker process
// must locate it by (kid, attempt, name) to attach its image subspan.
func TestSpanTracker_LookupStage_FindsAcrossProcesses(t *testing.T) {
	tracker, _ := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)
	mm := tracker.BeginStage(ctx, "kid", attempt, types.StageMultimodal, nil)
	require.NotNil(t, mm)

	// Pretend we're a different process — the in-memory `starts`
	// cache is the same map here, but the cross-process semantics
	// don't depend on it; LookupStage hits the DB.
	found := tracker.LookupStage(ctx, "kid", attempt, types.StageMultimodal)
	require.NotNil(t, found)
	assert.Equal(t, mm.SpanID, found.SpanID, "LookupStage must return the same span row")

	// A different stage must not be confused with multimodal.
	other := tracker.LookupStage(ctx, "kid", attempt, types.StageEmbedding)
	assert.Nil(t, other, "LookupStage(missing) must return nil")
}

func TestFitSpanName(t *testing.T) {
	short := "postprocess.wiki.extract"
	if got := fitSpanName(short); got != short {
		t.Fatalf("short name should pass through, got %q", got)
	}

	// Regression: names in the 65–223 char window failed under VARCHAR(64)
	// but must pass through unchanged at VARCHAR(255).
	mid := "postprocess.wiki.page[concept/" + strings.Repeat("a", 100) + "]"
	if got := fitSpanName(mid); got != mid {
		t.Fatalf("mid-length wiki page name should pass through, got %q", got)
	}

	// Use a synthetic overlong wiki span name (> varchar(255)).
	long := "postprocess.wiki.page[concept/" + strings.Repeat("a", 280) + "]"
	got := fitSpanName(long)
	if utf8.RuneCountInString(got) > maxSpanNameLen {
		t.Fatalf("fitted name runes=%d, want <= %d: %q", utf8.RuneCountInString(got), maxSpanNameLen, got)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("fitted name must be valid UTF-8: %q", got)
	}
	if got == long {
		t.Fatalf("expected truncation, got unchanged %q", got)
	}
	if fitSpanName(long) != got {
		t.Fatal("fitSpanName must be deterministic")
	}

	other := "postprocess.wiki.page[concept/" + strings.Repeat("b", 280) + "]"
	if fitSpanName(other) == got {
		t.Fatalf("different long names must not collapse to the same fitted name")
	}

	// CJK slugs must truncate on rune boundaries, not byte boundaries.
	cjkLong := "postprocess.wiki.page[" + strings.Repeat("中", 260) + "]"
	cjkGot := fitSpanName(cjkLong)
	if utf8.RuneCountInString(cjkGot) > maxSpanNameLen {
		t.Fatalf("CJK fitted name runes=%d, want <= %d", utf8.RuneCountInString(cjkGot), maxSpanNameLen)
	}
	if !utf8.ValidString(cjkGot) {
		t.Fatalf("CJK fitted name must be valid UTF-8: %q", cjkGot)
	}
}

// TestSpanTracker_BeginSubSpan_LongWikiPageName verifies wiki ingest's
// postprocess.wiki.page[<slug>] subspans persist even when the slug pushes
// the name past varchar(255).
func TestSpanTracker_BeginSubSpan_LongWikiPageName(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)
	parent := tracker.BeginStage(ctx, "kid", attempt, types.StagePostProcess, nil)
	require.NotNil(t, parent)

	rawName := "postprocess.wiki.page[concept/" + strings.Repeat("x", 280) + "]"
	sub := tracker.BeginSubSpan(ctx, parent, rawName, types.SpanKindSubSpan, types.JSONMap{
		"slug": "concept/" + strings.Repeat("x", 280),
	})
	require.NotNil(t, sub)
	require.LessOrEqual(t, utf8.RuneCountInString(sub.Name), maxSpanNameLen)

	var count int64
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND name = ?", "kid", sub.Name).
		Count(&count).Error)
	require.Equal(t, int64(1), count)
}

// TestSpanTracker_LookupSpanByName_FitsLongName verifies cross-process
// callers can look up a wiki page subspan using the raw overlong name.
func TestSpanTracker_LookupSpanByName_FitsLongName(t *testing.T) {
	tracker, _ := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)
	parent := tracker.BeginStage(ctx, "kid", attempt, types.StagePostProcess, nil)
	require.NotNil(t, parent)

	rawName := "postprocess.wiki.page[concept/" + strings.Repeat("y", 280) + "]"
	created := tracker.BeginSubSpan(ctx, parent, rawName, types.SpanKindSubSpan, nil)
	require.NotNil(t, created)

	found := tracker.LookupSpanByName(ctx, "kid", attempt, rawName)
	require.NotNil(t, found, "LookupSpanByName must normalize the raw name")
	assert.Equal(t, created.SpanID, found.SpanID)
	assert.Equal(t, created.Name, found.Name)
}

func TestSpanTracker_LookupSpanByName_ReturnsLatestRetrySpan(t *testing.T) {
	tracker, _ := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid-retry-parent", "")
	require.NoError(t, err)
	parent := tracker.BeginStage(ctx, "kid-retry-parent", attempt, types.StageMultimodal, nil)
	require.NotNil(t, parent)

	first := tracker.BeginSubSpan(ctx, parent, "multimodal.image[2]", types.SpanKindGeneration, nil)
	require.NotNil(t, first)
	second := tracker.BeginSubSpan(ctx, parent, "multimodal.image[2]", types.SpanKindGeneration, nil)
	require.NotNil(t, second)
	require.NotEqual(t, first.SpanID, second.SpanID)

	found := tracker.LookupSpanByName(ctx, "kid-retry-parent", attempt, "multimodal.image[2]")
	require.NotNil(t, found)
	assert.Equal(t, second.SpanID, found.SpanID,
		"vlm.predict from a retry must attach to the new image span, not the superseded row")
}

// TestSpanTracker_BeginSubSpan_HangsUnderParent confirms multimodal /
// embedding fan-out subspans reference the parent stage's span_id —
// the structural invariant the buildSpanTree handler walks.
func TestSpanTracker_BeginSubSpan_HangsUnderParent(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)
	parent := tracker.BeginStage(ctx, "kid", attempt, types.StageMultimodal, nil)
	require.NotNil(t, parent)

	sub := tracker.BeginSubSpan(ctx, parent, "multimodal.image[0]", types.SpanKindGeneration, types.JSONMap{
		"image_url": "x",
	})
	require.NotNil(t, sub)

	type row struct {
		Name, Kind, ParentSpanID string
	}
	var rows []row
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("name, kind, parent_span_id").
		Where("knowledge_id = ? AND name = ?", "kid", "multimodal.image[0]").
		Find(&rows).Error)
	require.Len(t, rows, 1)
	assert.Equal(t, types.SpanKindGeneration, rows[0].Kind)
	assert.Equal(t, parent.SpanID, rows[0].ParentSpanID, "subspan must reference parent stage's span_id")
}

func TestSpanTracker_RecordGeneration_PersistsUsageUnderProcessingStage(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-usage", "trace-root")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-usage", attempt, types.StagePostProcess, nil)
	require.NotNil(t, post)
	summary := tracker.BeginSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil)
	require.NotNil(t, summary)
	started := time.Now().Add(-250 * time.Millisecond)
	finished := time.Now()
	tracker.RecordGeneration(ctx, types.KnowledgeGenerationUsage{
		KnowledgeID:     "kid-usage",
		Attempt:         attempt,
		TraceID:         "trace-1",
		SpanID:          "generation-1",
		Stage:           "postprocess.summary",
		TaskType:        types.TypeSummaryGeneration,
		Name:            "chat.completion",
		ModelType:       "chat",
		ModelID:         "model-1",
		ModelName:       "gpt-test",
		Purpose:         "document_summary",
		InputTokens:     100,
		OutputTokens:    20,
		TotalTokens:     120,
		CacheReadTokens: 80,
		Unit:            "TOKENS",
		UsageAvailable:  true,
		Status:          types.SpanStatusDone,
		StartedAt:       started,
		FinishedAt:      finished,
	})

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND span_id = ?", "kid-usage", "generation-1").First(&row).Error)
	assert.Equal(t, types.SpanKindGeneration, row.Kind)
	assert.Equal(t, summary.SpanID, row.ParentSpanID)
	assert.Equal(t, "gpt-test", row.Metadata["model_name"])
	usage, ok := row.Output["usage"].(map[string]interface{})
	if !ok {
		usageMap, mapOK := row.Output["usage"].(types.JSONMap)
		require.True(t, mapOK)
		usage = map[string]interface{}(usageMap)
	}
	assert.EqualValues(t, 120, usage["total_tokens"])
}

func TestSpanTracker_RecordGenerationKeepsRetryAndCancellationHistory(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-generation-history", "trace-root")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-generation-history", attempt, types.StagePostProcess, nil)
	extract := tracker.BeginSubSpan(ctx, post, "postprocess.wiki.extract", types.SpanKindSubSpan, nil)
	require.NotNil(t, extract)
	started := time.Now().Add(-time.Second)

	first := types.KnowledgeGenerationUsage{
		KnowledgeID: "kid-generation-history",
		Attempt:     attempt,
		SpanID:      "wiki-generation-first",
		Stage:       "postprocess.wiki.extract",
		Name:        "chat.response.stream",
		Status:      types.SpanStatusRunning,
		StartedAt:   started,
	}
	tracker.RecordGeneration(ctx, first)

	var running types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND span_id = ?", first.KnowledgeID, first.SpanID).
		First(&running).Error)
	assert.Equal(t, types.SpanStatusRunning, running.Status)
	assert.Nil(t, running.FinishedAt, "active generation must remain visibly unfinished")

	first.Status = types.SpanStatusCancelled
	first.ErrorMessage = context.Canceled.Error()
	first.FinishedAt = time.Now()
	tracker.RecordGeneration(ctx, first)
	tracker.RecordGeneration(ctx, types.KnowledgeGenerationUsage{
		KnowledgeID: first.KnowledgeID,
		Attempt:     attempt,
		SpanID:      "wiki-generation-retry",
		Stage:       first.Stage,
		Name:        first.Name,
		Status:      types.SpanStatusDone,
		StartedAt:   time.Now().Add(-100 * time.Millisecond),
		FinishedAt:  time.Now(),
	})

	var rows []types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND kind = ?", first.KnowledgeID, types.SpanKindGeneration).
		Order("id ASC").Find(&rows).Error)
	require.Len(t, rows, 2, "retry must append a new generation instead of replacing cancelled history")
	assert.Equal(t, types.SpanStatusCancelled, rows[0].Status)
	assert.Equal(t, types.SpanStatusDone, rows[1].Status)
	assert.NotNil(t, rows[0].FinishedAt)
}

func TestSpanTracker_SettleQuestionGroup_WaitsForEveryBatch(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-wait", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-wait", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	batch0 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	batch1 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[1]", types.SpanKindSubSpan, nil)

	tracker.EndSpan(ctx, batch0, nil)
	tracker.SettleQuestionGroup(ctx, "kid-question-wait", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusRunning)

	tracker.EndSpan(ctx, batch1, nil)
	tracker.SettleQuestionGroup(ctx, "kid-question-wait", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusDone)
}

func TestSpanTracker_SettleQuestionGroup_RequiresExactBatchSlots(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-exact-slots", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-exact-slots", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	batch0 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	batch99 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[99]", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, batch0, nil)
	tracker.EndSpan(ctx, batch99, nil)

	tracker.SettleQuestionGroup(ctx, "kid-question-exact-slots", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusRunning)

	batch1 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[1]", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, batch1, nil)
	tracker.SettleQuestionGroup(ctx, "kid-question-exact-slots", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusDone)
}

func TestSpanTracker_SettlePostProcessTree_LegacyPlanWithoutBranchesStaysRunning(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-postprocess-legacy-empty", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-postprocess-legacy-empty", attempt, types.StagePostProcess, nil)

	tracker.SettlePostProcessTree(ctx, "kid-postprocess-legacy-empty", attempt)
	assertQuestionGroupStatus(t, db, post.SpanID, types.SpanStatusRunning)
}

func TestSpanTracker_SettleQuestionGroup_FailsOnlyAfterEveryBatchIsTerminal(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-fail", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-fail", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	batch0 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	batch1 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[1]", types.SpanKindSubSpan, nil)

	tracker.FailSpan(ctx, batch0, "QUESTION_FAILED", "upstream failed", errors.New("boom"))
	tracker.SettleQuestionGroup(ctx, "kid-question-fail", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusRunning)

	tracker.EndSpan(ctx, batch1, nil)
	tracker.SettleQuestionGroup(ctx, "kid-question-fail", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusFailed)
}

func TestSpanTracker_SettleQuestionGroup_UsesLatestRetryForEachBatch(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-retry", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-retry", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 1})
	_ = tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	retry := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)

	tracker.SettleQuestionGroup(ctx, "kid-question-retry", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusRunning)

	tracker.EndSpan(ctx, retry, nil)
	tracker.SettleQuestionGroup(ctx, "kid-question-retry", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusDone)
}

func TestSpanTracker_SettleQuestionGroup_PersistsWithCancelledContext(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-cancelled-ctx", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-cancelled-ctx", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 1})
	batch := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, batch, nil)

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	tracker.SettleQuestionGroup(cancelledCtx, "kid-question-cancelled-ctx", attempt)
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusDone)
}

func TestSpanTracker_SettleQuestionGroup_ConcurrentLastBatchesDoNotHang(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-concurrent", "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, "kid-question-concurrent", attempt, types.StagePostProcess, nil)
	group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
		types.JSONMap{"batch_count": 2})
	batch0 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)
	batch1 := tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[1]", types.SpanKindSubSpan, nil)

	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for _, batch := range []*Span{batch0, batch1} {
		go func(batch *Span) {
			<-start
			tracker.EndSpan(ctx, batch, nil)
			tracker.SettleQuestionGroup(ctx, "kid-question-concurrent", attempt)
			done <- struct{}{}
		}(batch)
	}
	close(start)
	<-done
	<-done
	assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusDone)
}

func TestSpanTracker_QuestionGroup_CancelAndSupersedeAreTerminal(t *testing.T) {
	t.Run("user cancel", func(t *testing.T) {
		tracker, db := setupSpanTrackerTest(t)
		ctx := context.Background()
		_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-user-cancel", "")
		require.NoError(t, err)
		post := tracker.BeginStage(ctx, "kid-question-user-cancel", attempt, types.StagePostProcess, nil)
		group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
			types.JSONMap{"batch_count": 1})
		_ = tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)

		tracker.AbortAttempt(ctx, "kid-question-user-cancel", attempt,
			"USER_CANCELLED", "cancelled", "user cancelled")
		assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusCancelled)
	})

	t.Run("new attempt supersedes old", func(t *testing.T) {
		tracker, db := setupSpanTrackerTest(t)
		ctx := context.Background()
		_, attempt, err := tracker.OpenAttempt(ctx, "kid-question-superseded", "")
		require.NoError(t, err)
		post := tracker.BeginStage(ctx, "kid-question-superseded", attempt, types.StagePostProcess, nil)
		group := tracker.BeginSubSpan(ctx, post, postprocessQuestionGroupSpanName, types.SpanKindSubSpan,
			types.JSONMap{"batch_count": 1})
		_ = tracker.BeginSubSpan(ctx, group, "postprocess.question.batch[0]", types.SpanKindSubSpan, nil)

		_, _, err = tracker.OpenAttempt(ctx, "kid-question-superseded", "")
		require.NoError(t, err)
		assertQuestionGroupStatus(t, db, group.SpanID, types.SpanStatusCancelled)
	})
}

func assertQuestionGroupStatus(t *testing.T, db *gorm.DB, spanID, expected string) {
	t.Helper()
	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").Where("span_id = ?", spanID).First(&row).Error)
	assert.Equal(t, expected, row.Status)
}

func assertProcessingSpanStatus(t *testing.T, db *gorm.DB, knowledgeID string, attempt int, name, kind, expected string) {
	t.Helper()
	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ? AND kind = ?", knowledgeID, attempt, name, kind).
		First(&row).Error)
	assert.Equal(t, expected, row.Status)
}

func TestSettlePostProcessTree_WaitsForEveryDurableBranch(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	const knowledgeID = "kid-postprocess-waits"
	_, attempt, err := tracker.OpenAttempt(ctx, knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, knowledgeID, attempt, types.StagePostProcess, nil)
	summary := tracker.BeginSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil)
	wiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)

	tracker.EndSpan(ctx, summary, nil)
	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusRunning)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusRunning)

	tracker.EndSpan(ctx, wiki, nil)
	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
}

func TestSettlePostProcessTree_PropagatesFinalBranchFailureToParents(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	const knowledgeID = "kid-postprocess-failed"
	_, attempt, err := tracker.OpenAttempt(ctx, knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, knowledgeID, attempt, types.StagePostProcess, nil)
	summary := tracker.BeginSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil)

	tracker.FailSpan(ctx, summary, "SUMMARY_FAILED", "upstream failed", errors.New("upstream failed"))
	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusFailed)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusFailed)
}

func TestSettlePostProcessTree_UsesLatestRetryForLogicalBranch(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	const knowledgeID = "kid-postprocess-retry"
	_, attempt, err := tracker.OpenAttempt(ctx, knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, knowledgeID, attempt, types.StagePostProcess, nil)

	failedWiki := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	tracker.FailSpan(ctx, failedWiki, "WIKI_FAILED", "transient", errors.New("transient"))
	successfulRetry := tracker.BeginSubSpan(ctx, post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, successfulRetry, nil)
	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
}

func TestSettlePostProcessTree_FailsWhenPlannedBranchIsMissing(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	const knowledgeID = "kid-postprocess-missing-planned-branch"
	_, attempt, err := tracker.OpenAttempt(ctx, knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, knowledgeID, attempt, types.StagePostProcess, types.JSONMap{
		"expected_branches": []string{"postprocess.summary", "postprocess.wiki"},
		"fanout_complete":   true,
	})
	summary := tracker.BeginSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, summary, nil)

	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusFailed)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusFailed)
	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Table("knowledge_processing_spans").
		Where("knowledge_id = ? AND attempt = ? AND name = ?", knowledgeID, attempt, types.StagePostProcess).
		First(&row).Error)
	assert.Equal(t, "POSTPROCESS_BRANCH_MISSING", row.ErrorCode)
}

func TestSettlePostProcessTree_PersistsThroughCancelledWorkerContext(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	const knowledgeID = "kid-postprocess-cancelled-context"
	_, attempt, err := tracker.OpenAttempt(ctx, knowledgeID, "")
	require.NoError(t, err)
	post := tracker.BeginStage(ctx, knowledgeID, attempt, types.StagePostProcess, nil)
	summary := tracker.BeginSubSpan(ctx, post, "postprocess.summary", types.SpanKindSubSpan, nil)
	tracker.EndSpan(ctx, summary, nil)
	cancel()

	tracker.SettlePostProcessTree(ctx, knowledgeID, attempt)

	assertProcessingSpanStatus(t, db, knowledgeID, attempt, types.StagePostProcess,
		types.SpanKindStage, types.SpanStatusDone)
	assertProcessingSpanStatus(t, db, knowledgeID, attempt, "knowledge_processing",
		types.SpanKindRoot, types.SpanStatusDone)
}

// TestSpanTracker_BeginStage_DoesNotReenterAfterTerminalRoot guarantees that
// a late delivery cannot reopen a stage after its main-pipeline failure has
// already closed the attempt root. Retryable failures are no longer finalized
// until their last Asynq delivery, so a failed root is a terminal boundary.
func TestSpanTracker_BeginStage_DoesNotReenterAfterTerminalRoot(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)

	first := tracker.BeginStage(ctx, "kid", attempt, types.StageDocReader, types.JSONMap{"pages": 1})
	require.NotNil(t, first)
	// A main-stage failure closes both the stage and the attempt root.
	tracker.FailSpan(ctx, first, "TEST", "first failure", errors.New("boom"))

	second := tracker.BeginStage(ctx, "kid", attempt, types.StageDocReader, types.JSONMap{"pages": 2})
	require.Nil(t, second)

	type row struct {
		SpanID, Status string
	}
	var rows []row
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("span_id, status").
		Where("knowledge_id = ? AND attempt = ? AND name = ?", "kid", attempt, types.StageDocReader).
		Find(&rows).Error)
	require.Len(t, rows, 1, "exactly one row per (knowledge, attempt, stage)")
	assert.Equal(t, types.SpanStatusFailed, rows[0].Status,
		"terminal attempt stages must stay frozen")
}

func TestSpanTracker_BeginStage_ReentryWithNilInputPreservesPersistedPlan(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-stage-input", "")
	require.NoError(t, err)
	want := types.JSONMap{
		"fanout_complete":   false,
		"expected_branches": []string{"postprocess.summary", "postprocess.wiki"},
		"fanout_plan":       types.JSONMap{"version": 1, "wiki": true},
	}
	first := tracker.BeginStage(ctx, "kid-stage-input", attempt, types.StagePostProcess, want)
	require.NotNil(t, first)

	second := tracker.BeginStage(ctx, "kid-stage-input", attempt, types.StagePostProcess, nil)
	require.NotNil(t, second)
	assert.Equal(t, false, second.Input["fanout_complete"])
	assert.ElementsMatch(t, []any{"postprocess.summary", "postprocess.wiki"}, second.Input["expected_branches"])

	var row types.KnowledgeProcessingSpan
	require.NoError(t, db.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
		"kid-stage-input", attempt, first.SpanID).Take(&row).Error)
	assert.Equal(t, false, row.Input["fanout_complete"])
	assert.ElementsMatch(t, []any{"postprocess.summary", "postprocess.wiki"}, row.Input["expected_branches"])
	require.NotNil(t, row.Input["fanout_plan"])
}

// TestSpanTracker_FailSpan_CascadesDependentSubspans verifies that when a
// chunking failure flips Embedding to "cancelled" (sibling cascade),
// embedding's already-running subspan (e.g. embedding.batch[0]) is ALSO
// cancelled. Without this, the UI rendered a cancelled stage with an
// orphan running batch hanging underneath.
func TestSpanTracker_FailSpan_CascadesDependentSubspans(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()

	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "")
	require.NoError(t, err)

	chunking := tracker.BeginStage(ctx, "kid", attempt, types.StageChunking, nil)
	embedding := tracker.BeginStage(ctx, "kid", attempt, types.StageEmbedding, nil)
	require.NotNil(t, embedding)
	// Subspan attached to the dependent (sibling) stage that's about to
	// be cascade-cancelled.
	batch := tracker.BeginSubSpan(ctx, embedding, "embedding.batch[0]", types.SpanKindGeneration, nil)
	require.NotNil(t, batch)

	tracker.FailSpan(ctx, chunking, "CHUNKING_FAILED", "synthetic", errors.New("boom"))

	type row struct {
		Name, Status string
	}
	var rows []row
	require.NoError(t, db.Table("knowledge_processing_spans").
		Select("name, status").
		Where("knowledge_id = ?", "kid").
		Find(&rows).Error)
	statusBy := map[string]string{}
	for _, r := range rows {
		statusBy[r.Name] = r.Status
	}
	assert.Equal(t, types.SpanStatusCancelled, statusBy[types.StageEmbedding],
		"dependent stage cascades to cancelled")
	assert.Equal(t, types.SpanStatusCancelled, statusBy["embedding.batch[0]"],
		"subspan under the cascaded stage must also be cancelled")
}

// TestPostprocessSubspan_AttachesUnderPostProcessStage covers the contract
// that the async post-pipeline tasks (summary, question, graph) rely on:
// after the parsing pipeline closes the postprocess stage span, an
// out-of-band worker can still LookupStage + BeginSubSpan to record its
// real processing time as a child of postprocess. Without this guarantee
// the trace viewer's postprocess row stays at the ~10ms enqueue duration
// even when summary generation takes 20 s.
func TestPostprocessSubspan_AttachesUnderPostProcessStage(t *testing.T) {
	tracker, db := setupSpanTrackerTest(t)
	ctx := context.Background()
	repo := repository.NewKnowledgeSpanRepository(db)

	// Set up the parent attempt with a closed postprocess stage — the
	// async worker must still find it via LookupStage.
	_, attempt, err := tracker.OpenAttempt(ctx, "kid", "lf-trace")
	require.NoError(t, err)

	post := tracker.BeginStage(ctx, "kid", attempt, types.StagePostProcess, types.JSONMap{
		"chunks_total": 20,
	})
	require.NotNil(t, post)
	tracker.EndSpan(ctx, post, types.JSONMap{"enqueued_summary": true})

	// Simulate ProcessSummaryGeneration entering: lookup parent +
	// BeginSubSpan (the same call shape as beginPostprocessSubspan).
	parent := tracker.LookupStage(ctx, "kid", attempt, types.StagePostProcess)
	require.NotNil(t, parent, "lookup must succeed even after EndSpan closed the parent")
	assert.Equal(t, types.StagePostProcess, parent.Name)
	assert.Equal(t, types.SpanKindStage, parent.Kind)

	sumSpan := tracker.BeginSubSpan(ctx, parent, "postprocess.summary", types.SpanKindSubSpan,
		types.JSONMap{"language": "zh-CN"})
	require.NotNil(t, sumSpan)
	assert.Equal(t, parent.SpanID, sumSpan.ParentSpanID,
		"subspan must hang off the postprocess stage's span_id")
	assert.Equal(t, types.SpanKindSubSpan, sumSpan.Kind)

	tracker.EndSpan(ctx, sumSpan, types.JSONMap{
		"text_chunks":   20,
		"summary_chars": 142,
	})

	// Verify the row landed under the right parent with the right name.
	rows, err := repo.ListByAttempt(ctx, "kid", attempt)
	require.NoError(t, err)
	var found *types.KnowledgeProcessingSpan
	for i := range rows {
		if rows[i].Name == "postprocess.summary" {
			cp := rows[i]
			found = &cp
			break
		}
	}
	require.NotNil(t, found, "summary subspan row must persist")
	assert.Equal(t, parent.SpanID, found.ParentSpanID,
		"persisted parent_span_id matches LookupStage result")
	assert.Equal(t, types.SpanStatusDone, found.Status)
	assert.NotNil(t, found.Output, "EndSpan must record the output map")
}

// TestPostprocessSubspan_MissingParentFallsThrough covers the legacy
// path: an in-flight async task may carry attempt=0 (queued before the
// span-tracking field was added) or hit a knowledge whose postprocess
// stage row is missing (parse predates tracker). LookupStage returning
// nil must NOT crash the handler — the caller is expected to skip span
// recording and continue normal processing.
func TestPostprocessSubspan_MissingParentFallsThrough(t *testing.T) {
	tracker, _ := setupSpanTrackerTest(t)
	ctx := context.Background()

	// No OpenAttempt → no rows for kid. LookupStage must return nil.
	parent := tracker.LookupStage(ctx, "kid-without-attempt", 7, types.StagePostProcess)
	assert.Nil(t, parent, "missing parent attempt yields nil, not an error")

	// Open an attempt but never begin postprocess. Lookup must still nil.
	_, attempt, err := tracker.OpenAttempt(ctx, "kid-no-postprocess", "")
	require.NoError(t, err)
	parent = tracker.LookupStage(ctx, "kid-no-postprocess", attempt, types.StagePostProcess)
	assert.Nil(t, parent, "missing postprocess stage row yields nil")
}

// TestChunkExtractPayload_AttemptRoundTrip verifies the new fields
// added to ExtractChunkPayload survive JSON marshal/unmarshal so a
// cross-process asynq worker can recover the parent attempt + chunk
// ordinal on the receiving side. Skipping this would let a typo in
// the JSON tag silently zero the attempt and disable span recording.
func TestChunkExtractPayload_AttemptRoundTrip(t *testing.T) {
	in := types.ExtractChunkPayload{
		TenantID:    42,
		ChunkID:     "chunk-x",
		ModelID:     "m1",
		KnowledgeID: "kid-7",
		Attempt:     3,
		ChunkIndex:  9,
	}
	bytes, err := json.Marshal(in)
	require.NoError(t, err)

	var out types.ExtractChunkPayload
	require.NoError(t, json.Unmarshal(bytes, &out))

	assert.Equal(t, in.KnowledgeID, out.KnowledgeID)
	assert.Equal(t, in.Attempt, out.Attempt)
	assert.Equal(t, in.ChunkIndex, out.ChunkIndex)
}

// TestSummaryQuestionPayload_AttemptRoundTrip mirrors the above for the
// summary + question payloads to keep the contract documented.
func TestSummaryQuestionPayload_AttemptRoundTrip(t *testing.T) {
	sumIn := types.SummaryGenerationPayload{
		TenantID:        42,
		KnowledgeBaseID: "kb-1",
		KnowledgeID:     "kid-7",
		Language:        "zh-CN",
		Attempt:         5,
		Refresh:         true,
	}
	sumBytes, err := json.Marshal(sumIn)
	require.NoError(t, err)
	var sumOut types.SummaryGenerationPayload
	require.NoError(t, json.Unmarshal(sumBytes, &sumOut))
	assert.Equal(t, 5, sumOut.Attempt)
	assert.True(t, sumOut.Refresh)

	qIn := types.QuestionGenerationPayload{
		TenantID:        42,
		KnowledgeBaseID: "kb-1",
		KnowledgeID:     "kid-7",
		QuestionCount:   3,
		Language:        "zh-CN",
		Attempt:         5,
	}
	qBytes, err := json.Marshal(qIn)
	require.NoError(t, err)
	var qOut types.QuestionGenerationPayload
	require.NoError(t, json.Unmarshal(qBytes, &qOut))
	assert.Equal(t, 5, qOut.Attempt)
}
