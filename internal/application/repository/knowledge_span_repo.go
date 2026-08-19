package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// KnowledgeSpanRepository persists the per-attempt span tree used by the
// processing pipeline. Operations are deliberately narrow:
//
//   - Upsert covers Begin/End/Fail/Skip — every state transition routes
//     through the same write so the row stays internally consistent.
//   - OpenAttempt atomically allocates a new attempt without touching
//     terminal historical rows. Old attempts stay queryable for post-mortem.
//   - ListByAttempt is the only read path; the handler builds the tree
//     in memory rather than recursing through the DB.
type KnowledgeSpanRepository interface {
	Upsert(ctx context.Context, row *types.KnowledgeProcessingSpan) error
	// UpsertRunningStageIfCurrent starts or re-enters a canonical stage only
	// while the same attempt is still the latest open root. It serializes with
	// OpenAttempt so a late worker cannot resurrect a cancelled historical row.
	UpsertRunningStageIfCurrent(ctx context.Context, row *types.KnowledgeProcessingSpan) (bool, error)
	OpenAttempt(ctx context.Context, root *types.KnowledgeProcessingSpan) (int, error)
	UpdateInput(ctx context.Context, knowledgeID string, attempt int, spanID string, input types.JSONMap) error
	LatestAttempt(ctx context.Context, knowledgeID string) (int, error)
	ListByAttempt(ctx context.Context, knowledgeID string, attempt int) ([]types.KnowledgeProcessingSpan, error)
	GetSpan(ctx context.Context, knowledgeID string, attempt int, spanID string) (*types.KnowledgeProcessingSpan, error)
	InspectSpanRetryTarget(ctx context.Context, request types.KnowledgeSpanRetryRequest) (*types.KnowledgeSpanRetryTargetSnapshot, error)
	// CancelDescendants marks every descendant of a parent span as
	// "cancelled" with the given reason. Used by the tracker to
	// cascade an upstream failure across a stage's downstream subtree
	// without iterating in Go memory.
	CancelDescendants(ctx context.Context, knowledgeID string, attempt int, parentSpanID, reason string) (int64, error)
	// CancelAllOpenSpans flips every non-terminal (pending/running) span
	// for (knowledgeID, attempt) to "cancelled" in one statement,
	// regardless of tree position. Used by the user-cancel path where
	// fan-out stages (e.g. "多模态识别") flip themselves to done as soon
	// as they finish dispatching, while their async children are still
	// running — a tree walk that stops at terminal parents would miss
	// those orphan leaves.
	CancelAllOpenSpans(ctx context.Context, knowledgeID string, attempt int, errorCode, reason string) (int64, error)
	// CancelOpenSpansByName flips pending/running rows with the given span
	// name for (knowledgeID, attempt). Used before re-opening a subspan
	// after asynq retry or server restart so the trace tree does not
	// accumulate duplicate postprocess.summary / question rows.
	CancelOpenSpansByName(ctx context.Context, knowledgeID string, attempt int, name, errorCode, reason string) (int64, error)
	// ClaimLatestPendingByName atomically transitions the latest queued child
	// under the exact parent from pending to running. A nil result means the
	// delivery is a retry without a queued claim.
	ClaimLatestPendingByName(ctx context.Context, knowledgeID string, attempt int, parentSpanID, name string, input types.JSONMap) (*types.KnowledgeProcessingSpan, error)
	// QueuePendingSpan idempotently returns the existing queued logical child
	// or inserts row when no pending claim exists.
	QueuePendingSpan(ctx context.Context, row *types.KnowledgeProcessingSpan) (*types.KnowledgeProcessingSpan, error)
	// SettleProcessingOutcome is the sole outcome reducer for an attempt. It
	// locks the knowledge and span rows, chooses the latest retry for each
	// stable logical child identity, recalculates the observer counter, and
	// atomically settles question/postprocess/root/knowledge when possible.
	SettleProcessingOutcome(ctx context.Context, knowledgeID string, attempt int) error
	// SettleWikiPendingOp verifies that the exact Wiki owner is durably
	// terminal, reduces the attempt, optionally archives its dead letter, and
	// consumes the matching durable queue rows in one transaction. A failure at
	// any point rolls the parent/root/knowledge writes and queue acknowledgement
	// back together, so recovery never re-runs an already-completed Wiki LLM.
	SettleWikiPendingOp(
		ctx context.Context,
		knowledgeID string,
		attempt int,
		pendingIDs []int64,
		deadLetter *types.TaskDeadLetter,
		owner *types.TaskClaimOwner,
	) error
	// PrepareFailedSpanRetry atomically creates a partial-repair attempt for a
	// supported failed owner while preserving the source attempt as history.
	PrepareFailedSpanRetry(ctx context.Context, request types.KnowledgeSpanRetryRequest) (*types.KnowledgeSpanRetryPreparation, error)
	// PrepareFailedSpanRetries atomically creates one partial-repair attempt and
	// one deterministic dispatch preparation per exact internal target.
	PrepareFailedSpanRetries(ctx context.Context, request types.KnowledgeSpanMultiRetryRequest) ([]*types.KnowledgeSpanRetryPreparation, error)
	// FindExistingFailedSpanRetryPlan resolves and canonically validates a
	// previously committed repair for one exact client request before liveness
	// replanning the superseded source attempt.
	FindExistingFailedSpanRetryPlan(ctx context.Context, knowledgeID string, sourceAttempt int, clientRequestID, requestKind string) ([]*types.KnowledgeSpanRetryPreparation, error)
	// FailPreparedSpanRetry atomically consumes the matching durable outbox,
	// fails the exact pending owner, and settles its partial-repair attempt.
	FailPreparedSpanRetry(ctx context.Context, prepared *types.KnowledgeSpanRetryPreparation, errorCode, errorMessage string) error
}

var (
	ErrKnowledgeSpanRetryNotFound    = errors.New("knowledge span retry target not found")
	ErrKnowledgeSpanRetryNotLatest   = errors.New("knowledge span retry requires the latest attempt")
	ErrKnowledgeSpanRetryNotTerminal = errors.New("knowledge span retry requires a terminal attempt and failed span")
	ErrKnowledgeSpanRetryUnsupported = errors.New("knowledge span retry target is unsupported")
)

var graphRetryOwnerPattern = regexp.MustCompile(`^postprocess\.graph\.chunk\[([0-9]+)\]$`)
var questionRetryOwnerPattern = regexp.MustCompile(`^postprocess\.question\.batch\[([0-9]+)\]$`)

// terminalSpanUpdates freezes the elapsed duration in the same bulk UPDATE
// that closes a span. Historical rows written before this helper may still
// have duration_ms=0; the frontend derives those from started_at/finished_at.
func terminalSpanUpdates(db *gorm.DB, now time.Time, updates map[string]any) map[string]any {
	result := make(map[string]any, len(updates)+3)
	for key, value := range updates {
		result[key] = value
	}
	result["finished_at"] = now
	result["updated_at"] = now
	switch db.Dialector.Name() {
	case "postgres":
		result["duration_ms"] = gorm.Expr(
			"CASE WHEN started_at IS NULL THEN COALESCE(duration_ms, 0) ELSE GREATEST(1, CAST(EXTRACT(EPOCH FROM (? - started_at)) * 1000 AS BIGINT)) END",
			now,
		)
	case "sqlite":
		result["duration_ms"] = gorm.Expr(
			"CASE WHEN started_at IS NULL THEN COALESCE(duration_ms, 0) ELSE MAX(1, CAST((julianday(?) - julianday(started_at)) * 86400000 AS INTEGER)) END",
			now,
		)
	default:
		result["duration_ms"] = gorm.Expr("COALESCE(duration_ms, 0)")
	}
	return result
}

func (r *knowledgeSpanRepository) ClaimLatestPendingByName(
	ctx context.Context,
	knowledgeID string,
	attempt int,
	parentSpanID string,
	name string,
	input types.JSONMap,
) (*types.KnowledgeProcessingSpan, error) {
	if knowledgeID == "" || attempt <= 0 || parentSpanID == "" || name == "" {
		return nil, nil
	}
	var claimed *types.KnowledgeProcessingSpan
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row types.KnowledgeProcessingSpan
		query := tx.Where(
			"knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ? AND status = ?",
			knowledgeID, attempt, parentSpanID, name, types.SpanStatusPending,
		).Order("id DESC")
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}
		if err := query.Take(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		now := time.Now()
		mergedInput := make(types.JSONMap, len(row.Input)+len(input))
		for key, value := range row.Input {
			mergedInput[key] = value
		}
		for key, value := range input {
			mergedInput[key] = value
		}
		updates := map[string]any{
			"status": types.SpanStatusRunning, "started_at": now, "updated_at": now,
		}
		if len(mergedInput) > 0 {
			updates["input"] = mergedInput
		}
		result := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("id = ? AND status = ?", row.ID, types.SpanStatusPending).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		row.Status = types.SpanStatusRunning
		row.StartedAt = &now
		row.Input = mergedInput
		row.UpdatedAt = now
		claimed = &row
		return nil
	})
	return claimed, err
}

func (r *knowledgeSpanRepository) QueuePendingSpan(
	ctx context.Context, row *types.KnowledgeProcessingSpan,
) (*types.KnowledgeProcessingSpan, error) {
	if row == nil || row.KnowledgeID == "" || row.Attempt <= 0 ||
		row.ParentSpanID == "" || row.Name == "" || row.SpanID == "" {
		return nil, errors.New("knowledgeSpanRepository.QueuePendingSpan: complete pending row required")
	}
	var queued *types.KnowledgeProcessingSpan
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", row.KnowledgeID,
			).Error; err != nil {
				return err
			}
		}
		// Serialize the pending-child insert with cancellation/deletion. The
		// caller deliberately uses a detached context so terminal bookkeeping can
		// survive worker cancellation; without this database guard that same
		// detachment could insert a new pending child after AbortAttempt swept the
		// tree. Some isolated tracker tests have no knowledge row, so not-found
		// remains a legacy/test-compatible path; real knowledge rows must still be
		// finalizing.
		var knowledgeState struct {
			ParseStatus string `gorm:"column:parse_status"`
		}
		knowledgeQuery := tx.Table("knowledges").Select("parse_status").Where("id = ?", row.KnowledgeID)
		if tx.Dialector.Name() == "postgres" {
			knowledgeQuery = knowledgeQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		knowledgeResult := knowledgeQuery.Take(&knowledgeState)
		if knowledgeResult.Error != nil && !errors.Is(knowledgeResult.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("guard queued child knowledge state: %w", knowledgeResult.Error)
		}
		if knowledgeResult.Error == nil && knowledgeState.ParseStatus != types.ParseStatusFinalizing {
			return fmt.Errorf("guard queued child knowledge state: status=%s", knowledgeState.ParseStatus)
		}

		var latestAttempt int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", row.KnowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt), 0)").Row().Scan(&latestAttempt); err != nil {
			return fmt.Errorf("guard queued child latest attempt: %w", err)
		}
		if latestAttempt > 0 && latestAttempt != row.Attempt {
			return fmt.Errorf("guard queued child latest attempt: queued=%d latest=%d", row.Attempt, latestAttempt)
		}
		var parentStatus string
		parentResult := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
				row.KnowledgeID, row.Attempt, row.ParentSpanID).
			Pluck("status", &parentStatus)
		if parentResult.Error != nil {
			return fmt.Errorf("guard queued child parent state: %w", parentResult.Error)
		}
		if parentResult.RowsAffected > 0 && parentStatus != types.SpanStatusPending &&
			parentStatus != types.SpanStatusRunning {
			return fmt.Errorf("guard queued child parent state: status=%s", parentStatus)
		}
		var existing types.KnowledgeProcessingSpan
		result := tx.Where(
			"knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ?",
			row.KnowledgeID, row.Attempt, row.ParentSpanID, row.Name,
		).Order("id DESC").Take(&existing)
		if result.Error == nil {
			queued = &existing
			return nil
		}
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return result.Error
		}
		insert := *row
		insert.Status = types.SpanStatusPending
		if err := tx.Create(&insert).Error; err != nil {
			return err
		}
		queued = &insert
		return nil
	})
	return queued, err
}

type knowledgeSpanRepository struct {
	db        *gorm.DB
	attemptMu sync.Mutex
}

// NewKnowledgeSpanRepository wires the GORM-backed implementation.
func NewKnowledgeSpanRepository(db *gorm.DB) KnowledgeSpanRepository {
	return &knowledgeSpanRepository{db: db}
}

func (r *knowledgeSpanRepository) Upsert(ctx context.Context, row *types.KnowledgeProcessingSpan) error {
	_, err := r.upsertWithResult(ctx, row)
	return err
}

func (r *knowledgeSpanRepository) upsertWithResult(ctx context.Context, row *types.KnowledgeProcessingSpan) (int64, error) {
	return upsertKnowledgeSpanWithResult(r.db.WithContext(ctx), row)
}

func upsertKnowledgeSpanWithResult(db *gorm.DB, row *types.KnowledgeProcessingSpan) (int64, error) {
	if row == nil || row.KnowledgeID == "" || row.SpanID == "" {
		return 0, errors.New("knowledgeSpanRepository.Upsert: knowledge_id and span_id required")
	}
	if row.Attempt == 0 {
		row.Attempt = 1
	}
	// We let GORM populate created_at/updated_at via the autoCreate /
	// autoUpdate tags. ON CONFLICT updates only the fields that may
	// transition between calls — name/kind/parent are immutable once
	// set so we don't list them in DoUpdates (saves a few bytes per
	// write, and any mismatch indicates a programming error).
	//
	// CRITICAL: input / output / metadata are CONTENT fields that
	// individual call sites only fill when they have something to set.
	// EndSpan e.g. only sets `output`; if we always listed `input` in
	// DoUpdates, the End call would clobber the input set by Begin with
	// NULL. Same for metadata. Build the DoUpdates list dynamically and
	// skip these three columns when the incoming row has nothing to
	// write — so "no value" preserves the existing column instead of
	// nuking it.
	cols := []string{
		"status",
		"error_code",
		"error_message",
		"error_detail",
		"started_at",
		"finished_at",
		"duration_ms",
		"updated_at",
	}
	if row.Input != nil {
		cols = append(cols, "input")
	}
	if row.Output != nil {
		cols = append(cols, "output")
	}
	if row.Metadata != nil {
		cols = append(cols, "metadata")
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{Name: "knowledge_id"},
			{Name: "attempt"},
			{Name: "span_id"},
		},
		DoUpdates: clause.AssignmentColumns(cols),
	}
	// Preserve the first terminal outcome atomically. Late goroutines can still
	// report progress or completion after cancellation/failure; those writes
	// must not resurrect a cancelled generation or turn a failed branch into a
	// false success. Canonical stages are the sole exception: BeginStage
	// deliberately reuses a stage row when Asynq retries the same attempt, so a
	// running stage re-entry is allowed to reopen its previous terminal state.
	allowStageReentry := row.Kind == types.SpanKindStage && row.Status == types.SpanStatusRunning
	if !allowStageReentry {
		onConflict.Where = clause.Where{Exprs: []clause.Expression{
			clause.IN{
				Column: clause.Column{Table: clause.CurrentTable, Name: "status"},
				Values: []interface{}{types.SpanStatusPending, types.SpanStatusRunning},
			},
		}}
	}
	result := db.Clauses(onConflict).Create(row)
	return result.RowsAffected, result.Error
}

// UpsertRunningStageIfCurrent is the atomic stage-entry counterpart to
// OpenAttempt. Both operations take the same PostgreSQL advisory lock, so a
// newer attempt cannot be opened between the latest-root check and the stage
// upsert. SQLite's transaction writer lock provides the equivalent test/runtime
// serialization.
func (r *knowledgeSpanRepository) UpsertRunningStageIfCurrent(
	ctx context.Context,
	row *types.KnowledgeProcessingSpan,
) (bool, error) {
	if row == nil || row.KnowledgeID == "" || row.SpanID == "" || row.Attempt <= 0 ||
		row.Kind != types.SpanKindStage || row.Status != types.SpanStatusRunning {
		return false, errors.New("knowledgeSpanRepository.UpsertRunningStageIfCurrent: valid running stage required")
	}

	accepted := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", row.KnowledgeID,
			).Error; err != nil {
				return fmt.Errorf("serialize running knowledge stage: %w", err)
			}
		}

		var root types.KnowledgeProcessingSpan
		query := tx.Where("knowledge_id = ? AND kind = ?", row.KnowledgeID, types.SpanKindRoot).
			Order("attempt DESC")
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		result := query.Take(&root)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil
		}
		if result.Error != nil {
			return fmt.Errorf("load latest knowledge attempt for running stage: %w", result.Error)
		}
		if root.Attempt != row.Attempt ||
			(root.Status != types.SpanStatusPending && root.Status != types.SpanStatusRunning) {
			return nil
		}

		rows, err := upsertKnowledgeSpanWithResult(tx, row)
		if err != nil {
			return err
		}
		accepted = rows > 0
		return nil
	})
	return accepted, err
}

// OpenAttempt atomically allocates the next attempt, supersedes every older
// open span, and inserts the new root. PostgreSQL serializes the transaction
// per knowledge so concurrent reparses cannot allocate the same attempt. The
// SQLite branch relies on its transaction writer lock and stays usable by the
// lightweight test/runtime backend.
func (r *knowledgeSpanRepository) OpenAttempt(
	ctx context.Context, root *types.KnowledgeProcessingSpan,
) (int, error) {
	if root == nil || root.KnowledgeID == "" || root.SpanID == "" || root.Kind != types.SpanKindRoot {
		return 0, errors.New("knowledgeSpanRepository.OpenAttempt: valid root span required")
	}

	if r.db.Dialector.Name() == "sqlite" {
		r.attemptMu.Lock()
		defer r.attemptMu.Unlock()
	}

	var attempt int
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", root.KnowledgeID,
			).Error; err != nil {
				return fmt.Errorf("serialize knowledge attempt: %w", err)
			}
		}

		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ?", root.KnowledgeID).
			Select("COALESCE(MAX(attempt), 0)").
			Row().Scan(&attempt); err != nil {
			return fmt.Errorf("allocate knowledge attempt: %w", err)
		}
		attempt++

		now := time.Now()
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt < ? AND status IN ?",
				root.KnowledgeID, attempt,
				[]string{types.SpanStatusPending, types.SpanStatusRunning}).
			Updates(terminalSpanUpdates(tx, now, map[string]any{
				"status":        types.SpanStatusCancelled,
				"error_code":    "ATTEMPT_SUPERSEDED",
				"error_message": "superseded by a newer processing attempt",
			})).Error; err != nil {
			return fmt.Errorf("supersede previous knowledge attempt: %w", err)
		}

		insert := *root
		insert.Attempt = attempt
		if err := tx.Create(&insert).Error; err != nil {
			return fmt.Errorf("insert knowledge attempt root: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	root.Attempt = attempt
	return attempt, nil
}

// WithAttemptCommitGuard serializes a worker's external durable mutation with
// OpenAttempt. PostgreSQL holds the same per-knowledge advisory transaction
// lock used by OpenAttempt; SQLite uses the repository mutex because its
// external chunk/vector/graph writes are not part of this transaction.
// The callback runs only while attempt is still the latest root and while the
// serialization guard remains held.
func (r *knowledgeSpanRepository) WithAttemptCommitGuard(
	ctx context.Context,
	knowledgeID string,
	attempt int,
	fn func(context.Context) error,
) error {
	if knowledgeID == "" || attempt <= 0 || fn == nil {
		return errors.New("knowledgeSpanRepository.WithAttemptCommitGuard: knowledge id, attempt and callback required")
	}
	if r.db.Dialector.Name() == "sqlite" {
		r.attemptMu.Lock()
		defer r.attemptMu.Unlock()
		var latestAttempt int
		if err := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", knowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt), 0)").
			Row().Scan(&latestAttempt); err != nil {
			return fmt.Errorf("guard knowledge attempt commit: %w", err)
		}
		if latestAttempt != attempt {
			return fmt.Errorf("guard knowledge attempt commit: attempt %d is superseded by %d", attempt, latestAttempt)
		}
		return fn(ctx)
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", knowledgeID,
			).Error; err != nil {
				return fmt.Errorf("serialize knowledge attempt commit: %w", err)
			}
		}

		var latestAttempt int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", knowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt), 0)").
			Row().Scan(&latestAttempt); err != nil {
			return fmt.Errorf("guard knowledge attempt commit: %w", err)
		}
		if latestAttempt != attempt {
			return fmt.Errorf("guard knowledge attempt commit: attempt %d is superseded by %d", attempt, latestAttempt)
		}
		if err := fn(withGuardedDB(ctx, tx)); err != nil {
			return err
		}
		return nil
	})
}

func (r *knowledgeSpanRepository) UpdateInput(
	ctx context.Context,
	knowledgeID string,
	attempt int,
	spanID string,
	input types.JSONMap,
) error {
	if knowledgeID == "" || attempt <= 0 || spanID == "" {
		return errors.New("knowledgeSpanRepository.UpdateInput: knowledge_id, attempt and span_id required")
	}
	result := r.db.WithContext(ctx).
		Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", knowledgeID, attempt, spanID).
		Update("input", input)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *knowledgeSpanRepository) LatestAttempt(ctx context.Context, knowledgeID string) (int, error) {
	var max int
	err := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ?", knowledgeID).
		Select("COALESCE(MAX(attempt), 0)").
		Row().Scan(&max)
	return max, err
}

func (r *knowledgeSpanRepository) ListByAttempt(ctx context.Context, knowledgeID string, attempt int) ([]types.KnowledgeProcessingSpan, error) {
	if knowledgeID == "" {
		return nil, nil
	}
	var rows []types.KnowledgeProcessingSpan
	q := r.db.WithContext(ctx).Where("knowledge_id = ?", knowledgeID)
	if attempt > 0 {
		q = q.Where("attempt = ?", attempt)
	}
	// id ASC keeps the natural insertion order — useful for stable
	// rendering of fan-out subspans (e.g. multimodal.image[0..N] in
	// the order they were enqueued).
	err := q.Order("id ASC").Find(&rows).Error
	return rows, err
}

func (r *knowledgeSpanRepository) GetSpan(ctx context.Context, knowledgeID string, attempt int, spanID string) (*types.KnowledgeProcessingSpan, error) {
	var row types.KnowledgeProcessingSpan
	err := r.db.WithContext(ctx).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ?", knowledgeID, attempt, spanID).
		Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

func (r *knowledgeSpanRepository) InspectSpanRetryTarget(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryTargetSnapshot, error) {
	if request.KnowledgeID == "" || request.Attempt <= 0 || request.SpanID == "" {
		return nil, ErrKnowledgeSpanRetryNotFound
	}
	var snapshot *types.KnowledgeSpanRetryTargetSnapshot
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var source types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			request.KnowledgeID, request.Attempt, request.SpanID).Take(&source).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrKnowledgeSpanRetryNotFound
			}
			return err
		}
		var parent types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
			request.KnowledgeID, request.Attempt, source.ParentSpanID).Take(&parent).Error; err != nil {
			return err
		}
		logicalParent := parent
		if questionRetryOwnerPattern.MatchString(source.Name) {
			if parent.Kind != types.SpanKindSubSpan || parent.Name != "postprocess.question" {
				return ErrKnowledgeSpanRetryUnsupported
			}
			var latestQuestionParent types.KnowledgeProcessingSpan
			if err := tx.Where("knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ?",
				request.KnowledgeID, request.Attempt, parent.ParentSpanID, parent.Name).
				Order("id DESC").Take(&latestQuestionParent).Error; err != nil {
				return err
			}
			if latestQuestionParent.ID != parent.ID {
				return ErrKnowledgeSpanRetryNotLatest
			}
			logicalParent = types.KnowledgeProcessingSpan{}
			if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
				request.KnowledgeID, request.Attempt, parent.ParentSpanID).Take(&logicalParent).Error; err != nil {
				return ErrKnowledgeSpanRetryUnsupported
			}
			if logicalParent.Kind != types.SpanKindStage || logicalParent.Name != types.StagePostProcess {
				return ErrKnowledgeSpanRetryUnsupported
			}
		}
		var latestRoot types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND kind = ?", request.KnowledgeID, types.SpanKindRoot).
			Order("attempt DESC").Take(&latestRoot).Error; err != nil {
			return err
		}
		var latestOwner types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ?",
			request.KnowledgeID, request.Attempt, source.ParentSpanID, source.Name).
			Order("id DESC").Take(&latestOwner).Error; err != nil {
			return err
		}
		var knowledge struct {
			TenantID        uint64 `gorm:"column:tenant_id"`
			KnowledgeBaseID string `gorm:"column:knowledge_base_id"`
			ParseStatus     string `gorm:"column:parse_status"`
		}
		if err := tx.Table("knowledges").Select("tenant_id", "knowledge_base_id", "parse_status").
			Where("id = ?", request.KnowledgeID).Take(&knowledge).Error; err != nil {
			return err
		}
		existingRetry := false
		if strings.TrimSpace(request.ClientRequestID) != "" {
			_, _, rootID := canonicalSingletonRetryPlan(request)
			var count int64
			if err := tx.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND kind = ? AND span_id = ?", request.KnowledgeID,
					types.SpanKindRoot, rootID).
				Count(&count).Error; err != nil {
				return err
			}
			existingRetry = count > 0
		}
		snapshot = &types.KnowledgeSpanRetryTargetSnapshot{
			Source: source, Parent: logicalParent, LatestRoot: latestRoot,
			LatestOwnerSpanID: latestOwner.SpanID, TenantID: knowledge.TenantID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID, KnowledgeStatus: knowledge.ParseStatus,
			ExistingRetry: existingRetry,
		}
		return nil
	})
	return snapshot, err
}

func isTerminalSpanStatus(status string) bool {
	switch status {
	case types.SpanStatusDone, types.SpanStatusFailed, types.SpanStatusSkipped, types.SpanStatusCancelled:
		return true
	default:
		return false
	}
}

func retryOwnerNameSupported(row *types.KnowledgeProcessingSpan) bool {
	if row == nil || row.Kind != types.SpanKindSubSpan {
		return false
	}
	return row.Name == "postprocess.summary" || row.Name == "postprocess.wiki" ||
		graphRetryOwnerPattern.MatchString(row.Name)
}

func internalRetryOwnerNameSupported(row *types.KnowledgeProcessingSpan) bool {
	return retryOwnerNameSupported(row) || (row != nil && row.Kind == types.SpanKindSubSpan &&
		questionRetryOwnerPattern.MatchString(row.Name))
}

func retryOwnerSupported(row *types.KnowledgeProcessingSpan) bool {
	return retryOwnerNameSupported(row) && row.Status == types.SpanStatusFailed
}

func retryInputString(input types.JSONMap, key string) (string, bool) {
	if input == nil {
		return "", false
	}
	value, ok := input[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	text = strings.TrimSpace(text)
	return text, ok && text != ""
}

func retryInputInt(input types.JSONMap, key string) (int, bool) {
	if input == nil {
		return 0, false
	}
	switch value := input[key].(type) {
	case int:
		return value, true
	case int32:
		return int(value), true
	case int64:
		return int(value), true
	case float64:
		return int(value), value == float64(int(value))
	case json.Number:
		parsed, err := value.Int64()
		return int(parsed), err == nil
	default:
		return 0, false
	}
}

func retryInputStrings(input types.JSONMap, key string) ([]string, bool) {
	if input == nil {
		return nil, false
	}
	var result []string
	switch values := input[key].(type) {
	case []string:
		result = append(result, values...)
	case []any:
		for _, value := range values {
			text, ok := value.(string)
			text = strings.TrimSpace(text)
			if !ok || text == "" {
				return nil, false
			}
			result = append(result, text)
		}
	default:
		return nil, false
	}
	return result, len(result) > 0
}

func partialRepairTaskID(knowledgeID string, attempt int, name string, input types.JSONMap) (string, error) {
	switch {
	case name == "postprocess.summary":
		return fmt.Sprintf("knowledge-fanout:%s:%d:summary", knowledgeID, attempt), nil
	case name == "postprocess.wiki":
		return fmt.Sprintf("knowledge-fanout:%s:%d:wiki", knowledgeID, attempt), nil
	case graphRetryOwnerPattern.MatchString(name):
		index, ok := retryInputInt(input, "chunk_index")
		if !ok {
			return "", ErrKnowledgeSpanRetryUnsupported
		}
		return fmt.Sprintf("knowledge-fanout:%s:%d:graph:%d", knowledgeID, attempt, index), nil
	case questionRetryOwnerPattern.MatchString(name):
		index, ok := retryInputInt(input, "batch_index")
		if !ok {
			return "", ErrKnowledgeSpanRetryUnsupported
		}
		return fmt.Sprintf("knowledge-fanout:%s:%d:question:%d", knowledgeID, attempt, index), nil
	default:
		return "", ErrKnowledgeSpanRetryUnsupported
	}
}

func partialRepairQueue(name string) (string, error) {
	switch {
	case name == "postprocess.summary":
		return types.QueueSummary, nil
	case name == "postprocess.wiki":
		return types.QueueWiki, nil
	case graphRetryOwnerPattern.MatchString(name):
		return types.QueueGraph, nil
	case questionRetryOwnerPattern.MatchString(name):
		return types.QueueQuestion, nil
	default:
		return "", ErrKnowledgeSpanRetryUnsupported
	}
}

func multiRepairSpanID(request types.KnowledgeSpanMultiRetryRequest, role string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%s", request.KnowledgeID,
		request.Attempt, request.ClientRequestID, role)))
	return "repair-" + hex.EncodeToString(sum[:20])
}

func multiRepairRootSpanID(request types.KnowledgeSpanMultiRetryRequest, planDigest string) string {
	return multiRepairSpanID(request, "root:"+planDigest)
}

func canonicalSingletonRetryPlan(
	request types.KnowledgeSpanRetryRequest,
) (types.KnowledgeSpanMultiRetryRequest, string, string) {
	multi := types.KnowledgeSpanMultiRetryRequest{
		KnowledgeID:     request.KnowledgeID,
		Attempt:         request.Attempt,
		ClientRequestID: request.ClientRequestID,
		Language:        request.Language,
		RequestKind:     "row",
		Targets: []types.KnowledgeSpanMultiRetryTarget{{
			SpanID:     request.SpanID,
			StallFence: request.StallFence,
		}},
	}
	planDigest := multiRepairPlanDigest(multi.Targets)
	return multi, planDigest, multiRepairRootSpanID(multi, planDigest)
}

func multiRepairRequestKind(request types.KnowledgeSpanMultiRetryRequest) string {
	kind := strings.TrimSpace(request.RequestKind)
	if kind == "" {
		return "internal"
	}
	return kind
}

func multiRepairPlanDigest(targets []types.KnowledgeSpanMultiRetryTarget) string {
	spanIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		spanIDs = append(spanIDs, target.SpanID)
	}
	sort.Strings(spanIDs)
	sum := sha256.Sum256([]byte(strings.Join(spanIDs, "\x00")))
	return hex.EncodeToString(sum[:])
}

func (r *knowledgeSpanRepository) FindExistingFailedSpanRetryPlan(
	ctx context.Context, knowledgeID string, sourceAttempt int, clientRequestID, requestKind string,
) ([]*types.KnowledgeSpanRetryPreparation, error) {
	if strings.TrimSpace(knowledgeID) == "" || sourceAttempt <= 0 || strings.TrimSpace(clientRequestID) == "" {
		return nil, nil
	}
	var roots []types.KnowledgeProcessingSpan
	if err := r.db.WithContext(ctx).Where("knowledge_id = ? AND kind = ? AND attempt > ?",
		knowledgeID, types.SpanKindRoot, sourceAttempt).Order("attempt ASC").Find(&roots).Error; err != nil {
		return nil, err
	}
	var matched *types.KnowledgeProcessingSpan
	for i := range roots {
		root := &roots[i]
		storedAttempt, attemptOK := retryInputInt(root.Input, "source_attempt")
		storedClient, clientOK := retryInputString(root.Input, "client_request_id")
		if !attemptOK || !clientOK || storedAttempt != sourceAttempt || storedClient != clientRequestID ||
			fmt.Sprint(root.Input["attempt_kind"]) != "partial_repair" {
			continue
		}
		if matched != nil {
			return nil, errors.New("multiple canonical retry plans match one client request")
		}
		matched = root
	}
	if matched == nil {
		return nil, nil
	}
	if fmt.Sprint(matched.Input["request_kind"]) != strings.TrimSpace(requestKind) {
		return nil, ErrKnowledgeSpanRetryNotLatest
	}
	sourceIDs, ok := retryInputStrings(matched.Input, "source_span_ids")
	if !ok || len(sourceIDs) == 0 {
		return nil, errors.New("canonical retry plan source ids are invalid")
	}
	targets := make([]types.KnowledgeSpanMultiRetryTarget, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		spanID := strings.TrimSpace(sourceID)
		if spanID == "" {
			return nil, errors.New("canonical retry plan source id is invalid")
		}
		targets = append(targets, types.KnowledgeSpanMultiRetryTarget{SpanID: spanID})
	}
	request := types.KnowledgeSpanMultiRetryRequest{KnowledgeID: knowledgeID, Attempt: sourceAttempt,
		ClientRequestID: clientRequestID, RequestKind: requestKind, Targets: targets}
	planDigest := multiRepairPlanDigest(targets)
	if fmt.Sprint(matched.Input["plan_digest"]) != planDigest || matched.SpanID != multiRepairRootSpanID(request, planDigest) {
		return nil, errors.New("canonical retry plan identity mismatch")
	}
	return r.PrepareFailedSpanRetries(ctx, request)
}

func retryTargetOrder(name string) (int, int, string) {
	switch {
	case name == "postprocess.summary":
		return 0, 0, name
	case name == "postprocess.question":
		return 1, 0, name
	case questionRetryOwnerPattern.MatchString(name):
		index, _ := strconv.Atoi(questionRetryOwnerPattern.FindStringSubmatch(name)[1])
		return 1, index, name
	case name == "postprocess.wiki":
		return 2, 0, name
	case graphRetryOwnerPattern.MatchString(name):
		index, _ := strconv.Atoi(graphRetryOwnerPattern.FindStringSubmatch(name)[1])
		return 3, index, name
	default:
		return 4, 0, name
	}
}

func sortRetrySpans(rows []types.KnowledgeProcessingSpan) {
	sort.Slice(rows, func(i, j int) bool {
		iGroup, iIndex, iName := retryTargetOrder(rows[i].Name)
		jGroup, jIndex, jName := retryTargetOrder(rows[j].Name)
		if iGroup != jGroup {
			return iGroup < jGroup
		}
		if iIndex != jIndex {
			return iIndex < jIndex
		}
		return iName < jName
	})
}

const retryDispatchMetadataKey = "retry_dispatch"

func persistedRetryDispatch(target *types.KnowledgeProcessingSpan) (*types.KnowledgeSpanRetryOutboxPayload, error) {
	if target == nil || target.Metadata == nil {
		return nil, errors.New("load idempotent repair target: persisted dispatch metadata is missing")
	}
	raw, ok := target.Metadata[retryDispatchMetadataKey]
	if !ok {
		return nil, errors.New("load idempotent repair target: persisted dispatch metadata is missing")
	}
	payloadBytes, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("load idempotent repair target dispatch: %w", err)
	}
	var payload types.KnowledgeSpanRetryOutboxPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("load idempotent repair target dispatch: %w", err)
	}
	if payload.TaskID == "" || payload.KnowledgeID != target.KnowledgeID ||
		payload.Attempt != target.Attempt || payload.SpanID != target.SpanID ||
		payload.TargetName != target.Name || payload.TenantID == 0 ||
		payload.KnowledgeBaseID == "" || payload.Input == nil {
		return nil, errors.New("load idempotent repair target: persisted dispatch metadata is inconsistent")
	}
	return &payload, nil
}

func sameRetryDispatch(left, right *types.KnowledgeSpanRetryOutboxPayload) bool {
	if left == nil || right == nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

// PrepareFailedSpanRetries creates exactly one canonical attempt for an
// internal exact target set, including a singleton question batch.
func (r *knowledgeSpanRepository) PrepareFailedSpanRetries(
	ctx context.Context, request types.KnowledgeSpanMultiRetryRequest,
) ([]*types.KnowledgeSpanRetryPreparation, error) {
	if request.KnowledgeID == "" || request.Attempt <= 0 ||
		strings.TrimSpace(request.ClientRequestID) == "" || len(request.Targets) == 0 {
		return nil, ErrKnowledgeSpanRetryNotFound
	}
	if r.db.Dialector.Name() == "sqlite" {
		r.attemptMu.Lock()
		defer r.attemptMu.Unlock()
	}
	uniqueTargets := make([]types.KnowledgeSpanMultiRetryTarget, 0, len(request.Targets))
	seenSpanIDs := make(map[string]struct{}, len(request.Targets))
	for _, target := range request.Targets {
		target.SpanID = strings.TrimSpace(target.SpanID)
		if target.SpanID == "" {
			return nil, ErrKnowledgeSpanRetryNotFound
		}
		if _, duplicate := seenSpanIDs[target.SpanID]; duplicate {
			continue
		}
		seenSpanIDs[target.SpanID] = struct{}{}
		uniqueTargets = append(uniqueTargets, target)
	}
	sort.Slice(uniqueTargets, func(i, j int) bool {
		return uniqueTargets[i].SpanID < uniqueTargets[j].SpanID
	})
	carryoverFences := make([]*types.KnowledgeSpanRetryStallFence, 0, len(request.CarryoverFences))
	seenCarryoverIDs := make(map[string]struct{}, len(request.CarryoverFences))
	for _, fence := range request.CarryoverFences {
		if fence == nil || strings.TrimSpace(fence.SourceSpanID) == "" {
			return nil, ErrKnowledgeSpanRetryNotFound
		}
		if _, executes := seenSpanIDs[fence.SourceSpanID]; executes {
			return nil, ErrKnowledgeSpanRetryNotLatest
		}
		if _, duplicate := seenCarryoverIDs[fence.SourceSpanID]; duplicate {
			continue
		}
		seenCarryoverIDs[fence.SourceSpanID] = struct{}{}
		carryoverFences = append(carryoverFences, fence)
	}
	sort.Slice(carryoverFences, func(i, j int) bool {
		return carryoverFences[i].SourceSpanID < carryoverFences[j].SourceSpanID
	})
	planDigest := multiRepairPlanDigest(uniqueTargets)

	rootID := multiRepairRootSpanID(request, planDigest)
	postID := multiRepairSpanID(request, "postprocess")
	var preparations []*types.KnowledgeSpanRetryPreparation
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", request.KnowledgeID).Error; err != nil {
				return fmt.Errorf("serialize failed span retries: %w", err)
			}
		}

		var existingRoot types.KnowledgeProcessingSpan
		existing := tx.Where("knowledge_id = ? AND kind = ? AND span_id = ?",
			request.KnowledgeID, types.SpanKindRoot, rootID).Take(&existingRoot)
		if existing.Error == nil {
			if fmt.Sprint(existingRoot.Input["plan_digest"]) != planDigest {
				return ErrKnowledgeSpanRetryNotLatest
			}
			if fmt.Sprint(existingRoot.Input["request_kind"]) != multiRepairRequestKind(request) {
				return ErrKnowledgeSpanRetryNotLatest
			}
			var targets []types.KnowledgeProcessingSpan
			if err := tx.Where("knowledge_id = ? AND attempt = ? AND kind = ?",
				request.KnowledgeID, existingRoot.Attempt, types.SpanKindSubSpan).Find(&targets).Error; err != nil {
				return fmt.Errorf("load idempotent repair targets: %w", err)
			}
			selected := make([]types.KnowledgeProcessingSpan, 0, len(targets))
			for _, target := range targets {
				value, _ := target.Input["retry_target"].(bool)
				clientRequestID, _ := target.Input["client_request_id"].(string)
				if value && clientRequestID == request.ClientRequestID {
					selected = append(selected, target)
				}
			}
			if len(selected) != len(uniqueTargets) {
				return fmt.Errorf("load idempotent repair targets: expected %d, found %d", len(uniqueTargets), len(selected))
			}
			sortRetrySpans(selected)
			preparations = make([]*types.KnowledgeSpanRetryPreparation, 0, len(selected))
			for i := range selected {
				target := &selected[i]
				canonical, err := persistedRetryDispatch(target)
				if err != nil {
					return err
				}
				var persistedOutbox types.TaskPendingOp
				outboxPresent := false
				if target.Status == types.SpanStatusPending {
					result := tx.Where(
						"task_type = ? AND scope = ? AND scope_id = ? AND dedup_key = ?",
						types.KnowledgeSpanRetryOutboxTaskType, types.KnowledgeSpanRetryOutboxScope,
						canonical.KnowledgeID, canonical.TaskID).Take(&persistedOutbox)
					if result.Error == nil {
						outboxPresent = true
						var outboxPayload types.KnowledgeSpanRetryOutboxPayload
						if err := json.Unmarshal(persistedOutbox.Payload, &outboxPayload); err != nil {
							return fmt.Errorf("load idempotent repair outbox: %w", err)
						}
						if !sameRetryDispatch(canonical, &outboxPayload) {
							return errors.New("load idempotent repair outbox: canonical payload mismatch")
						}
						canonical = &outboxPayload
					} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
						return result.Error
					}
				}
				sourceAttempt, ok := retryInputInt(target.Input, "source_attempt")
				if !ok {
					return errors.New("load idempotent repair target: source attempt is missing")
				}
				sourceSpanID, ok := retryInputString(target.Input, "source_span_id")
				if !ok {
					return errors.New("load idempotent repair target: source span is missing")
				}
				clientRequestID, ok := retryInputString(target.Input, "client_request_id")
				if !ok {
					return errors.New("load idempotent repair target: client request id is missing")
				}
				preparations = append(preparations, &types.KnowledgeSpanRetryPreparation{
					KnowledgeID: canonical.KnowledgeID, SourceAttempt: sourceAttempt,
					SourceSpanID: sourceSpanID, ClientRequestID: clientRequestID,
					Attempt: canonical.Attempt, SpanID: canonical.SpanID, Name: canonical.TargetName, TaskID: canonical.TaskID,
					Status: target.Status, ErrorCode: target.ErrorCode, ErrorMessage: target.ErrorMessage,
					DispatchRequired: outboxPresent, TenantID: canonical.TenantID,
					KnowledgeBaseID: canonical.KnowledgeBaseID, Language: canonical.Language, Input: canonical.Input,
				})
			}
			return nil
		}
		if !errors.Is(existing.Error, gorm.ErrRecordNotFound) {
			return existing.Error
		}

		var latestRoot types.KnowledgeProcessingSpan
		latestQuery := tx.Where("knowledge_id = ? AND kind = ?", request.KnowledgeID, types.SpanKindRoot).Order("attempt DESC")
		if tx.Dialector.Name() == "postgres" {
			latestQuery = latestQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := latestQuery.Take(&latestRoot).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrKnowledgeSpanRetryNotFound
			}
			return err
		}
		if latestRoot.Attempt != request.Attempt {
			return ErrKnowledgeSpanRetryNotLatest
		}

		var allSources []types.KnowledgeProcessingSpan
		spanIDs := make([]string, 0, len(uniqueTargets)+len(carryoverFences))
		fences := make(map[string]*types.KnowledgeSpanRetryStallFence, len(uniqueTargets)+len(carryoverFences))
		for _, target := range uniqueTargets {
			spanIDs = append(spanIDs, target.SpanID)
			fences[target.SpanID] = target.StallFence
		}
		for _, fence := range carryoverFences {
			spanIDs = append(spanIDs, fence.SourceSpanID)
			fences[fence.SourceSpanID] = fence
		}
		query := tx.Where("knowledge_id = ? AND attempt = ? AND span_id IN ?", request.KnowledgeID, request.Attempt, spanIDs)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.Find(&allSources).Error; err != nil {
			return err
		}
		if len(allSources) != len(uniqueTargets)+len(carryoverFences) {
			return ErrKnowledgeSpanRetryNotFound
		}
		sources := make([]types.KnowledgeProcessingSpan, 0, len(uniqueTargets))
		for _, source := range allSources {
			if _, executes := seenSpanIDs[source.SpanID]; executes {
				sources = append(sources, source)
			}
		}
		var knowledge struct {
			TenantID        uint64 `gorm:"column:tenant_id"`
			KnowledgeBaseID string `gorm:"column:knowledge_base_id"`
			ParseStatus     string `gorm:"column:parse_status"`
		}
		knowledgeQuery := tx.Table("knowledges").Select("tenant_id", "knowledge_base_id", "parse_status").
			Where("id = ?", request.KnowledgeID)
		if tx.Dialector.Name() == "postgres" {
			knowledgeQuery = knowledgeQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := knowledgeQuery.Take(&knowledge).Error; err != nil {
			return err
		}
		if knowledge.ParseStatus == types.ParseStatusDeleting || knowledge.ParseStatus == types.ParseStatusCancelled {
			return ErrKnowledgeSpanRetryNotTerminal
		}

		var post types.KnowledgeProcessingSpan
		postLoaded := false
		targetNames := make(map[string]string, len(sources))
		var stalledWikiFence *types.KnowledgeSpanRetryStallFence
		hasStalledSource := false
		for i := range allSources {
			source := &allSources[i]
			if !internalRetryOwnerNameSupported(source) {
				return ErrKnowledgeSpanRetryUnsupported
			}
			if _, executes := seenSpanIDs[source.SpanID]; executes {
				if previousSpanID, duplicate := targetNames[source.Name]; duplicate && previousSpanID != source.SpanID {
					return ErrKnowledgeSpanRetryNotLatest
				}
				targetNames[source.Name] = source.SpanID
			}

			var parent types.KnowledgeProcessingSpan
			if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
				request.KnowledgeID, request.Attempt, source.ParentSpanID).Take(&parent).Error; err != nil {
				return ErrKnowledgeSpanRetryUnsupported
			}
			logicalParent := parent
			if questionRetryOwnerPattern.MatchString(source.Name) {
				if parent.Kind != types.SpanKindSubSpan || parent.Name != "postprocess.question" {
					return ErrKnowledgeSpanRetryUnsupported
				}
				var latestQuestionParent types.KnowledgeProcessingSpan
				if err := tx.Where("knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ?",
					request.KnowledgeID, request.Attempt, parent.ParentSpanID, parent.Name).
					Order("id DESC").Take(&latestQuestionParent).Error; err != nil {
					return err
				}
				if latestQuestionParent.ID != parent.ID {
					return ErrKnowledgeSpanRetryNotLatest
				}
				logicalParent = types.KnowledgeProcessingSpan{}
				if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?",
					request.KnowledgeID, request.Attempt, parent.ParentSpanID).Take(&logicalParent).Error; err != nil {
					return ErrKnowledgeSpanRetryUnsupported
				}
			}
			if logicalParent.Kind != types.SpanKindStage || logicalParent.Name != types.StagePostProcess {
				return ErrKnowledgeSpanRetryUnsupported
			}
			if !postLoaded {
				post, postLoaded = logicalParent, true
			} else if post.ID != logicalParent.ID {
				return ErrKnowledgeSpanRetryUnsupported
			}

			var latestOwner types.KnowledgeProcessingSpan
			if err := tx.Where("knowledge_id = ? AND attempt = ? AND parent_span_id = ? AND name = ?",
				request.KnowledgeID, request.Attempt, source.ParentSpanID, source.Name).
				Order("id DESC").Take(&latestOwner).Error; err != nil {
				return err
			}
			if latestOwner.ID != source.ID {
				return ErrKnowledgeSpanRetryNotLatest
			}
			fence := fences[source.SpanID]
			if fence == nil {
				if source.Status != types.SpanStatusFailed {
					return ErrKnowledgeSpanRetryNotTerminal
				}
			} else {
				if source.Status != types.SpanStatusPending && source.Status != types.SpanStatusRunning {
					return ErrKnowledgeSpanRetryNotTerminal
				}
				if fence.KnowledgeID != request.KnowledgeID || fence.TenantID != knowledge.TenantID ||
					fence.OwnerName != source.Name || fence.SourceAttempt != request.Attempt ||
					fence.SourceSpanID != source.SpanID || fence.LatestRootAttempt != latestRoot.Attempt ||
					fence.SourceUpdatedAt.IsZero() || !source.UpdatedAt.Equal(fence.SourceUpdatedAt) ||
					fence.LastHeartbeatAt.IsZero() || fence.TaskID == "" || fence.Queue == "" {
					return ErrKnowledgeSpanRetryNotTerminal
				}
				expectedQueue, queueErr := partialRepairQueue(source.Name)
				if queueErr != nil || fence.Queue != expectedQueue {
					return ErrKnowledgeSpanRetryNotTerminal
				}
				hasStalledSource = true
				if source.Name == "postprocess.wiki" {
					if len(fence.PendingOpIDs) == 0 || fence.ClaimToken == "" || fence.ClaimedByTaskID == "" ||
						fence.ClaimHeartbeatAt.IsZero() || !fence.LastHeartbeatAt.Equal(fence.ClaimHeartbeatAt) ||
						fence.TaskID != fence.ClaimedByTaskID || fence.Queue != types.QueueWiki {
						return ErrKnowledgeSpanRetryNotTerminal
					}
					stalledWikiFence = fence
				} else {
					if !fence.LastHeartbeatAt.Equal(source.UpdatedAt) {
						return ErrKnowledgeSpanRetryNotTerminal
					}
					expectedTaskID, taskErr := partialRepairTaskID(
						request.KnowledgeID, request.Attempt, source.Name, source.Input)
					if taskErr != nil || fence.TaskID != expectedTaskID {
						return ErrKnowledgeSpanRetryNotTerminal
					}
				}
			}
			_, executes := seenSpanIDs[source.SpanID]
			if executes && graphRetryOwnerPattern.MatchString(source.Name) {
				if _, ok := retryInputString(source.Input, "chunk_id"); !ok {
					return ErrKnowledgeSpanRetryUnsupported
				}
				if _, ok := retryInputString(source.Input, "model_id"); !ok {
					return ErrKnowledgeSpanRetryUnsupported
				}
			}
			pattern := graphRetryOwnerPattern
			inputKey := "chunk_index"
			if executes && questionRetryOwnerPattern.MatchString(source.Name) {
				pattern, inputKey = questionRetryOwnerPattern, "batch_index"
				if _, ok := retryInputStrings(source.Input, "chunk_ids"); !ok {
					return ErrKnowledgeSpanRetryUnsupported
				}
				questionCount, ok := retryInputInt(source.Input, "question_count")
				if !ok || questionCount <= 0 {
					return ErrKnowledgeSpanRetryUnsupported
				}
			}
			if executes && pattern.MatchString(source.Name) {
				nameIndex, parseErr := strconv.Atoi(pattern.FindStringSubmatch(source.Name)[1])
				inputIndex, ok := retryInputInt(source.Input, inputKey)
				if parseErr != nil || !ok || inputIndex != nameIndex {
					return ErrKnowledgeSpanRetryUnsupported
				}
			}
		}

		var directRows []types.KnowledgeProcessingSpan
		directQuery := tx.Where("knowledge_id = ? AND attempt = ? AND parent_span_id = ?",
			request.KnowledgeID, request.Attempt, post.SpanID).Order("id ASC")
		if tx.Dialector.Name() == "postgres" {
			directQuery = directQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := directQuery.Find(&directRows).Error; err != nil {
			return err
		}
		latestDirect := make(map[string]types.KnowledgeProcessingSpan)
		for _, row := range directRows {
			if row.Kind == types.SpanKindSubSpan && isSettlementBranch(row.Name) {
				latestDirect[row.Name] = row
			}
		}
		selectedDirect := make(map[string]struct{}, len(allSources))
		for _, source := range allSources {
			name := source.Name
			if questionRetryOwnerPattern.MatchString(name) {
				name = "postprocess.question"
			}
			selectedDirect[name] = struct{}{}
		}
		executionSelectedDirect := make(map[string]struct{}, len(sources))
		for _, source := range sources {
			name := source.Name
			if questionRetryOwnerPattern.MatchString(name) {
				name = "postprocess.question"
			}
			executionSelectedDirect[name] = struct{}{}
		}
		for name, row := range latestDirect {
			if !isTerminalSpanStatus(row.Status) {
				if _, selected := selectedDirect[name]; !selected {
					return ErrKnowledgeSpanRetryNotTerminal
				}
			}
		}
		if !isTerminalSpanStatus(latestRoot.Status) && !hasStalledSource {
			return ErrKnowledgeSpanRetryNotTerminal
		}

		for i := range allSources {
			source := &allSources[i]
			fence := fences[source.SpanID]
			if fence == nil {
				continue
			}
			finishedAt := fence.LastHeartbeatAt
			now := time.Now()
			if finishedAt.After(now) {
				return ErrKnowledgeSpanRetryNotTerminal
			}
			duration := int64(1)
			if source.StartedAt != nil && finishedAt.After(*source.StartedAt) {
				duration = max(1, finishedAt.Sub(*source.StartedAt).Milliseconds())
			}
			updated := tx.Model(&types.KnowledgeProcessingSpan{}).
				Where("id = ? AND status IN ? AND updated_at = ?", source.ID,
					[]string{types.SpanStatusPending, types.SpanStatusRunning}, fence.SourceUpdatedAt).
				Updates(map[string]any{"status": types.SpanStatusFailed, "error_code": "ORPHANED_OWNER_RECOVERED",
					"error_message": "No live worker owns this stalled processing item", "finished_at": finishedAt,
					"duration_ms": duration, "updated_at": now})
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrKnowledgeSpanRetryNotTerminal
			}
			source.Status = types.SpanStatusFailed
			source.ErrorCode = "ORPHANED_OWNER_RECOVERED"
			source.ErrorMessage = "No live worker owns this stalled processing item"
			latestDirectName := source.Name
			if questionRetryOwnerPattern.MatchString(source.Name) {
				latestDirectName = "postprocess.question"
			}
			if latestDirectName != "postprocess.question" {
				latestDirect[latestDirectName] = *source
			}
		}
		if !isTerminalSpanStatus(latestRoot.Status) {
			if err := r.settleProcessingOutcomeTx(tx, request.KnowledgeID, request.Attempt); err != nil {
				return fmt.Errorf("settle stalled source attempt: %w", err)
			}
		}

		// Execution targets and settlement evidence are deliberately separate.
		// Only sources gets pending work/outbox rows. Every other latest failed
		// logical owner is inherited as a terminal failed row so a sparse retry
		// cannot hide unresolved work or report a false completed attempt.
		carryoverSources := make([]types.KnowledgeProcessingSpan, 0)
		for name, row := range latestDirect {
			if _, selected := executionSelectedDirect[name]; selected || row.Status != types.SpanStatusFailed {
				continue
			}
			if name == "postprocess.question" {
				continue
			}
			carryoverSources = append(carryoverSources, row)
		}
		selectedQuestionNames := make(map[string]struct{})
		for name := range targetNames {
			if questionRetryOwnerPattern.MatchString(name) {
				selectedQuestionNames[name] = struct{}{}
			}
		}
		topologyQuestionNames := make(map[string]struct{})
		for _, source := range allSources {
			if questionRetryOwnerPattern.MatchString(source.Name) {
				topologyQuestionNames[source.Name] = struct{}{}
			}
		}
		var carryoverQuestionParent *types.KnowledgeProcessingSpan
		if questionParent, ok := latestDirect["postprocess.question"]; ok {
			var questionRows []types.KnowledgeProcessingSpan
			questionQuery := tx.Where(
				"knowledge_id = ? AND attempt = ? AND parent_span_id = ?",
				request.KnowledgeID, request.Attempt, questionParent.SpanID,
			).Order("id ASC")
			if tx.Dialector.Name() == "postgres" {
				questionQuery = questionQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			if err := questionQuery.Find(&questionRows).Error; err != nil {
				return fmt.Errorf("load unresolved question retry branches: %w", err)
			}
			latestQuestion := make(map[string]types.KnowledgeProcessingSpan)
			hasFailedQuestionChild := false
			for _, row := range questionRows {
				if row.Kind == types.SpanKindSubSpan && questionRetryOwnerPattern.MatchString(row.Name) {
					latestQuestion[row.Name] = row
				}
			}
			for name, row := range latestQuestion {
				if !isTerminalSpanStatus(row.Status) {
					if _, selected := topologyQuestionNames[name]; !selected {
						return ErrKnowledgeSpanRetryNotTerminal
					}
				}
				if row.Status != types.SpanStatusFailed {
					continue
				}
				hasFailedQuestionChild = true
				if _, selected := selectedQuestionNames[name]; selected {
					continue
				}
				carryoverSources = append(carryoverSources, row)
			}
			_, questionSelected := executionSelectedDirect["postprocess.question"]
			if questionParent.Status == types.SpanStatusFailed && !questionSelected && !hasFailedQuestionChild {
				parent := questionParent
				carryoverQuestionParent = &parent
			}
		}
		sortRetrySpans(carryoverSources)

		sortRetrySpans(sources)
		attempt := latestRoot.Attempt + 1
		now := time.Now()
		targetSourceIDs := make([]any, 0, len(sources))
		for _, source := range sources {
			targetSourceIDs = append(targetSourceIDs, source.SpanID)
		}
		rootInput := types.JSONMap{"attempt_kind": "partial_repair", "mode": "partial_repair",
			"source_attempt": request.Attempt, "source_span_ids": targetSourceIDs,
			"client_request_id": request.ClientRequestID, "request_kind": multiRepairRequestKind(request),
			"plan_digest": planDigest}

		settlementBranches := make(map[string]struct{}, len(executionSelectedDirect)+len(carryoverSources))
		for name := range executionSelectedDirect {
			settlementBranches[name] = struct{}{}
		}
		for _, carryover := range carryoverSources {
			name := carryover.Name
			if questionRetryOwnerPattern.MatchString(name) {
				name = "postprocess.question"
			}
			settlementBranches[name] = struct{}{}
		}
		if carryoverQuestionParent != nil {
			settlementBranches["postprocess.question"] = struct{}{}
		}
		expectedBranches := make([]string, 0, len(settlementBranches))
		for name := range settlementBranches {
			expectedBranches = append(expectedBranches, name)
		}
		sort.Slice(expectedBranches, func(i, j int) bool {
			iGroup, iIndex, iName := retryTargetOrder(expectedBranches[i])
			jGroup, jIndex, jName := retryTargetOrder(expectedBranches[j])
			if iGroup != jGroup {
				return iGroup < jGroup
			}
			if iIndex != jIndex {
				return iIndex < jIndex
			}
			return iName < jName
		})

		expectedQuestionSet := make(map[string]struct{})
		for name := range selectedQuestionNames {
			expectedQuestionSet[name] = struct{}{}
		}
		for _, carryover := range carryoverSources {
			if questionRetryOwnerPattern.MatchString(carryover.Name) {
				expectedQuestionSet[carryover.Name] = struct{}{}
			}
		}
		expectedQuestion := make([]string, 0, len(expectedQuestionSet))
		for name := range expectedQuestionSet {
			expectedQuestion = append(expectedQuestion, name)
		}
		if len(expectedQuestion) > 0 {
			sort.Slice(expectedQuestion, func(i, j int) bool {
				_, ii, _ := retryTargetOrder(expectedQuestion[i])
				_, ji, _ := retryTargetOrder(expectedQuestion[j])
				return ii < ji
			})
		}
		expectedBranchValues := make([]any, len(expectedBranches))
		for i, name := range expectedBranches {
			expectedBranchValues[i] = name
		}
		expectedQuestionValues := make([]any, len(expectedQuestion))
		for i, name := range expectedQuestion {
			expectedQuestionValues[i] = name
		}
		postInput := types.JSONMap{"attempt_kind": "partial_repair", "expected_branches": expectedBranchValues,
			"expected_subtasks_count": len(sources), "fanout_complete": true}
		if len(expectedQuestionValues) > 0 {
			postInput["expected_question_children"] = expectedQuestionValues
		}

		rows := []types.KnowledgeProcessingSpan{{KnowledgeID: request.KnowledgeID, Attempt: attempt, SpanID: rootID,
			Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusRunning, Input: rootInput, StartedAt: &now}}
		for _, stageName := range []string{types.StageDocReader, types.StageChunking, types.StageEmbedding, types.StageMultimodal} {
			finished := now
			rows = append(rows, types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: attempt,
				SpanID: multiRepairSpanID(request, stageName), ParentSpanID: rootID, Name: stageName,
				Kind: types.SpanKindStage, Status: types.SpanStatusSkipped,
				Input: types.JSONMap{"attempt_kind": "partial_repair", "inherited": true}, StartedAt: &now, FinishedAt: &finished, DurationMs: 1})
		}
		rows = append(rows, types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: attempt,
			SpanID: postID, ParentSpanID: rootID, Name: types.StagePostProcess, Kind: types.SpanKindStage,
			Status: types.SpanStatusRunning, Input: postInput, StartedAt: &now})

		questionID := multiRepairSpanID(request, "question")
		if _, planned := settlementBranches["postprocess.question"]; planned {
			question := types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: attempt,
				SpanID: questionID, ParentSpanID: postID, Name: "postprocess.question", Kind: types.SpanKindSubSpan,
				Status: types.SpanStatusRunning, Input: types.JSONMap{"expected_question_children": expectedQuestionValues}, StartedAt: &now}
			if carryoverQuestionParent != nil {
				carryInput := make(types.JSONMap, len(carryoverQuestionParent.Input)+7)
				for key, value := range carryoverQuestionParent.Input {
					carryInput[key] = value
				}
				carryInput["source_attempt"] = request.Attempt
				carryInput["source_span_id"] = carryoverQuestionParent.SpanID
				carryInput["carryover_source_attempt"] = request.Attempt
				carryInput["carryover_source_span_id"] = carryoverQuestionParent.SpanID
				carryInput["attempt_kind"] = "partial_repair"
				carryInput["inherited"] = true
				carryInput["retry_target"] = false
				carryInput["terminal_failure"] = true
				finished := now
				question.Status = types.SpanStatusFailed
				question.Input = normalizeRetryJSONMap(carryInput)
				question.ErrorCode = carryoverQuestionParent.ErrorCode
				question.ErrorMessage = carryoverQuestionParent.ErrorMessage
				question.ErrorDetail = carryoverQuestionParent.ErrorDetail
				question.FinishedAt = &finished
				question.DurationMs = 1
			}
			rows = append(rows, question)
		}

		preparations = make([]*types.KnowledgeSpanRetryPreparation, 0, len(sources))
		for i := range sources {
			source := &sources[i]
			targetInput := make(types.JSONMap, len(source.Input)+5)
			for key, value := range source.Input {
				targetInput[key] = value
			}
			targetInput["source_attempt"] = request.Attempt
			targetInput["source_span_id"] = source.SpanID
			targetInput["client_request_id"] = request.ClientRequestID
			targetInput["attempt_kind"] = "partial_repair"
			targetInput["retry_target"] = true
			targetInput["source_retry_state"] = types.KnowledgeSpanRetryStateFailed
			if fences[source.SpanID] != nil {
				targetInput["source_retry_state"] = types.KnowledgeSpanRetryStateStalled
			}
			targetInput = normalizeRetryJSONMap(targetInput)
			targetID := multiRepairSpanID(request, "target:"+source.SpanID)
			parentID := postID
			if questionRetryOwnerPattern.MatchString(source.Name) {
				parentID = questionID
			}
			taskID, err := partialRepairTaskID(request.KnowledgeID, attempt, source.Name, targetInput)
			if err != nil {
				return err
			}
			language := strings.TrimSpace(request.Language)
			if language == "" {
				language, _ = retryInputString(source.Input, "language")
			}
			prepared := &types.KnowledgeSpanRetryPreparation{
				KnowledgeID: request.KnowledgeID, SourceAttempt: request.Attempt, SourceSpanID: source.SpanID,
				ClientRequestID: request.ClientRequestID, Attempt: attempt, SpanID: targetID, Name: source.Name,
				TaskID: taskID, Status: types.SpanStatusPending, DispatchRequired: true,
				TenantID: knowledge.TenantID, KnowledgeBaseID: knowledge.KnowledgeBaseID,
				Language: language, Input: targetInput,
			}
			canonical := types.KnowledgeSpanRetryOutboxPayload{TaskID: prepared.TaskID,
				KnowledgeID: prepared.KnowledgeID, Attempt: prepared.Attempt, SpanID: prepared.SpanID,
				TargetName: prepared.Name, TenantID: prepared.TenantID,
				KnowledgeBaseID: prepared.KnowledgeBaseID, Language: prepared.Language, Input: prepared.Input}
			rows = append(rows, types.KnowledgeProcessingSpan{KnowledgeID: request.KnowledgeID, Attempt: attempt,
				SpanID: targetID, ParentSpanID: parentID, Name: source.Name, Kind: types.SpanKindSubSpan,
				Status: types.SpanStatusPending, Input: targetInput,
				Metadata: normalizeRetryJSONMap(types.JSONMap{retryDispatchMetadataKey: canonical})})
			preparations = append(preparations, prepared)
		}
		for i := range carryoverSources {
			source := &carryoverSources[i]
			carryInput := make(types.JSONMap, len(source.Input)+6)
			for key, value := range source.Input {
				carryInput[key] = value
			}
			carryInput["source_attempt"] = request.Attempt
			carryInput["source_span_id"] = source.SpanID
			carryInput["carryover_source_attempt"] = request.Attempt
			carryInput["carryover_source_span_id"] = source.SpanID
			carryInput["attempt_kind"] = "partial_repair"
			carryInput["inherited"] = true
			carryInput["retry_target"] = false
			carryInput["terminal_failure"] = true
			parentID := postID
			if questionRetryOwnerPattern.MatchString(source.Name) {
				parentID = questionID
			}
			finished := now
			rows = append(rows, types.KnowledgeProcessingSpan{
				KnowledgeID: request.KnowledgeID, Attempt: attempt,
				SpanID: multiRepairSpanID(request, "carryover:"+source.SpanID), ParentSpanID: parentID,
				Name: source.Name, Kind: types.SpanKindSubSpan, Status: types.SpanStatusFailed,
				Input: normalizeRetryJSONMap(carryInput), ErrorCode: source.ErrorCode,
				ErrorMessage: source.ErrorMessage, ErrorDetail: source.ErrorDetail,
				StartedAt: &now, FinishedAt: &finished, DurationMs: 1,
			})
		}
		if err := tx.Create(&rows).Error; err != nil {
			return fmt.Errorf("seed multi-target partial repair spans: %w", err)
		}

		updates := map[string]any{"parse_status": types.ParseStatusFinalizing,
			"pending_subtasks_count": len(sources), "error_message": "", "updated_at": now}
		if _, summary := executionSelectedDirect["postprocess.summary"]; summary {
			updates["summary_status"] = types.SummaryStatusPending
		}
		if err := tx.Table("knowledges").Where("id = ?", request.KnowledgeID).Updates(updates).Error; err != nil {
			return fmt.Errorf("seed multi-target repair knowledge state: %w", err)
		}

		_, executeWiki := executionSelectedDirect["postprocess.wiki"]
		if executeWiki || stalledWikiFence != nil {
			deleteQuery := tx.Where("task_type = ? AND scope = ? AND scope_id = ? AND dedup_key = ? AND op = ?",
				types.TypeWikiIngest, types.TaskScopeKnowledgeBase, knowledge.KnowledgeBaseID, request.KnowledgeID, "ingest")
			if stalledWikiFence != nil {
				deleteQuery = deleteQuery.Where(
					"id IN ? AND claim_token = ? AND claimed_by_task_id = ? AND claim_heartbeat_at = ?",
					stalledWikiFence.PendingOpIDs, stalledWikiFence.ClaimToken,
					stalledWikiFence.ClaimedByTaskID, stalledWikiFence.ClaimHeartbeatAt,
				)
			}
			deleted := deleteQuery.Delete(&types.TaskPendingOp{})
			if deleted.Error != nil {
				return fmt.Errorf("replace prior wiki repair op: %w", deleted.Error)
			}
			if stalledWikiFence != nil && deleted.RowsAffected != int64(len(stalledWikiFence.PendingOpIDs)) {
				return ErrKnowledgeSpanRetryNotTerminal
			}
			if executeWiki {
				wikiWorkID := ""
				for _, prepared := range preparations {
					if prepared != nil && prepared.Name == "postprocess.wiki" {
						wikiWorkID = retryWikiWorkID(prepared.Input)
						break
					}
				}
				payload, err := json.Marshal(map[string]any{"op": "ingest", "knowledge_id": request.KnowledgeID,
					"attempt": attempt, "language": request.Language, "work_id": wikiWorkID})
				if err != nil {
					return err
				}
				if err := tx.Create(&types.TaskPendingOp{TenantID: knowledge.TenantID, TaskType: types.TypeWikiIngest,
					Scope: types.TaskScopeKnowledgeBase, ScopeID: knowledge.KnowledgeBaseID, Op: "ingest",
					DedupKey: request.KnowledgeID, Payload: payload, EnqueuedAt: now}).Error; err != nil {
					return fmt.Errorf("seed wiki repair op: %w", err)
				}
			}
		}
		for _, prepared := range preparations {
			payload, err := json.Marshal(types.KnowledgeSpanRetryOutboxPayload{TaskID: prepared.TaskID,
				KnowledgeID: prepared.KnowledgeID, Attempt: prepared.Attempt, SpanID: prepared.SpanID,
				TargetName: prepared.Name, TenantID: prepared.TenantID, KnowledgeBaseID: prepared.KnowledgeBaseID,
				Language: prepared.Language, Input: prepared.Input})
			if err != nil {
				return err
			}
			if err := tx.Create(&types.TaskPendingOp{TenantID: prepared.TenantID,
				TaskType: types.KnowledgeSpanRetryOutboxTaskType, Scope: types.KnowledgeSpanRetryOutboxScope,
				ScopeID: prepared.KnowledgeID, Op: types.KnowledgeSpanRetryOutboxOp,
				DedupKey: prepared.TaskID, Payload: payload, EnqueuedAt: now}).Error; err != nil {
				return fmt.Errorf("seed multi-target repair dispatch outbox: %w", err)
			}
		}
		return nil
	})
	return preparations, err
}

func retryWikiWorkID(input types.JSONMap) string {
	raw, ok := input[types.WikiIngestWorkBindingInputKey]
	if !ok || raw == nil {
		return ""
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return ""
	}
	var binding types.WikiIngestWorkBinding
	if json.Unmarshal(encoded, &binding) != nil {
		return ""
	}
	return strings.TrimSpace(binding.WorkID)
}

func normalizeRetryJSONMap(input types.JSONMap) types.JSONMap {
	payload, err := json.Marshal(input)
	if err != nil {
		return input
	}
	var result types.JSONMap
	if err := json.Unmarshal(payload, &result); err != nil {
		return input
	}
	return result
}

// PrepareFailedSpanRetry creates an isolated attempt containing only the
// failed logical owner. It intentionally does not reopen the old attempt:
// operators keep its exact failure/cancellation history while workers join a
// fresh attempt-scoped tree and counter.
func (r *knowledgeSpanRepository) PrepareFailedSpanRetry(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	multi, _, _ := canonicalSingletonRetryPlan(request)
	preparations, err := r.PrepareFailedSpanRetries(ctx, multi)
	if err != nil {
		return nil, err
	}
	if len(preparations) != 1 || preparations[0] == nil {
		return nil, fmt.Errorf("prepare failed span retry: expected one preparation, got %d", len(preparations))
	}
	return preparations[0], nil
}

func (r *knowledgeSpanRepository) FailPreparedSpanRetry(
	ctx context.Context,
	prepared *types.KnowledgeSpanRetryPreparation,
	errorCode, errorMessage string,
) error {
	if prepared == nil || prepared.KnowledgeID == "" || prepared.Attempt <= 0 ||
		prepared.SpanID == "" || prepared.TaskID == "" {
		return errors.New("fail prepared span retry: complete preparation required")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", prepared.KnowledgeID).Error; err != nil {
				return fmt.Errorf("serialize failed repair compensation: %w", err)
			}
		}
		deleted := tx.Where(
			"task_type = ? AND scope = ? AND scope_id = ? AND op = ? AND dedup_key = ?",
			types.KnowledgeSpanRetryOutboxTaskType, types.KnowledgeSpanRetryOutboxScope,
			prepared.KnowledgeID, types.KnowledgeSpanRetryOutboxOp, prepared.TaskID,
		).Delete(&types.TaskPendingOp{})
		if deleted.Error != nil {
			return fmt.Errorf("delete failed repair outbox: %w", deleted.Error)
		}
		if deleted.RowsAffected == 0 {
			return errors.New("delete failed repair outbox: durable dispatch row not found")
		}
		if prepared.Name == "postprocess.wiki" {
			if err := tx.Where(
				"task_type = ? AND scope = ? AND scope_id = ? AND op = ? AND dedup_key = ?",
				types.TypeWikiIngest, types.TaskScopeKnowledgeBase, prepared.KnowledgeBaseID,
				"ingest", prepared.KnowledgeID,
			).Delete(&types.TaskPendingOp{}).Error; err != nil {
				return fmt.Errorf("delete failed wiki repair op: %w", err)
			}
		}
		now := time.Now()
		updated := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND span_id = ? AND status IN ?",
				prepared.KnowledgeID, prepared.Attempt, prepared.SpanID,
				[]string{types.SpanStatusPending, types.SpanStatusRunning}).
			Updates(terminalSpanUpdates(tx, now, map[string]any{
				"status": types.SpanStatusFailed, "error_code": errorCode, "error_message": errorMessage,
			}))
		if updated.Error != nil {
			return fmt.Errorf("fail exact repair span: %w", updated.Error)
		}
		if updated.RowsAffected == 0 {
			return errors.New("fail exact repair span: pending owner not found")
		}
		if err := r.settleProcessingOutcomeTx(tx, prepared.KnowledgeID, prepared.Attempt); err != nil {
			return fmt.Errorf("settle failed repair attempt: %w", err)
		}
		return nil
	})
}

// CancelDescendants performs an iterative SQL walk: each level we update
// every row whose parent_span_id is in the previous level's span_id set,
// flipping pending/running rows to cancelled. We bail when a level adds
// zero rows (fixed point reached) or after a generous depth bound.
//
// Postgres-specific WITH RECURSIVE would be denser but harder to test on
// the SQLite Lite backend. The iterative path stays portable.
func (r *knowledgeSpanRepository) CancelDescendants(ctx context.Context, knowledgeID string, attempt int, parentSpanID, reason string) (int64, error) {
	frontier := []string{parentSpanID}
	var totalAffected int64
	for depth := 0; depth < 16 && len(frontier) > 0; depth++ {
		var nextFrontier []string
		// Find every child on the current frontier, including terminal
		// intermediates. A fan-out parent may already be done while one of its
		// descendants is still running; traversal must pass through that parent
		// even though only pending/running rows are actually cancelled.
		var children []types.KnowledgeProcessingSpan
		err := r.db.WithContext(ctx).
			Where("knowledge_id = ? AND attempt = ? AND parent_span_id IN ?",
				knowledgeID, attempt, frontier).
			Find(&children).Error
		if err != nil {
			return totalAffected, err
		}
		if len(children) == 0 {
			break
		}
		openIDs := make([]string, 0, len(children))
		for _, c := range children {
			nextFrontier = append(nextFrontier, c.SpanID)
			if c.Status == types.SpanStatusPending || c.Status == types.SpanStatusRunning {
				openIDs = append(openIDs, c.SpanID)
			}
		}
		if len(openIDs) > 0 {
			now := time.Now()
			res := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND attempt = ? AND span_id IN ? AND status IN ?",
					knowledgeID, attempt, openIDs,
					[]string{types.SpanStatusPending, types.SpanStatusRunning}).
				Updates(terminalSpanUpdates(r.db, now, map[string]any{
					"status":        types.SpanStatusCancelled,
					"error_code":    "UPSTREAM_FAILED",
					"error_message": reason,
				}))
			if res.Error != nil {
				return totalAffected, res.Error
			}
			totalAffected += res.RowsAffected
		}
		frontier = nextFrontier
	}
	return totalAffected, nil
}

// CancelAllOpenSpans is the "abort the attempt" counterpart to
// CancelDescendants. It avoids the BFS entirely so spans whose parent
// is already terminal (typical for stage fan-outs that EndSpan as soon
// as they finish dispatching async work) still get flipped to cancelled.
// finished_at and duration_ms are frozen atomically so cancelled history does
// not keep accruing elapsed time in the trace UI.
func (r *knowledgeSpanRepository) CancelAllOpenSpans(
	ctx context.Context, knowledgeID string, attempt int, errorCode, reason string,
) (int64, error) {
	now := time.Now()
	updates := terminalSpanUpdates(r.db, now, map[string]any{
		"status":        types.SpanStatusCancelled,
		"error_code":    errorCode,
		"error_message": reason,
	})
	res := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND status IN ?",
			knowledgeID, attempt,
			[]string{types.SpanStatusPending, types.SpanStatusRunning}).
		Updates(updates)
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

func (r *knowledgeSpanRepository) CancelOpenSpansByName(
	ctx context.Context, knowledgeID string, attempt int, name, errorCode, reason string,
) (int64, error) {
	if knowledgeID == "" || attempt <= 0 || name == "" {
		return 0, nil
	}
	now := time.Now()
	res := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND name = ? AND status IN ?",
			knowledgeID, attempt, name,
			[]string{types.SpanStatusPending, types.SpanStatusRunning}).
		Updates(terminalSpanUpdates(r.db, now, map[string]any{
			"status":        types.SpanStatusCancelled,
			"error_code":    errorCode,
			"error_message": reason,
		}))
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

type processingKnowledgeSnapshot struct {
	ParseStatus          string     `gorm:"column:parse_status"`
	PendingSubtasksCount int        `gorm:"column:pending_subtasks_count"`
	ErrorMessage         string     `gorm:"column:error_message"`
	ProcessedAt          *time.Time `gorm:"column:processed_at"`
}

type processingReduction struct {
	post              *types.KnowledgeProcessingSpan
	root              *types.KnowledgeProcessingSpan
	questionGroup     *types.KnowledgeProcessingSpan
	questionStatus    string
	questionErrorCode string
	questionMessage   string
	postStatus        string
	postErrorCode     string
	postMessage       string
	remaining         int
	terminal          bool
}

// SettleProcessingOutcome reduces the complete attempt inside one database
// transaction. pending_subtasks_count is only an observer/barrier; it is
// recalculated from exact logical children and never decides success itself.
func (r *knowledgeSpanRepository) SettleProcessingOutcome(
	ctx context.Context, knowledgeID string, attempt int,
) error {
	if knowledgeID == "" || attempt <= 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return r.settleProcessingOutcomeTx(tx, knowledgeID, attempt)
	})
}

func (r *knowledgeSpanRepository) SettleWikiPendingOp(
	ctx context.Context,
	knowledgeID string,
	attempt int,
	pendingIDs []int64,
	deadLetter *types.TaskDeadLetter,
	owner *types.TaskClaimOwner,
) error {
	if knowledgeID == "" || attempt <= 0 {
		return errors.New("settle wiki pending op: knowledge id and attempt are required")
	}
	ids := make([]int64, 0, len(pendingIDs))
	seen := make(map[int64]struct{}, len(pendingIDs))
	for _, id := range pendingIDs {
		if id <= 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return errors.New("settle wiki pending op: durable pending row is required")
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// This is the same lock used by OpenAttempt and the outcome reducer.
		// Holding it across the terminal check, reducer, dead-letter insert and
		// queue delete prevents a new attempt or a replay worker from observing
		// a half-acknowledged Wiki result.
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", knowledgeID,
			).Error; err != nil {
				return fmt.Errorf("serialize wiki pending settlement: %w", err)
			}
		}

		var latestRootAttempt int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", knowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt), 0)").Row().Scan(&latestRootAttempt); err != nil {
			return fmt.Errorf("read latest wiki processing attempt: %w", err)
		}
		if latestRootAttempt != attempt {
			return fmt.Errorf("settle wiki pending op: attempt %d is not latest %d", attempt, latestRootAttempt)
		}

		var wikiSpan types.KnowledgeProcessingSpan
		wikiQuery := tx.Where(
			"knowledge_id = ? AND attempt = ? AND name = ?",
			knowledgeID, attempt, "postprocess.wiki",
		).Order("id DESC")
		if tx.Dialector.Name() == "postgres" {
			wikiQuery = wikiQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := wikiQuery.Take(&wikiSpan).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("settle wiki pending op: terminal postprocess.wiki span is missing")
			}
			return fmt.Errorf("lock terminal postprocess.wiki span: %w", err)
		}
		switch wikiSpan.Status {
		case types.SpanStatusDone, types.SpanStatusFailed, types.SpanStatusSkipped, types.SpanStatusCancelled:
		default:
			return fmt.Errorf("settle wiki pending op: postprocess.wiki span is %s", wikiSpan.Status)
		}

		if err := r.settleProcessingOutcomeTx(tx, knowledgeID, attempt); err != nil {
			return fmt.Errorf("settle wiki processing outcome: %w", err)
		}
		if deadLetter != nil {
			archived := *deadLetter
			if archived.FailedAt.IsZero() {
				archived.FailedAt = time.Now()
			}
			if err := tx.Create(&archived).Error; err != nil {
				return fmt.Errorf("archive wiki pending dead letter: %w", err)
			}
		}
		deleteQuery := tx.Where(
			"id IN ? AND task_type = ? AND dedup_key = ?", ids, types.TypeWikiIngest, knowledgeID,
		)
		if owner != nil {
			if !owner.Valid() {
				return errors.New("settle wiki pending op: invalid claim owner")
			}
			deleteQuery = deleteQuery.Where(
				"claim_token = ? AND claimed_by_task_id = ?", owner.Token, owner.TaskID,
			)
		} else {
			deleteQuery = deleteQuery.Where(
				"claim_token IS NULL AND claimed_by_task_id IS NULL AND claim_heartbeat_at IS NULL",
			)
		}
		deleted := deleteQuery.Delete(&types.TaskPendingOp{})
		if deleted.Error != nil {
			return fmt.Errorf("consume settled wiki pending rows: %w", deleted.Error)
		}
		if deleted.RowsAffected != int64(len(ids)) {
			return fmt.Errorf(
				"consume settled wiki pending rows: deleted %d of %d expected rows",
				deleted.RowsAffected, len(ids),
			)
		}
		return nil
	})
}

func (r *knowledgeSpanRepository) settleProcessingOutcomeTx(
	tx *gorm.DB, knowledgeID string, attempt int,
) error {
	// Serialize against OpenAttempt so a late terminal delivery cannot pass
	// the latest-attempt check while a newer root is being inserted.
	if tx.Dialector.Name() == "postgres" {
		if err := tx.Exec(
			"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", knowledgeID,
		).Error; err != nil {
			return fmt.Errorf("serialize knowledge settlement: %w", err)
		}
	}
	var knowledge processingKnowledgeSnapshot
	knowledgeQuery := tx.Table("knowledges").
		Select("parse_status", "pending_subtasks_count", "error_message", "processed_at").
		Where("id = ?", knowledgeID)
	if tx.Dialector.Name() == "postgres" {
		knowledgeQuery = knowledgeQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	knowledgeResult := knowledgeQuery.Take(&knowledge)
	knowledgeExists := knowledgeResult.Error == nil
	if knowledgeResult.Error != nil && !errors.Is(knowledgeResult.Error, gorm.ErrRecordNotFound) {
		return fmt.Errorf("lock knowledge processing outcome: %w", knowledgeResult.Error)
	}
	// Cancellation/deletion wins before any parent or root success write.
	// AbortAttempt owns closing the remaining spans; the reducer must not
	// briefly resurrect their parents while that cancellation is draining.
	if knowledgeExists && (knowledge.ParseStatus == types.ParseStatusCancelled ||
		knowledge.ParseStatus == types.ParseStatusDeleting) {
		return nil
	}

	var latestRootAttempt int
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND kind = ?", knowledgeID, types.SpanKindRoot).
		Select("COALESCE(MAX(attempt), 0)").Row().Scan(&latestRootAttempt); err != nil {
		return fmt.Errorf("read latest processing attempt: %w", err)
	}
	if latestRootAttempt > attempt {
		return nil
	}

	var rows []types.KnowledgeProcessingSpan
	spanQuery := tx.Where("knowledge_id = ? AND attempt = ?", knowledgeID, attempt).Order("id ASC")
	if tx.Dialector.Name() == "postgres" {
		spanQuery = spanQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := spanQuery.Find(&rows).Error; err != nil {
		return fmt.Errorf("lock processing spans: %w", err)
	}
	reduction := reduceProcessingOutcome(rows)
	if reduction.post == nil || reduction.root == nil {
		return nil
	}
	if reduction.terminal && reduction.remaining > 0 && reduction.postStatus == types.SpanStatusFailed {
		now := time.Now()
		cancelled := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND attempt = ? AND span_id NOT IN ? AND status IN ?",
				knowledgeID, attempt, []string{reduction.root.SpanID, reduction.post.SpanID},
				[]string{types.SpanStatusPending, types.SpanStatusRunning}).
			Updates(terminalSpanUpdates(tx, now, map[string]any{
				"status": types.SpanStatusCancelled, "error_code": "POSTPROCESS_FAIL_FAST",
				"error_message": "cancelled after a postprocess owner reached terminal failure",
			}))
		if cancelled.Error != nil {
			return fmt.Errorf("cancel remaining postprocess work after terminal failure: %w", cancelled.Error)
		}
		for i := range rows {
			if rows[i].SpanID == reduction.root.SpanID || rows[i].SpanID == reduction.post.SpanID {
				continue
			}
			if rows[i].Status == types.SpanStatusPending || rows[i].Status == types.SpanStatusRunning {
				rows[i].Status = types.SpanStatusCancelled
				rows[i].ErrorCode = "POSTPROCESS_FAIL_FAST"
				rows[i].ErrorMessage = "cancelled after a postprocess owner reached terminal failure"
			}
		}
		reduction.remaining = 0
	}

	if reduction.questionGroup != nil && reduction.questionStatus != "" {
		if err := updateReducedSpan(tx, reduction.questionGroup, reduction.questionStatus,
			reduction.questionErrorCode, reduction.questionMessage); err != nil {
			return err
		}
	}
	if !reduction.terminal {
		if knowledgeExists {
			if err := tx.Table("knowledges").Where("id = ?", knowledgeID).
				Updates(map[string]any{
					"pending_subtasks_count": reduction.remaining,
					"updated_at":             time.Now(),
				}).Error; err != nil {
				return fmt.Errorf("update processing observer counter: %w", err)
			}
		}
		return nil
	}

	if err := updateReducedSpan(tx, reduction.post, reduction.postStatus,
		reduction.postErrorCode, reduction.postMessage); err != nil {
		return err
	}

	rootStatus, rootCode, rootMessage, allStagesTerminal := reduceRootStatus(rows, reduction.post)
	if !allStagesTerminal {
		if knowledgeExists {
			if err := tx.Table("knowledges").Where("id = ?", knowledgeID).
				Updates(map[string]any{
					"pending_subtasks_count": 0,
					"updated_at":             time.Now(),
				}).Error; err != nil {
				return fmt.Errorf("update settled child counter: %w", err)
			}
		}
		return nil
	}
	if err := updateReducedSpan(tx, reduction.root, rootStatus, rootCode, rootMessage); err != nil {
		return err
	}
	if !knowledgeExists {
		return nil
	}
	parseStatus := types.ParseStatusCompleted
	errorMessage := ""
	if rootStatus == types.SpanStatusFailed {
		parseStatus = types.ParseStatusFailed
		errorMessage = rootMessage
	}
	if rootStatus == types.SpanStatusCancelled {
		parseStatus = types.ParseStatusCancelled
		errorMessage = rootMessage
	}
	now := time.Now()
	updates := map[string]any{
		"parse_status":           parseStatus,
		"pending_subtasks_count": 0,
		"error_message":          errorMessage,
		"updated_at":             now,
	}
	if parseStatus == types.ParseStatusCompleted {
		updates["processed_at"] = now
	}
	if err := tx.Table("knowledges").Where("id = ?", knowledgeID).Updates(updates).Error; err != nil {
		return fmt.Errorf("settle knowledge processing outcome: %w", err)
	}
	if parseStatus == types.ParseStatusCompleted {
		completion := &types.KnowledgeCompletionOutbox{
			KnowledgeID: knowledgeID,
			Attempt:     attempt,
			State:       types.KnowledgeCompletionOutboxPending,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "knowledge_id"}, {Name: "attempt"}},
			DoNothing: true,
		}).Create(completion).Error; err != nil {
			return fmt.Errorf("persist knowledge completion event: %w", err)
		}
	}
	return nil
}

func reduceProcessingOutcome(rows []types.KnowledgeProcessingSpan) processingReduction {
	var reduction processingReduction
	parentNames := make(map[string]string, len(rows))
	for i := range rows {
		parentNames[rows[i].SpanID] = rows[i].Name
		if rows[i].Kind == types.SpanKindRoot && (reduction.root == nil || rows[i].ID > reduction.root.ID) {
			reduction.root = &rows[i]
		}
		if rows[i].Kind == types.SpanKindStage && rows[i].Name == types.StagePostProcess &&
			(reduction.post == nil || rows[i].ID > reduction.post.ID) {
			reduction.post = &rows[i]
		}
	}
	if reduction.post == nil || reduction.root == nil {
		return reduction
	}

	latest := make(map[types.KnowledgeProcessingLogicalChildIdentity]*types.KnowledgeProcessingSpan)
	for i := range rows {
		row := &rows[i]
		parentName := parentNames[row.ParentSpanID]
		if parentName == "" {
			continue
		}
		identity := row.LogicalChildIdentity(parentName)
		if previous := latest[identity]; previous == nil || row.ID > previous.ID {
			latest[identity] = row
		}
	}
	lookup := func(parentName, childName string) *types.KnowledgeProcessingSpan {
		return latest[types.KnowledgeProcessingLogicalChildIdentity{
			KnowledgeID: reduction.post.KnowledgeID, Attempt: reduction.post.Attempt,
			ParentBranchName: parentName, LogicalChildName: childName,
		}]
	}

	expectedBranches, planned := settlementExpectedBranches(reduction.post.Input)
	fanoutComplete := settlementJSONBool(reduction.post.Input["fanout_complete"])
	questionBatchCount := settlementPositiveJSONInt(reduction.post.Input["question_batch_count"])
	expectedQuestionChildren, sparseQuestionPlan := settlementStringList(reduction.post.Input, "expected_question_children")
	if group := lookup(types.StagePostProcess, "postprocess.question"); group != nil {
		reduction.questionGroup = group
		if questionBatchCount <= 0 {
			questionBatchCount = settlementPositiveJSONInt(group.Input["batch_count"])
		}
	}
	if !planned {
		for identity := range latest {
			if identity.ParentBranchName == types.StagePostProcess && isSettlementBranch(identity.LogicalChildName) {
				expectedBranches = append(expectedBranches, identity.LogicalChildName)
			}
		}
		if len(expectedBranches) == 0 {
			return reduction
		}
	}

	failedNames := make([]string, 0)
	terminalFailedNames := make([]string, 0)
	cancelledNames := make([]string, 0)
	missingNames := make([]string, 0)
	remaining := 0
	questionRemaining := 0
	questionFailed := false
	questionCancelled := false
	questionMissing := make([]string, 0)
	observe := func(name string, row *types.KnowledgeProcessingSpan, question bool) {
		if row == nil {
			if fanoutComplete {
				missingNames = append(missingNames, name)
				if question {
					questionMissing = append(questionMissing, name)
				}
			} else {
				remaining++
				if question {
					questionRemaining++
				}
			}
			return
		}
		switch row.Status {
		case types.SpanStatusDone, types.SpanStatusSkipped:
		case types.SpanStatusFailed:
			failedNames = append(failedNames, name)
			if settlementJSONBool(row.Input["terminal_failure"]) {
				terminalFailedNames = append(terminalFailedNames, name)
			}
			questionFailed = questionFailed || question
		case types.SpanStatusCancelled:
			cancelledNames = append(cancelledNames, name)
			questionCancelled = questionCancelled || question
		default:
			remaining++
			if question {
				questionRemaining++
			}
		}
	}
	for _, branch := range expectedBranches {
		if branch == "postprocess.question" {
			if sparseQuestionPlan {
				for _, name := range expectedQuestionChildren {
					observe(name, lookup("postprocess.question", name), true)
				}
				continue
			}
			if questionBatchCount <= 0 {
				observe(branch, reduction.questionGroup, true)
				continue
			}
			for index := 0; index < questionBatchCount; index++ {
				name := fmt.Sprintf("postprocess.question.batch[%d]", index)
				observe(name, lookup("postprocess.question", name), true)
			}
			continue
		}
		observe(branch, lookup(types.StagePostProcess, branch), false)
	}
	reduction.remaining = remaining
	if reduction.questionGroup != nil && questionRemaining == 0 {
		switch {
		case len(questionMissing) > 0:
			reduction.questionStatus = types.SpanStatusFailed
			reduction.questionErrorCode = "QUESTION_BATCH_MISSING"
			reduction.questionMessage = "missing logical question children: " + strings.Join(questionMissing, ", ")
		case questionFailed:
			reduction.questionStatus = types.SpanStatusFailed
			reduction.questionErrorCode = "QUESTION_BATCH_FAILED"
			reduction.questionMessage = "one or more question batches failed"
		case questionCancelled:
			reduction.questionStatus = types.SpanStatusCancelled
			reduction.questionErrorCode = "QUESTION_BATCH_CANCELLED"
			reduction.questionMessage = "one or more question batches were cancelled"
		default:
			reduction.questionStatus = types.SpanStatusDone
		}
	}
	if remaining > 0 && len(terminalFailedNames) == 0 && len(missingNames) == 0 {
		return reduction
	}
	reduction.terminal = true
	switch {
	case len(missingNames) > 0:
		reduction.postStatus = types.SpanStatusFailed
		reduction.postErrorCode = "POSTPROCESS_BRANCH_MISSING"
		reduction.postMessage = "missing logical postprocess children: " + strings.Join(missingNames, ", ")
	case len(failedNames) > 0:
		reduction.postStatus = types.SpanStatusFailed
		reduction.postErrorCode = "POSTPROCESS_BRANCH_FAILED"
		reduction.postMessage = "failed logical postprocess children: " + strings.Join(failedNames, ", ")
	case len(cancelledNames) > 0:
		reduction.postStatus = types.SpanStatusCancelled
		reduction.postErrorCode = "POSTPROCESS_BRANCH_CANCELLED"
		reduction.postMessage = "cancelled logical postprocess children: " + strings.Join(cancelledNames, ", ")
	default:
		reduction.postStatus = types.SpanStatusDone
	}
	return reduction
}

func reduceRootStatus(
	rows []types.KnowledgeProcessingSpan, post *types.KnowledgeProcessingSpan,
) (status, errorCode, message string, terminal bool) {
	status = types.SpanStatusDone
	terminal = true
	for i := range rows {
		row := &rows[i]
		if row.Kind != types.SpanKindStage {
			continue
		}
		rowStatus := row.Status
		if row.SpanID == post.SpanID {
			rowStatus = post.Status
		}
		switch rowStatus {
		case types.SpanStatusDone, types.SpanStatusSkipped:
		case types.SpanStatusFailed:
			status = types.SpanStatusFailed
			errorCode = "PROCESSING_STAGE_FAILED"
			message = "one or more processing stages failed"
			if row.SpanID == post.SpanID && post.ErrorMessage != "" {
				errorCode = post.ErrorCode
				message = post.ErrorMessage
			}
		case types.SpanStatusCancelled:
			if status != types.SpanStatusFailed {
				status = types.SpanStatusCancelled
				errorCode = "PROCESSING_STAGE_CANCELLED"
				message = "one or more processing stages were cancelled"
			}
		default:
			terminal = false
		}
	}
	return status, errorCode, message, terminal
}

func updateReducedSpan(
	tx *gorm.DB, span *types.KnowledgeProcessingSpan, status, errorCode, message string,
) error {
	if span == nil || status == "" || (span.Status != types.SpanStatusPending && span.Status != types.SpanStatusRunning) {
		return nil
	}
	now := time.Now()
	updates := terminalSpanUpdates(tx, now, map[string]any{
		"status":        status,
		"error_code":    errorCode,
		"error_message": message,
	})
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id = ? AND attempt = ? AND span_id = ? AND status IN ?",
			span.KnowledgeID, span.Attempt, span.SpanID,
			[]string{types.SpanStatusPending, types.SpanStatusRunning}).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("settle processing span %s: %w", span.Name, err)
	}
	span.Status = status
	span.ErrorCode = errorCode
	span.ErrorMessage = message
	span.FinishedAt = &now
	if span.StartedAt != nil {
		span.DurationMs = max(1, now.Sub(*span.StartedAt).Milliseconds())
	}
	return nil
}

func settlementExpectedBranches(input types.JSONMap) ([]string, bool) {
	return settlementStringList(input, "expected_branches")
}

func settlementStringList(input types.JSONMap, key string) ([]string, bool) {
	if input == nil {
		return nil, false
	}
	raw, ok := input[key]
	if !ok {
		return nil, false
	}
	seen := make(map[string]struct{})
	result := make([]string, 0)
	appendName := func(value any) {
		name, ok := value.(string)
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	switch values := raw.(type) {
	case []string:
		for _, value := range values {
			appendName(value)
		}
	case []any:
		for _, value := range values {
			appendName(value)
		}
	default:
		return nil, false
	}
	return result, true
}

func settlementPositiveJSONInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func settlementJSONBool(value any) bool {
	result, _ := value.(bool)
	return result
}

func isSettlementBranch(name string) bool {
	return name == "postprocess.summary" || name == "postprocess.question" ||
		name == "postprocess.wiki" || strings.HasPrefix(name, "postprocess.graph.chunk[")
}
