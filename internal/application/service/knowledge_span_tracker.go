// Package service: span tracker.
//
// SpanTracker is the pipeline-facing facade for recording per-attempt
// progress trees. It mirrors Langfuse's vocabulary (root / span /
// generation) so the UI's mental model matches what operators already use
// for LLM call observability.
//
// Lifecycle:
//
//	attempt := tracker.OpenAttempt(ctx, knowledgeID, langfuseTraceID)
//	  // creates the root span; every subsequent Begin* call uses this attempt
//
//	stage := tracker.BeginStage(ctx, knowledgeID, attempt, types.StageDocReader, input)
//	  // ...do work...
//	tracker.EndSpan(ctx, stage, output)         // success
//	tracker.FailSpan(ctx, stage, code, msg, err) // error
//	tracker.SkipSpan(ctx, stage, reason)        // intentionally not run
//
//	sub := tracker.BeginSubSpan(ctx, parentSpan, "multimodal.image[0]", types.SpanKindGeneration, input)
//	  // ...
//
// All operations are best-effort: a DB error is logged and swallowed so a
// tracker hiccup never breaks the parsing pipeline. Knowledge.parse_status
// remains the authoritative source of truth for completion.
package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// maxSpanNameLen matches knowledge_processing_spans.name (varchar(255)).
const (
	maxSpanNameLen            = 255
	spanTerminalWriteAttempts = 3
	spanTerminalWriteTimeout  = 5 * time.Second
)

// fitSpanName ensures a span name fits the DB column. Wiki ingest builds
// names like postprocess.wiki.page[<slug>] which can exceed 64 chars when
// the slug is a long romanized entity name; when truncated an 8-hex hash
// suffix keeps concurrent subspans distinct. Truncation is rune-aware to
// match PostgreSQL VARCHAR(255) character semantics and avoid splitting
// multi-byte UTF-8 sequences.
func fitSpanName(name string) string {
	runes := []rune(name)
	if len(runes) <= maxSpanNameLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	suffix := fmt.Sprintf("~%x", sum[:4])
	suffixRunes := []rune(suffix)
	keep := maxSpanNameLen - len(suffixRunes)
	if keep < 1 {
		if len(suffixRunes) > maxSpanNameLen {
			return string(suffixRunes[:maxSpanNameLen])
		}
		return suffix
	}
	return string(runes[:keep]) + suffix
}

// Span is the in-memory handle the pipeline holds while a stage / subspan
// is executing. It carries enough context for End/Fail/Skip to write back
// without re-querying the DB. Returned (and required) from every Begin*.
type Span struct {
	KnowledgeID  string
	Attempt      int
	SpanID       string
	ParentSpanID string
	Name         string
	Kind         string
	Status       string
	Input        types.JSONMap
	StartedAt    time.Time
}

// SpanTracker is the only public surface — kept as an interface so tests
// can swap in a no-op without spinning up a database.
type SpanTracker interface {
	// OpenAttempt creates a new root span for (knowledgeID,
	// nextAttempt) and returns its number plus the root *Span. Call
	// at the start of a parse / reparse, before any other Begin*.
	OpenAttempt(ctx context.Context, knowledgeID, langfuseTraceID string) (root *Span, attempt int, err error)

	// LatestAttempt returns the highest attempt number recorded for
	// the knowledge, or 0 if it's never been parsed. Used by the API
	// layer to default to "show me the most recent run".
	LatestAttempt(ctx context.Context, knowledgeID string) int

	// BeginStage starts one of the canonical stages. Looks up the
	// root span for (kid, attempt) — caller passes attempt to make
	// the wiring explicit and let cross-process workers join an
	// existing attempt without new repo lookups.
	BeginStage(ctx context.Context, knowledgeID string, attempt int, stage string, input types.JSONMap) *Span

	// UpdateSpanInput persists orchestration metadata after a stage has started
	// without reopening it or disturbing its timing/status fields.
	UpdateSpanInput(ctx context.Context, span *Span, input types.JSONMap) error

	// LookupAttemptRoot returns the durable root for one exact attempt. Reparse
	// workers use its Input as an attempt-scoped cleanup checkpoint across
	// process restarts and Asynq retries.
	LookupAttemptRoot(ctx context.Context, knowledgeID string, attempt int) (*Span, error)

	// BeginSubSpan creates a child span under parent. parent may be a
	// stage span (for multimodal.image[i] / embedding.batch[i]) or
	// another subspan. kind is "subspan" or "generation" — generations
	// will be stitched to a Langfuse generation by trace_id.
	BeginSubSpan(ctx context.Context, parent *Span, name, kind string, input types.JSONMap) *Span

	// QueueSubSpan persists a pending logical child before its task is
	// published. BeginSubSpan atomically claims this row when the worker starts.
	QueueSubSpan(ctx context.Context, parent *Span, name, kind string, input types.JSONMap) *Span

	// SettleQuestionGroup keeps postprocess.question running until the latest
	// delivery of every expected batch is terminal. It is safe to call from
	// every batch worker; the final caller closes the parent idempotently.
	SettleQuestionGroup(ctx context.Context, knowledgeID string, attempt int)

	// SettlePostProcessTree closes postprocess only after every counted async
	// branch is terminal, then closes the knowledge-processing root. A failed
	// or cancelled branch propagates failure upward; running branches keep both
	// ancestors open.
	SettlePostProcessTree(ctx context.Context, knowledgeID string, attempt int)

	// RecordGeneration mirrors one completed Langfuse Generation into the
	// document trace using only correlation/model/token metadata. It is
	// idempotent by the Generation span id and never stores prompts or outputs.
	RecordGeneration(ctx context.Context, record types.KnowledgeGenerationUsage)

	// EndSpan marks span as done with optional output. Safe with nil.
	EndSpan(ctx context.Context, span *Span, output types.JSONMap)

	// FailSpan marks span as failed and cascade-cancels its
	// descendants. errorDetail (a Go error) is recorded verbatim in
	// error_detail (truncated to 8 KB) for admin views.
	FailSpan(ctx context.Context, span *Span, errorCode, errorMessage string, errorDetail error)

	// SkipSpan marks an intentionally not-run span (e.g. multimodal
	// on a text-only document). Distinct from cancelled — skipped is
	// "we chose not to" while cancelled is "an upstream broke".
	SkipSpan(ctx context.Context, span *Span, reason string)

	// LookupStage returns the stage's *Span for an in-flight attempt
	// — the cross-process bridge that lets an asynq worker (e.g.
	// image_multimodal) attach subspans to the parent stage span
	// created by the upstream pipeline.
	LookupStage(ctx context.Context, knowledgeID string, attempt int, stage string) *Span

	// LookupSpanByName returns the latest span of any kind matching name
	// for (knowledgeID, attempt) — the cross-process bridge that lets a
	// fan-out worker (e.g. a question-generation batch) attach its subspan
	// under a grouping span created earlier by the orchestrator. Returns
	// nil when no such span exists (caller should fall back to the stage).
	LookupSpanByName(ctx context.Context, knowledgeID string, attempt int, name string) *Span

	// FinalizeAttempt closes the root span for (knowledgeID, attempt)
	// with the given terminal status (done | failed). Idempotent:
	// re-closing an already-terminal root is a no-op so callers from
	// multiple paths (success orchestrator, dead-letter handler,
	// housekeeping) can fire without coordination. status defaults to
	// done; output/error are written verbatim.
	FinalizeAttempt(ctx context.Context, knowledgeID string, attempt int, status string,
		output types.JSONMap, errorCode, errorMessage string)

	// AbortAttempt cascade-cancels every still-running descendant of the
	// attempt's root span and then closes the root as cancelled. Used by
	// the user-initiated cancel path so the trace viewer doesn't leave
	// stranded subspans (multimodal images, postprocess subtasks)
	// looking like they're still in flight forever after the user
	// stopped the parse. Idempotent.
	AbortAttempt(ctx context.Context, knowledgeID string, attempt int, errorCode, errorMessage, reason string)
}

type spanTracker struct {
	repo repository.KnowledgeSpanRepository
	// db is held purely for the heartbeat side-channel: every span
	// state transition pokes knowledge.updated_at so the housekeeping
	// sweep can tell "actively running long stage" from "abandoned".
	// nil-safe — when missing (test harness) the heartbeat is skipped.
	db *gorm.DB

	// startsMu guards the in-process duration cache. Cross-process
	// workers won't find their parent's start here — that's fine,
	// duration_ms falls back to (FinishedAt - row.StartedAt) computed
	// at write time when the cache misses.
	startsMu sync.Mutex
	starts   map[string]time.Time // span_id → started_at
}

type attemptCommitGuardRepository interface {
	WithAttemptCommitGuard(context.Context, string, int, func(context.Context) error) error
}

// NewSpanTracker constructs the GORM-backed tracker. A nil repo collapses
// to a no-op so test harnesses don't need to spin up a database. The db
// is optional too: it's used only for the housekeeping heartbeat (see
// touchKnowledgeHeartbeat) and a nil db just disables that side-channel.
func NewSpanTracker(repo repository.KnowledgeSpanRepository, db *gorm.DB) SpanTracker {
	if repo == nil {
		return noopSpanTracker{}
	}
	return &spanTracker{
		repo:   repo,
		db:     db,
		starts: make(map[string]time.Time),
	}
}

// PrepareFailedSpanRetry is intentionally an optional tracker capability
// rather than part of SpanTracker's broad recording interface. Only the HTTP
// repair path needs the repository transaction; existing test/no-op trackers
// remain small and do not gain a mutation they cannot safely implement.
func (t *spanTracker) PrepareFailedSpanRetry(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryPreparation, error) {
	return t.repo.PrepareFailedSpanRetry(ctx, request)
}

func (t *spanTracker) PrepareFailedSpanRetries(
	ctx context.Context, request types.KnowledgeSpanMultiRetryRequest,
) ([]*types.KnowledgeSpanRetryPreparation, error) {
	return t.repo.PrepareFailedSpanRetries(ctx, request)
}

func (t *spanTracker) FindExistingFailedSpanRetryPlan(
	ctx context.Context, knowledgeID string, sourceAttempt int, clientRequestID, requestKind string,
) ([]*types.KnowledgeSpanRetryPreparation, error) {
	return t.repo.FindExistingFailedSpanRetryPlan(ctx, knowledgeID, sourceAttempt, clientRequestID, requestKind)
}

// WithAttemptCommitGuard is an optional worker-fencing capability. Keeping it
// outside SpanTracker avoids forcing recording-only fakes to implement a
// write-side transaction boundary they cannot provide safely.
func (t *spanTracker) WithAttemptCommitGuard(
	ctx context.Context, knowledgeID string, attempt int, fn func(context.Context) error,
) error {
	guard, ok := t.repo.(attemptCommitGuardRepository)
	if !ok {
		return errors.New("attempt commit guard is unavailable")
	}
	return guard.WithAttemptCommitGuard(ctx, knowledgeID, attempt, fn)
}

func (t *spanTracker) InspectSpanRetryTarget(
	ctx context.Context, request types.KnowledgeSpanRetryRequest,
) (*types.KnowledgeSpanRetryTargetSnapshot, error) {
	return t.repo.InspectSpanRetryTarget(ctx, request)
}

func (t *spanTracker) ListFailedSpanRetryCandidates(
	ctx context.Context, knowledgeID string, attempt int,
) ([]types.KnowledgeProcessingSpan, error) {
	return t.repo.ListByAttempt(ctx, knowledgeID, attempt)
}

// FailPreparedSpanRetry closes the exact fresh owner and consumes its durable
// dispatch row in one repository transaction. It is an optional retry-only
// capability so the general SpanTracker contract remains unchanged.
func (t *spanTracker) FailPreparedSpanRetry(
	ctx context.Context,
	prepared *types.KnowledgeSpanRetryPreparation,
	errorCode, errorMessage string,
) error {
	return t.repo.FailPreparedSpanRetry(ctx, prepared, errorCode, errorMessage)
}

// GetPreparedSpanRetry reads the exact attempt/span identity used by the
// guarded retry dispatcher. Name-based lookup is intentionally insufficient:
// a concurrent compensation may have terminalized this specific row while a
// second HTTP request still holds a stale pending preparation.
func (t *spanTracker) GetPreparedSpanRetry(
	ctx context.Context, knowledgeID string, attempt int, spanID string,
) (*types.KnowledgeProcessingSpan, error) {
	return t.repo.GetSpan(ctx, knowledgeID, attempt, spanID)
}

// touchKnowledgeHeartbeat advances knowledge.updated_at to the current
// wall-clock so the housekeeping sweep treats this row as actively
// progressing. Called on every span Begin/End/Fail/Skip — the cost is
// one indexed UPDATE per transition (≤ a few dozen per knowledge), which
// is dwarfed by the work the stages themselves do.
//
// touchKnowledgeHeartbeat advances knowledge.updated_at to the current
// wall-clock so the housekeeping sweep treats this row as actively
// progressing. Called on root / stage span transitions only — subspan and
// generation transitions skip this side-channel because:
//
//   - The spans table itself is updated on every transition, and the
//     housekeeping sweep already reads MAX(spans.updated_at) per
//     knowledge, so subspan progress is observable without poking the
//     parent row.
//   - A multimodal stage with N images would produce 2*N+ extra UPDATEs
//     on the same hot row (Begin+End per image plus retries), which we
//     observed contributing to row-level contention under bursty
//     uploads.
//
// Best-effort. We deliberately do NOT bump status here: the parse_status
// column remains under the pipeline's control. Only the timestamp gets
// nudged, which is exactly what housekeeping reads as the fallback.
func (t *spanTracker) touchKnowledgeHeartbeat(ctx context.Context, knowledgeID, kind string) {
	if t.db == nil || knowledgeID == "" {
		return
	}
	// Subspan / generation transitions are observable through the spans
	// table directly; skip the parent-row UPDATE to avoid write
	// amplification on fan-out workloads.
	if kind != types.SpanKindRoot && kind != types.SpanKindStage {
		return
	}
	if err := t.db.WithContext(ctx).Model(&types.Knowledge{}).
		Where("id = ?", knowledgeID).
		Update("updated_at", time.Now()).Error; err != nil {
		// Don't log every failure — heartbeat is best-effort and
		// noisy logs would drown out real errors. Single line at
		// warn level is enough for ops to spot a chronic outage.
		logger.Warnf(ctx, "[SpanTracker] heartbeat update failed kid=%s: %v", knowledgeID, err)
	}
}

func newSpanID() string {
	// Stripping the dashes saves 4 bytes per row — JSON parsers don't
	// care, and operators paste these into queries / Langfuse where a
	// hex-only ID is friendlier.
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

func (t *spanTracker) recordStart(spanID string, at time.Time) {
	t.startsMu.Lock()
	t.starts[spanID] = at
	t.startsMu.Unlock()
}

func (t *spanTracker) takeStart(spanID string) (time.Time, bool) {
	t.startsMu.Lock()
	defer t.startsMu.Unlock()
	v, ok := t.starts[spanID]
	if ok {
		delete(t.starts, spanID)
	}
	return v, ok
}

func (t *spanTracker) OpenAttempt(ctx context.Context, knowledgeID, langfuseTraceID string) (*Span, int, error) {
	now := time.Now()
	rootID := newSpanID()
	meta := types.JSONMap{}
	if langfuseTraceID != "" {
		// The frontend renders a "open in Langfuse" link from this.
		meta["langfuse_trace_id"] = langfuseTraceID
	}
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: knowledgeID,
		SpanID:      rootID,
		Name:        "knowledge_processing",
		Kind:        types.SpanKindRoot,
		Status:      types.SpanStatusRunning,
		Metadata:    meta,
		StartedAt:   &now,
	}
	attempt, err := t.repo.OpenAttempt(ctx, row)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] OpenAttempt failed kid=%s: %v", knowledgeID, err)
		return nil, 0, err
	}
	t.recordStart(rootID, now)
	t.touchKnowledgeHeartbeat(ctx, knowledgeID, types.SpanKindRoot)
	return &Span{
		KnowledgeID: knowledgeID,
		Attempt:     attempt,
		SpanID:      rootID,
		Name:        "knowledge_processing",
		Kind:        types.SpanKindRoot,
		Status:      types.SpanStatusRunning,
		StartedAt:   now,
	}, attempt, nil
}

func (t *spanTracker) LatestAttempt(ctx context.Context, knowledgeID string) int {
	n, err := t.LatestAttemptStrict(ctx, knowledgeID)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] LatestAttempt failed kid=%s: %v", knowledgeID, err)
		return 0
	}
	return n
}

func (t *spanTracker) LatestAttemptStrict(ctx context.Context, knowledgeID string) (int, error) {
	return t.repo.LatestAttempt(ctx, knowledgeID)
}

func (t *spanTracker) LookupAttemptRoot(ctx context.Context, knowledgeID string, attempt int) (*Span, error) {
	if knowledgeID == "" || attempt <= 0 {
		return nil, nil
	}
	rows, err := t.repo.ListByAttempt(ctx, knowledgeID, attempt)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] LookupAttemptRoot failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return nil, err
	}
	for i := range rows {
		row := rows[i]
		if row.Kind != types.SpanKindRoot {
			continue
		}
		startedAt := time.Time{}
		if row.StartedAt != nil {
			startedAt = *row.StartedAt
		}
		return &Span{
			KnowledgeID: row.KnowledgeID, Attempt: row.Attempt, SpanID: row.SpanID,
			ParentSpanID: row.ParentSpanID, Name: row.Name, Kind: row.Kind,
			Status: row.Status, Input: row.Input, StartedAt: startedAt,
		}, nil
	}
	return nil, nil
}

func (t *spanTracker) BeginStage(ctx context.Context, knowledgeID string, attempt int, stage string, input types.JSONMap) *Span {
	if knowledgeID == "" || stage == "" {
		return nil
	}
	// Find root span — we need its span_id as parent for stage rows. We
	// also reuse this scan to detect an existing row for the same stage
	// name in this attempt: re-entry (asynq retry, double-call from
	// adjacent code paths) MUST NOT create a second row, otherwise the
	// timeline shows two segments for the same stage and LookupStage
	// becomes ambiguous.
	rows, err := t.repo.ListByAttempt(ctx, knowledgeID, attempt)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] BeginStage list failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return nil
	}
	var (
		rootID   string
		existing *types.KnowledgeProcessingSpan
	)
	for i := range rows {
		r := rows[i]
		if r.Kind == types.SpanKindRoot && rootID == "" {
			rootID = r.SpanID
		}
		if r.Kind == types.SpanKindStage && r.Name == stage {
			cp := r
			existing = &cp
		}
	}
	if rootID == "" {
		// Pipeline started before tracker was wired (legacy data,
		// or the OpenAttempt repo write failed). Synthesize a
		// rootless stage so we still record SOMETHING.
		logger.Warnf(ctx, "[SpanTracker] BeginStage: no root for kid=%s attempt=%d, recording rootless",
			knowledgeID, attempt)
	}
	now := time.Now()
	// Re-entry path: keep the original span_id so any subspan that
	// already references it stays attached. Reset state to running and
	// refresh started_at only when this attempt is still the latest open root.
	// The repository check is serialized with OpenAttempt, preventing a late
	// retry from reopening a stage cancelled by a newer rebuild.
	if existing != nil {
		effectiveInput := input
		if effectiveInput == nil {
			effectiveInput = existing.Input
		}
		row := &types.KnowledgeProcessingSpan{
			KnowledgeID:  existing.KnowledgeID,
			Attempt:      existing.Attempt,
			SpanID:       existing.SpanID,
			ParentSpanID: existing.ParentSpanID,
			Name:         existing.Name,
			Kind:         existing.Kind,
			Status:       types.SpanStatusRunning,
			Input:        effectiveInput,
			Output:       nil,
			StartedAt:    &now,
			FinishedAt:   nil,
			DurationMs:   0,
		}
		accepted, err := t.repo.UpsertRunningStageIfCurrent(ctx, row)
		if err != nil {
			logger.Warnf(ctx, "[SpanTracker] BeginStage re-enter failed kid=%s stage=%s: %v",
				knowledgeID, stage, err)
			return nil
		}
		if !accepted {
			logger.Infof(ctx, "[SpanTracker] BeginStage ignored stale attempt kid=%s attempt=%d stage=%s",
				knowledgeID, attempt, stage)
			return nil
		}
		t.recordStart(existing.SpanID, now)
		t.touchKnowledgeHeartbeat(ctx, knowledgeID, types.SpanKindStage)
		return &Span{
			KnowledgeID:  existing.KnowledgeID,
			Attempt:      existing.Attempt,
			SpanID:       existing.SpanID,
			ParentSpanID: existing.ParentSpanID,
			Name:         existing.Name,
			Kind:         existing.Kind,
			Status:       types.SpanStatusRunning,
			Input:        effectiveInput,
			StartedAt:    now,
		}
	}
	id := newSpanID()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  knowledgeID,
		Attempt:      attempt,
		SpanID:       id,
		ParentSpanID: rootID,
		Name:         stage,
		Kind:         types.SpanKindStage,
		Status:       types.SpanStatusRunning,
		Input:        input,
		StartedAt:    &now,
	}
	accepted, err := t.repo.UpsertRunningStageIfCurrent(ctx, row)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] BeginStage failed kid=%s stage=%s: %v",
			knowledgeID, stage, err)
		return nil
	}
	if !accepted {
		logger.Infof(ctx, "[SpanTracker] BeginStage ignored stale attempt kid=%s attempt=%d stage=%s",
			knowledgeID, attempt, stage)
		return nil
	}
	t.recordStart(id, now)
	t.touchKnowledgeHeartbeat(ctx, knowledgeID, types.SpanKindStage)
	return &Span{
		KnowledgeID:  knowledgeID,
		Attempt:      attempt,
		SpanID:       id,
		ParentSpanID: rootID,
		Name:         stage,
		Kind:         types.SpanKindStage,
		Status:       types.SpanStatusRunning,
		StartedAt:    now,
	}
}

func (t *spanTracker) UpdateSpanInput(ctx context.Context, span *Span, input types.JSONMap) error {
	if span == nil || input == nil {
		return nil
	}
	if err := t.repo.UpdateInput(ctx, span.KnowledgeID, span.Attempt, span.SpanID, input); err != nil {
		logger.Warnf(ctx, "[SpanTracker] UpdateSpanInput failed kid=%s attempt=%d span=%s: %v",
			span.KnowledgeID, span.Attempt, span.SpanID, err)
		return err
	}
	span.Input = input
	return nil
}

func (t *spanTracker) BeginSubSpan(ctx context.Context, parent *Span, name, kind string, input types.JSONMap) *Span {
	if parent == nil || name == "" {
		return nil
	}
	name = fitSpanName(name)
	if kind != types.SpanKindGeneration && kind != types.SpanKindSubSpan {
		kind = types.SpanKindSubSpan
	}
	if pending, err := t.repo.ClaimLatestPendingByName(
		ctx, parent.KnowledgeID, parent.Attempt, parent.SpanID, name, input,
	); err != nil {
		logger.Warnf(ctx, "[SpanTracker] claim queued %s failed: %v", name, err)
		return nil
	} else if pending != nil {
		startedAt := time.Now()
		if pending.StartedAt != nil {
			startedAt = *pending.StartedAt
		}
		t.recordStart(pending.SpanID, startedAt)
		return &Span{
			KnowledgeID: pending.KnowledgeID, Attempt: pending.Attempt, SpanID: pending.SpanID,
			ParentSpanID: pending.ParentSpanID, Name: pending.Name, Kind: pending.Kind,
			Status: types.SpanStatusRunning, Input: pending.Input, StartedAt: startedAt,
		}
	}
	// Asynq retry / server restart can re-run the same handler while the
	// previous invocation's span is still status=running (worker died
	// without EndSpan). Cancel same-name open rows so the UI shows one
	// logical subspan per (attempt, name) instead of duplicate stripes.
	if _, err := t.repo.CancelOpenSpansByName(ctx, parent.KnowledgeID, parent.Attempt, name,
		"TASK_SUPERSEDED", "superseded by a new run of the same subtask"); err != nil {
		logger.Warnf(ctx, "[SpanTracker] supersede %s before BeginSubSpan failed: %v", name, err)
	}
	now := time.Now()
	id := newSpanID()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  parent.KnowledgeID,
		Attempt:      parent.Attempt,
		SpanID:       id,
		ParentSpanID: parent.SpanID,
		Name:         name,
		Kind:         kind,
		Status:       types.SpanStatusRunning,
		Input:        input,
		StartedAt:    &now,
	}
	if err := t.repo.Upsert(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] BeginSubSpan failed parent=%s name=%s: %v",
			parent.SpanID, name, err)
		return nil
	}
	t.recordStart(id, now)
	t.touchKnowledgeHeartbeat(ctx, parent.KnowledgeID, kind)
	return &Span{
		KnowledgeID:  parent.KnowledgeID,
		Attempt:      parent.Attempt,
		SpanID:       id,
		ParentSpanID: parent.SpanID,
		Name:         name,
		Kind:         kind,
		Status:       types.SpanStatusRunning,
		Input:        input,
		StartedAt:    now,
	}
}

func (t *spanTracker) QueueSubSpan(ctx context.Context, parent *Span, name, kind string, input types.JSONMap) *Span {
	if parent == nil || name == "" {
		return nil
	}
	name = fitSpanName(name)
	if kind != types.SpanKindGeneration && kind != types.SpanKindSubSpan {
		kind = types.SpanKindSubSpan
	}
	id := newSpanID()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID: parent.KnowledgeID, Attempt: parent.Attempt, SpanID: id,
		ParentSpanID: parent.SpanID, Name: name, Kind: kind,
		Status: types.SpanStatusPending, Input: input,
	}
	queued, err := t.repo.QueuePendingSpan(ctx, row)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] QueueSubSpan failed parent=%s name=%s: %v",
			parent.SpanID, name, err)
		return nil
	}
	return &Span{
		KnowledgeID: queued.KnowledgeID, Attempt: queued.Attempt, SpanID: queued.SpanID,
		ParentSpanID: queued.ParentSpanID, Name: queued.Name, Kind: queued.Kind, Status: queued.Status,
		Input: queued.Input,
	}
}

func (t *spanTracker) SettleQuestionGroup(ctx context.Context, knowledgeID string, attempt int) {
	if knowledgeID == "" || attempt <= 0 {
		return
	}
	if err := t.settleProcessingOutcome(ctx, knowledgeID, attempt); err != nil {
		logger.Warnf(ctx, "[SpanTracker] settle question group failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
	}
}

func (t *spanTracker) SettlePostProcessTree(ctx context.Context, knowledgeID string, attempt int) {
	if knowledgeID == "" || attempt <= 0 {
		return
	}
	if err := t.settleProcessingOutcome(ctx, knowledgeID, attempt); err != nil {
		logger.Warnf(ctx, "[SpanTracker] settle postprocess tree failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
	}
}

// SettlePostProcessTreeStrict exposes the reducer error to durability
// boundaries (notably Wiki pending-row acknowledgement). Callers that are only
// emitting best-effort observer updates continue using SettlePostProcessTree.
func (t *spanTracker) SettlePostProcessTreeStrict(ctx context.Context, knowledgeID string, attempt int) error {
	if knowledgeID == "" || attempt <= 0 {
		return errors.New("settle postprocess tree: knowledge id and attempt are required")
	}
	return t.settleProcessingOutcome(ctx, knowledgeID, attempt)
}

func (t *spanTracker) SettleWikiPendingOpStrict(
	ctx context.Context,
	knowledgeID string,
	attempt int,
	pendingIDs []int64,
	deadLetter *types.TaskDeadLetter,
	owner *types.TaskClaimOwner,
) error {
	if knowledgeID == "" || attempt <= 0 || len(pendingIDs) == 0 {
		return errors.New("settle wiki pending op: knowledge id, attempt and pending rows are required")
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	var lastErr error
	for try := 1; try <= spanTerminalWriteAttempts; try++ {
		settleCtx, cancel := context.WithTimeout(base, spanTerminalWriteTimeout)
		lastErr = t.repo.SettleWikiPendingOp(
			settleCtx, knowledgeID, attempt, pendingIDs, deadLetter, owner,
		)
		cancel()
		if lastErr == nil {
			return nil
		}
		if try < spanTerminalWriteAttempts {
			time.Sleep(time.Duration(try) * 50 * time.Millisecond)
		}
	}
	return fmt.Errorf("wiki pending settlement failed after %d attempts: %w",
		spanTerminalWriteAttempts, lastErr)
}

func (t *spanTracker) settleProcessingOutcome(ctx context.Context, knowledgeID string, attempt int) error {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	var lastErr error
	for try := 1; try <= spanTerminalWriteAttempts; try++ {
		settleCtx, cancel := context.WithTimeout(base, spanTerminalWriteTimeout)
		lastErr = t.repo.SettleProcessingOutcome(settleCtx, knowledgeID, attempt)
		cancel()
		if lastErr == nil {
			return nil
		}
		if try < spanTerminalWriteAttempts {
			time.Sleep(time.Duration(try) * 50 * time.Millisecond)
		}
	}
	return fmt.Errorf("processing settlement failed after %d attempts: %w",
		spanTerminalWriteAttempts, lastErr)
}

func postProcessExpectedBranches(input types.JSONMap) ([]string, bool) {
	if input == nil {
		return nil, false
	}
	raw, ok := input["expected_branches"]
	if !ok {
		return nil, false
	}
	seen := make(map[string]struct{})
	branches := make([]string, 0)
	appendName := func(value any) {
		name, ok := value.(string)
		if !ok {
			return
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		branches = append(branches, name)
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
	return branches, true
}

func isCountedPostProcessBranch(name string) bool {
	return name == "postprocess.summary" ||
		name == postprocessQuestionGroupSpanName ||
		name == "postprocess.wiki" ||
		strings.HasPrefix(name, "postprocess.graph.chunk[")
}

func positiveJSONInt(value any) int {
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

func (t *spanTracker) RecordGeneration(ctx context.Context, record types.KnowledgeGenerationUsage) {
	if record.KnowledgeID == "" || record.Attempt <= 0 {
		return
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	ctx = recordCtx
	spanID := record.SpanID
	if spanID == "" {
		spanID = uuid.NewString()
	}
	parent := t.LookupSpanByName(ctx, record.KnowledgeID, record.Attempt, record.Stage)
	if parent == nil {
		parent = t.LookupStage(ctx, record.KnowledgeID, record.Attempt, canonicalStageForUsage(record.Stage))
	}
	parentID := ""
	if parent != nil {
		parentID = parent.SpanID
	}

	startedAt := record.StartedAt
	finishedAt := record.FinishedAt
	if startedAt.IsZero() {
		if finishedAt.IsZero() {
			startedAt = time.Now()
		} else {
			startedAt = finishedAt
		}
	}
	status := record.Status
	if status == "" {
		status = types.SpanStatusDone
	}
	open := status == types.SpanStatusPending || status == types.SpanStatusRunning
	var finishedAtPtr *time.Time
	var durationMs int64
	if !open {
		if finishedAt.IsZero() {
			finishedAt = time.Now()
		}
		finishedAtPtr = &finishedAt
		durationMs = finishedAt.Sub(startedAt).Milliseconds()
		if durationMs < 0 {
			durationMs = 0
		}
	}
	name := record.Name
	if name == "" {
		name = "model.generation"
	}
	usage := types.JSONMap{
		"input_tokens":       record.InputTokens,
		"output_tokens":      record.OutputTokens,
		"total_tokens":       record.TotalTokens,
		"cache_read_tokens":  record.CacheReadTokens,
		"cache_write_tokens": record.CacheWriteTokens,
		"cache_miss_tokens":  record.CacheMissTokens,
		"unit":               record.Unit,
		"available":          record.UsageAvailable,
		"estimated":          record.Estimated,
	}
	metadata := types.JSONMap{
		"langfuse_trace_id": record.TraceID,
		"processing_stage":  record.Stage,
		"task_type":         record.TaskType,
		"generation_name":   record.Name,
		"model_type":        record.ModelType,
		"model_id":          record.ModelID,
		"model_name":        record.ModelName,
		"call_purpose":      record.Purpose,
		"usage":             usage,
	}
	if len(record.Progress) > 0 {
		metadata["stream_progress"] = record.Progress
	}
	var output types.JSONMap
	if len(record.Output) > 0 {
		output = make(types.JSONMap, len(record.Output)+1)
		for key, value := range record.Output {
			output[key] = value
		}
		output["usage"] = usage
	} else if record.UsageAvailable {
		output = types.JSONMap{"usage": usage}
	}
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  record.KnowledgeID,
		Attempt:      record.Attempt,
		SpanID:       spanID,
		ParentSpanID: parentID,
		Name:         fitSpanName(name),
		Kind:         types.SpanKindGeneration,
		Status:       status,
		Output:       output,
		Metadata:     metadata,
		ErrorMessage: record.ErrorMessage,
		StartedAt:    &startedAt,
		FinishedAt:   finishedAtPtr,
		DurationMs:   durationMs,
	}
	if err := t.repo.Upsert(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] RecordGeneration failed kid=%s attempt=%d span=%s: %v",
			record.KnowledgeID, record.Attempt, spanID, err)
	}
}

func canonicalStageForUsage(stage string) string {
	switch {
	case stage == types.StageDocReader || strings.HasPrefix(stage, types.StageDocReader+"."):
		return types.StageDocReader
	case stage == types.StageChunking || strings.HasPrefix(stage, types.StageChunking+"."):
		return types.StageChunking
	case stage == types.StageEmbedding || strings.HasPrefix(stage, types.StageEmbedding+"."):
		return types.StageEmbedding
	case stage == types.StageMultimodal || strings.HasPrefix(stage, types.StageMultimodal+"."):
		return types.StageMultimodal
	case stage == types.StagePostProcess || strings.HasPrefix(stage, types.StagePostProcess+"."):
		return types.StagePostProcess
	default:
		return ""
	}
}

func (t *spanTracker) EndSpan(ctx context.Context, span *Span, output types.JSONMap) {
	if span == nil {
		return
	}
	now := time.Now()
	if span.StartedAt.IsZero() {
		span.StartedAt = now
	}
	dur := durationSince(t, span, now)
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  span.KnowledgeID,
		Attempt:      span.Attempt,
		SpanID:       span.SpanID,
		ParentSpanID: span.ParentSpanID,
		Name:         span.Name,
		Kind:         span.Kind,
		Status:       types.SpanStatusDone,
		Output:       output,
		StartedAt:    &span.StartedAt,
		FinishedAt:   &now,
		DurationMs:   dur,
	}
	if err := t.upsertTerminalSpan(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] EndSpan failed span=%s: %v", span.SpanID, err)
	}
	t.touchKnowledgeHeartbeat(ctx, span.KnowledgeID, span.Kind)
}

// upsertTerminalSpan detaches completion writes from an expired worker
// context. Wiki and other long-running post-process tasks commonly finish at
// the same instant as their asynq deadline; persisting through that cancelled
// context leaves a parent span running even though every child is terminal.
// Keep the tracker best-effort, but give the idempotent UPSERT three bounded
// attempts before conceding the write.
func (t *spanTracker) upsertTerminalSpan(ctx context.Context, row *types.KnowledgeProcessingSpan) error {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	var lastErr error
	for attempt := 1; attempt <= spanTerminalWriteAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(base, spanTerminalWriteTimeout)
		lastErr = t.repo.Upsert(writeCtx, row)
		cancel()
		if lastErr == nil {
			return nil
		}
		if attempt < spanTerminalWriteAttempts {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}
	}
	return fmt.Errorf("terminal span upsert failed after %d attempts: %w", spanTerminalWriteAttempts, lastErr)
}

func (t *spanTracker) FailSpan(ctx context.Context, span *Span, errorCode, errorMessage string, errorDetail error) {
	if span == nil {
		return
	}
	now := time.Now()
	if span.StartedAt.IsZero() {
		span.StartedAt = now
	}
	dur := durationSince(t, span, now)
	detail := ""
	if errorDetail != nil {
		detail = errorDetail.Error()
		if len(detail) > 8192 {
			detail = detail[:8192]
		}
	}
	if len(errorMessage) > 1024 {
		errorMessage = errorMessage[:1024]
	}
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  span.KnowledgeID,
		Attempt:      span.Attempt,
		SpanID:       span.SpanID,
		ParentSpanID: span.ParentSpanID,
		Name:         span.Name,
		Kind:         span.Kind,
		Status:       types.SpanStatusFailed,
		ErrorCode:    strings.TrimSpace(errorCode),
		ErrorMessage: errorMessage,
		ErrorDetail:  detail,
		StartedAt:    &span.StartedAt,
		FinishedAt:   &now,
		DurationMs:   dur,
	}
	if err := t.upsertTerminalSpan(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] FailSpan failed span=%s: %v", span.SpanID, err)
	}
	terminalCtx, cancel := detachedSpanTerminalContext(ctx)
	defer cancel()
	// Cascade: anything downstream of this span gets cancelled. The
	// reason string is what the UI surfaces under each cancelled
	// child's tooltip — keep it short and human.
	reason := "upstream " + span.Name + " failed"
	if errorCode != "" {
		reason = reason + " (" + errorCode + ")"
	}
	if _, err := t.repo.CancelDescendants(terminalCtx, span.KnowledgeID, span.Attempt, span.SpanID, reason); err != nil {
		logger.Warnf(ctx, "[SpanTracker] cancel descendants failed span=%s: %v", span.SpanID, err)
	}
	// For STAGE failures, also cascade to dependent stages declared
	// in StageDependencies (those are siblings, not descendants).
	if span.Kind == types.SpanKindStage {
		t.cascadeDependentStages(terminalCtx, span, reason)
		// Any failure in a MAIN pipeline stage means the attempt is
		// done — the parse cannot succeed past this point. Close the
		// root span as failed so the UI doesn't show "进行中" forever.
		// Optional downstream stages (summary/question/wiki/graph) do
		// not poison the attempt: they can fail without invalidating
		// the parsed document.
		if isMainPipelineStage(span.Name) {
			t.FinalizeAttempt(terminalCtx, span.KnowledgeID, span.Attempt,
				types.SpanStatusFailed, nil, errorCode, errorMessage)
		}
	}
	t.touchKnowledgeHeartbeat(terminalCtx, span.KnowledgeID, span.Kind)
}

func (t *spanTracker) SkipSpan(ctx context.Context, span *Span, reason string) {
	if span == nil {
		return
	}
	now := time.Now()
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  span.KnowledgeID,
		Attempt:      span.Attempt,
		SpanID:       span.SpanID,
		ParentSpanID: span.ParentSpanID,
		Name:         span.Name,
		Kind:         span.Kind,
		Status:       types.SpanStatusSkipped,
		ErrorMessage: reason,
		StartedAt:    &span.StartedAt,
		FinishedAt:   &now,
	}
	if err := t.upsertTerminalSpan(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] SkipSpan failed span=%s: %v", span.SpanID, err)
	}
	terminalCtx, cancel := detachedSpanTerminalContext(ctx)
	defer cancel()
	t.touchKnowledgeHeartbeat(terminalCtx, span.KnowledgeID, span.Kind)
}

func detachedSpanTerminalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, spanTerminalWriteTimeout)
}

func (t *spanTracker) LookupStage(ctx context.Context, knowledgeID string, attempt int, stage string) *Span {
	rows, err := t.repo.ListByAttempt(ctx, knowledgeID, attempt)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] LookupStage list failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return nil
	}
	for i := range rows {
		r := rows[i]
		if r.Kind != types.SpanKindStage || r.Name != stage {
			continue
		}
		started := time.Time{}
		if r.StartedAt != nil {
			started = *r.StartedAt
		}
		return &Span{
			KnowledgeID:  r.KnowledgeID,
			Attempt:      r.Attempt,
			SpanID:       r.SpanID,
			ParentSpanID: r.ParentSpanID,
			Name:         r.Name,
			Kind:         r.Kind,
			Status:       r.Status,
			Input:        r.Input,
			StartedAt:    started,
		}
	}
	return nil
}

func (t *spanTracker) LookupSpanByName(ctx context.Context, knowledgeID string, attempt int, name string) *Span {
	span, err := t.LookupSpanByNameStrict(ctx, knowledgeID, attempt, name)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] LookupSpanByName list failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return nil
	}
	return span
}

// LookupSpanByNameStrict is the durability-boundary counterpart to
// LookupSpanByName. It propagates repository failures so a queue row is never
// acknowledged merely because a best-effort lookup collapsed an error to nil.
func (t *spanTracker) LookupSpanByNameStrict(
	ctx context.Context, knowledgeID string, attempt int, name string,
) (*Span, error) {
	if name == "" || knowledgeID == "" || attempt <= 0 {
		return nil, errors.New("lookup span by name: knowledge id, attempt, and name are required")
	}
	name = fitSpanName(name)
	rows, err := t.repo.ListByAttempt(ctx, knowledgeID, attempt)
	if err != nil {
		return nil, fmt.Errorf("lookup span by name: %w", err)
	}
	for i := len(rows) - 1; i >= 0; i-- {
		r := rows[i]
		if r.Name != name {
			continue
		}
		started := time.Time{}
		if r.StartedAt != nil {
			started = *r.StartedAt
		}
		return &Span{
			KnowledgeID:  r.KnowledgeID,
			Attempt:      r.Attempt,
			SpanID:       r.SpanID,
			ParentSpanID: r.ParentSpanID,
			Name:         r.Name,
			Kind:         r.Kind,
			Status:       r.Status,
			Input:        r.Input,
			StartedAt:    started,
		}, nil
	}
	return nil, nil
}

// cascadeDependentStages flips downstream STAGE rows to "cancelled" using
// types.StageDependencies. Without this, a Chunking failure leaves
// Embedding / Multimodal as "pending" forever, even though they cannot
// possibly run. After flipping a dependent stage we ALSO cascade-cancel
// any subspan/generation that already attached to it (e.g. an embedding
// batch that started before the chunking failure was observed) — without
// this second walk those subspans would remain in pending/running and
// surface as orphan spinners under a cancelled parent.
func (t *spanTracker) cascadeDependentStages(ctx context.Context, failedStage *Span, reason string) {
	rows, err := t.repo.ListByAttempt(ctx, failedStage.KnowledgeID, failedStage.Attempt)
	if err != nil {
		return
	}
	dependents := stagesDependingOn(failedStage.Name)
	if len(dependents) == 0 {
		return
	}
	now := time.Now()
	for _, row := range rows {
		if row.Kind != types.SpanKindStage {
			continue
		}
		if row.Status != types.SpanStatusPending && row.Status != types.SpanStatusRunning {
			continue
		}
		if !contains(dependents, row.Name) {
			continue
		}
		updated := row // copy
		updated.Status = types.SpanStatusCancelled
		updated.ErrorCode = "UPSTREAM_FAILED"
		updated.ErrorMessage = reason
		updated.FinishedAt = &now
		if err := t.repo.Upsert(ctx, &updated); err != nil {
			logger.Warnf(ctx, "[SpanTracker] cascade dependent stage %s: %v", row.Name, err)
			continue
		}
		// Recurse into the cascaded stage's own subtree so any
		// in-flight subspan/generation is cancelled too. The
		// repo-level walk is iterative and cheap (small fan-out).
		if _, err := t.repo.CancelDescendants(ctx, row.KnowledgeID, row.Attempt, row.SpanID, reason); err != nil {
			logger.Warnf(ctx, "[SpanTracker] cascade descendants of dependent %s: %v", row.Name, err)
		}
	}
}

// stagesDependingOn returns the transitive closure of stages that have
// `stage` as an upstream dependency (direct or indirect). Computed by
// reverse-walking StageDependencies; the result is bounded to 5 since
// AllStages has five members, so a naive O(N²) walk is fine.
func stagesDependingOn(stage string) []string {
	var out []string
	seen := map[string]bool{}
	frontier := []string{stage}
	for len(frontier) > 0 {
		var next []string
		for _, candidate := range types.AllStages {
			if seen[candidate] {
				continue
			}
			deps := types.StageDependencies[candidate]
			for _, d := range deps {
				if contains(frontier, d) {
					seen[candidate] = true
					out = append(out, candidate)
					next = append(next, candidate)
					break
				}
			}
		}
		frontier = next
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// isMainPipelineStage reports whether stage is one of the 5 mandatory
// pipeline stages (docreader / chunking / embedding / multimodal /
// postprocess). A failure in any of these terminally invalidates the
// attempt and must close the root as failed. Optional downstream stages
// added later (summary, question, wiki, graph) do NOT match — those
// can fail individually without poisoning the parse result.
func isMainPipelineStage(name string) bool {
	for _, s := range types.AllStages {
		if s == name {
			return true
		}
	}
	return false
}

// durationSince computes elapsed ms preferring the in-process cache;
// falls back to the *Span's StartedAt for cross-process callers.
func durationSince(t *spanTracker, span *Span, now time.Time) int64 {
	if start, ok := t.takeStart(span.SpanID); ok {
		return now.Sub(start).Milliseconds()
	}
	if !span.StartedAt.IsZero() {
		return now.Sub(span.StartedAt).Milliseconds()
	}
	return 0
}

// FinalizeAttempt closes the root span for (knowledgeID, attempt). The
// pipeline calls this from two places: the success orchestrator
// (PostProcess) when the parse reaches Completed, and FailSpan when a
// MAIN stage fails terminally. Without this, the root row created by
// OpenAttempt would stay in "running" forever even after parse_status
// flips to completed/failed — operators would see a perpetually
// "running" trace despite a terminal knowledge state.
//
// Idempotent: re-closing a root that's already done/failed is a no-op
// so callers from different paths (success vs. cascade-fail vs.
// dead-letter) don't have to coordinate. We deliberately avoid the
// recordStart cache here (cross-process callers won't have it); we
// recompute duration from the persisted row's started_at.
func (t *spanTracker) FinalizeAttempt(ctx context.Context, knowledgeID string, attempt int, status string,
	output types.JSONMap, errorCode, errorMessage string,
) {
	if knowledgeID == "" || attempt <= 0 {
		return
	}
	if status == "" {
		status = types.SpanStatusDone
	}
	rows, err := t.repo.ListByAttempt(ctx, knowledgeID, attempt)
	if err != nil {
		logger.Warnf(ctx, "[SpanTracker] FinalizeAttempt list failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return
	}
	var root *types.KnowledgeProcessingSpan
	for i := range rows {
		if rows[i].Kind == types.SpanKindRoot {
			cp := rows[i]
			root = &cp
			break
		}
	}
	if root == nil {
		// No root means nothing to close — likely an attempt that
		// predates the tracker or whose OpenAttempt write failed.
		return
	}
	if root.Status == types.SpanStatusDone || root.Status == types.SpanStatusFailed ||
		root.Status == types.SpanStatusCancelled || root.Status == types.SpanStatusSkipped {
		return
	}
	now := time.Now()
	var started time.Time
	if root.StartedAt != nil {
		started = *root.StartedAt
	}
	dur := int64(0)
	if !started.IsZero() {
		dur = now.Sub(started).Milliseconds()
	}
	if len(errorMessage) > 1024 {
		errorMessage = errorMessage[:1024]
	}
	row := &types.KnowledgeProcessingSpan{
		KnowledgeID:  root.KnowledgeID,
		Attempt:      root.Attempt,
		SpanID:       root.SpanID,
		ParentSpanID: root.ParentSpanID,
		Name:         root.Name,
		Kind:         root.Kind,
		Status:       status,
		Input:        root.Input,
		Output:       output,
		Metadata:     root.Metadata,
		ErrorCode:    strings.TrimSpace(errorCode),
		ErrorMessage: errorMessage,
		StartedAt:    root.StartedAt,
		FinishedAt:   &now,
		DurationMs:   dur,
	}
	if err := t.repo.Upsert(ctx, row); err != nil {
		logger.Warnf(ctx, "[SpanTracker] FinalizeAttempt upsert failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		return
	}
	t.touchKnowledgeHeartbeat(ctx, knowledgeID, types.SpanKindRoot)
}

// AbortAttempt is the user-cancel counterpart to FinalizeAttempt. It
// flips every still-running / still-pending span for this attempt to
// cancelled — regardless of tree position — and then closes the root.
//
// Why a flat sweep instead of CancelDescendants' BFS: fan-out stages
// (e.g. 多模态识别) call EndSpan on the stage as soon as they finish
// DISPATCHING their async per-image work, so by the time the user
// hits cancel the stage row is already status=done but its image[*]
// children are still status=running. A BFS that stops at terminal
// parents would orphan those leaves. The flat sweep doesn't care
// about the tree shape — anything not-yet-terminal gets flipped.
func (t *spanTracker) AbortAttempt(ctx context.Context, knowledgeID string, attempt int,
	errorCode, errorMessage, reason string,
) {
	if knowledgeID == "" || attempt <= 0 {
		return
	}
	if reason == "" {
		reason = "user cancelled"
	}
	if errorCode == "" {
		errorCode = "USER_CANCELLED"
	}
	if n, err := t.repo.CancelAllOpenSpans(ctx, knowledgeID, attempt, errorCode, reason); err != nil {
		logger.Warnf(ctx, "[SpanTracker] AbortAttempt sweep failed kid=%s attempt=%d: %v",
			knowledgeID, attempt, err)
		// Fall through to FinalizeAttempt anyway — closing the root
		// is more important than perfectly closing every child.
	} else if n > 0 {
		logger.Infof(ctx,
			"[SpanTracker] AbortAttempt swept %d open span(s) for kid=%s attempt=%d",
			n, knowledgeID, attempt)
	}
	t.FinalizeAttempt(ctx, knowledgeID, attempt,
		types.SpanStatusCancelled, nil, errorCode, errorMessage)
}

// noopSpanTracker collapses every method to a no-op for tests/lite.
type noopSpanTracker struct{}

func (noopSpanTracker) OpenAttempt(_ context.Context, _, _ string) (*Span, int, error) {
	return nil, 0, nil
}
func (noopSpanTracker) LatestAttempt(_ context.Context, _ string) int { return 0 }
func (noopSpanTracker) BeginStage(_ context.Context, _ string, _ int, _ string, _ types.JSONMap) *Span {
	return nil
}
func (noopSpanTracker) UpdateSpanInput(_ context.Context, _ *Span, _ types.JSONMap) error {
	return nil
}
func (noopSpanTracker) LookupAttemptRoot(_ context.Context, _ string, _ int) (*Span, error) {
	return nil, nil
}
func (noopSpanTracker) BeginSubSpan(_ context.Context, _ *Span, _, _ string, _ types.JSONMap) *Span {
	return nil
}
func (noopSpanTracker) QueueSubSpan(_ context.Context, _ *Span, _, _ string, _ types.JSONMap) *Span {
	return nil
}
func (noopSpanTracker) SettleQuestionGroup(_ context.Context, _ string, _ int)               {}
func (noopSpanTracker) SettlePostProcessTree(_ context.Context, _ string, _ int)             {}
func (noopSpanTracker) RecordGeneration(_ context.Context, _ types.KnowledgeGenerationUsage) {}
func (noopSpanTracker) EndSpan(_ context.Context, _ *Span, _ types.JSONMap)                  {}
func (noopSpanTracker) FailSpan(_ context.Context, _ *Span, _, _ string, _ error)            {}
func (noopSpanTracker) SkipSpan(_ context.Context, _ *Span, _ string)                        {}
func (noopSpanTracker) LookupStage(_ context.Context, _ string, _ int, _ string) *Span       { return nil }
func (noopSpanTracker) LookupSpanByName(_ context.Context, _ string, _ int, _ string) *Span {
	return nil
}
func (noopSpanTracker) FinalizeAttempt(_ context.Context, _ string, _ int, _ string, _ types.JSONMap, _, _ string) {
}
func (noopSpanTracker) AbortAttempt(_ context.Context, _ string, _ int, _, _, _ string) {}
