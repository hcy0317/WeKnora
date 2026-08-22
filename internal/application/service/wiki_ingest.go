package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/agent"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// ErrWikiIngestConcurrent is returned by the wiki ingest handler in Lite mode
// when another batch is already running for the same KB (the in-process
// liteLocks guard is held). The asynq/sync executor's RetryDelayFunc uses
// errors.Is on this sentinel to apply a short, fixed retry delay instead of
// exponential backoff, so the deferred batch retries promptly once the active
// one releases. Standard (Redis) mode no longer takes an exclusive per-KB
// lock (Phase 3) and never returns this.
var ErrWikiIngestConcurrent = errors.New("concurrent wiki task active")

const (
	// maxContentForWiki limits the document content sent to LLM for wiki generation
	maxContentForWiki = 32768

	// --- Phase 3: concurrent per-KB batches (standard/Redis mode) ---------
	//
	// Phase 3 removed the exclusive per-KB Redis "batch in progress" lock
	// (wiki:active:<kbID>). Standard mode now allows concurrent batches for
	// one KB, kept safe by row claiming (claimPendingList) plus per-slug
	// reduce locks (withSlugLock). Lite mode still serializes per KB via the
	// in-process liteLocks map.
	//
	// wikiClaimStaleAfter is how long a claimed-but-undrained ingest row
	// waits before another worker may re-claim it. The 15-minute margin lets a
	// task that used its full 90-minute budget owner-safely renew the claim before
	// entering detached publication/settlement; that renewed lease then covers
	// the complete durable tail without extending the task's user-visible limit.
	wikiClaimStaleAfter = WikiIngestTaskTimeout + 15*time.Minute

	// wikiSlugLockPrefix guards read-modify-write on a single shared wiki
	// page (entity/concept/summary/index) so two concurrent batches for the
	// same KB can't lost-update the same slug. Key: wiki:slug:{kbID}:{slug}.
	wikiSlugLockPrefix = "wiki:slug:"
	// wikiSlugLockTTL covers the complete durable task plus the detached page
	// commit. Owner tokens and compare-delete release make expiry/reacquisition
	// safe; a long LLM reduce cannot silently lose serialization after 5m.
	wikiSlugLockTTL = WikiIngestTaskTimeout + wikiPagePersistTimeout + 5*time.Minute
	// wikiSlugLockWait / Poll bound how long a reduce goroutine blocks
	// waiting for a contended slug before falling back to a best-effort
	// skip (matching the pre-existing reduce-failure semantics).
	wikiSlugLockWait = 2 * time.Minute
	wikiSlugLockPoll = 50 * time.Millisecond

	// --- Phase 4: per-KB in-flight cap (standard/Redis mode) --------------
	//
	// Since Phase 3 lets many batches for one KB run at once, a single KB's
	// bulk import could otherwise occupy the entire wiki pool and starve
	// other KBs. Each running batch reserves a slot in a per-KB Redis sorted
	// set (score = expiry). ZREMRANGEBYSCORE purges slots left behind by a
	// crashed worker, so the cap is self-healing without an explicit lock.
	//
	// wikiInflightPrefix keys the per-KB slot set: wiki:inflight:{kbID}.
	wikiInflightPrefix = "wiki:inflight:"
	// wikiInflightDefault is the fallback cap when WikiConfig.IngestMaxInflight
	// is unset. 4 leaves half of the default 8-worker wiki pool for other
	// KBs while still giving one KB solid parallelism.
	wikiInflightDefault = 4
	// wikiInflightTTL is how long a reserved slot survives without renewal.
	// MUST exceed wikiInflightRenew by a comfortable margin so a single
	// missed renew (GC pause, Redis blip) doesn't drop a live batch's slot.
	wikiInflightTTL = 90 * time.Second
	// wikiInflightRenew is how often a running batch bumps its slot's expiry.
	wikiInflightRenew = 30 * time.Second
	// wikiInflightBackoff delays the follow-up trigger when a batch is turned
	// away by the cap, so it retries after a slot plausibly frees.
	wikiInflightBackoff = 10 * time.Second

	// wikiIngestDelay is how long to wait after a document is added before
	// the batch task fires. Debounces rapid uploads.
	wikiIngestDelay = 30 * time.Second

	// wikiFollowUpDelay is the normal light debounce before a follow-up
	// batch drains the remaining pending rows. Standard mode already fans a
	// KB's backlog across concurrent claiming batches, so this is just a
	// short breather, not a lock-release wait.
	wikiFollowUpDelay = 5 * time.Second

	// wikiRateLimitBackoff is the (much longer) follow-up delay used when a
	// batch's failures were caused by upstream rate limiting (HTTP 429 /
	// quota). Retrying a rate-limited document at the normal 5s cadence just
	// throws more requests at an already-saturated rpm budget — every retry
	// re-issues extract + classify + summary calls for the doc — which keeps
	// the limiter tripped and drags the whole KB's ingest out. Backing off
	// gives the per-minute window time to reset before we try again.
	wikiRateLimitBackoff = 60 * time.Second

	// wikiMaxDocsPerBatch limits how many documents a single batch processes.
	// Prevents unbounded execution time. Remaining ops stay in
	// task_pending_ops and are picked up by the follow-up task.
	wikiMaxDocsPerBatch = 5

	// wikiMaxFailRetries is the maximum number of times a single document op
	// may be re-attempted via requeueFailedOps before it is permanently
	// archived to task_dead_letters. 5 retries ≈ five full batch cycles
	// (each with a ~30 s delay), giving transient LLM errors a fair chance
	// to recover without letting a persistently-broken doc clog the queue
	// indefinitely.
	wikiMaxFailRetries = 5

	// wikiIngestMaxRetry controls asynq retry budget for wiki:ingest tasks.
	// Keep this moderate: lock conflicts already retry every 15s via
	// asynqRetryDelayFunc, and follow-up/retract paths fire quickly.
	wikiIngestMaxRetry = 10

	// WikiIngestTaskTimeout is the outer durable batch ceiling. Individual Wiki
	// LLM fragments have three 30-minute attempts; one document can legitimately
	// need extraction, summary, citations and page modification in sequence.
	// The old 60-minute ceiling could cancel the parent before later fragments
	// received their retry budget, so keep the task alive long enough for the
	// bounded per-fragment policy to run. Pending rows still survive crashes and
	// are retried by the durable task_pending_ops lane.
	WikiIngestTaskTimeout = 90 * time.Minute

	// wikiDeletedKeyPrefix is the Redis key prefix for "recently deleted
	// knowledge" tombstones. Key: wiki:deleted:{kbID}:{knowledgeID}. Written
	// by cleanupWikiOnKnowledgeDelete so that any wiki_ingest task still in
	// flight (or queued) for this knowledge can fast-path skip without
	// hitting the DB. TTL > wikiIngestDelay so it's guaranteed to outlast
	// any in-flight ingest.
	wikiDeletedKeyPrefix = "wiki:deleted:"

	// wikiDeletedTTL bounds how long we remember a deletion. Must comfortably
	// exceed the longest plausible ingest run (LLM extraction + reduce).
	wikiDeletedTTL = 1 * time.Hour

	// wikiLLMMaxAttempts is the total attempt count (initial + retries) for
	// every LLM call routed through generateWithTemplate. 3 was chosen to
	// absorb transient 504/timeouts from upstream gateways without
	// materially prolonging task runtime when the remote is genuinely down.
	wikiLLMMaxAttempts = 3

	// wikiLLMMaxTokens is the completion-token budget for every LLM call
	// routed through generateWithTemplate. Combined wiki extraction emits a
	// single large JSON document (entities + concepts + details). When
	// MaxTokens is left at 0, OpenAI-compatible clients omit max_tokens and
	// providers such as DeepSeek apply a default of 8192 — long extracts are
	// truncated mid-JSON with finish_reason=length, and parse fails with
	// "unexpected end of JSON input" (EXTRACT_FAILED). Raising the budget to
	// 32768 matches verified complete outputs for large Chinese policy docs;
	// shorter replies still stop early via finish_reason=stop. See #2604.
	wikiLLMMaxTokens = 32768

	// wikiTaskType is the task_type stamp used in task_pending_ops and
	// task_dead_letters rows for this pipeline. Stable across the lifetime
	// of any pending op so the follow-up consumer can pull it back.
	wikiTaskType = "wiki:ingest"

	// wikiTaskScope is the scope used by both pending ops and dead letters.
	// Wiki ingest is per-KB, so every op is scoped to a knowledge_base.
	wikiTaskScope = types.TaskScopeKnowledgeBase

	// --- Finalize lane (Phase 1: debounced KB-global convergence) ---------
	//
	// The KB-global convergence steps (index-intro rebuild, dead-link
	// cleanup, cross-link injection) used to run at the tail of EVERY
	// 5-doc ingest batch. On a bulk import that meant O(docs/batchSize)
	// index rebuilds — each an LLM call plus an index-page write. They are
	// now deferred to a debounced per-KB wiki:finalize task: each ingest
	// batch records what changed into a separate lane of task_pending_ops
	// (task_type=wiki:finalize) and schedules a coalesced trigger, so a
	// burst of N documents rebuilds the index ONCE at the end.

	// wikiFinalizeTaskType is the task_pending_ops lane for finalize work.
	// Keyed by (task_type, scope, scope_id) like the ingest lane, but with a
	// distinct task_type so PeekBatch never mixes the two.
	wikiFinalizeTaskType = "wiki:finalize"

	// wikiFinalizeOpSlug rows carry one affected page slug (+ its fresh title
	// when this batch wrote it) for the dead-link / cross-link passes.
	wikiFinalizeOpSlug = "slug"
	// wikiFinalizeOpChange rows carry a doc-level add/remove change entry for
	// the index-intro change description.
	wikiFinalizeOpChange = "change"
	// wikiFinalizeOpCanonicalReconcile requests a full, idempotent pass over
	// historical exact identities. Normal slug rows use affected-only mode.
	wikiFinalizeOpCanonicalReconcile = "canonical_reconcile"
	// wikiFinalizeOpFolderPrune rows carry folders that may have become empty
	// after a document retract. Keeping this in the durable finalize lane lets
	// us wait until every ingest op for the KB has settled before deleting the
	// directories; taxonomy planning creates folders before reduce writes the
	// corresponding pages, so pruning any earlier can invalidate in-flight
	// folder assignments.
	wikiFinalizeOpFolderPrune = "folder_prune"

	wikiFinalizeAdded   = "added"
	wikiFinalizeRemoved = "removed"

	// wikiFinalizeDelay debounces the finalize trigger so multiple ingest
	// batches within the window coalesce into a single index rebuild.
	wikiFinalizeDelay = 20 * time.Second

	// wikiFinalizeMaxRows caps how many finalize rows one run drains. A run
	// that hits the cap reschedules itself for the remainder.
	wikiFinalizeMaxRows = 5000

	// wikiFinalizeLockTTL / Renew guard against two finalize runs for the
	// same KB writing the index page concurrently (belt-and-suspenders on
	// top of the asynq.TaskID coalescing).
	wikiFinalizeLockTTL   = 60 * time.Second
	wikiFinalizeLockRenew = 20 * time.Second

	// Folder cleanup is maintenance, not user-blocking work. When an ingest is
	// still active, retry slowly so pruning never competes with the primary wiki
	// pipeline for worker capacity.
	wikiFolderPruneRetryDelay = 1 * time.Minute

	// wikiIngestCleanupTimeout bounds detached tail cleanup after the asynq
	// task context has been cancelled or hit its timeout.
	wikiIngestCleanupTimeout = 10 * time.Second

	// wikiPagePersistTimeout is a narrow, detached commit window used only
	// after a Wiki page result has already been generated. The expensive LLM
	// work stays governed by the 90-minute task deadline; this grace period
	// prevents a response that completed at the deadline edge from being lost
	// because its final atomic CreatePage/UpdatePage inherited a cancelled
	// context. A blocked database still fails and requeues after this bound.
	wikiPagePersistTimeout = 2 * time.Minute

	// wikiTaskSettlementReserve keeps the hard asynq deadline out of the LLM
	// retry loop. generateWithTemplate divides the remaining work window across
	// its outstanding attempts and leaves this tail for failure accounting,
	// claim release, and atomic parse-tree settlement.
	wikiTaskSettlementReserve = 5 * time.Minute
)

var (
	// wikiLLMAttemptTimeout is the ceiling for one provider attempt. The actual
	// timeout is reduced near the parent deadline by wikiLLMAttemptBudget so a
	// fully stalled provider cannot consume the durable settlement reserve.
	wikiLLMAttemptTimeout = 30 * time.Minute

	// wikiLLMBackoffBase is a variable so timeout/retry tests can exercise the
	// production loop without sleeping for seconds. Production keeps the
	// existing 2s, 4s delays between the three total attempts.
	wikiLLMBackoffBase = 2 * time.Second
)

// wikiFinalizeChange is a doc-level add/remove entry for the index-intro
// change description, persisted as a finalize-lane row payload.
type wikiFinalizeChange struct {
	Action     string `json:"action"` // wikiFinalizeAdded | wikiFinalizeRemoved
	DocTitle   string `json:"doc_title,omitempty"`
	DocSummary string `json:"doc_summary,omitempty"`
}

// wikiFinalizeRow is the JSON payload of a task_pending_ops row in the
// finalize lane. Exactly one of {Slug, Change, FolderIDs} is set,
// distinguished by the row's Op column.
type wikiFinalizeRow struct {
	Slug      string              `json:"slug,omitempty"`
	Title     string              `json:"title,omitempty"`
	Change    *wikiFinalizeChange `json:"change,omitempty"`
	FolderIDs []string            `json:"folder_ids,omitempty"`
}

// WikiDeletedTombstoneKey returns the Redis key used to mark a knowledge as
// recently deleted, so wiki_ingest tasks in flight can short-circuit. Exposed
// so knowledgeService.cleanupWikiOnKnowledgeDelete can write the same key
// without duplicating the format string.
func WikiDeletedTombstoneKey(kbID, knowledgeID string) string {
	return wikiDeletedKeyPrefix + kbID + ":" + knowledgeID
}

// WikiIngestPayload is the asynq task payload for wiki ingest batch trigger.
// The actual document IDs are stored in the task_pending_ops table; this
// payload only carries the trigger metadata so the worker can resolve
// the queue tuple (task_type, scope, scope_id) and process whatever rows
// are queued under it.
type WikiIngestPayload struct {
	types.TracingContext
	TenantID        uint64 `json:"tenant_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
	Language        string `json:"language,omitempty"`
}

// WikiRetractPayload is the asynq task payload for wiki content retraction
type WikiRetractPayload struct {
	types.TracingContext
	TenantID        uint64   `json:"tenant_id"`
	KnowledgeBaseID string   `json:"knowledge_base_id"`
	KnowledgeID     string   `json:"knowledge_id"`
	DocTitle        string   `json:"doc_title"`
	DocSummary      string   `json:"doc_summary,omitempty"` // one-line summary of the deleted document
	Language        string   `json:"language,omitempty"`
	PageSlugs       []string `json:"page_slugs"`
	FolderIDs       []string `json:"folder_ids,omitempty"`
}

const (
	WikiOpIngest  = "ingest"
	WikiOpRetract = "retract"
)

// WikiPendingOp represents a single operation queued in task_pending_ops
// under task_type="wiki:ingest". The struct is the JSON payload of the
// task_pending_ops row; the surrounding (task_type, scope, scope_id,
// dedup_key) fields live as separate columns and are not serialized
// here.
//
// dbID is the auto-increment primary key of the task_pending_ops row
// the op was loaded from. PeekBatch fills it; consumers carry it
// through Map/Reduce so DeleteByIDs (after consume) and IncrFailCount
// (after failure) can address the right row. It is intentionally
// unexported and excluded from JSON so the persisted payload does not
// duplicate the column.
type WikiPendingOp struct {
	Op          string `json:"op"`
	KnowledgeID string `json:"knowledge_id"`
	Attempt     int    `json:"attempt,omitempty"`
	// WorkID, when known, distinguishes immutable source revisions queued for
	// the same knowledge. Legacy rows are kept distinct because an attempt is
	// not a revision identity.
	WorkID string `json:"work_id,omitempty"`
	// Ingest fields
	Language string `json:"language,omitempty"`
	// Retract fields
	DocTitle   string   `json:"doc_title,omitempty"`
	DocSummary string   `json:"doc_summary,omitempty"`
	PageSlugs  []string `json:"page_slugs,omitempty"`
	FolderIDs  []string `json:"folder_ids,omitempty"`

	// dbID is the canonical row. dbIDs contains exact logical duplicates with a
	// durable WorkID so settlement can acknowledge them atomically.
	dbID       int64                 `json:"-"`
	dbIDs      []int64               `json:"-"`
	failCount  int                   `json:"-"`
	claimOwner *types.TaskClaimOwner `json:"-"`
}

func (op WikiPendingOp) pendingRowIDs() []int64 {
	if len(op.dbIDs) > 0 {
		return append([]int64(nil), op.dbIDs...)
	}
	if op.dbID > 0 {
		return []int64{op.dbID}
	}
	return nil
}

// wikiIngestService handles the LLM-powered wiki generation pipeline.
//
// Durable state lives in two places:
//   - task_pending_ops (rows tagged task_type="wiki:ingest", scope=
//     "knowledge_base"): the per-document op queue. Replaces the
//     legacy Redis wiki:pending:<kbID> list, which was vulnerable to
//     24h TTL eviction at 4w-document scale.
//   - task_dead_letters: in-batch failures that exhausted
//     wikiMaxFailRetries land here. The asynq dead-letter middleware
//     also writes asynq-level archived rows here uniformly across
//     every task type.
//
// Redis is still used for per-slug locks, in-flight limits, delete tombstones,
// and short-lived cross-batch identity reservations. These are
// correctness-critical coordination flags rather than durable source data.
type wikiIngestService struct {
	wikiService    interfaces.WikiPageService
	kbService      interfaces.KnowledgeBaseService
	knowledgeSvc   interfaces.KnowledgeService
	knowledgeRepo  interfaces.KnowledgeRepository
	chunkRepo      interfaces.ChunkRepository
	modelService   interfaces.ModelService
	task           interfaces.TaskEnqueuer
	audit          interfaces.AuditLogService
	pendingRepo    interfaces.TaskPendingOpsRepository
	deadLetterRepo interfaces.TaskDeadLetterRepository
	redisClient    *redis.Client // nil in Lite mode (no Redis)
	// spanTracker lets per-document map work surface as a
	// postprocess.wiki subspan in the knowledge trace tree. The KB-scoped
	// asynq payload is ambiguous across documents, so each durable pending op
	// carries its own attempt and all settlement uses that exact value.
	spanTracker SpanTracker
	// liteLocks provides per-KB mutual exclusion in Lite mode (no Redis).
	// Keys are kbID strings; values are unused (presence = locked).
	liteLocks sync.Map
	// liteFinalizeLocks is the Lite-mode counterpart of the Redis
	// wiki:finalize:active:<kbID> lock, keeping two finalize runs for the
	// same KB from overlapping when there is no Redis.
	liteFinalizeLocks sync.Map
	// llmRequests coalesces byte-identical concurrent prompts within this
	// process. Keys include tenant and model to preserve isolation.
	llmRequests singleflight.Group
	// promptWarmups serializes only the first request for a reusable Wiki page
	// prefix. Other prefixes and already-warmed cohorts stay parallel.
	promptWarmups sync.Map
}

type wikiPromptWarmup struct {
	done chan struct{}
	once sync.Once
}

// NewWikiIngestService creates a new wiki ingest service
func NewWikiIngestService(
	wikiService interfaces.WikiPageService,
	kbService interfaces.KnowledgeBaseService,
	knowledgeSvc interfaces.KnowledgeService,
	knowledgeRepo interfaces.KnowledgeRepository,
	chunkRepo interfaces.ChunkRepository,
	modelService interfaces.ModelService,
	task interfaces.TaskEnqueuer,
	audit interfaces.AuditLogService,
	pendingRepo interfaces.TaskPendingOpsRepository,
	deadLetterRepo interfaces.TaskDeadLetterRepository,
	redisClient *redis.Client,
	spanTracker SpanTracker,
) interfaces.TaskHandler {
	svc := &wikiIngestService{
		wikiService:    wikiService,
		kbService:      kbService,
		knowledgeSvc:   knowledgeSvc,
		knowledgeRepo:  knowledgeRepo,
		chunkRepo:      chunkRepo,
		modelService:   modelService,
		task:           task,
		audit:          audit,
		pendingRepo:    pendingRepo,
		deadLetterRepo: deadLetterRepo,
		redisClient:    redisClient,
		spanTracker:    spanTracker,
	}
	return svc
}

// tracker returns a non-nil span tracker so callers don't have to
// nil-check on every Begin/End. Matches the noopSpanTracker pattern
// used elsewhere (see knowledgeService.tracker, KnowledgePostProcessService.tracker).
func (s *wikiIngestService) tracker() SpanTracker {
	if s.spanTracker == nil {
		return noopSpanTracker{}
	}
	return s.spanTracker
}

// beginWikiSubspan opens a postprocess.wiki subspan for this document
// under the attempt persisted with the durable op. Returns nil when there is
// no parse attempt to attach to (e.g. a wiki ingest fired from a manual
// reparse path that never went through the tracker) — callers must
// pair every begin with a tolerant end / fail / skip below.
//
// Rows created by manual maintenance paths have attempt=0 and deliberately do
// not attach to, or settle, a parse tree.
func (s *wikiIngestService) beginWikiSubspan(ctx context.Context, op WikiPendingOp, input types.JSONMap) *Span {
	if op.KnowledgeID == "" || op.Attempt <= 0 {
		return nil
	}
	parent := s.tracker().LookupStage(ctx, op.KnowledgeID, op.Attempt, types.StagePostProcess)
	if parent == nil {
		return nil
	}
	return s.tracker().BeginSubSpan(ctx, parent, "postprocess.wiki", types.SpanKindSubSpan, input)
}

// withWikiGenerationSpan makes the next model generation a child of the
// concrete Wiki processing subspan mirrored in the knowledge timeline. The
// reduce lane starts from a KB-scoped context, so it must reconstruct the
// per-document correlation from the selected page span rather than inheriting
// the structural postprocess.wiki parent.
func withWikiGenerationSpan(ctx context.Context, span *Span) context.Context {
	if span == nil {
		return ctx
	}
	ctx = langfuse.WithKnowledgeTraceContext(ctx, langfuse.KnowledgeTraceContext{
		KnowledgeID: span.KnowledgeID,
		Attempt:     span.Attempt,
		Stage:       "postprocess.wiki",
		TaskType:    types.TypeWikiIngest,
	})
	return langfuse.WithKnowledgeGenerationStage(ctx, span.Name)
}

// EnqueueWikiIngest queues a document for wiki ingestion. The returned bool
// reports whether the durable pending op was persisted. A trigger enqueue
// error may therefore be returned together with true; callers can retry only
// the KB-scoped trigger without appending a duplicate operation.
//
// Architecture: each upload inserts one row into task_pending_ops
// (task_type="wiki:ingest", scope="knowledge_base", scope_id=kbID,
// dedup_key=knowledgeID), then schedules a debounced asynq trigger task.
// When the trigger fires, the worker peeks a batch from
// task_pending_ops, processes it, deletes consumed rows, and (if more
// remain) schedules a follow-up. Multiple debounced triggers within the
// 30s window all coalesce: the first one to acquire the per-KB active
// lock drains the batch; subsequent ones see an empty queue and exit.
//
// Lite mode (no Redis) still works as long as Postgres is reachable —
// the queue lives in PG, only the active-batch lock is Redis-only and
// has a process-local fallback (liteLocks) inside the worker.
func enqueueWikiPendingOp(
	ctx context.Context,
	pendingRepo interfaces.TaskPendingOpsRepository,
	op *types.TaskPendingOp,
) (bool, error) {
	if pendingRepo == nil {
		return true, nil
	}
	if guard, ok := pendingRepo.(interfaces.TaskPendingOpsKnowledgeBaseGuard); ok {
		return guard.EnqueueIfKnowledgeBaseActive(ctx, op)
	}
	if err := pendingRepo.Enqueue(ctx, op); err != nil {
		return false, err
	}
	return true, nil
}

func EnqueueWikiIngest(
	ctx context.Context,
	task interfaces.TaskEnqueuer,
	pendingRepo interfaces.TaskPendingOpsRepository,
	tenantID uint64,
	kbID, knowledgeID string,
) (bool, error) {
	pendingOp, err := newWikiIngestPendingOp(ctx, tenantID, kbID, knowledgeID, 0)

	// Persist the pending op. A re-ingest of the same knowledge id while
	// a previous op is still queued simply appends another row; the
	// peekPendingList consumer collapses by dedup_key (== knowledge_id),
	// keeping the LATEST op for each knowledge — matching the legacy
	// "RPush + reverse-dedupe" semantics.
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: failed to marshal pending op for %s: %v", knowledgeID, err)
		return false, err
	}
	accepted, err := enqueueWikiPendingOp(ctx, pendingRepo, pendingOp)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: failed to enqueue pending op for %s: %v", knowledgeID, err)
		return false, fmt.Errorf("enqueue wiki ingest pending op: %w", err)
	}
	if !accepted {
		logger.Infof(ctx, "wiki ingest: skip enqueue for deleted KB %s", kbID)
		return false, nil
	}
	if err := enqueueWikiIngestTrigger(ctx, task, tenantID, kbID); err != nil {
		return true, err
	}
	return true, nil
}

func newWikiIngestPendingOp(
	ctx context.Context,
	tenantID uint64,
	kbID, knowledgeID string,
	attempt int,
) (*types.TaskPendingOp, error) {
	lang := types.LanguageFromContextOrDefault(ctx)
	op := WikiPendingOp{
		Op:          WikiOpIngest,
		KnowledgeID: knowledgeID,
		Attempt:     attempt,
		Language:    lang,
	}
	payloadBytes, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("marshal wiki ingest pending op: %w", err)
	}
	return &types.TaskPendingOp{
		TenantID: tenantID,
		TaskType: wikiTaskType,
		Scope:    wikiTaskScope,
		ScopeID:  kbID,
		Op:       WikiOpIngest,
		DedupKey: knowledgeID,
		Payload:  payloadBytes,
	}, nil
}

func enqueueWikiIngestTrigger(
	ctx context.Context,
	task interfaces.TaskEnqueuer,
	tenantID uint64,
	kbID string,
) error {
	lang := types.LanguageFromContextOrDefault(ctx)
	trigger := WikiIngestPayload{
		TenantID:        tenantID,
		KnowledgeBaseID: kbID,
		Language:        lang,
	}
	langfuse.InjectTracing(ctx, &trigger)
	triggerBytes, err := json.Marshal(trigger)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: failed to marshal trigger task: %v", err)
		return fmt.Errorf("marshal wiki ingest trigger: %w", err)
	}
	if task == nil {
		return errors.New("enqueue wiki ingest trigger: task enqueuer is nil")
	}

	t := asynq.NewTask(types.TypeWikiIngest, triggerBytes,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout),
		asynq.ProcessIn(wikiIngestDelay),
	)
	if _, err := task.Enqueue(t); err != nil {
		logger.Warnf(ctx, "wiki ingest: failed to enqueue trigger task: %v", err)
		return fmt.Errorf("enqueue wiki ingest trigger: %w", err)
	}
	return nil
}

// EnqueueWikiRetract queues a wiki retraction op (a delete cleanup).
// Identical persistence model as EnqueueWikiIngest — the op rides in
// task_pending_ops and an asynq trigger fires shortly after to
// process the batch. Retracts use a slightly shorter ProcessIn delay
// because there is no "user upload arriving in waves" pattern to
// debounce against — a deletion fires once and we want the cleanup
// to land promptly.
func EnqueueWikiRetract(
	ctx context.Context,
	task interfaces.TaskEnqueuer,
	pendingRepo interfaces.TaskPendingOpsRepository,
	payload WikiRetractPayload,
) {
	op := WikiPendingOp{
		Op:          WikiOpRetract,
		KnowledgeID: payload.KnowledgeID,
		DocTitle:    payload.DocTitle,
		DocSummary:  payload.DocSummary,
		PageSlugs:   payload.PageSlugs,
		FolderIDs:   payload.FolderIDs,
		Language:    payload.Language,
	}
	payloadBytes, err := json.Marshal(op)
	if err != nil {
		logger.Warnf(ctx, "wiki retract: failed to marshal pending op: %v", err)
		return
	}
	accepted, err := enqueueWikiPendingOp(ctx, pendingRepo, &types.TaskPendingOp{
		TenantID: payload.TenantID,
		TaskType: wikiTaskType,
		Scope:    wikiTaskScope,
		ScopeID:  payload.KnowledgeBaseID,
		Op:       WikiOpRetract,
		DedupKey: payload.KnowledgeID,
		Payload:  payloadBytes,
	})
	if err != nil {
		logger.Warnf(ctx, "wiki retract: failed to enqueue pending op: %v", err)
		return
	}
	if !accepted {
		logger.Infof(ctx, "wiki retract: skip enqueue for deleted KB %s", payload.KnowledgeBaseID)
		return
	}

	trigger := WikiIngestPayload{
		TenantID:        payload.TenantID,
		KnowledgeBaseID: payload.KnowledgeBaseID,
		Language:        payload.Language,
	}
	langfuse.InjectTracing(ctx, &trigger)
	triggerBytes, _ := json.Marshal(trigger)
	t := asynq.NewTask(types.TypeWikiIngest, triggerBytes,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout),
		asynq.ProcessIn(5*time.Second), // Retract can trigger the batch quickly
	)
	if _, err := task.Enqueue(t); err != nil {
		logger.Warnf(ctx, "wiki retract: failed to enqueue trigger task: %v", err)
	}
}

// Handle implements interfaces.TaskHandler for asynq task processing. The
// wiki service owns two task types, dispatched here by type so a single
// registered handler covers both the ingest batch and the debounced
// KB-global finalize pass.
func (s *wikiIngestService) Handle(ctx context.Context, t *asynq.Task) error {
	switch t.Type() {
	case types.TypeWikiFinalize:
		return s.ProcessWikiFinalize(ctx, t)
	default:
		return s.ProcessWikiIngest(ctx, t)
	}
}

func wikiIngestCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return detachedProcessingOwnerContext(ctx, wikiIngestCleanupTimeout)
}

func wikiPagePersistContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return detachedProcessingOwnerContext(ctx, wikiPagePersistTimeout)
}

func (s *wikiIngestService) clearDeletedKnowledgeBasePendingOps(ctx context.Context, kbID string) error {
	cleaner, ok := s.pendingRepo.(interfaces.TaskPendingOpsScopeCleaner)
	if !ok || kbID == "" {
		return nil
	}
	cleanupCtx, cancel := wikiIngestCleanupContext(ctx)
	defer cancel()
	return cleaner.DeleteByScope(cleanupCtx, types.TaskScopeKnowledgeBase, kbID)
}

func (s *wikiIngestService) enqueueFinalizeRow(ctx context.Context, op *types.TaskPendingOp) (bool, error) {
	accepted, err := enqueueWikiPendingOp(ctx, s.pendingRepo, op)
	if err != nil {
		return false, fmt.Errorf("enqueue wiki finalize %s row: %w", op.Op, err)
	}
	if !accepted {
		return false, fmt.Errorf("enqueue wiki finalize %s row: knowledge base is no longer active", op.Op)
	}
	return true, nil
}

// enqueueFinalize persists this batch's KB-global convergence work into the
// finalize lane of task_pending_ops and schedules a debounced trigger. One
// "slug" row per affected page (carrying its fresh title when this batch wrote
// it, for cross-linking) plus one "change" row per doc added/removed (for the
// index-intro change description).
func (s *wikiIngestService) enqueueFinalize(
	ctx context.Context,
	payload WikiIngestPayload,
	affectedSlugs []string,
	freshTitleBySlug map[string]string,
	changes []wikiFinalizeChange,
	folderIDs []string,
) error {
	if s.pendingRepo == nil {
		return errors.New("wiki finalize pending repository is unavailable")
	}
	acceptedAny := false
	var enqueueErrs []error
	for _, slug := range affectedSlugs {
		row := wikiFinalizeRow{Slug: slug, Title: freshTitleBySlug[slug]}
		b, err := json.Marshal(row)
		if err != nil {
			enqueueErrs = append(enqueueErrs, fmt.Errorf("marshal wiki finalize slug %s: %w", slug, err))
			continue
		}
		accepted, enqueueErr := s.enqueueFinalizeRow(ctx, &types.TaskPendingOp{
			TenantID: payload.TenantID,
			TaskType: wikiFinalizeTaskType,
			Scope:    wikiTaskScope,
			ScopeID:  payload.KnowledgeBaseID,
			Op:       wikiFinalizeOpSlug,
			DedupKey: slug,
			Payload:  b,
		})
		if enqueueErr != nil {
			enqueueErrs = append(enqueueErrs, enqueueErr)
		} else if accepted {
			acceptedAny = true
		}
	}
	for i := range changes {
		row := wikiFinalizeRow{Change: &changes[i]}
		b, err := json.Marshal(row)
		if err != nil {
			enqueueErrs = append(enqueueErrs, fmt.Errorf("marshal wiki finalize change: %w", err))
			continue
		}
		accepted, enqueueErr := s.enqueueFinalizeRow(ctx, &types.TaskPendingOp{
			TenantID: payload.TenantID,
			TaskType: wikiFinalizeTaskType,
			Scope:    wikiTaskScope,
			ScopeID:  payload.KnowledgeBaseID,
			Op:       wikiFinalizeOpChange,
			DedupKey: "",
			Payload:  b,
		})
		if enqueueErr != nil {
			enqueueErrs = append(enqueueErrs, enqueueErr)
		} else if accepted {
			acceptedAny = true
		}
	}
	if len(folderIDs) > 0 {
		row := wikiFinalizeRow{FolderIDs: uniqueWikiFolderIDs(folderIDs)}
		if b, err := json.Marshal(row); err != nil {
			enqueueErrs = append(enqueueErrs, fmt.Errorf("marshal wiki finalize folder prune: %w", err))
		} else {
			accepted, enqueueErr := s.enqueueFinalizeRow(ctx, &types.TaskPendingOp{
				TenantID: payload.TenantID,
				TaskType: wikiFinalizeTaskType,
				Scope:    wikiTaskScope,
				ScopeID:  payload.KnowledgeBaseID,
				Op:       wikiFinalizeOpFolderPrune,
				DedupKey: "",
				Payload:  b,
			})
			if enqueueErr != nil {
				enqueueErrs = append(enqueueErrs, enqueueErr)
			} else if accepted {
				acceptedAny = true
			}
		}
	}
	if !acceptedAny {
		if len(enqueueErrs) == 0 && (len(affectedSlugs) > 0 || len(changes) > 0 || len(folderIDs) > 0) {
			enqueueErrs = append(enqueueErrs, errors.New("wiki finalize work was not durably accepted"))
		}
		return errors.Join(enqueueErrs...)
	}
	if err := s.scheduleFinalize(ctx, payload); err != nil {
		enqueueErrs = append(enqueueErrs, err)
	}
	return errors.Join(enqueueErrs...)
}

func uniqueWikiFolderIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// scheduleFinalize enqueues a debounced, coalesced KB-global finalize trigger.
// asynq.TaskID ("wiki-finalize-<kbID>") makes concurrent schedules within the
// debounce window collapse into one pending task; the conflict error is the
// expected coalescing signal, not a failure. In Lite mode the sync executor
// ignores TaskID, so finalize simply runs once per batch (acceptable at the
// small scale Lite mode targets).
func (s *wikiIngestService) scheduleFinalize(ctx context.Context, payload WikiIngestPayload) error {
	langfuse.InjectTracing(ctx, &payload)
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal wiki finalize trigger: %w", err)
	}
	if s.task == nil {
		return errors.New("wiki finalize task queue is unavailable")
	}
	t := asynq.NewTask(types.TypeWikiFinalize, b,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(30*time.Minute),
		asynq.ProcessIn(wikiFinalizeDelay),
		asynq.TaskID("wiki-finalize-"+payload.KnowledgeBaseID),
	)
	if _, err := s.task.Enqueue(t); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return nil // a finalize is already scheduled/running for this KB — coalesced
		}
		return fmt.Errorf("schedule wiki finalize trigger: %w", err)
	}
	return nil
}

// scheduleFinalizeRetry is used when folder pruning is waiting for ingest
// rows to drain. It deliberately has no stable TaskID: the currently-running
// finalize task still owns that ID until it returns, so reusing it here would
// coalesce the only retry away. Duplicate retries are harmless because the
// durable prune row is deleted exactly once and an empty lane is a no-op.
func (s *wikiIngestService) scheduleFinalizeRetry(ctx context.Context, payload WikiIngestPayload) error {
	langfuse.InjectTracing(ctx, &payload)
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal wiki finalize retry: %w", err)
	}
	if s.task == nil {
		return errors.New("wiki finalize task queue is unavailable")
	}
	t := asynq.NewTask(types.TypeWikiFinalize, b,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(30*time.Minute),
		asynq.ProcessIn(wikiFolderPruneRetryDelay),
	)
	if _, err := s.task.Enqueue(t); err != nil {
		return fmt.Errorf("schedule deferred wiki folder prune: %w", err)
	}
	return nil
}

// peekPendingList loads up to `limit` ops from task_pending_ops for
// this KB, ordered FIFO. Rows are NOT removed; callers must
// DeleteByIDs once they have been consumed (or IncrFailCount + leave
// them in place for the next pass).
//
// peekedIDs returns the DB ids of every row included in the peek
// (NOT just the ones that survived dedup) so trimPendingList can
// delete them all in one statement at the end of the batch — this
// matches the legacy "LTrim peekedCount entries" semantics, where
// duplicates collapsed by the consumer were also drained from the
// list once their canonical sibling had been processed.
func (s *wikiIngestService) peekPendingList(ctx context.Context, kbID string, limit int) (ops []WikiPendingOp, peekedIDs []int64, err error) {
	if s.pendingRepo == nil {
		return nil, nil, nil
	}
	if limit <= 0 {
		limit = wikiMaxDocsPerBatch
	}
	rows, err := s.pendingRepo.PeekBatch(ctx, wikiTaskType, wikiTaskScope, kbID, limit)
	if err != nil {
		// Surface the error so the caller can distinguish a transient DB
		// failure from a genuinely empty queue: the former must trigger an
		// asynq retry, whereas returning "no rows" here would ack the task
		// as a false success and strand the pending list.
		return nil, nil, err
	}
	ops, peekedIDs = s.decodePendingRows(ctx, rows)
	return ops, peekedIDs, nil
}

// claimPendingList is the standard-mode (Redis) counterpart of
// peekPendingList: it atomically CLAIMS up to `limit` ops (marks
// claimed_at) so concurrent batches for the same KB — allowed since Phase 3
// removed the exclusive per-KB lock — pull DISJOINT documents instead of
// double-processing. Stale claims (older than wikiClaimStaleAfter, i.e. from
// a crashed worker) are recovered. Dedup / peekedIDs semantics match
// peekPendingList; the returned peekedIDs are the claimed rows that the
// caller must DeleteByIDs on success or ReleaseByIDs to retry.
func (s *wikiIngestService) claimPendingList(
	ctx context.Context, kbID string, limit int,
) (ops []WikiPendingOp, peekedIDs []int64, owner *types.TaskClaimOwner, err error) {
	if s.pendingRepo == nil {
		return nil, nil, nil, nil
	}
	if limit <= 0 {
		limit = wikiMaxDocsPerBatch
	}
	taskID, taskIDOK := asynq.GetTaskID(ctx)
	if !taskIDOK || strings.TrimSpace(taskID) == "" {
		return nil, nil, nil, fmt.Errorf("wiki ingest: concrete Asynq task id is unavailable")
	}
	claimRepo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease)
	if !ok {
		return nil, nil, nil, fmt.Errorf("wiki ingest: owner-safe pending claim repository is unavailable")
	}
	claimOwner := types.TaskClaimOwner{Token: uuid.NewString(), TaskID: taskID}
	rows, err := claimRepo.ClaimBatchOwned(ctx, wikiTaskType, wikiTaskScope, kbID, limit,
		time.Now().Add(-wikiClaimStaleAfter), claimOwner)
	if err != nil {
		// A claim failure is transient (DB blip). Propagate it so the batch
		// returns an error and asynq retries, instead of acking the trigger
		// as a false "no pending ops" success and stranding the queue.
		return nil, nil, nil, err
	}
	for _, row := range rows {
		if row == nil || row.ClaimedAt == nil || row.ClaimHeartbeatAt == nil ||
			row.ClaimToken != claimOwner.Token || row.ClaimedByTaskID != claimOwner.TaskID {
			return nil, nil, nil, fmt.Errorf("wiki ingest: claimed row is missing its ownership token")
		}
	}
	ops, peekedIDs = s.decodePendingRows(ctx, rows)
	return ops, peekedIDs, &claimOwner, nil
}

// withSlugLock serializes read-modify-write on one shared wiki page across
// concurrent batches. Phase 3 removed the exclusive per-KB batch lock, so
// two batches for the same KB can both produce updates for a shared
// entity/concept slug; without this lock their GetPageBySlug→UpdatePage
// cycles would race and lose one contribution.
//
// Redis mode only: in Lite mode a single process runs one batch per KB at a
// time (liteLocks), so same-slug contention cannot occur and fn runs
// directly. Returns (false, nil) if the lock could not be acquired within
// wikiSlugLockWait — the caller then keeps the durable row for retry. On a Redis
// error we fail closed and let the durable pending row retry. Running the
// read-modify-write callback unlocked can lose a successful contribution and
// then falsely settle the document as complete.
func (s *wikiIngestService) withSlugLock(ctx context.Context, kbID, slug string, fn func() error) (bool, error) {
	if s.redisClient == nil {
		return true, fn()
	}
	key := wikiSlugLockPrefix + kbID + ":" + slug
	owner := uuid.NewString()
	deadline := time.Now().Add(wikiSlugLockWait)
	for {
		ok, rerr := s.redisClient.SetNX(ctx, key, owner, wikiSlugLockTTL).Result()
		if rerr != nil {
			return false, fmt.Errorf("wiki reduce: acquire slug lock %s: %w", slug, rerr)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(wikiSlugLockPoll):
		}
	}
	callbackErr := fn()
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer releaseCancel()
	released, releaseErr := s.redisClient.Eval(
		releaseCtx, releaseWikiSlugLockScript, []string{key}, owner,
	).Int64()
	if releaseErr != nil {
		return true, errors.Join(callbackErr, fmt.Errorf("wiki reduce: release slug lock %s: %w", slug, releaseErr))
	}
	if released == 0 {
		return true, errors.Join(callbackErr, fmt.Errorf("wiki reduce: slug lock %s ownership was lost", slug))
	}
	return true, callbackErr
}

const releaseWikiSlugLockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// wikiInflightReserveScript atomically enforces the per-KB in-flight cap.
// It purges expired slots (crash recovery), counts live slots, and adds the
// caller's slot iff under the cap. Returning 1 = reserved, 0 = at cap.
// Keeping purge+count+add in one Lua call closes the check-then-act race two
// concurrent reservers would otherwise have.
const wikiInflightReserveScript = `
local now = tonumber(ARGV[1])
local expiry = tonumber(ARGV[2])
local maxInflight = tonumber(ARGV[3])
local token = ARGV[4]
local ttl = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now)
if redis.call('ZCARD', KEYS[1]) >= maxInflight then
  return 0
end
redis.call('ZADD', KEYS[1], expiry, token)
redis.call('PEXPIRE', KEYS[1], ttl)
return 1
`

// reserveInflightSlot claims one of KB's concurrent-batch slots (Phase 4,
// standard/Redis mode). Returns (release, true) when granted — release() MUST
// run when the batch finishes; (nil, false) when the KB is already at
// maxInflight, so the caller should reschedule and bail. A background renew
// keeps the slot alive for the batch's duration; a crashed batch's slot simply
// expires (wikiInflightTTL) and is purged by the next reserver. Lite mode has
// no shared-pool contention (liteLocks already serialize per KB), so it always
// grants a no-op slot. Fails OPEN on a Redis error: a blip must not halt wiki
// generation, and the pool size still bounds total work.
func (s *wikiIngestService) reserveInflightSlot(ctx context.Context, kbID string, maxInflight int) (func(), bool) {
	if s.redisClient == nil || maxInflight <= 0 {
		return func() {}, true
	}
	key := wikiInflightPrefix + kbID
	token := uuid.New().String()
	now := time.Now()
	res, err := s.redisClient.Eval(ctx, wikiInflightReserveScript,
		[]string{key},
		now.UnixMilli(),
		now.Add(wikiInflightTTL).UnixMilli(),
		maxInflight,
		token,
		wikiInflightTTL.Milliseconds(),
	).Int()
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: inflight reserve failed for KB %s: %v (running uncapped)", kbID, err)
		return func() {}, true
	}
	if res == 0 {
		return nil, false
	}

	renewCtx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(wikiInflightRenew)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				exp := float64(time.Now().Add(wikiInflightTTL).UnixMilli())
				s.redisClient.ZAdd(context.Background(), key, redis.Z{Score: exp, Member: token})
				s.redisClient.PExpire(context.Background(), key, wikiInflightTTL)
			}
		}
	}()
	return func() {
		cancel()
		s.redisClient.ZRem(context.Background(), key, token)
	}, true
}

// scheduleCappedRetry enqueues a single coalesced follow-up trigger after a
// batch was turned away by the in-flight cap. asynq.TaskID collapses all
// turned-away triggers for one KB into a single pending retry (no thundering
// herd), and the running batches that hold the slots also chain their own
// follow-ups on completion, so the turned-away rows are guaranteed to drain
// once a slot frees.
func (s *wikiIngestService) scheduleCappedRetry(ctx context.Context, payload WikiIngestPayload) {
	langfuse.InjectTracing(ctx, &payload)
	b, _ := json.Marshal(payload)
	t := asynq.NewTask(types.TypeWikiIngest, b,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout),
		asynq.ProcessIn(wikiInflightBackoff),
		asynq.TaskID("wiki-ingest-capped-"+payload.KnowledgeBaseID),
	)
	if _, err := s.task.Enqueue(t); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return // a capped retry is already pending for this KB — coalesced
		}
		logger.Warnf(ctx, "wiki ingest: capped-retry enqueue failed: %v", err)
	}
}

// scheduleStaleClaimRecheck arms a single, far-future safety-net trigger for a
// KB that still has pending rows but yielded nothing to claim (every eligible
// row is held by a FRESH claim). Normally a running batch drains those rows and
// chains its own fast follow-up on completion; this net exists only for the
// case where the claim holder CRASHED, leaving claimed_at stamped so no worker
// can re-claim until wikiClaimStaleAfter elapses — and where nothing else would
// ever re-trigger the KB afterwards.
//
// The delay is set past the stale threshold so that when the net fires the
// abandoned claims are guaranteed eligible again. Two alternating asynq task
// IDs coalesce rechecks without letting a running recheck conflict with the
// successor it is trying to schedule. If PendingCount is already zero the KB
// has fully drained and no net is needed. Returns true if a net is (or already
// was) scheduled.
func (s *wikiIngestService) scheduleStaleClaimRecheck(ctx context.Context, payload WikiIngestPayload) bool {
	if s.pendingRepo == nil {
		return false
	}
	count, err := s.pendingRepo.PendingCount(ctx, wikiTaskType, wikiTaskScope, payload.KnowledgeBaseID)
	if err != nil || count == 0 {
		return false
	}

	logger.Infof(ctx, "wiki ingest: %d rows for KB %s held by fresh claims, arming stale-claim recheck", count, payload.KnowledgeBaseID)

	langfuse.InjectTracing(ctx, &payload)
	b, _ := json.Marshal(payload)
	recheckTaskID := "wiki-ingest-recheck-" + payload.KnowledgeBaseID
	// Asynq retains a running task's ID until its handler returns, so that
	// task cannot enqueue its own successor under the same ID. Alternate once;
	// the -next task falls back to the base ID and keeps the ID set bounded.
	if currentTaskID, ok := asynq.GetTaskID(ctx); ok && currentTaskID == recheckTaskID {
		recheckTaskID += "-next"
	}
	t := asynq.NewTask(types.TypeWikiIngest, b,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout),
		asynq.ProcessIn(wikiClaimStaleAfter+wikiFollowUpDelay),
		asynq.TaskID(recheckTaskID),
	)
	if _, err := s.task.Enqueue(t); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return true // a recheck is already armed for this KB — coalesced
		}
		logger.Warnf(ctx, "wiki ingest: stale-claim recheck enqueue failed: %v", err)
		return false
	}
	return true
}

// decodePendingRows converts raw task_pending_ops rows into WikiPendingOps.
// Only rows carrying the same explicit WorkID are collapsed. peekedIDs carries the
// db ids of EVERY row (including dedup-collapsed ones) so the caller can
// drain them all at trim time. Shared by peekPendingList (no claim) and
// claimPendingList (claimed rows).
func (s *wikiIngestService) decodePendingRows(ctx context.Context, rows []*types.TaskPendingOp) (ops []WikiPendingOp, peekedIDs []int64) {
	if len(rows) == 0 {
		return nil, nil
	}

	all := make([]WikiPendingOp, 0, len(rows))
	rowIDsByWork := make(map[string][]int64, len(rows))
	workKey := func(op WikiPendingOp) string {
		if op.WorkID != "" {
			return op.KnowledgeID + "\x00work\x00" + op.WorkID
		}
		return fmt.Sprintf("%s\x00legacy-row\x00%d", op.KnowledgeID, op.dbID)
	}
	peekedIDs = make([]int64, 0, len(rows))
	for _, r := range rows {
		peekedIDs = append(peekedIDs, r.ID)
		var op WikiPendingOp
		if len(r.Payload) > 0 {
			if err := json.Unmarshal(r.Payload, &op); err != nil {
				logger.Warnf(ctx, "wiki ingest: failed to unmarshal pending op id=%d: %v", r.ID, err)
				continue
			}
		} else {
			// Defensive: if payload was lost, fall back to column data
			// so the row is still drainable (otherwise it would loop
			// on every batch as un-deletable).
			op = WikiPendingOp{
				Op:          r.Op,
				KnowledgeID: r.DedupKey,
			}
		}
		op.dbID = r.ID
		op.failCount = r.FailCount
		if r.ClaimToken != "" || r.ClaimedByTaskID != "" {
			owner := types.TaskClaimOwner{Token: r.ClaimToken, TaskID: r.ClaimedByTaskID}
			op.claimOwner = &owner
		}
		if op.KnowledgeID != "" {
			key := workKey(op)
			rowIDsByWork[key] = append(rowIDsByWork[key], r.ID)
		} else {
			op.dbIDs = []int64{r.ID}
		}
		all = append(all, op)
	}

	// Collapse only exact durable work identities. Legacy rows have no revision
	// key, so treating them as last-write-wins could drop an older or newer
	// revision before either one reaches its checkpoint.
	seen := make(map[string]bool)
	reversedUnique := make([]WikiPendingOp, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		op := all[i]
		if op.KnowledgeID == "" {
			// No dedup key — keep verbatim (rare; edge case for
			// future ops without a knowledge anchor).
			reversedUnique = append(reversedUnique, op)
			continue
		}
		key := workKey(op)
		if seen[key] {
			continue
		}
		seen[key] = true
		reversedUnique = append(reversedUnique, op)
	}

	ops = make([]WikiPendingOp, 0, len(reversedUnique))
	for i := len(reversedUnique) - 1; i >= 0; i-- {
		op := reversedUnique[i]
		if op.KnowledgeID != "" {
			op.dbIDs = append([]int64(nil), rowIDsByWork[workKey(op)]...)
		}
		ops = append(ops, op)
	}
	return ops, peekedIDs
}

// trimPendingList deletes consumed rows from task_pending_ops. Empty
// input is a no-op so callers can invoke unconditionally at the end
// of a batch.
func (s *wikiIngestService) trimPendingList(
	ctx context.Context, ids []int64, owner *types.TaskClaimOwner,
) error {
	if s.pendingRepo == nil || len(ids) == 0 {
		return nil
	}
	var err error
	if owner == nil {
		err = s.pendingRepo.DeleteByIDs(ctx, ids)
	} else if claimRepo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease); ok {
		err = claimRepo.DeleteClaims(ctx, ids, *owner)
	} else {
		err = fmt.Errorf("owner-safe pending delete is unavailable")
	}
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: failed to trim %d pending rows: %v", len(ids), err)
		return err
	}
	return nil
}

type strictWikiSpanLookup interface {
	LookupSpanByNameStrict(ctx context.Context, knowledgeID string, attempt int, name string) (*Span, error)
}

type strictWikiPendingSettler interface {
	SettleWikiPendingOpStrict(
		ctx context.Context,
		knowledgeID string,
		attempt int,
		pendingIDs []int64,
		deadLetter *types.TaskDeadLetter,
		owner *types.TaskClaimOwner,
	) error
}

func (s *wikiIngestService) wikiOpAlreadyDoneStrict(
	ctx context.Context, knowledgeID string, attempt int,
) (bool, error) {
	lookup, ok := s.tracker().(strictWikiSpanLookup)
	if !ok {
		return false, fmt.Errorf("strict wiki span lookup is unavailable for %s", knowledgeID)
	}
	span, err := lookup.LookupSpanByNameStrict(ctx, knowledgeID, attempt, "postprocess.wiki")
	if err != nil {
		return false, fmt.Errorf("load existing wiki span for %s attempt %d: %w", knowledgeID, attempt, err)
	}
	return span != nil && span.Status == types.SpanStatusDone, nil
}

// finalizeWikiSubtask atomically reduces the exact attempt and consumes every
// durable queue row collapsed into this document operation. The pending rows
// are deleted in the same database transaction as the parent/root/knowledge
// settlement (and optional dead-letter insert), so a crash can never leave an
// already-successful Wiki result available for another LLM run.
func (s *wikiIngestService) finalizeWikiSubtask(
	ctx context.Context, op WikiPendingOp, deadLetter *types.TaskDeadLetter,
) (bool, error) {
	if op.Attempt <= 0 {
		return false, fmt.Errorf("finalize wiki subtask %s: attempt is required", op.KnowledgeID)
	}
	latest, err := wikiAttemptCurrentStrict(context.WithoutCancel(ctx), s.tracker(), op.KnowledgeID, op.Attempt)
	if err != nil {
		return false, err
	}
	if !latest {
		logger.Infof(ctx,
			"wiki ingest: skip finalizing superseded op knowledge=%s op_attempt=%d",
			op.KnowledgeID, op.Attempt)
		return false, nil
	}
	pendingIDs := op.pendingRowIDs()
	if len(pendingIDs) == 0 {
		return false, fmt.Errorf("finalize wiki subtask %s attempt %d: durable pending row is required", op.KnowledgeID, op.Attempt)
	}
	settler, ok := s.tracker().(strictWikiPendingSettler)
	if !ok {
		return false, fmt.Errorf("strict wiki pending settlement is unavailable for %s", op.KnowledgeID)
	}
	dctx, cancel := wikiPagePersistContext(ctx)
	defer cancel()
	if err := settler.SettleWikiPendingOpStrict(
		dctx, op.KnowledgeID, op.Attempt, pendingIDs, deadLetter, op.claimOwner,
	); err != nil {
		return false, fmt.Errorf("settle wiki pending op for %s attempt %d: %w", op.KnowledgeID, op.Attempt, err)
	}
	return true, nil
}

// settleWikiIngestRows makes the queue row the source of truth for whether an
// ingest op is terminal. Successful and terminally-skipped documents only
// release their finalizing slot after every consumed pending row has been
// deleted. If the delete fails, the knowledge stays finalizing and the row is
// left available for recovery instead of being reported completed early.
func (s *wikiIngestService) settleWikiIngestRows(
	ctx context.Context,
	payload WikiIngestPayload,
	trimIDs []int64,
	failedOps []WikiPendingOp,
	terminalOps []WikiPendingOp,
	claimOwner *types.TaskClaimOwner,
) error {
	seen := make(map[string]struct{}, len(terminalOps))
	atomicallyConsumed := make(map[int64]struct{}, len(trimIDs))
	for _, op := range terminalOps {
		knowledgeID := strings.TrimSpace(op.KnowledgeID)
		if knowledgeID == "" {
			continue
		}
		key := fmt.Sprintf("%s:%d", knowledgeID, op.Attempt)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		// attempt=0 is an intentional maintenance ingest (for example after
		// reuse_vectors clone/move). It has no parse-tree owner to reduce, so
		// successful processing is acknowledged by the detached row trim below.
		if op.Attempt <= 0 {
			continue
		}
		consumed, err := s.finalizeWikiSubtask(ctx, op, nil)
		if err != nil {
			return err
		}
		if consumed {
			for _, id := range op.pendingRowIDs() {
				atomicallyConsumed[id] = struct{}{}
			}
		}
	}

	remainingTrimIDs := make([]int64, 0, len(trimIDs))
	for _, id := range trimIDs {
		if _, consumed := atomicallyConsumed[id]; !consumed {
			remainingTrimIDs = append(remainingTrimIDs, id)
		}
	}
	// Maintenance ingests, retracts, superseded attempts and malformed legacy
	// rows have no current Wiki owner to reduce. A separate detached budget
	// keeps their lightweight acknowledgement from inheriting an exhausted
	// batch context.
	trimCtx, trimCancel := wikiPagePersistContext(ctx)
	if err := s.trimPendingList(trimCtx, remainingTrimIDs, claimOwner); err != nil {
		trimCancel()
		return err
	}
	trimCancel()

	requeueCtx, requeueCancel := wikiPagePersistContext(ctx)
	defer requeueCancel()
	return s.requeueFailedOps(requeueCtx, payload, failedOps)
}

// requeueFailedOps records in-batch failures.
//
// For each failed op:
//
//   - IncrFailCount on the source row. The repo returns the new total,
//     so a single round trip handles both bookkeeping and retry-budget
//     check.
//   - If the count is <= wikiMaxFailRetries: leave the row in place.
//     The next follow-up batch's PeekBatch will pick it up naturally
//     (rows are ordered by id ASC and we never moved/touched it).
//   - If the count exceeds the retry cap: archive the op into
//     task_dead_letters and DeleteByIDs to remove it from the queue.
//     Settlement failures are returned so the caller does not mark claims
//     settled while rows are still claimed or undeleted.
func (s *wikiIngestService) requeueFailedOps(ctx context.Context, payload WikiIngestPayload, ops []WikiPendingOp) error {
	if s.pendingRepo == nil || len(ops) == 0 {
		return nil
	}
	var settleErrs []error
	for _, op := range ops {
		if op.dbID == 0 {
			// Op was never persisted (synthetic / test) — nothing to
			// retry against.
			continue
		}
		count := 0
		var err error
		if op.claimOwner == nil {
			count, err = s.pendingRepo.IncrFailCount(ctx, op.dbID)
		} else if claimRepo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease); ok {
			count, err = claimRepo.IncrClaimFailCount(ctx, op.dbID, *op.claimOwner)
		} else {
			err = fmt.Errorf("owner-safe pending failure increment is unavailable")
		}
		if err != nil {
			logger.Warnf(ctx, "wiki ingest: failed to increment fail count for %s (id=%d): %v", op.KnowledgeID, op.dbID, err)
			settleErrs = append(settleErrs, fmt.Errorf("increment fail count id=%d: %w", op.dbID, err))
			// Without a fresh count we can't tell whether to drop. Be
			// conservative: leave the row in place; the next PeekBatch
			// will see it again and we'll try once more.
			continue
		}
		if count <= wikiMaxFailRetries {
			// Release the claim so the row is immediately eligible for the
			// next trigger's ClaimBatch instead of waiting out
			// wikiClaimStaleAfter. No-op in Lite mode (row was peeked, never
			// claimed). ReleaseByIDs preserves fail_count, so the retry
			// budget still counts down.
			var releaseErr error
			if op.claimOwner == nil {
				releaseErr = s.pendingRepo.ReleaseByIDs(ctx, []int64{op.dbID})
			} else if claimRepo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease); ok {
				releaseErr = claimRepo.ReleaseClaims(ctx, []int64{op.dbID}, *op.claimOwner)
			} else {
				releaseErr = fmt.Errorf("owner-safe pending release is unavailable")
			}
			if releaseErr != nil {
				logger.Warnf(ctx, "wiki ingest: failed to release claim for retry id=%d: %v", op.dbID, releaseErr)
				settleErrs = append(settleErrs, fmt.Errorf("release retry claim id=%d: %w", op.dbID, releaseErr))
			}
			logger.Infof(ctx, "wiki ingest: re-queued failed op %s (%s) for retry (attempt %d/%d)", op.KnowledgeID, op.DocTitle, count, wikiMaxFailRetries)
			continue
		}

		// Exhausted in-batch retries — archive and remove. The knowledge is
		// only terminal after BOTH durable operations succeed; otherwise its
		// finalizing slot remains held and the pending row stays recoverable.
		logger.Warnf(ctx, "wiki ingest: dropping op %s (%s) after %d failures (limit %d)", op.KnowledgeID, op.DocTitle, count, wikiMaxFailRetries)
		payloadBytes, marshalErr := json.Marshal(op)
		if marshalErr != nil {
			settleErrs = append(settleErrs, fmt.Errorf("marshal dead letter id=%d: %w", op.dbID, marshalErr))
			continue
		}
		deadLetter := &types.TaskDeadLetter{
			TenantID:  payload.TenantID,
			TaskType:  wikiTaskType,
			Scope:     wikiTaskScope,
			ScopeID:   payload.KnowledgeBaseID,
			RelatedID: op.KnowledgeID,
			Payload:   payloadBytes,
			LastError: fmt.Sprintf("exceeded wikiMaxFailRetries=%d (in-batch retries)", wikiMaxFailRetries),
			FailCount: count,
		}
		if op.Op == WikiOpIngest {
			consumed, err := s.finalizeWikiSubtask(ctx, op, deadLetter)
			if err != nil {
				settleErrs = append(settleErrs, fmt.Errorf("finalize dead-lettered wiki op id=%d: %w", op.dbID, err))
				continue
			}
			if consumed {
				continue
			}
		}
		if s.deadLetterRepo == nil {
			settleErrs = append(settleErrs, fmt.Errorf("archive dead letter id=%d: repository unavailable", op.dbID))
			continue
		}
		if dlErr := s.deadLetterRepo.Insert(ctx, deadLetter); dlErr != nil {
			logger.Warnf(ctx, "wiki ingest: failed to archive op %s to dead letters: %v", op.KnowledgeID, dlErr)
			settleErrs = append(settleErrs, fmt.Errorf("archive dead letter id=%d: %w", op.dbID, dlErr))
			continue
		}
		var deleteErr error
		if op.claimOwner == nil {
			deleteErr = s.pendingRepo.DeleteByIDs(ctx, []int64{op.dbID})
		} else if claimRepo, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease); ok {
			deleteErr = claimRepo.DeleteClaims(ctx, []int64{op.dbID}, *op.claimOwner)
		} else {
			deleteErr = fmt.Errorf("owner-safe pending delete is unavailable")
		}
		if deleteErr != nil {
			logger.Warnf(ctx, "wiki ingest: failed to drop dead-lettered row id=%d: %v", op.dbID, deleteErr)
			settleErrs = append(settleErrs, fmt.Errorf("drop dead-lettered row id=%d: %w", op.dbID, deleteErr))
			continue
		}
	}
	return errors.Join(settleErrs...)
}

// settleAbortedWikiBatch accounts for every claimed operation when a batch
// exits before the normal durable tail. Previously that path only released
// claims, so task-level timeouts could re-run the same rows forever without
// advancing fail_count. Completed Wiki owners are acknowledged immediately;
// unfinished owners become terminal failures before entering the bounded
// retry/dead-letter path.
func (s *wikiIngestService) settleAbortedWikiBatch(
	ctx context.Context,
	payload WikiIngestPayload,
	ops []WikiPendingOp,
	cause error,
) error {
	if len(ops) == 0 {
		return nil
	}
	if cause == nil {
		cause = errors.New("wiki ingest batch exited before durable settlement")
	}

	lookup, hasStrictLookup := s.tracker().(strictWikiSpanLookup)
	retryOps := make([]WikiPendingOp, 0, len(ops))
	var settleErrs []error
	for _, op := range ops {
		if op.Op != WikiOpIngest || op.Attempt <= 0 {
			retryOps = append(retryOps, op)
			continue
		}
		if !hasStrictLookup {
			settleErrs = append(settleErrs,
				fmt.Errorf("abort wiki op %s: strict span lookup is unavailable", op.KnowledgeID))
			retryOps = append(retryOps, op)
			continue
		}

		span, err := lookup.LookupSpanByNameStrict(ctx, op.KnowledgeID, op.Attempt, "postprocess.wiki")
		if err != nil {
			settleErrs = append(settleErrs,
				fmt.Errorf("abort wiki op %s: load owner span: %w", op.KnowledgeID, err))
			retryOps = append(retryOps, op)
			continue
		}
		if span == nil {
			span = s.beginWikiSubspan(ctx, op, types.JSONMap{"abort_recovery": true})
		}
		if span == nil {
			settleErrs = append(settleErrs,
				fmt.Errorf("abort wiki op %s: postprocess.wiki owner is unavailable", op.KnowledgeID))
			retryOps = append(retryOps, op)
			continue
		}

		switch span.Status {
		case types.SpanStatusDone, types.SpanStatusSkipped:
			if _, err := s.finalizeWikiSubtask(ctx, op, nil); err != nil {
				settleErrs = append(settleErrs,
					fmt.Errorf("ack completed wiki op %s after batch abort: %w", op.KnowledgeID, err))
			}
		case types.SpanStatusFailed, types.SpanStatusCancelled:
			retryOps = append(retryOps, op)
		default:
			s.tracker().FailSpan(
				ctx, span, "WIKI_BATCH_ABORTED",
				"Wiki batch exited before durable settlement", cause,
			)
			retryOps = append(retryOps, op)
		}
	}

	if err := s.requeueFailedOps(ctx, payload, retryOps); err != nil {
		settleErrs = append(settleErrs, err)
	}
	return errors.Join(settleErrs...)
}

// docIngestResult captures per-document info for batch post-processing.
type wikiIngestPageRef struct {
	Slug  string
	Title string
}

type docIngestResult struct {
	KnowledgeID string
	WorkID      string
	// CheckpointReused marks map output restored from a durable checkpoint.
	// Restored output must re-probe current wiki identities because another
	// document may have established a canonical slug after it was checkpointed.
	CheckpointReused bool
	SourceOp         WikiPendingOp
	DocTitle         string
	Summary          string // one-line summary of the document (from summary page)
	// Pages records the wiki pages this document touched, carrying both
	// the slug used for link/retract bookkeeping and its human-readable title.
	Pages []wikiIngestPageRef
	// MapStats are the per-doc map-phase metrics captured at the moment
	// mapOneDocument finishes. Surfaced into the postprocess.wiki span's
	// output so the trace viewer can show "what the map phase produced"
	// even though the span itself stays open until the batch's reduce +
	// cleanup phases complete (so the user-visible duration covers the
	// whole pipeline for this doc, not just LLM extraction).
	MapStats types.JSONMap
	// WikiSpan is the postprocess.wiki subspan opened at the start of
	// mapOneDocument. ProcessWikiIngest holds it open across the reduce
	// + cleanup phases and closes it once this doc's pages have all
	// been materialised — see the EndSpan call near the end of
	// ProcessWikiIngest. nil when no parent attempt was found, in which
	// case the tracker helpers are all no-ops anyway.
	WikiSpan *Span
}

// wikiPageWriteOutcome reports what the reduce phase actually persisted for
// one document. Map output is only a set of candidates: it must never be
// presented as pages_written until CreatePage/UpdatePage has returned
// successfully. Slugs that failed or timed out are kept separate so callers
// can fail/retry the Wiki branch without publishing a false-success count.
func wikiPageWriteOutcome(
	result *docIngestResult,
	persistedSlugs map[string]struct{},
	unappliedSlugs map[string]struct{},
) types.JSONMap {
	writtenPages := make([]map[string]string, 0)
	droppedPages := make([]map[string]string, 0)
	unchangedPages := make([]map[string]string, 0)
	if result != nil {
		writtenPages = make([]map[string]string, 0, len(result.Pages))
		droppedPages = make([]map[string]string, 0, len(result.Pages))
		unchangedPages = make([]map[string]string, 0, len(result.Pages))
		for _, page := range result.Pages {
			entry := map[string]string{
				"slug":  page.Slug,
				"title": previewText(page.Title, 80),
			}
			if _, ok := persistedSlugs[page.Slug]; ok {
				writtenPages = append(writtenPages, entry)
				continue
			}
			if _, ok := unappliedSlugs[page.Slug]; ok {
				droppedPages = append(droppedPages, entry)
				continue
			}
			unchangedPages = append(unchangedPages, entry)
		}
	}

	output := types.JSONMap{}
	if result != nil {
		for key, value := range result.MapStats {
			output[key] = value
		}
	}
	// Persistence facts are reserved keys. Write them after MapStats so a
	// stale/map-only metric can never overwrite the durable outcome.
	output["pages_written"] = len(writtenPages)
	output["pages_dropped"] = len(droppedPages)
	output["pages_unchanged"] = len(unchangedPages)
	output["pages_total"] = len(writtenPages) + len(droppedPages) + len(unchangedPages)
	output["failed_slug_writes"] = len(unappliedSlugs)
	output["pages_written_preview"] = writtenPages
	output["pages_unchanged_preview"] = unchangedPages
	if len(droppedPages) > 0 {
		output["pages_dropped_preview"] = droppedPages
	}
	return output
}

// WikiBatchContext holds shared data across Map and Reduce phases.
//
// Historically this carried a fully materialized `AllPages` slice plus
// pre-built SlugTitleMap / SummaryContentByKnowledgeID lookup tables.
// At 4w-document scale that meant the very first thing every batch
// did was load 100K+ wiki_pages rows (content TEXT included) into Go
// memory — and then walk them several more times for cleanDeadLinks /
// injectCrossLinks / getExistingPageSlugsForKnowledge.
//
// We now lazy-load via fetchers backed by lightweight projections
// (ListBySlugs / ListSummariesByKnowledgeIDs). Each fetcher caches
// results keyed by its input so repeat lookups within a batch are
// free; the cache is per-batch and goroutine-local-via-mutex (sync.Map
// would also work but mutex keeps the surface small).
type WikiBatchContext struct {
	// SlugTitle resolves a slug to its current title (or "" if missing).
	// Backed by ListBySlugs; cache is populated as callers ask, so we
	// only pay for the slugs we actually look at.
	SlugTitle func(ctx context.Context, slug string) string

	// SlugTitleMany batches a slug-set into a single ListBySlugs query
	// and returns the resolved titles map. Convenient when a caller
	// already has the full slug list; results are still cached.
	SlugTitleMany func(ctx context.Context, slugs []string) map[string]string

	// SummaryContentByKnowledgeID returns the surviving summary page's
	// content for the given knowledge id (or "" if no summary page
	// exists / was archived). Backed by ListSummariesByKnowledgeIDs;
	// cache is populated lazily as well.
	SummaryContentByKnowledgeID func(ctx context.Context, kid string) string

	// ExtractionGranularity drives Pass 0 (candidate slug extraction)
	// aggressiveness. Resolved once per batch from the KnowledgeBase's
	// WikiConfig so every doc in the batch sees the same scope rules.
	// Already Normalize()'d — consumers can assume it is one of the
	// three valid values.
	ExtractionGranularity types.WikiExtractionGranularity

	// ContentInstructions and ExtractionInstructions are KB-scoped business
	// guidance. Stable citation, merge, taxonomy and JSON rules remain in the
	// system templates and cannot be replaced by these fields.
	ContentInstructions    string
	ExtractionInstructions string

	// PlannedFolderID holds the per-slug wiki_folders.id assigned by the batch
	// taxonomy planning pass (planBatchTaxonomy + folder resolution), keyed by
	// page slug. Reduce applies it only to pages that aren't already filed
	// (FolderID == ""), so the whole batch lands on one coherent tree without
	// churning user-curated placements. The folders themselves are created
	// sequentially before reduce, so the parallel reduce phase only assigns
	// pre-resolved ids and never races on folder creation. Read-only during
	// reduce.
	PlannedFolderID map[string]string
}

// SlugUpdate represents a single update operation for a specific slug
type SlugUpdate struct {
	Slug        string
	Type        string        // "entity", "concept", "summary", "retract", "retractStale"
	Item        extractedItem // For entity/concept
	DocTitle    string
	KnowledgeID string
	// WorkID identifies the attempt-independent source revision whose mapped
	// output produced this contribution. It is the exact-once key used by the
	// per-slug application ledger.
	WorkID string
	// ApplicationPlanID is filled under the per-slug lock after the base page
	// snapshot is fixed. Publication uses it to atomically advance every
	// contribution marker with the page status mutation.
	ApplicationPlanID string
	// Attempt freezes the source document attempt that produced this update.
	// It is revalidated immediately before the durable page write so an old
	// worker cannot publish after a newer reparse has started. Zero is reserved
	// for legacy/manual maintenance updates that predate attempt tracking.
	Attempt   int
	SourceRef string
	// Language is the already-resolved, human-readable language name the
	// Reduce phase interpolates into the editor prompt (e.g. "Chinese
	// (Simplified)"), NOT a locale code. Map resolves it once per document
	// so every page derived from that document shares one language.
	Language          string
	SummaryBody       string // For summary
	SummaryLine       string // For summary
	RetractDocContent string // For retract / retractStale
	// SourceChunks lists the chunk IDs (within KnowledgeID) that substantively
	// support this update. Mirrors Item.SourceChunks for convenience — the
	// Reduce phase reads from here to avoid an extra field hop.
	SourceChunks []string
	// DocSummary is the document-level summary body produced by
	// WikiSummaryPrompt (everything after the SUMMARY: ... headline, falling
	// back to the raw output if no headline could be parsed out). Carried
	// here so the Reduce phase can frame cited chunks with a rich
	// <source_context> block that tells the editor model what the document
	// is about AND what kind of document it is (resume vs announcement vs
	// product page). The one-line headline alone was too terse to keep the
	// editor grounded on longer / multi-topic source documents.
	DocSummary string
}

func previewText(s string, maxRunes int) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	r := []rune(s)
	if maxRunes <= 0 || len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "...(truncated)"
}

func previewStringSlice(items []string, limit int) string {
	if len(items) == 0 {
		return "[]"
	}
	if limit <= 0 {
		limit = 1
	}
	n := len(items)
	if n > limit {
		items = items[:limit]
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, previewText(it, 48))
	}
	if n > limit {
		return fmt.Sprintf("[%s ...(+%d)]", strings.Join(out, ", "), n-limit)
	}
	return fmt.Sprintf("[%s]", strings.Join(out, ", "))
}

// previewExtractedItems returns a JSON-friendly preview of the first
// `limit` extracted entities or concepts so the trace viewer's
// postprocess.wiki.extract span shows actual names/slugs/descriptions
// instead of bare counts. Each item is trimmed to a small fixed
// budget — these end up serialised into the spans table's JSONB
// output column, so the cumulative size matters more than per-item
// fidelity.
func previewExtractedItems(items []extractedItem, limit int) []map[string]string {
	if limit <= 0 {
		limit = 1
	}
	n := len(items)
	if n > limit {
		items = items[:limit]
	}
	out := make([]map[string]string, 0, len(items))
	for _, it := range items {
		out = append(out, map[string]string{
			"name":        previewText(it.Name, 60),
			"slug":        it.Slug,
			"description": previewText(it.Description, 120),
		})
	}
	return out
}

// topCitedSlugs returns the top `limit` slugs by chunk-citation count.
// Used by postprocess.wiki.classify so the trace surfaces which
// candidate slugs the citation pass attached the most chunks to —
// useful when triaging "this LLM run extracted weird things" without
// having to open and diff full chunk lists.
func topCitedSlugs(citations map[string][]string, limit int) []map[string]any {
	if len(citations) == 0 {
		return nil
	}
	type entry struct {
		slug  string
		count int
	}
	entries := make([]entry, 0, len(citations))
	for slug, ids := range citations {
		entries = append(entries, entry{slug: slug, count: len(ids)})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].slug < entries[j].slug
	})
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			"slug":   e.slug,
			"chunks": e.count,
		})
	}
	return out
}

// previewNewSlugs returns a JSON-friendly preview of the first
// `limit` slugs that the citation pass discovered (i.e. did not appear
// in pass-0's candidate list). Surfacing these makes "the citation
// LLM kept inventing entries" trivially diagnosable from the trace
// viewer.
func previewNewSlugs(items []newSlugFromCitation, limit int) []map[string]string {
	if limit <= 0 {
		limit = 1
	}
	n := len(items)
	if n > limit {
		items = items[:limit]
	}
	out := make([]map[string]string, 0, len(items))
	for _, it := range items {
		out = append(out, map[string]string{
			"name":   previewText(it.Name, 60),
			"slug":   it.Slug,
			"type":   it.Type,
			"chunks": fmt.Sprintf("%d", len(it.SourceChunks)),
		})
	}
	return out
}

// wikiLinkRE matches `[[slug]]` and `[[slug|display text]]` references
// inside wiki page content. The slug capture group rejects whitespace and
// the closing-bracket / pipe characters so we don't accidentally swallow
// adjacent text. Display text (group 2) is optional.
var wikiLinkRE = regexp.MustCompile(`\[\[([^\[\]\|\s]+)(?:\|([^\]]+))?\]\]`)

// sanitizeDeadSummaryLinks rewrites the summary pages produced by THIS
// batch to fix `[[slug]]` / `[[slug|display]]` references that point
// at slugs whose entity/concept page generation failed in reduce.
//
// Background: WikiSummaryPrompt instructs the LLM to embed wiki links
// for every extracted slug it knows about, but slug extraction happens
// during map (parallel with summary generation) and the actual page
// creation happens later in reduce. When reduce's WikiPageModifyUserPrompt
// fails on an entity/concept slug the page never gets written — and
// the already-persisted summary is left holding a `[[entity/foo|name]]`
// link that 404s.
//
// We pass the batch's affected-slug set + the SlugTitleMany fetcher
// to the resolver so that LLM-mangled slugs (e.g. extra pinyin hyphens
// in "shang-hai-tower" vs "shanghai-tower") are healed in place rather
// than stripped to plain text — preserving cross-link information
// whenever the display text or surface form unambiguously identifies a
// live page.
//
// Pure text replacement, no LLM call. Scoped to the doc-summary slugs
// in this batch (`summary/<slugify(knowledgeID)>`), keeping the work
// proportional to batch size.
func (s *wikiIngestService) sanitizeDeadSummaryLinks(
	ctx context.Context,
	kbID string,
	docResults []*docIngestResult,
	failedSlugs map[string]struct{},
	batchCtx *WikiBatchContext,
) {
	if len(failedSlugs) == 0 || len(docResults) == 0 {
		return
	}
	// Build a (live-slug-set, title->slug) pair the resolver can consult.
	// We seed liveSlugs from batchCtx (the slugs that DID make it into
	// pages this batch) and expand it lazily as needed via SlugTitleMany.
	// titleToSlug is filled with the same successful pages' titles so the
	// display-text reverse lookup works on first try.
	for _, r := range docResults {
		if r == nil || r.KnowledgeID == "" {
			continue
		}
		summarySlug := "summary/" + slugify(r.KnowledgeID)
		page, err := s.wikiService.GetPageBySlug(ctx, kbID, summarySlug)
		if err != nil || page == nil {
			continue
		}

		// Collect the slugs this summary actually links to (so the
		// resolver has a non-empty pool of candidates), plus all the
		// successfully-written sibling pages from the same doc. These
		// two sets together cover the LLM-vs-actual mismatch cases
		// without paying for a full ListAll scan.
		candidateSlugs := make(map[string]struct{}, len(page.OutLinks)+len(r.Pages))
		for _, slug := range page.OutLinks {
			candidateSlugs[slug] = struct{}{}
		}
		for _, ref := range r.Pages {
			if _, bad := failedSlugs[ref.Slug]; bad {
				continue
			}
			candidateSlugs[ref.Slug] = struct{}{}
		}
		liveSlugs, titleToSlug := s.resolveLiveSlugs(ctx, batchCtx, candidateSlugs)

		newContent, changed := stripDeadWikiLinks(page.Content, failedSlugs, liveSlugs, titleToSlug)
		if !changed {
			continue
		}
		page.Content = newContent
		if err := s.wikiService.UpdateAutoLinkedContent(ctx, page); err != nil {
			logger.Warnf(ctx, "wiki ingest: failed to sanitize dead links in summary %s: %v", summarySlug, err)
			continue
		}
		logger.Infof(ctx, "wiki ingest: sanitized dead [[slug]] refs in summary %s", summarySlug)
	}
}

// resolveLiveSlugs builds the (liveSlugs, titleToSlug) pair that
// stripDeadWikiLinks / cleanDeadLinks pass into resolveDeadSlug.
//
// We start from a caller-supplied candidate set (typically the page's
// own out-links + this batch's freshly-written slugs) and ask the
// batch's SlugTitleMany fetcher to resolve them in one batched query.
// The fetcher already filters out archived / system pages, so missing
// entries naturally translate to "not live" without an extra check.
//
// titleToSlug is keyed by the page's exact title only — we don't have
// aliases in the lite projection. That's an acceptable trade-off: the
// reported breakage pattern is "slug munged, display = title", not
// "slug munged, display = alias", so display-by-title carries the
// majority of the rescue value at a fraction of the storage cost.
func (s *wikiIngestService) resolveLiveSlugs(
	ctx context.Context,
	batchCtx *WikiBatchContext,
	candidates map[string]struct{},
) (map[string]struct{}, map[string]string) {
	if len(candidates) == 0 || batchCtx == nil || batchCtx.SlugTitleMany == nil {
		return nil, nil
	}
	slugList := make([]string, 0, len(candidates))
	for s := range candidates {
		slugList = append(slugList, s)
	}
	titles := batchCtx.SlugTitleMany(ctx, slugList)
	live := make(map[string]struct{}, len(titles))
	titleToSlug := make(map[string]string, len(titles))
	for slug, title := range titles {
		live[slug] = struct{}{}
		if title != "" {
			titleToSlug[title] = slug
		}
	}
	return live, titleToSlug
}

// stripDeadWikiLinks rewrites `[[slug]]` / `[[slug|display]]` references
// whose `slug` falls into the dead set. The handling depends on whether
// the dead slug can be repaired:
//
//   - If the resolver maps the dead slug to a live one (typically via
//     display-text reverse lookup or hyphen-normalized equality —
//     see resolveDeadSlug), the link is REWRITTEN with the corrected
//     slug. Display text is preserved.
//   - If no live candidate is close enough, the link is STRIPPED to
//     plain text (display text when present; otherwise a humanized
//     last-segment of the slug). This is the original behaviour.
//
// The resolver is optional: when liveSlugs / titleToSlug are nil or
// empty, every dead slug falls through to the strip path. This keeps
// backward compatibility for tests / call sites that don't yet wire
// the resolution data.
func stripDeadWikiLinks(
	content string,
	deadSlugs map[string]struct{},
	liveSlugs map[string]struct{},
	titleToSlug map[string]string,
) (string, bool) {
	if len(deadSlugs) == 0 || content == "" {
		return content, false
	}
	changed := false
	out := wikiLinkRE.ReplaceAllStringFunc(content, func(match string) string {
		sub := wikiLinkRE.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		slug := sub[1]
		if _, dead := deadSlugs[slug]; !dead {
			return match
		}
		display := ""
		if len(sub) >= 3 {
			display = strings.TrimSpace(sub[2])
		}

		// (1) Try fuzzy resolve before falling back to strip. The
		// resolver consults display-text reverse lookup, hyphen-
		// normalized equality, and bigram similarity in that order;
		// returns "" only when no candidate is safe.
		if resolved, ok := resolveDeadSlug(slug, display, liveSlugs, titleToSlug); ok && resolved != slug {
			changed = true
			if display != "" {
				return "[[" + resolved + "|" + display + "]]"
			}
			return "[[" + resolved + "]]"
		}

		// (2) Strip — best-effort plain text. Prefer the LLM-supplied
		// display text; otherwise humanize the slug's last path segment
		// so the prose stays readable.
		changed = true
		if display != "" {
			return display
		}
		parts := strings.Split(slug, "/")
		label := parts[len(parts)-1]
		label = strings.ReplaceAll(label, "-", " ")
		return label
	})
	return out, changed
}

// cleanDeadLinks rewrites `[[slug]]` references in the batch's affected
// pages whose targets no longer exist (or were archived). Pure text
// cleanup — no LLM call.
//
// Scope is intentionally limited to the slugs touched by this batch:
// at 4w-document scale the legacy "scan every page in the KB" path was
// the dominant tail in the post-batch phase, and the long-tail
// historical dead links are better handled by the lint AutoFix pipeline
// (which runs out-of-band and can afford a full table walk).
//
// For each affected page:
//
//  1. Pull its lite projection (out_links + status) via the batch's
//     SlugTitle fetcher (one IN query for the whole affected set,
//     amortized via the batchCtx cache).
//  2. Probe the union of out-link targets through ExistsSlugs to
//     classify them as live vs dead.
//  3. For each dead link, try resolveDeadSlug first; rewrite if a
//     safe candidate exists, otherwise strip to plain text.
//  4. Persist the rewritten content via UpdateAutoLinkedContent so
//     the version counter stays unchanged (this is a maintenance
//     pass, not a user-visible edit).
func (s *wikiIngestService) cleanDeadLinks(ctx context.Context, kbID string, affectedSlugs []string, batchCtx *WikiBatchContext) {
	if len(affectedSlugs) == 0 {
		return
	}

	// (1) Load the affected pages' content + out-links in one go.
	// We need the full WikiPage rows here (not just lite projections)
	// because we're going to rewrite content; the lite path saves
	// nothing once we're touching content anyway.
	cleaned := 0
	for _, slug := range affectedSlugs {
		page, err := s.wikiService.GetPageBySlug(ctx, kbID, slug)
		if err != nil || page == nil {
			continue
		}
		if page.Status == types.WikiPageStatusArchived {
			continue
		}
		if page.PageType == types.WikiPageTypeIndex {
			continue
		}
		if len(page.OutLinks) == 0 {
			continue
		}

		// (2) Classify out-links as live vs dead via one batched
		// ExistsSlugs query. Empty slug list → no-op.
		liveMap, err := s.wikiService.ExistsSlugs(ctx, kbID, []string(page.OutLinks))
		if err != nil {
			logger.Warnf(ctx, "wiki: ExistsSlugs failed during dead-link cleanup for %s: %v", slug, err)
			continue
		}
		deadSlugs := make(map[string]struct{})
		liveSlugs := make(map[string]struct{}, len(liveMap))
		for outSlug, alive := range liveMap {
			if alive {
				liveSlugs[outSlug] = struct{}{}
			} else {
				deadSlugs[outSlug] = struct{}{}
			}
		}
		if len(deadSlugs) == 0 {
			continue
		}

		// (3) Build the title->slug reverse-lookup map for fuzzy
		// resolve. We pull titles for the live slugs only — those
		// are the candidates a dead reference could be remapped to.
		titles := batchCtx.SlugTitleMany(ctx, []string(page.OutLinks))
		titleToSlug := make(map[string]string, len(titles))
		for s, t := range titles {
			if t != "" {
				titleToSlug[t] = s
			}
		}

		newContent, changed := stripDeadWikiLinks(page.Content, deadSlugs, liveSlugs, titleToSlug)
		if !changed {
			continue
		}

		// (4) Persist. UpdateAutoLinkedContent skips the version bump
		// because dead-link cleanup is a machine-only edit.
		page.Content = newContent
		if err := s.wikiService.UpdateAutoLinkedContent(ctx, page); err != nil {
			logger.Warnf(ctx, "wiki: failed to clean dead links in page %s: %v", page.Slug, err)
			continue
		}
		cleaned++
	}

	if cleaned > 0 {
		logger.Infof(ctx, "wiki: cleaned dead links in %d pages", cleaned)
	}
}

// injectCrossLinks scans the batch's affected pages and injects
// `[[wiki-links]]` for mentions of other wiki page titles / aliases
// in the content. Pure text replacement, no LLM call.
//
// Scope is intentionally limited to two slug sets:
//
//  1. The affected pages themselves — we only rewrite their content.
//  2. The candidate refs come from (a) the affected pages' existing
//     out-links (already known to be relevant via prior linkification
//     or manual edits) plus (b) the batch's freshly-written sibling
//     slugs supplied via `linkRefs` from the caller.
//
// At 4w-document scale this is the difference between loading 100K+
// pages just to find link candidates vs O(batch-size) lookups. We
// trade off some long-tail recall (a brand new entity in this batch
// won't be linkified into pages from previous batches until they get
// re-edited), but lint AutoFix is the right place for that.
//
// linkifyContent does the actual matching work, including code-block /
// existing-link / word-boundary exclusions.
func (s *wikiIngestService) injectCrossLinks(
	ctx context.Context,
	kbID string,
	affectedSlugs []string,
	freshRefs []linkRef,
	batchCtx *WikiBatchContext,
) {
	if len(affectedSlugs) == 0 {
		return
	}

	updated := 0
	for _, slug := range affectedSlugs {
		page, err := s.wikiService.GetPageBySlug(ctx, kbID, slug)
		if err != nil || page == nil {
			continue
		}
		if page.PageType == types.WikiPageTypeIndex {
			continue
		}

		// Build the per-page candidate ref set: the existing out-links
		// (resolved via the batch's title fetcher to skip archived /
		// system pages) plus the freshly-written sibling slugs from
		// this batch.
		var refs []linkRef
		if len(page.OutLinks) > 0 {
			titles := batchCtx.SlugTitleMany(ctx, []string(page.OutLinks))
			for outSlug, title := range titles {
				if title == "" || outSlug == slug {
					continue
				}
				refs = append(refs, linkRef{slug: outSlug, matchText: title})
			}
		}
		for _, fr := range freshRefs {
			if fr.slug == slug {
				continue
			}
			refs = append(refs, fr)
		}
		if len(refs) == 0 {
			continue
		}

		newContent, changed := linkifyContent(page.Content, refs, page.Slug)
		if !changed {
			continue
		}
		page.Content = newContent
		if err := s.wikiService.UpdateAutoLinkedContent(ctx, page); err != nil {
			logger.Warnf(ctx, "wiki ingest: cross-link injection failed for %s: %v", page.Slug, err)
			continue
		}
		updated++
	}

	if updated > 0 {
		logger.Infof(ctx, "wiki ingest: injected cross-links in %d pages", updated)
	}
}

// collectLinkRefs flattens (title + aliases) of all non-system pages into a
// single linkRef slice suitable for linkifyContent.
func collectLinkRefs(pages []*types.WikiPage) []linkRef {
	refs := make([]linkRef, 0, len(pages)*2)
	for _, p := range pages {
		if p.PageType == types.WikiPageTypeIndex {
			continue
		}
		if p.Title != "" {
			refs = append(refs, linkRef{slug: p.Slug, matchText: p.Title})
		}
		for _, alias := range p.Aliases {
			if alias != "" {
				refs = append(refs, linkRef{slug: p.Slug, matchText: alias})
			}
		}
	}
	return refs
}

// wikiTaxonomyPromptMaxPaths caps how many existing folders are rendered into a
// planning prompt as the set to reuse. Reached only for pathologically large
// taxonomies; the similarity preprocessing keeps the fed set well under it.
const wikiTaxonomyPromptMaxPaths = 150

// wikiTaxonomyFolderPoolMax bounds the existing folders pulled from the DB as the
// candidate pool for similarity selection. Distinct folders are few even for
// large KBs, so this only guards against a degenerate taxonomy.
const wikiTaxonomyFolderPoolMax = 400

// wikiTaxonomyFeedAllMaxFolders is the folder count at or below which the whole
// folder set is fed to the planner as-is: a healthy navigation directory is
// small, so feeding everything gives perfect reuse recall with no embedding cost
// (similarity preprocessing only earns its keep once folders are numerous).
const wikiTaxonomyFeedAllMaxFolders = 60

// wikiTaxonomyRelevantTopK is how many nearest existing deeper folders each item
// contributes to the reuse set when similarity preprocessing kicks in.
const wikiTaxonomyRelevantTopK = 3

// wikiTaxonomyPlanChunkSize caps how many items go into a single planning call.
// Larger batches are split into chunks; folders assigned by earlier chunks are
// fed forward as "existing folders" so later chunks converge onto the same tree.
const wikiTaxonomyPlanChunkSize = 60

const wikiTaxonomyEmptyTreeHint = "(none yet — this knowledge base has no folders, design a fresh directory)"

type wikiTaxonomyNode struct {
	children map[string]*wikiTaxonomyNode
}

func insertWikiTaxonomyPath(root *wikiTaxonomyNode, path []string) {
	if root == nil || len(path) == 0 {
		return
	}
	cur := root
	for _, part := range path {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if cur.children == nil {
			cur.children = make(map[string]*wikiTaxonomyNode)
		}
		child := cur.children[part]
		if child == nil {
			child = &wikiTaxonomyNode{}
			cur.children[part] = child
		}
		cur = child
	}
}

func appendWikiTaxonomyNode(buf *strings.Builder, label string, node *wikiTaxonomyNode, depth int) {
	if label != "" {
		fmt.Fprintf(buf, "%s%s\n", strings.Repeat("  ", depth), label)
	}
	if node == nil || len(node.children) == 0 {
		return
	}
	keys := make([]string, 0, len(node.children))
	for k := range node.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		appendWikiTaxonomyNode(buf, k, node.children[k], depth+1)
	}
}

// formatExistingTaxonomyForPrompt renders distinct category_path values as an
// indented folder tree for LLM extraction prompts.
func formatExistingTaxonomyForPrompt(paths [][]string) string {
	if len(paths) == 0 {
		return ""
	}
	root := &wikiTaxonomyNode{}
	for _, path := range paths {
		insertWikiTaxonomyPath(root, path)
	}
	if len(root.children) == 0 {
		return ""
	}
	var buf strings.Builder
	keys := make([]string, 0, len(root.children))
	for k := range root.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		appendWikiTaxonomyNode(&buf, k, root.children[k], 0)
	}
	return strings.TrimSpace(buf.String())
}

// getExistingPageSlugsForKnowledge returns all page slugs that currently
// reference a given knowledge ID in their source_refs. Used to snapshot
// state before re-ingest so the reduce phase can reconcile additions vs
// retractions.
//
// Backed by idx_wiki_pages_source_refs (GIN jsonb_path_ops, migration
// 000041) and the legacy text-index fallback for "kid|title" entries.
// We project to slugs only — no need to load full row content for a
// per-doc snapshot.
//
// Index/log slugs (wiki-intrinsic system pages) never carry real
// source_refs in practice, but we filter them out explicitly here as
// a defense-in-depth measure: an old buggy ingest that mistakenly
// stamped a system page with a knowledge ref would otherwise show up
// in the reparse "old set" and confuse the reduce stage.
func (s *wikiIngestService) getExistingPageSlugsForKnowledge(
	ctx context.Context, kbID, knowledgeID string,
) (map[string]bool, error) {
	slugs, err := s.wikiService.ListSlugsBySourceRef(ctx, kbID, knowledgeID)
	if err != nil {
		return nil, fmt.Errorf("list existing Wiki pages for %s: %w", knowledgeID, err)
	}
	if len(slugs) == 0 {
		return nil, nil
	}
	out := make(map[string]bool, len(slugs))
	for _, slug := range slugs {
		// Defense-in-depth: skip wiki-intrinsic slugs that never have
		// real source refs.
		if slug == "index" {
			continue
		}
		out[slug] = true
	}
	return out, nil
}

// retractStalePages handles pages that were previously linked to this document
// but are no longer produced by the updated extraction.
// - Single-source stale pages → deleted
// - Multi-source stale pages → LLM retract to clean content synchronously

// Build set of newly affected slugs (including summary)

// Stale = was in old set but not in new set

// Remove this doc's source ref

// No other sources → delete the page

// Multi-source → remove ref, queue retract

// extractedItem represents a single extracted entity or concept.
//
// SourceChunks holds the stable chunk IDs (from the source document) that
// substantively discuss this item. Populated by the chunk-citation pass; when
// non-empty the Reduce phase uses these chunks verbatim as the item's
// evidence instead of the shorter Description/Details fields.
type extractedItem struct {
	Name         string   `json:"name"`
	Slug         string   `json:"slug"`
	Aliases      []string `json:"aliases"`
	Description  string   `json:"description"`
	Details      string   `json:"details"`
	SourceChunks []string `json:"source_chunks,omitempty"`
}

// combinedExtraction represents the parsed result of the combined entity+concept extraction
type combinedExtraction struct {
	Entities []extractedItem `json:"entities"`
	Concepts []extractedItem `json:"concepts"`
}

// rebuildIndexPage refreshes the LLM-generated intro that sits on the
// index wiki_pages row.
//
// History: the index page used to store "intro + full directory listing" as
// a single multi-MB markdown blob in content. Every ingest batch rewrote
// the whole column, which on KBs with tens of thousands of pages caused
// O(N) TOAST writes per batch. The directory was lifted out into the
// structured GET /wiki/index endpoint (see wikiPageService.GetIndexView),
// and this method now only maintains the intro.
//
// Intro lifecycle:
//   - First time (empty or legacy placeholder): generate from all document
//     summaries via WikiIndexIntroPrompt.
//   - Subsequent calls with a change description: incremental update via
//     WikiIndexIntroUpdatePrompt so the intro reflects what just landed.
//   - No change description: keep the existing intro untouched.
//
// The new intro is written to both Content and Summary so readers that
// still fall back to Summary (older clients, legacy migrations) stay in
// sync with the column the view actually renders.
// indexIntroSummaryCap caps how many summary pages we feed into the
// LLM when generating the wiki index intro from scratch. A 4w-document
// KB would otherwise blow the context window every batch, and the
// intro is a "set the scene" artifact where the most-recently-touched
// documents carry disproportionately more signal anyway. We pick the
// top-N most-recently-updated summaries and add a "showing N of M"
// hint to the prompt so the LLM can be honest about its sample.
const indexIntroSummaryCap = 200

// rebuildIndexPage refreshes the LLM-generated intro on the index
// page. Two paths:
//
//   - First-time generation (no existing intro, or only the legacy
//     placeholder): the LLM gets a CAPPED window of the most recent
//     summary pages (most-recently-updated wins). Compare with the
//     legacy path which loaded ALL summaries — at 4w-document scale
//     that produced multi-MB prompts that simply broke the context
//     window and silently fell back to a hardcoded intro.
//   - Incremental update: the LLM gets only the existing intro plus
//     the change description for THIS batch. Document summaries are
//     intentionally NOT included — at scale the change-description
//     alone is enough signal for "what landed?", and excluding the
//     full summary set keeps the prompt size bounded regardless of
//     KB size.
//
// The intro is written to both Content and Summary so legacy readers
// that fall through to Summary stay in sync.
func (s *wikiIngestService) rebuildIndexPage(ctx context.Context, chatModel chat.Chat, payload WikiIngestPayload,
	changeDesc, lang, customInstructions string,
) error {
	indexPage, _ := s.wikiService.GetIndex(ctx, payload.KnowledgeBaseID)
	if indexPage == nil {
		return nil
	}

	// The intro lives on both Content and Summary. Prefer Content since
	// that's what the new index view returns; fall back to Summary for
	// rows written before this refactor so the incremental-update prompt
	// has something to work with.
	existingIntro := strings.TrimSpace(indexPage.Content)
	if existingIntro == "" {
		existingIntro = strings.TrimSpace(indexPage.Summary)
	}
	// Detect the legacy "intro + directory" payload. Such rows embed the
	// fence-separated "## Summary" sections right after the intro, so we
	// clip everything from the first directory heading onward to keep the
	// intro length bounded when we feed it back into the update prompt.
	if idx := strings.Index(existingIntro, "\n## "); idx >= 0 {
		existingIntro = strings.TrimSpace(existingIntro[:idx])
	}
	indexWorkRevision := wikiCheckpointDigest(
		"index", indexPage.ID, strconv.Itoa(indexPage.Version), existingIntro,
		changeDesc, lang, customInstructions,
	)
	ctx = withWikiGenerationScope(ctx, wikiGenerationScope{
		TenantID: payload.TenantID, KnowledgeBaseID: payload.KnowledgeBaseID,
		WorkRevision:    indexWorkRevision,
		RuntimeSnapshot: wikiRuntimeSnapshotDigest(chatModel, lang),
	})

	var intro string
	switch {
	case existingIntro == "" || existingIntro == "Wiki index - table of contents":
		// First-time generation: pull the top-N most-recent summary
		// pages via the lite projection. CountByType lets us tell the
		// LLM "showing N of M" so it can frame the intro honestly when
		// the KB is bigger than what we're sampling.
		recentSummaries, listErr := s.wikiService.ListByTypeRecent(ctx, payload.KnowledgeBaseID, types.WikiPageTypeSummary, indexIntroSummaryCap)
		if listErr != nil {
			return listErr
		}
		var docSummaries strings.Builder
		for _, e := range recentSummaries {
			fmt.Fprintf(&docSummaries, "<document>\n<title>%s</title>\n<summary>%s</summary>\n</document>\n\n", e.Title, e.Summary)
		}
		// Best-effort total count for the framing hint. CountByType
		// counts every page type; we need just summary, so we read
		// directly. A failure here doesn't block intro generation.
		totalSummaries := int64(len(recentSummaries))
		if counts, cntErr := s.wikiService.CountByType(ctx, payload.KnowledgeBaseID); cntErr == nil {
			if t, ok := counts[types.WikiPageTypeSummary]; ok {
				totalSummaries = t
			}
		}
		framing := ""
		if int(totalSummaries) > len(recentSummaries) && len(recentSummaries) > 0 {
			framing = fmt.Sprintf("(showing %d most recent of %d total documents)\n\n", len(recentSummaries), totalSummaries)
		}
		if docSummaries.Len() == 0 {
			docSummaries.WriteString("(no documents yet)")
		}
		generatedIntro, genErr := s.generateWithTemplate(ctx, chatModel, agent.WikiIndexIntroPrompt, map[string]string{
			"DocumentSummaries":  framing + docSummaries.String(),
			"Language":           lang,
			"CustomInstructions": customInstructions,
			"InstructionScope":   "wiki_content",
		})
		if genErr != nil {
			return fmt.Errorf("generate wiki index intro: %w", genErr)
		}
		intro = strings.TrimSpace(generatedIntro)
	case changeDesc != "":
		// Incremental update: only the existing intro + this batch's
		// change description go into the prompt. We deliberately stop
		// passing the full DocumentSummaries set here — at 4w docs it
		// would re-flood the context every batch, and the
		// change-description block already encodes the "what just
		// changed" signal the prompt is asking for.
		updatedIntro, genErr := s.generateWithTemplate(ctx, chatModel, agent.WikiIndexIntroUpdatePrompt, map[string]string{
			"ExistingIntro":      existingIntro,
			"ChangeDescription":  changeDesc,
			"DocumentSummaries":  "",
			"Language":           lang,
			"CustomInstructions": customInstructions,
			"InstructionScope":   "wiki_content",
		})
		if genErr != nil {
			return fmt.Errorf("update wiki index intro: %w", genErr)
		}
		intro = strings.TrimSpace(updatedIntro)
	default:
		// No change description and an existing intro: leave it as-is so
		// we don't bump the version for a no-op.
		intro = existingIntro
	}

	// Defensive: some LLM outputs occasionally bleed into a directory-
	// like section even when the intro prompt doesn't ask for one. If
	// the freshly-generated intro starts to look like a legacy payload,
	// clip it at the first "\n## " just like we did on the read path
	// above. This keeps indexPage.Content a bounded intro-only blob.
	if idx := strings.Index(intro, "\n## "); idx >= 0 {
		intro = strings.TrimSpace(intro[:idx])
	}

	indexPage.Content = intro
	indexPage.Summary = intro
	if _, err := s.wikiService.UpdatePage(ctx, indexPage); err != nil {
		return err
	}
	if err := s.markWikiGenerationFragmentsSucceeded(ctx, indexWorkRevision); err != nil {
		return fmt.Errorf("settle wiki index generation: %w", err)
	}
	return nil
}

// splitSummaryLine extracts the "SUMMARY: ..." line from LLM output.
// Returns (summary, content). If no SUMMARY line found, summary is empty.
func splitSummaryLine(raw string) (summary string, content string) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "SUMMARY:") || strings.HasPrefix(raw, "SUMMARY：") {
		idx := strings.IndexByte(raw, '\n')
		if idx < 0 {
			// Only one line
			return strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(raw, "SUMMARY:"), "SUMMARY：")), ""
		}
		summaryLine := raw[:idx]
		summaryLine = strings.TrimPrefix(summaryLine, "SUMMARY:")
		summaryLine = strings.TrimPrefix(summaryLine, "SUMMARY：")
		return strings.TrimSpace(summaryLine), strings.TrimSpace(raw[idx+1:])
	}
	return "", raw
}

// publishDraftPages transitions draft pages to published status after ingest completes.
// This ensures users don't see half-built pages during the ingest process.
func (s *wikiIngestService) publishDraftPages(
	ctx context.Context,
	tenantID uint64,
	kbID string,
	slugs []string,
	updatesBySlug map[string][]SlugUpdate,
) map[string]error {
	persistCtx, persistCancel := wikiPagePersistContext(ctx)
	defer persistCancel()
	ctx = persistCtx
	failed := make(map[string]error)
	writer, ok := s.wikiService.(guardedWikiPageWriter)
	if !ok {
		for _, slug := range slugs {
			failed[slug] = errors.New("guarded page writer is unavailable")
		}
		return failed
	}
	for _, slug := range slugs {
		page, err := s.wikiService.GetPageBySlug(ctx, kbID, slug)
		if err != nil {
			failed[slug] = fmt.Errorf("load draft page %s: %w", slug, err)
			continue
		}
		if page == nil {
			failed[slug] = fmt.Errorf("load draft page %s: repository returned no page", slug)
			continue
		}
		updates := updatesBySlug[slug]
		publishCtx := ctx
		planID := ""
		_, _, markers := wikiSlugContributionIdentity(slug, updates)
		for _, update := range updates {
			if update.ApplicationPlanID == "" {
				continue
			}
			if planID != "" && planID != update.ApplicationPlanID {
				failed[slug] = fmt.Errorf("publish draft page %s: conflicting application plans", slug)
				planID = ""
				break
			}
			planID = update.ApplicationPlanID
		}
		if _, failedPlan := failed[slug]; failedPlan {
			continue
		}
		if planID == "" && len(markers) > 0 {
			contributionKey, _, _ := wikiSlugContributionIdentity(slug, updates)
			store, storeErr := s.checkpointStore()
			if storeErr != nil {
				failed[slug] = fmt.Errorf("publish draft page %s: restore application plan: %w", slug, storeErr)
				continue
			}
			application, findErr := store.FindWikiSlugApplication(ctx, tenantID, kbID, slug, contributionKey)
			if findErr != nil {
				failed[slug] = fmt.Errorf("publish draft page %s: restore application plan: %w", slug, findErr)
				continue
			}
			if application == nil {
				failed[slug] = fmt.Errorf("publish draft page %s: applying application plan was not found", slug)
				continue
			}
			planID = application.PlanID
			bindWikiSlugApplicationPlan(updates, planID)
			updatesBySlug[slug] = updates
		}
		if planID != "" && len(markers) > 0 {
			publishCtx = types.WithWikiSlugApplicationTransition(ctx, types.WikiSlugApplicationTransition{
				PlanID: planID, State: types.WikiSlugApplicationPublished, Markers: markers,
			})
		}
		if page.Status != types.WikiPageStatusDraft && page.Status != types.WikiPageStatusPublished &&
			!isCompletedArchivedRetraction(page, updates) {
			failed[slug] = fmt.Errorf("publish draft page %s: status changed to %s", slug, page.Status)
			continue
		}
		if page.Status == types.WikiPageStatusDraft || planID != "" {
			if page.Status == types.WikiPageStatusDraft {
				page.Status = types.WikiPageStatusPublished
			}
			guards := make([]types.WikiSourceAttemptGuard, 0, len(updates))
			for _, update := range updates {
				if update.KnowledgeID != "" && update.Attempt >= 0 &&
					!(update.Attempt == 0 && (update.Type == "retract" || update.Type == "retractStale")) {
					guards = append(guards, types.WikiSourceAttemptGuard{
						KnowledgeID: update.KnowledgeID,
						Attempt:     update.Attempt,
					})
				}
			}
			if err := writer.UpdatePageMetaGuarded(publishCtx, page, guards); err != nil {
				failed[slug] = fmt.Errorf("publish draft page %s: %w", slug, err)
			}
		}
	}
	return failed
}

func isCompletedArchivedRetraction(page *types.WikiPage, updates []SlugUpdate) bool {
	if page == nil || page.Status != types.WikiPageStatusArchived || len(page.SourceRefs) != 0 || len(updates) == 0 {
		return false
	}
	for _, update := range updates {
		if update.Type != "retract" && update.Type != "retractStale" {
			return false
		}
	}
	return true
}

// writeDedupCandidateGroup renders one new item together with its own
// similarity-candidate existing pages, nested under a <candidates> element.
// This per-item grouping is what constrains the dedup model to local
// decisions (see the grouping rationale in deduplicateExtractedBatch). The
// candidate pages keep their aliases so the model still has the acronym /
// translation signal it needs to accept a legitimate merge.
func writeDedupCandidateGroup(
	buf *strings.Builder, item extractedItem, itemType string, candidates []*types.WikiPageLite,
) {
	fmt.Fprintf(buf, "  <item slug=%q type=%q>\n", item.Slug, itemType)
	fmt.Fprintf(buf, "    <name>%s</name>\n", xmlEscape(item.Name))
	for _, alias := range item.Aliases {
		if alias == "" {
			continue
		}
		fmt.Fprintf(buf, "    <alias>%s</alias>\n", xmlEscape(alias))
	}
	buf.WriteString("    <candidates>\n")
	for _, p := range candidates {
		if p == nil {
			continue
		}
		fmt.Fprintf(buf, "      <page slug=%q type=%q>\n", p.Slug, p.PageType)
		fmt.Fprintf(buf, "        <name>%s</name>\n", xmlEscape(p.Title))
		for _, alias := range []string(p.Aliases) {
			if alias == "" {
				continue
			}
			fmt.Fprintf(buf, "        <alias>%s</alias>\n", xmlEscape(alias))
		}
		buf.WriteString("      </page>\n")
	}
	buf.WriteString("    </candidates>\n")
	buf.WriteString("  </item>\n")
}

// xmlEscape escapes the minimal set of characters that can break XML text
// content. Slugs are ASCII-only so they don't need escaping when used as
// attribute values.
func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// deduplicateExtractedBatch deduplicates both entities and concepts against
// existing wiki pages in a single LLM call. Uses pre-loaded allPages to avoid
// redundant DB queries. This replaces the two separate deduplicateItems calls
// that each queried ListAllPages + made a separate LLM call.
// deduplicateExtractedBatch deduplicates both entities and concepts against
// existing wiki pages in a single LLM call. Pre-filters candidates via the
// pg_trgm trigram index on lower(title) — every new item issues a
// FindSimilarPages probe and the union of top-K hits across all items is
// the candidate set. This replaces the legacy "ListAllPages + Go-side
// surface-form Jaccard" path that scaled O(P × N) on large KBs.
//
// The KB-id-keyed query relies on idx_wiki_pages_title_trgm (added in
// migration 000041); pg_search environments load pg_trgm in the same
// init step (see migrations/paradedb/00-init-db.sql).
func (s *wikiIngestService) deduplicateExtractedBatch(
	ctx context.Context,
	chatModel chat.Chat,
	kbID string,
	entities, concepts []extractedItem,
) ([]extractedItem, []extractedItem) {
	entities, concepts = compactExtractedItems(entities, concepts)
	if len(entities) == 0 && len(concepts) == 0 {
		return entities, concepts
	}
	if s.wikiService == nil {
		return entities, concepts
	}

	// Build the candidate set: for each new item, ask the repo for
	// the top-K trigram-similar pages and union the results. Dedup by
	// slug as we go so the prompt only carries each candidate once.
	//
	// itemCandidates additionally records, per new item, the slugs that
	// surfaced for THAT item specifically. The prompt only ever sees the
	// flattened union, so validMerge below uses this per-item scoping to
	// reject a merge whose target was pulled in for a *different* item —
	// the class of hallucination the union otherwise enables (e.g. weak
	// models emitting entity/tencent-open → entity/hiring-agent, which
	// share no trigram signal and were never candidates for each other).
	candidatePages := make(map[string]*types.WikiPageLite)
	itemCandidates := make(map[string]map[string]bool)
	probe := func(item extractedItem) {
		queries := make([]string, 0, 1+len(item.Aliases))
		if item.Name != "" {
			queries = append(queries, item.Name)
		}
		for _, alias := range item.Aliases {
			if alias != "" {
				queries = append(queries, alias)
			}
		}
		own := itemCandidates[item.Slug]
		if own == nil {
			own = make(map[string]bool)
			itemCandidates[item.Slug] = own
		}
		for _, q := range queries {
			pages, err := s.wikiService.FindSimilarPages(ctx, kbID, q,
				[]string{types.WikiPageTypeEntity, types.WikiPageTypeConcept},
				dedupCandidateTopK)
			if err != nil {
				logger.Warnf(ctx, "wiki ingest: dedup FindSimilarPages(%q) failed: %v", q, err)
				continue
			}
			for _, p := range pages {
				if p == nil || p.Slug == "" {
					continue
				}
				if _, ok := candidatePages[p.Slug]; !ok {
					candidatePages[p.Slug] = p
				}
				own[p.Slug] = true
			}
		}
	}
	for _, e := range entities {
		probe(e)
	}
	for _, c := range concepts {
		probe(c)
	}
	if len(candidatePages) == 0 {
		// No similar existing pages — nothing to merge against. The
		// items pass through unchanged.
		logger.Infof(ctx, "wiki ingest: no similar existing pages found for %d new items", len(entities)+len(concepts))
		return compactExtractedItems(entities, concepts)
	}
	logger.Infof(ctx, "wiki ingest: %d similar existing pages selected for %d new items",
		len(candidatePages), len(entities)+len(concepts))

	// Exact identities do not need an LLM judgment. Resolve them first and
	// exclude them from the fuzzy prompt. This both stabilizes canonical slug
	// selection and ensures exact matches still merge when the dedup LLM is
	// unavailable or emits malformed JSON.
	deterministicMerges := make(map[string]string)
	for _, item := range entities {
		if target := deterministicExistingMergeTarget(
			item, types.WikiPageTypeEntity, itemCandidates[item.Slug], candidatePages,
		); target != "" {
			deterministicMerges[item.Slug] = target
		}
	}
	for _, item := range concepts {
		if target := deterministicExistingMergeTarget(
			item, types.WikiPageTypeConcept, itemCandidates[item.Slug], candidatePages,
		); target != "" {
			deterministicMerges[item.Slug] = target
		}
	}

	// Group each new item with ONLY the existing pages that surfaced for
	// its own similarity probe. Presenting the model two flat lists (all
	// new items × all candidates) invites cross-item mispairings — it has
	// no way to tell which candidate is relevant to which item, so a weak
	// model pairs unrelated slugs that merely coexist in the prompt. A
	// per-item shortlist turns dedup into a local yes/no decision against
	// a handful of genuinely-similar pages and makes cross-item pairings
	// structurally unnatural to express. Items with no candidate are
	// omitted entirely (they cannot merge and only add hallucination
	// surface + tokens).
	var candBuf strings.Builder
	groups := 0
	renderGroup := func(item extractedItem, itemType string) {
		if deterministicMerges[item.Slug] != "" {
			return
		}
		cset := itemCandidates[item.Slug]
		if len(cset) == 0 {
			return
		}
		slugs := make([]string, 0, len(cset))
		for slug := range cset {
			// Skip the item's own slug: an identically-slugged existing
			// page is a re-ingest/update, not a merge target.
			if slug == item.Slug {
				continue
			}
			if _, ok := candidatePages[slug]; ok {
				slugs = append(slugs, slug)
			}
		}
		if len(slugs) == 0 {
			return
		}
		sort.Strings(slugs)
		pages := make([]*types.WikiPageLite, 0, len(slugs))
		for _, slug := range slugs {
			pages = append(pages, candidatePages[slug])
		}
		writeDedupCandidateGroup(&candBuf, item, itemType, pages)
		groups++
	}
	for _, item := range entities {
		renderGroup(item, "entity")
	}
	for _, item := range concepts {
		renderGroup(item, "concept")
	}
	var dedupeResult struct {
		Merges map[string]string `json:"merges"`
	}
	if groups > 0 {
		dedupeJSON, err := s.generateWithTemplate(ctx, chatModel, agent.WikiDeduplicationPrompt, map[string]string{
			"Candidates": candBuf.String(),
		})
		if err != nil {
			logger.Warnf(ctx, "wiki ingest: deduplication LLM call failed: %v", err)
		} else {
			dedupeJSON = cleanLLMJSON(dedupeJSON)
			if err := json.Unmarshal([]byte(dedupeJSON), &dedupeResult); err != nil {
				logger.Warnf(ctx, "wiki ingest: failed to parse dedup JSON: %v\nRaw: %s", err, dedupeJSON)
			}
		}
	}

	validMerge := func(srcSlug, dstSlug string) bool {
		if reason := dedupMergeRejectReason(srcSlug, dstSlug, itemCandidates[srcSlug]); reason != "" {
			logger.Warnf(ctx, "wiki ingest: dedup rejected %s → %s (%s)", srcSlug, dstSlug, reason)
			return false
		}
		return true
	}

	for i, item := range entities {
		if existingSlug := deterministicMerges[item.Slug]; existingSlug != "" {
			if existingSlug != item.Slug {
				logger.Infof(ctx, "wiki ingest: deterministic dedup merge %s → %s", item.Slug, existingSlug)
				entities[i].Slug = existingSlug
			}
		} else if existingSlug, ok := dedupeResult.Merges[item.Slug]; ok && validMerge(item.Slug, existingSlug) {
			logger.Infof(ctx, "wiki ingest: dedup merge %s → %s", item.Slug, existingSlug)
			entities[i].Slug = existingSlug
		}
	}
	for i, item := range concepts {
		if existingSlug := deterministicMerges[item.Slug]; existingSlug != "" {
			if existingSlug != item.Slug {
				logger.Infof(ctx, "wiki ingest: deterministic dedup merge %s → %s", item.Slug, existingSlug)
				concepts[i].Slug = existingSlug
			}
		} else if existingSlug, ok := dedupeResult.Merges[item.Slug]; ok && validMerge(item.Slug, existingSlug) {
			logger.Infof(ctx, "wiki ingest: dedup merge %s → %s", item.Slug, existingSlug)
			concepts[i].Slug = existingSlug
		}
	}

	return compactExtractedItems(entities, concepts)
}

// generateWithTemplate executes a prompt template and calls the LLM with
// bounded exponential-backoff retries for transient infrastructure errors.
//
// Retry policy:
//   - Up to wikiLLMMaxAttempts total attempts (initial + retries).
//   - Only retry errors classified as transient by isTransientLLMError:
//     HTTP 408/429/5xx, context deadline exceeded (when the parent ctx is
//     still alive), or generic "timeout"/"connection reset" wording.
//     4xx (except 408/429) is a caller-side fault and fails fast.
//   - Backoff is exponential base 2s: 2s, 4s, 8s — roughly wikiLLMBackoffBase
//   - 2^(attempt-1). Honors ctx cancellation so the task can abort.
//
// This exists because wiki ingest makes several independent LLM calls per
// document (extraction, summary, dedup, citations, intro) and a single
// transient 504 from the upstream gateway used to drop the document's
// summary page permanently. Retries plus failedOps requeuing (see
// mapOneDocument) turn those events into at-most-a-few-minute hiccups.
func (s *wikiIngestService) generateWithTemplate(ctx context.Context, chatModel chat.Chat, promptTpl string, data map[string]string) (string, error) {
	tmpl, err := template.New("wiki").Parse(promptTpl)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}

	maskedData, urlMap := maskTemplateDataImageURLs(data)

	var buf strings.Builder
	if err := tmpl.Execute(&buf, maskedData); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}

	prompt := buf.String()
	purpose := wikiPromptPurpose(promptTpl)
	messages := []chat.Message{{Role: "user", Content: prompt}}
	if promptTpl == agent.WikiPageModifyUserPrompt {
		systemPrompt := types.AppendCustomPromptInstructions(
			agent.WikiPageModifySystemPrompt,
			maskedData["CustomInstructions"],
			maskedData["InstructionScope"],
		)
		messages = []chat.Message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: prompt},
		}
	} else {
		messages[0].Content = types.AppendCustomPromptInstructions(
			prompt, maskedData["CustomInstructions"], maskedData["InstructionScope"],
		)
	}
	thinking := false
	opts := &chat.ChatOptions{Temperature: 0.3, Thinking: &thinking, MaxTokens: wikiLLMMaxTokens}
	if format := wikiJSONOutputSchema(purpose); len(format) > 0 {
		opts.Format = format
	}
	prefixFingerprint := chat.PromptPrefixFingerprint(messages, opts)
	warmupKey := ""
	if promptTpl == agent.WikiPageModifyUserPrompt {
		prefixFingerprint = chat.FingerprintPromptPrefix(
			messages[0].Content, maskedData["SharedSourceContexts"],
		)
		if tenantID, ok := types.TenantIDFromContext(ctx); ok {
			warmupKey = chat.BuildPromptCacheKey(
				tenantID, chatModel.GetModelID(), purpose, prefixFingerprint,
			)
		}
	}
	ctx = types.WithLLMCallMetadata(ctx, purpose, prefixFingerprint)

	tenantID, tenantScoped := types.TenantIDFromContext(ctx)
	requestJSON, err := json.Marshal(struct {
		Messages []chat.Message    `json:"messages"`
		Options  *chat.ChatOptions `json:"options"`
	}{Messages: messages, Options: opts})
	if err != nil {
		return "", fmt.Errorf("encode wiki generation request: %w", err)
	}
	requestKey := chat.BuildPromptCacheKey(
		tenantID, chatModel.GetModelID(), "wiki_exact_request",
		chat.FingerprintPromptPrefix(string(requestJSON)),
	)
	ledger, err := s.prepareWikiGenerationLedger(ctx, purpose, requestJSON, chatModel)
	if err != nil {
		return "", err
	}

	execute := func() (interface{}, error) {
		releaseWarmup := func() {}
		if tenantScoped && promptTpl == agent.WikiPageModifyUserPrompt && strings.TrimSpace(maskedData["SharedSourceContexts"]) != "" {
			var warmupErr error
			releaseWarmup, warmupErr = s.awaitWikiPromptWarmup(ctx, warmupKey)
			if warmupErr != nil {
				return "", warmupErr
			}
		}
		defer releaseWarmup()

		var lastErr error
		for attempt := 1; attempt <= wikiLLMMaxAttempts; attempt++ {
			attemptTimeout, budgetErr := wikiLLMAttemptBudget(ctx, wikiLLMMaxAttempts-attempt+1)
			if budgetErr != nil {
				return "", budgetErr
			}
			var reserved *types.WikiGenerationFragment
			if ledger != nil {
				var granted bool
				reserved, granted, budgetErr = ledger.reserve(ctx, attemptTimeout)
				if budgetErr != nil {
					return "", budgetErr
				}
				if !granted {
					return reserved.Output, nil
				}
			}
			attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
			response, callErr := callWikiLLMWithFallbacks(
				attemptCtx, chatModel, messages, opts, purpose,
				maskedData["ExistingSummary"], maskedData["PageTitle"],
			)
			attemptTimedOut := errors.Is(callErr, context.DeadlineExceeded) && ctx.Err() == nil
			cancelAttempt()
			if attemptTimedOut {
				callErr = &WikiGenerationError{
					Class: WikiGenerationErrorTransientTransport,
					Err:   fmt.Errorf("wiki LLM attempt timed out after %s: %w", attemptTimeout, callErr),
				}
			}
			if callErr == nil && response != nil {
				if strings.TrimSpace(response.Content) == "" {
					callErr = newWikiGenerationError(
						WikiGenerationErrorDeterministicOutput,
						errors.New("wiki LLM returned empty response content"),
					)
				} else {
					if ledgerErr := ledger.complete(ctx, reserved, response.Content); ledgerErr != nil {
						return "", ledgerErr
					}
					return response.Content, nil
				}
			}
			if callErr == nil {
				callErr = newWikiGenerationError(
					WikiGenerationErrorDeterministicOutput,
					errors.New("LLM returned nil response"),
				)
			}
			lastErr = callErr
			if ledger != nil {
				if settleErr := ledger.settleFailure(ctx, reserved, callErr); settleErr != nil {
					return "", settleErr
				}
			}

			if !isTransientLLMError(ctx, callErr) {
				return "", fmt.Errorf("LLM call failed: %w", callErr)
			}
			if attempt == wikiLLMMaxAttempts {
				break
			}

			backoff := wikiLLMBackoffBase << (attempt - 1)
			logger.Warnf(ctx, "wiki ingest: LLM call failed (attempt %d/%d), retrying in %s: %v",
				attempt, wikiLLMMaxAttempts, backoff, callErr)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", fmt.Errorf("LLM call aborted during backoff: %w", ctx.Err())
			case <-timer.C:
			}
		}
		return "", fmt.Errorf("LLM call failed after %d attempts: %w", wikiLLMMaxAttempts, lastErr)
	}

	// Missing tenant context is unexpected for production Wiki work. Fail safe
	// by skipping cross-call coalescing instead of putting unrelated requests
	// into a synthetic tenant-0 bucket.
	if !tenantScoped {
		value, executeErr := execute()
		if executeErr != nil {
			return "", executeErr
		}
		content, _ := value.(string)
		return unmaskImageURLs(content, urlMap), nil
	}
	resultCh := s.llmRequests.DoChan(requestKey, execute)

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return "", result.Err
		}
		content, _ := result.Val.(string)
		return unmaskImageURLs(content, urlMap), nil
	}
}

func wikiLLMAttemptBudget(ctx context.Context, attemptsRemaining int) (time.Duration, error) {
	if attemptsRemaining < 1 {
		attemptsRemaining = 1
	}
	timeout := wikiLLMAttemptTimeout
	deadline, ok := ctx.Deadline()
	if !ok {
		return timeout, nil
	}
	workRemaining := time.Until(deadline) - wikiTaskSettlementReserve
	if workRemaining <= 0 {
		return 0, fmt.Errorf(
			"wiki LLM retry budget exhausted; reserving %s for durable settlement: %w",
			wikiTaskSettlementReserve, context.DeadlineExceeded,
		)
	}
	if fairShare := workRemaining / time.Duration(attemptsRemaining); fairShare < timeout {
		timeout = fairShare
	}
	return timeout, nil
}

func wikiPromptPurpose(promptTpl string) string {
	switch promptTpl {
	case agent.WikiPageModifyUserPrompt:
		return "wiki_page_modify"
	case agent.WikiChunkCitationPrompt:
		return "wiki_chunk_citation"
	case agent.WikiCandidateSlugPrompt:
		return "wiki_candidate_slug"
	case agent.WikiSummaryPrompt:
		return "wiki_summary"
	case agent.WikiKnowledgeExtractPrompt:
		return "wiki_knowledge_extract"
	case agent.WikiTaxonomyPlanPrompt:
		return "wiki_taxonomy_plan"
	case agent.WikiDeduplicationPrompt:
		return "wiki_deduplication"
	case agent.WikiIndexIntroPrompt, agent.WikiIndexIntroUpdatePrompt:
		return "wiki_index_intro"
	default:
		return "wiki_generation"
	}
}

func (s *wikiIngestService) awaitWikiPromptWarmup(ctx context.Context, key string) (func(), error) {
	if key == "" {
		return func() {}, nil
	}
	candidate := &wikiPromptWarmup{done: make(chan struct{})}
	actual, loaded := s.promptWarmups.LoadOrStore(key, candidate)
	entry := actual.(*wikiPromptWarmup)
	if !loaded {
		return func() {
			entry.once.Do(func() { close(entry.done) })
			// Keep the local warmed marker long enough to cover the parallel
			// reduce burst without turning it into a persistent application cache.
			time.AfterFunc(4*time.Minute, func() {
				s.promptWarmups.CompareAndDelete(key, entry)
			})
		}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-entry.done:
		return func() {}, nil
	}
}

// isTransientLLMError retries only errors backed by typed transport facts.
// Ambiguous provider text fails closed so a possibly-paid call is not repeated.
func isTransientLLMError(ctx context.Context, err error) bool {
	return wikiGenerationErrorClassOf(classifyWikiGenerationError(ctx, err)) == WikiGenerationErrorTransientTransport
}

// --- Helpers ---

// isKnowledgeGone returns true only when the given knowledge is authoritatively
// absent, deleting, or cancelled. It first consults the Redis tombstone
// (written by cleanupWikiOnKnowledgeDelete) as a fast path, then falls back
// to the DB. Transient DB/context errors are returned to the caller instead of
// being collapsed into "gone", because doing that would silently discard Wiki
// additions and report a false success.
func (s *wikiIngestService) isKnowledgeGone(ctx context.Context, kbID, knowledgeID string) (bool, error) {
	if knowledgeID == "" {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.redisClient != nil {
		exists, err := s.redisClient.Exists(ctx, WikiDeletedTombstoneKey(kbID, knowledgeID)).Result()
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, fmt.Errorf("lookup deleted knowledge tombstone %s: %w", knowledgeID, err)
		}
		if exists > 0 {
			return true, nil
		}
	}
	if s.knowledgeSvc == nil {
		return false, fmt.Errorf("knowledge service is unavailable")
	}
	kn, err := s.knowledgeSvc.GetKnowledgeByIDOnly(ctx, knowledgeID)
	if errors.Is(err, apprepo.ErrKnowledgeNotFound) || (err == nil && kn == nil) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("load knowledge %s: %w", knowledgeID, err)
	}
	if kn.KnowledgeBaseID != kbID {
		return true, nil
	}
	switch kn.ParseStatus {
	case types.ParseStatusDeleting, types.ParseStatusCancelled:
		return true, nil
	}
	return false, nil
}

// filterLiveUpdates drops updates whose source attempt was superseded, plus
// additions/summaries whose source knowledge was deleted after Map finished.
// Untracked retract updates are preserved so deletion cleanup still lands.
// Lookup failures are propagated and keep the durable op retryable. Results
// are cached per knowledge/attempt to avoid DB hammering when a single reduce
// slug carries many updates for the same document.
func (s *wikiIngestService) filterLiveUpdates(ctx context.Context, kbID string, updates []SlugUpdate) ([]SlugUpdate, error) {
	if len(updates) == 0 {
		return updates, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type knowledgeState struct {
		gone bool
		err  error
	}
	goneCache := make(map[string]knowledgeState)
	type attemptKey struct {
		knowledgeID string
		attempt     int
	}
	type attemptState struct {
		current bool
		err     error
	}
	attemptCache := make(map[attemptKey]attemptState)
	isGone := func(kid string) (bool, error) {
		if kid == "" {
			return false, nil
		}
		if v, ok := goneCache[kid]; ok {
			return v.gone, v.err
		}
		gone, err := s.isKnowledgeGone(ctx, kbID, kid)
		goneCache[kid] = knowledgeState{gone: gone, err: err}
		return gone, err
	}
	isCurrentAttempt := func(kid string, attempt int) (bool, error) {
		if kid == "" || attempt <= 0 {
			return true, nil
		}
		key := attemptKey{knowledgeID: kid, attempt: attempt}
		if v, ok := attemptCache[key]; ok {
			return v.current, v.err
		}
		current, err := wikiAttemptCurrentStrict(ctx, s.tracker(), kid, attempt)
		attemptCache[key] = attemptState{current: current, err: err}
		return current, err
	}
	filtered := make([]SlugUpdate, 0, len(updates))
	dropped := 0
	for _, u := range updates {
		current, err := isCurrentAttempt(u.KnowledgeID, u.Attempt)
		if err != nil {
			return nil, err
		}
		if !current {
			dropped++
			continue
		}
		switch u.Type {
		case "retract", "retractStale":
			filtered = append(filtered, u)
		default:
			gone, err := isGone(u.KnowledgeID)
			if err != nil {
				return nil, err
			}
			if gone {
				dropped++
				continue
			}
			filtered = append(filtered, u)
		}
	}
	if dropped > 0 {
		logger.Infof(ctx, "wiki ingest: reduce dropped %d stale/deleted source updates", dropped)
	}
	return filtered, nil
}

func wikiAttemptCurrentStrict(
	ctx context.Context,
	tracker SpanTracker,
	knowledgeID string,
	attempt int,
) (bool, error) {
	if knowledgeID == "" || attempt <= 0 {
		return true, nil
	}
	strict, ok := tracker.(strictAttemptTracker)
	if !ok {
		return false, fmt.Errorf("strict latest attempt lookup is unavailable for wiki source %s", knowledgeID)
	}
	latest, err := strict.LatestAttemptStrict(ctx, knowledgeID)
	if err != nil {
		return false, fmt.Errorf("load latest attempt for wiki source %s: %w", knowledgeID, err)
	}
	if latest <= 0 {
		return false, fmt.Errorf("latest attempt is unavailable for wiki source %s", knowledgeID)
	}
	if latest < attempt {
		return false, fmt.Errorf(
			"latest attempt %d precedes wiki source attempt %d for %s",
			latest, attempt, knowledgeID,
		)
	}
	return latest == attempt, nil
}

// reconstructContent rebuilds document text from chunks.
//
// This only concatenates text-type chunks — image OCR / caption information is
// stored on image_ocr / image_caption child chunks (see image_multimodal.go),
// not on the parent text chunk's ImageInfo field. Callers that need the full
// enriched content (with OCR / captions inlined) should call
// reconstructEnrichedContent instead so image info is fetched from child
// chunks and embedded alongside Markdown image links.
func reconstructContent(chunks []*types.Chunk) string {
	var textChunks []*types.Chunk
	for _, c := range chunks {
		if c.ChunkType == types.ChunkTypeText || c.ChunkType == "" {
			textChunks = append(textChunks, c)
		}
	}

	// 重叠去重与排序统一交给公共逻辑（按文本匹配，兼容补写表头 / HTML 实体）。
	return searchutil.MergeTextChunks(textChunks, "\n")
}

func sampleWikiContent(content string, maxChars int) string {
	if len([]rune(content)) <= maxChars {
		return content
	}

	const (
		sampleNotice = "[Document content is sampled from its beginning, middle, and end. Omission markers represent skipped source text, not the end of the document.]"
		outlineLabel = "Document outline:"
		contentLabel = "Representative content:"
	)
	prefix := sampleNotice + "\n\n" + outlineLabel + "\n"
	suffix := "\n\n" + contentLabel + "\n"
	reserved := len([]rune(prefix + suffix))
	if maxChars <= reserved+100 {
		return sampleLongContent(content, maxChars)
	}

	outlineBudget := min(4096, maxChars/4)
	outline := sampleLongContent(markdownHeadingOutline(content), outlineBudget)
	bodyBudget := maxChars - reserved - len([]rune(outline))
	if bodyBudget < 100 {
		return sampleLongContent(content, maxChars)
	}

	return prefix + outline + suffix + sampleLongContent(content, bodyBudget)
}

func markdownHeadingOutline(content string) string {
	headings := make([]string, 0)
	seen := make(map[string]struct{})
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !isMarkdownHeading(line) {
			continue
		}
		if _, exists := seen[line]; exists {
			continue
		}
		seen[line] = struct{}{}
		headings = append(headings, line)
	}
	return strings.Join(headings, "\n")
}

func isMarkdownHeading(line string) bool {
	level := 0
	for level < len(line) && level < 6 && line[level] == '#' {
		level++
	}
	return level > 0 && level < len(line) && line[level] == ' ' &&
		strings.TrimSpace(line[level+1:]) != ""
}

// reconstructEnrichedContent rebuilds document text and inlines image_info
// (OCR text + caption) pulled from image_ocr / image_caption child chunks.
//
// Without this enrichment, image-heavy documents (e.g. a scanned PDF or a
// standalone .jpg) reach the LLM as bare Markdown image links, causing
// extraction / summarization to produce empty or "no textual content" output.
func reconstructEnrichedContent(
	ctx context.Context,
	chunkRepo interfaces.ChunkRepository,
	tenantID uint64,
	chunks []*types.Chunk,
) string {
	content := reconstructContent(chunks)

	var textChunkIDs []string
	for _, c := range chunks {
		if c.ChunkType == types.ChunkTypeText || c.ChunkType == "" {
			if c.ID != "" {
				textChunkIDs = append(textChunkIDs, c.ID)
			}
		}
	}
	if len(textChunkIDs) == 0 || chunkRepo == nil {
		return content
	}

	imageInfoMap := searchutil.CollectImageInfoByChunkIDs(ctx, chunkRepo, tenantID, textChunkIDs)
	mergedImageInfo := searchutil.MergeImageInfoJSON(imageInfoMap)
	if mergedImageInfo == "" {
		return content
	}
	return searchutil.EnrichContentWithImageInfo(content, mergedImageInfo)
}

// slugify creates a URL-friendly slug from a string
func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '/' {
			return r
		}
		if r == ' ' || r == '_' {
			return '-'
		}
		// Keep CJK characters
		if r >= 0x4E00 && r <= 0x9FFF {
			return r
		}
		return -1
	}, s)
	// Collapse multiple hyphens
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// truncateString truncates a string to maxLen runes
func truncateString(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// appendUnique appends a string to a StringArray if not already present
func appendUnique(arr types.StringArray, s string) types.StringArray {
	for _, v := range arr {
		if v == s {
			return arr
		}
	}
	return append(arr, s)
}

// minTextContentRunes is the minimum number of non-whitespace, non-image-reference
// runes required for content to be considered substantive enough for LLM
// summarization or wiki extraction. Documents below this threshold (e.g. a
// scanned PDF where OCR yielded nothing AND no caption either) are routed to
// a deterministic empty-content fallback instead of being passed to the LLM,
// which would otherwise hallucinate based on metadata alone.
//
// The threshold is intentionally low: legitimate short documents (brief
// memos, single-line notes) must still pass. The goal is only to catch
// the empty-image-only case.
//
// Declared as a var (not const) so tests can override it and future config
// plumbing can adjust it at runtime without a rebuild.
var minTextContentRunes = 10

var (
	// Markdown image references like ![alt](path) — pure visual placeholders
	// with no extractable text, so the whole reference is removed.
	mdImageRefRE = regexp.MustCompile(`!\[[^\]]*\]\([^)]*\)`)

	// <image_original>...</image_original> blocks wrap the verbatim Markdown
	// image reference inside an enriched <image> block (see
	// searchutil.EnrichContentWithImageInfo). The content is just a redundant
	// copy of an already-stripped image link, so the whole block (tags +
	// content) is removed.
	imageOriginalBlockRE = regexp.MustCompile(`(?is)<image_original\b[^>]*>.*?</image_original>`)

	// Self-closing or attribute-only HTML <img> tags.
	htmlImgTagRE = regexp.MustCompile(`(?i)<img\b[^>]*/?>`)

	// Wrapper-style <image>, <images>, <image_caption>, <image_ocr> tags
	// (opening or closing). Matches ONLY the tag; the text content between
	// open and close tags is preserved. This is critical: VLM-generated OCR
	// and caption text live inside <image_ocr>...</image_ocr> and
	// <image_caption>...</image_caption> blocks, and stripping the content
	// would silently destroy the very text we want to keep.
	imageWrapperTagRE = regexp.MustCompile(`(?i)</?image[a-z_]*\b[^>]*/?>`)

	// Markdown image references with the URL captured separately so LLM-bound
	// image URLs can be frozen while captions remain editable.
	mdImageURLRE = regexp.MustCompile(`!\[[^\]]*\]\(([^)]*)\)`)

	// Enriched image blocks store the original object URL as an attribute,
	// e.g. <image url="...">. Capture both double- and single-quoted forms.
	imageURLAttrRE = regexp.MustCompile(`(?i)<image\b[^>]*\surl\s*=\s*(?:"([^"]*)"|'([^']*)')`)

	imagePlaceholderTokenRE = regexp.MustCompile(`wkimg:[A-Za-z0-9_-]+`)
)

func maskTemplateDataImageURLs(data map[string]string) (map[string]string, map[string]string) {
	if len(data) == 0 {
		return data, nil
	}

	masked := make(map[string]string, len(data))
	urlToToken := make(map[string]string)
	tokenToURL := make(map[string]string)

	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		masked[key] = maskImageURLsWithState(data[key], urlToToken, tokenToURL)
	}

	return masked, tokenToURL
}

// maskImageURLs replaces image URLs with low-entropy placeholders. It only
// freezes URLs; alt/caption text remains in place for the LLM to edit.
func maskImageURLs(s string) (string, map[string]string) {
	urlToToken := make(map[string]string)
	tokenToURL := make(map[string]string)
	return maskImageURLsWithState(s, urlToToken, tokenToURL), tokenToURL
}

func maskImageURLsWithState(s string, urlToToken, tokenToURL map[string]string) string {
	urls := collectMaskableImageURLs(s)
	if len(urls) == 0 {
		return s
	}

	for _, url := range urls {
		if _, ok := urlToToken[url]; ok {
			continue
		}
		token := fmt.Sprintf("wkimg:%04d", len(tokenToURL)+1)
		urlToToken[url] = token
		tokenToURL[token] = url
	}

	replaceURLs := append([]string(nil), urls...)
	sort.SliceStable(replaceURLs, func(i, j int) bool {
		return len(replaceURLs[i]) > len(replaceURLs[j])
	})

	masked := s
	for _, url := range replaceURLs {
		masked = strings.ReplaceAll(masked, url, urlToToken[url])
	}
	return masked
}

func collectMaskableImageURLs(s string) []string {
	seen := make(map[string]struct{})
	var urls []string

	addURL := func(url string) {
		url = strings.TrimSpace(url)
		if url == "" {
			return
		}
		if _, ok := seen[url]; ok {
			return
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}

	for _, match := range mdImageURLRE.FindAllStringSubmatch(s, -1) {
		addURL(match[1])
	}
	for _, match := range imageURLAttrRE.FindAllStringSubmatch(s, -1) {
		if match[1] != "" {
			addURL(match[1])
			continue
		}
		addURL(match[2])
	}

	return urls
}

// unmaskImageURLs restores known placeholders and drops any corrupted or
// invented image placeholders so broken image links never reach storage.
func unmaskImageURLs(out string, urlMap map[string]string) string {
	out = mdImageURLRE.ReplaceAllStringFunc(out, func(match string) string {
		parts := mdImageURLRE.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		url := strings.TrimSpace(parts[1])
		if realURL, ok := urlMap[url]; ok {
			idx := strings.LastIndex(match, "(")
			if idx < 0 {
				return match
			}
			return match[:idx+1] + realURL + ")"
		}
		if strings.HasPrefix(url, "wkimg:") {
			return ""
		}
		return match
	})

	return replaceImagePlaceholderTokensOutsideMarkdown(out, urlMap)
}

func replaceImagePlaceholderTokensOutsideMarkdown(s string, urlMap map[string]string) string {
	matches := mdImageURLRE.FindAllStringIndex(s, -1)
	if len(matches) == 0 {
		return replaceImagePlaceholderTokens(s, urlMap)
	}

	var b strings.Builder
	last := 0
	for _, match := range matches {
		if match[0] > last {
			b.WriteString(replaceImagePlaceholderTokens(s[last:match[0]], urlMap))
		}
		b.WriteString(s[match[0]:match[1]])
		last = match[1]
	}
	if last < len(s) {
		b.WriteString(replaceImagePlaceholderTokens(s[last:], urlMap))
	}
	return b.String()
}

func replaceImagePlaceholderTokens(s string, urlMap map[string]string) string {
	return imagePlaceholderTokenRE.ReplaceAllStringFunc(s, func(token string) string {
		if realURL, ok := urlMap[token]; ok {
			return realURL
		}
		return ""
	})
}

// stripImageMarkup removes image-only placeholders (Markdown image refs,
// <img> tags, <image_original> redundancy blocks) and unwraps the
// <image>/<image_caption>/<image_ocr> XML wrappers produced by the search
// enrichment layer, leaving any OCR or caption text as plain inline text.
//
// This shape matters: when VLM OCR succeeds on a scanned PDF page, the
// extracted text reaches downstream code wrapped in <image_ocr> tags inside
// an <image> block. A naive "strip the whole <image>...</image> block"
// approach would discard the OCR text — the exact opposite of what we want.
func stripImageMarkup(s string) string {
	s = imageOriginalBlockRE.ReplaceAllString(s, "")
	s = mdImageRefRE.ReplaceAllString(s, "")
	s = htmlImgTagRE.ReplaceAllString(s, "")
	s = imageWrapperTagRE.ReplaceAllString(s, "")
	return s
}

// extractRealText returns the trimmed content with image markup stripped.
// Cached at the call site for use both in the threshold check and in any
// subsequent log message, avoiding redundant regex passes over large docs.
func extractRealText(content string) string {
	return strings.TrimSpace(stripImageMarkup(content))
}

// hasSufficientTextContent reports whether the given content carries enough
// real text (after image markup is stripped, with OCR/caption text retained)
// to warrant an LLM call. It is the primary defence against filename-driven
// hallucinations on scanned PDFs that have NO usable text at all.
func hasSufficientTextContent(content string) bool {
	return realTextRuneCount(content) >= minTextContentRunes
}

// realTextRuneCount returns the rune length of the content after image
// markup is stripped. Uses utf8.RuneCountInString to avoid allocating a
// rune slice for the count.
func realTextRuneCount(content string) int {
	return utf8.RuneCountInString(extractRealText(content))
}

// cleanLLMJSON strips markdown code-fence wrappers and sanitizes control characters
// from LLM-generated JSON output so it can be safely unmarshalled.
func cleanLLMJSON(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimSpace(s)
	return sanitizeJSONString(s)
}

// sanitizeJSONString sanitizes a string that is intended to be parsed as JSON,
// by properly escaping unescaped control characters (like newlines) inside string literals.
func sanitizeJSONString(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	inString := false
	escape := false
	for _, r := range s {
		if escape {
			if r == '\n' {
				buf.WriteString(`n`)
			} else if r == '\r' {
				buf.WriteString(`r`)
			} else if r == '\t' {
				buf.WriteString(`t`)
			} else {
				buf.WriteRune(r)
			}
			escape = false
			continue
		}
		if r == '\\' {
			escape = true
			buf.WriteRune(r)
			continue
		}
		if r == '"' {
			inString = !inString
			buf.WriteRune(r)
			continue
		}
		if inString {
			if r == '\n' {
				buf.WriteString(`\n`)
				continue
			}
			if r == '\r' {
				buf.WriteString(`\r`)
				continue
			}
			if r == '\t' {
				buf.WriteString(`\t`)
				continue
			}
		}
		buf.WriteRune(r)
	}
	return buf.String()
}
