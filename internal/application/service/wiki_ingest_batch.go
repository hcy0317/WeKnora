package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/agent"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"golang.org/x/sync/errgroup"
)

// scheduleFollowUp enqueues another asynq trigger task if there are
// still pending ops in task_pending_ops for this KB. Returns true when
// a follow-up was scheduled.
//
// Post-Phase-3 this only backstops the case where a batch drained its
// claimed window but more rows remain and no other trigger is pending
// (e.g. steady trickle of uploads). Standard mode already fans a KB's
// backlog across concurrent claiming batches, so the short delay is
// normally just a light debounce rather than a lock-release wait.
//
// `delay` is the ProcessIn before the follow-up fires. Callers pass
// wikiFollowUpDelay for the normal case and wikiRateLimitBackoff when the
// batch tripped an upstream rate limit — the released failed rows are
// eligible immediately, but nothing claims them until a trigger fires, so
// stretching the follow-up interval is what actually paces retries down
// during a 429 storm.
func (s *wikiIngestService) scheduleFollowUp(ctx context.Context, payload WikiIngestPayload, delay time.Duration) bool {
	if s.pendingRepo == nil {
		return false
	}
	count, err := s.pendingRepo.PendingCount(ctx, wikiTaskType, wikiTaskScope, payload.KnowledgeBaseID)
	if err != nil || count == 0 {
		return false
	}

	logger.Infof(ctx, "wiki ingest: %d more documents pending for KB %s, scheduling follow-up in %s", count, payload.KnowledgeBaseID, delay)

	langfuse.InjectTracing(ctx, &payload)
	payloadBytes, _ := json.Marshal(payload)
	t := asynq.NewTask(types.TypeWikiIngest, payloadBytes,
		asynq.Queue(types.QueueWiki),
		asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout),
		asynq.ProcessIn(delay), // debounce (or rate-limit backoff) before draining the remainder
	)
	if _, err := s.task.Enqueue(t); err != nil {
		logger.Warnf(ctx, "wiki ingest: follow-up enqueue failed: %v", err)
		return false
	}
	return true
}

// newWikiBatchContext builds the per-run lazy fetchers used by both the ingest
// batch and the debounced finalize task. These replace the legacy pre-batch
// ListAllPages dump: instead of pulling ~100MB of rows up front (and walking
// them several more times), callers pay only for the slugs / knowledge ids they
// actually reach for. Cache hits keep repeat lookups within a single run free.
// The cache is per-call (goroutine-safe via fetchMu), so each task gets a fresh,
// isolated view.
func (s *wikiIngestService) newWikiBatchContext(
	kbID string,
	wikiConfig *types.WikiConfig,
) *WikiBatchContext {
	var (
		fetchMu         sync.Mutex
		slugTitleCache  = make(map[string]string) // slug -> title; "" = known-missing
		summaryKIDCache = make(map[string]string) // kid -> content; "" = known-missing
	)

	resolveSlugs := func(ctx context.Context, slugs []string) map[string]string {
		// Filter to the slugs we don't already have cached.
		fetchMu.Lock()
		need := slugs[:0:0]
		for _, slug := range slugs {
			if _, ok := slugTitleCache[slug]; ok {
				continue
			}
			need = append(need, slug)
		}
		fetchMu.Unlock()

		if len(need) > 0 {
			pages, err := s.wikiService.ListBySlugs(ctx, kbID, need)
			if err != nil {
				logger.Warnf(ctx, "wiki ingest: ListBySlugs(%d slugs) failed: %v", len(need), err)
			}
			fetchMu.Lock()
			for _, slug := range need {
				if p, ok := pages[slug]; ok && p != nil {
					if p.Status == types.WikiPageStatusArchived || p.PageType == types.WikiPageTypeIndex {
						// Treat archived / system pages as missing from the
						// title-resolution map: cleanDeadLinks shouldn't link
						// to them or surface them as cross-link candidates.
						slugTitleCache[slug] = ""
						continue
					}
					slugTitleCache[slug] = p.Title
				} else {
					slugTitleCache[slug] = ""
				}
			}
			fetchMu.Unlock()
		}

		out := make(map[string]string, len(slugs))
		fetchMu.Lock()
		for _, slug := range slugs {
			if title := slugTitleCache[slug]; title != "" {
				out[slug] = title
			}
		}
		fetchMu.Unlock()
		return out
	}

	resolveSummaries := func(ctx context.Context, kids []string) map[string]string {
		fetchMu.Lock()
		need := kids[:0:0]
		for _, kid := range kids {
			if _, ok := summaryKIDCache[kid]; ok {
				continue
			}
			need = append(need, kid)
		}
		fetchMu.Unlock()

		if len(need) > 0 {
			contents, err := s.wikiService.ListSummariesByKnowledgeIDs(ctx, kbID, need)
			if err != nil {
				logger.Warnf(ctx, "wiki ingest: ListSummariesByKnowledgeIDs(%d kids) failed: %v", len(need), err)
			}
			fetchMu.Lock()
			for _, kid := range need {
				if c, ok := contents[kid]; ok && c != "" {
					summaryKIDCache[kid] = c
				} else {
					summaryKIDCache[kid] = ""
				}
			}
			fetchMu.Unlock()
		}

		out := make(map[string]string, len(kids))
		fetchMu.Lock()
		for _, kid := range kids {
			if content := summaryKIDCache[kid]; content != "" {
				out[kid] = content
			}
		}
		fetchMu.Unlock()
		return out
	}

	granularity := types.WikiExtractionStandard
	contentInstructions := ""
	extractionInstructions := ""
	if wikiConfig != nil {
		granularity = wikiConfig.ExtractionGranularity.Normalize()
		contentInstructions = wikiConfig.ContentInstructions
		extractionInstructions = wikiConfig.ExtractionInstructions
	}
	return &WikiBatchContext{
		SlugTitle: func(ctx context.Context, slug string) string {
			m := resolveSlugs(ctx, []string{slug})
			return m[slug]
		},
		SlugTitleMany: resolveSlugs,
		SummaryContentByKnowledgeID: func(ctx context.Context, kid string) string {
			m := resolveSummaries(ctx, []string{kid})
			return m[kid]
		},
		ExtractionGranularity:  granularity,
		ContentInstructions:    contentInstructions,
		ExtractionInstructions: extractionInstructions,
	}
}

func (s *wikiIngestService) ProcessWikiIngest(ctx context.Context, t *asynq.Task) (retErr error) {
	taskStartedAt := time.Now()
	retryCount, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)

	var payload WikiIngestPayload
	exitStatus := "success"
	mode := "redis"
	pendingOpsCount := 0
	ingestOps := 0
	retractOps := 0
	ingestSucceeded := 0
	ingestFailed := 0
	retractHandled := 0
	followUpScheduled := false
	totalPagesAffected := 0
	docPreview := make([]string, 0, 6)
	// Tunables resolved from KB.WikiConfig once we've loaded the KB.
	// Captured up here so the deferred stats log can observe them
	// regardless of which exit path we took. (Index-intro rebuild moved to
	// the debounced wiki:finalize task in Phase 1, so it's logged there, not
	// here; the exclusive per-KB lock was removed in Phase 3.)
	loggedBatchSize := 0
	loggedMapPar := 0
	loggedReducePar := 0
	loggedMaxInflight := 0

	defer func() {
		logger.Infof(
			ctx,
			"wiki ingest stats: kb=%s tenant=%d retry=%d/%d status=%s elapsed=%s mode=%s pending_ops=%d ops(ingest=%d,retract=%d) ingest(success=%d,failed=%d) retract_handled=%d pages(total=%d) followup=%v tunables(batch=%d,map_par=%d,reduce_par=%d,max_inflight=%d) preview=%s",
			payload.KnowledgeBaseID,
			payload.TenantID,
			retryCount,
			maxRetry,
			exitStatus,
			time.Since(taskStartedAt).Round(time.Millisecond),
			mode,
			pendingOpsCount,
			ingestOps,
			retractOps,
			ingestSucceeded,
			ingestFailed,
			retractHandled,
			totalPagesAffected,
			followUpScheduled,
			loggedBatchSize,
			loggedMapPar,
			loggedReducePar,
			loggedMaxInflight,
			previewStringSlice(docPreview, 6),
		)
	}()

	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		exitStatus = "invalid_payload"
		return fmt.Errorf("wiki ingest: unmarshal payload: %w", err)
	}

	// Inject context
	ctx = context.WithValue(ctx, types.TenantIDContextKey, payload.TenantID)
	if payload.Language != "" {
		ctx = context.WithValue(ctx, types.LanguageContextKey, payload.Language)
	}

	// Concurrency model (Phase 3):
	//
	//   - Standard (Redis) mode: NO exclusive per-KB lock. Multiple batches
	//     for the same KB may run at once, each claiming a DISJOINT set of
	//     rows via claimPendingList (below). This lets one KB's backlog fan
	//     out across the whole wiki worker pool instead of draining one
	//     5-doc batch at a time. Same-slug reduce safety is provided by
	//     withSlugLock, not by a batch-wide lock.
	//
	//   - Lite mode (no Redis, single process): keep the in-process
	//     liteLocks guard so exactly one batch per KB runs at a time. Lite
	//     targets small/local scale where serial-per-KB is simplest and the
	//     claiming machinery (which needs FOR UPDATE SKIP LOCKED) buys
	//     nothing.
	if s.redisClient == nil {
		mode = "lite"
		if _, loaded := s.liteLocks.LoadOrStore(payload.KnowledgeBaseID, struct{}{}); loaded {
			exitStatus = "active_lock_conflict"
			logger.Infof(ctx, "wiki ingest: another batch active for KB %s (lite lock), deferring to asynq retry", payload.KnowledgeBaseID)
			return ErrWikiIngestConcurrent
		}
		defer s.liteLocks.Delete(payload.KnowledgeBaseID)
	}

	kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, payload.KnowledgeBaseID)
	if errors.Is(err, apprepo.ErrKnowledgeBaseNotFound) || (err == nil && kb == nil) {
		exitStatus = "kb_deleted"
		if cleanupErr := s.clearDeletedKnowledgeBasePendingOps(ctx, payload.KnowledgeBaseID); cleanupErr != nil {
			return fmt.Errorf("wiki ingest: clear deleted KB queue: %w", cleanupErr)
		}
		return nil
	}
	if err != nil {
		exitStatus = "get_kb_failed"
		return fmt.Errorf("wiki ingest: get KB: %w", err)
	}
	if !kb.IsWikiEnabled() {
		exitStatus = "kb_not_wiki_enabled"
		return fmt.Errorf("wiki ingest: KB %s is not wiki type", kb.ID)
	}

	var synthesisModelID string
	if kb.WikiConfig != nil {
		synthesisModelID = kb.WikiConfig.SynthesisModelID
	}
	if synthesisModelID == "" {
		synthesisModelID = kb.SummaryModelID
	}
	if synthesisModelID == "" {
		exitStatus = "missing_synthesis_model"
		return fmt.Errorf("wiki ingest: no synthesis model configured for KB %s", kb.ID)
	}
	chatModel, err := s.modelService.GetChatModel(ctx, synthesisModelID)
	if err != nil {
		exitStatus = "get_chat_model_failed"
		return fmt.Errorf("wiki ingest: get chat model: %w", err)
	}

	// Resolve per-KB tunables once. WikiConfig.IngestBatchSize /
	// IngestMapParallel / IngestReduceParallel let operators on
	// 4w-document KBs raise the throughput knob (more docs per batch +
	// more concurrent LLM calls) without a code deploy. Zero falls back
	// to the historical defaults so existing KBs see no behaviour
	// change until they opt in.
	batchSize := kb.WikiConfig.IngestBatchSizeOrDefault(wikiMaxDocsPerBatch)
	mapParallel := kb.WikiConfig.IngestMapParallelOrDefault(10)
	reduceParallel := kb.WikiConfig.IngestReduceParallelOrDefault(10)
	loggedBatchSize = batchSize
	loggedMapPar = mapParallel
	loggedReducePar = reduceParallel

	lang := types.LanguageNameFromContext(ctx)

	// Per-KB in-flight cap (Phase 4, standard mode): keep one KB's bulk
	// import from monopolizing the shared wiki pool. If the KB is already at
	// its cap we schedule a coalesced retry and bail without claiming, so the
	// rows stay available for whichever running batch frees a slot first.
	maxInflight := kb.WikiConfig.IngestMaxInflightOrDefault(wikiInflightDefault)
	loggedMaxInflight = maxInflight
	releaseSlot, gotSlot := s.reserveInflightSlot(ctx, payload.KnowledgeBaseID, maxInflight)
	if !gotSlot {
		exitStatus = "inflight_cap"
		logger.Infof(ctx, "wiki ingest: KB %s at in-flight cap (%d), rescheduling", payload.KnowledgeBaseID, maxInflight)
		s.scheduleCappedRetry(ctx, payload)
		return nil
	}
	defer releaseSlot()

	// Standard mode claims rows (marks claimed_at, disjoint across concurrent
	// batches); Lite mode peeks under its in-process lock.
	var pendingOps []WikiPendingOp
	var peekedIDs []int64
	var claimOwner *types.TaskClaimOwner
	var loadErr error
	if s.redisClient != nil {
		pendingOps, peekedIDs, claimOwner, loadErr = s.claimPendingList(ctx, payload.KnowledgeBaseID, batchSize)
	} else {
		pendingOps, peekedIDs, loadErr = s.peekPendingList(ctx, payload.KnowledgeBaseID, batchSize)
	}
	if loadErr != nil {
		// Transient failure loading the pending list. Return an error so
		// asynq retries this trigger with backoff — acking here would strand
		// the queue until an unrelated upload happened to re-trigger it.
		exitStatus = "load_pending_failed"
		logger.Warnf(ctx, "wiki ingest: failed to load pending list for KB %s: %v", payload.KnowledgeBaseID, loadErr)
		return fmt.Errorf("wiki ingest: load pending list: %w", loadErr)
	}
	pendingOpsCount = len(pendingOps)
	if len(pendingOps) == 0 {
		exitStatus = "no_pending_ops"
		logger.Infof(ctx, "wiki ingest: no pending operations for KB %s", payload.KnowledgeBaseID)
		// We claimed nothing, but rows may still exist held by a FRESH claim
		// (a concurrent batch that is still running, or one that crashed
		// mid-flight and left claimed_at stamped). This no-op return does not
		// chain a follow-up, so without a safety net a crashed batch's rows
		// would sit unclaimable until wikiClaimStaleAfter AND never get
		// re-triggered afterwards — stranding the KB indefinitely. Schedule a
		// coalesced recheck timed past the stale threshold so those rows are
		// reclaimed automatically once eligible.
		followUpScheduled = s.scheduleStaleClaimRecheck(ctx, payload)
		return nil
	}

	logger.Infof(ctx, "wiki ingest: batch processing %d ops for KB %s", len(pendingOps), payload.KnowledgeBaseID)

	claimsSettled := false

	var ownerGuard *wikiOwnerGuard
	if claimOwner != nil {
		ownerGuard, loadErr = s.acquireWikiOwnerGuard(
			ctx, payload.TenantID, pendingOps, peekedIDs, *claimOwner,
		)
		if loadErr != nil {
			exitStatus = "owner_lease_failed"
			if lease, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease); ok {
				releaseCtx, releaseCancel := wikiIngestCleanupContext(ctx)
				releaseErr := lease.ReleaseClaims(releaseCtx, peekedIDs, *claimOwner)
				releaseCancel()
				if releaseErr != nil {
					return errors.Join(
						fmt.Errorf("wiki ingest: acquire processing owner lease: %w", loadErr),
						fmt.Errorf("wiki ingest: release claims after owner lease failure: %w", releaseErr),
					)
				}
			}
			return fmt.Errorf("wiki ingest: acquire processing owner lease: %w", loadErr)
		}
		defer ownerGuard.Release()
		var workCancel context.CancelFunc
		ctx, workCancel = ownerGuard.WorkContext(ctx)
		defer workCancel()
	}

	// Register this after the owner guard's defers so LIFO ordering performs
	// durable abort accounting while the owner lease is still valid. The old
	// order released the guard first, which cancelled the supposedly detached
	// cleanup context and left rows claimed until the stale timeout.
	if s.redisClient != nil && len(peekedIDs) > 0 {
		defer func() {
			if claimsSettled {
				return
			}
			abortCause := retErr
			if abortCause == nil {
				abortCause = context.Cause(ctx)
			}
			if abortCause == nil {
				abortCause = errors.New("wiki ingest batch aborted before durable settlement")
			}

			var settleErr error
			if ownerGuard == nil || ownerGuard.Err() == nil {
				settleCtx, settleCancel := wikiPagePersistContext(ctx)
				settleErr = s.settleAbortedWikiBatch(
					settleCtx, payload, pendingOps, abortCause,
				)
				settleCancel()
				if settleErr == nil {
					logger.Warnf(ctx,
						"wiki ingest: durably accounted for %d claimed rows on abnormal exit for KB %s: %v",
						len(peekedIDs), payload.KnowledgeBaseID, abortCause)
					return
				}
			} else {
				settleErr = fmt.Errorf("processing owner lease lost: %w", ownerGuard.Err())
			}
			logger.Warnf(ctx,
				"wiki ingest: abort settlement failed for KB %s; falling back to claim release: %v",
				payload.KnowledgeBaseID, settleErr)

			lease, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease)
			if !ok || claimOwner == nil {
				logger.Warnf(ctx, "wiki ingest: cannot owner-safely release %d claims for KB %s", len(peekedIDs), payload.KnowledgeBaseID)
				return
			}
			// Ignore owner-context cancellation only for the fenced DB release. The
			// claim token/task id CAS prevents a stale worker from releasing a
			// successor's ownership.
			releaseCtx, releaseCancel := context.WithTimeout(
				context.WithoutCancel(ctx), wikiIngestCleanupTimeout,
			)
			defer releaseCancel()
			if err := lease.ReleaseClaims(releaseCtx, peekedIDs, *claimOwner); err != nil {
				logger.Warnf(ctx, "wiki ingest: failed to release %d claims on abnormal exit for KB %s: %v", len(peekedIDs), payload.KnowledgeBaseID, err)
				return
			}
			logger.Warnf(ctx, "wiki ingest: released %d claimed rows on abnormal exit for KB %s (re-claimable immediately)", len(peekedIDs), payload.KnowledgeBaseID)
		}()
	}

	// Resolve extraction granularity once per batch. Historical rows with
	// empty/unknown values fall back to Standard via Normalize(). Failures
	// to load the KB (unlikely since we're already acting on it) also
	// degrade gracefully to Standard.
	var wikiConfig *types.WikiConfig
	if kb, kbErr := s.kbService.GetKnowledgeBaseByID(ctx, payload.KnowledgeBaseID); kbErr == nil && kb != nil && kb.WikiConfig != nil {
		wikiConfig = kb.WikiConfig
	}

	batchCtx := s.newWikiBatchContext(payload.KnowledgeBaseID, wikiConfig)

	// 1. MAP PHASE (Parallel extraction and generation of updates)
	var mapMu sync.Mutex
	var failedOps []WikiPendingOp
	var terminalOps []WikiPendingOp
	slugUpdates := make(map[string][]SlugUpdate)
	var docResults []*docIngestResult
	var retractFolderIDs []string
	// rateLimited flips true when any map/reduce LLM failure looks like an
	// upstream 429/quota trip. It steers the follow-up scheduler onto the
	// longer wikiRateLimitBackoff so retries don't keep hammering an already
	// saturated rpm budget. Guarded by mapMu (map phase) / reduceMu (reduce
	// phase) — both are held when written.
	rateLimited := false

	eg, mapCtx := errgroup.WithContext(ctx)
	eg.SetLimit(mapParallel) // Map phase limit (configurable via WikiConfig)

	for _, op := range pendingOps {
		op := op
		eg.Go(func() error {
			if op.Op == WikiOpRetract {
				// Resolve the authoritative page set at run-time. The caller
				// (knowledgeService.cleanupWikiOnKnowledgeDelete) captures
				// PageSlugs from a DB snapshot taken *before* this task fires,
				// but there is a window where:
				//   - cleanup ran before ingest → snapshot is empty, but a
				//     concurrent ingest may have already created pages by now
				//   - a previous ingest batch created new pages after cleanup
				//     captured its snapshot
				// Re-querying ListPagesBySourceRef here unions the caller's
				// slugs with whatever currently references the knowledge, so
				// no page is left un-retracted. It also lets us support
				// callers that deliberately enqueue retract with empty
				// PageSlugs as "figure it out yourself" — see
				// cleanupWikiOnKnowledgeDelete's comment (3).
				slugSet := make(map[string]struct{}, len(op.PageSlugs))
				folderSet := make(map[string]struct{}, len(op.FolderIDs))
				for _, slug := range op.PageSlugs {
					if slug == "" {
						continue
					}
					slugSet[slug] = struct{}{}
				}
				for _, folderID := range op.FolderIDs {
					if folderID != "" {
						folderSet[folderID] = struct{}{}
					}
				}
				if op.KnowledgeID != "" {
					livePages, err := s.wikiService.ListPagesBySourceRef(mapCtx, payload.KnowledgeBaseID, op.KnowledgeID)
					if err != nil {
						mapMu.Lock()
						failedOps = append(failedOps, op)
						mapMu.Unlock()
						return nil
					}
					for _, p := range livePages {
						if p == nil || p.Slug == "" {
							continue
						}
						// Index pages never carry real source_refs;
						// if they somehow surface here, skip — the
						// reduce stage would be a no-op anyway.
						if p.PageType == types.WikiPageTypeIndex {
							continue
						}
						slugSet[p.Slug] = struct{}{}
						if p.FolderID != "" {
							folderSet[p.FolderID] = struct{}{}
						}
					}
				}

				mapMu.Lock()
				retractOps++
				retractHandled++
				docPreview = append(docPreview, fmt.Sprintf("retract[%s]: %s (%d slugs)", previewText(op.KnowledgeID, 24), previewText(op.DocTitle, 48), len(slugSet)))

				for slug := range slugSet {
					slugUpdates[slug] = append(slugUpdates[slug], SlugUpdate{
						Slug:              slug,
						Type:              "retract",
						RetractDocContent: op.DocSummary,
						DocTitle:          op.DocTitle,
						KnowledgeID:       op.KnowledgeID,
						Attempt:           op.Attempt,
						Language:          types.ResolveLanguageName(ctx, op.Language),
					})
				}
				for folderID := range folderSet {
					retractFolderIDs = append(retractFolderIDs, folderID)
				}
				mapMu.Unlock()
				return nil
			}

			// Ingest
			if op.Attempt > 0 {
				alreadyDone, doneErr := s.wikiOpAlreadyDoneStrict(
					mapCtx, op.KnowledgeID, op.Attempt,
				)
				if doneErr != nil {
					mapMu.Lock()
					failedOps = append(failedOps, op)
					mapMu.Unlock()
					logger.Warnf(mapCtx,
						"wiki ingest: failed to verify replay state for knowledge %s: %v",
						op.KnowledgeID, doneErr)
					return nil
				}
				if alreadyDone {
					mapMu.Lock()
					terminalOps = append(terminalOps, op)
					docPreview = append(docPreview, fmt.Sprintf(
						"ingest-replay-ack[%s]", previewText(op.KnowledgeID, 24),
					))
					mapMu.Unlock()
					logger.Infof(mapCtx,
						"wiki ingest: exact Wiki owner already done; acknowledging durable row without another LLM run knowledge=%s attempt=%d",
						op.KnowledgeID, op.Attempt)
					return nil
				}
			}
			mapMu.Lock()
			ingestOps++
			mapMu.Unlock()

			logger.Infof(mapCtx, "wiki ingest: processing document '%s' (%s)", op.DocTitle, op.KnowledgeID)
			result, updates, err := s.mapOneDocument(mapCtx, chatModel, payload, op, batchCtx)
			if err != nil {
				mapMu.Lock()
				ingestFailed++
				failedOps = append(failedOps, op)
				if isLikelyRateLimitError(err) {
					rateLimited = true
				}
				mapMu.Unlock()
				logger.Warnf(mapCtx, "wiki ingest: failed to map knowledge %s: %v", op.KnowledgeID, err)
				return nil // Don't fail the whole batch
			}

			if result != nil {
				mapMu.Lock()
				ingestSucceeded++
				docResults = append(docResults, result)
				docPreview = append(docPreview, fmt.Sprintf("ingest[%s]: title=%s summary=%s", previewText(result.KnowledgeID, 24), previewText(result.DocTitle, 40), previewText(result.Summary, 64)))
				for _, u := range updates {
					slugUpdates[u.Slug] = append(slugUpdates[u.Slug], u)
				}
				mapMu.Unlock()

				// No fail-count reset needed: a successful op is added
				// to peekedIDs and gets DELETEd from task_pending_ops at
				// trim time, so there is no stale fail_count column to
				// scrub. Compare with the legacy Redis path, which kept
				// a separate wiki:failcount:<...> key alive for 24h
				// regardless of whether the original op had drained.
				//
				// The finalizing slot is drained later (after reduce +
				// publish) in the docResults loop, so "completed" only
				// arrives once wiki is fully written.
			} else {
				// err == nil && result == nil: mapOneDocument skipped this
				// doc at a terminal, non-retryable state (knowledge
				// deleted / no chunks / insufficient text). Record it as
				// terminal, but do not release the finalizing slot until the
				// matching pending row has been deleted durably at settlement.
				mapMu.Lock()
				terminalOps = append(terminalOps, op)
				mapMu.Unlock()
			}
			return nil
		})
	}
	_ = eg.Wait()

	// Documents are mapped concurrently, so two documents in this batch may
	// independently emit spelling variants for the same real subject before
	// either page exists. Resolve those exact identities as one canonical slug
	// before contribution checkpoints, taxonomy planning, and reduce.
	restoredKnowledgeIDs := make(map[string]bool)
	for _, result := range docResults {
		if result != nil && result.CheckpointReused {
			restoredKnowledgeIDs[result.KnowledgeID] = true
		}
	}
	restoredAliases := s.existingAliasesForRestoredUpdates(
		ctx, payload.KnowledgeBaseID, slugUpdates, restoredKnowledgeIDs,
	)
	if len(restoredAliases) > 0 {
		slugUpdates = rewriteBatchSlugUpdates(slugUpdates, restoredAliases)
		rewriteDocResultSlugs(docResults, restoredAliases)
		logger.Infof(ctx, "wiki ingest: remapped %d restored checkpoint slug variant(s) to current pages", len(restoredAliases))
	}
	durableAliases, err := s.resolveDurableCanonicalAliases(
		ctx, payload.TenantID, payload.KnowledgeBaseID, slugUpdates,
	)
	if err != nil {
		return fmt.Errorf("wiki ingest: resolve durable canonical identities: %w", err)
	}
	if len(durableAliases) > 0 {
		slugUpdates = rewriteBatchSlugUpdates(slugUpdates, durableAliases)
		rewriteDocResultSlugs(docResults, durableAliases)
		logger.Infof(ctx, "wiki ingest: reused %d durable canonical page identity(s)", len(durableAliases))
	}

	existingEntityConceptSlugs := make(map[string]bool)
	if batchCtx != nil && batchCtx.SlugTitleMany != nil {
		candidateSlugs := make([]string, 0, len(slugUpdates))
		for slug, updates := range slugUpdates {
			for _, update := range updates {
				if update.Type == types.WikiPageTypeEntity || update.Type == types.WikiPageTypeConcept {
					candidateSlugs = append(candidateSlugs, slug)
					break
				}
			}
		}
		sort.Strings(candidateSlugs)
		for slug := range batchCtx.SlugTitleMany(ctx, candidateSlugs) {
			existingEntityConceptSlugs[slug] = true
		}
	}
	var batchSlugAliases map[string]string
	slugUpdates, batchSlugAliases = canonicalizeBatchSlugUpdates(slugUpdates, existingEntityConceptSlugs)
	rewriteDocResultSlugs(docResults, batchSlugAliases)
	if len(batchSlugAliases) > 0 {
		logger.Infof(ctx, "wiki ingest: canonicalized %d duplicate slug variant(s) across mapped documents", len(batchSlugAliases))
	}
	reservedAliases := s.reserveConcurrentCanonicalAliases(
		ctx, payload.KnowledgeBaseID, slugUpdates, existingEntityConceptSlugs,
	)
	if len(reservedAliases) > 0 {
		slugUpdates = rewriteBatchSlugUpdates(slugUpdates, reservedAliases)
		rewriteDocResultSlugs(docResults, reservedAliases)
		logger.Infof(ctx, "wiki ingest: converged %d slug variant(s) across concurrent batches", len(reservedAliases))
	}

	var reusedPublishedSlugsByKID map[string]map[string]struct{}
	slugUpdates, reusedPublishedSlugsByKID, err = s.filterPublishedWikiContributions(ctx, slugUpdates)
	if err != nil {
		return fmt.Errorf("wiki ingest: filter published contribution checkpoints: %w", err)
	}

	// Plan the directory once for the whole batch BEFORE reduce. Reduce writes
	// pages in parallel, so it can't converge on shared folders on its own; this
	// single pass assigns every new entity/concept slug a coherent category_path
	// that reuses existing folders. Reduce then only applies the plan to pages
	// that don't already have a category (user-curated pages are never churned).
	taxonomyPlan, taxonomyErr := s.planBatchTaxonomy(ctx, chatModel, kb, slugUpdates, lang)
	if taxonomyErr != nil {
		return fmt.Errorf("wiki ingest: plan taxonomy: %w", taxonomyErr)
	}
	batchCtx.PlannedFolderID = s.resolvePlannedFolders(ctx, kb, taxonomyPlan)

	// 2. REDUCE PHASE (Parallel upserting grouped by Slug)
	egReduce, reduceCtx := errgroup.WithContext(ctx)
	egReduce.SetLimit(reduceParallel) // Reduce phase limit (LLM + DB concurrent connections, configurable)

	var reduceMu sync.Mutex
	var allPagesAffected []string
	var ingestPagesAffected []string
	var retractPagesAffected []string
	// failedAdditionSlugs collects entity/concept slugs whose page
	// generation LLM call failed (so the page was never written). The
	// post-reduce cleanup step uses this set to strip dead [[slug]]
	// references from the same batch's summary pages and exclude failed
	// pages from finalize processing.
	failedAdditionSlugs := make(map[string]struct{})
	// unappliedSlugKIDs collects the knowledge_ids that contributed to a
	// slug whose update never landed — either because we could NOT acquire
	// the per-slug lock within wikiSlugLockWait, or because reduce returned
	// an error. In both cases the page keeps its prior content, so the
	// owning document(s) must be re-queued rather than trimmed — otherwise
	// the row is deleted and the contribution is silently lost forever
	// (finalize only rebuilds the index / cross-links, it does not re-run
	// reduce). requeueFailedOps' fail_count budget bounds the retries and
	// dead-letters a document whose slug fails/stays hot permanently.
	unappliedSlugKIDs := make(map[string]struct{})
	// The per-document slug maps distinguish durable page writes from model
	// candidates. They are read only after egReduce.Wait, and all writes are
	// protected by reduceMu.
	persistedSlugsByKID := make(map[string]map[string]struct{})
	for knowledgeID, slugs := range reusedPublishedSlugsByKID {
		persistedSlugsByKID[knowledgeID] = make(map[string]struct{}, len(slugs))
		for slug := range slugs {
			persistedSlugsByKID[knowledgeID][slug] = struct{}{}
		}
	}
	unappliedSlugsByKID := make(map[string]map[string]struct{})
	// collectUnapplied records every knowledge_id backing a slug we failed
	// to apply. Caller holds no lock; it takes reduceMu itself.
	collectUnapplied := func(slug string, updates []SlugUpdate) {
		reduceMu.Lock()
		for _, u := range updates {
			if u.KnowledgeID != "" {
				unappliedSlugKIDs[u.KnowledgeID] = struct{}{}
				if unappliedSlugsByKID[u.KnowledgeID] == nil {
					unappliedSlugsByKID[u.KnowledgeID] = make(map[string]struct{})
				}
				unappliedSlugsByKID[u.KnowledgeID][slug] = struct{}{}
			}
		}
		reduceMu.Unlock()
	}
	recordPersisted := func(slug string, updates []SlugUpdate) {
		reduceMu.Lock()
		for _, u := range updates {
			if u.KnowledgeID == "" {
				continue
			}
			if persistedSlugsByKID[u.KnowledgeID] == nil {
				persistedSlugsByKID[u.KnowledgeID] = make(map[string]struct{})
			}
			persistedSlugsByKID[u.KnowledgeID][slug] = struct{}{}
		}
		reduceMu.Unlock()
	}

	// Build the kid → wikiSpan lookup before kicking off reduce. Each
	// per-slug reduce attaches a postprocess.wiki.page[slug] subspan
	// under the FIRST contributing doc's wiki span — see comment in
	// reduceSlugUpdates for the multi-contributor attribution rule.
	kidToWikiSpan := make(map[string]*Span, len(docResults))
	for _, r := range docResults {
		if r != nil && r.WikiSpan != nil {
			kidToWikiSpan[r.KnowledgeID] = r.WikiSpan
		}
	}

	for slug, updates := range slugUpdates {
		slug := slug
		updates := updates
		egReduce.Go(func() error {
			var (
				changed        bool
				affectedType   string
				additionFailed bool
				reduceErr      error
			)
			// Serialize same-slug read-modify-write across concurrent batches
			// (standard mode). runs fn directly in Lite mode.
			acquired, lockErr := s.withSlugLock(reduceCtx, payload.KnowledgeBaseID, slug, func() error {
				changed, affectedType, additionFailed, reduceErr = s.reduceSlugUpdates(
					reduceCtx, chatModel, payload.KnowledgeBaseID, slug, updates, payload.TenantID, batchCtx, kidToWikiSpan)
				return reduceErr
			})
			if lockErr != nil {
				if reduceErr != nil {
					logger.Warnf(reduceCtx, "wiki ingest: reduce failed for slug %s: %v", slug, reduceErr)
					if isLikelyRateLimitError(reduceErr) {
						reduceMu.Lock()
						rateLimited = true
						reduceMu.Unlock()
					}
					if additionFailed {
						reduceMu.Lock()
						failedAdditionSlugs[slug] = struct{}{}
						reduceMu.Unlock()
					}
				} else {
					logger.Warnf(reduceCtx, "wiki ingest: lock failed for slug %s: %v", slug, lockErr)
				}
				collectUnapplied(slug, updates)
				return nil
			}
			if !acquired {
				// Contended slug we couldn't get in time. The page keeps its
				// prior content, so the documents that fed this slug are NOT
				// done: record their knowledge_ids so the trim phase re-queues
				// them (via the failed-op retry budget) for a later,
				// hopefully-uncontended batch instead of deleting their rows.
				logger.Warnf(reduceCtx, "wiki ingest: slug %s busy > %s, deferring update", slug, wikiSlugLockWait)
				collectUnapplied(slug, updates)
				return nil
			}
			if changed {
				recordPersisted(slug, updates)
				reduceMu.Lock()
				allPagesAffected = append(allPagesAffected, slug)
				if affectedType == "ingest" {
					ingestPagesAffected = append(ingestPagesAffected, slug)
				} else if affectedType == "retract" {
					retractPagesAffected = append(retractPagesAffected, slug)
				}
				reduceMu.Unlock()
			}
			if additionFailed {
				reduceMu.Lock()
				failedAdditionSlugs[slug] = struct{}{}
				reduceMu.Unlock()
			}
			return nil
		})
	}
	_ = egReduce.Wait()
	if ownerGuard != nil && ownerGuard.Err() != nil {
		return fmt.Errorf("wiki ingest: processing owner lease lost: %w", ownerGuard.Err())
	}

	// The expensive model phase can consume nearly the whole 90-minute task
	// budget. Refresh the row lease before detached publication and settlement
	// so those successful durable writes cannot overlap a stale-claim worker.
	if claimOwner != nil {
		lease, ok := s.pendingRepo.(interfaces.TaskPendingOpsClaimLease)
		if !ok {
			return fmt.Errorf("wiki ingest: owner-safe claim renewal is unavailable")
		}
		renewCtx, renewCancel := wikiPagePersistContext(ctx)
		renewErr := lease.RenewClaims(renewCtx, peekedIDs, *claimOwner)
		renewCancel()
		if renewErr != nil {
			return fmt.Errorf("wiki ingest: renew claims before durable tail: %w", renewErr)
		}
	}

	tailCtx, tailCancel := wikiIngestCleanupContext(ctx)
	defer tailCancel()

	// Sanitize the doc summary pages produced by this batch BEFORE we
	// rebuild the index. The summary LLM (run during
	// map) was free to inject [[entity/foo|name]] links to every slug it
	// saw extracted, but reduce may have failed to materialize some of
	// those slugs into actual pages. Rewrite those dead links to plain
	// text so the summary doesn't contain unresolvable references.
	if len(failedAdditionSlugs) > 0 && len(docResults) > 0 {
		s.sanitizeDeadSummaryLinks(tailCtx, payload.KnowledgeBaseID, docResults, failedAdditionSlugs, batchCtx)
	}

	// Publish freshly-generated pages immediately (NOT deferred to finalize):
	// users should see a document's wiki pages as soon as their content is
	// written, not after the debounce window. This is a cheap status flip.
	if len(allPagesAffected) > 0 {
		logger.Infof(ctx, "wiki ingest: publishing draft pages")
		publishFailures := s.publishDraftPages(
			tailCtx, payload.TenantID, payload.KnowledgeBaseID, allPagesAffected, slugUpdates,
		)
		if len(publishFailures) > 0 {
			failedSlugs := make(map[string]struct{}, len(publishFailures))
			for slug, publishErr := range publishFailures {
				failedSlugs[slug] = struct{}{}
				logger.Warnf(ctx, "wiki ingest: page %s was written but not durably published: %v", slug, publishErr)
				collectUnapplied(slug, slugUpdates[slug])
			}
			filterPublished := func(slugs []string) []string {
				kept := slugs[:0]
				for _, slug := range slugs {
					if _, failed := failedSlugs[slug]; !failed {
						kept = append(kept, slug)
					}
				}
				return kept
			}
			allPagesAffected = filterPublished(allPagesAffected)
			ingestPagesAffected = filterPublished(ingestPagesAffected)
			retractPagesAffected = filterPublished(retractPagesAffected)
		}
	}

	completedDocResults := make([]*docIngestResult, 0, len(docResults))
	for _, result := range docResults {
		if result == nil {
			continue
		}
		if _, unapplied := unappliedSlugKIDs[result.KnowledgeID]; unapplied {
			continue
		}
		completedDocResults = append(completedDocResults, result)
	}
	totalPagesAffected = len(allPagesAffected)

	// Project one bounded summary into the knowledge-base activity feed.
	// Detailed per-document Wiki log rows duplicated that feed and were never
	// consumed by retrieval, so the activity record is now written directly.
	wikiActivityActions := make(map[string]int, 2)
	for _, op := range pendingOps {
		if op.Op == WikiOpRetract {
			wikiActivityActions["retract"]++
		}
	}
	for _, r := range completedDocResults {
		if r != nil {
			wikiActivityActions["ingest"]++
		}
	}
	RecordWikiContentActivity(tailCtx, s.audit, payload.TenantID, payload.KnowledgeBaseID, wikiActivityActions)

	// Defer KB-global convergence (index-intro rebuild + dead-link cleanup +
	// cross-link injection) to a debounced per-KB wiki:finalize task instead of
	// running it at the tail of every 5-doc batch. We record what changed into
	// the finalize lane and schedule a coalesced trigger; a burst of N documents
	// then rebuilds the index ONCE. See ProcessWikiFinalize.
	//
	// freshTitleBySlug carries the (slug → title) pairs this batch successfully
	// wrote (minus reduce-phase failures) so finalize's cross-link pass can
	// linkify mentions of the new pages.
	freshTitleBySlug := make(map[string]string, len(completedDocResults)*4)
	for _, dr := range completedDocResults {
		if dr == nil {
			continue
		}
		for _, p := range dr.Pages {
			if p.Slug == "" || p.Title == "" {
				continue
			}
			if _, persisted := persistedSlugsByKID[dr.KnowledgeID][p.Slug]; !persisted {
				continue
			}
			freshTitleBySlug[p.Slug] = p.Title
		}
	}
	if len(allPagesAffected) > 0 || len(completedDocResults) > 0 || retractHandled > 0 || len(retractFolderIDs) > 0 {
		changes := make([]wikiFinalizeChange, 0, len(completedDocResults)+len(pendingOps))
		for _, r := range completedDocResults {
			changes = append(changes, wikiFinalizeChange{
				Action: wikiFinalizeAdded, DocTitle: r.DocTitle, DocSummary: r.Summary,
			})
		}
		for _, op := range pendingOps {
			if op.Op == WikiOpRetract {
				changes = append(changes, wikiFinalizeChange{
					Action: wikiFinalizeRemoved, DocTitle: op.DocTitle, DocSummary: op.DocSummary,
				})
			}
		}
		finalizeCtx, finalizeCancel := wikiPagePersistContext(ctx)
		finalizeErr := s.enqueueFinalize(
			finalizeCtx, payload, allPagesAffected, freshTitleBySlug, changes, retractFolderIDs,
		)
		finalizeCancel()
		if finalizeErr != nil {
			logger.Warnf(ctx, "wiki ingest: durable finalize enqueue failed: %v", finalizeErr)
			for _, op := range pendingOps {
				if op.KnowledgeID == "" {
					continue
				}
				unappliedSlugKIDs[op.KnowledgeID] = struct{}{}
				if unappliedSlugsByKID[op.KnowledgeID] == nil {
					unappliedSlugsByKID[op.KnowledgeID] = make(map[string]struct{})
				}
				unappliedSlugsByKID[op.KnowledgeID]["__finalize__"] = struct{}{}
			}
		}
	}

	// Close postprocess.wiki spans for every successfully-mapped doc.
	// Span duration now spans map + reduce + index rebuild + cleanup +
	// cross-link injection + publish, matching the wall-clock window
	// the user thinks of as "wiki processing for this knowledge".
	// Per-doc page write outcomes are summarised in the output so the
	// trace viewer can show how many of the doc's extracted pages
	// actually landed (vs. dropped because reduce-phase generation
	// failed).
	spanCtx, spanCancel := wikiPagePersistContext(ctx)
	for _, r := range docResults {
		if r == nil {
			continue
		}
		// A successfully-mapped doc is terminal for its wiki op unless one
		// of its slug writes was unapplied. Record it now, then release its
		// finalizing slot only after the matching pending rows are deleted
		// durably in settleWikiIngestRows below.
		if _, unapplied := unappliedSlugKIDs[r.KnowledgeID]; !unapplied {
			terminalOps = append(terminalOps, r.SourceOp)
		}
		if r.WikiSpan == nil {
			continue
		}
		unappliedSlugs := unappliedSlugsByKID[r.KnowledgeID]
		if len(unappliedSlugs) > 0 {
			persistErr := fmt.Errorf("%d Wiki page write(s) were not durably persisted", len(unappliedSlugs))
			s.tracker().FailSpan(spanCtx, r.WikiSpan, "WIKI_PERSIST_FAILED", persistErr.Error(), persistErr)
			continue
		}
		output := wikiPageWriteOutcome(r, persistedSlugsByKID[r.KnowledgeID], nil)
		s.tracker().EndSpan(spanCtx, r.WikiSpan, output)
	}
	spanCancel()
	// Failed-map docs already had FailSpan called inside
	// mapOneDocument (the failedOps path returns before reaching
	// docResults). Nothing extra to do here for them.

	// Fold documents with an unapplied slug (contended lock or reduce error)
	// into failedOps so they are neither trimmed nor promoted to completed:
	// requeueFailedOps then runs them through the same fail_count budget
	// (retry now, dead-letter once the slug stays permanently hot/broken) as
	// a map-phase failure. A doc already counted as a map failure is skipped
	// to avoid a double fail_count bump.
	if len(unappliedSlugKIDs) > 0 {
		failedKIDs := make(map[string]struct{}, len(failedOps))
		for _, op := range failedOps {
			failedKIDs[op.KnowledgeID] = struct{}{}
		}
		for _, op := range pendingOps {
			if _, unapplied := unappliedSlugKIDs[op.KnowledgeID]; !unapplied {
				continue
			}
			if _, already := failedKIDs[op.KnowledgeID]; already {
				continue
			}
			failedOps = append(failedOps, op)
			failedKIDs[op.KnowledgeID] = struct{}{}
		}
	}

	// Build the trim set: rows that should be removed from
	// task_pending_ops. We start from the full peekedIDs (every row we
	// pulled, even ones de-duplicated by knowledge_id) and subtract
	// any failed op's dbID — those need to stay in place so the
	// requeueFailedOps path can decide between retry and dead-letter.
	failedIDSet := make(map[int64]struct{}, len(failedOps))
	for _, op := range failedOps {
		if op.dbID != 0 {
			failedIDSet[op.dbID] = struct{}{}
		}
	}
	trimIDs := make([]int64, 0, len(peekedIDs))
	for _, id := range peekedIDs {
		if _, fail := failedIDSet[id]; fail {
			continue
		}
		trimIDs = append(trimIDs, id)
	}
	// The atomic settlement below deletes/releases the claimed rows. Stop only
	// the DB heartbeat first so its renewal cannot race the owner-CAS delete;
	// per-owner Redis leases remain live through the complete durable tail.
	if ownerGuard != nil {
		ownerGuard.StopClaimRenewal()
	}
	settleErr := s.settleWikiIngestRows(
		ctx, payload, trimIDs, failedOps, terminalOps, claimOwner,
	)
	if settleErr != nil {
		exitStatus = "settle_failed"
		return fmt.Errorf("wiki ingest: settle claimed rows: %w", settleErr)
	}
	if ownerGuard != nil && ownerGuard.Err() != nil {
		return fmt.Errorf("wiki ingest: processing owner lease lost during durable tail: %w", ownerGuard.Err())
	}
	// All claimed rows have now reached a terminal state (deleted on success,
	// released for retry, or dead-lettered), so disarm the abnormal-exit
	// release net.
	claimsSettled = true

	logger.Infof(ctx, "wiki ingest: batch completed for KB %s, %d ops, %d pages affected", payload.KnowledgeBaseID, len(pendingOps), len(allPagesAffected))

	// Pace the follow-up: on a rate-limit trip, back off so the per-minute
	// window can reset instead of retrying the failed docs immediately.
	followUpDelay := wikiFollowUpDelay
	if rateLimited {
		followUpDelay = wikiRateLimitBackoff
		logger.Warnf(ctx, "wiki ingest: KB %s hit upstream rate limiting, backing off follow-up to %s", payload.KnowledgeBaseID, followUpDelay)
	}
	followCtx, followCancel := wikiIngestCleanupContext(ctx)
	followUpScheduled = s.scheduleFollowUp(followCtx, payload, followUpDelay)
	followCancel()
	return nil
}

// settleWikiFinalizeFailure keeps index-change rows durable while an index
// rebuild is retryable. Once a row exhausts the same bounded retry budget as
// ingest work, it is archived and only then removed from the pending lane.
// A nil return means every affected change row reached that terminal archive;
// otherwise the returned error asks asynq to retry without trimming the rows.
func (s *wikiIngestService) settleWikiFinalizeFailure(
	ctx context.Context,
	payload WikiIngestPayload,
	rows []*types.TaskPendingOp,
	cause error,
) error {
	handled := false
	retry := false
	var settleErrs []error
	for _, row := range rows {
		if row == nil || row.Op != wikiFinalizeOpChange {
			continue
		}
		handled = true
		count, err := s.pendingRepo.IncrFailCount(ctx, row.ID)
		if err != nil {
			retry = true
			settleErrs = append(settleErrs, fmt.Errorf("increment finalize fail count id=%d: %w", row.ID, err))
			continue
		}
		if count == 0 {
			// The row disappeared after PeekBatch; another terminal path
			// already consumed it, so there is nothing left to retry.
			continue
		}
		if count <= wikiMaxFailRetries {
			retry = true
			continue
		}
		if s.deadLetterRepo == nil {
			retry = true
			settleErrs = append(settleErrs, fmt.Errorf("archive finalize dead letter id=%d: repository unavailable", row.ID))
			continue
		}
		if err := s.deadLetterRepo.Insert(ctx, &types.TaskDeadLetter{
			TenantID:  payload.TenantID,
			TaskType:  wikiFinalizeTaskType,
			Scope:     wikiTaskScope,
			ScopeID:   payload.KnowledgeBaseID,
			RelatedID: row.DedupKey,
			Payload:   append(json.RawMessage(nil), row.Payload...),
			LastError: cause.Error(),
			FailCount: count,
		}); err != nil {
			retry = true
			settleErrs = append(settleErrs, fmt.Errorf("archive finalize dead letter id=%d: %w", row.ID, err))
			continue
		}
		if err := s.pendingRepo.DeleteByIDs(ctx, []int64{row.ID}); err != nil {
			retry = true
			settleErrs = append(settleErrs, fmt.Errorf("drop finalize dead letter id=%d: %w", row.ID, err))
			continue
		}
		logger.Warnf(ctx, "wiki finalize: archived change row id=%d after %d failures: %v", row.ID, count, cause)
	}
	if !handled {
		return cause
	}
	if retry {
		return errors.Join(cause, errors.Join(settleErrs...))
	}
	return errors.Join(settleErrs...)
}

// ProcessWikiFinalize runs the debounced, per-KB KB-global convergence pass:
// index-intro rebuild, dead-link cleanup, and cross-link injection. It drains
// the finalize lane of task_pending_ops (written by ProcessWikiIngest via
// enqueueFinalize) so a burst of documents rebuilds the index ONCE instead of
// once per 5-doc batch.
func (s *wikiIngestService) ProcessWikiFinalize(ctx context.Context, t *asynq.Task) error {
	startedAt := time.Now()
	var payload WikiIngestPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("wiki finalize: unmarshal payload: %w", err)
	}
	ctx = context.WithValue(ctx, types.TenantIDContextKey, payload.TenantID)
	if payload.Language != "" {
		ctx = context.WithValue(ctx, types.LanguageContextKey, payload.Language)
	}
	if s.pendingRepo == nil {
		return nil
	}

	// Per-KB finalize lock, separate from the ingest active lock so finalize
	// and ingest batches never block each other. Coalescing via asynq.TaskID
	// already keeps at most one finalize pending per KB; this guards the
	// reschedule-overlap window against a concurrent index-page write.
	if s.redisClient != nil {
		key := "wiki:finalize:active:" + payload.KnowledgeBaseID
		acquired, err := s.redisClient.SetNX(ctx, key, "1", wikiFinalizeLockTTL).Result()
		if err != nil {
			// Fail CLOSED: proceeding unlocked would let two finalize runs
			// drain the same PeekBatch rows and double-rebuild the index page
			// (GetPage→modify→UpdatePage lost update). Return an error so
			// asynq retries the (coalesced) finalize once Redis recovers,
			// instead of racing a concurrent finalize.
			logger.Warnf(ctx, "wiki finalize: SetNX failed for KB %s: %v (retrying)", payload.KnowledgeBaseID, err)
			return fmt.Errorf("wiki finalize: acquire lock: %w", err)
		} else if !acquired {
			// Another finalize is running; it will drain the lane and
			// reschedule if rows remain. Safe to no-op.
			return nil
		}
		lockCtx, cancelLock := context.WithCancel(context.Background())
		defer func() {
			cancelLock()
			s.redisClient.Del(context.Background(), key)
		}()
		go func() {
			ticker := time.NewTicker(wikiFinalizeLockRenew)
			defer ticker.Stop()
			for {
				select {
				case <-lockCtx.Done():
					return
				case <-ticker.C:
					s.redisClient.Expire(context.Background(), key, wikiFinalizeLockTTL)
				}
			}
		}()
	} else {
		if _, loaded := s.liteFinalizeLocks.LoadOrStore(payload.KnowledgeBaseID, struct{}{}); loaded {
			return nil
		}
		defer s.liteFinalizeLocks.Delete(payload.KnowledgeBaseID)
	}

	rows, err := s.pendingRepo.PeekBatch(ctx, wikiFinalizeTaskType, wikiTaskScope, payload.KnowledgeBaseID, wikiFinalizeMaxRows)
	if err != nil {
		return fmt.Errorf("wiki finalize: peek: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, payload.KnowledgeBaseID)
	if errors.Is(err, apprepo.ErrKnowledgeBaseNotFound) || (err == nil && kb == nil) {
		if cleanupErr := s.clearDeletedKnowledgeBasePendingOps(ctx, payload.KnowledgeBaseID); cleanupErr != nil {
			return fmt.Errorf("wiki finalize: clear deleted KB queue: %w", cleanupErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("wiki finalize: get KB: %w", err)
	}

	// Aggregate the drained rows into: affected slugs (dedup), fresh cross-link
	// refs, and the index-intro change description. We collect ids up front so
	// we can drain the lane even on the KB-disabled short-circuit below.
	ids := make([]int64, 0, len(rows))
	pruneRowIDs := make([]int64, 0)
	affectedSet := make(map[string]struct{}, len(rows))
	var affectedSlugs []string
	var freshRefs []linkRef
	var folderPruneIDs []string
	fullCanonicalReconcile := false
	var changeDesc strings.Builder
	for _, r := range rows {
		ids = append(ids, r.ID)
		if r.Op == wikiFinalizeOpFolderPrune {
			pruneRowIDs = append(pruneRowIDs, r.ID)
		}
		if r.Op == wikiFinalizeOpCanonicalReconcile {
			fullCanonicalReconcile = true
			continue
		}
		if len(r.Payload) == 0 {
			continue
		}
		var row wikiFinalizeRow
		if err := json.Unmarshal(r.Payload, &row); err != nil {
			logger.Warnf(ctx, "wiki finalize: unmarshal row id=%d failed: %v", r.ID, err)
			continue
		}
		if r.Op == wikiFinalizeOpFolderPrune {
			folderPruneIDs = append(folderPruneIDs, row.FolderIDs...)
			continue
		}
		if row.Change != nil {
			if row.Change.Action == wikiFinalizeRemoved {
				fmt.Fprintf(&changeDesc, "<document_removed>\n<title>%s</title>\n<summary>%s</summary>\n</document_removed>\n\n", row.Change.DocTitle, row.Change.DocSummary)
			} else {
				fmt.Fprintf(&changeDesc, "<document_added>\n<title>%s</title>\n<summary>%s</summary>\n</document_added>\n\n", row.Change.DocTitle, row.Change.DocSummary)
			}
			continue
		}
		if row.Slug != "" {
			if _, ok := affectedSet[row.Slug]; !ok {
				affectedSet[row.Slug] = struct{}{}
				affectedSlugs = append(affectedSlugs, row.Slug)
			}
			if row.Title != "" {
				freshRefs = append(freshRefs, linkRef{slug: row.Slug, matchText: row.Title})
			}
		}
	}

	// KB flipped away from wiki (deleted / type change) — drain the lane so the
	// rows don't accumulate, then stop.
	if !kb.IsWikiEnabled() {
		drainCtx, drainCancel := wikiIngestCleanupContext(ctx)
		err := s.trimPendingList(drainCtx, ids, nil)
		drainCancel()
		if err != nil {
			return fmt.Errorf("wiki finalize: trim disabled lane: %w", err)
		}
		return nil
	}

	synthesisModelID := ""
	if kb.WikiConfig != nil {
		synthesisModelID = kb.WikiConfig.SynthesisModelID
	}
	if synthesisModelID == "" {
		synthesisModelID = kb.SummaryModelID
	}
	if synthesisModelID == "" {
		// No model to rebuild the index with; still run the pure-text passes,
		// then drain. Missing model is a config gap, not a transient error.
		logger.Warnf(ctx, "wiki finalize: no synthesis model for KB %s, skipping index rebuild", payload.KnowledgeBaseID)
	}

	batchCtx := s.newWikiBatchContext(payload.KnowledgeBaseID, kb.WikiConfig)
	lang := types.LanguageNameFromContext(ctx)

	indexRebuilt := false
	if changeDesc.Len() > 0 && synthesisModelID != "" {
		chatModel, mErr := s.modelService.GetChatModel(ctx, synthesisModelID)
		if mErr != nil {
			logger.Warnf(ctx, "wiki finalize: get chat model failed: %v", mErr)
			if failureErr := s.settleWikiFinalizeFailure(ctx, payload, rows,
				fmt.Errorf("wiki finalize: get chat model: %w", mErr)); failureErr != nil {
				return failureErr
			}
		} else if err := s.rebuildIndexPage(ctx, chatModel, payload, changeDesc.String(), lang,
			batchCtx.ContentInstructions); err != nil {
			logger.Warnf(ctx, "wiki finalize: rebuild index failed: %v", err)
			if failureErr := s.settleWikiFinalizeFailure(ctx, payload, rows,
				fmt.Errorf("wiki finalize: rebuild index: %w", err)); failureErr != nil {
				return failureErr
			}
		} else {
			indexRebuilt = true
		}
	}

	if len(affectedSlugs) > 0 || fullCanonicalReconcile {
		if canonicalizer, ok := s.wikiService.(wikiCanonicalPageService); ok {
			reconcileSlugs := affectedSlugs
			if fullCanonicalReconcile {
				reconcileSlugs = nil
			}
			reconciled, reconcileErr := canonicalizer.ReconcileCanonicalWikiPages(ctx, payload.KnowledgeBaseID, reconcileSlugs)
			if reconcileErr != nil {
				return fmt.Errorf("wiki finalize: reconcile canonical pages: %w", reconcileErr)
			}
			if reconciled != nil {
				for _, canonicalSlug := range reconciled.Aliases {
					if _, seen := affectedSet[canonicalSlug]; !seen {
						affectedSet[canonicalSlug] = struct{}{}
						affectedSlugs = append(affectedSlugs, canonicalSlug)
					}
				}
				logger.Infof(ctx, "wiki finalize: canonical reconciliation merged=%d deferred=%d",
					reconciled.MergedPages, reconciled.DeferredPages)
			}
		}
		s.cleanDeadLinks(ctx, payload.KnowledgeBaseID, affectedSlugs, batchCtx)
		s.injectCrossLinks(ctx, payload.KnowledgeBaseID, affectedSlugs, freshRefs, batchCtx)
	}

	// A retract may leave one or more generated folders empty. Do not prune
	// while any ingest row for this KB is queued or claimed: taxonomy planning
	// creates folders before reduce writes pages, so an apparently-empty folder
	// can still be owned by an in-flight batch. The durable prune rows stay in
	// the finalize lane and are retried after the ingest lane drains.
	pruneDeferred := false
	deletedFolders := 0
	if len(folderPruneIDs) > 0 {
		pending, pErr := s.pendingRepo.PendingCount(ctx, wikiTaskType, wikiTaskScope, payload.KnowledgeBaseID)
		if pErr != nil {
			logger.Warnf(ctx, "wiki finalize: cannot verify ingest drain before folder prune: %v", pErr)
			pruneDeferred = true
		} else if pending > 0 {
			pruneDeferred = true
		} else {
			deleted, pruneErr := s.wikiService.PruneEmptyFolderChains(
				ctx, payload.KnowledgeBaseID, uniqueWikiFolderIDs(folderPruneIDs))
			if pruneErr != nil {
				logger.Warnf(ctx, "wiki finalize: prune empty folders failed: %v", pruneErr)
				pruneDeferred = true
			} else {
				deletedFolders = len(deleted)
			}
		}
	}

	// Drain the processed rows. Index failures returned above keep change rows
	// queued until they either rebuild successfully or are durably archived.
	idsToTrim := ids
	if pruneDeferred && len(pruneRowIDs) > 0 {
		deferred := make(map[int64]struct{}, len(pruneRowIDs))
		for _, id := range pruneRowIDs {
			deferred[id] = struct{}{}
		}
		idsToTrim = idsToTrim[:0:0]
		for _, id := range ids {
			if _, keep := deferred[id]; !keep {
				idsToTrim = append(idsToTrim, id)
			}
		}
	}
	drainCtx, drainCancel := wikiIngestCleanupContext(ctx)
	err = s.trimPendingList(drainCtx, idsToTrim, nil)
	drainCancel()
	if err != nil {
		return fmt.Errorf("wiki finalize: trim pending rows: %w", err)
	}

	// If more finalize rows landed while we were working, reschedule so they
	// get their own convergence pass.
	rescheduled := false
	if pruneDeferred {
		if err := s.scheduleFinalizeRetry(ctx, payload); err != nil {
			return err
		}
		rescheduled = true
	}
	n, countErr := s.pendingRepo.PendingCount(ctx, wikiFinalizeTaskType, wikiTaskScope, payload.KnowledgeBaseID)
	if countErr != nil {
		return fmt.Errorf("count remaining wiki finalize rows: %w", countErr)
	}
	if n > 0 {
		if !pruneDeferred {
			if err := s.scheduleFinalize(ctx, payload); err != nil {
				return err
			}
		}
		rescheduled = true
	}

	logger.Infof(ctx,
		"wiki finalize: kb=%s rows=%d affected_slugs=%d deleted_folders=%d folder_prune_deferred=%v index_rebuilt=%v rescheduled=%v elapsed=%s",
		payload.KnowledgeBaseID, len(rows), len(affectedSlugs), deletedFolders, pruneDeferred, indexRebuilt, rescheduled,
		time.Since(startedAt).Round(time.Millisecond),
	)
	return nil
}

func (s *wikiIngestService) mapOneDocument(
	ctx context.Context,
	chatModel chat.Chat,
	payload WikiIngestPayload,
	op WikiPendingOp,
	batchCtx *WikiBatchContext,
) (*docIngestResult, []SlugUpdate, error) {
	docStartedAt := time.Now()
	knowledgeID := op.KnowledgeID
	lang := types.ResolveLanguageName(ctx, op.Language)
	if op.Attempt > 0 {
		current, err := wikiAttemptCurrentStrict(ctx, s.tracker(), knowledgeID, op.Attempt)
		if err != nil {
			return nil, nil, err
		}
		if !current {
			logger.Infof(ctx,
				"wiki ingest: skip superseded op knowledge=%s op_attempt=%d",
				knowledgeID, op.Attempt)
			return nil, nil, nil
		}
	}

	// Open a postprocess.wiki subspan under the parent attempt's
	// postprocess stage so the actual per-doc work (LLM extraction +
	// summary + classification) shows up in the trace tree. Returns
	// nil when the parent attempt is gone (no panic on missing
	// lookups — span tracker is best-effort).
	wikiSpan := s.beginWikiSubspan(ctx, op, types.JSONMap{
		"language":          lang,
		"knowledge_base_id": payload.KnowledgeBaseID,
	})
	if op.Attempt > 0 && wikiSpan == nil {
		return nil, nil, fmt.Errorf("wiki ingest: durable owner span is unavailable for attempt %d", op.Attempt)
	}
	attempt := op.Attempt
	if wikiSpan != nil {
		attempt = wikiSpan.Attempt
	}
	ctx = langfuse.WithKnowledgeTraceContext(ctx, langfuse.KnowledgeTraceContext{
		KnowledgeID: knowledgeID,
		Attempt:     attempt,
		Stage:       "postprocess.wiki",
		TaskType:    types.TypeWikiIngest,
	})

	// Guard against the ingest/delete race: if the user deleted the doc while
	// this task was queued (wikiIngestDelay = 30s) or while an earlier stage
	// was in flight, we must NOT proceed to LLM extraction — doing so would
	// create wiki pages whose source_refs point at a ghost knowledge ID,
	// permanently unreachable via wiki_read_source_doc.
	knowledgeGone, knowledgeStateErr := s.isKnowledgeGone(ctx, payload.KnowledgeBaseID, knowledgeID)
	if knowledgeStateErr != nil {
		s.tracker().FailSpan(ctx, wikiSpan, "KNOWLEDGE_STATE_FAILED", knowledgeStateErr.Error(), knowledgeStateErr)
		return nil, nil, fmt.Errorf("check source knowledge state: %w", knowledgeStateErr)
	}
	if knowledgeGone {
		logger.Infof(ctx, "wiki ingest: knowledge %s has been deleted, skip map", knowledgeID)
		s.tracker().SkipSpan(ctx, wikiSpan, "knowledge_deleted")
		return nil, nil, nil
	}

	chunks, err := s.chunkRepo.ListChunksByKnowledgeID(ctx, payload.TenantID, knowledgeID)
	if err != nil {
		s.tracker().FailSpan(ctx, wikiSpan, "LIST_CHUNKS_FAILED", err.Error(), err)
		return nil, nil, fmt.Errorf("get chunks: %w", err)
	}
	if len(chunks) == 0 {
		logger.Infof(ctx, "wiki ingest: document %s has no chunks, skip", knowledgeID)
		s.tracker().SkipSpan(ctx, wikiSpan, "no_chunks")
		return nil, nil, nil
	}

	docTitle := knowledgeID
	if kn, err := s.knowledgeSvc.GetKnowledgeByIDOnly(ctx, knowledgeID); err == nil && kn != nil && kn.Title != "" {
		docTitle = kn.Title
	} else {
		for _, ch := range chunks {
			if ch.Content != "" {
				lines := strings.SplitN(ch.Content, "\n", 2)
				if len(lines) > 0 && len(lines[0]) > 0 && len(lines[0]) < 200 {
					docTitle = strings.TrimPrefix(strings.TrimSpace(lines[0]), "# ")
					break
				}
			}
		}
	}

	workUnit, checkpointErr := s.prepareWikiWorkUnit(
		ctx, chatModel, payload, knowledgeID, docTitle, lang, chunks, batchCtx, wikiSpan,
	)
	if checkpointErr != nil {
		if op.Attempt > 0 {
			s.tracker().FailSpan(ctx, wikiSpan, "WIKI_CHECKPOINT_FAILED", checkpointErr.Error(), checkpointErr)
			return nil, nil, fmt.Errorf("prepare wiki work checkpoint: %w", checkpointErr)
		}
		workUnit = nil
	}
	if workUnit != nil && workUnit.State == types.WikiIngestWorkUnitMapped {
		result, updates, restoreErr := restoreWikiMappedCheckpoint(workUnit, op, wikiSpan)
		if restoreErr != nil {
			return nil, nil, restoreErr
		}
		logger.Infof(ctx, "wiki ingest: reusing mapped work unit %s for knowledge %s", workUnit.WorkID, knowledgeID)
		return result, updates, nil
	}

	content := reconstructEnrichedContent(ctx, s.chunkRepo, payload.TenantID, chunks)
	rawRuneCount := len([]rune(content))
	if len([]rune(content)) > maxContentForWiki {
		content = string([]rune(content)[:maxContentForWiki])
	}
	logger.Infof(ctx, "wiki ingest: doc %s chunks=%d content_len(raw=%d,truncated=%d)", knowledgeID, len(chunks), rawRuneCount, len([]rune(content)))

	// Refuse to run LLM-based extraction when the document carries no real
	// text — e.g. a scanned PDF whose pages were converted to images but where
	// VLM OCR produced nothing usable. Without this guard the LLM would have
	// only image markup left and would happily fabricate entities/concepts.
	if !hasSufficientTextContent(content) {
		logger.Warnf(ctx,
			"wiki ingest: doc %s has insufficient text content after stripping image markup (raw_len=%d), skipping LLM extraction",
			knowledgeID, rawRuneCount,
		)
		s.tracker().SkipSpan(ctx, wikiSpan, "insufficient_text_content")
		return nil, nil, nil
	}

	// Citation source reference. We deliberately use only the knowledge ID
	// (not docTitle, which is typically the upload filename) so the filename
	// does not leak into citation strings that downstream LLM prompts may
	// surface during wiki page editing.
	sourceRef := knowledgeID
	oldPageSlugs, err := s.getExistingPageSlugsForKnowledge(ctx, payload.KnowledgeBaseID, knowledgeID)
	if err != nil {
		return nil, nil, err
	}

	// Pass 0: lightweight candidate slug extraction (skeleton only).
	// On failure we fall back to the legacy single-shot extractor so the doc
	// still gets ingested, just without chunk-level citations.
	var (
		extractedEntities []extractedItem
		extractedConcepts []extractedItem
		slugItems         map[string]extractedItem
		pass0Failed       bool
	)
	logger.Infof(ctx, "wiki ingest: pass 0 — extracting candidate slugs for %s", knowledgeID)
	extractSpan := s.tracker().BeginSubSpan(ctx, wikiSpan, "postprocess.wiki.extract", types.SpanKindSubSpan, types.JSONMap{
		"content_chars": utf8.RuneCountInString(content),
		"old_pages":     len(oldPageSlugs),
	})
	extractCtx := withWikiGenerationSpan(ctx, extractSpan)
	extractedEntities, extractedConcepts, slugItems, err = s.extractCandidateSlugs(extractCtx, chatModel, payload.KnowledgeBaseID, content, lang, oldPageSlugs, batchCtx)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: pass 0 failed for %s (%v) — falling back to legacy extractor", knowledgeID, err)
		pass0Failed = true
		extractedEntities, extractedConcepts, slugItems, err = s.extractEntitiesAndConceptsNoUpsert(extractCtx, chatModel, payload.KnowledgeBaseID, content, lang, oldPageSlugs, batchCtx)
		if err != nil {
			logger.Warnf(ctx, "wiki ingest: legacy fallback also failed for %s: %v", knowledgeID, err)
			s.tracker().FailSpan(ctx, extractSpan, "EXTRACT_FAILED", err.Error(), err)
			s.tracker().FailSpan(ctx, wikiSpan, "EXTRACT_FAILED", err.Error(), err)
			return nil, nil, err
		}
	}
	s.tracker().EndSpan(ctx, extractSpan, types.JSONMap{
		"entities":         len(extractedEntities),
		"concepts":         len(extractedConcepts),
		"pass0_fallback":   pass0Failed,
		"entities_preview": previewExtractedItems(extractedEntities, 8),
		"concepts_preview": previewExtractedItems(extractedConcepts, 8),
	})

	// Build slug listing for Summary's wiki-link input.
	var summaryExtractedPages []string
	for slug := range slugItems {
		summaryExtractedPages = append(summaryExtractedPages, slug)
	}
	// Wiki summary slug is derived from the knowledge ID rather than the
	// docTitle (which is typically the upload filename). Filename-based slugs
	// like "summary/mx5280-pdf" expose the filename in cross-link contexts
	// that downstream LLM prompts read; a UUID-based slug is uglier but
	// hallucination-safe.
	summarySlug := fmt.Sprintf("summary/%s", slugify(knowledgeID))
	var slugListing string
	for _, slug := range summaryExtractedPages {
		if item, ok := slugItems[slug]; ok {
			aliases := ""
			if len(item.Aliases) > 0 {
				aliases = fmt.Sprintf(" (Aliases: %s)", strings.Join(item.Aliases, ", "))
			}
			slugListing += fmt.Sprintf("- [[%s]] = %s%s\n", slug, item.Name, aliases)
		} else {
			slugListing += fmt.Sprintf("- [[%s]]\n", slug)
		}
	}

	// Summary and chunk classification are independent given Pass 0 output —
	// run them in parallel. Summary handles wiki-link injection; classification
	// attaches concrete chunk IDs to each candidate slug.
	var (
		summaryContent string
		summaryErr     error
		citations      map[string][]string
		newSlugs       []newSlugFromCitation
		batchCount     int
		classifyErr    error
	)

	// Both calls run in parallel goroutines under the same wikiSpan
	// parent — their subspans will visually overlap in the trace view,
	// which correctly reflects their wall-clock concurrency.
	summarySpan := s.tracker().BeginSubSpan(ctx, wikiSpan, "postprocess.wiki.summary", types.SpanKindSubSpan, types.JSONMap{
		"content_chars":   utf8.RuneCountInString(content),
		"extracted_slugs": len(summaryExtractedPages),
	})
	var classifySpan *Span
	if !pass0Failed {
		classifySpan = s.tracker().BeginSubSpan(ctx, wikiSpan, "postprocess.wiki.classify", types.SpanKindSubSpan, types.JSONMap{
			"chunks":     len(chunks),
			"candidates": len(extractedEntities) + len(extractedConcepts),
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		summaryCtx := withWikiGenerationSpan(ctx, summarySpan)
		summaryContent, summaryErr = s.generateWithTemplate(summaryCtx, chatModel, agent.WikiSummaryPrompt, map[string]string{
			"Content":            content,
			"Language":           lang,
			"PageTitle":          docTitle,
			"ExtractedSlugs":     slugListing,
			"CustomInstructions": batchCtx.ContentInstructions,
			"InstructionScope":   "wiki_content",
		})
		if summaryErr != nil {
			s.tracker().FailSpan(ctx, summarySpan, "SUMMARY_FAILED", summaryErr.Error(), summaryErr)
		} else {
			sumLine, sumBody := splitSummaryLine(summaryContent)
			s.tracker().EndSpan(ctx, summarySpan, types.JSONMap{
				"chars":        utf8.RuneCountInString(summaryContent),
				"summary_line": previewText(sumLine, 160),
				"body_preview": previewText(sumBody, 320),
			})
		}
	}()
	go func() {
		defer wg.Done()
		// Skip citation pass when Pass 0 has fallen back to the legacy path —
		// the legacy output already contains paraphrased Details, so chunk
		// citations would be redundant and we'd spend LLM calls for nothing.
		if pass0Failed {
			citations = map[string][]string{}
			return
		}
		candidatesXML := renderCandidateSlugsXML(extractedEntities, extractedConcepts)
		classifyCtx := withWikiGenerationSpan(ctx, classifySpan)
		citations, newSlugs, batchCount, classifyErr = s.classifyChunkCitations(classifyCtx, chatModel, candidatesXML, chunks, lang, batchCtx)
		if classifyErr != nil {
			s.tracker().FailSpan(ctx, classifySpan, "CITATION_CLASSIFICATION_FAILED", classifyErr.Error(), classifyErr)
			return
		}
		s.tracker().EndSpan(ctx, classifySpan, types.JSONMap{
			"cited_slugs":      len(citations),
			"new_slugs":        len(newSlugs),
			"batches":          batchCount,
			"top_cited":        topCitedSlugs(citations, 8),
			"new_slugs_sample": previewNewSlugs(newSlugs, 8),
		})
	}()
	wg.Wait()
	if classifyErr != nil {
		return nil, nil, fmt.Errorf("classify wiki chunk citations: %w", classifyErr)
	}

	// Merge citations back into the item structs (non-failing; items without
	// citations simply keep their Description+Details fallback).
	var uncited int
	preCitationEntities := append([]extractedItem(nil), extractedEntities...)
	preCitationConcepts := append([]extractedItem(nil), extractedConcepts...)
	extractedEntities, extractedConcepts, uncited = mergeCitationsIntoItems(extractedEntities, extractedConcepts, citations, newSlugs)
	if len(newSlugs) > 0 {
		// Citation-discovered items did not participate in Pass 0's DB probe.
		// Run the same exact/fuzzy guard now so they cannot bypass cross-file
		// canonicalization merely because they surfaced one stage later.
		extractedEntities, extractedConcepts = s.deduplicateExtractedBatch(
			ctx, chatModel, payload.KnowledgeBaseID, extractedEntities, extractedConcepts,
		)
	}
	summaryContent = rewriteWikiSlugAliases(summaryContent, extractedItemSlugAliases(
		preCitationEntities, preCitationConcepts, extractedEntities, extractedConcepts,
	))

	// Rebuild slugItems so stale entries (for slugs that did not survive the
	// merge) and brand-new slugs discovered by the citation pass are both
	// reflected in summaryExtractedPages tracking.
	slugItems = make(map[string]extractedItem, len(extractedEntities)+len(extractedConcepts))
	for _, item := range extractedEntities {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}
	for _, item := range extractedConcepts {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}

	// extractedPages records every wiki page this document materialized
	// (entities, concepts, plus the summary page appended below). The
	// slug is used for link/retract bookkeeping; the title is retained for
	// trace output and finalize processing.
	extractedPages := make([]wikiIngestPageRef, 0, len(slugItems)+1)
	for slug, item := range slugItems {
		title := item.Name
		if title == "" {
			title = slug
		}
		extractedPages = append(extractedPages, wikiIngestPageRef{Slug: slug, Title: title})
	}

	// Count total distinct chunks cited across all slugs for logging.
	citedChunkSet := make(map[string]bool)
	for _, ids := range citations {
		for _, id := range ids {
			citedChunkSet[id] = true
		}
	}

	var updates []SlugUpdate
	// docSummaryLine is the one-sentence headline used for terse log/audit
	// previews and for <document_added> blocks in retract prompts.
	// docSummary is the full summary body attached to each entity/concept
	// update so the editor model gets rich framing in <source_context>.
	var docSummaryLine string
	var docSummary string

	if summaryErr != nil {
		// Summary is the headline artifact of an ingested document — a
		// document with no summary page is half-ingested and leaves the
		// entity/concept updates hanging without a root to link back to
		// from the index. Historically we just logged and moved on,
		// which meant a single transient 504 permanently dropped the
		// summary page for that document.
		//
		// Returning an error here sends the op to failedOps (see the
		// map-phase loop in ProcessWikiIngest), which requeueFailedOps
		// appends back onto the pending list so the next batch retries.
		// The internal retries in generateWithTemplate already exhaust
		// the LLM's own transient-error budget before we give up here.
		logger.Errorf(ctx, "wiki ingest: generate summary failed for %s, will requeue: %v", knowledgeID, summaryErr)
		s.tracker().FailSpan(ctx, wikiSpan, "SUMMARY_FAILED", summaryErr.Error(), summaryErr)
		return nil, nil, fmt.Errorf("generate summary: %w", summaryErr)
	}
	sumLine, sumBody := splitSummaryLine(summaryContent)
	if sumBody == "" {
		sumBody = summaryContent
	}
	if sumLine == "" {
		sumLine = docTitle
	}
	docSummaryLine = sumLine
	docSummary = sumBody
	if strings.TrimSpace(docSummary) == "" {
		docSummary = sumLine
	}
	updates = append(updates, SlugUpdate{
		Slug:        summarySlug,
		Type:        types.WikiPageTypeSummary,
		DocTitle:    docTitle,
		KnowledgeID: knowledgeID,
		Attempt:     op.Attempt,
		SourceRef:   sourceRef,
		Language:    lang,
		SummaryLine: sumLine,
		SummaryBody: sumBody,
	})
	extractedPages = append(extractedPages, wikiIngestPageRef{Slug: summarySlug, Title: docTitle})

	// Entities
	for _, item := range extractedEntities {
		if item.Slug != "" {
			updates = append(updates, SlugUpdate{
				Slug:         item.Slug,
				Type:         types.WikiPageTypeEntity,
				Item:         item,
				DocTitle:     docTitle,
				KnowledgeID:  knowledgeID,
				Attempt:      op.Attempt,
				SourceRef:    sourceRef,
				Language:     lang,
				SourceChunks: item.SourceChunks,
				DocSummary:   docSummary,
			})
		}
	}

	// Concepts
	for _, item := range extractedConcepts {
		if item.Slug != "" {
			updates = append(updates, SlugUpdate{
				Slug:         item.Slug,
				Type:         types.WikiPageTypeConcept,
				Item:         item,
				DocTitle:     docTitle,
				KnowledgeID:  knowledgeID,
				Attempt:      op.Attempt,
				SourceRef:    sourceRef,
				Language:     lang,
				SourceChunks: item.SourceChunks,
				DocSummary:   docSummary,
			})
		}
	}

	// Reconcile old page set against new extraction.
	//
	// Three cases:
	//
	//  (a) oldSlug ∉ new  → "retractStale": the doc no longer mentions this
	//      page's subject, so strip its ref (and possibly delete the page
	//      if this was the only source). Passes the NEW content as the
	//      retract context — if the LLM finds matching facts it trims
	//      them, otherwise the retract is a near no-op, which is fine.
	//
	//  (b) oldSlug ∈ new AND slug is an entity/concept page  → reparse
	//      swap: emit BOTH a "retract" (carrying the doc's PRIOR summary
	//      body as the old-version signal) AND the normal addition. The
	//      reduce stage sees HasAdditions=1 + HasRetractions=1 and the
	//      WikiPageModifyUserPrompt correctly tells the editor model to
	//      remove the old K section and add the new K section in one
	//      pass — giving us replace-not-append semantics that "append
	//      new K on top of old K" would otherwise violate.
	//
	//  (c) oldSlug ∈ new AND slug is a summary page (summary/...) →
	//      nothing to do here. reduceSlugUpdates' summary branch
	//      unconditionally overwrites the whole page from the new
	//      SummaryBody, so emitting an extra retract would just be
	//      dead weight that the summary branch discards anyway.
	//
	// priorContribution is the doc's LAST summary body, fetched lazily
	// at this point (rather than pre-loaded into the batch context).
	// Empty on first-ever ingest — in that case oldPageSlugs is also
	// empty, so we never consult it.
	priorContribution := batchCtx.SummaryContentByKnowledgeID(ctx, knowledgeID)

	newSlugSet := make(map[string]bool, len(extractedPages))
	for _, ns := range extractedPages {
		newSlugSet[ns.Slug] = true
	}

	var reparseOverlap, staleCount int
	for oldSlug := range oldPageSlugs {
		if newSlugSet[oldSlug] {
			// Skip summary slugs — they're overwritten wholesale by the
			// summary update, retract would be ignored downstream.
			if strings.HasPrefix(oldSlug, "summary/") {
				continue
			}
			reparseOverlap++
			updates = append(updates, SlugUpdate{
				Slug:              oldSlug,
				Type:              "retract",
				RetractDocContent: priorContribution,
				DocTitle:          docTitle,
				KnowledgeID:       knowledgeID,
				Attempt:           op.Attempt,
				Language:          lang,
			})
			continue
		}
		staleCount++
		updates = append(updates, SlugUpdate{
			Slug:              oldSlug,
			Type:              "retractStale",
			RetractDocContent: content,
			DocTitle:          docTitle,
			KnowledgeID:       knowledgeID,
			Attempt:           op.Attempt,
			Language:          lang,
		})
	}

	logger.Infof(ctx,
		"wiki ingest: mapped knowledge %s title=%q candidates=%d chunks=%d batches=%d cited_chunks=%d uncited_slugs=%d new_slugs=%d updates=%d reparse_slugs=%d stale_slugs=%d pass0_fallback=%v elapsed=%s",
		knowledgeID, previewText(docTitle, 80),
		len(slugItems), len(chunks), batchCount, len(citedChunkSet), uncited, len(newSlugs),
		len(updates), reparseOverlap, staleCount, pass0Failed,
		time.Since(docStartedAt).Round(time.Millisecond),
	)

	// Map-phase metrics get attached to the postprocess.wiki span's
	// output, but we do NOT EndSpan here — the batch driver keeps the
	// span open through reduce + index rebuild + cross-link injection
	// + page publish, then closes it once this doc's pages have all
	// been written. That way the span's duration reflects the full
	// "wiki processing for this knowledge" time the user sees in the
	// trace viewer, not just the LLM extraction slice.
	mapStats := types.JSONMap{
		"doc_title":        previewText(docTitle, 120),
		"chunks":           len(chunks),
		"candidate_slugs":  len(slugItems),
		"cited_chunks":     len(citedChunkSet),
		"uncited_slugs":    uncited,
		"new_slugs":        len(newSlugs),
		"updates":          len(updates),
		"reparse_slugs":    reparseOverlap,
		"stale_slugs":      staleCount,
		"extracted_pages":  len(extractedPages),
		"summary_chars":    utf8.RuneCountInString(docSummary),
		"pass0_fallback":   pass0Failed,
		"classify_batches": batchCount,
		"summary_preview":  previewText(docSummaryLine, 160),
	}

	result := &docIngestResult{
		KnowledgeID: knowledgeID,
		SourceOp:    op,
		DocTitle:    docTitle,
		Summary:     docSummaryLine,
		Pages:       extractedPages,
		MapStats:    mapStats,
		WikiSpan:    wikiSpan,
	}
	if workUnit != nil {
		if err := s.persistWikiMappedCheckpoint(ctx, workUnit, result, updates); err != nil {
			return nil, nil, fmt.Errorf("persist wiki mapped checkpoint: %w", err)
		}
	}
	return result, updates, nil
}

func (s *wikiIngestService) extractEntitiesAndConceptsNoUpsert(
	ctx context.Context,
	chatModel chat.Chat,
	kbID string,
	content, lang string,
	oldPageSlugs map[string]bool,
	batchCtx *WikiBatchContext,
) ([]extractedItem, []extractedItem, map[string]extractedItem, error) {
	result, batchCount, err := runWikiExtractionBatches(content, func(batch string, accumulated combinedExtraction) (combinedExtraction, error) {
		extractionJSON, generateErr := s.generateWithTemplate(ctx, chatModel, agent.WikiKnowledgeExtractPrompt, map[string]string{
			"Content":            batch,
			"Language":           lang,
			"PreviousSlugs":      renderExtractionPreviousSlugs(oldPageSlugs, accumulated),
			"CustomInstructions": batchCtx.ExtractionInstructions,
			"InstructionScope":   "wiki_extraction",
		})
		if generateErr != nil {
			return combinedExtraction{}, generateErr
		}
		extractionJSON = cleanLLMJSON(extractionJSON)
		var batchResult combinedExtraction
		if unmarshalErr := json.Unmarshal([]byte(extractionJSON), &batchResult); unmarshalErr != nil {
			logger.Warnf(ctx, "wiki ingest: failed to parse combined extraction JSON: %v\nRaw: %s", unmarshalErr, extractionJSON)
			return combinedExtraction{}, fmt.Errorf("parse combined extraction JSON: %w", unmarshalErr)
		}
		return batchResult, nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("combined extraction failed: %w", err)
	}
	logger.Infof(ctx, "wiki ingest: legacy extraction completed in %d bounded batch(es)", batchCount)

	// Dedup pre-filter is dispatched against the wiki page repo via
	// pg_trgm (see deduplicateExtractedBatch). Until the trgm path
	// lands the dedup pre-filter degrades to "no dedup" which is the
	// safe default — the LLM merge call simply doesn't get a candidate
	// list and the items pass through unchanged.
	result.Entities, result.Concepts = s.deduplicateExtractedBatch(
		ctx, chatModel, kbID, result.Entities, result.Concepts,
	)

	slugItems := make(map[string]extractedItem)
	for _, item := range result.Entities {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}
	for _, item := range result.Concepts {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}

	return result.Entities, result.Concepts, slugItems, nil
}

// resolveSlugUpdateLanguage picks the language the editor prompt should write
// the page in.
//
// A single page aggregates updates from every document that cites it, and an
// update queued before the language field existed — or enqueued from a
// background path that never saw the HTTP language middleware — carries none.
// Scanning for the first update that resolved a language, rather than indexing
// into one bucket, keeps the page localised as long as ANY contributor knew the
// language; the deployment default covers the case where none did. Without the
// fallback the prompt renders "Write in ." and the model picks a language at
// random, which is how single pages ended up in the wrong language.
func resolveSlugUpdateLanguage(ctx context.Context, updates []SlugUpdate) string {
	for _, u := range updates {
		if u.Language != "" {
			return u.Language
		}
	}
	return types.LanguageNameFromContext(ctx)
}

// reduceSlugUpdates returns:
//   - changed:          whether the wiki page was created or updated
//   - affectedType:     "ingest" or "retract" — drives downstream bookkeeping
//   - additionFailed:   true iff the slug had entity/concept additions queued
//     AND the WikiPageModifyUserPrompt LLM call failed, so no page exists/was
//     refreshed for it. Callers use this to sanitize dead [[slug]] links
//     elsewhere (e.g. in the doc's summary page) and to drop the slug from
//     the wiki log feed so users don't see a clickable entry that 404s.
//   - err:              transport / repo error from the persisted upsert.
func (s *wikiIngestService) reduceSlugUpdates(
	ctx context.Context,
	chatModel chat.Chat,
	kbID string,
	slug string,
	updates []SlugUpdate,
	tenantID uint64,
	batchCtx *WikiBatchContext,
	kidToWikiSpan map[string]*Span,
) (changed bool, affectedType string, additionFailed bool, err error) {
	// Final safety net for the ingest/delete race: between Map (which already
	// checks isKnowledgeGone) and Reduce there is a long LLM call where the
	// source document may be deleted. Drop any addition/summary updates whose
	// knowledge no longer exists so we don't resurrect a ghost source_ref.
	// Retract updates are kept — they actively remove refs, which is what we
	// want when the doc is gone.
	updates, err = s.filterLiveUpdates(ctx, kbID, updates)
	if err != nil {
		return false, "", false, fmt.Errorf("validate live wiki updates: %w", err)
	}
	if len(updates) == 0 {
		return false, "", false, nil
	}

	// Per-slug page span attribution: a single slug can receive
	// contributions from multiple docs in the same batch (entity /
	// concept pages aggregate across sources). We attach the
	// postprocess.wiki.page[slug] subspan under whichever
	// contributing doc's wikiSpan is encountered first in the updates
	// list — span tree topology only allows one parent. Every
	// contributing knowledge id is recorded in the span's `contributors`
	// output so users can still see the full attribution. Pages whose
	// only contributors had no wikiSpan (e.g. their parse attempt
	// already closed and was archived) simply get a nil pageSpan,
	// which the tracker helpers no-op on.
	var (
		pageSpan     *Span
		contributors []string
	)
	{
		seen := make(map[string]bool, len(updates))
		for _, u := range updates {
			kid := u.KnowledgeID
			if kid == "" || seen[kid] {
				continue
			}
			seen[kid] = true
			contributors = append(contributors, kid)
			if pageSpan == nil {
				if sp, ok := kidToWikiSpan[kid]; ok && sp != nil {
					pageSpan = s.tracker().BeginSubSpan(ctx, sp, fmt.Sprintf("postprocess.wiki.page[%s]", slug), types.SpanKindSubSpan, types.JSONMap{
						"slug":         slug,
						"updates":      len(updates),
						"contributors": contributors,
					})
				}
			}
		}
	}
	var page *types.WikiPage
	// Deferred output captures `&page` so it observes the post-merge
	// state (title, page type, content snippet) at function return —
	// that's what's actually useful in the trace viewer, not the
	// stale pre-reduce shell that exists when the defer is registered.
	defer func() {
		if pageSpan == nil {
			return
		}
		if err != nil {
			s.tracker().FailSpan(ctx, pageSpan, "REDUCE_FAILED", err.Error(), err)
			return
		}
		if !changed {
			s.tracker().SkipSpan(ctx, pageSpan, "no_change")
			return
		}
		out := types.JSONMap{
			"affected_type":   affectedType,
			"addition_failed": additionFailed,
			"contributors":    contributors,
		}
		if page != nil {
			out["page_title"] = previewText(page.Title, 160)
			out["page_type"] = string(page.PageType)
			out["page_summary"] = previewText(page.Summary, 200)
			out["content_preview"] = previewText(page.Content, 320)
			out["source_refs"] = len(page.SourceRefs)
			out["chunk_refs"] = len(page.ChunkRefs)
			out["aliases"] = []string(page.Aliases)
		}
		s.tracker().EndSpan(ctx, pageSpan, out)
	}()

	page, err = s.wikiService.GetPageBySlug(ctx, kbID, slug)
	exists := err == nil && page != nil
	if err != nil && !errors.Is(err, apprepo.ErrWikiPageNotFound) {
		return false, "", false, fmt.Errorf("load wiki page %s: %w", slug, err)
	}

	if !exists {
		hasAdditions := false
		for _, u := range updates {
			if u.Type == types.WikiPageTypeEntity || u.Type == types.WikiPageTypeConcept || u.Type == "summary" {
				hasAdditions = true
				break
			}
		}
		if !hasAdditions {
			return false, "", false, nil
		}

		page = &types.WikiPage{
			ID:              uuid.New().String(),
			TenantID:        tenantID,
			KnowledgeBaseID: kbID,
			Slug:            slug,
			Status:          types.WikiPageStatusDraft,
			SourceRefs:      types.StringArray{},
			Aliases:         types.StringArray{},
		}
		// Reset err: GetPageBySlug returned "not found" which we just
		// handled by synthesizing the page. Don't leak that error to
		// the named return — subsequent assignments would mask it
		// anyway, but be explicit.
		err = nil
	}

	application, applicationMarkers, replayed, alreadyApplied, checkpointErr := s.prepareWikiSlugApplication(
		ctx, tenantID, kbID, slug, updates, page, exists,
	)
	if checkpointErr != nil {
		return false, "", false, fmt.Errorf("prepare wiki slug application %s: %w", slug, checkpointErr)
	}
	if replayed != nil {
		bindWikiSlugApplicationPlan(updates, application.PlanID)
		affectedType = wikiSlugUpdatesAffectedType(updates)
		page = replayed.Page
		if alreadyApplied {
			return true, affectedType, false, nil
		}
		page, err = s.persistReducedWikiPageApplication(ctx, kbID, updates, page, replayed.BaseExists,
			application, applicationMarkers)
		if err != nil {
			return false, affectedType, false, err
		}
		return true, affectedType, false, nil
	}

	affectedType = "ingest"

	var summaryUpdate *SlugUpdate
	var retracts []SlugUpdate
	var additions []SlugUpdate

	for i, u := range updates {
		if u.Type == "summary" {
			summaryUpdate = &updates[i]
		} else if u.Type == "retract" || u.Type == "retractStale" {
			retracts = append(retracts, u)
			affectedType = "retract"
		} else if u.Type == types.WikiPageTypeEntity || u.Type == types.WikiPageTypeConcept {
			additions = append(additions, u)
			affectedType = "ingest" // Additions override retracts type
		}
	}

	if summaryUpdate != nil {
		page.Title = summaryUpdate.DocTitle + " - Summary"
		page.Content = summaryUpdate.SummaryBody
		page.Summary = summaryUpdate.SummaryLine
		page.PageType = types.WikiPageTypeSummary
		page.SourceRefs = appendUnique(page.SourceRefs, summaryUpdate.SourceRef)
		// Summary pages don't carry chunk-level citations (they are document-
		// level synopses generated from the whole content). Clear any stale
		// chunk refs that may remain if this slug was once an entity page
		// and got converted to a summary page.
		page.ChunkRefs = types.StringArray{}
		changed = true

		page, err = s.persistReducedWikiPageApplication(ctx, kbID, updates, page, exists,
			application, applicationMarkers)
		if err != nil {
			return false, affectedType, false, err
		}
		return true, affectedType, false, nil
	}

	var remainingSourcesContent strings.Builder
	var deletedContent strings.Builder
	var relatedSlugs strings.Builder
	var newContentBuilder strings.Builder
	var sharedSourceContexts strings.Builder
	var docTitles []string

	language := resolveSlugUpdateLanguage(ctx, updates)

	if len(retracts) > 0 {
		for _, r := range retracts {
			fmt.Fprintf(&deletedContent, "<document>\n<title>%s</title>\n<content>\n%s\n</content>\n</document>\n\n", r.DocTitle, r.RetractDocContent)
		}

		retractKIDs := make(map[string]bool)
		for _, r := range retracts {
			retractKIDs[r.KnowledgeID] = true
		}

		for _, ref := range page.SourceRefs {
			pipeIdx := strings.Index(ref, "|")
			var refKnowledgeID, refTitle string
			if pipeIdx > 0 {
				refKnowledgeID = ref[:pipeIdx]
				refTitle = ref[pipeIdx+1:]
			} else {
				refKnowledgeID = ref
				refTitle = ref
			}

			if retractKIDs[refKnowledgeID] {
				continue
			}

			if content := batchCtx.SummaryContentByKnowledgeID(ctx, refKnowledgeID); content != "" {
				fmt.Fprintf(&remainingSourcesContent, "<document>\n<title>%s</title>\n<content>\n%s\n</content>\n</document>\n\n", refTitle, content)
			} else {
				fmt.Fprintf(&remainingSourcesContent, "<document>\n<title>%s</title>\n<content>\n(summary not available)\n</content>\n</document>\n\n", refTitle)
			}
		}
		if remainingSourcesContent.Len() == 0 {
			remainingSourcesContent.WriteString("(no remaining sources)")
		}

		newRefs := types.StringArray{}
		for _, ref := range page.SourceRefs {
			pipeIdx := strings.Index(ref, "|")
			refKnowledgeID := ref
			if pipeIdx > 0 {
				refKnowledgeID = ref[:pipeIdx]
			}
			if !retractKIDs[refKnowledgeID] {
				newRefs = append(newRefs, ref)
			}
		}
		page.SourceRefs = newRefs
	}

	if len(additions) > 0 {
		// Resolve SourceChunks → chunk contents in a single batched query per
		// knowledge ID, so the <new_information> block can quote the chunks
		// verbatim instead of relying on the short Details paraphrase.
		chunkContentByID := s.resolveCitedChunks(ctx, tenantID, additions)
		// A document summary is shared by every page derived from that document.
		// Render a deterministic, de-duplicated block before any page-specific
		// metadata so provider prefix caches can reuse it across parallel reduce
		// calls.
		sourceContextByRef := make(map[string]string)

		for _, add := range additions {
			cited := collectCitedChunkContent(add.SourceChunks, chunkContentByID)
			// Frame the chunks with the document-level summary body so the
			// editor model knows BOTH what the document is about AND what
			// kind of document it is (resume vs announcement vs product
			// page vs schedule). The one-sentence headline alone was too
			// terse to keep the editor grounded on longer or multi-topic
			// source documents, and calibrating tone (self-reported vs
			// third-party authoritative) benefits from the richer context.
			sourceCtx := strings.TrimSpace(add.DocSummary)
			if sourceCtx != "" {
				contextKey := add.SourceRef
				if contextKey == "" {
					contextKey = add.KnowledgeID + "\x00" + add.DocTitle
				}
				sourceContextByRef[contextKey] = fmt.Sprintf(
					"<document>\n<title>%s</title>\n<context>\n%s\n</context>\n</document>\n",
					add.DocTitle, sourceCtx,
				)
			}
			if cited != "" {
				fmt.Fprintf(&newContentBuilder,
					"<document>\n<title>%s</title>\n<content>\n**%s**: %s\n\n%s\n</content>\n</document>\n\n",
					add.DocTitle, add.Item.Name, add.Item.Description, cited)
			} else {
				// Fallback: no citations available (legacy path, citation pass
				// failed, or bad chunk IDs were filtered out) — stick with
				// the short Details summary so the page still gets real text.
				fmt.Fprintf(&newContentBuilder,
					"<document>\n<title>%s</title>\n<content>\n**%s**: %s\n\n%s\n</content>\n</document>\n\n",
					add.DocTitle, add.Item.Name, add.Item.Description, add.Item.Details)
			}
			docTitles = appendUnique(docTitles, add.DocTitle)

			for _, alias := range add.Item.Aliases {
				page.Aliases = appendUnique(page.Aliases, alias)
			}
			page.SourceRefs = appendUnique(page.SourceRefs, add.SourceRef)

			if page.Title == "" {
				page.Title = add.Item.Name
			}
			if page.PageType == "" {
				page.PageType = add.Type
			}
		}

		contextKeys := make([]string, 0, len(sourceContextByRef))
		for key := range sourceContextByRef {
			contextKeys = append(contextKeys, key)
		}
		sort.Strings(contextKeys)
		for _, key := range contextKeys {
			sharedSourceContexts.WriteString(sourceContextByRef[key])
		}
	}

	if len(additions) > 0 || len(retracts) > 0 {
		titles := batchCtx.SlugTitleMany(ctx, []string(page.OutLinks))

		// slugHandles escape high-entropy slugs behind short reference handles
		// (ref-1, ref-2, …) for the editor LLM, then translates them back to
		// real slugs after generation (see decodeContent below).
		slugHandles := newWikiSlugHandles()

		// Escape every out-link slug behind a request-local handle so the editor
		// never has to retype a real slug — this is where UUID-based summary
		// slugs (summary/<uuid>) got mangled into 404-ing links. Both the
		// <valid_wiki_links> listing AND the [[...]] refs already present in
		// <existing_page_content> use the SAME handle table, so the
		// model sees one consistent, copy-safe identifier space; we translate
		// the handles back to real slugs after generation.
		known := make(map[string]struct{}, len(page.OutLinks))
		for _, outSlug := range page.OutLinks {
			known[outSlug] = struct{}{}
			slugHandles.handle(outSlug) // pre-assign for stable ordering
		}
		for _, outSlug := range page.OutLinks {
			if title := titles[outSlug]; title != "" {
				fmt.Fprintf(&relatedSlugs, "- %s (%s)\n", slugHandles.handle(outSlug), title)
			}
		}

		// Older generated pages may still contain short chunk aliases such as
		// [c003]. They are internal ingest metadata; keep the editor context
		// clean so a subsequent update cannot copy them into rewritten prose.
		existingContent := stripWikiInlineChunkCitations(page.Content)
		existingContent = slugHandles.encodeContent(existingContent, known)
		if !exists || existingContent == "" {
			existingContent = "(New page)"
		}

		hasAdditionsStr := ""
		if len(additions) > 0 {
			hasAdditionsStr = "1"
		}
		hasRetractionsStr := ""
		if len(retracts) > 0 {
			hasRetractionsStr = "1"
		}

		// Fall back gracefully if title/type are still unset (shouldn't happen
		// for well-formed updates — both get populated from `additions` above,
		// and retract-only paths require an existing page — but stay defensive
		// so we never feed the LLM an empty identity block).
		pageTitle := page.Title
		if pageTitle == "" {
			pageTitle = slug
		}
		pageType := string(page.PageType)
		if pageType == "" {
			pageType = "wiki page"
		}
		pageAliases := strings.Join(page.Aliases, ", ")

		var updatedContent string
		pageCtx := withWikiGenerationSpan(ctx, pageSpan)
		updatedContent, err = s.generateWithTemplate(pageCtx, chatModel, agent.WikiPageModifyUserPrompt, map[string]string{
			"HasAdditions":            hasAdditionsStr,
			"HasRetractions":          hasRetractionsStr,
			"PageSlug":                slug,
			"PageTitle":               pageTitle,
			"PageType":                pageType,
			"PageAliases":             pageAliases,
			"ExistingSummary":         page.Summary,
			"ExistingContent":         existingContent,
			"SharedSourceContexts":    sharedSourceContexts.String(),
			"NewContent":              newContentBuilder.String(),
			"DeletedContent":          deletedContent.String(),
			"RemainingSourcesContent": remainingSourcesContent.String(),
			"AvailableSlugs":          relatedSlugs.String(),
			"Language":                language,
			"CustomInstructions":      batchCtx.ContentInstructions,
			"InstructionScope":        "wiki_content",
		})

		if err == nil && updatedContent != "" {
			// Translate request-local handles (ref-N) the model copied from the
			// encoded context back to their real slugs BEFORE the content is
			// parsed/stored, so out_links reflect real pages again.
			updatedContent = slugHandles.decodeContent(updatedContent)
			updatedSummary, updatedBody := splitSummaryLine(updatedContent)
			if updatedBody != "" {
				page.Content = updatedBody
			} else {
				page.Content = updatedContent
			}
			if updatedSummary != "" {
				page.Summary = updatedSummary
			}
			changed = true
		} else if err != nil {
			// Flag addition failures so the batch can sanitize stale
			// [[slug]] references in the doc's summary page and keep the
			// missing page out of finalize processing.
			// Retract-only failures don't poison anything (they leave
			// the existing page unchanged), so don't flag those.
			if len(additions) > 0 {
				additionFailed = true
			}
			// Propagate the generation error. The reduce caller records every
			// contributing knowledge ID as unapplied, keeps its pending row,
			// and sends it through the normal retry/dead-letter budget.
			return false, affectedType, additionFailed,
				fmt.Errorf("generate wiki page %s: %w", slug, err)
		}
	}

	// Apply the batch taxonomy plan, but only to pages that aren't already
	// filed — so brand-new pages get a coherent folder while previously-filed
	// or user-moved pages keep their placement (manual edits are authoritative).
	// The page's category_path cache is derived from folder_id downstream by
	// CreatePage/UpdatePage, so assigning the folder id is sufficient here.
	if page.FolderID == "" && batchCtx != nil {
		if fid := batchCtx.PlannedFolderID[slug]; fid != "" {
			page.FolderID = fid
		}
	}

	if changed {
		// Refresh chunk refs in-place on the page so they persist alongside
		// the rest of the row. Retract-only updates (no additions) preserve
		// the existing refs; addition rounds append the newly-cited chunks
		// on top of what was already there, deduplicated.
		page.ChunkRefs = mergeChunkRefs(page.ChunkRefs, additions)
		page, err = s.persistReducedWikiPageApplication(ctx, kbID, updates, page, exists,
			application, applicationMarkers)
		if err != nil {
			return false, affectedType, len(additions) > 0, err
		}
		return true, affectedType, additionFailed, nil
	}

	return false, "", additionFailed, nil
}

func wikiSlugUpdatesAffectedType(updates []SlugUpdate) string {
	affected := ""
	for _, update := range updates {
		switch update.Type {
		case "retract", "retractStale":
			if affected == "" {
				affected = "retract"
			}
		case "summary", types.WikiPageTypeEntity, types.WikiPageTypeConcept:
			return "ingest"
		}
	}
	return affected
}

func (s *wikiIngestService) persistReducedWikiPageApplication(
	ctx context.Context, kbID string, updates []SlugUpdate, page *types.WikiPage, exists bool,
	application *types.WikiSlugApplication, markers []types.WikiSlugContributionMarker,
) (*types.WikiPage, error) {
	checkpointCtx, err := s.markWikiSlugApplicationApplying(ctx, application, markers, page, exists)
	if err != nil {
		return nil, fmt.Errorf("checkpoint wiki page %s before persist: %w", page.Slug, err)
	}
	return s.persistReducedWikiPage(checkpointCtx, kbID, updates, page, exists)
}

// persistReducedWikiPage gives the already-generated page one bounded commit
// window independent of the expensive task/LLM deadline. It revalidates all
// contributing source documents before writing, so detaching cancellation
// cannot resurrect a page for a document deleted while generation was in
// flight. A nil success response is not a durable acknowledgement.
func (s *wikiIngestService) persistReducedWikiPage(
	ctx context.Context,
	kbID string,
	updates []SlugUpdate,
	page *types.WikiPage,
	exists bool,
) (*types.WikiPage, error) {
	persistCtx, cancel := wikiPagePersistContext(ctx)
	defer cancel()

	liveUpdates, err := s.filterLiveUpdates(persistCtx, kbID, updates)
	if err != nil {
		return nil, fmt.Errorf("revalidate wiki page sources before persist: %w", err)
	}
	if len(liveUpdates) != len(updates) {
		return nil, fmt.Errorf("wiki page sources changed while generation was in flight")
	}
	writer, ok := s.wikiService.(guardedWikiPageWriter)
	if !ok {
		return nil, fmt.Errorf("persist wiki page %s: guarded page writer is unavailable", page.Slug)
	}
	guards := make([]types.WikiSourceAttemptGuard, 0, len(liveUpdates))
	for _, update := range liveUpdates {
		if update.KnowledgeID == "" || update.Attempt < 0 ||
			(update.Attempt == 0 && (update.Type == "retract" || update.Type == "retractStale")) {
			continue
		}
		guards = append(guards, types.WikiSourceAttemptGuard{
			KnowledgeID: update.KnowledgeID,
			Attempt:     update.Attempt,
		})
	}

	var saved *types.WikiPage
	if exists {
		saved, err = writer.UpdatePageGuarded(persistCtx, page, guards)
	} else {
		saved, err = writer.CreatePageGuarded(persistCtx, page, guards)
	}
	if err != nil {
		return nil, fmt.Errorf("persist wiki page %s: %w", page.Slug, err)
	}
	if saved == nil {
		return nil, fmt.Errorf("persist wiki page %s: repository returned no durable result", page.Slug)
	}
	return saved, nil
}

// mergeChunkRefs unions the chunk IDs currently on the page with the ones
// cited by this batch's additions, preserving insertion order and dropping
// duplicates. Empty strings are filtered out so a malformed source_chunks
// array can't leave junk in the column.
//
// A retract round with no additions leaves the current refs untouched —
// retract-only paths don't carry chunk IDs (only knowledge IDs), and we
// can't surgically filter without that info. The next time the slug is
// re-materialized via additions the fresh chunks will overlay on top.
func mergeChunkRefs(current types.StringArray, additions []SlugUpdate) types.StringArray {
	seen := make(map[string]bool, len(current))
	out := make(types.StringArray, 0, len(current))
	for _, id := range current {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, add := range additions {
		for _, chunkID := range add.SourceChunks {
			if chunkID == "" || seen[chunkID] {
				continue
			}
			seen[chunkID] = true
			out = append(out, chunkID)
		}
	}
	return out
}
