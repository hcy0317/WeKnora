package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// KnowledgePostProcessService acts as an orchestrator for all post-processing tasks
// after a document has been parsed and split into chunks (including multimodal OCR/Caption).
type KnowledgePostProcessService struct {
	knowledgeRepo interfaces.KnowledgeRepository
	kbService     interfaces.KnowledgeBaseService
	chunkService  interfaces.ChunkService
	taskEnqueuer  interfaces.TaskEnqueuer
	pendingRepo   interfaces.TaskPendingOpsRepository
	redisClient   *redis.Client
	spanTracker   SpanTracker
}

const postProcessFanoutPlanVersion = 1

type postProcessQuestionBatchPlan struct {
	BatchIndex  int      `json:"batch_index"`
	ChunkIDs    []string `json:"chunk_ids"`
	PrevChunkID string   `json:"prev_chunk_id,omitempty"`
	NextChunkID string   `json:"next_chunk_id,omitempty"`
}

type postProcessGraphChunkPlan struct {
	ChunkID    string `json:"chunk_id"`
	ChunkIndex int    `json:"chunk_index"`
	ModelID    string `json:"model_id"`
}

// postProcessFanoutPlan is immutable once persisted on the postprocess stage.
// A finalizing delivery may run after KB settings or the chunk set changed;
// recovery must replay this exact plan instead of inventing or dropping work.
type postProcessFanoutPlan struct {
	Version          int                            `json:"version"`
	ExpectedBranches []string                       `json:"expected_branches"`
	ExpectedSubtasks int                            `json:"expected_subtasks"`
	Summary          bool                           `json:"summary"`
	Wiki             bool                           `json:"wiki"`
	AutoTag          bool                           `json:"auto_tag"`
	QuestionConfig   types.QuestionGenerationConfig `json:"question_config"`
	QuestionBatches  []postProcessQuestionBatchPlan `json:"question_batches"`
	GraphChunks      []postProcessGraphChunkPlan    `json:"graph_chunks"`
}

func buildPostProcessFanoutPlan(
	kb *types.KnowledgeBase,
	eff types.EffectiveProcessConfig,
	textChunks []*types.Chunk,
) postProcessFanoutPlan {
	plan := postProcessFanoutPlan{
		Version: postProcessFanoutPlanVersion,
		Summary: len(textChunks) > 0,
		Wiki:    kb.IndexingStrategy.WikiEnabled && len(textChunks) > 0,
		AutoTag: kb.Type == types.KnowledgeBaseTypeDocument && kb.AutoTagConfig != nil &&
			kb.AutoTagConfig.Enabled && len(textChunks) > 0,
		QuestionConfig: eff.QuestionGenerationConfig,
	}

	if plan.Summary && kb.NeedsEmbeddingModel() && eff.QuestionGenerationConfig.Enabled {
		questionChunks := make([]*types.Chunk, 0, len(textChunks))
		for _, chunk := range textChunks {
			if chunk.ChunkType == types.ChunkTypeText {
				questionChunks = append(questionChunks, chunk)
			}
		}
		sort.Slice(questionChunks, func(i, j int) bool {
			return questionChunks[i].StartAt < questionChunks[j].StartAt
		})
		for start, batchIndex := 0, 0; start < len(questionChunks); start, batchIndex = start+questionGenChunkBatchSize, batchIndex+1 {
			end := start + questionGenChunkBatchSize
			if end > len(questionChunks) {
				end = len(questionChunks)
			}
			batch := postProcessQuestionBatchPlan{BatchIndex: batchIndex}
			for _, chunk := range questionChunks[start:end] {
				batch.ChunkIDs = append(batch.ChunkIDs, chunk.ID)
			}
			if start > 0 {
				batch.PrevChunkID = questionChunks[start-1].ID
			}
			if end < len(questionChunks) {
				batch.NextChunkID = questionChunks[end].ID
			}
			plan.QuestionBatches = append(plan.QuestionBatches, batch)
		}
	}

	if eff.GraphEnabled {
		for index, chunk := range textChunks {
			plan.GraphChunks = append(plan.GraphChunks, postProcessGraphChunkPlan{
				ChunkID: chunk.ID, ChunkIndex: index, ModelID: kb.SummaryModelID,
			})
		}
	}
	plan.ExpectedBranches, plan.ExpectedSubtasks = expectedPostProcessFanout(plan)
	return plan
}

func expectedPostProcessFanout(plan postProcessFanoutPlan) ([]string, int) {
	branches := make([]string, 0, 2+len(plan.GraphChunks))
	expectedSubtasks := 0
	if plan.Summary {
		branches = append(branches, "postprocess.summary")
		expectedSubtasks++
	}
	if len(plan.QuestionBatches) > 0 {
		branches = append(branches, postprocessQuestionGroupSpanName)
		expectedSubtasks += len(plan.QuestionBatches)
	}
	if plan.Wiki {
		branches = append(branches, "postprocess.wiki")
		expectedSubtasks++
	}
	for _, graph := range plan.GraphChunks {
		branches = append(branches, fmt.Sprintf("postprocess.graph.chunk[%d]", graph.ChunkIndex))
		expectedSubtasks++
	}
	return branches, expectedSubtasks
}

func postProcessFanoutPlanInput(plan postProcessFanoutPlan, fanoutComplete bool) types.JSONMap {
	return types.JSONMap{
		"fanout_plan":             plan,
		"expected_branches":       plan.ExpectedBranches,
		"expected_subtasks_count": plan.ExpectedSubtasks,
		"question_batch_count":    len(plan.QuestionBatches),
		"wiki_slot_owned":         plan.Wiki,
		"fanout_complete":         fanoutComplete,
	}
}

func loadPostProcessFanoutPlan(input types.JSONMap) (postProcessFanoutPlan, bool, error) {
	if input == nil {
		return postProcessFanoutPlan{}, false, nil
	}
	raw, ok := input["fanout_plan"]
	if !ok {
		return postProcessFanoutPlan{}, false, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return postProcessFanoutPlan{}, false, fmt.Errorf("marshal persisted postprocess fanout plan: %w", err)
	}
	var plan postProcessFanoutPlan
	if err := json.Unmarshal(encoded, &plan); err != nil {
		return postProcessFanoutPlan{}, false, fmt.Errorf("decode persisted postprocess fanout plan: %w", err)
	}
	if plan.Version != postProcessFanoutPlanVersion {
		return postProcessFanoutPlan{}, false,
			fmt.Errorf("unsupported postprocess fanout plan version %d", plan.Version)
	}
	derivedBranches, derivedSubtasks := expectedPostProcessFanout(plan)
	if plan.ExpectedSubtasks != derivedSubtasks || !equalStringSlice(plan.ExpectedBranches, derivedBranches) {
		return postProcessFanoutPlan{}, false, errors.New("persisted postprocess fanout plan failed consistency validation")
	}
	for index, batch := range plan.QuestionBatches {
		if batch.BatchIndex != index || len(batch.ChunkIDs) == 0 || !plan.QuestionConfig.Enabled {
			return postProcessFanoutPlan{}, false, errors.New("persisted postprocess question batch plan is invalid")
		}
	}
	for index, graph := range plan.GraphChunks {
		if graph.ChunkIndex != index || graph.ChunkID == "" {
			return postProcessFanoutPlan{}, false, errors.New("persisted postprocess graph chunk plan is invalid")
		}
	}
	return plan, true, nil
}

func buildLegacyPostProcessFanoutPlan(
	kb *types.KnowledgeBase,
	eff types.EffectiveProcessConfig,
	textChunks []*types.Chunk,
	input types.JSONMap,
) (postProcessFanoutPlan, error) {
	branches, planned := postProcessExpectedBranches(input)
	if !planned || len(branches) == 0 {
		return postProcessFanoutPlan{}, errors.New("versionless postprocess plan has no recoverable branches")
	}
	plan := postProcessFanoutPlan{
		Version:          postProcessFanoutPlanVersion,
		ExpectedBranches: append([]string(nil), branches...),
		QuestionConfig:   eff.QuestionGenerationConfig,
	}
	questionBatchCount := positiveJSONInt(input["question_batch_count"])
	graphIndexes := make([]int, 0)
	for _, branch := range branches {
		switch {
		case branch == "postprocess.summary":
			plan.Summary = true
		case branch == postprocessQuestionGroupSpanName:
			if questionBatchCount <= 0 {
				return postProcessFanoutPlan{}, errors.New("versionless question branch is missing batch count")
			}
		case branch == "postprocess.wiki":
			plan.Wiki = true
		case strings.HasPrefix(branch, "postprocess.graph.chunk[") && strings.HasSuffix(branch, "]"):
			rawIndex := strings.TrimSuffix(strings.TrimPrefix(branch, "postprocess.graph.chunk["), "]")
			index, err := strconv.Atoi(rawIndex)
			if err != nil || index < 0 {
				return postProcessFanoutPlan{}, fmt.Errorf("invalid versionless graph branch %q", branch)
			}
			graphIndexes = append(graphIndexes, index)
		default:
			return postProcessFanoutPlan{}, fmt.Errorf("unsupported versionless postprocess branch %q", branch)
		}
	}

	if questionBatchCount > 0 {
		questionChunks := make([]*types.Chunk, 0, len(textChunks))
		for _, chunk := range textChunks {
			if chunk.ChunkType == types.ChunkTypeText {
				questionChunks = append(questionChunks, chunk)
			}
		}
		sort.Slice(questionChunks, func(i, j int) bool {
			return questionChunks[i].StartAt < questionChunks[j].StartAt
		})
		currentBatchCount := (len(questionChunks) + questionGenChunkBatchSize - 1) / questionGenChunkBatchSize
		if currentBatchCount != questionBatchCount {
			return postProcessFanoutPlan{}, fmt.Errorf(
				"versionless question plan expected %d batches but current persisted chunks provide %d",
				questionBatchCount, currentBatchCount)
		}
		plan.QuestionConfig.Enabled = true
		for start, batchIndex := 0, 0; start < len(questionChunks); start, batchIndex = start+questionGenChunkBatchSize, batchIndex+1 {
			end := start + questionGenChunkBatchSize
			if end > len(questionChunks) {
				end = len(questionChunks)
			}
			batch := postProcessQuestionBatchPlan{BatchIndex: batchIndex}
			for _, chunk := range questionChunks[start:end] {
				batch.ChunkIDs = append(batch.ChunkIDs, chunk.ID)
			}
			if start > 0 {
				batch.PrevChunkID = questionChunks[start-1].ID
			}
			if end < len(questionChunks) {
				batch.NextChunkID = questionChunks[end].ID
			}
			plan.QuestionBatches = append(plan.QuestionBatches, batch)
		}
	}

	if len(graphIndexes) > 0 {
		sort.Ints(graphIndexes)
		for position, index := range graphIndexes {
			if index != position || index >= len(textChunks) {
				return postProcessFanoutPlan{}, errors.New("versionless graph plan cannot recover chunk inputs")
			}
			plan.GraphChunks = append(plan.GraphChunks, postProcessGraphChunkPlan{
				ChunkID: textChunks[index].ID, ChunkIndex: index, ModelID: kb.SummaryModelID,
			})
		}
	}
	_, derivedSubtasks := expectedPostProcessFanout(plan)
	plan.ExpectedSubtasks = derivedSubtasks
	if persisted := positiveJSONInt(input["expected_subtasks_count"]); persisted > 0 && persisted != derivedSubtasks {
		return postProcessFanoutPlan{}, errors.New("versionless postprocess subtask count is inconsistent")
	}
	derivedBranches, _ := expectedPostProcessFanout(plan)
	if !equalStringSlice(plan.ExpectedBranches, derivedBranches) {
		return postProcessFanoutPlan{}, errors.New("versionless postprocess branch order is inconsistent")
	}
	return plan, nil
}

func equalStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func NewKnowledgePostProcessService(
	knowledgeRepo interfaces.KnowledgeRepository,
	kbService interfaces.KnowledgeBaseService,
	chunkService interfaces.ChunkService,
	taskEnqueuer interfaces.TaskEnqueuer,
	pendingRepo interfaces.TaskPendingOpsRepository,
	redisClient *redis.Client,
	spanTracker SpanTracker,
) interfaces.TaskHandler {
	return &KnowledgePostProcessService{
		knowledgeRepo: knowledgeRepo,
		kbService:     kbService,
		chunkService:  chunkService,
		taskEnqueuer:  taskEnqueuer,
		pendingRepo:   pendingRepo,
		redisClient:   redisClient,
		spanTracker:   spanTracker,
	}
}

func (s *KnowledgePostProcessService) tracker() SpanTracker {
	if s.spanTracker == nil {
		return noopSpanTracker{}
	}
	return s.spanTracker
}

// finishRunningMultimodalStage closes the multimodal stage only when image
// work really ran and is still open. The canonical stage also exists when
// multimodal processing is disabled, but that row is already "skipped" and
// must not be rewritten to "done" with the postprocess queueing delay as its
// duration.
func (s *KnowledgePostProcessService) finishRunningMultimodalStage(
	ctx context.Context,
	knowledgeID string,
	attempt int,
) {
	mm := s.tracker().LookupStage(ctx, knowledgeID, attempt, types.StageMultimodal)
	if mm == nil ||
		mm.Kind != types.SpanKindStage ||
		mm.Status != types.SpanStatusRunning {
		return
	}
	s.tracker().EndSpan(ctx, mm, nil)
}

// Handle implements asynq handler for TypeKnowledgePostProcess.
func (s *KnowledgePostProcessService) Handle(ctx context.Context, task *asynq.Task) error {
	var payload types.KnowledgePostProcessPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal knowledge post process payload: %w", err)
	}

	logger.Infof(ctx, "[KnowledgePostProcess] Orchestrating post processing for knowledge: %s", payload.KnowledgeID)

	ctx = context.WithValue(ctx, types.TenantIDContextKey, payload.TenantID)
	if payload.Language != "" {
		ctx = context.WithValue(ctx, types.LanguageContextKey, payload.Language)
	}

	// Resolve attempt before any span or knowledge write. An asynq delivery from
	// an older parse can arrive after a reparse has opened a newer attempt; it
	// must not promote the new knowledge row to finalizing or fan out children
	// whose workers will correctly refuse to drain the stale attempt.
	latestAttempt := s.tracker().LatestAttempt(ctx, payload.KnowledgeID)
	attempt := payload.Attempt
	if attempt <= 0 {
		attempt = latestAttempt
	} else if latestAttempt > 0 && attempt != latestAttempt {
		logger.Infof(ctx,
			"[KnowledgePostProcess] Ignore superseded task for knowledge %s: payload_attempt=%d latest_attempt=%d",
			payload.KnowledgeID, attempt, latestAttempt)
		return nil
	}

	// Close the multimodal stage span (parent enqueued it as "running"
	// and we never see the per-image fan-in here other than by reaching
	// post-process). If the parent skipped multimodal entirely, the
	// stage row will already be in "skipped" state and must remain so.
	// Per-image success/failure counts are NOT
	// aggregated here — the frontend already walks the children when
	// rendering the multimodal stage detail and counts them itself,
	// avoiding an extra query path.
	s.finishRunningMultimodalStage(ctx, payload.KnowledgeID, attempt)

	// Re-delivery is normal for asynq retries. Reuse an existing postprocess
	// stage so a Wiki trigger retry does not reset its original start time or
	// briefly reopen an already-settled parent.
	postSpan := s.tracker().LookupStage(ctx, payload.KnowledgeID, attempt, types.StagePostProcess)
	if postSpan == nil {
		postSpan = s.tracker().BeginStage(ctx, payload.KnowledgeID, attempt, types.StagePostProcess, nil)
	}

	// 1. Fetch Knowledge and KB
	knowledge, err := s.knowledgeRepo.GetKnowledgeByIDOnly(ctx, payload.KnowledgeID)
	if err != nil {
		return fmt.Errorf("get knowledge %s: %w", payload.KnowledgeID, err)
	}
	if knowledge == nil {
		logger.Warnf(ctx, "[KnowledgePostProcess] Knowledge %s not found, aborting.", payload.KnowledgeID)
		return nil
	}

	// Skip post-processing entirely when the knowledge has been cancelled
	// by the user or marked for deletion. We must NOT enqueue summary /
	// question / graph / wiki child tasks for an aborted knowledge. We
	// MUST also close postSpan before returning, otherwise it stays in
	// running state forever and the trace viewer shows an orange bar
	// long after the user cancelled (the AbortAttempt sweep ran before
	// we opened postSpan, so the sweep didn't catch this row).
	switch knowledge.ParseStatus {
	case types.ParseStatusCancelled, types.ParseStatusDeleting:
		logger.Infof(ctx,
			"[KnowledgePostProcess] Knowledge %s aborted (%s), skipping post-processing.",
			payload.KnowledgeID, knowledge.ParseStatus,
		)
		s.tracker().SkipSpan(ctx, postSpan,
			"knowledge "+knowledge.ParseStatus+" before postprocess started")
		return nil
	}

	if postSpan == nil {
		return errors.New("begin postprocess stage: span persistence failed")
	}

	// A completed fan-out only needs reducer reconciliation. It does not need
	// current KB settings or chunks, which may legitimately have changed since
	// the original processing attempt.
	if knowledge.ParseStatus == types.ParseStatusFinalizing {
		fanoutComplete, _ := postSpan.Input["fanout_complete"].(bool)
		if fanoutComplete {
			s.tracker().SettlePostProcessTree(ctx, payload.KnowledgeID, attempt)
			return nil
		}
	}
	switch knowledge.ParseStatus {
	case types.ParseStatusCompleted:
		s.tracker().SettlePostProcessTree(ctx, payload.KnowledgeID, attempt)
		return nil
	case types.ParseStatusFailed:
		message := "knowledge entered postprocess in failed state"
		s.tracker().FailSpan(ctx, postSpan, "KNOWLEDGE_ALREADY_FAILED", message, errors.New(message))
		return nil
	case types.ParseStatusProcessing, types.ParseStatusFinalizing:
	default:
		logger.Infof(ctx, "[KnowledgePostProcess] Knowledge %s is in %s, skipping enrichment fan-out.",
			payload.KnowledgeID, knowledge.ParseStatus)
		return fmt.Errorf("knowledge %s is not ready for postprocess: status=%s",
			payload.KnowledgeID, knowledge.ParseStatus)
	}

	plan, planExists, err := loadPostProcessFanoutPlan(postSpan.Input)
	if err != nil {
		if knowledge.ParseStatus == types.ParseStatusFinalizing {
			return s.failPostProcessFanoutPlan(ctx, postSpan, err)
		}
		return fmt.Errorf("load persisted postprocess fanout plan: %w", err)
	}
	if !planExists {
		_, legacyPlan := postProcessExpectedBranches(postSpan.Input)
		if knowledge.ParseStatus == types.ParseStatusFinalizing && !legacyPlan {
			return s.failPostProcessFanoutPlan(ctx, postSpan,
				errors.New("recover postprocess fanout: authoritative persisted plan is missing"))
		}
		kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, payload.KnowledgeBaseID)
		if err != nil || kb == nil {
			return fmt.Errorf("get knowledge base %s: %w", payload.KnowledgeBaseID, err)
		}
		chunks, err := s.chunkService.ListChunksByKnowledgeID(ctx, payload.KnowledgeID)
		if err != nil {
			return fmt.Errorf("list chunks for knowledge %s: %w", payload.KnowledgeID, err)
		}
		textChunks := make([]*types.Chunk, 0, len(chunks))
		for _, chunk := range chunks {
			if chunk.ChunkType == types.ChunkTypeText || chunk.ChunkType == types.ChunkTypeImageOCR ||
				chunk.ChunkType == types.ChunkTypeImageCaption {
				textChunks = append(textChunks, chunk)
			}
		}
		processOverrides, _ := knowledge.ProcessOverrides()
		eff := ResolveProcessConfig(kb, processOverrides)
		if legacyPlan {
			plan, err = buildLegacyPostProcessFanoutPlan(kb, eff, textChunks, postSpan.Input)
			if err != nil {
				return s.failPostProcessFanoutPlan(ctx, postSpan, err)
			}
		} else {
			plan = buildPostProcessFanoutPlan(kb, eff, textChunks)
		}
		if err := s.tracker().UpdateSpanInput(ctx, postSpan, postProcessFanoutPlanInput(plan, false)); err != nil {
			return fmt.Errorf("persist postprocess branch plan: %w", err)
		}
		postSpan.Input = postProcessFanoutPlanInput(plan, false)
	}

	willSpawnSummary := plan.Summary
	willSpawnWiki := plan.Wiki
	willSpawnAutoTag := plan.AutoTag
	questionBatchCount := len(plan.QuestionBatches)
	graphChunkCount := len(plan.GraphChunks)
	expectedSubtasks := plan.ExpectedSubtasks
	enqueuedAutoTag := false

	// enteredFinalizing is set only when the processing-to-finalizing handoff
	// actually seeded the counter. For Wiki-enabled knowledge, that handoff
	// also persists the pending Wiki op in the same transaction.
	enteredFinalizing := false
	wikiSlotOwned := false
	recoveringFanout := knowledge.ParseStatus == types.ParseStatusFinalizing

	switch {
	case knowledge.ParseStatus == types.ParseStatusFinalizing:
		// Crash recovery: the counter and optional Wiki pending op were already
		// committed. Re-run only the incomplete durable fan-out; queued logical
		// children and deterministic task IDs make every publish idempotent.
		enteredFinalizing = true
		wikiSlotOwned = plan.Wiki
		logger.Infof(ctx, "[KnowledgePostProcess] Recovering incomplete fan-out for %s.", payload.KnowledgeID)
	case expectedSubtasks == 0:
		// Empty fan-out still goes through the transactional reducer so an
		// earlier open/failed stage cannot be hidden by a direct success write.
		if err := s.tracker().UpdateSpanInput(ctx, postSpan, postProcessFanoutPlanInput(plan, true)); err != nil {
			return fmt.Errorf("persist empty postprocess plan: %w", err)
		}
		s.tracker().SettlePostProcessTree(ctx, payload.KnowledgeID, attempt)
		return nil
	default:
		// Flip processing to finalizing before fan-out so a parallel
		// cancel/delete cannot race us into completed.
		var promoted bool
		var err error
		if willSpawnWiki {
			seeder, ok := s.pendingRepo.(interfaces.TaskPendingOpsFinalizingSeeder)
			if !ok {
				return errors.New("wiki post-process requires atomic finalizing handoff")
			}
			pendingOp, buildErr := newWikiIngestPendingOp(
				ctx, payload.TenantID, payload.KnowledgeBaseID, payload.KnowledgeID, attempt,
			)
			if buildErr != nil {
				return buildErr
			}
			promoted, err = seeder.SeedKnowledgeFinalizingWithPendingOp(
				ctx, payload.KnowledgeID, expectedSubtasks, pendingOp,
			)
			wikiSlotOwned = promoted
		} else {
			promoted, err = s.knowledgeRepo.SetFinalizing(ctx, payload.KnowledgeID, expectedSubtasks)
		}
		if err != nil {
			logger.Warnf(ctx, "[KnowledgePostProcess] SetFinalizing failed for %s: %v",
				payload.KnowledgeID, err)
			return fmt.Errorf("seed knowledge finalizing: %w", err)
		}
		if promoted {
			enteredFinalizing = true
			// Reflect summary status separately so the UI shows the
			// summary as queued for users who already had it visible.
			summaryStatus := types.SummaryStatusNone
			if willSpawnSummary {
				summaryStatus = types.SummaryStatusPending
			}
			if err := s.knowledgeRepo.UpdateKnowledgeColumn(ctx,
				payload.KnowledgeID, "summary_status", summaryStatus); err != nil {
				logger.Warnf(ctx, "[KnowledgePostProcess] Failed to update summary_status for %s: %v",
					payload.KnowledgeID, err)
			}
			logger.Infof(ctx,
				"[KnowledgePostProcess] Knowledge %s entered finalizing (pending_subtasks=%d).",
				payload.KnowledgeID, expectedSubtasks)
		} else {
			// Row was no longer 'processing' (cancel / delete won the race).
			// Skip enrichment entirely so we don't waste LLM quota on a row
			// the user already abandoned.
			logger.Infof(ctx,
				"[KnowledgePostProcess] Knowledge %s no longer in processing, skipping enrichment fan-out.",
				payload.KnowledgeID)
			s.tracker().SkipSpan(ctx, postSpan, "knowledge no longer processing before fan-out")
			return nil
		}
	}

	// Queue best-effort automatic tagging only after the processing row has
	// successfully handed off to finalizing. This avoids model calls from a
	// duplicate post-process delivery that observes an already terminal row.
	if willSpawnAutoTag && !recoveringFanout {
		enqueuedAutoTag = s.enqueueAutoTagTask(ctx, payload, attempt)
	}

	// 4. Spawn Summary and Question Tasks
	enqueuedSummary := false
	enqueuedQuestionCount := 0
	if willSpawnSummary {
		queuedSummary, queueErr := s.recordPostProcessQueuedChild(ctx, postSpan, "postprocess.summary", nil)
		if queueErr != nil {
			return queueErr
		}
		enqueuedSummary = queuedSummary.Status != types.SpanStatusPending ||
			s.enqueueSummaryGenerationTask(ctx, payload, attempt)
		if !enqueuedSummary {
			_ = s.knowledgeRepo.UpdateKnowledgeColumn(
				ctx, payload.KnowledgeID, "summary_status", types.SummaryStatusFailed,
			)
			if err := s.failPostProcessQueuedChild(ctx, queuedSummary,
				"SUMMARY_ENQUEUE_FAILED", errors.New("summary generation task was not enqueued")); err != nil {
				return err
			}
		}
		if questionBatchCount > 0 {
			questionChunkCount := 0
			for _, batch := range plan.QuestionBatches {
				questionChunkCount += len(batch.ChunkIDs)
			}
			// Create the postprocess.question grouping span up front so the
			// per-batch subspans (enqueued just below, run later in their own
			// workers) have a parent to nest under. The group stays running
			// until every batch reaches a terminal state.
			questionGroup := s.tracker().LookupSpanByName(
				ctx, payload.KnowledgeID, attempt, postprocessQuestionGroupSpanName)
			if questionGroup == nil {
				questionGroup = s.tracker().BeginSubSpan(ctx, postSpan, postprocessQuestionGroupSpanName,
					types.SpanKindSubSpan, types.JSONMap{
						"batch_count": questionBatchCount,
						"chunk_count": questionChunkCount,
						"batch_size":  questionGenChunkBatchSize,
					})
			}
			var enqueueErr error
			enqueuedQuestionCount, enqueueErr = s.enqueueQuestionGenerationPlan(
				ctx, payload, plan.QuestionConfig, attempt, plan.QuestionBatches, questionGroup,
			)
			if enqueueErr != nil {
				return enqueueErr
			}
		}
	}

	// 5. Spawn Graph RAG Tasks — only when graph indexing is enabled in IndexingStrategy
	enqueuedGraphCount := 0
	if graphChunkCount > 0 {
		logger.Infof(ctx, "[KnowledgePostProcess] Spawning Graph RAG extract tasks for %d persisted chunks", graphChunkCount)
		for _, graph := range plan.GraphChunks {
			name := fmt.Sprintf("postprocess.graph.chunk[%d]", graph.ChunkIndex)
			input := types.JSONMap{"chunk_id": graph.ChunkID, "chunk_index": graph.ChunkIndex, "model_id": graph.ModelID}
			queuedGraph, queueErr := s.recordPostProcessQueuedChild(ctx, postSpan, name, input)
			if queueErr != nil {
				return queueErr
			}
			ok := queuedGraph.Status != types.SpanStatusPending
			var err error
			if !ok {
				ok, err = NewChunkExtractTask(ctx, s.taskEnqueuer, payload.TenantID, graph.ChunkID, graph.ModelID,
					payload.KnowledgeID, attempt, graph.ChunkIndex)
			}
			if err != nil {
				logger.Errorf(ctx, "[KnowledgePostProcess] Failed to create chunk extract task for %s: %v", graph.ChunkID, err)
				if recordErr := s.failPostProcessQueuedChild(ctx, queuedGraph,
					"GRAPH_ENQUEUE_FAILED", err); recordErr != nil {
					return recordErr
				}
				continue
			}
			if !ok {
				enqueueErr := errors.New("graph extraction task was not enqueued")
				if recordErr := s.failPostProcessQueuedChild(ctx, queuedGraph,
					"GRAPH_ENQUEUE_FAILED", enqueueErr); recordErr != nil {
					return recordErr
				}
				continue
			}
			enqueuedGraphCount++
		}
	}

	// 6. Schedule the Wiki trigger. The durable per-knowledge op already owns
	//    its finalizing slot because it was committed atomically with the state
	//    transition above. A trigger failure is returned so the post-process
	//    task retries only the trigger without double-accounting.
	var wikiEnqueueErr error
	if willSpawnWiki {
		queuedWiki, queueErr := s.recordPostProcessQueuedChild(ctx, postSpan, "postprocess.wiki", nil)
		if queueErr != nil {
			return queueErr
		}
		if queuedWiki.Status == types.SpanStatusPending {
			wikiEnqueueErr = s.enqueueWikiIngestTriggerForAttempt(
				ctx, payload.TenantID, payload.KnowledgeBaseID, payload.KnowledgeID, attempt,
			)
		}
		if wikiEnqueueErr != nil {
			logger.Warnf(ctx, "[KnowledgePostProcess] Failed to enqueue wiki ingest for %s: %v",
				payload.KnowledgeID, wikiEnqueueErr)
			if isFinalAsynqAttempt(ctx) {
				if err := s.failPostProcessQueuedChild(ctx, queuedWiki,
					"WIKI_ENQUEUE_FAILED", wikiEnqueueErr); err != nil {
					return err
				}
			}
		} else if wikiSlotOwned {
			logger.Infof(ctx, "[KnowledgePostProcess] Enqueued wiki ingest task for %s", payload.KnowledgeID)
		}
	}

	// Reconcile the seeded counter against what was actually enqueued.
	// summary/question/graph each own a counted slot that ONLY their own
	// task drains; a slot whose task was never enqueued (graph with NEO4J
	// off, a transient enqueue/marshal failure, a nil enqueuer) has no owner
	// and would otherwise strand the row in "finalizing". Release exactly the
	// shortfall — each release is a clamped observer decrement. Safe against fast workers:
	// shortfall slots have no draining
	// task, so total drains == seeded count regardless of ordering.
	//
	// Detached ctx: the same reasoning that motivates finalizeSubtaskDetached
	// for terminal worker drains applies here. If the postprocess handler's
	// ctx is cancelled (graceful shutdown, preempted worker) between SetFinalizing
	// and this point, the seeded slots have NO other path to drain — every
	// owning task either failed to enqueue or was never created. Riding a
	// cancelled ctx would silently abort the releases and strand the row in
	// "finalizing". The bound is per-call (matches the helper) so a wedged
	// connection can't pin the goroutine for the whole serial loop.
	if enteredFinalizing {
		plannedOwned := questionBatchCount + graphChunkCount
		if willSpawnSummary {
			plannedOwned++
		}
		if willSpawnWiki {
			plannedOwned++
		}
		actualOwned := enqueuedQuestionCount + enqueuedGraphCount
		if enqueuedSummary {
			actualOwned++
		}
		if wikiSlotOwned && (wikiEnqueueErr == nil || !isFinalAsynqAttempt(ctx)) {
			actualOwned++
		}
		if shortfall := plannedOwned - actualOwned; shortfall > 0 {
			logger.Warnf(ctx,
				"[KnowledgePostProcess] Releasing %d un-enqueued subtask slot(s) for %s (planned=%d actual=%d)",
				shortfall, payload.KnowledgeID, plannedOwned, actualOwned)
			for i := 0; i < shortfall; i++ {
				rctx, cancel := context.WithTimeout(
					context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
				_, _, err := s.knowledgeRepo.FinalizeSubtaskForAttempt(rctx, payload.KnowledgeID, attempt)
				cancel()
				if err != nil {
					logger.Warnf(ctx, "[KnowledgePostProcess] Failed to release subtask slot for %s: %v",
						payload.KnowledgeID, err)
					break
				}
			}
		}
	}
	if wikiEnqueueErr == nil || isFinalAsynqAttempt(ctx) {
		if err := s.tracker().UpdateSpanInput(context.WithoutCancel(ctx), postSpan,
			postProcessFanoutPlanInput(plan, true)); err != nil {
			return fmt.Errorf("complete postprocess branch plan: %w", err)
		}
	}
	// Every fan-out terminal transition runs the repository reducer; the
	// observer counter is never a trigger gate.
	s.tracker().SettlePostProcessTree(ctx, payload.KnowledgeID, attempt)
	logger.Infof(ctx,
		"[KnowledgePostProcess] Fan-out queued for %s: summary=%t question_batches=%d graph_chunks=%d wiki=%t auto_tag=%t",
		payload.KnowledgeID, enqueuedSummary, enqueuedQuestionCount, enqueuedGraphCount,
		wikiSlotOwned && wikiEnqueueErr == nil, enqueuedAutoTag)
	if wikiSlotOwned && wikiEnqueueErr != nil {
		return fmt.Errorf("enqueue wiki ingest trigger: %w", wikiEnqueueErr)
	}
	return nil
}

// enqueueAutoTagTask schedules best-effort classification against the KB's
// existing tags. It intentionally owns no pending-subtask slot: a model or
// configuration failure must never keep document parsing in finalizing.
func (s *KnowledgePostProcessService) enqueueAutoTagTask(
	ctx context.Context,
	payload types.KnowledgePostProcessPayload,
	attempt int,
) bool {
	if s.taskEnqueuer == nil {
		return false
	}
	taskPayload := types.KnowledgeAutoTagPayload{
		TenantID:        payload.TenantID,
		KnowledgeBaseID: payload.KnowledgeBaseID,
		KnowledgeID:     payload.KnowledgeID,
		Language:        payload.Language,
		Attempt:         attempt,
	}
	langfuse.InjectTracing(ctx, &taskPayload)
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		logger.Warnf(ctx, "[KnowledgePostProcess] Failed to marshal auto tag payload: %v", err)
		return false
	}
	task := asynq.NewTask(types.TypeKnowledgeAutoTag, payloadBytes,
		asynq.Queue(types.QueueSummary), asynq.MaxRetry(2), asynq.Timeout(2*time.Minute))
	if _, err := s.taskEnqueuer.Enqueue(task,
		asynq.TaskID(fmt.Sprintf("knowledge-fanout:%s:%d:auto-tag", payload.KnowledgeID, attempt))); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return true
		}
		logger.Warnf(ctx, "[KnowledgePostProcess] Failed to enqueue automatic tagging for %s: %v", payload.KnowledgeID, err)
		return false
	}
	logger.Infof(ctx, "[KnowledgePostProcess] Enqueued automatic tagging for %s", payload.KnowledgeID)
	return true
}

// enqueueSummaryGenerationTask enqueues the summary task. Returns true only
// when a task was actually placed on the queue, so the caller can release the
// seeded pending-subtask slot when enqueue is skipped or fails.
func (s *KnowledgePostProcessService) enqueueSummaryGenerationTask(ctx context.Context, payload types.KnowledgePostProcessPayload, attempt int) bool {
	if s.taskEnqueuer == nil {
		return false
	}

	taskPayload := types.SummaryGenerationPayload{
		TenantID:        payload.TenantID,
		KnowledgeBaseID: payload.KnowledgeBaseID,
		KnowledgeID:     payload.KnowledgeID,
		Language:        payload.Language,
		Attempt:         attempt,
	}
	langfuse.InjectTracing(ctx, &taskPayload)
	payloadBytes, err := json.Marshal(taskPayload)
	if err != nil {
		logger.Warnf(ctx, "[KnowledgePostProcess] Failed to marshal summary generation payload: %v", err)
		return false
	}

	task := asynq.NewTask(types.TypeSummaryGeneration, payloadBytes,
		asynq.Queue(types.QueueSummary), asynq.MaxRetry(3), asynq.Timeout(30*time.Minute))
	if _, err := s.taskEnqueuer.Enqueue(task,
		asynq.TaskID(fmt.Sprintf("knowledge-fanout:%s:%d:summary", payload.KnowledgeID, attempt))); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return true
		}
		logger.Warnf(ctx, "[KnowledgePostProcess] Failed to enqueue summary generation for %s: %v", payload.KnowledgeID, err)
		return false
	}
	logger.Infof(ctx, "[KnowledgePostProcess] Enqueued summary generation task for %s", payload.KnowledgeID)
	return true
}

// questionGenChunkBatchSize is the number of text chunks handled by a single
// question-generation task. Batching keeps the task count bounded for very
// large documents (a 5k-chunk doc becomes ~250 tasks instead of 5k) while
// preserving per-batch retry / cancellation granularity and letting each task
// do one embedding BatchIndex over the whole batch.
const questionGenChunkBatchSize = 20

// postprocessQuestionGroupSpanName is the grouping span the per-batch
// question subspans (postprocess.question.batch[i]) nest under, so the trace
// viewer shows one "postprocess.question" node instead of dozens of siblings
// directly beneath the postprocess stage.
const postprocessQuestionGroupSpanName = "postprocess.question"

// enqueueQuestionGenerationTasks fans out one TypeQuestionGeneration task per
// batch of questionGenChunkBatchSize text chunks. Each task carries only chunk
// ids (+ the adjacent boundary ids for context) — never the chunk content — so
// the payload stays small and the worker reads fresh content at run time,
// matching the ExtractChunkPayload precedent.
//
// Returns the number of batch tasks successfully enqueued. A failed
// marshal/enqueue is logged and skipped; the caller's reconciliation
// step (the shortfall-release loop in Handle) compares this count
// against questionBatchCount and releases any unowned slots so a
// half-fanned-out batch can't strand the row in "finalizing".
func (s *KnowledgePostProcessService) enqueueQuestionGenerationTasks(
	ctx context.Context,
	payload types.KnowledgePostProcessPayload,
	qg types.QuestionGenerationConfig,
	attempt int,
	questionChunks []*types.Chunk,
	questionGroup *Span,
) (int, error) {
	if len(questionChunks) == 0 {
		return 0, nil
	}
	batches := make([]postProcessQuestionBatchPlan, 0,
		(len(questionChunks)+questionGenChunkBatchSize-1)/questionGenChunkBatchSize)
	for start, batchIndex := 0, 0; start < len(questionChunks); start, batchIndex = start+questionGenChunkBatchSize, batchIndex+1 {
		end := start + questionGenChunkBatchSize
		if end > len(questionChunks) {
			end = len(questionChunks)
		}
		batch := postProcessQuestionBatchPlan{BatchIndex: batchIndex}
		for _, chunk := range questionChunks[start:end] {
			batch.ChunkIDs = append(batch.ChunkIDs, chunk.ID)
		}
		if start > 0 {
			batch.PrevChunkID = questionChunks[start-1].ID
		}
		if end < len(questionChunks) {
			batch.NextChunkID = questionChunks[end].ID
		}
		batches = append(batches, batch)
	}
	return s.enqueueQuestionGenerationPlan(ctx, payload, qg, attempt, batches, questionGroup)
}

func (s *KnowledgePostProcessService) enqueueQuestionGenerationPlan(
	ctx context.Context,
	payload types.KnowledgePostProcessPayload,
	qg types.QuestionGenerationConfig,
	attempt int,
	batches []postProcessQuestionBatchPlan,
	questionGroup *Span,
) (int, error) {
	if len(batches) == 0 || !qg.Enabled {
		return 0, nil
	}

	questionCount := qg.QuestionCount
	if questionCount <= 0 {
		questionCount = 3
	}
	if questionCount > 10 {
		questionCount = 10
	}

	total := 0
	enqueued := 0
	for _, batch := range batches {
		total += len(batch.ChunkIDs)
		taskPayload := types.QuestionGenerationPayload{
			TenantID:        payload.TenantID,
			KnowledgeBaseID: payload.KnowledgeBaseID,
			KnowledgeID:     payload.KnowledgeID,
			QuestionCount:   questionCount,
			Language:        payload.Language,
			Attempt:         attempt,
			ChunkIDs:        append([]string(nil), batch.ChunkIDs...),
			BatchIndex:      batch.BatchIndex,
			PrevChunkID:     batch.PrevChunkID,
			NextChunkID:     batch.NextChunkID,
		}
		queuedBatch, queueErr := s.recordPostProcessQueuedChild(ctx, questionGroup,
			fmt.Sprintf("postprocess.question.batch[%d]", batch.BatchIndex),
			types.JSONMap{
				"plan_version":   postProcessFanoutPlanVersion,
				"batch_index":    batch.BatchIndex,
				"chunk_ids":      append([]string(nil), batch.ChunkIDs...),
				"prev_chunk_id":  batch.PrevChunkID,
				"next_chunk_id":  batch.NextChunkID,
				"question_count": questionCount,
				"language":       payload.Language,
			})
		if queueErr != nil {
			return enqueued, queueErr
		}
		if queuedBatch.Status != types.SpanStatusPending {
			enqueued++
			continue
		}

		langfuse.InjectTracing(ctx, &taskPayload)
		payloadBytes, err := json.Marshal(taskPayload)
		if err != nil {
			logger.Warnf(ctx, "[KnowledgePostProcess] Failed to marshal question generation payload for batch %d: %v", batch.BatchIndex, err)
			if recordErr := s.failPostProcessQueuedChild(ctx, queuedBatch,
				"QUESTION_ENQUEUE_FAILED", err); recordErr != nil {
				return enqueued, recordErr
			}
			continue
		}

		task := asynq.NewTask(types.TypeQuestionGeneration, payloadBytes,
			asynq.Queue(types.QueueQuestion), asynq.MaxRetry(3), asynq.Timeout(30*time.Minute))
		if s.taskEnqueuer == nil {
			err = errors.New("question generation task enqueuer is not configured")
			logger.Warnf(ctx, "[KnowledgePostProcess] Failed to enqueue question generation batch %d for %s: %v", batch.BatchIndex, payload.KnowledgeID, err)
			if recordErr := s.failPostProcessQueuedChild(ctx, queuedBatch,
				"QUESTION_ENQUEUE_FAILED", err); recordErr != nil {
				return enqueued, recordErr
			}
			continue
		}
		if _, err := s.taskEnqueuer.Enqueue(task,
			asynq.TaskID(fmt.Sprintf("knowledge-fanout:%s:%d:question:%d",
				payload.KnowledgeID, attempt, batch.BatchIndex))); err != nil {
			if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
				enqueued++
				continue
			}
			logger.Warnf(ctx, "[KnowledgePostProcess] Failed to enqueue question generation batch %d for %s: %v", batch.BatchIndex, payload.KnowledgeID, err)
			if recordErr := s.failPostProcessQueuedChild(ctx, queuedBatch,
				"QUESTION_ENQUEUE_FAILED", err); recordErr != nil {
				return enqueued, recordErr
			}
			continue
		}
		enqueued++
	}
	s.tracker().SettleQuestionGroup(ctx, payload.KnowledgeID, attempt)
	logger.Infof(ctx, "[KnowledgePostProcess] Enqueued %d question generation batch tasks (%d chunks, batch_size=%d) for %s (count=%d)",
		enqueued, total, questionGenChunkBatchSize, payload.KnowledgeID, questionCount)
	return enqueued, nil
}

func (s *KnowledgePostProcessService) failPostProcessQueuedChild(
	ctx context.Context,
	span *Span,
	errorCode string,
	err error,
) error {
	if span == nil || err == nil {
		return errors.New("persist postprocess enqueue failure: queued span and error are required")
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
	defer cancel()
	markTerminalPostprocessFailure(dctx, s.tracker(), span)
	s.tracker().FailSpan(dctx, span, errorCode, err.Error(), err)
	stored := s.tracker().LookupSpanByName(dctx, span.KnowledgeID, span.Attempt, span.Name)
	if stored == nil || stored.Status != types.SpanStatusFailed {
		return fmt.Errorf("persist postprocess enqueue failure %s: terminal write not visible", span.Name)
	}
	return nil
}

func (s *KnowledgePostProcessService) failPostProcessFanoutPlan(
	ctx context.Context,
	postSpan *Span,
	err error,
) error {
	if postSpan == nil || err == nil {
		return errors.New("fail postprocess fanout plan: span and error are required")
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
	defer cancel()
	s.tracker().FailSpan(dctx, postSpan, "POSTPROCESS_PLAN_INVALID", err.Error(), err)
	s.tracker().SettlePostProcessTree(dctx, postSpan.KnowledgeID, postSpan.Attempt)
	stored := s.tracker().LookupStage(dctx, postSpan.KnowledgeID, postSpan.Attempt, types.StagePostProcess)
	if stored == nil {
		return errors.New("fail postprocess fanout plan: terminal write not visible")
	}
	if stored.Status == types.SpanStatusFailed || stored.Status == types.SpanStatusCancelled {
		return nil
	}
	return fmt.Errorf("fail postprocess fanout plan: unexpected stored status %s", stored.Status)
}

func (s *KnowledgePostProcessService) recordPostProcessQueuedChild(
	ctx context.Context,
	parent *Span,
	name string,
	input types.JSONMap,
) (*Span, error) {
	if parent == nil {
		return nil, fmt.Errorf("persist queued postprocess child %s: parent is missing", name)
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
	defer cancel()
	span := s.tracker().QueueSubSpan(dctx, parent, name, types.SpanKindSubSpan, input)
	if span == nil {
		return nil, fmt.Errorf("persist queued postprocess child %s: begin failed", name)
	}
	return span, nil
}

func (s *KnowledgePostProcessService) enqueueWikiIngestTriggerForAttempt(
	ctx context.Context,
	tenantID uint64,
	kbID string,
	knowledgeID string,
	attempt int,
) error {
	if s.taskEnqueuer == nil {
		return errors.New("enqueue wiki ingest trigger: task enqueuer is nil")
	}
	payload := WikiIngestPayload{
		TenantID: tenantID, KnowledgeBaseID: kbID,
		Language: types.LanguageFromContextOrDefault(ctx),
	}
	langfuse.InjectTracing(ctx, &payload)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal wiki ingest trigger: %w", err)
	}
	task := asynq.NewTask(types.TypeWikiIngest, payloadBytes,
		asynq.Queue(types.QueueWiki), asynq.MaxRetry(wikiIngestMaxRetry),
		asynq.Timeout(WikiIngestTaskTimeout), asynq.ProcessIn(wikiIngestDelay))
	_, err = s.taskEnqueuer.Enqueue(task,
		asynq.TaskID(fmt.Sprintf("knowledge-fanout:%s:%d:wiki", knowledgeID, attempt)))
	if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("enqueue wiki ingest trigger: %w", err)
	}
	return nil
}
