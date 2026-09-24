package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

func multimodalPendingKey(knowledgeID string) string {
	return fmt.Sprintf("multimodal:pending:%s", knowledgeID)
}

const (
	multimodalSlotReleaseAttempts = 3
	multimodalSlotReleaseBackoff  = 100 * time.Millisecond
)

func (s *knowledgeService) releaseUnownedMultimodalSlots(
	ctx context.Context,
	knowledge *types.Knowledge,
	redisKey string,
	counterSeeded bool,
	planned, enqueued int,
) {
	if enqueued == 0 {
		if counterSeeded {
			dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
			s.redisClient.Del(dctx, redisKey)
			cancel()
		}
		s.enqueueKnowledgePostProcessTask(ctx, knowledge)
		return
	}
	shortfall := planned - enqueued
	if shortfall <= 0 || !counterSeeded {
		return
	}
	logger.Warnf(ctx, "Releasing %d un-enqueued image slot(s) for %s (planned=%d enqueued=%d)",
		shortfall, knowledge.ID, planned, enqueued)
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeSubtaskDetachedTimeout)
	defer cancel()
	pending, err := s.decrMultimodalSlots(rctx, redisKey, int64(shortfall))
	if err != nil {
		logger.Errorf(ctx, "Failed to release %d multimodal slot(s) for %s after %d attempts: %v",
			shortfall, knowledge.ID, multimodalSlotReleaseAttempts, err)
		return
	}
	if pending <= 0 {
		s.redisClient.Del(rctx, redisKey)
		s.enqueueKnowledgePostProcessTask(ctx, knowledge)
	}
}

func (s *knowledgeService) decrMultimodalSlots(ctx context.Context, redisKey string, by int64) (int64, error) {
	var lastErr error
	for attempt := 1; attempt <= multimodalSlotReleaseAttempts; attempt++ {
		pending, err := s.redisClient.DecrBy(ctx, redisKey, by).Result()
		if err == nil {
			return pending, nil
		}
		lastErr = err
		if attempt == multimodalSlotReleaseAttempts {
			break
		}
		logger.Warnf(ctx, "multimodal slot release attempt %d/%d failed for %s: %v",
			attempt, multimodalSlotReleaseAttempts, redisKey, err)
		select {
		case <-time.After(time.Duration(attempt) * multimodalSlotReleaseBackoff):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, lastErr
}

func (s *knowledgeService) enqueueKnowledgePostProcessTask(ctx context.Context, knowledge *types.Knowledge) {
	if s.task == nil || knowledge == nil {
		return
	}
	payload := types.KnowledgePostProcessPayload{
		TenantID:        knowledge.TenantID,
		KnowledgeID:     knowledge.ID,
		KnowledgeBaseID: knowledge.KnowledgeBaseID,
		Language:        types.LanguageFromContextOrDefault(ctx),
		Attempt:         attemptFromCtx(ctx),
	}
	langfuse.InjectTracing(ctx, &payload)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		logger.Errorf(ctx, "Failed to marshal knowledge post process payload: %v", err)
		return
	}
	task := asynq.NewTask(types.TypeKnowledgePostProcess, payloadBytes, knowledgePostProcessTaskOptions()...)
	if _, err := s.task.Enqueue(task); err != nil {
		logger.Errorf(ctx, "Failed to enqueue knowledge post process task: %v", err)
		return
	}
	logger.Infof(ctx, "Enqueued knowledge post process task for %s", knowledge.ID)
}
