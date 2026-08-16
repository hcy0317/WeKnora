package service

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type processingWorkerWriteCountingKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	writes atomic.Int32
}

func (r *processingWorkerWriteCountingKnowledgeRepo) UpdateKnowledge(context.Context, *types.Knowledge) error {
	r.writes.Add(1)
	return nil
}

func (r *processingWorkerWriteCountingKnowledgeRepo) UpdateKnowledgeColumn(context.Context, string, string, any) error {
	r.writes.Add(1)
	return nil
}

func TestNormalPostprocessWorkersFailLeaseAcquisitionBeforeWrites(t *testing.T) {
	repo := &processingWorkerWriteCountingKnowledgeRepo{}
	summaryPayload, err := json.Marshal(types.SummaryGenerationPayload{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "knowledge", Attempt: 3,
	})
	require.NoError(t, err)
	questionPayload := types.QuestionGenerationPayload{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "knowledge", Attempt: 3,
	}

	t.Run("summary", func(t *testing.T) {
		svc := &knowledgeService{repo: repo}
		err := svc.ProcessSummaryGeneration(context.Background(), asynq.NewTask(types.TypeSummaryGeneration, summaryPayload))
		require.ErrorContains(t, err, "processing owner")
		require.Zero(t, repo.writes.Load())
	})

	t.Run("question legacy", func(t *testing.T) {
		svc := &knowledgeService{repo: repo}
		err := svc.processQuestionGenerationForKnowledge(context.Background(),
			asynq.NewTask(types.TypeQuestionGeneration, nil), questionPayload)
		require.ErrorContains(t, err, "processing owner")
		require.Zero(t, repo.writes.Load())
	})

	t.Run("question batch", func(t *testing.T) {
		svc := &knowledgeService{repo: repo}
		payload := questionPayload
		payload.BatchIndex = 2
		payload.ChunkIDs = []string{"chunk"}
		err := svc.processQuestionGenerationForChunks(context.Background(),
			asynq.NewTask(types.TypeQuestionGeneration, nil), payload)
		require.ErrorContains(t, err, "processing owner")
		require.Zero(t, repo.writes.Load())
	})

	t.Run("graph", func(t *testing.T) {
		payload, marshalErr := json.Marshal(types.ExtractChunkPayload{
			TenantID: 7, KnowledgeID: "knowledge", Attempt: 3, ChunkID: "chunk", ChunkIndex: 4,
		})
		require.NoError(t, marshalErr)
		svc := &ChunkExtractService{knowledgeRepo: repo}
		err := svc.Handle(context.Background(), asynq.NewTask(types.TypeChunkExtract, payload))
		require.ErrorContains(t, err, "processing owner")
		require.Zero(t, repo.writes.Load())
	})
}
