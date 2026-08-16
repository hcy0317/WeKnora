package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

const questionGenerationIdentityVersion = 1
const questionGenerationPublicationTimeout = 5 * time.Minute

type questionPublicationExternalError struct{ err error }

func (e *questionPublicationExternalError) Error() string { return e.err.Error() }
func (e *questionPublicationExternalError) Unwrap() error { return e.err }

func questionPublicationExternalFailure(format string, err error) error {
	return &questionPublicationExternalError{err: fmt.Errorf(format, err)}
}

func questionGenerationTaskID(ctx context.Context, fallback string) string {
	if taskID, ok := asynq.GetTaskID(ctx); ok && taskID != "" {
		return taskID
	}
	return fallback
}

func buildStableGeneratedQuestions(
	knowledgeID, chunkID string,
	contentRevision, batchIndex int,
	questions []string,
) []types.GeneratedQuestion {
	generated := make([]types.GeneratedQuestion, len(questions))
	for ordinal, question := range questions {
		revision := contentRevision
		generated[ordinal] = types.GeneratedQuestion{
			ID: types.StableGeneratedQuestionID(
				knowledgeID, chunkID, contentRevision, batchIndex, ordinal,
			),
			Question:        question,
			ContentRevision: &revision,
		}
	}
	return generated
}

func stablePublishedQuestions(
	metadata *types.DocumentChunkMetadata,
	knowledgeID, chunkID string,
	contentRevision, batchIndex int,
) bool {
	if metadata == nil || metadata.GeneratedQuestionsRevision != contentRevision ||
		len(metadata.GeneratedQuestions) == 0 {
		return false
	}
	for ordinal, question := range metadata.GeneratedQuestions {
		if question.ID != types.StableGeneratedQuestionID(
			knowledgeID, chunkID, contentRevision, batchIndex, ordinal,
		) {
			return false
		}
		if question.ContentRevision != nil && *question.ContentRevision != contentRevision {
			return false
		}
	}
	return true
}

func generatedQuestionSourceIDs(chunkID string, questions []types.GeneratedQuestion) []string {
	sourceIDs := make([]string, 0, len(questions))
	seen := make(map[string]struct{}, len(questions))
	for _, question := range questions {
		sourceID := types.GeneratedQuestionSourceID(chunkID, question.ID)
		if _, ok := seen[sourceID]; ok {
			continue
		}
		seen[sourceID] = struct{}{}
		sourceIDs = append(sourceIDs, sourceID)
	}
	return sourceIDs
}

func questionPublicationSourceSets(
	chunkID string,
	previous, desired []types.GeneratedQuestion,
) (desiredSourceIDs, abandonedSourceIDs []string) {
	desiredSourceIDs = generatedQuestionSourceIDs(chunkID, desired)
	desiredSet := make(map[string]struct{}, len(desiredSourceIDs))
	for _, sourceID := range desiredSourceIDs {
		desiredSet[sourceID] = struct{}{}
	}
	for _, sourceID := range generatedQuestionSourceIDs(chunkID, previous) {
		if _, keep := desiredSet[sourceID]; !keep {
			abandonedSourceIDs = append(abandonedSourceIDs, sourceID)
		}
	}
	return desiredSourceIDs, abandonedSourceIDs
}

func questionManifestJSON(value any) (types.JSON, error) {
	data, err := json.Marshal(value)
	return types.JSON(data), err
}

func decodeQuestionManifestJSON[T any](data types.JSON) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type questionPublicationEngine interface {
	BatchIndex(context.Context, embedding.Embedder, []*types.IndexInfo) error
	DeleteBySourceIDList(context.Context, []string, int, string) error
}

type questionPublicationCommit func(context.Context, func(context.Context) error) error

type questionPublicationSnapshot struct {
	TenantID           uint64
	KnowledgeID        string
	KnowledgeBaseID    string
	BatchIndex         int
	Attempt            int
	TaskID             string
	VectorStoreID      string
	EmbeddingModelID   string
	EmbeddingDimension int
	KnowledgeType      string
	EffectiveEngines   []types.RetrieverEngineParams
}

type questionManifestRuntimeSnapshot struct {
	EmbeddingModelID string
	VectorStoreID    *string
	EffectiveEngines []types.RetrieverEngineParams
}

func runtimeSnapshotFromQuestionManifest(
	manifest *types.QuestionGenerationManifest,
) (questionManifestRuntimeSnapshot, error) {
	if manifest == nil {
		return questionManifestRuntimeSnapshot{}, errors.New("question manifest is required")
	}
	engines, err := decodeQuestionManifestJSON[[]types.RetrieverEngineParams](manifest.EffectiveEngines)
	if err != nil {
		return questionManifestRuntimeSnapshot{}, fmt.Errorf("decode effective engines snapshot: %w", err)
	}
	var vectorStoreID *string
	if manifest.VectorStoreID != "" {
		value := manifest.VectorStoreID
		vectorStoreID = &value
	}
	return questionManifestRuntimeSnapshot{
		EmbeddingModelID: manifest.EmbeddingModelID,
		VectorStoreID:    vectorStoreID, EffectiveEngines: engines,
	}, nil
}

func (s *knowledgeService) resolveQuestionManifestRuntime(
	ctx context.Context, manifest *types.QuestionGenerationManifest,
) (embedding.Embedder, questionPublicationEngine, error) {
	engine, err := s.resolveQuestionManifestEngine(ctx, manifest)
	if err != nil {
		return nil, nil, err
	}
	embedder, err := s.resolveQuestionManifestEmbedder(ctx, manifest)
	return embedder, engine, err
}

func (s *knowledgeService) resolveQuestionManifestEmbedder(
	ctx context.Context, manifest *types.QuestionGenerationManifest,
) (embedding.Embedder, error) {
	snapshot, err := runtimeSnapshotFromQuestionManifest(manifest)
	if err != nil {
		return nil, err
	}
	embedder, err := s.modelService.GetEmbeddingModel(ctx, snapshot.EmbeddingModelID)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest embedding model %s: %w", snapshot.EmbeddingModelID, err)
	}
	if embedder.GetDimensions() != manifest.EmbeddingDimension {
		return nil, fmt.Errorf(
			"question manifest embedding dimension drift: snapshot=%d current=%d",
			manifest.EmbeddingDimension, embedder.GetDimensions(),
		)
	}
	return embedder, nil
}

func (s *knowledgeService) resolveQuestionManifestEngine(
	ctx context.Context, manifest *types.QuestionGenerationManifest,
) (questionPublicationEngine, error) {
	snapshot, err := runtimeSnapshotFromQuestionManifest(manifest)
	if err != nil {
		return nil, err
	}
	engine, err := retriever.CreateRetrieveEngineFromPayload(
		ctx, s.retrieveEngine, s.ownership, manifest.TenantID,
		snapshot.EffectiveEngines, snapshot.VectorStoreID,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve question manifest retrieve engine: %w", err)
	}
	return engine, nil
}

func buildQuestionIndexEntries(
	knowledge *types.Knowledge, chunk *types.Chunk, questions []types.GeneratedQuestion,
) []*types.IndexInfo {
	entries := make([]*types.IndexInfo, 0, len(questions))
	for _, question := range questions {
		entries = append(entries, &types.IndexInfo{
			Content:         buildKnowledgeIndexContent(knowledge, question.Question),
			SourceID:        types.GeneratedQuestionSourceID(chunk.ID, question.ID),
			SourceType:      types.ChunkSourceType,
			ChunkID:         chunk.ID,
			KnowledgeID:     knowledge.ID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			IsEnabled:       true,
		})
	}
	return entries
}

func newQuestionGenerationManifest(
	snapshot questionPublicationSnapshot,
	knowledge *types.Knowledge,
	chunk *types.Chunk,
	questions []types.GeneratedQuestion,
	state types.QuestionGenerationManifestState,
) (*types.QuestionGenerationManifest, error) {
	metadata, err := chunk.DocumentMetadata()
	if err != nil {
		return nil, err
	}
	var previous []types.GeneratedQuestion
	if metadata != nil {
		previous = metadata.GeneratedQuestions
	}
	desired, abandoned := questionPublicationSourceSets(chunk.ID, previous, questions)
	entries := buildQuestionIndexEntries(knowledge, chunk, questions)
	questionsJSON, err := questionManifestJSON(questions)
	if err != nil {
		return nil, err
	}
	entriesJSON, err := questionManifestJSON(entries)
	if err != nil {
		return nil, err
	}
	desiredJSON, err := questionManifestJSON(desired)
	if err != nil {
		return nil, err
	}
	abandonedJSON, err := questionManifestJSON(abandoned)
	if err != nil {
		return nil, err
	}
	enginesJSON, err := questionManifestJSON(snapshot.EffectiveEngines)
	if err != nil {
		return nil, err
	}
	return &types.QuestionGenerationManifest{
		ID: uuid.NewString(), TenantID: snapshot.TenantID,
		KnowledgeID: snapshot.KnowledgeID, KnowledgeBaseID: snapshot.KnowledgeBaseID,
		ChunkID: chunk.ID, ContentRevision: chunk.ContentRevision,
		BatchIndex: snapshot.BatchIndex, Attempt: snapshot.Attempt,
		IdentityVersion: questionGenerationIdentityVersion,
		GenerationKey: types.QuestionGenerationKey(
			snapshot.TenantID, snapshot.KnowledgeID, chunk.ID, chunk.ContentRevision, snapshot.BatchIndex,
		),
		TaskID: snapshot.TaskID, VectorStoreID: snapshot.VectorStoreID,
		EmbeddingModelID:   snapshot.EmbeddingModelID,
		EmbeddingDimension: snapshot.EmbeddingDimension,
		KnowledgeType:      snapshot.KnowledgeType, EffectiveEngines: enginesJSON,
		State: state, Questions: questionsJSON, IndexEntries: entriesJSON,
		DesiredSourceIDs: desiredJSON, AbandonedSourceIDs: abandonedJSON,
	}, nil
}

func publishQuestionGenerationManifest(
	ctx context.Context,
	commit questionPublicationCommit,
	manifestRepo interfaces.QuestionGenerationManifestRepository,
	chunkRepo interfaces.ChunkRepository,
	engine questionPublicationEngine,
	embedder embedding.Embedder,
	manifest *types.QuestionGenerationManifest,
) ([]types.GeneratedQuestion, error) {
	if commit == nil || manifestRepo == nil || chunkRepo == nil || engine == nil || manifest == nil {
		return nil, errors.New("question publication saga: complete dependencies are required")
	}
	if embedder == nil && (manifest.State == types.QuestionGenerationManifestPrepared ||
		manifest.State == types.QuestionGenerationManifestIndexing) {
		return nil, errors.New("question publication saga: embedding model is required for indexing")
	}
	questions, err := decodeQuestionManifestJSON[[]types.GeneratedQuestion](manifest.Questions)
	if err != nil {
		return nil, fmt.Errorf("decode canonical questions: %w", err)
	}
	desired, err := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
	if err != nil {
		return nil, fmt.Errorf("decode desired source ids: %w", err)
	}
	abandoned, err := decodeQuestionManifestJSON[[]string](manifest.AbandonedSourceIDs)
	if err != nil {
		return nil, fmt.Errorf("decode abandoned source ids: %w", err)
	}
	entries, err := decodeQuestionManifestJSON[[]*types.IndexInfo](manifest.IndexEntries)
	if err != nil {
		return nil, fmt.Errorf("decode question index entries: %w", err)
	}
	cleanupDesiredAndManifest := func() error {
		if len(desired) > 0 {
			if cleanupErr := commit(ctx, func(guardedCtx context.Context) error {
				return engine.DeleteBySourceIDList(
					guardedCtx, desired, manifest.EmbeddingDimension, manifest.KnowledgeType,
				)
			}); cleanupErr != nil {
				return questionPublicationExternalFailure("cleanup desired question vectors: %w", cleanupErr)
			}
		}
		if cleanupErr := commit(ctx, func(guardedCtx context.Context) error {
			return manifestRepo.DeleteQuestionGenerationManifest(guardedCtx, manifest.Key())
		}); cleanupErr != nil {
			return fmt.Errorf("delete aborted question manifest: %w", cleanupErr)
		}
		return nil
	}
	abortCleanup := func() error {
		if cleanupErr := cleanupDesiredAndManifest(); cleanupErr != nil {
			return cleanupErr
		}
		return repository.ErrChunkRevisionConflict
	}
	transitionToAbortCleanup := func() error {
		if manifest.State == types.QuestionGenerationManifestAbortCleanup {
			return nil
		}
		return commit(ctx, func(guardedCtx context.Context) error {
			changed, transitionErr := manifestRepo.TransitionQuestionGenerationManifest(
				guardedCtx, manifest.Key(), types.QuestionGenerationManifestIndexed,
				types.QuestionGenerationManifestAbortCleanup,
			)
			if transitionErr != nil {
				return transitionErr
			}
			if !changed {
				return errors.New("question publication saga: indexed manifest changed concurrently")
			}
			return nil
		})
	}

	if manifest.State == types.QuestionGenerationManifestAbortCleanup {
		return nil, abortCleanup()
	}

	if manifest.State == types.QuestionGenerationManifestPrepared {
		if err := commit(ctx, func(guardedCtx context.Context) error {
			changed, transitionErr := manifestRepo.TransitionQuestionGenerationManifest(
				guardedCtx, manifest.Key(), types.QuestionGenerationManifestPrepared,
				types.QuestionGenerationManifestIndexing,
			)
			if transitionErr != nil {
				return transitionErr
			}
			if !changed {
				return errors.New("question publication saga: prepared manifest changed concurrently")
			}
			return nil
		}); err != nil {
			return nil, err
		}
		manifest.State = types.QuestionGenerationManifestIndexing
	}

	if manifest.State == types.QuestionGenerationManifestIndexing {
		latest, latestErr := chunkRepo.GetChunkByID(ctx, manifest.TenantID, manifest.ChunkID)
		if latestErr != nil {
			return nil, latestErr
		}
		if latest.ContentRevision != manifest.ContentRevision {
			if abortErr := commit(ctx, func(guardedCtx context.Context) error {
				changed, transitionErr := manifestRepo.TransitionQuestionGenerationManifest(
					guardedCtx, manifest.Key(), types.QuestionGenerationManifestIndexing,
					types.QuestionGenerationManifestAbortCleanup,
				)
				if transitionErr != nil {
					return transitionErr
				}
				if !changed {
					return errors.New("question publication saga: indexing manifest changed concurrently")
				}
				return nil
			}); abortErr != nil {
				return nil, abortErr
			}
			manifest.State = types.QuestionGenerationManifestAbortCleanup
			return nil, abortCleanup()
		}
		if err := commit(ctx, func(guardedCtx context.Context) error {
			if len(desired) > 0 {
				if deleteErr := engine.DeleteBySourceIDList(
					guardedCtx, desired, manifest.EmbeddingDimension, manifest.KnowledgeType,
				); deleteErr != nil {
					return questionPublicationExternalFailure("pre-index desired cleanup: %w", deleteErr)
				}
			}
			if indexErr := engine.BatchIndex(guardedCtx, embedder, entries); indexErr != nil {
				return questionPublicationExternalFailure("index desired questions: %w", indexErr)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if err := commit(ctx, func(guardedCtx context.Context) error {
			changed, transitionErr := manifestRepo.TransitionQuestionGenerationManifest(
				guardedCtx, manifest.Key(), types.QuestionGenerationManifestIndexing,
				types.QuestionGenerationManifestIndexed,
			)
			if transitionErr != nil {
				return transitionErr
			}
			if !changed {
				return errors.New("question publication saga: indexing manifest changed concurrently")
			}
			return nil
		}); err != nil {
			return nil, err
		}
		manifest.State = types.QuestionGenerationManifestIndexed
	}

	if manifest.State == types.QuestionGenerationManifestIndexed {
		chunk, err := chunkRepo.GetChunkByID(ctx, manifest.TenantID, manifest.ChunkID)
		if err != nil {
			return nil, err
		}
		if chunk.ContentRevision != manifest.ContentRevision {
			if abortErr := transitionToAbortCleanup(); abortErr != nil {
				return nil, abortErr
			}
			manifest.State = types.QuestionGenerationManifestAbortCleanup
			return nil, abortCleanup()
		}

		nextChunk := *chunk
		if err := nextChunk.SetDocumentMetadata(&types.DocumentChunkMetadata{
			GeneratedQuestions: questions, GeneratedQuestionsRevision: manifest.ContentRevision,
		}); err != nil {
			return nil, err
		}
		if !bytes.Equal(chunk.Metadata, nextChunk.Metadata) {
			var swapped bool
			if err := commit(ctx, func(guardedCtx context.Context) error {
				var swapErr error
				swapped, swapErr = chunkRepo.CompareAndSwapChunkMetadata(
					guardedCtx, manifest.TenantID, manifest.ChunkID, manifest.ContentRevision,
					chunk.Metadata, nextChunk.Metadata,
				)
				if swapErr != nil {
					return swapErr
				}
				return nil
			}); err != nil {
				return nil, err
			}
			if !swapped {
				latest, latestErr := chunkRepo.GetChunkByID(ctx, manifest.TenantID, manifest.ChunkID)
				if latestErr != nil {
					return nil, latestErr
				}
				if latest.ContentRevision != manifest.ContentRevision ||
					!bytes.Equal(latest.Metadata, nextChunk.Metadata) {
					if abortErr := transitionToAbortCleanup(); abortErr != nil {
						return nil, abortErr
					}
					manifest.State = types.QuestionGenerationManifestAbortCleanup
					return nil, abortCleanup()
				}
			}
		}
		if err := commit(ctx, func(guardedCtx context.Context) error {
			changed, transitionErr := manifestRepo.TransitionQuestionGenerationManifest(
				guardedCtx, manifest.Key(), types.QuestionGenerationManifestIndexed,
				types.QuestionGenerationManifestPublished,
			)
			if transitionErr != nil {
				return transitionErr
			}
			if !changed {
				return errors.New("question publication saga: indexed manifest changed concurrently")
			}
			return nil
		}); err != nil {
			return nil, err
		}
		manifest.State = types.QuestionGenerationManifestPublished
	}

	if manifest.State != types.QuestionGenerationManifestPublished {
		return nil, fmt.Errorf("question publication saga: unsupported state %q", manifest.State)
	}
	if len(abandoned) > 0 {
		if err := commit(ctx, func(guardedCtx context.Context) error {
			return engine.DeleteBySourceIDList(
				guardedCtx, abandoned, manifest.EmbeddingDimension, manifest.KnowledgeType,
			)
		}); err != nil {
			return nil, questionPublicationExternalFailure("cleanup abandoned question vectors: %w", err)
		}
	}
	if err := commit(ctx, func(guardedCtx context.Context) error {
		return manifestRepo.DeleteQuestionGenerationManifest(guardedCtx, manifest.Key())
	}); err != nil {
		return nil, err
	}
	return questions, nil
}

func getQuestionManifest(
	ctx context.Context,
	repo interfaces.QuestionGenerationManifestRepository,
	key types.QuestionGenerationManifestKey,
) (*types.QuestionGenerationManifest, error) {
	manifest, err := repo.GetQuestionGenerationManifest(ctx, key)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return manifest, err
}

func ensureQuestionGenerationManifest(
	ctx context.Context,
	commit questionPublicationCommit,
	repo interfaces.QuestionGenerationManifestRepository,
	snapshot questionPublicationSnapshot,
	knowledge *types.Knowledge,
	chunk *types.Chunk,
	generated []types.GeneratedQuestion,
	state types.QuestionGenerationManifestState,
) (*types.QuestionGenerationManifest, bool, error) {
	key := types.QuestionGenerationManifestKey{
		TenantID: snapshot.TenantID, KnowledgeID: snapshot.KnowledgeID,
		ChunkID: chunk.ID, ContentRevision: chunk.ContentRevision, BatchIndex: snapshot.BatchIndex,
	}
	existing, err := getQuestionManifest(ctx, repo, key)
	if err != nil || existing != nil {
		return existing, false, err
	}
	candidate, err := newQuestionGenerationManifest(snapshot, knowledge, chunk, generated, state)
	if err != nil {
		return nil, false, err
	}
	var canonical *types.QuestionGenerationManifest
	var created bool
	err = commit(ctx, func(guardedCtx context.Context) error {
		var createErr error
		canonical, created, createErr = repo.GetOrCreateQuestionGenerationManifest(guardedCtx, candidate)
		return createErr
	})
	return canonical, created, err
}

func (s *knowledgeService) publishQuestionGenerationManifestWithGuard(
	ctx context.Context,
	outerCommit questionPublicationCommit,
	observed *types.QuestionGenerationManifest,
) ([]types.GeneratedQuestion, error) {
	if outerCommit == nil || observed == nil {
		return nil, errors.New("guarded question publication: complete dependencies are required")
	}
	publicationCtx, cancel := context.WithTimeout(ctx, questionGenerationPublicationTimeout)
	defer cancel()
	ctx = publicationCtx
	engine, err := s.resolveQuestionManifestEngine(ctx, observed)
	if err != nil {
		return nil, err
	}
	var embedder embedding.Embedder
	var embedderErr error
	if observed.State == types.QuestionGenerationManifestPrepared ||
		observed.State == types.QuestionGenerationManifestIndexing {
		embedder, embedderErr = s.resolveQuestionManifestEmbedder(ctx, observed)
	}
	key := observed.Key()
	var published []types.GeneratedQuestion
	var deferredExternalErr error
	var deferredSemanticErr error
	var deferredRuntimeErr error
	err = outerCommit(ctx, func(fencedCtx context.Context) error {
		return s.questionManifestRepo.WithQuestionGenerationGuard(
			fencedCtx, key, func(guardedCtx context.Context) error {
				directCommit := questionPublicationCommit(func(
					commitCtx context.Context, fn func(context.Context) error,
				) error {
					return fn(commitCtx)
				})
				manifest, loadErr := getQuestionManifest(guardedCtx, s.questionManifestRepo, key)
				if loadErr != nil {
					return loadErr
				}
				if manifest == nil {
					canonical, decodeErr := decodeQuestionManifestJSON[[]types.GeneratedQuestion](observed.Questions)
					if decodeErr != nil {
						return fmt.Errorf("decode completed question manifest: %w", decodeErr)
					}
					latest, latestErr := s.chunkRepo.GetChunkByID(guardedCtx, key.TenantID, key.ChunkID)
					if latestErr != nil {
						return latestErr
					}
					target := *latest
					if latest.ContentRevision == key.ContentRevision {
						if metadataErr := target.SetDocumentMetadata(&types.DocumentChunkMetadata{
							GeneratedQuestions: canonical, GeneratedQuestionsRevision: key.ContentRevision,
						}); metadataErr != nil {
							return metadataErr
						}
						if bytes.Equal(latest.Metadata, target.Metadata) {
							published = canonical
							return nil
						}
					}
					return errors.New("guarded question publication: manifest disappeared before canonical metadata was published")
				}
				if !sameQuestionManifestRuntimeSnapshot(observed, manifest) {
					return errors.New("guarded question publication: manifest runtime snapshot changed concurrently")
				}
				if embedderErr != nil && (manifest.State == types.QuestionGenerationManifestPrepared ||
					manifest.State == types.QuestionGenerationManifestIndexing) {
					if manifest.State == types.QuestionGenerationManifestPrepared {
						changed, transitionErr := s.questionManifestRepo.TransitionQuestionGenerationManifest(
							guardedCtx, manifest.Key(), types.QuestionGenerationManifestPrepared,
							types.QuestionGenerationManifestIndexing,
						)
						if transitionErr != nil {
							return transitionErr
						}
						if !changed {
							return errors.New("guarded question publication: prepared manifest changed concurrently")
						}
						manifest.State = types.QuestionGenerationManifestIndexing
					}
					desired, decodeErr := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
					if decodeErr != nil {
						return decodeErr
					}
					if len(desired) > 0 {
						if cleanupErr := engine.DeleteBySourceIDList(
							guardedCtx, desired, manifest.EmbeddingDimension, manifest.KnowledgeType,
						); cleanupErr != nil {
							deferredExternalErr = questionPublicationExternalFailure(
								"cleanup desired questions after embedding drift: %w", cleanupErr,
							)
							return nil
						}
					}
					deferredRuntimeErr = embedderErr
					return nil
				}
				published, loadErr = publishQuestionGenerationManifest(
					guardedCtx, directCommit, s.questionManifestRepo, s.chunkRepo,
					engine, embedder, manifest,
				)
				var externalErr *questionPublicationExternalError
				if errors.As(loadErr, &externalErr) {
					deferredExternalErr = loadErr
					return nil
				}
				if errors.Is(loadErr, repository.ErrChunkRevisionConflict) {
					deferredSemanticErr = loadErr
					return nil
				}
				return loadErr
			},
		)
	})
	if err != nil {
		return nil, err
	}
	if deferredExternalErr != nil {
		return nil, deferredExternalErr
	}
	if deferredSemanticErr != nil {
		return nil, deferredSemanticErr
	}
	if deferredRuntimeErr != nil {
		return nil, deferredRuntimeErr
	}
	return published, nil
}

func sameQuestionManifestRuntimeSnapshot(left, right *types.QuestionGenerationManifest) bool {
	return left != nil && right != nil && left.Key() == right.Key() &&
		left.IdentityVersion == right.IdentityVersion &&
		left.GenerationKey == right.GenerationKey &&
		left.TaskID == right.TaskID &&
		left.VectorStoreID == right.VectorStoreID &&
		left.EmbeddingModelID == right.EmbeddingModelID &&
		left.EmbeddingDimension == right.EmbeddingDimension &&
		left.KnowledgeType == right.KnowledgeType &&
		bytes.Equal(left.EffectiveEngines, right.EffectiveEngines)
}
