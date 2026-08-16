package service

import (
	"context"
	"errors"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type sagaEmbedder struct{}

type sagaDimensionEmbedder struct {
	sagaEmbedder
	dimensions int
}

func (e sagaDimensionEmbedder) GetDimensions() int { return e.dimensions }

func (sagaEmbedder) Embed(context.Context, string) ([]float32, error) { return []float32{1}, nil }
func (sagaEmbedder) BatchEmbed(context.Context, []string) ([][]float32, error) {
	return [][]float32{{1}}, nil
}
func (sagaEmbedder) GetModelName() string { return "test" }
func (sagaEmbedder) GetDimensions() int   { return 1 }
func (sagaEmbedder) GetModelID() string   { return "embedding" }
func (sagaEmbedder) BatchEmbedWithPool(
	context.Context, embedding.Embedder, []string,
) ([][]float32, error) {
	return [][]float32{{1}}, nil
}

type sagaVectorEngine struct {
	indexCalls      int
	failIndex       bool
	failDeleteCalls int
	deleted         [][]string
	events          []string
}

type racingChunkRepository struct {
	interfaces.ChunkRepository
	compareAndSwap func(context.Context, uint64, string, int, types.JSON, types.JSON) (bool, error)
}

func (r *racingChunkRepository) CompareAndSwapChunkMetadata(
	ctx context.Context, tenantID uint64, chunkID string, expectedRevision int,
	expectedMetadata, nextMetadata types.JSON,
) (bool, error) {
	return r.compareAndSwap(ctx, tenantID, chunkID, expectedRevision, expectedMetadata, nextMetadata)
}

type snapshotModelService struct {
	interfaces.ModelService
	requested string
	embedder  embedding.Embedder
	err       error
	calls     int
}

func (s *snapshotModelService) GetEmbeddingModel(_ context.Context, modelID string) (embedding.Embedder, error) {
	s.requested = modelID
	s.calls++
	return s.embedder, s.err
}

type snapshotRetrieveService struct {
	interfaces.RetrieveEngineService
	db           *gorm.DB
	indexCalls   *int
	deleteCalls  *int
	batchEntered chan struct{}
	batchRelease chan struct{}
}

func (snapshotRetrieveService) EngineType() types.RetrieverEngineType {
	return types.RetrieverEngineType("snapshot-engine")
}
func (snapshotRetrieveService) Support() []types.RetrieverType {
	return []types.RetrieverType{types.RetrieverType("snapshot-retriever")}
}
func (s snapshotRetrieveService) BatchIndex(
	_ context.Context, _ embedding.Embedder, _ []*types.IndexInfo, _ []types.RetrieverType,
) error {
	if s.indexCalls != nil {
		(*s.indexCalls)++
	}
	if s.batchEntered != nil {
		select {
		case s.batchEntered <- struct{}{}:
		default:
		}
	}
	if s.batchRelease != nil {
		<-s.batchRelease
	}
	if s.db != nil {
		return s.db.Transaction(func(*gorm.DB) error { return nil })
	}
	return nil
}
func (s snapshotRetrieveService) DeleteBySourceIDList(
	_ context.Context, _ []string, _ int, _ string,
) error {
	if s.deleteCalls != nil {
		(*s.deleteCalls)++
	}
	if s.db != nil {
		return s.db.Transaction(func(*gorm.DB) error { return nil })
	}
	return nil
}

type snapshotRegistry struct {
	interfaces.RetrieveEngineRegistry
	requestedStore string
	service        interfaces.RetrieveEngineService
}

func (r *snapshotRegistry) GetOrLoadByStoreID(
	_ context.Context, _ uint64, storeID string,
) (interfaces.RetrieveEngineService, error) {
	r.requestedStore = storeID
	return r.service, nil
}

type snapshotOwnership struct{ requestedStore string }

func (o *snapshotOwnership) StoreOwnedBy(_ context.Context, storeID string, _ uint64) (bool, error) {
	o.requestedStore = storeID
	return true, nil
}

func (e *sagaVectorEngine) BatchIndex(
	_ context.Context, _ embedding.Embedder, _ []*types.IndexInfo,
) error {
	e.indexCalls++
	e.events = append(e.events, "index")
	if e.failIndex {
		return errors.New("partial backend failure")
	}
	return nil
}

func (e *sagaVectorEngine) DeleteBySourceIDList(
	_ context.Context, sourceIDs []string, _ int, _ string,
) error {
	e.deleted = append(e.deleted, append([]string(nil), sourceIDs...))
	e.events = append(e.events, "delete")
	if e.failDeleteCalls > 0 {
		e.failDeleteCalls--
		return errors.New("injected desired cleanup failure")
	}
	return nil
}

func TestBuildStableGeneratedQuestionsUsesLogicalSlots(t *testing.T) {
	first := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first", "second"})
	retry := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"changed", "wording"})
	nextRevision := buildStableGeneratedQuestions("knowledge", "chunk", 5, 3, []string{"first", "second"})

	require.Len(t, first, 2)
	assert.Equal(t, first[0].ID, retry[0].ID)
	assert.Equal(t, first[1].ID, retry[1].ID)
	assert.NotEqual(t, first[0].ID, nextRevision[0].ID)
	assert.Equal(t, 4, *first[0].ContentRevision)
}

func TestStablePublishedQuestionsCanBeReusedWithoutManifest(t *testing.T) {
	questions := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first", "second"})
	meta := &types.DocumentChunkMetadata{
		GeneratedQuestions: questions, GeneratedQuestionsRevision: 4,
	}

	assert.True(t, stablePublishedQuestions(meta, "knowledge", "chunk", 4, 3))
	meta.GeneratedQuestions[0].ID = "legacy-random"
	assert.False(t, stablePublishedQuestions(meta, "knowledge", "chunk", 4, 3))
}

func TestQuestionManifestRuntimeSnapshotWinsOverCurrentKBDrift(t *testing.T) {
	engines := []types.RetrieverEngineParams{{
		RetrieverEngineType: types.RetrieverEngineType("snapshot-engine"),
		RetrieverType:       types.RetrieverType("snapshot-retriever"),
	}}
	encoded, err := questionManifestJSON(engines)
	require.NoError(t, err)
	manifest := &types.QuestionGenerationManifest{
		EmbeddingModelID: "snapshot-embedding", VectorStoreID: "snapshot-store",
		EffectiveEngines: encoded,
	}

	runtime, err := runtimeSnapshotFromQuestionManifest(manifest)
	require.NoError(t, err)
	assert.Equal(t, "snapshot-embedding", runtime.EmbeddingModelID)
	require.NotNil(t, runtime.VectorStoreID)
	assert.Equal(t, "snapshot-store", *runtime.VectorStoreID)
	assert.Equal(t, engines, runtime.EffectiveEngines)
}

func TestResolveQuestionManifestRuntimeUsesManifestModelAndStore(t *testing.T) {
	encoded, err := questionManifestJSON([]types.RetrieverEngineParams{})
	require.NoError(t, err)
	manifest := &types.QuestionGenerationManifest{
		TenantID: 7, EmbeddingModelID: "snapshot-embedding", EmbeddingDimension: 1,
		VectorStoreID: "snapshot-store", EffectiveEngines: encoded,
	}
	models := &snapshotModelService{embedder: sagaEmbedder{}}
	registry := &snapshotRegistry{service: snapshotRetrieveService{}}
	ownership := &snapshotOwnership{}
	svc := &knowledgeService{modelService: models, retrieveEngine: registry, ownership: ownership}

	embedder, engine, err := svc.resolveQuestionManifestRuntime(context.Background(), manifest)
	require.NoError(t, err)
	require.NotNil(t, embedder)
	require.NotNil(t, engine)
	assert.Equal(t, "snapshot-embedding", models.requested)
	assert.Equal(t, "snapshot-store", ownership.requestedStore)
	assert.Equal(t, "snapshot-store", registry.requestedStore)
}

func TestQuestionPublicationSourceSetsDeleteOnlyAbandoned(t *testing.T) {
	oldQuestions := []types.GeneratedQuestion{{ID: "old-a"}, {ID: "old-b"}}
	desired := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first", "second"})
	desiredIDs, abandoned := questionPublicationSourceSets("chunk", oldQuestions, desired)

	require.Len(t, desiredIDs, 2)
	assert.ElementsMatch(t, []string{"chunk-old-a", "chunk-old-b"}, abandoned)
	for _, sourceID := range desiredIDs {
		assert.NotContains(t, abandoned, sourceID)
	}
}

func setupIndexedQuestionManifest(t *testing.T) (
	*gorm.DB,
	interfaces.ChunkRepository,
	interfaces.QuestionGenerationManifestRepository,
	*types.QuestionGenerationManifest,
	[]types.GeneratedQuestion,
) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Chunk{}, &types.QuestionGenerationManifest{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "test"}).Error)
	chunkRepo := apprepo.NewChunkRepository(db)
	manifestRepo := apprepo.NewQuestionGenerationManifestRepository(db)
	chunk := &types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb",
		Content: "body", SourceContent: "body", ContentRevision: 4,
		ChunkType: types.ChunkTypeText, IsEnabled: true,
	}
	require.NoError(t, chunk.SetDocumentMetadata(&types.DocumentChunkMetadata{
		GeneratedQuestions:         []types.GeneratedQuestion{{ID: "old", Question: "old"}},
		GeneratedQuestionsRevision: 4,
	}))
	require.NoError(t, chunkRepo.CreateChunks(context.Background(), []*types.Chunk{chunk}))
	questions := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first"})
	manifest, err := newQuestionGenerationManifest(questionPublicationSnapshot{
		TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", BatchIndex: 3,
		Attempt: 11, TaskID: "task", EmbeddingModelID: "embedding",
		EmbeddingDimension: 1, KnowledgeType: "document",
	}, &types.Knowledge{ID: "knowledge", KnowledgeBaseID: "kb"}, chunk, questions,
		types.QuestionGenerationManifestIndexed)
	require.NoError(t, err)
	manifest, _, err = manifestRepo.GetOrCreateQuestionGenerationManifest(context.Background(), manifest)
	require.NoError(t, err)
	return db, chunkRepo, manifestRepo, manifest, questions
}

func configureQuestionManifestRuntime(
	t *testing.T, db *gorm.DB, manifest *types.QuestionGenerationManifest,
	state types.QuestionGenerationManifestState,
) {
	t.Helper()
	engines, err := questionManifestJSON([]types.RetrieverEngineParams{{
		RetrieverEngineType: types.RetrieverEngineType("snapshot-engine"),
		RetrieverType:       types.RetrieverType("snapshot-retriever"),
	}})
	require.NoError(t, err)
	manifest.VectorStoreID = "snapshot-store"
	manifest.EffectiveEngines = engines
	manifest.State = state
	require.NoError(t, db.Model(&types.QuestionGenerationManifest{}).Where("id = ?", manifest.ID).
		Updates(map[string]any{
			"vector_store_id":   manifest.VectorStoreID,
			"effective_engines": engines,
			"state":             manifest.State,
		}).Error)
}

func newGuardedQuestionPublicationService(
	chunkRepo interfaces.ChunkRepository,
	manifestRepo interfaces.QuestionGenerationManifestRepository,
	models interfaces.ModelService,
	engine interfaces.RetrieveEngineService,
) *knowledgeService {
	return &knowledgeService{
		chunkRepo: chunkRepo, questionManifestRepo: manifestRepo, modelService: models,
		retrieveEngine: &snapshotRegistry{service: engine}, ownership: &snapshotOwnership{},
	}
}

func TestPublishQuestionGenerationManifestCASRaceCanonicalMetadataSucceedsWithoutDeletingDesired(t *testing.T) {
	db, chunkRepo, manifestRepo, manifest, questions := setupIndexedQuestionManifest(t)
	engine := &sagaVectorEngine{}
	tracingRepo := &racingChunkRepository{
		ChunkRepository: chunkRepo,
		compareAndSwap: func(_ context.Context, tenantID uint64, chunkID string, revision int, _, next types.JSON) (bool, error) {
			require.NoError(t, db.Model(&types.Chunk{}).
				Where("tenant_id = ? AND id = ? AND content_revision = ?", tenantID, chunkID, revision).
				Update("metadata", next).Error)
			return false, nil
		},
	}
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})
	desired, err := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
	require.NoError(t, err)

	published, err := publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, tracingRepo, engine, sagaEmbedder{}, manifest,
	)
	require.NoError(t, err)
	assert.Equal(t, questions, published)
	for _, deleted := range engine.deleted {
		assert.NotEqual(t, desired, deleted, "canonical CAS race must retain desired vectors")
	}
	assert.Equal(t, [][]string{{"chunk-old"}}, engine.deleted)
	_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestPublishQuestionGenerationManifestCASRaceDifferentMetadataAbortsAndCleansDesired(t *testing.T) {
	db, chunkRepo, manifestRepo, manifest, _ := setupIndexedQuestionManifest(t)
	engine := &sagaVectorEngine{}
	differentMetadata := types.JSON(`{"generated_questions":[{"id":"other","question":"other"}],"generated_questions_revision":4}`)
	tracingRepo := &racingChunkRepository{
		ChunkRepository: chunkRepo,
		compareAndSwap: func(_ context.Context, tenantID uint64, chunkID string, revision int, _, _ types.JSON) (bool, error) {
			require.NoError(t, db.Model(&types.Chunk{}).
				Where("tenant_id = ? AND id = ? AND content_revision = ?", tenantID, chunkID, revision).
				Update("metadata", differentMetadata).Error)
			return false, nil
		},
	}
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})
	desired, err := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
	require.NoError(t, err)

	_, err = publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, tracingRepo, engine, sagaEmbedder{}, manifest,
	)
	assert.ErrorIs(t, err, apprepo.ErrChunkRevisionConflict)
	assert.Equal(t, [][]string{desired}, engine.deleted)
	_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestPublishQuestionGenerationManifestCASRaceCleanupFailureIsRetryableAndRetainsAbortManifest(t *testing.T) {
	db, chunkRepo, manifestRepo, manifest, _ := setupIndexedQuestionManifest(t)
	engine := &sagaVectorEngine{failDeleteCalls: 1}
	differentMetadata := types.JSON(`{"generated_questions":[{"id":"other","question":"other"}],"generated_questions_revision":4}`)
	tracingRepo := &racingChunkRepository{
		ChunkRepository: chunkRepo,
		compareAndSwap: func(_ context.Context, tenantID uint64, chunkID string, revision int, _, _ types.JSON) (bool, error) {
			require.NoError(t, db.Model(&types.Chunk{}).
				Where("tenant_id = ? AND id = ? AND content_revision = ?", tenantID, chunkID, revision).
				Update("metadata", differentMetadata).Error)
			return false, nil
		},
	}
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})

	_, err := publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, tracingRepo, engine, sagaEmbedder{}, manifest,
	)
	require.ErrorContains(t, err, "injected desired cleanup failure")
	assert.False(t, errors.Is(err, apprepo.ErrChunkRevisionConflict))
	retained, err := manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	require.NoError(t, err)
	assert.Equal(t, types.QuestionGenerationManifestAbortCleanup, retained.State)

	_, err = publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, tracingRepo, engine, sagaEmbedder{}, retained,
	)
	assert.ErrorIs(t, err, apprepo.ErrChunkRevisionConflict)
	_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestGuardedQuestionPublicationSQLiteMaxOpenConnsOneDoesNotDeadlock(t *testing.T) {
	db, chunkRepo, manifestRepo, manifest, questions := setupIndexedQuestionManifest(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	engines, err := questionManifestJSON([]types.RetrieverEngineParams{{
		RetrieverEngineType: types.RetrieverEngineType("snapshot-engine"),
		RetrieverType:       types.RetrieverType("snapshot-retriever"),
	}})
	require.NoError(t, err)
	manifest.VectorStoreID = "snapshot-store"
	manifest.EffectiveEngines = engines
	manifest.State = types.QuestionGenerationManifestIndexing
	require.NoError(t, db.Model(&types.QuestionGenerationManifest{}).
		Where("id = ?", manifest.ID).
		Updates(map[string]any{
			"vector_store_id":   manifest.VectorStoreID,
			"effective_engines": engines,
			"state":             manifest.State,
		}).Error)
	indexCalls, deleteCalls := 0, 0
	service := snapshotRetrieveService{db: db, indexCalls: &indexCalls, deleteCalls: &deleteCalls}
	svc := &knowledgeService{
		chunkRepo: chunkRepo, questionManifestRepo: manifestRepo,
		modelService:   &snapshotModelService{embedder: sagaEmbedder{}},
		retrieveEngine: &snapshotRegistry{service: service}, ownership: &snapshotOwnership{},
	}
	done := make(chan error, 1)
	go func() {
		_, publishErr := svc.publishQuestionGenerationManifestWithGuard(
			context.Background(), func(ctx context.Context, fn func(context.Context) error) error {
				return fn(ctx)
			}, manifest,
		)
		done <- publishErr
	}()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("guarded SQLite publication deadlocked with MaxOpenConns(1)")
	}
	assert.Equal(t, 1, indexCalls)
	assert.Equal(t, 2, deleteCalls, "desired pre-delete and abandoned cleanup must both execute")
	stored, err := chunkRepo.GetChunkByID(context.Background(), 7, "chunk")
	require.NoError(t, err)
	metadata, err := stored.DocumentMetadata()
	require.NoError(t, err)
	assert.Equal(t, questions, metadata.GeneratedQuestions)
}

func TestGuardedQuestionPublicationConcurrentLoserReusesCanonicalWithoutVectorMutation(t *testing.T) {
	db, chunkRepo, manifestRepo, manifest, _ := setupIndexedQuestionManifest(t)
	engines, err := questionManifestJSON([]types.RetrieverEngineParams{{
		RetrieverEngineType: types.RetrieverEngineType("snapshot-engine"),
		RetrieverType:       types.RetrieverType("snapshot-retriever"),
	}})
	require.NoError(t, err)
	manifest.VectorStoreID = "snapshot-store"
	manifest.EffectiveEngines = engines
	manifest.State = types.QuestionGenerationManifestIndexing
	require.NoError(t, db.Model(&types.QuestionGenerationManifest{}).Where("id = ?", manifest.ID).
		Updates(map[string]any{
			"vector_store_id":   manifest.VectorStoreID,
			"effective_engines": engines,
			"state":             manifest.State,
		}).Error)
	indexCalls, deleteCalls := 0, 0
	entered, release := make(chan struct{}, 1), make(chan struct{})
	service := snapshotRetrieveService{
		indexCalls: &indexCalls, deleteCalls: &deleteCalls,
		batchEntered: entered, batchRelease: release,
	}
	svc := &knowledgeService{
		chunkRepo: chunkRepo, questionManifestRepo: manifestRepo,
		modelService:   &snapshotModelService{embedder: sagaEmbedder{}},
		retrieveEngine: &snapshotRegistry{service: service}, ownership: &snapshotOwnership{},
	}
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})
	results := make(chan error, 2)
	go func() {
		_, publishErr := svc.publishQuestionGenerationManifestWithGuard(context.Background(), commit, manifest)
		results <- publishErr
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first publisher did not reach BatchIndex")
	}
	go func() {
		_, publishErr := svc.publishQuestionGenerationManifestWithGuard(context.Background(), commit, manifest)
		results <- publishErr
	}()
	select {
	case err = <-results:
		t.Fatalf("second publisher escaped manifest guard early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	assert.Equal(t, 1, indexCalls)
	assert.Equal(t, 2, deleteCalls, "loser must not delete desired or abandoned vectors")
}

func TestPublishQuestionGenerationManifestTerminalStatesDoNotRequireEmbedder(t *testing.T) {
	for _, state := range []types.QuestionGenerationManifestState{
		types.QuestionGenerationManifestIndexed,
		types.QuestionGenerationManifestPublished,
		types.QuestionGenerationManifestAbortCleanup,
	} {
		t.Run(string(state), func(t *testing.T) {
			_, chunkRepo, manifestRepo, manifest, _ := setupIndexedQuestionManifest(t)
			if state != types.QuestionGenerationManifestIndexed {
				changed, err := manifestRepo.TransitionQuestionGenerationManifest(
					context.Background(), manifest.Key(), types.QuestionGenerationManifestIndexed, state,
				)
				if state == types.QuestionGenerationManifestPublished {
					require.NoError(t, err)
					require.True(t, changed)
				} else {
					require.NoError(t, err)
					require.True(t, changed)
				}
				manifest.State = state
			}
			engine := &sagaVectorEngine{}
			_, err := publishQuestionGenerationManifest(
				context.Background(), func(ctx context.Context, fn func(context.Context) error) error {
					return fn(ctx)
				}, manifestRepo, chunkRepo, engine, nil, manifest,
			)
			if state == types.QuestionGenerationManifestAbortCleanup {
				assert.ErrorIs(t, err, apprepo.ErrChunkRevisionConflict)
			} else {
				require.NoError(t, err)
			}
			_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
			assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
		})
	}
}

func TestGuardedQuestionPublicationIndexingModelDriftCleansDesiredAndRetainsManifest(t *testing.T) {
	for _, test := range []struct {
		name   string
		models *snapshotModelService
		want   string
	}{
		{name: "model deleted", models: &snapshotModelService{err: errors.New("model deleted")}, want: "model deleted"},
		{name: "dimension drift", models: &snapshotModelService{embedder: sagaDimensionEmbedder{dimensions: 2}}, want: "dimension drift"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, chunkRepo, manifestRepo, manifest, _ := setupIndexedQuestionManifest(t)
			configureQuestionManifestRuntime(t, db, manifest, types.QuestionGenerationManifestIndexing)
			indexCalls, deleteCalls := 0, 0
			service := snapshotRetrieveService{indexCalls: &indexCalls, deleteCalls: &deleteCalls}
			svc := newGuardedQuestionPublicationService(chunkRepo, manifestRepo, test.models, service)

			_, err := svc.publishQuestionGenerationManifestWithGuard(
				context.Background(), func(ctx context.Context, fn func(context.Context) error) error {
					return fn(ctx)
				}, manifest,
			)
			require.ErrorContains(t, err, test.want)
			assert.False(t, errors.Is(err, apprepo.ErrChunkRevisionConflict))
			assert.Equal(t, 0, indexCalls)
			assert.Equal(t, 1, deleteCalls)
			retained, err := manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
			require.NoError(t, err)
			assert.Equal(t, types.QuestionGenerationManifestIndexing, retained.State)
		})
	}
}

func TestPublishQuestionGenerationManifestResumesAfterIndexFailure(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Chunk{}, &types.QuestionGenerationManifest{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "test"}).Error)
	chunkRepo := apprepo.NewChunkRepository(db)
	manifestRepo := apprepo.NewQuestionGenerationManifestRepository(db)
	old := []types.GeneratedQuestion{{ID: "legacy-random", Question: "old"}}
	chunk := &types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb",
		Content: "body", SourceContent: "body", ContentRevision: 4,
		ChunkType: types.ChunkTypeText, IsEnabled: true,
	}
	require.NoError(t, chunk.SetDocumentMetadata(&types.DocumentChunkMetadata{
		GeneratedQuestions: old, GeneratedQuestionsRevision: 4,
	}))
	require.NoError(t, chunkRepo.CreateChunks(context.Background(), []*types.Chunk{chunk}))

	questions := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first", "second"})
	manifest, err := newQuestionGenerationManifest(questionPublicationSnapshot{
		TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", BatchIndex: 3,
		Attempt: 11, TaskID: "task", EmbeddingModelID: "embedding",
		EmbeddingDimension: 1, KnowledgeType: "document",
	}, &types.Knowledge{ID: "knowledge", KnowledgeBaseID: "kb"}, chunk, questions,
		types.QuestionGenerationManifestPrepared)
	require.NoError(t, err)
	manifest, _, err = manifestRepo.GetOrCreateQuestionGenerationManifest(context.Background(), manifest)
	require.NoError(t, err)
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})
	engine := &sagaVectorEngine{failIndex: true}

	_, err = publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, chunkRepo, engine, sagaEmbedder{}, manifest,
	)
	require.ErrorContains(t, err, "partial backend failure")
	manifest, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	require.NoError(t, err)
	assert.Equal(t, types.QuestionGenerationManifestIndexing, manifest.State)
	stored, err := chunkRepo.GetChunkByID(context.Background(), 7, "chunk")
	require.NoError(t, err)
	metadata, err := stored.DocumentMetadata()
	require.NoError(t, err)
	assert.Equal(t, "legacy-random", metadata.GeneratedQuestions[0].ID)

	engine.failIndex = false
	desired, err := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
	require.NoError(t, err)
	published, err := publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, chunkRepo, engine, sagaEmbedder{}, manifest,
	)
	require.NoError(t, err)
	assert.Equal(t, questions, published)
	assert.Equal(t, 2, engine.indexCalls, "retry reindexes the same desired physical IDs")
	_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	stored, err = chunkRepo.GetChunkByID(context.Background(), 7, "chunk")
	require.NoError(t, err)
	metadata, err = stored.DocumentMetadata()
	require.NoError(t, err)
	assert.Equal(t, questions, metadata.GeneratedQuestions)
	require.Len(t, engine.deleted, 3)
	assert.Equal(t, desired, engine.deleted[0])
	assert.Equal(t, desired, engine.deleted[1])
	assert.Equal(t, []string{"chunk-legacy-random"}, engine.deleted[2])
	assert.Equal(t, []string{"delete", "index", "delete", "index", "delete"}, engine.events)
}

func TestPublishQuestionGenerationManifestRevisionConflictCleansOnlyDesiredAndDeletesManifest(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Chunk{}, &types.QuestionGenerationManifest{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "test"}).Error)
	chunkRepo := apprepo.NewChunkRepository(db)
	manifestRepo := apprepo.NewQuestionGenerationManifestRepository(db)
	chunk := &types.Chunk{
		ID: "chunk", TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb",
		Content: "body", SourceContent: "body", ContentRevision: 4,
		ChunkType: types.ChunkTypeText, IsEnabled: true,
	}
	require.NoError(t, chunkRepo.CreateChunks(context.Background(), []*types.Chunk{chunk}))
	questions := buildStableGeneratedQuestions("knowledge", "chunk", 4, 3, []string{"first"})
	manifest, err := newQuestionGenerationManifest(questionPublicationSnapshot{
		TenantID: 7, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", BatchIndex: 3,
		Attempt: 11, TaskID: "task", EmbeddingModelID: "embedding",
		EmbeddingDimension: 1, KnowledgeType: "document",
	}, &types.Knowledge{ID: "knowledge", KnowledgeBaseID: "kb"}, chunk, questions,
		types.QuestionGenerationManifestIndexing)
	require.NoError(t, err)
	manifest, _, err = manifestRepo.GetOrCreateQuestionGenerationManifest(context.Background(), manifest)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "chunk").Update("content_revision", 5).Error)
	commit := questionPublicationCommit(func(ctx context.Context, fn func(context.Context) error) error {
		return fn(ctx)
	})
	engine := &sagaVectorEngine{}
	desired, err := decodeQuestionManifestJSON[[]string](manifest.DesiredSourceIDs)
	require.NoError(t, err)

	_, err = publishQuestionGenerationManifest(
		context.Background(), commit, manifestRepo, chunkRepo, engine, sagaEmbedder{}, manifest,
	)
	assert.ErrorIs(t, err, apprepo.ErrChunkRevisionConflict)
	_, err = manifestRepo.GetQuestionGenerationManifest(context.Background(), manifest.Key())
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	require.Len(t, engine.deleted, 1)
	assert.Equal(t, desired, engine.deleted[0])
	assert.Equal(t, []string{"delete"}, engine.events)
}
