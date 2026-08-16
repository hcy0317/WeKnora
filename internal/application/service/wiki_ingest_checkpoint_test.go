package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type wikiCheckpointPageService struct {
	interfaces.WikiPageService
	mappedOutput         types.JSON
	prepared             *types.WikiIngestWorkUnit
	taxonomyPlan         *types.WikiTaxonomyPlan
	folderPaths          [][]string
	applications         map[string]*types.WikiSlugApplication
	markers              []types.WikiSlugContributionMarker
	page                 *types.WikiPage
	writes               int
	publishTransition    types.WikiSlugApplicationTransition
	hasPublishTransition bool
	taxonomyMappedWrites int
}

func (s *wikiCheckpointPageService) PrepareWikiIngestWorkUnit(
	_ context.Context, unit *types.WikiIngestWorkUnit,
) (*types.WikiIngestWorkUnit, error) {
	copy := *unit
	copy.State = types.WikiIngestWorkUnitMapped
	copy.MappedOutput = append(types.JSON(nil), s.mappedOutput...)
	s.prepared = &copy
	return &copy, nil
}

func (s *wikiCheckpointPageService) PrepareAndBindWikiIngestWorkUnit(
	ctx context.Context, _ types.WikiIngestWorkBinding, unit *types.WikiIngestWorkUnit,
) (*types.WikiIngestWorkUnit, error) {
	return s.PrepareWikiIngestWorkUnit(ctx, unit)
}

func (s *wikiCheckpointPageService) MarkWikiIngestWorkUnitMapped(context.Context, string, types.JSON) error {
	return nil
}

func (s *wikiCheckpointPageService) PrepareWikiTaxonomyPlan(_ context.Context, plan *types.WikiTaxonomyPlan) (*types.WikiTaxonomyPlan, error) {
	if s.taxonomyPlan != nil && s.taxonomyPlan.PlanID == plan.PlanID {
		copy := *s.taxonomyPlan
		return &copy, nil
	}
	copy := *plan
	s.taxonomyPlan = &copy
	return &copy, nil
}

func (s *wikiCheckpointPageService) FindMappedWikiTaxonomyPlan(
	_ context.Context, tenantID uint64, kbID, workDigest, missingDigest, contract string,
) (*types.WikiTaxonomyPlan, error) {
	plan := s.taxonomyPlan
	if plan != nil && plan.State == types.WikiTaxonomyPlanMapped && plan.TenantID == tenantID &&
		plan.KnowledgeBaseID == kbID && plan.WorkSetDigest == workDigest &&
		plan.MissingSetDigest == missingDigest && plan.ContractKey == contract {
		copy := *plan
		return &copy, nil
	}
	return nil, nil
}

func (s *wikiCheckpointPageService) SaveWikiTaxonomyPlanProgress(
	_ context.Context, planID string, expected, output types.JSON,
) error {
	if s.taxonomyPlan == nil || s.taxonomyPlan.PlanID != planID {
		return errors.New("taxonomy plan missing")
	}
	if !taxonomyJSONEqual(s.taxonomyPlan.ResolvedOutput, expected) {
		return errors.New("taxonomy checkpoint changed concurrently")
	}
	s.taxonomyPlan.ResolvedOutput = append(types.JSON(nil), output...)
	return nil
}

func taxonomyJSONEqual(left, right []byte) bool {
	var l, r any
	return json.Unmarshal(left, &l) == nil && json.Unmarshal(right, &r) == nil && reflect.DeepEqual(l, r)
}

func (s *wikiCheckpointPageService) MarkWikiTaxonomyPlanMapped(_ context.Context, planID string, output types.JSON) error {
	if s.taxonomyPlan != nil && s.taxonomyPlan.PlanID == planID {
		s.taxonomyMappedWrites++
		s.taxonomyPlan.State = types.WikiTaxonomyPlanMapped
		s.taxonomyPlan.ResolvedOutput = append(types.JSON(nil), output...)
	}
	return nil
}

func (s *wikiCheckpointPageService) ListDistinctCategoryPaths(context.Context, string, int) ([][]string, error) {
	return append([][]string(nil), s.folderPaths...), nil
}

func (s *wikiCheckpointPageService) PrepareWikiSlugApplication(
	_ context.Context, application *types.WikiSlugApplication,
) (*types.WikiSlugApplication, error) {
	if s.applications == nil {
		s.applications = make(map[string]*types.WikiSlugApplication)
	}
	if existing := s.applications[application.PlanID]; existing != nil {
		copy := *existing
		return &copy, nil
	}
	copy := *application
	s.applications[copy.PlanID] = &copy
	return &copy, nil
}

func (s *wikiCheckpointPageService) MarkWikiSlugApplicationApplying(_ context.Context, planID, output string) error {
	application := s.applications[planID]
	if application == nil {
		return errors.New("application missing")
	}
	application.State = types.WikiSlugApplicationApplying
	application.GeneratedOutput = output
	return nil
}

func (s *wikiCheckpointPageService) FindWikiSlugApplication(
	_ context.Context, _ uint64, kbID, slug, contributionKey string,
) (*types.WikiSlugApplication, error) {
	for _, application := range s.applications {
		if application.KnowledgeBaseID == kbID && application.Slug == slug && application.ContributionKey == contributionKey &&
			(application.State == types.WikiSlugApplicationApplying || application.State == types.WikiSlugApplicationPublished) {
			copy := *application
			return &copy, nil
		}
	}
	return nil, nil
}

func (s *wikiCheckpointPageService) ListWikiSlugContributionMarkers(
	context.Context, []string,
) ([]types.WikiSlugContributionMarker, error) {
	return append([]types.WikiSlugContributionMarker(nil), s.markers...), nil
}

func (s *wikiCheckpointPageService) GetPageBySlug(context.Context, string, string) (*types.WikiPage, error) {
	if s.page == nil {
		return nil, apprepo.ErrWikiPageNotFound
	}
	copy := *s.page
	return &copy, nil
}

func (s *wikiCheckpointPageService) CreatePageGuarded(_ context.Context, page *types.WikiPage, _ []types.WikiSourceAttemptGuard) (*types.WikiPage, error) {
	copy := *page
	s.page = &copy
	s.writes++
	return &copy, nil
}

func (s *wikiCheckpointPageService) UpdatePageGuarded(_ context.Context, page *types.WikiPage, _ []types.WikiSourceAttemptGuard) (*types.WikiPage, error) {
	copy := *page
	s.page = &copy
	s.writes++
	return &copy, nil
}

func (s *wikiCheckpointPageService) UpdatePageMetaGuarded(ctx context.Context, page *types.WikiPage, _ []types.WikiSourceAttemptGuard) error {
	copy := *page
	s.page = &copy
	s.writes++
	s.publishTransition, s.hasPublishTransition = types.WikiSlugApplicationTransitionFromContext(ctx)
	return nil
}

func TestPublishDraftPagesRestoresApplyingPlanFromCheckpoint(t *testing.T) {
	const (
		tenantID = uint64(7)
		kbID     = "kb"
		slug     = "entity/retired"
		workID   = "work-1"
		planID   = "plan-1"
	)
	updates := []SlugUpdate{{
		Slug: slug, Type: "retractStale", KnowledgeID: "kid", Attempt: 3, WorkID: workID,
	}}
	contributionKey, _, _ := wikiSlugContributionIdentity(slug, updates)
	pages := &wikiCheckpointPageService{
		page: &types.WikiPage{
			ID: "page-1", KnowledgeBaseID: kbID, Slug: slug,
			Status: types.WikiPageStatusArchived, SourceRefs: types.StringArray{},
		},
		applications: map[string]*types.WikiSlugApplication{planID: {
			PlanID: planID, TenantID: tenantID, KnowledgeBaseID: kbID, Slug: slug,
			ContributionKey: contributionKey, State: types.WikiSlugApplicationApplying,
		}},
	}
	service := &wikiIngestService{wikiService: pages}

	failed := service.publishDraftPages(
		context.Background(), tenantID, kbID, []string{slug}, map[string][]SlugUpdate{slug: updates},
	)

	require.Empty(t, failed)
	require.Equal(t, 1, pages.writes)
	require.True(t, pages.hasPublishTransition)
	require.Equal(t, planID, pages.publishTransition.PlanID)
	require.Equal(t, types.WikiSlugApplicationPublished, pages.publishTransition.State)
}

type wikiCheckpointChunkRepo struct {
	interfaces.ChunkRepository
	chunks []*types.Chunk
}

type wikiCheckpointAttemptTracker struct{ strictWikiAttemptTracker }

func (t *wikiCheckpointAttemptTracker) LookupStage(_ context.Context, knowledgeID string, attempt int, stage string) *Span {
	return &Span{KnowledgeID: knowledgeID, Attempt: attempt, SpanID: "post", Name: stage, Kind: types.SpanKindStage}
}

func (t *wikiCheckpointAttemptTracker) BeginSubSpan(_ context.Context, parent *Span, name, kind string, input types.JSONMap) *Span {
	return &Span{KnowledgeID: parent.KnowledgeID, Attempt: parent.Attempt, SpanID: "wiki", ParentSpanID: parent.SpanID,
		Name: name, Kind: kind, Input: input}
}

func (r *wikiCheckpointChunkRepo) ListChunksByKnowledgeID(
	context.Context, uint64, string,
) ([]*types.Chunk, error) {
	return append([]*types.Chunk(nil), r.chunks...), nil
}

type wikiCheckpointCountingChat struct{ calls atomic.Int32 }

func (m *wikiCheckpointCountingChat) Chat(
	context.Context, []chat.Message, *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.calls.Add(1)
	return &types.ChatResponse{Content: "unexpected"}, nil
}

func (m *wikiCheckpointCountingChat) ChatStream(
	context.Context, []chat.Message, *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	ch := make(chan types.StreamResponse)
	close(ch)
	return ch, nil
}

func (*wikiCheckpointCountingChat) GetModelName() string { return "checkpoint" }
func (*wikiCheckpointCountingChat) GetModelID() string   { return "checkpoint-model" }

type wikiTaxonomyCountingChat struct {
	calls atomic.Int32
}

func (m *wikiTaxonomyCountingChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls.Add(1)
	return &types.ChatResponse{Content: `{"assignments":[{"slug":"entity/a","path":["People"]}]}`}, nil
}

func (m *wikiTaxonomyCountingChat) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	ch := make(chan types.StreamResponse, 1)
	ch <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer,
		Content: `{"assignments":[{"slug":"entity/a","path":["People"]}]}`, Done: true, FinishReason: "stop"}
	close(ch)
	return ch, nil
}

func (*wikiTaxonomyCountingChat) GetModelName() string { return "taxonomy" }
func (*wikiTaxonomyCountingChat) GetModelID() string   { return "taxonomy-current-config" }

type wikiPageCountingChat struct{ calls atomic.Int32 }

type wikiTaxonomySecondChunkFailChat struct {
	calls atomic.Int32
}

func (m *wikiTaxonomySecondChunkFailChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, errors.New("unexpected non-stream call")
}

func (m *wikiTaxonomySecondChunkFailChat) ChatStream(_ context.Context, messages []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	call := m.calls.Add(1)
	if call == 2 {
		return nil, errors.New("invalid taxonomy request")
	}
	assignments := make([]map[string]any, 0)
	for _, line := range strings.Split(messages[len(messages)-1].Content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- slug: ") {
			continue
		}
		slug := strings.TrimSpace(strings.SplitN(strings.TrimPrefix(line, "- slug: "), " |", 2)[0])
		assignments = append(assignments, map[string]any{"slug": slug, "path": []string{}})
	}
	encoded, _ := json.Marshal(map[string]any{"assignments": assignments})
	ch := make(chan types.StreamResponse, 1)
	ch <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer,
		Content: string(encoded), Done: true, FinishReason: "stop"}
	close(ch)
	return ch, nil
}

func (*wikiTaxonomySecondChunkFailChat) GetModelName() string { return "taxonomy-scripted" }
func (*wikiTaxonomySecondChunkFailChat) GetModelID() string   { return "taxonomy-scripted" }

func (m *wikiPageCountingChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls.Add(1)
	return &types.ChatResponse{Content: "SUMMARY: A summary\nA durable page body."}, nil
}

func (m *wikiPageCountingChat) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	ch := make(chan types.StreamResponse, 1)
	ch <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer,
		Content: "SUMMARY: A summary\nA durable page body.", Done: true, FinishReason: "stop"}
	close(ch)
	return ch, nil
}

func (*wikiPageCountingChat) GetModelName() string { return "page" }
func (*wikiPageCountingChat) GetModelID() string   { return "page-model" }

func TestMapOneDocumentReusesMappedWorkUnitAcrossFreshAttemptWithoutModelCall(t *testing.T) {
	mappedJSON, err := json.Marshal(wikiMappedCheckpoint{
		DocTitle: "Document", Summary: "Done",
		Pages:    []wikiIngestPageRef{{Slug: "entity/done", Title: "Done"}},
		MapStats: types.JSONMap{"updates": 1},
		Updates: []SlugUpdate{{Slug: "entity/done", Type: types.WikiPageTypeEntity,
			KnowledgeID: "kid", Item: extractedItem{Name: "Done", Slug: "entity/done"}}},
	})
	require.NoError(t, err)
	pageService := &wikiCheckpointPageService{mappedOutput: mappedJSON}
	model := &wikiCheckpointCountingChat{}
	svc := &wikiIngestService{
		wikiService: pageService,
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "kid", KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusFinalizing,
		}},
		chunkRepo: &wikiCheckpointChunkRepo{chunks: []*types.Chunk{{
			ID: "chunk-1", ChunkIndex: 0, ContentRevision: 7, Content: "enough source text",
		}}},
		spanTracker: &wikiCheckpointAttemptTracker{strictWikiAttemptTracker: strictWikiAttemptTracker{latest: 5}},
	}

	result, updates, err := svc.mapOneDocument(context.Background(), model,
		WikiIngestPayload{TenantID: 7, KnowledgeBaseID: "kb"},
		WikiPendingOp{Op: WikiOpIngest, KnowledgeID: "kid", Attempt: 5},
		&WikiBatchContext{ExtractionGranularity: types.WikiExtractionStandard})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 5, result.SourceOp.Attempt)
	require.Len(t, updates, 1)
	require.NotEmpty(t, result.WorkID)
	require.Equal(t, result.WorkID, updates[0].WorkID)
	require.Zero(t, model.calls.Load(), "mapped replay must not invoke any Wiki model generation")
}

func TestPartialWikiRetryRealRepositoryChainRestoresMappedWorkWithoutModelCall(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(
		&types.Knowledge{}, &types.KnowledgeProcessingSpan{}, &types.TaskPendingOp{},
		&types.WikiIngestWorkUnit{}, &types.WikiTaxonomyPlan{},
		&types.WikiSlugApplication{}, &types.WikiSlugContributionMarker{},
	))
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX uq_test_span_identity
		ON knowledge_processing_spans (knowledge_id, attempt, span_id)`).Error)
	require.NoError(t, db.Exec(`CREATE UNIQUE INDEX uq_test_root_attempt
		ON knowledge_processing_spans (knowledge_id, attempt) WHERE kind = 'root'`).Error)
	knowledge := &types.Knowledge{ID: "kid", TenantID: 7, KnowledgeBaseID: "kb",
		Title: "Document", ParseStatus: types.ParseStatusFailed}
	require.NoError(t, db.Create(knowledge).Error)
	spanRepo := apprepo.NewKnowledgeSpanRepository(db)
	now := time.Now().Add(-time.Minute)
	finished := now.Add(time.Second)
	mappedJSON, err := json.Marshal(wikiMappedCheckpoint{
		DocTitle: "Document", Summary: "Done", Pages: []wikiIngestPageRef{{Slug: "entity/done", Title: "Done"}},
		Updates: []SlugUpdate{{Slug: "entity/done", Type: types.WikiPageTypeEntity,
			KnowledgeID: "kid", Item: extractedItem{Name: "Done", Slug: "entity/done"}}},
	})
	require.NoError(t, err)
	chunks := []*types.Chunk{{ID: "chunk-1", ChunkIndex: 0, ContentRevision: 7, Content: "enough source text"}}
	sourceDigest := wikiSourceRevisionDigest(chunks)
	titleKey := wikiCheckpointDigest("Document")
	work := &types.WikiIngestWorkUnit{WorkID: "mapped-work", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "kid",
		SourceRevisionDigest: sourceDigest, SourceDocumentKey: titleKey,
		GenerationContractKey: "old-contract", RuntimeSnapshotKey: "old-runtime",
		State: types.WikiIngestWorkUnitMapped, MappedOutput: types.JSON(mappedJSON)}
	require.NoError(t, db.Create(work).Error)
	binding := types.WikiIngestWorkBinding{WorkID: work.WorkID, SourceRevisionDigest: sourceDigest,
		SourceDocumentKey: titleKey, GenerationContractKey: work.GenerationContractKey, RuntimeSnapshotKey: work.RuntimeSnapshotKey}
	for _, row := range []*types.KnowledgeProcessingSpan{
		{KnowledgeID: "kid", Attempt: 1, SpanID: "root-old", Name: "knowledge_processing", Kind: types.SpanKindRoot, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid", Attempt: 1, SpanID: "post-old", ParentSpanID: "root-old", Name: types.StagePostProcess, Kind: types.SpanKindStage, Status: types.SpanStatusFailed, StartedAt: &now, FinishedAt: &finished},
		{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-old", ParentSpanID: "post-old", Name: "postprocess.wiki", Kind: types.SpanKindSubSpan,
			Status: types.SpanStatusFailed, Input: types.JSONMap{types.WikiIngestWorkBindingInputKey: binding}, StartedAt: &now, FinishedAt: &finished},
	} {
		require.NoError(t, spanRepo.Upsert(context.Background(), row))
	}
	prepared, err := spanRepo.PrepareFailedSpanRetry(context.Background(), types.KnowledgeSpanRetryRequest{
		KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-old", ClientRequestID: "retry-wiki",
	})
	require.NoError(t, err)
	var pending types.TaskPendingOp
	require.NoError(t, db.Where("task_type = ? AND dedup_key = ?", types.TypeWikiIngest, "kid").Take(&pending).Error)

	wikiRepo := apprepo.NewWikiPageRepository(db)
	pageService := NewWikiPageService(wikiRepo, nil, nil, nil, nil)
	tracker := NewSpanTracker(spanRepo, db)
	model := &wikiCheckpointCountingChat{}
	svc := &wikiIngestService{
		wikiService:  pageService,
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: knowledge},
		chunkRepo:    &wikiCheckpointChunkRepo{chunks: chunks},
		spanTracker:  tracker,
	}
	ops, _ := svc.decodePendingRows(context.Background(), []*types.TaskPendingOp{&pending})
	require.Len(t, ops, 1)
	require.Equal(t, prepared.Attempt, ops[0].Attempt)
	require.Equal(t, work.WorkID, ops[0].WorkID)
	result, updates, err := svc.mapOneDocument(context.Background(), model,
		WikiIngestPayload{TenantID: 7, KnowledgeBaseID: "kb"}, ops[0],
		&WikiBatchContext{ExtractionGranularity: types.WikiExtractionStandard})
	require.NoError(t, err)
	require.Equal(t, work.WorkID, result.WorkID)
	require.Len(t, updates, 1)
	require.Zero(t, model.calls.Load())
}

func TestPlanBatchTaxonomyCrashReplayUsesDurableOutputWithoutModelCall(t *testing.T) {
	pageService := &wikiCheckpointPageService{folderPaths: [][]string{{"Existing"}}}
	model := &wikiTaxonomyCountingChat{}
	svc := &wikiIngestService{wikiService: pageService}
	updates := map[string][]SlugUpdate{"entity/a": {{
		Slug: "entity/a", Type: types.WikiPageTypeEntity, WorkID: "bound-work",
		Item: extractedItem{Name: "A", Slug: "entity/a", Description: "About A"},
	}}}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7}

	first, err := svc.planBatchTaxonomy(context.Background(), model, kb, updates, "English")
	require.NoError(t, err)
	require.Equal(t, []string{"People"}, first["entity/a"])
	require.EqualValues(t, 1, model.calls.Load())

	// A fresh attempt can resolve a different runtime model, while the bound
	// work IDs and folder base remain the same.
	driftedModel := &wikiTaxonomyCountingChat{}
	replayed, err := svc.planBatchTaxonomy(context.Background(), driftedModel, kb, updates, "Chinese")
	require.NoError(t, err)
	require.Equal(t, first, replayed)
	require.EqualValues(t, 1, model.calls.Load(), "mapped taxonomy replay must not call the model again")
	require.Zero(t, driftedModel.calls.Load(), "current runtime model drift must not rerun a mapped taxonomy plan")
}

func TestPlanBatchTaxonomyFolderMaterializationDoesNotForkMappedPlan(t *testing.T) {
	pageService := &wikiCheckpointPageService{folderPaths: [][]string{{"Existing"}}}
	firstModel := &wikiTaxonomyCountingChat{}
	svc := &wikiIngestService{wikiService: pageService}
	updates := map[string][]SlugUpdate{"entity/a": {{
		Slug: "entity/a", Type: types.WikiPageTypeEntity, WorkID: "bound-work",
		Item: extractedItem{Name: "A", Slug: "entity/a", Description: "About A"},
	}}}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7}
	first, err := svc.planBatchTaxonomy(context.Background(), firstModel, kb, updates, "English")
	require.NoError(t, err)
	require.EqualValues(t, 1, firstModel.calls.Load())

	// Folder materialization happens after the plan is mapped. A crash before
	// page writes must not turn the planner's own new folder into a new plan.
	pageService.folderPaths = [][]string{{"Existing"}, {"People"}}
	replayModel := &wikiTaxonomyCountingChat{}
	replayed, err := svc.planBatchTaxonomy(context.Background(), replayModel, kb, updates, "English")
	require.NoError(t, err)
	require.Equal(t, first, replayed)
	require.Zero(t, replayModel.calls.Load(), "folder materialization must not fork completed taxonomy generation")
}

func TestPlanBatchTaxonomyPreparedBaseDriftGeneratesOnlyCurrentBase(t *testing.T) {
	workDigest := wikiCheckpointDigest("bound-work")
	missingDigest := wikiCheckpointDigest("entity/a")
	contract := wikiCheckpointDigest(agent.WikiTaxonomyPlanPrompt)
	pageService := &wikiCheckpointPageService{
		folderPaths: [][]string{{"Current"}},
		taxonomyPlan: &types.WikiTaxonomyPlan{
			PlanID: "prepared-old-base", TenantID: 7, KnowledgeBaseID: "kb",
			WorkSetDigest: workDigest, MissingSetDigest: missingDigest,
			FolderBaseDigest: "old-base", ContractKey: contract,
			State: types.WikiTaxonomyPlanPrepared, ResolvedOutput: types.JSON([]byte(`{}`)),
		},
	}
	model := &wikiTaxonomyCountingChat{}
	svc := &wikiIngestService{wikiService: pageService}
	updates := map[string][]SlugUpdate{"entity/a": {{
		Slug: "entity/a", Type: types.WikiPageTypeEntity, WorkID: "bound-work",
		Item: extractedItem{Name: "A", Slug: "entity/a", Description: "About A"},
	}}}
	_, err := svc.planBatchTaxonomy(context.Background(), model, &types.KnowledgeBase{ID: "kb", TenantID: 7}, updates, "English")
	require.NoError(t, err)
	require.EqualValues(t, 1, model.calls.Load(), "only the current folder base may generate")
	require.NotEqual(t, "prepared-old-base", pageService.taxonomyPlan.PlanID)
	require.Equal(t, types.WikiTaxonomyPlanMapped, pageService.taxonomyPlan.State)
	baseJSON, err := json.Marshal([][]string{{"Current"}})
	require.NoError(t, err)
	require.Equal(t, wikiCheckpointDigest(string(baseJSON)), pageService.taxonomyPlan.FolderBaseDigest)
}

func TestPlanBatchTaxonomyChunkFailureDoesNotMapPartialOutputAndRetryMapsOnce(t *testing.T) {
	pageService := &wikiCheckpointPageService{}
	model := &wikiTaxonomySecondChunkFailChat{}
	svc := &wikiIngestService{wikiService: pageService}
	updates := make(map[string][]SlugUpdate, wikiTaxonomyPlanChunkSize+1)
	for i := 0; i <= wikiTaxonomyPlanChunkSize; i++ {
		slug := fmt.Sprintf("entity/%03d", i)
		updates[slug] = []SlugUpdate{{
			Slug: slug, Type: types.WikiPageTypeEntity, WorkID: "bound-work",
			Item: extractedItem{Name: slug, Slug: slug, Description: "about " + slug},
		}}
	}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7}

	_, err := svc.planBatchTaxonomy(context.Background(), model, kb, updates, "English")
	require.ErrorContains(t, err, "taxonomy plan chunk")
	require.NotNil(t, pageService.taxonomyPlan)
	require.Equal(t, types.WikiTaxonomyPlanPrepared, pageService.taxonomyPlan.State)
	require.Zero(t, pageService.taxonomyMappedWrites)
	var partial wikiTaxonomyChunkLedger
	require.NoError(t, json.Unmarshal(pageService.taxonomyPlan.ResolvedOutput, &partial))
	require.Len(t, partial.Chunks, 1, "the successful first chunk must be durable while the plan remains prepared")
	require.EqualValues(t, 2, model.calls.Load())

	result, err := svc.planBatchTaxonomy(context.Background(), model, kb, updates, "English")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, types.WikiTaxonomyPlanMapped, pageService.taxonomyPlan.State)
	require.Equal(t, 1, pageService.taxonomyMappedWrites)
	require.EqualValues(t, 3, model.calls.Load(), "retry must restore the completed first chunk and call only the missing chunk")
}

func TestParseExactTaxonomyChunkAssignmentsRejectsInvalidCoverage(t *testing.T) {
	items := []wikiTaxonomyItem{{slug: "entity/a"}, {slug: "entity/b"}}
	for name, raw := range map[string]string{
		"malformed": `{`,
		"missing":   `{"assignments":[{"slug":"entity/a","path":[]}]}`,
		"extra":     `{"assignments":[{"slug":"entity/a","path":[]},{"slug":"entity/b","path":[]},{"slug":"entity/c","path":[]}]}`,
		"duplicate": `{"assignments":[{"slug":"entity/a","path":[]},{"slug":"entity/a","path":[]},{"slug":"entity/b","path":[]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseExactTaxonomyChunkAssignments(raw, items)
			require.Error(t, err)
		})
	}
	valid, err := parseExactTaxonomyChunkAssignments(
		`{"assignments":[{"slug":"entity/a","path":[]},{"slug":"entity/b","path":[]}]}`, items)
	require.NoError(t, err)
	require.Contains(t, valid, "entity/a")
	require.Empty(t, valid["entity/a"])
}

func TestMixedCompletedAndMissingWikiContributionsOnlyGenerateMissingOnce(t *testing.T) {
	pageService := &wikiCheckpointPageService{markers: []types.WikiSlugContributionMarker{{
		WorkID: "work-complete", Slug: "entity/complete",
		OperationDigest: wikiSlugOperationDigest(SlugUpdate{Slug: "entity/complete", Type: types.WikiPageTypeEntity,
			WorkID: "work-complete", KnowledgeID: "kid", Item: extractedItem{Name: "Complete", Slug: "entity/complete"}}),
		State: types.WikiSlugApplicationPublished,
	}}}
	model := &wikiPageCountingChat{}
	svc := &wikiIngestService{
		wikiService: pageService,
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "kid", KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusFinalizing,
		}},
		spanTracker: &strictWikiAttemptTracker{latest: 1},
	}
	complete := SlugUpdate{Slug: "entity/complete", Type: types.WikiPageTypeEntity,
		WorkID: "work-complete", KnowledgeID: "kid", Item: extractedItem{Name: "Complete", Slug: "entity/complete"}}
	missing := SlugUpdate{Slug: "entity/missing", Type: types.WikiPageTypeEntity,
		WorkID: "work-missing", KnowledgeID: "kid", SourceRef: "kid", DocTitle: "Doc",
		Item: extractedItem{Name: "Missing", Slug: "entity/missing", Description: "Missing description", Details: "Missing details"}}
	filtered, reused, err := svc.filterPublishedWikiContributions(context.Background(), map[string][]SlugUpdate{
		complete.Slug: {complete}, missing.Slug: {missing},
	})
	require.NoError(t, err)
	require.Contains(t, reused["kid"], complete.Slug)
	require.NotContains(t, filtered, complete.Slug)

	batchCtx := &WikiBatchContext{
		ContentInstructions:         "",
		SlugTitleMany:               func(context.Context, []string) map[string]string { return nil },
		SummaryContentByKnowledgeID: func(context.Context, string) string { return "" },
	}
	changed, _, _, err := svc.reduceSlugUpdates(context.Background(), model, "kb", missing.Slug,
		filtered[missing.Slug], 7, batchCtx, nil)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 1, model.calls.Load())
	require.Equal(t, 1, pageService.writes)

	// Simulate a crash after the atomic page+applying-marker transaction but
	// before publish/settlement; replay must reuse the durable generated page.
	changed, _, _, err = svc.reduceSlugUpdates(context.Background(), model, "kb", missing.Slug,
		filtered[missing.Slug], 7, batchCtx, nil)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 1, model.calls.Load(), "completed generation must not repeat after crash")
	require.Equal(t, 1, pageService.writes, "already-applied page must not be written again")
}
