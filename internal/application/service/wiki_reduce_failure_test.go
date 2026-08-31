package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingWikiPageChat struct {
	err error
}

func (m *failingWikiPageChat) Chat(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (*types.ChatResponse, error) {
	return nil, m.err
}

func (m *failingWikiPageChat) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return nil, m.err
}

func (m *failingWikiPageChat) GetModelName() string { return "test-model" }
func (m *failingWikiPageChat) GetModelID() string   { return "test-model" }

type existingWikiPageService struct {
	interfaces.WikiPageService
	page *types.WikiPage
}

func (s *existingWikiPageService) GetPageBySlug(
	context.Context,
	string,
	string,
) (*types.WikiPage, error) {
	return s.page, nil
}

func TestReduceSlugUpdatesPropagatesPageGenerationFailure(t *testing.T) {
	wantErr := errors.New("API request failed with status 502: upstream request failed")
	service := &wikiIngestService{
		wikiService: &existingWikiPageService{page: &types.WikiPage{
			ID:              "page-1",
			KnowledgeBaseID: "kb-1",
			Slug:            "concept/alpha",
			Title:           "Alpha",
			Content:         "old content",
			PageType:        types.WikiPageTypeConcept,
		}},
	}
	batchCtx := &WikiBatchContext{
		SlugTitleMany: func(context.Context, []string) map[string]string { return nil },
		SummaryContentByKnowledgeID: func(context.Context, string) string {
			return ""
		},
		PlannedFolderID: map[string]string{},
	}

	changed, _, additionFailed, err := service.reduceSlugUpdates(
		context.Background(),
		&failingWikiPageChat{err: wantErr},
		"kb-1",
		"concept/alpha",
		[]SlugUpdate{{
			Slug:        "concept/alpha",
			Type:        types.WikiPageTypeConcept,
			KnowledgeID: "",
			DocTitle:    "Document",
			Item: extractedItem{
				Name:        "Alpha",
				Description: "new evidence",
			},
		}},
		1,
		batchCtx,
		nil,
	)
	if changed {
		t.Fatal("failed page generation must not report a write")
	}
	if !additionFailed {
		t.Fatal("failed entity page generation must be marked as an addition failure")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("reduceSlugUpdates() error = %v, want wrapped %v", err, wantErr)
	}
}

type wikiReduceKnowledgeService struct {
	interfaces.KnowledgeService
	knowledge *types.Knowledge
	err       error
}

type strictWikiAttemptTracker struct {
	noopSpanTracker
	latest         int
	err            error
	latestSequence []int
	calls          int
}

func (t *strictWikiAttemptTracker) LatestAttempt(_ context.Context, _ string) int {
	return t.latest
}

func (t *strictWikiAttemptTracker) LatestAttemptStrict(_ context.Context, _ string) (int, error) {
	if t.calls < len(t.latestSequence) {
		latest := t.latestSequence[t.calls]
		t.calls++
		return latest, t.err
	}
	return t.latest, t.err
}

func (s *wikiReduceKnowledgeService) GetKnowledgeByIDOnly(
	context.Context,
	string,
) (*types.Knowledge, error) {
	if s.knowledge == nil || s.err != nil {
		return s.knowledge, s.err
	}
	copy := *s.knowledge
	if copy.KnowledgeBaseID == "" {
		copy.KnowledgeBaseID = "kb-1"
	}
	return &copy, nil
}

type wikiReducePageService struct {
	interfaces.WikiPageService
	page          *types.WikiPage
	lookupErr     error
	writeErr      error
	metaErr       error
	cancelWrite   context.CancelFunc
	writes        int
	metaWrites    int
	transition    types.WikiSlugApplicationTransition
	hasTransition bool
}

func (s *wikiReducePageService) UpdatePageMeta(context.Context, *types.WikiPage) error {
	return s.metaErr
}

func (s *wikiReducePageService) GetPageBySlug(
	context.Context,
	string,
	string,
) (*types.WikiPage, error) {
	return s.page, s.lookupErr
}

func (s *wikiReducePageService) CreatePage(
	ctx context.Context,
	page *types.WikiPage,
) (*types.WikiPage, error) {
	return s.persist(ctx, page)
}

func (s *wikiReducePageService) UpdatePage(
	ctx context.Context,
	page *types.WikiPage,
) (*types.WikiPage, error) {
	return s.persist(ctx, page)
}

func (s *wikiReducePageService) CreatePageGuarded(
	ctx context.Context,
	page *types.WikiPage,
	_ []types.WikiSourceAttemptGuard,
) (*types.WikiPage, error) {
	return s.persist(ctx, page)
}

func (s *wikiReducePageService) UpdatePageGuarded(
	ctx context.Context,
	page *types.WikiPage,
	_ []types.WikiSourceAttemptGuard,
) (*types.WikiPage, error) {
	return s.persist(ctx, page)
}

func (s *wikiReducePageService) UpdatePageMetaGuarded(
	ctx context.Context,
	page *types.WikiPage,
	_ []types.WikiSourceAttemptGuard,
) error {
	s.metaWrites++
	s.transition, s.hasTransition = types.WikiSlugApplicationTransitionFromContext(ctx)
	return s.UpdatePageMeta(ctx, page)
}

func (s *wikiReducePageService) persist(
	ctx context.Context,
	page *types.WikiPage,
) (*types.WikiPage, error) {
	if s.cancelWrite != nil {
		s.cancelWrite()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.writeErr != nil {
		return nil, s.writeErr
	}
	s.writes++
	return page, nil
}

func wikiSummaryUpdate(knowledgeID string) []SlugUpdate {
	return []SlugUpdate{{
		Slug:        "summary/" + knowledgeID,
		Type:        "summary",
		KnowledgeID: knowledgeID,
		SourceRef:   knowledgeID,
		DocTitle:    "Document",
		SummaryLine: "A persisted summary",
		SummaryBody: "# Summary\n\nPersisted body.",
	}}
}

func TestReduceSlugUpdatesTreatsKnowledgeLookupFailureAsRetryable(t *testing.T) {
	wantErr := context.DeadlineExceeded
	pages := &wikiReducePageService{}
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{err: wantErr},
		wikiService:  pages,
	}

	changed, _, _, err := service.reduceSlugUpdates(
		context.Background(), nil, "kb-1", "summary/k-1",
		wikiSummaryUpdate("k-1"), 1, &WikiBatchContext{}, nil,
	)

	require.ErrorIs(t, err, wantErr)
	assert.False(t, changed)
	assert.Zero(t, pages.writes, "a transient lookup error must not be treated as deleted or persisted")
}

func TestReduceSlugUpdatesDoesNotCreatePageAfterLookupFailure(t *testing.T) {
	wantErr := errors.New("wiki page lookup timed out")
	pages := &wikiReducePageService{lookupErr: wantErr}
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-1", ParseStatus: types.ParseStatusCompleted,
		}},
		wikiService: pages,
	}

	changed, _, _, err := service.reduceSlugUpdates(
		context.Background(), nil, "kb-1", "summary/k-1",
		wikiSummaryUpdate("k-1"), 1, &WikiBatchContext{}, nil,
	)

	require.ErrorIs(t, err, wantErr)
	assert.False(t, changed)
	assert.Zero(t, pages.writes, "a failed read must not be converted into a create")
}

func TestReduceSlugUpdatesPersistsCompletedSummaryAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pages := &wikiReducePageService{
		page: &types.WikiPage{
			ID:              "page-1",
			KnowledgeBaseID: "kb-1",
			Slug:            "summary/k-1",
		},
		cancelWrite: cancel,
	}
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-1", ParseStatus: types.ParseStatusCompleted,
		}},
		wikiService: pages,
	}

	changed, _, _, err := service.reduceSlugUpdates(
		ctx, nil, "kb-1", "summary/k-1",
		wikiSummaryUpdate("k-1"), 1, &WikiBatchContext{}, nil,
	)

	require.NoError(t, err)
	assert.True(t, changed)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Equal(t, 1, pages.writes, "a completed Wiki result needs its own bounded commit context")
}

func TestReduceSlugUpdatesRejectsCompletedResultFromSupersededAttempt(t *testing.T) {
	pages := &wikiReducePageService{
		page: &types.WikiPage{
			ID:              "page-1",
			KnowledgeBaseID: "kb-1",
			Slug:            "summary/k-1",
		},
	}
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-1", ParseStatus: types.ParseStatusCompleted,
		}},
		wikiService: pages,
		spanTracker: &strictWikiAttemptTracker{latest: 2, latestSequence: []int{1, 2}},
	}
	updates := wikiSummaryUpdate("k-1")
	updates[0].Attempt = 1

	changed, _, _, err := service.reduceSlugUpdates(
		context.Background(), nil, "kb-1", "summary/k-1",
		updates, 1, &WikiBatchContext{}, nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "sources changed")
	assert.False(t, changed)
	assert.Zero(t, pages.writes, "an old attempt must not publish after a newer reparse starts")
}

func TestReduceSlugUpdatesDoesNotReportChangedWhenPersistenceFails(t *testing.T) {
	wantErr := errors.New("database commit failed")
	pages := &wikiReducePageService{
		page: &types.WikiPage{
			ID:              "page-1",
			KnowledgeBaseID: "kb-1",
			Slug:            "summary/k-1",
		},
		writeErr: wantErr,
	}
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-1", ParseStatus: types.ParseStatusCompleted,
		}},
		wikiService: pages,
	}

	changed, _, _, err := service.reduceSlugUpdates(
		context.Background(), nil, "kb-1", "summary/k-1",
		wikiSummaryUpdate("k-1"), 1, &WikiBatchContext{}, nil,
	)

	require.ErrorIs(t, err, wantErr)
	assert.False(t, changed, "failed database persistence must not enter pages_written or finalize")
	assert.Zero(t, pages.writes)
}

func TestWithSlugLockFailsClosedWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	service := &wikiIngestService{redisClient: client}
	called := false

	acquired, err := service.withSlugLock(ctx, "kb-1", "summary/k-1", func() error {
		called = true
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, acquired)
	assert.False(t, called, "a failed distributed lock must never run the write unlocked")
}

func TestWithSlugLockUsesFullTaskLeaseAndDoesNotDeleteSuccessor(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	service := &wikiIngestService{redisClient: client}
	key := wikiSlugLockPrefix + "kb-1:summary/k-1"

	acquired, err := service.withSlugLock(context.Background(), "kb-1", "summary/k-1", func() error {
		assert.GreaterOrEqual(t, server.TTL(key), WikiIngestTaskTimeout+wikiPagePersistTimeout)
		server.Set(key, "successor-owner")
		server.SetTTL(key, time.Minute)
		return nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ownership was lost")
	assert.True(t, acquired)
	value, getErr := server.Get(key)
	require.NoError(t, getErr)
	assert.Equal(t, "successor-owner", value, "owner-safe release must not delete a successor lease")
}

func TestWikiClaimLeaseReachesRenewalAndCoversTheWholeDetachedTail(t *testing.T) {
	detachedTailUpperBound := time.Duration(5+wikiMaxDocsPerBatch)*wikiPagePersistTimeout + wikiIngestCleanupTimeout
	assert.Greater(
		t,
		wikiClaimStaleAfter,
		WikiIngestTaskTimeout+wikiPagePersistTimeout,
		"the original claim must remain owned long enough to reach the pre-tail renewal",
	)
	assert.Greater(
		t,
		wikiClaimStaleAfter,
		detachedTailUpperBound,
		"the renewed claim must cover publish, finalize enqueue, span writes, every document settlement, trim and requeue",
	)
}

func TestIsKnowledgeGoneFailsClosedWhenRedisTombstoneLookupFails(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 5 * time.Millisecond,
		ReadTimeout: 5 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	service := &wikiIngestService{
		redisClient: client,
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-1", ParseStatus: types.ParseStatusCompleted,
		}},
	}

	gone, err := service.isKnowledgeGone(context.Background(), "kb-1", "k-1")

	require.Error(t, err)
	assert.False(t, gone)
}

func TestIsKnowledgeGoneTreatsMoveToAnotherKnowledgeBaseAsGone(t *testing.T) {
	service := &wikiIngestService{
		knowledgeSvc: &wikiReduceKnowledgeService{knowledge: &types.Knowledge{
			ID: "k-moved", KnowledgeBaseID: "kb-new", ParseStatus: types.ParseStatusCompleted,
		}},
	}

	gone, err := service.isKnowledgeGone(context.Background(), "kb-old", "k-moved")

	require.NoError(t, err)
	assert.True(t, gone)
}

func TestPublishDraftPagesReturnsEveryDurabilityFailure(t *testing.T) {
	wantErr := errors.New("metadata write failed")
	pages := &wikiReducePageService{
		page: &types.WikiPage{
			ID: "page-1", KnowledgeBaseID: "kb-1", Slug: "summary/k-1",
			Status: types.WikiPageStatusDraft,
		},
		metaErr: wantErr,
	}
	service := &wikiIngestService{wikiService: pages}

	failed := service.publishDraftPages(
		context.Background(), 1, "kb-1", []string{"summary/k-1"}, nil,
	)

	require.Contains(t, failed, "summary/k-1")
	require.ErrorIs(t, failed["summary/k-1"], wantErr)
}

func TestPublishDraftPagesAcceptsArchivedRetraction(t *testing.T) {
	const slug = "entity/retired"
	pages := &wikiReducePageService{page: &types.WikiPage{
		ID: "page-1", KnowledgeBaseID: "kb-1", Slug: slug,
		Status: types.WikiPageStatusArchived, SourceRefs: types.StringArray{},
	}}
	service := &wikiIngestService{wikiService: pages}

	failed := service.publishDraftPages(
		context.Background(),
		1,
		"kb-1",
		[]string{slug},
		map[string][]SlugUpdate{slug: {{
			Slug: slug, Type: "retractStale", KnowledgeID: "k-1", Attempt: 3,
			WorkID: "work-1", ApplicationPlanID: "plan-1",
		}}},
	)

	require.Empty(t, failed)
	require.Equal(t, 1, pages.metaWrites)
	require.True(t, pages.hasTransition)
	require.Equal(t, "plan-1", pages.transition.PlanID)
	require.Equal(t, types.WikiSlugApplicationPublished, pages.transition.State)
}

func TestPublishDraftPagesRejectsArchivedAddition(t *testing.T) {
	const slug = "entity/unexpected"
	pages := &wikiReducePageService{page: &types.WikiPage{
		ID: "page-1", KnowledgeBaseID: "kb-1", Slug: slug,
		Status: types.WikiPageStatusArchived, SourceRefs: types.StringArray{},
	}}
	service := &wikiIngestService{wikiService: pages}

	failed := service.publishDraftPages(
		context.Background(),
		1,
		"kb-1",
		[]string{slug},
		map[string][]SlugUpdate{slug: {{
			Slug: slug, Type: types.WikiPageTypeEntity, KnowledgeID: "k-1", Attempt: 3,
		}}},
	)

	require.Contains(t, failed, slug)
	require.Contains(t, failed[slug].Error(), "status changed to archived")
}

func TestWikiPageWriteOutcomeCountsOnlyPersistedSlugs(t *testing.T) {
	result := &docIngestResult{
		Pages: []wikiIngestPageRef{
			{Slug: "summary/k-1", Title: "Summary"},
			{Slug: "entity/alpha", Title: "Alpha"},
		},
		MapStats: types.JSONMap{"pages_written": 99, "candidate_slugs": 2},
	}
	out := wikiPageWriteOutcome(
		result,
		map[string]struct{}{"summary/k-1": {}},
		map[string]struct{}{"entity/alpha": {}},
	)

	assert.EqualValues(t, 1, out["pages_written"])
	assert.EqualValues(t, 1, out["pages_dropped"])
	assert.EqualValues(t, 1, out["failed_slug_writes"])
	assert.Len(t, out["pages_written_preview"], 1)
}

type wikiSettlementPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	rows          []*types.TaskPendingOp
	deleteErr     error
	failCount     int
	deletedIDs    []int64
	releasedIDs   []int64
	incrementedID []int64
	order         *[]string
}

func (r *wikiSettlementPendingRepo) PeekBatch(
	context.Context, string, string, string, int,
) ([]*types.TaskPendingOp, error) {
	return r.rows, nil
}

func (r *wikiSettlementPendingRepo) DeleteByIDs(_ context.Context, ids []int64) error {
	if r.order != nil {
		*r.order = append(*r.order, "delete")
	}
	r.deletedIDs = append(r.deletedIDs, ids...)
	return r.deleteErr
}

func (r *wikiSettlementPendingRepo) IncrFailCount(_ context.Context, id int64) (int, error) {
	if r.order != nil {
		*r.order = append(*r.order, "increment")
	}
	r.incrementedID = append(r.incrementedID, id)
	return r.failCount, nil
}

func (r *wikiSettlementPendingRepo) ReleaseByIDs(_ context.Context, ids []int64) error {
	if r.order != nil {
		*r.order = append(*r.order, "release")
	}
	r.releasedIDs = append(r.releasedIDs, ids...)
	return nil
}

func (r *wikiSettlementPendingRepo) PendingCount(context.Context, string, string, string) (int64, error) {
	return int64(len(r.rows)), nil
}

type wikiSettlementSpanTracker struct {
	noopSpanTracker
	latest     map[string]int
	settleErr  error
	order      *[]string
	settled    []string
	deadLetter []*types.TaskDeadLetter
}

func (t *wikiSettlementSpanTracker) LatestAttemptStrict(_ context.Context, knowledgeID string) (int, error) {
	return t.latest[knowledgeID], nil
}

func (t *wikiSettlementSpanTracker) SettleWikiPendingOpStrict(
	_ context.Context,
	knowledgeID string,
	_ int,
	_ []int64,
	deadLetter *types.TaskDeadLetter,
	_ *types.TaskClaimOwner,
) error {
	if t.order != nil {
		*t.order = append(*t.order, "settle:"+knowledgeID)
	}
	if t.settleErr != nil {
		return t.settleErr
	}
	t.settled = append(t.settled, knowledgeID)
	if deadLetter != nil {
		copy := *deadLetter
		t.deadLetter = append(t.deadLetter, &copy)
	}
	return nil
}

func TestSettleWikiIngestRowsFinalizesOnlyAfterSuccessfulTrim(t *testing.T) {
	wantErr := errors.New("delete failed")
	for _, tc := range []struct {
		name        string
		settleErr   error
		wantErr     error
		wantOrder   []string
		wantSettled []string
	}{
		{
			name:        "successful settlement atomically acknowledges rows",
			wantOrder:   []string{"settle:k-1", "settle:k-2"},
			wantSettled: []string{"k-1", "k-2"},
		},
		{
			name:      "failed settlement never acknowledges durable rows",
			settleErr: wantErr,
			wantErr:   wantErr,
			wantOrder: []string{"settle:k-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			pending := &wikiSettlementPendingRepo{order: &order}
			tracker := &wikiSettlementSpanTracker{
				latest: map[string]int{"k-1": 1, "k-2": 2}, settleErr: tc.settleErr, order: &order,
			}
			svc := &wikiIngestService{pendingRepo: pending, spanTracker: tracker}

			err := svc.settleWikiIngestRows(
				context.Background(), WikiIngestPayload{}, []int64{11, 12}, nil,
				nil,
				[]WikiPendingOp{
					{KnowledgeID: "k-1", Attempt: 1, dbID: 11, dbIDs: []int64{11}},
					{KnowledgeID: "k-2", Attempt: 2, dbID: 12, dbIDs: []int64{12}},
					{KnowledgeID: "k-1", Attempt: 1, dbID: 11, dbIDs: []int64{11}},
				},
				nil,
			)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOrder, order)
			require.Equal(t, tc.wantSettled, tracker.settled)
		})
	}
}

func TestSettleWikiIngestRowsDefersSlugContentionWithoutChargingFailureBudget(t *testing.T) {
	var order []string
	pending := &wikiSettlementPendingRepo{order: &order, failCount: wikiMaxFailRetries + 1}
	svc := &wikiIngestService{pendingRepo: pending, spanTracker: &wikiSettlementSpanTracker{}}

	err := svc.settleWikiIngestRows(
		context.Background(), WikiIngestPayload{}, nil, nil,
		[]WikiPendingOp{{KnowledgeID: "k-contended", dbID: 31}},
		nil, nil,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"release"}, order)
	assert.Equal(t, []int64{31}, pending.releasedIDs)
	assert.Empty(t, pending.incrementedID, "healthy slug contention must preserve fail_count")
	assert.Empty(t, pending.deletedIDs)
}

func TestSettleWikiIngestRowsDeletesSupersededOpWithoutDrainingLatestAttempt(t *testing.T) {
	var order []string
	pending := &wikiSettlementPendingRepo{order: &order}
	tracker := &wikiSettlementSpanTracker{latest: map[string]int{"k-stale": 2}, order: &order}
	svc := &wikiIngestService{pendingRepo: pending, spanTracker: tracker}

	err := svc.settleWikiIngestRows(
		context.Background(), WikiIngestPayload{}, []int64{41}, nil,
		nil,
		[]WikiPendingOp{{KnowledgeID: "k-stale", Attempt: 1, dbID: 41, dbIDs: []int64{41}}},
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"delete"}, order)
	assert.Empty(t, tracker.settled)
}

func TestSettleWikiIngestRowsAcknowledgesSuccessfulMaintenanceOpWithoutParseTree(t *testing.T) {
	var order []string
	pending := &wikiSettlementPendingRepo{order: &order}
	tracker := &wikiSettlementSpanTracker{order: &order}
	svc := &wikiIngestService{pendingRepo: pending, spanTracker: tracker}

	err := svc.settleWikiIngestRows(
		context.Background(), WikiIngestPayload{}, []int64{45}, nil,
		nil,
		[]WikiPendingOp{{KnowledgeID: "k-maintenance", Attempt: 0, dbID: 45, dbIDs: []int64{45}}},
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"delete"}, order)
	assert.Empty(t, tracker.settled, "maintenance ingest must not invent a parse-tree settlement")
}

func TestSettleWikiIngestRowsKeepsDurableRowWhenWikiSpanIsNotTerminal(t *testing.T) {
	var order []string
	pending := &wikiSettlementPendingRepo{order: &order}
	tracker, _ := setupSpanTrackerTest(t)
	root, attempt, err := tracker.OpenAttempt(context.Background(), "k-running-wiki", "")
	require.NoError(t, err)
	post := tracker.BeginStage(context.Background(), root.KnowledgeID, attempt, types.StagePostProcess, nil)
	require.NotNil(t, post)
	wiki := tracker.BeginSubSpan(context.Background(), post, "postprocess.wiki", types.SpanKindSubSpan, nil)
	require.NotNil(t, wiki)
	const pendingRowID int64 = 51

	svc := &wikiIngestService{
		pendingRepo: pending, spanTracker: tracker,
	}
	err = svc.settleWikiIngestRows(
		context.Background(), WikiIngestPayload{}, []int64{pendingRowID}, nil,
		nil,
		[]WikiPendingOp{{KnowledgeID: root.KnowledgeID, Attempt: attempt, dbID: pendingRowID, dbIDs: []int64{pendingRowID}}},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "postprocess.wiki")
	assert.NotContains(t, order, "delete", "a running Wiki owner must retain its durable row")
}

func TestRequeueFailedOpsFinalizesBeforeAcknowledgingDeadLetter(t *testing.T) {
	wantErr := errors.New("settlement failed")
	for _, tc := range []struct {
		name        string
		settleErr   error
		wantOrder   []string
		wantSettled []string
	}{
		{
			name:        "terminal row settles and archives in one transaction",
			wantOrder:   []string{"increment", "settle:k-1"},
			wantSettled: []string{"k-1"},
		},
		{
			name:      "atomic archive failure keeps row recoverable",
			settleErr: wantErr,
			wantOrder: []string{"increment", "settle:k-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			pending := &wikiSettlementPendingRepo{
				failCount: wikiMaxFailRetries + 1,
				order:     &order,
			}
			tracker := &wikiSettlementSpanTracker{
				latest: map[string]int{"k-1": 1}, settleErr: tc.settleErr, order: &order,
			}
			svc := &wikiIngestService{
				pendingRepo: pending,
				spanTracker: tracker,
			}

			err := svc.requeueFailedOps(context.Background(), WikiIngestPayload{}, []WikiPendingOp{{
				dbID: 31, dbIDs: []int64{31}, Op: WikiOpIngest, KnowledgeID: "k-1", Attempt: 1,
			}})
			if tc.settleErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantOrder, order)
			require.Equal(t, tc.wantSettled, tracker.settled)
			if tc.settleErr == nil {
				require.Len(t, tracker.deadLetter, 1)
				assert.Equal(t, "k-1", tracker.deadLetter[0].RelatedID)
			}
		})
	}
}

type wikiIndexFailureService struct {
	interfaces.WikiPageService
	index       *types.WikiPage
	updateCalls int
}

func (s *wikiIndexFailureService) GetIndex(context.Context, string) (*types.WikiPage, error) {
	return s.index, nil
}

func (s *wikiIndexFailureService) UpdatePage(
	context.Context, *types.WikiPage,
) (*types.WikiPage, error) {
	s.updateCalls++
	return s.index, nil
}

type wikiIndexFailureKBService struct {
	interfaces.KnowledgeBaseService
}

func (wikiIndexFailureKBService) GetKnowledgeBaseByIDOnly(
	_ context.Context, id string,
) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{
		ID:             id,
		SummaryModelID: "summary-model",
		IndexingStrategy: types.IndexingStrategy{
			WikiEnabled: true,
		},
	}, nil
}

type wikiIndexFailureModelService struct {
	interfaces.ModelService
	model chat.Chat
}

func (s wikiIndexFailureModelService) GetChatModel(context.Context, string) (chat.Chat, error) {
	return s.model, nil
}

func TestRebuildIndexPagePropagatesLLMFailure(t *testing.T) {
	wantErr := errors.New("upstream index request failed")
	wikiSvc := &wikiIndexFailureService{index: &types.WikiPage{
		ID: "index-1", KnowledgeBaseID: "kb-1", Slug: types.WikiPageTypeIndex,
		Content: "Existing intro",
	}}
	svc := &wikiIngestService{wikiService: wikiSvc}

	err := svc.rebuildIndexPage(
		context.Background(), &failingWikiPageChat{err: wantErr},
		WikiIngestPayload{KnowledgeBaseID: "kb-1"},
		"<document_added><title>Doc</title></document_added>", "Chinese", "",
	)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, wikiSvc.updateCalls, "failed index generation must not be written as success")
}

func TestProcessWikiFinalizeKeepsChangeRowsWhenIndexRebuildFails(t *testing.T) {
	wantErr := errors.New("upstream index request failed")
	changePayload, err := json.Marshal(wikiFinalizeRow{Change: &wikiFinalizeChange{
		Action: wikiFinalizeAdded, DocTitle: "Doc", DocSummary: "Summary",
	}})
	require.NoError(t, err)
	taskPayload, err := json.Marshal(WikiIngestPayload{TenantID: 1, KnowledgeBaseID: "kb-1"})
	require.NoError(t, err)
	pending := &wikiSettlementPendingRepo{
		rows:      []*types.TaskPendingOp{{ID: 41, Op: wikiFinalizeOpChange, Payload: changePayload}},
		failCount: 1,
	}
	wikiSvc := &wikiIndexFailureService{index: &types.WikiPage{
		ID: "index-1", KnowledgeBaseID: "kb-1", Slug: types.WikiPageTypeIndex,
		Content: "Existing intro",
	}}
	svc := &wikiIngestService{
		pendingRepo: pending,
		kbService:   wikiIndexFailureKBService{},
		modelService: wikiIndexFailureModelService{
			model: &failingWikiPageChat{err: wantErr},
		},
		wikiService: wikiSvc,
	}

	err = svc.ProcessWikiFinalize(context.Background(), asynq.NewTask(types.TypeWikiFinalize, taskPayload))
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, pending.deletedIDs, "failed finalize rows must remain queued")
	require.Equal(t, []int64{41}, pending.incrementedID)
	require.Zero(t, wikiSvc.updateCalls)
}
