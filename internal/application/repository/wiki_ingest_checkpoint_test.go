package repository

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var wikiCheckpointTestDBSequence atomic.Uint64

func setupWikiCheckpointRepository(t *testing.T) (*wikiPageRepository, *gorm.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:wiki-checkpoint-%d?mode=memory&cache=shared", wikiCheckpointTestDBSequence.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(
		&types.KnowledgeProcessingSpan{}, &types.WikiIngestWorkUnit{},
		&types.WikiTaxonomyPlan{}, &types.WikiSlugApplication{}, &types.WikiSlugContributionMarker{},
	))
	return NewWikiPageRepository(db).(*wikiPageRepository), db
}

func TestWikiCheckpointPostgresConcurrentBindingIsCanonical(t *testing.T) {
	repo, db := setupPostgresWikiPageTestRepo(t)
	require.NoError(t, db.AutoMigrate(
		&types.KnowledgeProcessingSpan{}, &types.WikiIngestWorkUnit{},
		&types.WikiTaxonomyPlan{}, &types.WikiSlugApplication{}, &types.WikiSlugContributionMarker{},
	))
	insertWikiOwnerSpan(t, db, "kid-pg", "wiki-pg", 1, types.JSONMap{})
	binding := types.WikiIngestWorkBinding{KnowledgeID: "kid-pg", Attempt: 1, SpanID: "wiki-pg", SourceRevisionDigest: "source", SourceDocumentKey: "title"}

	results := make(chan *types.WikiIngestWorkUnit, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range []string{"caller-a", "caller-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unit := checkpointUnit("work-canonical", "source", "title", "contract", "runtime")
			unit.KnowledgeID = "kid-pg"
			stored, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding, unit)
			results <- stored
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var canonical string
	for result := range results {
		require.NotNil(t, result)
		if canonical == "" {
			canonical = result.WorkID
		}
		require.Equal(t, canonical, result.WorkID)
	}
	var count int64
	require.NoError(t, db.Model(&types.WikiIngestWorkUnit{}).Where("knowledge_id = ?", "kid-pg").Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestWikiCheckpointPostgresTaxonomyProgressUsesSemanticCAS(t *testing.T) {
	repo, db := setupPostgresWikiPageTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.WikiTaxonomyPlan{}))
	plan := &types.WikiTaxonomyPlan{
		PlanID: "taxonomy-progress-pg", TenantID: 7, KnowledgeBaseID: "kb",
		WorkSetDigest: "work", MissingSetDigest: "missing", FolderBaseDigest: "base",
		ContractKey: "contract", State: types.WikiTaxonomyPlanPrepared, ResolvedOutput: types.JSON([]byte(`{}`)),
	}
	_, err := repo.PrepareWikiTaxonomyPlan(context.Background(), plan)
	require.NoError(t, err)
	first := types.JSON([]byte(`{"chunks":{"0000:a":{"entity/a":["People"],"entity/b":[]}}}`))
	require.NoError(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), plan.PlanID, types.JSON([]byte(`{}`)), first))

	expectedEquivalent := types.JSON([]byte(`{"chunks": {"0000:a": {"entity/b": [], "entity/a": ["People"]}}}`))
	second := types.JSON([]byte(`{"chunks":{"0001:b":{"entity/c":["Places"]},"0000:a":{"entity/b":[],"entity/a":["People"]}}}`))
	require.NoError(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), plan.PlanID, expectedEquivalent, second))

	final := types.JSON([]byte(`{"entity/a":["People"],"entity/b":[],"entity/c":["Places"]}`))
	require.NoError(t, repo.MarkWikiTaxonomyPlanMapped(context.Background(), plan.PlanID, final))
	require.NoError(t, repo.MarkWikiTaxonomyPlanMapped(context.Background(), plan.PlanID,
		types.JSON([]byte(`{"entity/c":["Places"],"entity/b":[],"entity/a":["People"]}`))))
}

func insertWikiOwnerSpan(t *testing.T, db *gorm.DB, kid, spanID string, attempt int, input types.JSONMap) {
	t.Helper()
	require.NoError(t, db.Create(&types.KnowledgeProcessingSpan{
		KnowledgeID: kid, Attempt: attempt, SpanID: spanID, ParentSpanID: "post",
		Name: "postprocess.wiki", Kind: types.SpanKindSubSpan, Status: types.SpanStatusRunning,
		Input: input,
	}).Error)
}

func checkpointUnit(workID, source, title, contract, runtime string) *types.WikiIngestWorkUnit {
	return &types.WikiIngestWorkUnit{
		WorkID: workID, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "kid",
		SourceRevisionDigest: source, SourceDocumentKey: title,
		GenerationContractKey: contract, RuntimeSnapshotKey: runtime,
		State: types.WikiIngestWorkUnitPrepared, MappedOutput: types.JSON([]byte(`{}`)),
	}
}

func TestWikiCheckpointBindingReusesMappedWorkAcrossConfigDrift(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
	binding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source-a", SourceDocumentKey: "title-a"}

	first, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
		checkpointUnit("work-old", "source-a", "title-a", "contract-old", "runtime-old"))
	require.NoError(t, err)
	require.NoError(t, repo.MarkWikiIngestWorkUnitMapped(context.Background(), first.WorkID, types.JSON([]byte(`{"done":true}`))))

	replayed, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
		checkpointUnit("work-new", "source-a", "title-a", "contract-new", "runtime-new"))
	require.NoError(t, err)
	require.Equal(t, "work-old", replayed.WorkID, "current config/model must not fork bound completed work")
	require.Equal(t, types.WikiIngestWorkUnitMapped, replayed.State)
}

func TestWikiCheckpointPreparedBindingRotatesOnContractOrRuntimeDrift(t *testing.T) {
	for _, drift := range []struct {
		name, contract, runtime string
	}{
		{name: "contract", contract: "contract-new", runtime: "runtime-old"},
		{name: "runtime", contract: "contract-old", runtime: "runtime-new"},
	} {
		t.Run(drift.name, func(t *testing.T) {
			repo, db := setupWikiCheckpointRepository(t)
			insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
			binding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source", SourceDocumentKey: "title"}
			first, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
				checkpointUnit("work-old", "source", "title", "contract-old", "runtime-old"))
			require.NoError(t, err)
			require.Equal(t, types.WikiIngestWorkUnitPrepared, first.State)

			current, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
				checkpointUnit("work-new", "source", "title", drift.contract, drift.runtime))
			require.NoError(t, err)
			require.Equal(t, "work-new", current.WorkID)
			var old types.WikiIngestWorkUnit
			require.NoError(t, db.First(&old, "work_id = ?", "work-old").Error)
			require.Equal(t, types.WikiIngestWorkUnitAbandoned, old.State)
			var span types.KnowledgeProcessingSpan
			require.NoError(t, db.First(&span, "span_id = ?", "wiki-1").Error)
			require.Equal(t, "work-new", retryWikiWorkID(span.Input))
		})
	}
}

func TestWikiCheckpointPreparedBindingReusesIdenticalIdentity(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
	binding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source", SourceDocumentKey: "title"}
	first, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
		checkpointUnit("work", "source", "title", "contract", "runtime"))
	require.NoError(t, err)
	second, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
		checkpointUnit("work", "source", "title", "contract", "runtime"))
	require.NoError(t, err)
	require.Equal(t, first.WorkID, second.WorkID)
	require.Equal(t, types.WikiIngestWorkUnitPrepared, second.State)
	var count int64
	require.NoError(t, db.Model(&types.WikiIngestWorkUnit{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestWikiCheckpointBindingRejectsForgedCrossScopeWorkIDWithoutMutation(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	forged := checkpointUnit("forged-work", "source", "title", "contract", "runtime")
	forged.TenantID = 99
	forged.KnowledgeBaseID = "other-kb"
	require.NoError(t, db.Create(forged).Error)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{
		types.WikiIngestWorkBindingInputKey: types.WikiIngestWorkBinding{
			WorkID: "forged-work", SourceRevisionDigest: "source", SourceDocumentKey: "title",
			GenerationContractKey: "contract", RuntimeSnapshotKey: "runtime",
		},
	})
	binding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source", SourceDocumentKey: "title"}
	_, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
		checkpointUnit("candidate", "source", "title", "contract", "runtime"))
	require.ErrorContains(t, err, "bound work unit identity differs")
	var candidateCount int64
	require.NoError(t, db.Model(&types.WikiIngestWorkUnit{}).Where("work_id = ?", "candidate").Count(&candidateCount).Error)
	require.Zero(t, candidateCount)
	var span types.KnowledgeProcessingSpan
	require.NoError(t, db.First(&span, "span_id = ?", "wiki-1").Error)
	require.Equal(t, "forged-work", retryWikiWorkID(span.Input))
}

func TestWikiCheckpointBindingRejectsForgedKnowledgeSourceOrTitle(t *testing.T) {
	for name, mutate := range map[string]func(*types.WikiIngestWorkUnit){
		"knowledge": func(unit *types.WikiIngestWorkUnit) { unit.KnowledgeID = "other-kid" },
		"source":    func(unit *types.WikiIngestWorkUnit) { unit.SourceRevisionDigest = "other-source" },
		"title":     func(unit *types.WikiIngestWorkUnit) { unit.SourceDocumentKey = "other-title" },
	} {
		t.Run(name, func(t *testing.T) {
			repo, db := setupWikiCheckpointRepository(t)
			forged := checkpointUnit("forged-work", "source", "title", "contract", "runtime")
			mutate(forged)
			require.NoError(t, db.Create(forged).Error)
			insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{
				types.WikiIngestWorkBindingInputKey: types.WikiIngestWorkBinding{
					WorkID: "forged-work", SourceRevisionDigest: "source", SourceDocumentKey: "title",
					GenerationContractKey: "contract", RuntimeSnapshotKey: "runtime",
				},
			})
			binding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source", SourceDocumentKey: "title"}
			_, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), binding,
				checkpointUnit("candidate", "source", "title", "contract", "runtime"))
			require.ErrorContains(t, err, "bound work unit identity differs")
			var span types.KnowledgeProcessingSpan
			require.NoError(t, db.First(&span, "span_id = ?", "wiki-1").Error)
			require.Equal(t, "forged-work", retryWikiWorkID(span.Input))
			var candidates int64
			require.NoError(t, db.Model(&types.WikiIngestWorkUnit{}).Where("work_id = ?", "candidate").Count(&candidates).Error)
			require.Zero(t, candidates)
		})
	}
}

func TestWikiCheckpointBindingRejectsTitleDriftAndAbandonsOldUnit(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
	oldBinding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source-a", SourceDocumentKey: "title-a"}
	old, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), oldBinding,
		checkpointUnit("work-old", "source-a", "title-a", "contract", "runtime"))
	require.NoError(t, err)
	require.NoError(t, repo.MarkWikiIngestWorkUnitMapped(context.Background(), old.WorkID, types.JSON([]byte(`{"done":true}`))))

	newBinding := oldBinding
	newBinding.SourceDocumentKey = "title-b"
	fresh, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), newBinding,
		checkpointUnit("work-new", "source-a", "title-b", "contract", "runtime"))
	require.NoError(t, err)
	require.Equal(t, "work-new", fresh.WorkID)
	var stale types.WikiIngestWorkUnit
	require.NoError(t, db.First(&stale, "work_id = ?", "work-old").Error)
	require.Equal(t, types.WikiIngestWorkUnitAbandoned, stale.State)
}

func TestWikiCheckpointBindingDoesNotReuseStaleSourceRevision(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
	oldBinding := types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source-a", SourceDocumentKey: "title"}
	old, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), oldBinding,
		checkpointUnit("work-old", "source-a", "title", "contract", "runtime"))
	require.NoError(t, err)
	require.NoError(t, repo.MarkWikiIngestWorkUnitMapped(context.Background(), old.WorkID, types.JSON([]byte(`{"done":true}`))))

	freshBinding := oldBinding
	freshBinding.SourceRevisionDigest = "source-b"
	fresh, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(), freshBinding,
		checkpointUnit("work-new", "source-b", "title", "contract", "runtime"))
	require.NoError(t, err)
	require.Equal(t, "work-new", fresh.WorkID)
	require.Equal(t, types.WikiIngestWorkUnitPrepared, fresh.State)
}

func TestWikiCheckpointLegacyAmbiguityFailsClosed(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	insertWikiOwnerSpan(t, db, "kid", "wiki-1", 1, types.JSONMap{})
	for _, id := range []string{"legacy-a", "legacy-b"} {
		unit := checkpointUnit(id, "source-a", "title-a", id, id)
		unit.State = types.WikiIngestWorkUnitMapped
		unit.MappedOutput = types.JSON([]byte(`{"done":true}`))
		require.NoError(t, db.Create(unit).Error)
	}
	_, err := repo.PrepareAndBindWikiIngestWorkUnit(context.Background(),
		types.WikiIngestWorkBinding{KnowledgeID: "kid", Attempt: 1, SpanID: "wiki-1", SourceRevisionDigest: "source-a", SourceDocumentKey: "title-a"},
		checkpointUnit("candidate", "source-a", "title-a", "contract", "runtime"))
	require.ErrorContains(t, err, "ambiguous legacy mapped checkpoints")
}

func TestWikiSlugApplicationPageAndMarkersShareTransaction(t *testing.T) {
	db := setupWikiPagesTestDB(t)
	require.NoError(t, db.AutoMigrate(
		&types.WikiIngestWorkUnit{}, &types.WikiTaxonomyPlan{},
		&types.WikiSlugApplication{}, &types.WikiSlugContributionMarker{},
	))
	repo := NewWikiPageRepository(db).(*wikiPageRepository)
	page := makeWikiPage("kb", "entity/atomic", types.WikiPageTypeEntity, types.WikiPageStatusDraft)
	marker := types.WikiSlugContributionMarker{WorkID: "work", Slug: page.Slug, OperationDigest: "operation"}

	forgedCtx := types.WithWikiSlugApplicationTransition(context.Background(), types.WikiSlugApplicationTransition{
		PlanID: "missing-plan", State: types.WikiSlugApplicationApplying, Markers: []types.WikiSlugContributionMarker{marker},
	})
	require.Error(t, repo.CreateWithLinks(forgedCtx, page))
	var pageCount int64
	require.NoError(t, db.Model(&types.WikiPage{}).Where("slug = ?", page.Slug).Count(&pageCount).Error)
	require.Zero(t, pageCount, "checkpoint failure must roll back the page write")

	application := &types.WikiSlugApplication{
		PlanID: "plan", TenantID: 7, KnowledgeBaseID: "kb", Slug: page.Slug,
		ContributionKey: "contribution", ExpectedPageHash: "base", OperationDigest: "operation",
		State: types.WikiSlugApplicationPrepared,
	}
	_, err := repo.PrepareWikiSlugApplication(context.Background(), application)
	require.NoError(t, err)
	require.NoError(t, repo.MarkWikiSlugApplicationApplying(context.Background(), application.PlanID, `{"page":{}}`))
	applyCtx := types.WithWikiSlugApplicationTransition(context.Background(), types.WikiSlugApplicationTransition{
		PlanID: application.PlanID, State: types.WikiSlugApplicationApplying, Markers: []types.WikiSlugContributionMarker{marker},
	})
	require.NoError(t, repo.CreateWithLinks(applyCtx, page))

	var storedMarker types.WikiSlugContributionMarker
	require.NoError(t, db.Where("work_id = ? AND slug = ?", marker.WorkID, marker.Slug).First(&storedMarker).Error)
	require.Equal(t, types.WikiSlugApplicationApplying, storedMarker.State)

	page.Status = types.WikiPageStatusPublished
	publishCtx := types.WithWikiSlugApplicationTransition(context.Background(), types.WikiSlugApplicationTransition{
		PlanID: application.PlanID, State: types.WikiSlugApplicationPublished, Markers: []types.WikiSlugContributionMarker{marker},
	})
	require.NoError(t, repo.UpdateMetaWithLinks(publishCtx, page, page.OutLinks))
	require.NoError(t, db.Where("work_id = ? AND slug = ?", marker.WorkID, marker.Slug).First(&storedMarker).Error)
	require.Equal(t, types.WikiSlugApplicationPublished, storedMarker.State)
	var storedApp types.WikiSlugApplication
	require.NoError(t, db.First(&storedApp, "plan_id = ?", application.PlanID).Error)
	require.Equal(t, types.WikiSlugApplicationPublished, storedApp.State)
}

func TestWikiTaxonomyPlanReusesMappedOutputAcrossFolderBaseDriftAndRejectsIdentityMismatch(t *testing.T) {
	repo, _ := setupWikiCheckpointRepository(t)
	plan := &types.WikiTaxonomyPlan{
		PlanID: "taxonomy", TenantID: 7, KnowledgeBaseID: "kb",
		WorkSetDigest: "work", MissingSetDigest: "missing", FolderBaseDigest: "base-old",
		ContractKey: "contract", State: types.WikiTaxonomyPlanPrepared, ResolvedOutput: types.JSON([]byte(`{}`)),
	}
	prepared, err := repo.PrepareWikiTaxonomyPlan(context.Background(), plan)
	require.NoError(t, err)
	require.NoError(t, repo.MarkWikiTaxonomyPlanMapped(context.Background(), prepared.PlanID,
		types.JSON([]byte(`{"entity/a":["People"]}`))))

	replayed, err := repo.FindMappedWikiTaxonomyPlan(context.Background(), 7, "kb", "work", "missing", "contract")
	require.NoError(t, err)
	require.NotNil(t, replayed)
	require.Equal(t, types.WikiTaxonomyPlanMapped, replayed.State)
	require.Equal(t, "base-old", replayed.FolderBaseDigest, "first folder snapshot remains the audit record")

	forged := *plan
	forged.WorkSetDigest = "different-work"
	_, err = repo.PrepareWikiTaxonomyPlan(context.Background(), &forged)
	require.ErrorContains(t, err, "existing plan identity differs")
}

func TestWikiTaxonomyPreparedBaseDriftSupersedesOldPlan(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	base := types.WikiTaxonomyPlan{TenantID: 7, KnowledgeBaseID: "kb", WorkSetDigest: "work",
		MissingSetDigest: "missing", ContractKey: "contract", State: types.WikiTaxonomyPlanPrepared,
		ResolvedOutput: types.JSON([]byte(`{}`))}
	old := base
	old.PlanID, old.FolderBaseDigest = "plan-a", "base-a"
	_, err := repo.PrepareWikiTaxonomyPlan(context.Background(), &old)
	require.NoError(t, err)
	progress := types.JSON([]byte(`{"chunks":{"0000:a":{"entity/a":["People"]}}}`))
	require.NoError(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), old.PlanID,
		types.JSON([]byte(`{}`)), progress))
	current := base
	current.PlanID, current.FolderBaseDigest = "plan-b", "base-b"
	prepared, err := repo.PrepareWikiTaxonomyPlan(context.Background(), &current)
	require.NoError(t, err)
	require.Equal(t, "plan-b", prepared.PlanID)
	var stale types.WikiTaxonomyPlan
	require.NoError(t, db.First(&stale, "plan_id = ?", "plan-a").Error)
	require.Equal(t, types.WikiTaxonomyPlanAbandoned, stale.State)
	require.Error(t, repo.MarkWikiTaxonomyPlanMapped(context.Background(), "plan-a", types.JSON([]byte(`{"wrong":true}`))))

	revived, err := repo.PrepareWikiTaxonomyPlan(context.Background(), &old)
	require.NoError(t, err)
	require.Equal(t, "plan-a", revived.PlanID)
	require.Equal(t, types.WikiTaxonomyPlanPrepared, revived.State)
	require.JSONEq(t, string(progress), string(revived.ResolvedOutput), "revival must preserve successful chunk checkpoints")
	var superseded types.WikiTaxonomyPlan
	require.NoError(t, db.First(&superseded, "plan_id = ?", "plan-b").Error)
	require.Equal(t, types.WikiTaxonomyPlanAbandoned, superseded.State)
}

func TestFindMappedWikiTaxonomyPlanFailsClosedOnAmbiguity(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	for _, id := range []string{"mapped-a", "mapped-b"} {
		require.NoError(t, db.Create(&types.WikiTaxonomyPlan{
			PlanID: id, TenantID: 7, KnowledgeBaseID: "kb", WorkSetDigest: "work",
			MissingSetDigest: "missing", FolderBaseDigest: id, ContractKey: "contract",
			State: types.WikiTaxonomyPlanMapped, ResolvedOutput: types.JSON([]byte(`{}`)),
		}).Error)
	}
	_, err := repo.FindMappedWikiTaxonomyPlan(context.Background(), 7, "kb", "work", "missing", "contract")
	require.ErrorContains(t, err, "ambiguous mapped checkpoints")
}

func TestWikiTaxonomyProgressCASKeepsPlanPrepared(t *testing.T) {
	repo, db := setupWikiCheckpointRepository(t)
	plan := &types.WikiTaxonomyPlan{PlanID: "progress", TenantID: 7, KnowledgeBaseID: "kb",
		WorkSetDigest: "work", MissingSetDigest: "missing", FolderBaseDigest: "base",
		ContractKey: "contract", State: types.WikiTaxonomyPlanPrepared, ResolvedOutput: types.JSON([]byte(`{}`))}
	_, err := repo.PrepareWikiTaxonomyPlan(context.Background(), plan)
	require.NoError(t, err)
	progress := types.JSON([]byte(`{"chunks":{"0000:digest":{"entity/a":[]}}}`))
	require.NoError(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), plan.PlanID, types.JSON([]byte(`{}`)), progress))
	var stored types.WikiTaxonomyPlan
	require.NoError(t, db.First(&stored, "plan_id = ?", plan.PlanID).Error)
	require.Equal(t, types.WikiTaxonomyPlanPrepared, stored.State)
	require.JSONEq(t, string(progress), string(stored.ResolvedOutput))
	next := types.JSON([]byte(`{"chunks":{"0001:next":{"entity/b":["Places"]},"0000:digest":{"entity/a":[]}}}`))
	require.NoError(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), plan.PlanID,
		types.JSON([]byte(`{"chunks": {"0000:digest": {"entity/a": []}}}`)), next))
	require.ErrorContains(t, repo.SaveWikiTaxonomyPlanProgress(context.Background(), plan.PlanID,
		types.JSON([]byte(`{}`)), types.JSON([]byte(`{"chunks":{}}`))), "changed concurrently")
}
