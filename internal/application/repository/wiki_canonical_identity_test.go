package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func setupWikiCanonicalIdentityTestRepo(t *testing.T) (*wikiPageRepository, context.Context) {
	t.Helper()
	db := setupWikiPagesTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.WikiCanonicalIdentity{}, &types.WikiPageMergeAudit{}))
	repo := NewWikiPageRepository(db).(*wikiPageRepository)
	return repo, context.Background()
}

func TestResolveCanonicalWikiPageSlugsReusesExistingTitleAcrossSlugVariants(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.Create(ctx, &types.WikiPage{
		ID: "existing", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/jin-du-yu-808", Title: "金都玉808",
		PageType: types.WikiPageTypeEntity, Status: types.WikiPageStatusPublished,
		Version: 4, SourceRefs: types.StringArray{"doc-1|来源"}, CreatedAt: now, UpdatedAt: now,
	}))

	resolved, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/jinduyu-808", Title: "金 都-玉 808", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)
	require.Equal(t, "entity/jin-du-yu-808", resolved["entity/jinduyu-808"])

	var identities []types.WikiCanonicalIdentity
	require.NoError(t, repo.db.Find(&identities).Error)
	require.Len(t, identities, 1)
	require.Equal(t, "金都玉808", identities[0].IdentityKey)
}

func TestResolveCanonicalWikiPageSlugsKeepsEntityAndConceptSeparate(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	resolved, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{
		{Slug: "entity/demo", Title: "同名主题", PageType: types.WikiPageTypeEntity},
		{Slug: "concept/demo", Title: "同名主题", PageType: types.WikiPageTypeConcept},
	})
	require.NoError(t, err)
	require.Equal(t, "entity/demo", resolved["entity/demo"])
	require.Equal(t, "concept/demo", resolved["concept/demo"])
}

func TestResolveCanonicalWikiPageSlugsKeepsFirstReservationBeforePageExists(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	first, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/tongyu-609", Title: "同玉609", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)
	require.Equal(t, "entity/tongyu-609", first["entity/tongyu-609"])

	second, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/tong-yu-609", Title: "同 玉-609", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)
	require.Equal(t, "entity/tongyu-609", second["entity/tong-yu-609"])
}

func TestCreateWithLinksRejectsNonCanonicalSlugForRegisteredIdentity(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	_, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/tongyu-609", Title: "同玉609", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)

	err = repo.CreateWithLinks(ctx, &types.WikiPage{
		ID: "wrong", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/tong-yu-609", Title: "同 玉 609", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished,
	})
	require.ErrorIs(t, err, ErrWikiCanonicalConflict)

	require.NoError(t, repo.CreateWithLinks(ctx, &types.WikiPage{
		ID: "right", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/tongyu-609", Title: "同玉609", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished,
	}))
}

func TestReconcileCanonicalWikiPagesArchivesOnlyCoveredDuplicatesAndIsIdempotent(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	now := time.Now()
	pages := []*types.WikiPage{
		{
			ID: "canonical", TenantID: 1, KnowledgeBaseID: "kb-1",
			Slug: "entity/jin-du-yu-808", Title: "金都玉808", PageType: types.WikiPageTypeEntity,
			Status: types.WikiPageStatusPublished, Content: "覆盖了完整来源的较长正文内容。",
			SourceRefs: types.StringArray{"doc-1|来源一", "doc-2|来源二"}, ChunkRefs: types.StringArray{"chunk-1"},
			OutLinks: types.StringArray{"entity/original-link"},
			Version:  5, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
		},
		{
			ID: "covered", TenantID: 1, KnowledgeBaseID: "kb-1",
			Slug: "entity/jinduyu-808", Title: "金 都玉808", PageType: types.WikiPageTypeEntity,
			Status: types.WikiPageStatusPublished, Content: "较短正文。",
			SourceRefs: types.StringArray{"doc-1|来源一"}, ChunkRefs: types.StringArray{"chunk-2"},
			OutLinks: types.StringArray{"entity/merged-link"},
			Version:  1, CreatedAt: now, UpdatedAt: now,
		},
		{
			ID: "uncovered", TenantID: 1, KnowledgeBaseID: "kb-1",
			Slug: "entity/jinduyu808-extra", Title: "金都玉 808", PageType: types.WikiPageTypeEntity,
			Status: types.WikiPageStatusPublished, Content: "来自尚未覆盖来源的正文。",
			SourceRefs: types.StringArray{"doc-3|来源三"}, Version: 2, CreatedAt: now, UpdatedAt: now,
		},
		{
			ID: "referencer", TenantID: 1, KnowledgeBaseID: "kb-1",
			Slug: "summary/doc", Title: "引用页", PageType: types.WikiPageTypeSummary,
			Status:  types.WikiPageStatusPublished,
			Content: "[[entity/jinduyu-808]]、[[entity/jinduyu-808|金都玉808]]、[[entity/jinduyu-808-extra]]",
			Version: 1, CreatedAt: now, UpdatedAt: now,
		},
	}
	for _, page := range pages {
		// Simulate duplicates created before the canonical registry existed.
		require.NoError(t, repo.db.Create(page).Error)
	}

	result, err := repo.ReconcileCanonicalWikiPages(ctx, "kb-1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, result.MergedPages)
	require.Equal(t, 1, result.DeferredPages)

	covered, err := repo.GetBySlug(ctx, "kb-1", "entity/jinduyu-808")
	require.NoError(t, err)
	require.Equal(t, types.WikiPageStatusArchived, covered.Status)
	uncovered, err := repo.GetBySlug(ctx, "kb-1", "entity/jinduyu808-extra")
	require.NoError(t, err)
	require.Equal(t, types.WikiPageStatusPublished, uncovered.Status)
	canonical, err := repo.GetBySlug(ctx, "kb-1", "entity/jin-du-yu-808")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"chunk-1", "chunk-2"}, []string(canonical.ChunkRefs))
	require.ElementsMatch(t, []string{"entity/original-link", "entity/merged-link"}, []string(canonical.OutLinks))
	referencer, err := repo.GetBySlug(ctx, "kb-1", "summary/doc")
	require.NoError(t, err)
	require.Equal(t,
		"[[entity/jin-du-yu-808]]、[[entity/jin-du-yu-808|金都玉808]]、[[entity/jinduyu-808-extra]]",
		referencer.Content,
	)

	// Source-incomplete duplicates are deliberately deferred, not frozen.
	uncovered.Content = "仍在补充的正文。"
	require.NoError(t, repo.Update(ctx, uncovered))

	second, err := repo.ReconcileCanonicalWikiPages(ctx, "kb-1", nil)
	require.NoError(t, err)
	require.Zero(t, second.MergedPages)

	var audits []types.WikiPageMergeAudit
	require.NoError(t, repo.db.Find(&audits).Error)
	require.Len(t, audits, 1)
	require.Equal(t, "entity/jin-du-yu-808", audits[0].CanonicalSlug)
	require.Equal(t, "entity/jinduyu-808", audits[0].MergedSlug)
}

func TestReconcileCanonicalWikiPagesKeepsRegisteredSlugStable(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	now := time.Now()
	canonical := &types.WikiPage{
		ID: "stable", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/stable", Title: "稳定身份", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished, Content: "原 canonical。",
		SourceRefs: types.StringArray{"doc-1|来源"}, Version: 1, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	newer := &types.WikiPage{
		ID: "newer", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/newer", Title: "稳定 身份", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished, Content: "版本更高且内容更长，但不能改变已登记身份地址。",
		SourceRefs: types.StringArray{"doc-1|来源"}, Version: 99, CreatedAt: now, UpdatedAt: now,
	}
	require.NoError(t, repo.db.Create(canonical).Error)
	require.NoError(t, repo.db.Create(newer).Error)
	require.NoError(t, repo.db.Create(&types.WikiCanonicalIdentity{
		TenantID: 1, KnowledgeBaseID: "kb-1", PageType: types.WikiPageTypeEntity,
		IdentityKey: types.NormalizeWikiIdentityTitle(canonical.Title), CanonicalSlug: canonical.Slug,
	}).Error)

	result, err := repo.ReconcileCanonicalWikiPages(ctx, "kb-1", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"entity/newer": "entity/stable"}, result.Aliases)

	var identity types.WikiCanonicalIdentity
	require.NoError(t, repo.db.First(&identity).Error)
	require.Equal(t, "entity/stable", identity.CanonicalSlug)
}

func TestArchiveAndDeleteReleaseCanonicalIdentity(t *testing.T) {
	repo, ctx := setupWikiCanonicalIdentityTestRepo(t)
	page := &types.WikiPage{
		ID: "first", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/first", Title: "可重新生成", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished, Version: 1,
	}
	require.NoError(t, repo.Create(ctx, page))
	page.Status = types.WikiPageStatusArchived
	require.NoError(t, repo.UpdateMeta(ctx, page))

	resolved, err := repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/second", Title: "可重新生成", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)
	require.Equal(t, "entity/second", resolved["entity/second"])

	second := &types.WikiPage{
		ID: "second", TenantID: 1, KnowledgeBaseID: "kb-1",
		Slug: "entity/second", Title: "可重新生成", PageType: types.WikiPageTypeEntity,
		Status: types.WikiPageStatusPublished, Version: 1,
	}
	require.NoError(t, repo.Create(ctx, second))
	require.NoError(t, repo.Delete(ctx, "kb-1", second.Slug))

	resolved, err = repo.ResolveCanonicalWikiPageSlugs(ctx, 1, "kb-1", []types.WikiCanonicalCandidate{{
		Slug: "entity/third", Title: "可重新生成", PageType: types.WikiPageTypeEntity,
	}})
	require.NoError(t, err)
	require.Equal(t, "entity/third", resolved["entity/third"])
}
