package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrWikiPageNotFound is returned when a wiki page is not found
var ErrWikiPageNotFound = errors.New("wiki page not found")

// ErrWikiPageConflict is returned when an optimistic lock conflict is detected
var ErrWikiPageConflict = errors.New("wiki page version conflict")

// ErrWikiCanonicalConflict prevents any write path from materializing a
// second slug for a registered exact Wiki identity.
var ErrWikiCanonicalConflict = errors.New("wiki page conflicts with canonical identity")

// wikiPageRepository implements the WikiPageRepository interface
type wikiPageRepository struct {
	db                                *gorm.DB
	canonicalIdentityEnabled          bool
	testAfterWikiLinkSourceSerialized func()
}

// NewWikiPageRepository creates a new wiki page repository
func NewWikiPageRepository(db *gorm.DB) interfaces.WikiPageRepository {
	return &wikiPageRepository{
		db: db, canonicalIdentityEnabled: db != nil && db.Migrator().HasTable(&types.WikiCanonicalIdentity{}),
	}
}

func (r *wikiPageRepository) wikiCategoryRankOrder() string {
	if r.db != nil && r.db.Dialector != nil && r.db.Dialector.Name() == "sqlite" {
		return "CASE WHEN COALESCE(json_array_length(category_path), 0) > 0 THEN 0 ELSE 1 END ASC"
	}
	return "CASE WHEN COALESCE(jsonb_array_length(category_path), 0) > 0 THEN 0 ELSE 1 END ASC"
}

func (r *wikiPageRepository) wikiEmptyInLinksPredicate() string {
	if r.db != nil && r.db.Dialector != nil && r.db.Dialector.Name() == "sqlite" {
		return "(in_links IS NULL OR json_array_length(in_links) = 0)"
	}
	return "(in_links IS NULL OR in_links = '[]'::JSONB)"
}

// Create inserts a new wiki page record
func (r *wikiPageRepository) Create(ctx context.Context, page *types.WikiPage) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := validateWikiCanonicalCreate(tx, page, r.canonicalIdentityEnabled); err != nil {
			return err
		}
		return tx.Create(page).Error
	})
}

// CreateWithLinks inserts a page and applies its outbound-link contribution
// to every existing target page in one transaction.
func (r *wikiPageRepository) CreateWithLinks(ctx context.Context, page *types.WikiPage) error {
	return r.withWikiLinkTransaction(ctx, page.KnowledgeBaseID, page.Slug, nil, page.OutLinks,
		func(tx *gorm.DB, _ *types.WikiPage) error {
			if err := validateWikiCanonicalCreate(tx, page, r.canonicalIdentityEnabled); err != nil {
				return err
			}
			return tx.Create(page).Error
		})
}

func validateWikiCanonicalCreate(tx *gorm.DB, page *types.WikiPage, enabled bool) error {
	return validateWikiCanonicalWrite(tx, page, enabled)
}

func validateWikiCanonicalWrite(tx *gorm.DB, page *types.WikiPage, enabled bool) error {
	if page == nil || (page.PageType != types.WikiPageTypeEntity && page.PageType != types.WikiPageTypeConcept) {
		return nil
	}
	identityKey := types.NormalizeWikiIdentityTitle(page.Title)
	if identityKey == "" || !enabled {
		return nil
	}
	if page.ID != "" {
		var stored types.WikiPage
		if err := tx.Select("id, knowledge_base_id, slug, title, page_type, status").Where("id = ?", page.ID).First(&stored).Error; err == nil {
			oldKey := types.NormalizeWikiIdentityTitle(stored.Title)
			if page.Status == types.WikiPageStatusArchived {
				return tx.Where(
					"knowledge_base_id = ? AND page_type = ? AND identity_key = ? AND canonical_slug = ?",
					stored.KnowledgeBaseID, stored.PageType, oldKey, stored.Slug,
				).Delete(&types.WikiCanonicalIdentity{}).Error
			}
			// Historical duplicates that are not yet safe to merge remain
			// editable. An unchanged identity does not create any new
			// ambiguity, so only identity-changing updates enter the registry
			// gate below.
			if stored.Status != types.WikiPageStatusArchived && stored.Slug == page.Slug &&
				stored.PageType == page.PageType && oldKey == identityKey {
				return nil
			}
			if oldKey != "" && (oldKey != identityKey || stored.PageType != page.PageType) {
				if err := tx.Where(
					"knowledge_base_id = ? AND page_type = ? AND identity_key = ? AND canonical_slug = ?",
					stored.KnowledgeBaseID, stored.PageType, oldKey, stored.Slug,
				).Delete(&types.WikiCanonicalIdentity{}).Error; err != nil {
					return err
				}
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	var identity types.WikiCanonicalIdentity
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("knowledge_base_id = ? AND page_type = ? AND identity_key = ?",
			page.KnowledgeBaseID, page.PageType, identityKey).
		First(&identity).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		identity = types.WikiCanonicalIdentity{
			TenantID: page.TenantID, KnowledgeBaseID: page.KnowledgeBaseID,
			PageType: page.PageType, IdentityKey: identityKey, CanonicalSlug: page.Slug,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "knowledge_base_id"}, {Name: "page_type"}, {Name: "identity_key"}},
			DoNothing: true,
		}).Create(&identity).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("knowledge_base_id = ? AND page_type = ? AND identity_key = ?",
				page.KnowledgeBaseID, page.PageType, identityKey).
			First(&identity).Error; err != nil {
			return err
		}
		err = nil
	}
	if err != nil {
		return err
	}
	if identity.CanonicalSlug != page.Slug {
		return fmt.Errorf("%w: %s must use %s", ErrWikiCanonicalConflict, page.Slug, identity.CanonicalSlug)
	}
	return nil
}

// Update updates an existing wiki page record with optimistic locking.
// Increments version — use only for content changes visible to the user.
// The caller must set page.Version to the expected current version.
//
// The write goes through an explicit column map (not GORM's struct Updates)
// so cleared fields persist: struct Updates skips zero values, which used to
// make "empty the summary" silently not stick while the follow-up UpdateMeta
// map call *did* write empty status — one inconsistent half-update. The map
// covers every column UpdatePage mutates, so no UpdateMeta chaser is needed.
func (r *wikiPageRepository) Update(ctx context.Context, page *types.WikiPage) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := validateWikiCanonicalWrite(tx, page, r.canonicalIdentityEnabled); err != nil {
			return err
		}
		return updateWikiPageRow(tx, page)
	})
}

// UpdateWithRevision snapshots the version being superseded and applies the
// update atomically. Doing both in one transaction is what makes the history
// trustworthy: a failed update can no longer leave behind a snapshot of a
// version that is still current, which would show up twice in the history
// and be un-revertable.
func (r *wikiPageRepository) UpdateWithRevision(
	ctx context.Context, page *types.WikiPage, rev *types.WikiPageRevision,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := validateWikiCanonicalWrite(tx, page, r.canonicalIdentityEnabled); err != nil {
			return err
		}
		return updateWikiPageWithRevisionRow(tx, page, rev)
	})
}

// UpdateWithRevisionAndLinks snapshots the superseded revision, updates the
// source page, and applies the outbound-link diff atomically.
func (r *wikiPageRepository) UpdateWithRevisionAndLinks(
	ctx context.Context,
	page *types.WikiPage,
	rev *types.WikiPageRevision,
	oldOutLinks types.StringArray,
) error {
	expectedVersion := page.Version
	err := r.withWikiLinkTransaction(
		ctx, page.KnowledgeBaseID, page.Slug, oldOutLinks, page.OutLinks,
		func(tx *gorm.DB, lockedSource *types.WikiPage) error {
			if lockedSource != nil {
				page.InLinks = append(types.StringArray(nil), lockedSource.InLinks...)
			}
			if err := validateWikiCanonicalWrite(tx, page, r.canonicalIdentityEnabled); err != nil {
				return err
			}
			return updateWikiPageWithRevisionRow(tx, page, rev)
		},
	)
	if err != nil {
		page.Version = expectedVersion
	}
	return err
}

func updateWikiPageWithRevisionRow(tx *gorm.DB, page *types.WikiPage, rev *types.WikiPageRevision) error {
	if rev != nil {
		// An already-present (page_id, version) pair means a concurrent
		// writer snapshotted the same version first; its copy is identical.
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "page_id"}, {Name: "version"}},
			DoNothing: true,
		}).Create(rev).Error; err != nil {
			return err
		}
	}
	return updateWikiPageRow(tx, page)
}

// updateWikiPageRow performs the versioned page write on the given handle
// (plain connection or transaction). On failure page.Version is restored so
// the caller never observes a bumped version for a write that did not land.
func updateWikiPageRow(db *gorm.DB, page *types.WikiPage) error {
	expectedVersion := page.Version
	page.Version = expectedVersion + 1

	result := db.
		Model(page).
		Where("id = ? AND version = ?", page.ID, expectedVersion).
		Updates(map[string]interface{}{
			"title":            page.Title,
			"content":          page.Content,
			"summary":          page.Summary,
			"page_type":        page.PageType,
			"status":           page.Status,
			"aliases":          page.Aliases,
			"out_links":        page.OutLinks,
			"source_refs":      page.SourceRefs,
			"chunk_refs":       page.ChunkRefs,
			"page_metadata":    page.PageMetadata,
			"parent_slug":      page.ParentSlug,
			"folder_id":        page.FolderID,
			"category_path":    page.CategoryPath,
			"wiki_path":        page.WikiPath,
			"depth":            page.Depth,
			"sort_order":       page.SortOrder,
			"last_edit_source": page.LastEditSource,
			"last_editor_id":   page.LastEditorID,
			"version":          page.Version,
			"updated_at":       page.UpdatedAt,
		})
	if result.Error != nil {
		page.Version = expectedVersion
		return result.Error
	}
	if result.RowsAffected == 0 {
		page.Version = expectedVersion
		// Could be not found or version conflict — check which
		var count int64
		db.Model(&types.WikiPage{}).Where("id = ?", page.ID).Count(&count)
		if count == 0 {
			return ErrWikiPageNotFound
		}
		return ErrWikiPageConflict
	}
	return nil
}

// wikiRevisionListColumns is the projection for revision listings — every
// column except the potentially multi-hundred-KB content body.
const wikiRevisionListColumns = "id, tenant_id, knowledge_base_id, page_id, slug, version, " +
	"title, page_type, status, summary, aliases, edit_source, editor_id, edited_at, created_at"

// ListRevisions returns snapshots for a page newest-first, content omitted.
func (r *wikiPageRepository) ListRevisions(
	ctx context.Context, kbID string, pageID string, limit int, offset int,
) ([]*types.WikiPageRevision, int64, error) {
	base := r.db.WithContext(ctx).Model(&types.WikiPageRevision{}).
		Where("knowledge_base_id = ? AND page_id = ?", kbID, pageID)

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var revs []*types.WikiPageRevision
	if err := base.
		Select(wikiRevisionListColumns).
		Order("version DESC").
		Limit(limit).
		Offset(offset).
		Find(&revs).Error; err != nil {
		return nil, 0, err
	}
	return revs, total, nil
}

// GetRevision returns one snapshot with content.
func (r *wikiPageRepository) GetRevision(
	ctx context.Context, kbID string, pageID string, version int,
) (*types.WikiPageRevision, error) {
	var rev types.WikiPageRevision
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND page_id = ? AND version = ?", kbID, pageID, version).
		First(&rev).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWikiPageNotFound
		}
		return nil, err
	}
	return &rev, nil
}

// PruneRevisions applies the two-tier retention described by req: the soft
// cap only touches snapshots whose author is listed as prunable, the hard cap
// applies to everything.
func (r *wikiPageRepository) PruneRevisions(ctx context.Context, req types.WikiRevisionPruneRequest) error {
	if req.PageID == "" {
		return nil
	}
	db := r.db.WithContext(ctx)
	if req.KeepFromVersion > 0 && len(req.PrunableSources) > 0 {
		if err := db.
			Where("page_id = ? AND version < ? AND edit_source IN ?",
				req.PageID, req.KeepFromVersion, req.PrunableSources).
			Delete(&types.WikiPageRevision{}).Error; err != nil {
			return err
		}
	}
	if req.HardKeepFromVersion > 0 {
		if err := db.
			Where("page_id = ? AND version < ?", req.PageID, req.HardKeepFromVersion).
			Delete(&types.WikiPageRevision{}).Error; err != nil {
			return err
		}
	}
	return nil
}

// DeleteRevisionsByPage hard-deletes a page's whole snapshot history. Pages
// themselves are soft-deleted, but a soft-deleted page is unreachable through
// every read path, so its snapshots would be dead weight — and they are the
// bulkiest rows the wiki stores.
func (r *wikiPageRepository) DeleteRevisionsByPage(ctx context.Context, pageID string) error {
	if pageID == "" {
		return nil
	}
	return r.db.WithContext(ctx).
		Where("page_id = ?", pageID).
		Delete(&types.WikiPageRevision{}).Error
}

// UpdateAutoLinkedContent persists content changes produced by the automatic
// link decorators (cross-link injection, dead-link cleanup) without bumping
// `version`. These passes rewrite the same revision with wiki-link markup
// added or removed; treating them as real edits would make newly-ingested
// pages appear as v2 on first view and confuse users who expect `version` to
// correspond to the number of intentional revisions.
func (r *wikiPageRepository) UpdateAutoLinkedContent(ctx context.Context, page *types.WikiPage) error {
	return updateWikiAutoLinkedContentRow(r.db.WithContext(ctx), page)
}

// UpdateAutoLinkedContentWithLinks rewrites machine-decorated content and its
// bidirectional link diff without exposing a partially-updated graph.
func (r *wikiPageRepository) UpdateAutoLinkedContentWithLinks(
	ctx context.Context, page *types.WikiPage, oldOutLinks types.StringArray,
) error {
	return r.withWikiLinkTransaction(
		ctx, page.KnowledgeBaseID, page.Slug, oldOutLinks, page.OutLinks,
		func(tx *gorm.DB, lockedSource *types.WikiPage) error {
			if lockedSource != nil {
				page.InLinks = append(types.StringArray(nil), lockedSource.InLinks...)
			}
			return updateWikiAutoLinkedContentRow(tx, page)
		},
	)
}

func updateWikiAutoLinkedContentRow(db *gorm.DB, page *types.WikiPage) error {
	result := db.
		Model(page).
		Where("id = ?", page.ID).
		Updates(map[string]interface{}{
			"content":    page.Content,
			"out_links":  page.OutLinks,
			"updated_at": page.UpdatedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWikiPageNotFound
	}
	return nil
}

// UpdateMeta updates bookkeeping / provenance fields WITHOUT incrementing the
// version number. "Content" for versioning purposes is the user-visible page
// body (title/content/summary/page_type/status); everything else — links,
// source refs, chunk refs, page_metadata — is considered bookkeeping and is
// refreshed here so the version counter only advances on real edits.
//
// Used by link maintenance, re-ingest (same-content case), and status changes.
func (r *wikiPageRepository) UpdateMeta(ctx context.Context, page *types.WikiPage) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := validateWikiCanonicalWrite(tx, page, r.canonicalIdentityEnabled); err != nil {
			return err
		}
		return updateWikiPageMetaRow(tx, page)
	})
}

// UpdateMetaWithLinks persists bookkeeping fields and the link diff in one
// transaction. It covers stale stored out_links even when content is unchanged.
func (r *wikiPageRepository) UpdateMetaWithLinks(
	ctx context.Context, page *types.WikiPage, oldOutLinks types.StringArray,
) error {
	return r.withWikiLinkTransaction(
		ctx, page.KnowledgeBaseID, page.Slug, oldOutLinks, page.OutLinks,
		func(tx *gorm.DB, lockedSource *types.WikiPage) error {
			if lockedSource != nil {
				page.InLinks = append(types.StringArray(nil), lockedSource.InLinks...)
			}
			if err := validateWikiCanonicalWrite(tx, page, r.canonicalIdentityEnabled); err != nil {
				return err
			}
			return updateWikiPageMetaRow(tx, page)
		},
	)
}

func updateWikiPageMetaRow(db *gorm.DB, page *types.WikiPage) error {
	result := db.
		Model(page).
		Where("id = ?", page.ID).
		Updates(map[string]interface{}{
			"in_links":      page.InLinks,
			"out_links":     page.OutLinks,
			"aliases":       page.Aliases,
			"status":        page.Status,
			"source_refs":   page.SourceRefs,
			"chunk_refs":    page.ChunkRefs,
			"page_metadata": page.PageMetadata,
			"parent_slug":   page.ParentSlug,
			"folder_id":     page.FolderID,
			"category_path": page.CategoryPath,
			"wiki_path":     page.WikiPath,
			"depth":         page.Depth,
			"sort_order":    page.SortOrder,
			"updated_at":    page.UpdatedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWikiPageNotFound
	}
	return nil
}

// withWikiLinkTransaction locks every involved live page in slug order on
// PostgreSQL, executes the source-page write, then applies the target in_links
// diff. SQLite relies on its transaction write lock. Any target failure rolls
// back the source row and revision as well.
func (r *wikiPageRepository) withWikiLinkTransaction(
	ctx context.Context,
	kbID string,
	sourceSlug string,
	oldOutLinks types.StringArray,
	newOutLinks types.StringArray,
	writeSource func(*gorm.DB, *types.WikiPage) error,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := validateWikiSourceAttemptGuards(tx, ctx, kbID); err != nil {
			return err
		}
		if err := serializeWikiLinkSource(tx, kbID, sourceSlug); err != nil {
			return err
		}
		currentSource, err := getWikiLinkSource(tx, kbID, sourceSlug)
		if err != nil {
			return err
		}
		if r.testAfterWikiLinkSourceSerialized != nil {
			r.testAfterWikiLinkSourceSerialized()
		}
		effectiveOldOutLinks := oldOutLinks
		if currentSource != nil {
			effectiveOldOutLinks = append(types.StringArray(nil), currentSource.OutLinks...)
		}
		targetSlugs := wikiLinkTargetSlugs(
			types.StringArray{sourceSlug}, effectiveOldOutLinks, newOutLinks,
		)
		targets, err := lockWikiLinkTargets(tx, kbID, targetSlugs)
		if err != nil {
			return err
		}
		lockedSource := targets[sourceSlug]
		if err := writeSource(tx, lockedSource); err != nil {
			return err
		}

		oldSet := wikiSlugSet(effectiveOldOutLinks)
		newSet := wikiSlugSet(newOutLinks)
		for _, slug := range targetSlugs {
			if slug == sourceSlug {
				// A newly-created self-link was absent during the pre-write lock.
				if _, ok := targets[slug]; !ok {
					var source types.WikiPage
					if err := tx.Where("knowledge_base_id = ? AND slug = ?", kbID, slug).First(&source).Error; err != nil {
						return err
					}
					targets[slug] = &source
				}
			}
			target := targets[slug]
			if target == nil { // links to pages that do not exist yet are allowed
				continue
			}
			want := newSet[slug]
			had := oldSet[slug]
			if want == had && wikiLinkMembershipIsCanonical(target.InLinks, sourceSlug, want) {
				continue
			}
			updated := wikiSetLinkMembership(target.InLinks, sourceSlug, want)
			result := tx.Model(&types.WikiPage{}).
				Where("id = ?", target.ID).
				Updates(map[string]interface{}{"in_links": updated, "updated_at": time.Now()})
			if result.Error != nil {
				return fmt.Errorf("update in_links for %s: %w", slug, result.Error)
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("update in_links for %s: %w", slug, ErrWikiPageNotFound)
			}
			target.InLinks = updated
		}
		return applyWikiSlugApplicationTransition(tx, ctx)
	})
}

// validateWikiSourceAttemptGuards serializes Wiki writes with OpenAttempt and
// validates every source document inside the page mutation transaction. A
// detached worker can therefore never write after a newer attempt has opened,
// nor after the source/KB was deleted or Wiki was disabled.
func validateWikiSourceAttemptGuards(tx *gorm.DB, ctx context.Context, kbID string) error {
	guards, guarded := types.WikiSourceAttemptGuardsFromContext(ctx)
	if !guarded {
		return nil
	}
	if strings.TrimSpace(kbID) == "" {
		return errors.New("wiki source attempt guard: knowledge base id is required")
	}

	for _, guard := range guards {
		if tx.Dialector != nil && tx.Dialector.Name() == "postgres" {
			if err := tx.Exec(
				"SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", guard.KnowledgeID,
			).Error; err != nil {
				return fmt.Errorf("lock wiki source %s: %w", guard.KnowledgeID, err)
			}
		}
	}

	query := tx
	if tx.Dialector != nil && tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var kb types.KnowledgeBase
	if err := query.Where("id = ?", kbID).First(&kb).Error; err != nil {
		return fmt.Errorf("validate wiki knowledge base %s: %w", kbID, err)
	}
	if !kb.IsWikiEnabled() {
		return fmt.Errorf("validate wiki knowledge base %s: wiki is disabled", kbID)
	}

	for _, guard := range guards {
		query = tx
		if tx.Dialector != nil && tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var knowledge types.Knowledge
		if err := query.Where("id = ? AND knowledge_base_id = ?", guard.KnowledgeID, kbID).
			First(&knowledge).Error; err != nil {
			return fmt.Errorf("validate wiki source %s: %w", guard.KnowledgeID, err)
		}
		if knowledge.ParseStatus == types.ParseStatusDeleting || knowledge.ParseStatus == types.ParseStatusCancelled {
			return fmt.Errorf("validate wiki source %s: knowledge is %s", guard.KnowledgeID, knowledge.ParseStatus)
		}
		if guard.Attempt == 0 {
			if knowledge.ParseStatus != types.ParseStatusCompleted {
				return fmt.Errorf(
					"validate wiki maintenance source %s: knowledge is %s",
					guard.KnowledgeID, knowledge.ParseStatus,
				)
			}
			continue
		}

		var latestAttempt int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", guard.KnowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt), 0)").Scan(&latestAttempt).Error; err != nil {
			return fmt.Errorf("validate wiki source %s latest attempt: %w", guard.KnowledgeID, err)
		}
		if latestAttempt != guard.Attempt {
			return fmt.Errorf(
				"validate wiki source %s: attempt %d was superseded by attempt %d",
				guard.KnowledgeID, guard.Attempt, latestAttempt,
			)
		}
	}
	return nil
}

// serializeWikiLinkSource prevents two writers for the same source from
// deriving their diff from the same stale row. It deliberately does not lock
// a wiki_pages row: reciprocal A->B / B->A writers take different advisory
// locks, then both acquire all involved rows in the same slug order below.
func serializeWikiLinkSource(tx *gorm.DB, kbID string, slug string) error {
	if tx.Dialector == nil || tx.Dialector.Name() != "postgres" {
		return nil
	}
	key := fmt.Sprintf("wiki-link:%d:%s:%s", len(kbID), kbID, slug)
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", key).Error
}

func getWikiLinkSource(tx *gorm.DB, kbID string, slug string) (*types.WikiPage, error) {
	var source types.WikiPage
	if err := tx.Where("knowledge_base_id = ? AND slug = ?", kbID, slug).First(&source).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &source, nil
}

func lockWikiLinkTargets(tx *gorm.DB, kbID string, slugs []string) (map[string]*types.WikiPage, error) {
	targets := make(map[string]*types.WikiPage, len(slugs))
	if len(slugs) == 0 {
		return targets, nil
	}
	query := tx.Where("knowledge_base_id = ? AND slug IN ?", kbID, slugs).Order("slug ASC")
	if tx.Dialector != nil && tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var pages []*types.WikiPage
	if err := query.Find(&pages).Error; err != nil {
		return nil, err
	}
	for _, page := range pages {
		targets[page.Slug] = page
	}
	return targets, nil
}

func wikiLinkTargetSlugs(sets ...types.StringArray) []string {
	seen := make(map[string]struct{})
	for _, links := range sets {
		for _, slug := range links {
			if slug != "" {
				seen[slug] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(seen))
	for slug := range seen {
		result = append(result, slug)
	}
	sort.Strings(result)
	return result
}

func wikiSlugSet(links types.StringArray) map[string]bool {
	set := make(map[string]bool, len(links))
	for _, slug := range links {
		set[slug] = true
	}
	return set
}

func wikiLinkMembershipIsCanonical(links types.StringArray, slug string, want bool) bool {
	count := 0
	for _, item := range links {
		if item == slug {
			count++
		}
	}
	if want {
		return count == 1
	}
	return count == 0
}

func wikiSetLinkMembership(links types.StringArray, slug string, want bool) types.StringArray {
	seen := make(map[string]struct{}, len(links)+1)
	result := make(types.StringArray, 0, len(links)+1)
	for _, item := range links {
		if item == "" || item == slug {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	if want {
		result = append(result, slug)
	}
	sort.Strings(result)
	return result
}

// GetByID retrieves a wiki page by its unique ID
func (r *wikiPageRepository) GetByID(ctx context.Context, id string) (*types.WikiPage, error) {
	var page types.WikiPage
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&page).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWikiPageNotFound
		}
		return nil, err
	}
	return &page, nil
}

// GetBySlug retrieves a wiki page by slug within a knowledge base
func (r *wikiPageRepository) GetBySlug(ctx context.Context, kbID string, slug string) (*types.WikiPage, error) {
	var page types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND slug = ?", kbID, slug).
		First(&page).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWikiPageNotFound
		}
		return nil, err
	}
	return &page, nil
}

// List retrieves wiki pages with filtering and pagination
func (r *wikiPageRepository) List(ctx context.Context, req *types.WikiPageListRequest) ([]*types.WikiPage, int64, error) {
	query := r.db.WithContext(ctx).Model(&types.WikiPage{}).
		Where("knowledge_base_id = ?", req.KnowledgeBaseID)

	if pageTypes := types.SplitWikiPageTypes(req.PageType); len(pageTypes) == 1 {
		query = query.Where("page_type = ?", pageTypes[0])
	} else if len(pageTypes) > 1 {
		query = query.Where("page_type IN ?", pageTypes)
	}
	if req.Status != "" {
		query = query.Where("status = ?", req.Status)
	} else {
		query = query.Where("status <> ?", types.WikiPageStatusArchived)
	}
	if req.Query != "" {
		// Use PostgreSQL full-text search + ILIKE for aliases
		query = query.Where(
			"(to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(content, '')) @@ plainto_tsquery('simple', ?) OR aliases::text ILIKE ?)",
			req.Query,
			"%"+req.Query+"%",
		)
	}
	// Directory filters are pushed to SQL so the DB does the counting and
	// pagination instead of loading every page of the type into memory. `depth`
	// is a cached column (= len(category_path)); `category_path` is a JSON column
	// whose stored text is json.Marshal of the cleaned path, so we compare
	// against the same encoding. Postgres needs an explicit jsonb cast for array
	// equality; SQLite stores JSON as TEXT and compares directly.
	if req.FolderID != nil {
		query = query.Where("folder_id = ?", *req.FolderID)
	}
	if req.CategoryDepth != nil {
		query = query.Where("depth = ?", *req.CategoryDepth)
	}
	if wantPath := types.CleanWikiCategoryPath(req.CategoryPath); len(wantPath) > 0 {
		if encoded, err := json.Marshal([]string(wantPath)); err == nil {
			if r.db.Dialector != nil && r.db.Dialector.Name() == "postgres" {
				query = query.Where("category_path::jsonb = ?::jsonb", string(encoded))
			} else {
				query = query.Where("category_path = ?", string(encoded))
			}
		}
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// Sort
	sortBy := "updated_at"
	if req.SortBy != "" {
		switch req.SortBy {
		case "title", "created_at", "updated_at", "page_type", "wiki_path", "sort_order", "depth":
			sortBy = req.SortBy
		}
	}
	sortOrder := "DESC"
	if req.SortOrder == "asc" {
		sortOrder = "ASC"
	}
	if sortBy == "wiki_path" {
		query = query.Order(r.wikiCategoryRankOrder()).
			Order(fmt.Sprintf("wiki_path %s", sortOrder)).
			Order("sort_order ASC").
			Order("title ASC")
	} else {
		query = query.Order(fmt.Sprintf("%s %s", sortBy, sortOrder))
	}

	page := req.Page
	if page < 1 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	// Pagination
	query = query.Offset(offset).Limit(pageSize)

	var pages []*types.WikiPage
	if err := query.Find(&pages).Error; err != nil {
		return nil, 0, err
	}
	return pages, total, nil
}

// ListByType retrieves all wiki pages of a given type within a knowledge base
func (r *wikiPageRepository) ListByType(ctx context.Context, kbID string, pageType string) ([]*types.WikiPage, error) {
	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND page_type = ?", kbID, pageType).
		Order("updated_at DESC").
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// ListByTypeLight projects only the columns needed to render an index
// directory entry (slug, title, summary) and paginates by title ASC.
// This keeps the GET /wiki/index response cheap on KBs with tens of
// thousands of pages — the old path loaded every row including its TEXT
// content just to throw the content away on the way out.
//
// Archived pages are excluded. `limit` clamps to [1, 200]; `offset` is
// honored as-is. Returns the total non-archived count for the type
// alongside the page so the caller can render "showing N of M".
func (r *wikiPageRepository) ListByTypeLight(
	ctx context.Context,
	kbID string,
	pageType string,
	limit int,
	offset int,
) ([]types.WikiIndexEntry, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	base := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND page_type = ? AND status <> ?",
			kbID, pageType, types.WikiPageStatusArchived)

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return nil, 0, nil
	}

	var entries []types.WikiIndexEntry
	if err := base.
		Select("slug", "title", "summary", "parent_slug", "category_path", "wiki_path", "depth", "sort_order").
		Order(r.wikiCategoryRankOrder()).
		Order("wiki_path ASC").
		Order("sort_order ASC").
		Order("title ASC").
		Limit(limit).
		Offset(offset).
		Scan(&entries).Error; err != nil {
		return nil, 0, err
	}
	return entries, total, nil
}

// ListBySourceRef retrieves all wiki pages that reference a given source knowledge ID.
// Handles both old format ("knowledgeID") and new format ("knowledgeID|title") in source_refs JSON array.
func (r *wikiPageRepository) ListBySourceRef(ctx context.Context, kbID string, sourceKnowledgeID string) ([]*types.WikiPage, error) {
	// Build the JSON needle safely so arbitrary IDs cannot break out of the
	// quoted string (e.g. ids containing quotes or backslashes).
	needle, err := json.Marshal([]string{sourceKnowledgeID})
	if err != nil {
		return nil, fmt.Errorf("marshal source ref needle: %w", err)
	}

	// For the "knowledgeID|title" prefix form, match against the JSON-encoded
	// value: json.Marshal escapes special chars so the LIKE pattern is safe.
	prefix, err := json.Marshal(sourceKnowledgeID + "|")
	if err != nil {
		return nil, fmt.Errorf("marshal source ref prefix: %w", err)
	}
	// prefix is a JSON string including the surrounding quotes; e.g. "abc|".
	// We strip the trailing quote so LIKE can continue into the title portion.
	prefixStr := string(prefix)
	if len(prefixStr) >= 2 && prefixStr[len(prefixStr)-1] == '"' {
		prefixStr = prefixStr[:len(prefixStr)-1]
	}
	// Escape LIKE metacharacters in the already-JSON-escaped prefix, then wrap
	// with %…% to match anywhere in the serialized JSON array.
	likePattern := "%" + escapeLikePattern(prefixStr) + "%"

	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND (source_refs @> ?::jsonb OR source_refs::text LIKE ?)",
			kbID,
			string(needle),
			likePattern,
		).
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// ListSlugsBySourceRef returns just the slugs of pages that reference the
// given knowledge id. Same predicate as ListBySourceRef (both forms
// "knowledgeID" and "knowledgeID|title"), but projected down to a single
// column so the wiki ingest pipeline doesn't have to load full rows when
// it only needs a "before" set of slugs.
//
// Backed by idx_wiki_pages_source_refs (GIN jsonb_path_ops) for the
// containment branch and idx_wiki_pages_source_refs_text for the legacy
// text-LIKE branch — both added in migration 000041.
func (r *wikiPageRepository) ListSlugsBySourceRef(ctx context.Context, kbID string, sourceKnowledgeID string) ([]string, error) {
	needle, err := json.Marshal([]string{sourceKnowledgeID})
	if err != nil {
		return nil, fmt.Errorf("marshal source ref needle: %w", err)
	}
	prefix, err := json.Marshal(sourceKnowledgeID + "|")
	if err != nil {
		return nil, fmt.Errorf("marshal source ref prefix: %w", err)
	}
	prefixStr := string(prefix)
	if len(prefixStr) >= 2 && prefixStr[len(prefixStr)-1] == '"' {
		prefixStr = prefixStr[:len(prefixStr)-1]
	}
	likePattern := "%" + escapeLikePattern(prefixStr) + "%"

	var slugs []string
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND (source_refs @> ?::jsonb OR source_refs::text LIKE ?)",
			kbID,
			string(needle),
			likePattern,
		).
		Pluck("slug", &slugs).Error; err != nil {
		return nil, err
	}
	return slugs, nil
}

// ListBySlugs returns lightweight projections (slug, title, page_type,
// status, aliases, out_links) for the given slugs in one IN query.
// Used by wiki ingest's lazy fetcher path to resolve slug -> title /
// out-links during Map/Reduce without paying for a full ListAll scan.
//
// Empty input returns nil, nil. Slugs not present in the KB are silently
// dropped from the returned map (caller treats absent slugs as "no
// such page" — the same shape ListAll had via missing keys).
func (r *wikiPageRepository) ListBySlugs(
	ctx context.Context,
	kbID string,
	slugs []string,
) (map[string]*types.WikiPageLite, error) {
	if len(slugs) == 0 {
		return nil, nil
	}
	var rows []types.WikiPageLite
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("slug", "title", "page_type", "status", "aliases", "out_links").
		Where("knowledge_base_id = ? AND slug IN ?", kbID, slugs).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]*types.WikiPageLite, len(rows))
	for i := range rows {
		r := rows[i]
		out[r.Slug] = &r
	}
	return out, nil
}

// ListDistinctCategoryPaths returns the materialized paths of existing wiki
// folders (split into segments), ordered by path and capped at maxPaths. Used
// by the batch taxonomy planner as the candidate pool of folders to reuse
// (similarity preprocessing then narrows it per batch). The folder tree is the
// single source of truth, so this no longer scans page rows.
func (r *wikiPageRepository) ListDistinctCategoryPaths(
	ctx context.Context,
	kbID string,
	maxPaths int,
) ([][]string, error) {
	if maxPaths <= 0 {
		maxPaths = 150
	}
	var paths []string
	if err := r.db.WithContext(ctx).
		Model(&types.WikiFolder{}).
		Where("knowledge_base_id = ? AND path <> ?", kbID, "").
		Order("path ASC").
		Limit(maxPaths).
		Pluck("path", &paths).Error; err != nil {
		return nil, err
	}
	out := make([][]string, 0, len(paths))
	for _, p := range paths {
		if seg := types.CleanWikiCategoryPath(strings.Split(p, "/")); len(seg) > 0 {
			out = append(out, seg)
		}
	}
	return out, nil
}

// --- Folder tree (wiki_folders) ---

// ErrWikiFolderNotFound is returned when a wiki folder is not found.
var ErrWikiFolderNotFound = errors.New("wiki folder not found")

// ErrWikiFolderConflict is returned when a sibling folder with the same name
// already exists under the same parent.
var ErrWikiFolderConflict = errors.New("wiki folder name conflict")

// ErrWikiFolderNotEmpty is returned when a folder still has a live page or
// child folder at the instant an atomic delete is attempted.
var ErrWikiFolderNotEmpty = errors.New("wiki folder is not empty")

func (r *wikiPageRepository) CreateFolder(ctx context.Context, folder *types.WikiFolder) error {
	return r.db.WithContext(ctx).Create(folder).Error
}

func (r *wikiPageRepository) GetFolderByID(ctx context.Context, kbID string, id string) (*types.WikiFolder, error) {
	var folder types.WikiFolder
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND id = ?", kbID, id).
		First(&folder).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWikiFolderNotFound
		}
		return nil, err
	}
	return &folder, nil
}

func (r *wikiPageRepository) GetChildFolderByName(
	ctx context.Context, kbID string, parentID string, name string,
) (*types.WikiFolder, error) {
	var folder types.WikiFolder
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND parent_id = ? AND name = ?", kbID, parentID, name).
		First(&folder).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrWikiFolderNotFound
		}
		return nil, err
	}
	return &folder, nil
}

func (r *wikiPageRepository) ListChildFolders(
	ctx context.Context, kbID string, parentID string,
) ([]*types.WikiFolder, error) {
	var folders []*types.WikiFolder
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND parent_id = ?", kbID, parentID).
		Order("sort_order ASC").
		Order("name ASC").
		Find(&folders).Error; err != nil {
		return nil, err
	}
	return folders, nil
}

func (r *wikiPageRepository) ListAllFolders(ctx context.Context, kbID string) ([]*types.WikiFolder, error) {
	var folders []*types.WikiFolder
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ?", kbID).
		Order("depth ASC").
		Order("path ASC").
		Find(&folders).Error; err != nil {
		return nil, err
	}
	return folders, nil
}

func (r *wikiPageRepository) UpdateFolder(ctx context.Context, folder *types.WikiFolder) error {
	result := r.db.WithContext(ctx).
		Model(&types.WikiFolder{}).
		Where("id = ?", folder.ID).
		Updates(map[string]interface{}{
			"parent_id":  folder.ParentID,
			"name":       folder.Name,
			"path":       folder.Path,
			"depth":      folder.Depth,
			"sort_order": folder.SortOrder,
			"updated_at": folder.UpdatedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWikiFolderNotFound
	}
	return nil
}

func (r *wikiPageRepository) DeleteFolder(ctx context.Context, kbID string, id string) error {
	// Keep the emptiness test in the same SQL statement as the soft delete.
	// A page move or child-folder create can race the service's earlier checks;
	// a check-then-delete sequence would otherwise leave a dangling folder_id.
	result := r.db.WithContext(ctx).Exec(`
UPDATE wiki_folders
SET deleted_at = ?
WHERE knowledge_base_id = ? AND id = ? AND deleted_at IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM wiki_pages
    WHERE knowledge_base_id = ? AND folder_id = ? AND deleted_at IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM wiki_folders AS child
    WHERE child.knowledge_base_id = ? AND child.parent_id = ? AND child.deleted_at IS NULL
  )`, time.Now(), kbID, id, kbID, id, kbID, id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		var count int64
		if err := r.db.WithContext(ctx).Model(&types.WikiFolder{}).
			Where("knowledge_base_id = ? AND id = ?", kbID, id).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return ErrWikiFolderNotFound
		}
		return ErrWikiFolderNotEmpty
	}
	return nil
}

func (r *wikiPageRepository) CountPagesInFolder(ctx context.Context, kbID string, folderID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND folder_id = ? AND status <> ?",
			kbID, folderID, types.WikiPageStatusArchived).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *wikiPageRepository) CountPagesByFolder(
	ctx context.Context, kbID string, pageTypes []string,
) (map[string]int64, error) {
	type folderCount struct {
		FolderID string
		Cnt      int64
	}
	var rows []folderCount
	q := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("folder_id, COUNT(*) as cnt").
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived)
	if len(pageTypes) > 0 {
		q = q.Where("page_type IN ?", pageTypes)
	}
	if err := q.Group("folder_id").Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		out[row.FolderID] = row.Cnt
	}
	return out, nil
}

func (r *wikiPageRepository) ListPagesByFolderIDs(
	ctx context.Context, kbID string, folderIDs []string,
) ([]*types.WikiPage, error) {
	if len(folderIDs) == 0 {
		return nil, nil
	}
	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND folder_id IN ?", kbID, folderIDs).
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// ListSummariesByKnowledgeIDs returns summary-page content keyed by the
// knowledge id that authored it. The page_type filter is applied first
// (only summary pages have content suitable for retract framing); within
// that subset we look at source_refs for either the bare knowledge id
// or the "knowledgeID|title" legacy form.
//
// Empty kids returns nil, nil. A knowledge id with no surviving summary
// page is silently absent from the result map.
//
// Used by reduceSlugUpdates' retract branch so it can frame "what did
// the now-departed sibling document contribute?" for the WikiPageModify
// LLM call without needing to keep the whole batchCtx.SummaryContent
// map in memory ahead of time.
func (r *wikiPageRepository) ListSummariesByKnowledgeIDs(
	ctx context.Context,
	kbID string,
	kids []string,
) (map[string]string, error) {
	if len(kids) == 0 {
		return nil, nil
	}

	// Build a JSONB containment-OR with one needle per knowledge id,
	// plus a single text-LIKE OR over the legacy prefix forms. The
	// containment branches each get their own GIN index probe; the
	// LIKE branch falls back to the text fulltext GIN.
	type row struct {
		Content    string            `gorm:"column:content"`
		SourceRefs types.StringArray `gorm:"column:source_refs"`
	}

	q := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("content", "source_refs").
		Where("knowledge_base_id = ? AND page_type = ? AND status <> ?",
			kbID, types.WikiPageTypeSummary, types.WikiPageStatusArchived)

	// Build OR clauses without using overly-clever GORM tricks: assemble
	// raw SQL fragments + args. Keeping this defensive because source_refs
	// patterns include user-controlled knowledge ids.
	clauses := make([]string, 0, len(kids)*2)
	args := make([]interface{}, 0, len(kids)*2)
	for _, kid := range kids {
		if kid == "" {
			continue
		}
		needle, err := json.Marshal([]string{kid})
		if err != nil {
			return nil, fmt.Errorf("marshal kid needle: %w", err)
		}
		clauses = append(clauses, "source_refs @> ?::jsonb")
		args = append(args, string(needle))

		prefix, err := json.Marshal(kid + "|")
		if err != nil {
			return nil, fmt.Errorf("marshal kid prefix: %w", err)
		}
		prefixStr := string(prefix)
		if len(prefixStr) >= 2 && prefixStr[len(prefixStr)-1] == '"' {
			prefixStr = prefixStr[:len(prefixStr)-1]
		}
		clauses = append(clauses, "source_refs::text LIKE ?")
		args = append(args, "%"+escapeLikePattern(prefixStr)+"%")
	}
	if len(clauses) == 0 {
		return nil, nil
	}
	q = q.Where("("+strings.Join(clauses, " OR ")+")", args...)

	var rows []row
	if err := q.Scan(&rows).Error; err != nil {
		return nil, err
	}

	// Map each row's content to every kid in its source_refs (a single
	// summary may carry multiple sources after a previous merge / re-
	// ingest). Caller looks up by kid, so duplicates resolve to the
	// same content string.
	kidSet := make(map[string]struct{}, len(kids))
	for _, kid := range kids {
		if kid != "" {
			kidSet[kid] = struct{}{}
		}
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		for _, ref := range r.SourceRefs {
			refKID := ref
			if pipeIdx := strings.Index(ref, "|"); pipeIdx > 0 {
				refKID = ref[:pipeIdx]
			}
			if _, want := kidSet[refKID]; !want {
				continue
			}
			if _, exists := out[refKID]; !exists {
				out[refKID] = r.Content
			}
		}
	}
	return out, nil
}

// ExistsSlugs reports which of the given slugs are live (non-archived,
// non-deleted) in the KB. Used by cleanDeadLinks to validate out-link
// targets without loading the referenced pages' content. Slugs not
// present in the KB at all map to false; archived slugs also map to
// false so dead-link cleanup treats them as gone.
//
// Empty input returns nil, nil so callers can branch cheaply.
func (r *wikiPageRepository) ExistsSlugs(
	ctx context.Context,
	kbID string,
	slugs []string,
) (map[string]bool, error) {
	if len(slugs) == 0 {
		return nil, nil
	}
	var live []string
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND slug IN ? AND status <> ?",
			kbID, slugs, types.WikiPageStatusArchived).
		Pluck("slug", &live).Error; err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(slugs))
	for _, s := range slugs {
		out[s] = false
	}
	for _, s := range live {
		out[s] = true
	}
	return out, nil
}

// ListAllSlugs returns every non-archived page slug in the KB. Used by
// the lint pipeline to compute the "live slug set" for broken-link
// detection without paying for ListAll's full row materialization.
func (r *wikiPageRepository) ListAllSlugs(
	ctx context.Context,
	kbID string,
) ([]string, error) {
	var slugs []string
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived).
		Pluck("slug", &slugs).Error; err != nil {
		return nil, err
	}
	return slugs, nil
}

// ListPagesCursor returns up to `limit` pages for the KB ordered by
// (knowledge_base_id, id) ascending, paginated by an opaque numeric
// cursor. The cursor is the stringified id of the last row from the
// previous page; "" starts from the beginning. Empty nextCursor =
// end-of-stream.
//
// Used by lint to walk the entire KB without ever holding the full
// page set in memory. `limit` is clamped to [1, 500].
func (r *wikiPageRepository) ListPagesCursor(
	ctx context.Context,
	kbID string,
	cursor string,
	limit int,
) ([]*types.WikiPage, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	q := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived).
		Order("id ASC").
		Limit(limit)
	if cursor != "" {
		q = q.Where("id > ?", cursor)
	}
	var pages []*types.WikiPage
	if err := q.Find(&pages).Error; err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(pages) == limit {
		nextCursor = pages[len(pages)-1].ID
	}
	return pages, nextCursor, nil
}

// ListByTypeRecent returns up to `limit` summary-typed pages ordered
// by updated_at DESC, projected to slug/title/summary. Used by the
// rebuildIndexPage first-time generation path — historically that
// loaded EVERY summary page and concatenated them into the prompt,
// which broke the LLM context window once a KB grew past a few
// thousand documents. The recent-N projection caps the prompt size
// at the cost of intro framing for very old documents (which are
// unlikely to be the most-relevant index introductions anyway).
//
// `limit` is clamped to [1, 1000]; 0 falls back to 200.
func (r *wikiPageRepository) ListByTypeRecent(
	ctx context.Context,
	kbID string,
	pageType string,
	limit int,
) ([]types.WikiIndexEntry, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}
	var entries []types.WikiIndexEntry
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("slug", "title", "summary").
		Where("knowledge_base_id = ? AND page_type = ? AND status <> ?",
			kbID, pageType, types.WikiPageStatusArchived).
		Order("updated_at DESC").
		Limit(limit).
		Scan(&entries).Error; err != nil {
		return nil, err
	}
	return entries, nil
}

// FindSimilarPages returns the top-k entity/concept pages whose lowercase
// title is most similar to the given query under PostgreSQL pg_trgm
// trigram similarity. Backed by idx_wiki_pages_title_trgm (GIN
// gin_trgm_ops, migration 000041). Used by the dedup pre-filter to
// surface candidate merge targets without loading every entity/concept
// page into Go.
//
// types is an optional page_type allow-list; empty means entity+concept.
// limit is clamped to [1, 50]. Pages whose title similarity is below
// 0.1 are dropped server-side via the `%` operator (which respects
// pg_trgm.similarity_threshold).
func (r *wikiPageRepository) FindSimilarPages(
	ctx context.Context,
	kbID string,
	query string,
	pageTypes []string,
	limit int,
) ([]*types.WikiPageLite, error) {
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}
	if len(pageTypes) == 0 {
		pageTypes = []string{types.WikiPageTypeEntity, types.WikiPageTypeConcept}
	}

	q := strings.ToLower(strings.TrimSpace(query))

	var rows []types.WikiPageLite
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("slug, title, page_type, status, aliases, out_links, similarity(lower(title), ?) AS sim", q).
		Where("knowledge_base_id = ? AND page_type IN ? AND status <> ? AND lower(title) % ?",
			kbID, pageTypes, types.WikiPageStatusArchived, q).
		Order("sim DESC").
		Limit(limit).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]*types.WikiPageLite, len(rows))
	for i := range rows {
		r := rows[i]
		out[i] = &r
	}
	return out, nil
}

func wikiNormalizedTitleSQL(db *gorm.DB) string {
	if db != nil && db.Dialector != nil && db.Dialector.Name() == "sqlite" {
		// SQLite has no POSIX [[:space:]]. Strip the common separators that
		// model formatting drift actually produces; callers still re-check
		// with the Go identity fold so extras cannot leak through.
		return "lower(replace(replace(replace(replace(replace(replace(title, ' ', ''), char(9), ''), char(10), ''), char(13), ''), char(160), ''), char(12288), ''))"
	}
	return "regexp_replace(lower(title), '[[:space:]]+', '', 'g')"
}

const (
	wikiNormalizedTitleQueryChunk = 100
	wikiNormalizedTitleRowCap     = 500
)

func wikiNormalizedTitleLookupLimit(n int) int {
	if n <= 0 {
		return 0
	}
	limit := n * 8
	if limit < 50 {
		limit = 50
	}
	if limit > wikiNormalizedTitleRowCap {
		limit = wikiNormalizedTitleRowCap
	}
	return limit
}

func uniqNormalizedTitleIdentities(identities []string) []string {
	if len(identities) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(identities))
	out := make([]string, 0, len(identities))
	for _, identity := range identities {
		identity = strings.TrimSpace(identity)
		if identity == "" {
			continue
		}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		out = append(out, identity)
	}
	return out
}

// FindPagesByNormalizedTitle returns non-archived pages of pageType whose
// whitespace-stripped, lowercased title equals identity.
func (r *wikiPageRepository) FindPagesByNormalizedTitle(
	ctx context.Context,
	kbID, pageType, identity string,
) ([]*types.WikiPageLite, error) {
	return r.FindPagesByNormalizedTitles(ctx, kbID, pageType, []string{identity})
}

// FindPagesByNormalizedTitles returns non-archived pages of pageType whose
// whitespace-stripped, lowercased title is in identities.
func (r *wikiPageRepository) FindPagesByNormalizedTitles(
	ctx context.Context,
	kbID, pageType string,
	identities []string,
) ([]*types.WikiPageLite, error) {
	identities = uniqNormalizedTitleIdentities(identities)
	if kbID == "" || pageType == "" || len(identities) == 0 {
		return nil, nil
	}

	normSQL := wikiNormalizedTitleSQL(r.db)
	out := make([]*types.WikiPageLite, 0, len(identities))
	for start := 0; start < len(identities); start += wikiNormalizedTitleQueryChunk {
		end := start + wikiNormalizedTitleQueryChunk
		if end > len(identities) {
			end = len(identities)
		}
		chunk := identities[start:end]
		var rows []types.WikiPageLite
		if err := r.db.WithContext(ctx).
			Model(&types.WikiPage{}).
			Select("slug, title, page_type, status, aliases, out_links").
			Where("knowledge_base_id = ? AND page_type = ? AND status <> ? AND "+normSQL+" IN ?",
				kbID, pageType, types.WikiPageStatusArchived, chunk).
			Order("slug ASC").
			Limit(wikiNormalizedTitleLookupLimit(len(chunk))).
			Scan(&rows).Error; err != nil {
			return nil, err
		}
		for i := range rows {
			row := rows[i]
			out = append(out, &row)
		}
	}
	return out, nil
}

// ListAll retrieves all non-archived wiki pages in a knowledge base.
func (r *wikiPageRepository) ListAll(ctx context.Context, kbID string) ([]*types.WikiPage, error) {
	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived).
		Order("page_type ASC, title ASC").
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// ListRecentForSuggestions returns recent user-visible wiki pages across the given
// knowledge bases, used as a fallback source for agent suggested questions when
// the KB has no FAQ entries or AI-generated document questions (typical for
// Wiki-only KBs). Excludes the index page and archived pages.
func (r *wikiPageRepository) ListRecentForSuggestions(
	ctx context.Context,
	tenantID uint64,
	kbIDs []string,
	limit int,
) ([]*types.WikiPage, error) {
	if len(kbIDs) == 0 || limit <= 0 {
		return nil, nil
	}
	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Where("tenant_id = ?", tenantID).
		Where("knowledge_base_id IN ?", kbIDs).
		Where("page_type <> ?", types.WikiPageTypeIndex).
		Where("status = ?", types.WikiPageStatusPublished).
		Where("title <> ''").
		Order("updated_at DESC").
		Limit(limit).
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// Delete soft-deletes a wiki page by knowledge base ID and slug
func (r *wikiPageRepository) Delete(ctx context.Context, kbID string, slug string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var page types.WikiPage
		if err := tx.Where("knowledge_base_id = ? AND slug = ?", kbID, slug).First(&page).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWikiPageNotFound
			}
			return err
		}
		return deleteWikiPageAndCanonicalIdentity(tx, &page, r.canonicalIdentityEnabled)
	})
}

// DeleteByID soft-deletes a wiki page by ID
func (r *wikiPageRepository) DeleteByID(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var page types.WikiPage
		if err := tx.Where("id = ?", id).First(&page).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrWikiPageNotFound
			}
			return err
		}
		return deleteWikiPageAndCanonicalIdentity(tx, &page, r.canonicalIdentityEnabled)
	})
}

func deleteWikiPageAndCanonicalIdentity(tx *gorm.DB, page *types.WikiPage, enabled bool) error {
	if enabled && page != nil && (page.PageType == types.WikiPageTypeEntity || page.PageType == types.WikiPageTypeConcept) {
		if err := tx.Where(
			"knowledge_base_id = ? AND page_type = ? AND identity_key = ? AND canonical_slug = ?",
			page.KnowledgeBaseID, page.PageType, types.NormalizeWikiIdentityTitle(page.Title), page.Slug,
		).Delete(&types.WikiCanonicalIdentity{}).Error; err != nil {
			return err
		}
	}
	result := tx.Delete(page)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrWikiPageNotFound
	}
	return nil
}

// ResolveCanonicalWikiPageSlugs atomically registers or reads the durable
// canonical slug for every exact title identity. The unique registry key is
// authoritative across workers, retries, Redis expiry, and process restarts.
func (r *wikiPageRepository) ResolveCanonicalWikiPageSlugs(
	ctx context.Context,
	tenantID uint64,
	kbID string,
	candidates []types.WikiCanonicalCandidate,
) (map[string]string, error) {
	resolved := make(map[string]string, len(candidates))
	ordered := append([]types.WikiCanonicalCandidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i].PageType + "\x00" + types.NormalizeWikiIdentityTitle(ordered[i].Title) + "\x00" + ordered[i].Slug
		right := ordered[j].PageType + "\x00" + types.NormalizeWikiIdentityTitle(ordered[j].Title) + "\x00" + ordered[j].Slug
		return left < right
	})

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, candidate := range ordered {
			if candidate.Slug == "" || (candidate.PageType != types.WikiPageTypeEntity && candidate.PageType != types.WikiPageTypeConcept) {
				continue
			}
			identityKey := types.NormalizeWikiIdentityTitle(candidate.Title)
			if identityKey == "" {
				resolved[candidate.Slug] = candidate.Slug
				continue
			}

			existingCanonicalSlug, err := r.bestExistingCanonicalSlug(tx, kbID, candidate.PageType, identityKey)
			if err != nil {
				return err
			}
			canonicalSlug := existingCanonicalSlug
			if canonicalSlug == "" {
				canonicalSlug = candidate.Slug
			}
			identity := types.WikiCanonicalIdentity{
				TenantID: tenantID, KnowledgeBaseID: kbID, PageType: candidate.PageType,
				IdentityKey: identityKey, CanonicalSlug: canonicalSlug,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "knowledge_base_id"}, {Name: "page_type"}, {Name: "identity_key"}},
				DoNothing: true,
			}).Create(&identity).Error; err != nil {
				return err
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("knowledge_base_id = ? AND page_type = ? AND identity_key = ?", kbID, candidate.PageType, identityKey).
				First(&identity).Error; err != nil {
				return err
			}
			var registered types.WikiPage
			registeredErr := tx.Where(
				"knowledge_base_id = ? AND slug = ? AND page_type = ? AND status <> ?",
				kbID, identity.CanonicalSlug, candidate.PageType, types.WikiPageStatusArchived,
			).First(&registered).Error
			registeredValid := registeredErr == nil &&
				types.NormalizeWikiIdentityTitle(registered.Title) == identityKey
			if registeredErr != nil && !errors.Is(registeredErr, gorm.ErrRecordNotFound) {
				return registeredErr
			}
			// A registry row whose page has not been created yet is still
			// authoritative: it may belong to a concurrent worker between
			// reservation and persist. Only retarget an identity when another
			// live page already proves the registry stale.
			if !registeredValid && existingCanonicalSlug != "" && existingCanonicalSlug != identity.CanonicalSlug {
				identity.CanonicalSlug = canonicalSlug
				identity.UpdatedAt = time.Now()
				if err := tx.Model(&types.WikiCanonicalIdentity{}).
					Where("id = ?", identity.ID).
					Updates(map[string]interface{}{
						"canonical_slug": canonicalSlug,
						"updated_at":     identity.UpdatedAt,
					}).Error; err != nil {
					return err
				}
			}
			resolved[candidate.Slug] = identity.CanonicalSlug
		}
		return nil
	})
	return resolved, err
}

func (r *wikiPageRepository) bestExistingCanonicalSlug(
	db *gorm.DB, kbID, pageType, identityKey string,
) (string, error) {
	var pages []*types.WikiPage
	query := db.Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND page_type = ? AND status <> ?", kbID, pageType, types.WikiPageStatusArchived)
	if db.Dialector.Name() == "postgres" {
		query = query.Where("regexp_replace(lower(trim(title)), '[^[:alnum:]]+', '', 'g') = ?", identityKey)
	}
	if err := query.Find(&pages).Error; err != nil {
		return "", err
	}
	filtered := pages[:0]
	for _, page := range pages {
		if types.NormalizeWikiIdentityTitle(page.Title) == identityKey {
			filtered = append(filtered, page)
		}
	}
	if len(filtered) == 0 {
		return "", nil
	}
	sort.SliceStable(filtered, func(i, j int) bool { return betterWikiCanonicalPage(filtered[i], filtered[j]) })
	return filtered[0].Slug, nil
}

// ReconcileCanonicalWikiPages is a repeatable convergence pass. It only
// archives a duplicate after the canonical page covers every source document;
// ambiguous groups remain live and will be reconsidered on a later finalize.
func (r *wikiPageRepository) ReconcileCanonicalWikiPages(
	ctx context.Context, kbID string, affectedSlugs []string,
) (*types.WikiCanonicalReconcileResult, error) {
	result := &types.WikiCanonicalReconcileResult{Aliases: make(map[string]string)}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		targetKeys, err := r.reconcileWikiIdentityKeys(tx, kbID, affectedSlugs)
		if err != nil || (len(affectedSlugs) > 0 && len(targetKeys) == 0) {
			return err
		}
		var pages []*types.WikiPage
		pageQuery := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("knowledge_base_id = ? AND page_type IN ? AND status <> ?", kbID,
				[]string{types.WikiPageTypeEntity, types.WikiPageTypeConcept}, types.WikiPageStatusArchived)
		if len(targetKeys) > 0 && tx.Dialector.Name() == "postgres" {
			var scopes []string
			var args []interface{}
			for key := range targetKeys {
				parts := strings.SplitN(key, "\x00", 2)
				if len(parts) != 2 {
					continue
				}
				scopes = append(scopes, "(page_type = ? AND regexp_replace(lower(trim(title)), '[^[:alnum:]]+', '', 'g') = ?)")
				args = append(args, parts[0], parts[1])
			}
			if len(scopes) > 0 {
				pageQuery = pageQuery.Where("("+strings.Join(scopes, " OR ")+")", args...)
			}
		}
		if err := pageQuery.Find(&pages).Error; err != nil {
			return err
		}

		groups := make(map[string][]*types.WikiPage)
		for _, page := range pages {
			identityKey := types.NormalizeWikiIdentityTitle(page.Title)
			groupKey := page.PageType + "\x00" + identityKey
			if identityKey != "" && (len(targetKeys) == 0 || targetKeys[groupKey]) {
				groups[groupKey] = append(groups[groupKey], page)
			}
		}
		keys := make([]string, 0, len(groups))
		for key := range groups {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			group := groups[key]
			if len(group) < 2 {
				continue
			}
			sort.SliceStable(group, func(i, j int) bool { return betterWikiCanonicalPage(group[i], group[j]) })
			canonical := group[0]
			identityKey := types.NormalizeWikiIdentityTitle(canonical.Title)
			var registered types.WikiCanonicalIdentity
			registeredErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("knowledge_base_id = ? AND page_type = ? AND identity_key = ?", kbID, canonical.PageType, identityKey).
				First(&registered).Error
			if registeredErr != nil && !errors.Is(registeredErr, gorm.ErrRecordNotFound) {
				return registeredErr
			}
			if registeredErr == nil {
				for _, page := range group {
					if page.Slug == registered.CanonicalSlug {
						canonical = page
						break
					}
				}
			}
			identity := types.WikiCanonicalIdentity{
				TenantID: canonical.TenantID, KnowledgeBaseID: kbID, PageType: canonical.PageType,
				IdentityKey: identityKey, CanonicalSlug: canonical.Slug,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "knowledge_base_id"}, {Name: "page_type"}, {Name: "identity_key"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"canonical_slug": canonical.Slug, "updated_at": time.Now(),
				}),
			}).Create(&identity).Error; err != nil {
				return err
			}

			for _, duplicate := range group {
				if duplicate.ID == canonical.ID {
					continue
				}
				if !wikiSourceRefsCover(canonical.SourceRefs, duplicate.SourceRefs) {
					result.DeferredPages++
					continue
				}
				canonical.ChunkRefs = mergeWikiStringArrays(canonical.ChunkRefs, duplicate.ChunkRefs)
				canonical.Aliases = mergeWikiStringArrays(canonical.Aliases, duplicate.Aliases)
				canonical.InLinks = mergeWikiStringArrays(canonical.InLinks, duplicate.InLinks)
				canonical.OutLinks = mergeWikiStringArrays(canonical.OutLinks, duplicate.OutLinks)
				if duplicate.Title != canonical.Title {
					canonical.Aliases = mergeWikiStringArrays(canonical.Aliases, types.StringArray{duplicate.Title})
				}
				if err := tx.Model(&types.WikiPage{}).Where("id = ?", canonical.ID).Updates(map[string]interface{}{
					"chunk_refs": canonical.ChunkRefs, "aliases": canonical.Aliases,
					"in_links": canonical.InLinks, "out_links": canonical.OutLinks, "updated_at": time.Now(),
				}).Error; err != nil {
					return err
				}
				if err := migrateWikiSlugCheckpointReferences(tx, kbID, duplicate.Slug, canonical.Slug); err != nil {
					return err
				}
				if err := tx.Model(&types.WikiPage{}).Where("id = ? AND status <> ?", duplicate.ID, types.WikiPageStatusArchived).
					Updates(map[string]interface{}{"status": types.WikiPageStatusArchived, "updated_at": time.Now()}).Error; err != nil {
					return err
				}
				audit := types.WikiPageMergeAudit{
					TenantID: duplicate.TenantID, KnowledgeBaseID: kbID, PageType: duplicate.PageType,
					IdentityKey: identityKey, CanonicalPageID: canonical.ID, CanonicalSlug: canonical.Slug,
					MergedPageID: duplicate.ID, MergedSlug: duplicate.Slug, Reason: "canonical_source_coverage",
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&audit).Error; err != nil {
					return err
				}
				result.Aliases[duplicate.Slug] = canonical.Slug
				result.MergedPages++
			}
		}
		return rewriteWikiSlugReferences(tx, kbID, result.Aliases)
	})
	if len(result.Aliases) == 0 {
		result.Aliases = nil
	}
	return result, err
}

func (r *wikiPageRepository) reconcileWikiIdentityKeys(
	tx *gorm.DB, kbID string, affectedSlugs []string,
) (map[string]bool, error) {
	if len(affectedSlugs) == 0 {
		return nil, nil
	}
	var pages []*types.WikiPage
	if err := tx.Select("slug, title, page_type").
		Where("knowledge_base_id = ? AND slug IN ? AND page_type IN ? AND status <> ?", kbID, affectedSlugs,
			[]string{types.WikiPageTypeEntity, types.WikiPageTypeConcept}, types.WikiPageStatusArchived).
		Find(&pages).Error; err != nil {
		return nil, err
	}
	keys := make(map[string]bool, len(pages))
	for _, page := range pages {
		identityKey := types.NormalizeWikiIdentityTitle(page.Title)
		if identityKey != "" {
			keys[page.PageType+"\x00"+identityKey] = true
		}
	}
	return keys, nil
}

func betterWikiCanonicalPage(left, right *types.WikiPage) bool {
	if len(wikiSourceRefIDs(left.SourceRefs)) != len(wikiSourceRefIDs(right.SourceRefs)) {
		return len(wikiSourceRefIDs(left.SourceRefs)) > len(wikiSourceRefIDs(right.SourceRefs))
	}
	if left.Version != right.Version {
		return left.Version > right.Version
	}
	if len([]rune(left.Content)) != len([]rune(right.Content)) {
		return len([]rune(left.Content)) > len([]rune(right.Content))
	}
	if !left.CreatedAt.Equal(right.CreatedAt) {
		return left.CreatedAt.Before(right.CreatedAt)
	}
	return left.Slug < right.Slug
}

func wikiSourceRefIDs(refs types.StringArray) map[string]struct{} {
	out := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		id := strings.TrimSpace(strings.SplitN(ref, "|", 2)[0])
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func wikiSourceRefsCover(canonical, duplicate types.StringArray) bool {
	covered := wikiSourceRefIDs(canonical)
	for id := range wikiSourceRefIDs(duplicate) {
		if _, ok := covered[id]; !ok {
			return false
		}
	}
	return true
}

func mergeWikiStringArrays(left, right types.StringArray) types.StringArray {
	seen := make(map[string]struct{}, len(left)+len(right))
	out := make(types.StringArray, 0, len(left)+len(right))
	for _, value := range append(append(types.StringArray(nil), left...), right...) {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func rewriteWikiSlugReferences(tx *gorm.DB, kbID string, aliases map[string]string) error {
	if len(aliases) == 0 {
		return nil
	}
	oldSlugs := make([]string, 0, len(aliases))
	for oldSlug := range aliases {
		oldSlugs = append(oldSlugs, oldSlug)
	}
	sort.Strings(oldSlugs)

	var pages []*types.WikiPage
	if err := tx.Where("knowledge_base_id = ? AND status <> ? AND slug NOT IN ?", kbID,
		types.WikiPageStatusArchived, oldSlugs).Find(&pages).Error; err != nil {
		return err
	}
	for _, page := range pages {
		updates := make(map[string]interface{})
		content := page.Content
		summary := page.Summary
		parentSlug := page.ParentSlug
		inLinks := page.InLinks
		outLinks := page.OutLinks
		for _, oldSlug := range oldSlugs {
			canonicalSlug := aliases[oldSlug]
			content = rewriteExactWikiSlug(content, oldSlug, canonicalSlug)
			summary = rewriteExactWikiSlug(summary, oldSlug, canonicalSlug)
			if parentSlug == oldSlug {
				parentSlug = canonicalSlug
			}
			inLinks, _ = replaceWikiArrayValue(inLinks, oldSlug, canonicalSlug)
			outLinks, _ = replaceWikiArrayValue(outLinks, oldSlug, canonicalSlug)
		}
		if content != page.Content {
			updates["content"] = content
		}
		if summary != page.Summary {
			updates["summary"] = summary
		}
		if parentSlug != page.ParentSlug {
			updates["parent_slug"] = parentSlug
		}
		if !wikiStringArraysEqual(inLinks, page.InLinks) {
			updates["in_links"] = inLinks
		}
		if !wikiStringArraysEqual(outLinks, page.OutLinks) {
			updates["out_links"] = outLinks
		}
		if len(updates) > 0 {
			updates["updated_at"] = time.Now()
			if err := tx.Model(&types.WikiPage{}).Where("id = ?", page.ID).Updates(updates).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func rewriteExactWikiSlug(value, oldSlug, canonicalSlug string) string {
	value = strings.ReplaceAll(value, "[["+oldSlug+"]]", "[["+canonicalSlug+"]]")
	return strings.ReplaceAll(value, "[["+oldSlug+"|", "[["+canonicalSlug+"|")
}

func wikiStringArraysEqual(left, right types.StringArray) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func migrateWikiSlugCheckpointReferences(tx *gorm.DB, kbID, oldSlug, canonicalSlug string) error {
	if tx.Migrator().HasTable(&types.WikiSlugContributionMarker{}) &&
		tx.Migrator().HasTable(&types.WikiIngestWorkUnit{}) {
		if err := migrateWikiContributionMarkers(tx, kbID, oldSlug, canonicalSlug); err != nil {
			return err
		}
	}
	if tx.Migrator().HasTable(&types.WikiSlugApplication{}) {
		if err := tx.Model(&types.WikiSlugApplication{}).
			Where("knowledge_base_id = ? AND slug = ?", kbID, oldSlug).
			Update("slug", canonicalSlug).Error; err != nil {
			return err
		}
	}
	if tx.Migrator().HasTable(&types.WikiPageIssue{}) {
		return tx.Model(&types.WikiPageIssue{}).
			Where("knowledge_base_id = ? AND slug = ?", kbID, oldSlug).
			Update("slug", canonicalSlug).Error
	}
	return nil
}

func migrateWikiContributionMarkers(tx *gorm.DB, kbID, oldSlug, canonicalSlug string) error {
	// A work unit may already have published both spellings. Remove only the
	// row that would collide, then rewrite every remaining marker so replay
	// recognizes the canonical contribution as already published.
	if tx.Dialector.Name() == "postgres" {
		if err := tx.Exec(`DELETE FROM wiki_slug_contribution_markers old_marker
			USING wiki_slug_contribution_markers canonical_marker, wiki_ingest_work_units work_unit
			WHERE old_marker.slug = ? AND canonical_marker.slug = ?
			  AND old_marker.work_id = canonical_marker.work_id
			  AND old_marker.operation_digest = canonical_marker.operation_digest
			  AND work_unit.work_id = old_marker.work_id
			  AND work_unit.knowledge_base_id = ?`, oldSlug, canonicalSlug, kbID).Error; err != nil {
			return err
		}
	} else {
		if err := tx.Exec(`DELETE FROM wiki_slug_contribution_markers
			WHERE slug = ? AND EXISTS (
				SELECT 1 FROM wiki_slug_contribution_markers canonical_marker
				WHERE canonical_marker.slug = ?
				  AND canonical_marker.work_id = wiki_slug_contribution_markers.work_id
				  AND canonical_marker.operation_digest = wiki_slug_contribution_markers.operation_digest
			) AND work_id IN (
				SELECT work_id FROM wiki_ingest_work_units WHERE knowledge_base_id = ?
			)`, oldSlug, canonicalSlug, kbID).Error; err != nil {
			return err
		}
	}
	return tx.Model(&types.WikiSlugContributionMarker{}).
		Where("slug = ? AND work_id IN (?)", oldSlug,
			tx.Model(&types.WikiIngestWorkUnit{}).Select("work_id").Where("knowledge_base_id = ?", kbID)).
		Update("slug", canonicalSlug).Error
}

func replaceWikiArrayValue(values types.StringArray, oldValue, newValue string) (types.StringArray, bool) {
	changed := false
	out := make(types.StringArray, 0, len(values))
	for _, value := range values {
		if value == oldValue {
			value = newValue
			changed = true
		}
		out = append(out, value)
	}
	if !changed {
		return values, false
	}
	return mergeWikiStringArrays(nil, out), true
}

// escapeLikePattern escapes LIKE / ILIKE metacharacters so the returned string
// can be safely concatenated with % wildcards without unintended matches.
// Order matters: escape the backslash first, then the wildcards.
func escapeLikePattern(s string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return replacer.Replace(s)
}

// Search performs case-insensitive POSIX regex search on wiki pages within a knowledge base.
// The query is interpreted as a PostgreSQL regular expression (via ~*).
//
// Results are ranked by where the query hit, highest-relevance first:
//
//	title    hit → rank 4 (most obvious intent: user typed what the page is called)
//	slug     hit → rank 3 (url-like identifiers, direct jump)
//	summary  hit → rank 2 (short authored abstract)
//	content  hit → rank 1 (body mention — often surfaces unrelated pages whose
//	                       prose merely mentions the query as trivia)
//
// Without this ranking, a user searching for "王新" on a 4万-page wiki will
// see pages like "华为" or "Index" ahead of the actual 王新 page just
// because they mention 王新 in their body and were updated more recently.
// updated_at stays as the tiebreaker so same-rank ties stay deterministic.
func (r *wikiPageRepository) Search(ctx context.Context, kbID string, query string, limit int) ([]*types.WikiPage, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}

	// CASE expression is evaluated per-row during SELECT; we order by the
	// alias so the DB only computes the rank once. Parameterized four
	// times with the same regex to avoid coupling to GORM's positional
	// arg rewriting quirks.
	rankExpr := "CASE " +
		"WHEN title ~* ? THEN 4 " +
		"WHEN slug ~* ? THEN 3 " +
		"WHEN summary ~* ? THEN 2 " +
		"WHEN content ~* ? THEN 1 " +
		"ELSE 0 END AS match_rank"

	var pages []*types.WikiPage
	if err := r.db.WithContext(ctx).
		Select("*, "+rankExpr, query, query, query, query).
		Where("knowledge_base_id = ? AND (title ~* ? OR content ~* ? OR summary ~* ? OR slug ~* ?)",
			kbID, query, query, query, query).
		Where("status != ?", "archived").
		Order("match_rank DESC, updated_at DESC").
		Limit(limit).
		Find(&pages).Error; err != nil {
		return nil, err
	}
	return pages, nil
}

// CountByType returns page counts grouped by type for a knowledge base
func (r *wikiPageRepository) CountByType(ctx context.Context, kbID string) (map[string]int64, error) {
	type result struct {
		PageType string
		Count    int64
	}
	var results []result
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Select("page_type, count(*) as count").
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived).
		Group("page_type").
		Scan(&results).Error; err != nil {
		return nil, err
	}

	counts := make(map[string]int64)
	for _, r := range results {
		counts[r.PageType] = r.Count
	}
	return counts, nil
}

// CountOrphans returns the number of pages with no inbound links
func (r *wikiPageRepository) CountOrphans(ctx context.Context, kbID string) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&types.WikiPage{}).
		Where("knowledge_base_id = ? AND status <> ?", kbID, types.WikiPageStatusArchived).
		Where(r.wikiEmptyInLinksPredicate()).
		// Exclude the index page because it is naturally a root page.
		Where("page_type <> ?", types.WikiPageTypeIndex).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *wikiPageRepository) CreateIssue(ctx context.Context, issue *types.WikiPageIssue) error {
	return r.db.WithContext(ctx).Create(issue).Error
}

func (r *wikiPageRepository) ListIssues(ctx context.Context, kbID string, slug string, status string) ([]*types.WikiPageIssue, error) {
	query := r.db.WithContext(ctx).Where("knowledge_base_id = ?", kbID)
	if slug != "" {
		query = query.Where("slug = ?", slug)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var issues []*types.WikiPageIssue
	if err := query.Order("created_at DESC").Find(&issues).Error; err != nil {
		return nil, err
	}
	return issues, nil
}

func (r *wikiPageRepository) UpdateIssueStatus(ctx context.Context, issueID string, status string) error {
	return r.db.WithContext(ctx).Model(&types.WikiPageIssue{}).
		Where("id = ?", issueID).
		Update("status", status).Error
}
