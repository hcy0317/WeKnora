package service

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const wikiIdentityReservationPrefix = "wiki:identity:"

type wikiCanonicalPageService interface {
	ResolveCanonicalWikiPageSlugs(
		ctx context.Context, tenantID uint64, kbID string, candidates []types.WikiCanonicalCandidate,
	) (map[string]string, error)
	ReconcileCanonicalWikiPages(ctx context.Context, kbID string, affectedSlugs []string) (*types.WikiCanonicalReconcileResult, error)
}

const reserveWikiIdentityLua = `
local canonical = false
if ARGV[3] == '1' then
  canonical = ARGV[1]
else
  for _, key in ipairs(KEYS) do
    local value = redis.call('GET', key)
    if value and (not canonical or value < canonical) then
      canonical = value
    end
  end
end
if not canonical then
  canonical = ARGV[1]
end
for _, key in ipairs(KEYS) do
  redis.call('SET', key, canonical, 'PX', ARGV[2])
end
return canonical
`

// canonicalizeBatchSlugUpdates collapses exact identity variants emitted by
// documents mapped concurrently in the same batch. A slug already persisted
// in the wiki wins; otherwise canonicalizeItemRecords applies deterministic
// entity/type and lexical ordering.
func canonicalizeBatchSlugUpdates(
	updates map[string][]SlugUpdate,
	existingSlugs map[string]bool,
) (map[string][]SlugUpdate, map[string]string) {
	if len(updates) == 0 {
		return updates, nil
	}

	keys := make([]string, 0, len(updates))
	for slug := range updates {
		keys = append(keys, slug)
	}
	sort.Strings(keys)

	records := make([]canonicalItemRecord, 0)
	for _, slug := range keys {
		for _, update := range updates[slug] {
			if update.Type != types.WikiPageTypeEntity && update.Type != types.WikiPageTypeConcept {
				continue
			}
			item := update.Item
			if item.Slug == "" {
				item.Slug = update.Slug
			}
			records = append(records, canonicalItemRecord{
				item:      item,
				kind:      update.Type,
				preferred: existingSlugs[item.Slug],
			})
		}
	}
	_, aliases := canonicalizeItemRecords(records)
	if len(aliases) == 0 {
		return updates, nil
	}
	return rewriteBatchSlugUpdates(updates, aliases), aliases
}

// resolveDurableCanonicalAliases binds every exact same-type title identity to
// a persistent canonical slug. Unlike the Redis reservation, this survives
// worker restarts and arbitrarily distant ingest batches.
func (s *wikiIngestService) resolveDurableCanonicalAliases(
	ctx context.Context,
	tenantID uint64,
	kbID string,
	updates map[string][]SlugUpdate,
) (map[string]string, error) {
	canonicalizer, ok := s.wikiService.(wikiCanonicalPageService)
	if !ok {
		// Lightweight test adapters predate the production-only capability.
		return nil, nil
	}
	keys := make([]string, 0, len(updates))
	for slug := range updates {
		keys = append(keys, slug)
	}
	sort.Strings(keys)
	candidates := make([]types.WikiCanonicalCandidate, 0, len(keys))
	for _, slug := range keys {
		for _, update := range updates[slug] {
			if update.Type != types.WikiPageTypeEntity && update.Type != types.WikiPageTypeConcept {
				continue
			}
			title := strings.TrimSpace(update.Item.Name)
			if title == "" {
				continue
			}
			candidates = append(candidates, types.WikiCanonicalCandidate{
				Slug: slug, Title: title, PageType: update.Type,
			})
			break
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	resolved, err := canonicalizer.ResolveCanonicalWikiPageSlugs(ctx, tenantID, kbID, candidates)
	if err != nil {
		return nil, err
	}
	aliases := make(map[string]string)
	for proposed, canonical := range resolved {
		if canonical != "" && canonical != proposed {
			aliases[proposed] = canonical
		}
	}
	if len(aliases) == 0 {
		return nil, nil
	}
	return aliases, nil
}

func rewriteBatchSlugUpdates(updates map[string][]SlugUpdate, aliases map[string]string) map[string][]SlugUpdate {
	if len(aliases) == 0 {
		return updates
	}
	keys := make([]string, 0, len(updates))
	for slug := range updates {
		keys = append(keys, slug)
	}
	sort.Strings(keys)
	rewritten := make(map[string][]SlugUpdate, len(updates))
	for _, bucketSlug := range keys {
		for _, original := range updates[bucketSlug] {
			update := original
			targetBucket := bucketSlug
			if update.Type == types.WikiPageTypeEntity || update.Type == types.WikiPageTypeConcept {
				canonical := resolveCanonicalSlug(update.Slug, aliases)
				if canonical == "" {
					canonical = update.Slug
				}
				update.Slug = canonical
				update.Item.Slug = canonical
				if kind := slugKind(canonical); kind == types.WikiPageTypeEntity || kind == types.WikiPageTypeConcept {
					update.Type = kind
				}
				targetBucket = canonical
			}
			update.SummaryBody = rewriteWikiSlugAliases(update.SummaryBody, aliases)
			update.SummaryLine = rewriteWikiSlugAliases(update.SummaryLine, aliases)
			update.DocSummary = rewriteWikiSlugAliases(update.DocSummary, aliases)
			rewritten[targetBucket] = append(rewritten[targetBucket], update)
		}
	}
	return rewritten
}

// reserveConcurrentCanonicalAliases coordinates fresh identities across
// concurrently running batches for the same KB. The reservation is temporary:
// once a page exists, normal DB dedup is authoritative; if a writer fails, the
// next batch can safely materialize the reserved slug itself.
func (s *wikiIngestService) reserveConcurrentCanonicalAliases(
	ctx context.Context,
	kbID string,
	updates map[string][]SlugUpdate,
	existingSlugs map[string]bool,
) map[string]string {
	// Lite mode already serializes the whole KB. Standard mode has Redis and
	// permits multiple disjoint map batches, which is where this guard matters.
	if s == nil || s.redisClient == nil || len(updates) == 0 {
		return nil
	}
	slugs := make([]string, 0, len(updates))
	for slug := range updates {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)

	aliases := make(map[string]string)
	for _, slug := range slugs {
		identities := make(map[string]struct{})
		for _, update := range updates[slug] {
			if update.Type == types.WikiPageTypeEntity || update.Type == types.WikiPageTypeConcept {
				for surface := range extractedItemIdentitySurfaces(update.Item) {
					identities["surface:"+surface] = struct{}{}
				}
			}
		}
		if compactSlug := compactSlugIdentity(slug); compactSlug != "" {
			identities["slug:"+slugKind(slug)+":"+compactSlug] = struct{}{}
		}
		if len(identities) == 0 {
			continue
		}
		keys := make([]string, 0, len(identities))
		for identity := range identities {
			keys = append(keys, wikiIdentityReservationPrefix+kbID+":"+wikiCheckpointDigest(identity))
		}
		sort.Strings(keys)
		forceExisting := "0"
		if existingSlugs[slug] {
			forceExisting = "1"
		}
		result, err := s.redisClient.Eval(ctx, reserveWikiIdentityLua, keys,
			slug, strconv.FormatInt(wikiSlugLockTTL.Milliseconds(), 10),
			forceExisting).Result()
		if err != nil {
			logger.Warnf(ctx, "wiki ingest: reserve canonical identity for %s failed: %v", slug, err)
			continue
		}
		canonical, ok := result.(string)
		if ok && canonical != "" && canonical != slug {
			aliases[slug] = canonical
		}
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

// existingAliasesForRestoredUpdates revalidates durable map checkpoints
// against the wiki's current pages. A checkpoint can predate another
// document's successful reduce, so replaying its raw generated slug without
// this probe could recreate the spelling variant that the first run merged.
func (s *wikiIngestService) existingAliasesForRestoredUpdates(
	ctx context.Context,
	kbID string,
	updates map[string][]SlugUpdate,
	restoredKnowledgeIDs map[string]bool,
) map[string]string {
	if s == nil || s.wikiService == nil || len(restoredKnowledgeIDs) == 0 {
		return nil
	}
	aliases := make(map[string]string)
	seen := make(map[string]bool)
	for slug, bucket := range updates {
		for _, update := range bucket {
			if seen[slug] || !restoredKnowledgeIDs[update.KnowledgeID] ||
				(update.Type != types.WikiPageTypeEntity && update.Type != types.WikiPageTypeConcept) {
				continue
			}
			seen[slug] = true
			item := update.Item
			if item.Slug == "" {
				item.Slug = slug
			}
			candidatePages := make(map[string]*types.WikiPageLite)
			candidates := make(map[string]bool)
			queries := append([]string{item.Name}, item.Aliases...)
			for _, query := range queries {
				query = strings.TrimSpace(query)
				if query == "" {
					continue
				}
				pages, err := s.wikiService.FindSimilarPages(ctx, kbID, query,
					[]string{types.WikiPageTypeEntity, types.WikiPageTypeConcept}, dedupCandidateTopK)
				if err != nil {
					logger.Warnf(ctx, "wiki ingest: restored checkpoint canonical probe %q failed: %v", query, err)
					continue
				}
				for _, page := range pages {
					if page == nil || page.Slug == "" {
						continue
					}
					candidatePages[page.Slug] = page
					candidates[page.Slug] = true
				}
			}
			if target := deterministicExistingMergeTarget(item, update.Type, candidates, candidatePages); target != "" && target != slug {
				aliases[slug] = target
			}
		}
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

func resolveCanonicalSlug(slug string, aliases map[string]string) string {
	seen := make(map[string]bool)
	for aliases[slug] != "" && !seen[slug] {
		seen[slug] = true
		slug = aliases[slug]
	}
	return slug
}

func rewriteWikiSlugAliases(content string, aliases map[string]string) string {
	if content == "" || len(aliases) == 0 {
		return content
	}
	oldSlugs := make([]string, 0, len(aliases))
	for oldSlug := range aliases {
		oldSlugs = append(oldSlugs, oldSlug)
	}
	sort.Slice(oldSlugs, func(i, j int) bool {
		if len(oldSlugs[i]) != len(oldSlugs[j]) {
			return len(oldSlugs[i]) > len(oldSlugs[j])
		}
		return oldSlugs[i] < oldSlugs[j]
	})
	for _, oldSlug := range oldSlugs {
		canonical := resolveCanonicalSlug(oldSlug, aliases)
		if canonical != "" && canonical != oldSlug {
			content = strings.ReplaceAll(content, "[["+oldSlug, "[["+canonical)
		}
	}
	return content
}

// extractedItemSlugAliases describes canonical changes between two extraction
// stages (currently candidate extraction -> citation augmentation). It is used
// to repair summary links that were generated concurrently from the earlier
// candidate inventory.
func extractedItemSlugAliases(
	previousEntities, previousConcepts, currentEntities, currentConcepts []extractedItem,
) map[string]string {
	previous := make([]canonicalItemRecord, 0, len(previousEntities)+len(previousConcepts))
	current := make([]canonicalItemRecord, 0, len(currentEntities)+len(currentConcepts))
	for _, item := range previousEntities {
		previous = append(previous, canonicalItemRecord{item: item, kind: types.WikiPageTypeEntity})
	}
	for _, item := range previousConcepts {
		previous = append(previous, canonicalItemRecord{item: item, kind: types.WikiPageTypeConcept})
	}
	for _, item := range currentEntities {
		current = append(current, canonicalItemRecord{item: item, kind: types.WikiPageTypeEntity})
	}
	for _, item := range currentConcepts {
		current = append(current, canonicalItemRecord{item: item, kind: types.WikiPageTypeConcept})
	}

	aliases := make(map[string]string)
	for _, oldItem := range previous {
		var targets []string
		for _, newItem := range current {
			if canonicalRecordsShareIdentity(oldItem, newItem) && newItem.item.Slug != "" {
				targets = append(targets, newItem.item.Slug)
			}
		}
		if len(targets) == 0 {
			continue
		}
		sort.Strings(targets)
		if oldItem.item.Slug != "" && oldItem.item.Slug != targets[0] {
			aliases[oldItem.item.Slug] = targets[0]
		}
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

// rewriteDocResultSlugs keeps map-phase observability and completion
// bookkeeping aligned with the canonical update buckets.
func rewriteDocResultSlugs(results []*docIngestResult, aliases map[string]string) {
	if len(aliases) == 0 {
		return
	}
	for _, result := range results {
		if result == nil {
			continue
		}
		result.Summary = rewriteWikiSlugAliases(result.Summary, aliases)
		seen := make(map[string]bool, len(result.Pages))
		pages := make([]wikiIngestPageRef, 0, len(result.Pages))
		for _, page := range result.Pages {
			page.Slug = resolveCanonicalSlug(page.Slug, aliases)
			if page.Slug == "" || seen[page.Slug] {
				continue
			}
			seen[page.Slug] = true
			pages = append(pages, page)
		}
		result.Pages = pages
	}
}
