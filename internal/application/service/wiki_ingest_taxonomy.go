package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// wikiTaxonomyItem is one entity/concept page to be filed into the directory.
type wikiTaxonomyItem struct {
	slug     string
	title    string
	pageType string
	about    string
}

type wikiTaxonomyChunkLedger struct {
	Chunks map[string]map[string][]string `json:"chunks"`
}

func wikiTaxonomyChunkKey(ordinal int, items []wikiTaxonomyItem) string {
	snapshot := make([]struct {
		Slug, Title, PageType, About string
	}, 0, len(items))
	for _, item := range items {
		snapshot = append(snapshot, struct{ Slug, Title, PageType, About string }{
			Slug: item.slug, Title: item.title, PageType: item.pageType, About: item.about,
		})
	}
	encoded, _ := json.Marshal(snapshot)
	return fmt.Sprintf("%04d:%s", ordinal, wikiCheckpointDigest(string(encoded)))
}

func parseExactTaxonomyChunkAssignments(raw string, items []wikiTaxonomyItem) (map[string][]string, error) {
	raw = cleanLLMJSON(raw)
	var parsed struct {
		Assignments []struct {
			Slug string          `json:"slug"`
			Path json.RawMessage `json:"path"`
		} `json:"assignments"`
	}
	if raw == "" || json.Unmarshal([]byte(raw), &parsed) != nil {
		return nil, errors.New("taxonomy chunk output is malformed")
	}
	expected := make(map[string]struct{}, len(items))
	for _, item := range items {
		expected[item.slug] = struct{}{}
	}
	result := make(map[string][]string, len(items))
	for _, assignment := range parsed.Assignments {
		slug := strings.TrimSpace(assignment.Slug)
		if _, ok := expected[slug]; !ok {
			return nil, fmt.Errorf("taxonomy chunk output contains unexpected slug %q", slug)
		}
		if _, duplicate := result[slug]; duplicate {
			return nil, fmt.Errorf("taxonomy chunk output contains duplicate slug %q", slug)
		}
		if len(assignment.Path) == 0 || string(assignment.Path) == "null" {
			return nil, fmt.Errorf("taxonomy chunk output has invalid path for %q", slug)
		}
		var path []string
		if err := json.Unmarshal(assignment.Path, &path); err != nil {
			return nil, fmt.Errorf("taxonomy chunk output has invalid path for %q: %w", slug, err)
		}
		result[slug] = types.CleanWikiCategoryPath(path)
	}
	if len(result) != len(expected) {
		return nil, fmt.Errorf("taxonomy chunk output coverage mismatch: got %d want %d", len(result), len(expected))
	}
	return result, nil
}

func validateStoredTaxonomyChunk(assignments map[string][]string, items []wikiTaxonomyItem) error {
	if len(assignments) != len(items) {
		return errors.New("stored taxonomy chunk coverage differs")
	}
	for _, item := range items {
		if _, ok := assignments[item.slug]; !ok {
			return fmt.Errorf("stored taxonomy chunk is missing %q", item.slug)
		}
	}
	return nil
}

// planBatchTaxonomy assigns a directory path to every entity/concept slug in the
// batch in ONE planning pass (chunked for large batches), so the whole set lands
// on a single coherent tree that reuses existing folders. This replaces per-page,
// parallel CATEGORY invention — which couldn't converge, worst of all on the
// founding batch when the KB has no folders to anchor on. The returned map is
// keyed by slug; an entry may be an empty slice when the item is unclassifiable.
// Reduce applies these only to pages that don't already have a category.
func (s *wikiIngestService) planBatchTaxonomy(
	ctx context.Context,
	chatModel chat.Chat,
	kb *types.KnowledgeBase,
	slugUpdates map[string][]SlugUpdate,
	lang string,
) (map[string][]string, error) {
	if kb == nil {
		return nil, nil
	}
	items := collectTaxonomyItems(slugUpdates)
	if len(items) == 0 {
		return nil, nil
	}

	store, err := s.checkpointStore()
	if err != nil {
		return nil, err
	}
	workSet := make(map[string]struct{})
	missingSlugs := make([]string, 0, len(items))
	for _, item := range items {
		missingSlugs = append(missingSlugs, item.slug)
		for _, update := range slugUpdates[item.slug] {
			if update.WorkID != "" {
				workSet[update.WorkID] = struct{}{}
			}
		}
	}
	workIDs := make([]string, 0, len(workSet))
	for workID := range workSet {
		workIDs = append(workIDs, workID)
	}
	sort.Strings(workIDs)
	sort.Strings(missingSlugs)
	workDigest := wikiCheckpointDigest(workIDs...)
	missingDigest := wikiCheckpointDigest(missingSlugs...)
	contract := wikiCheckpointDigest(agent.WikiTaxonomyPlanPrompt)
	mapped, err := store.FindMappedWikiTaxonomyPlan(ctx, kb.TenantID, kb.ID, workDigest, missingDigest, contract)
	if err != nil {
		return nil, fmt.Errorf("find mapped taxonomy checkpoint: %w", err)
	}
	if mapped != nil {
		var restored map[string][]string
		if err := json.Unmarshal(mapped.ResolvedOutput, &restored); err != nil {
			return nil, fmt.Errorf("restore taxonomy checkpoint: %w", err)
		}
		return restored, nil
	}

	// No completed logical plan exists. Only now snapshot the folder base used
	// by this generation; prepared plans remain base-specific so drift before a
	// successful model result supersedes rather than contaminates the old row.
	var pool [][]string
	if s.wikiService != nil {
		paths, listErr := s.wikiService.ListDistinctCategoryPaths(ctx, kb.ID, wikiTaxonomyFolderPoolMax)
		if listErr != nil {
			logger.Warnf(ctx, "wiki ingest: list category paths for plan failed: %v", listErr)
		} else {
			pool = paths
		}
	}
	existing := s.selectRelevantFolders(ctx, kb, items, pool)
	basePaths := append([][]string(nil), existing...)
	sort.Slice(basePaths, func(i, j int) bool { return strings.Join(basePaths[i], "\x00") < strings.Join(basePaths[j], "\x00") })
	baseJSON, _ := json.Marshal(basePaths)
	baseDigest := wikiCheckpointDigest(string(baseJSON))
	planID := wikiCheckpointDigest(fmt.Sprintf("%d", kb.TenantID), kb.ID, workDigest, missingDigest, contract, baseDigest)
	plan, err := store.PrepareWikiTaxonomyPlan(ctx, &types.WikiTaxonomyPlan{
		PlanID: planID, TenantID: kb.TenantID, KnowledgeBaseID: kb.ID,
		WorkSetDigest: workDigest, MissingSetDigest: missingDigest, FolderBaseDigest: baseDigest,
		ContractKey: contract, State: types.WikiTaxonomyPlanPrepared, ResolvedOutput: types.JSON([]byte(`{}`)),
	})
	if err != nil {
		return nil, fmt.Errorf("prepare taxonomy checkpoint: %w", err)
	}
	if plan.State == types.WikiTaxonomyPlanMapped {
		var restored map[string][]string
		if err := json.Unmarshal(plan.ResolvedOutput, &restored); err != nil {
			return nil, fmt.Errorf("restore taxonomy checkpoint: %w", err)
		}
		return restored, nil
	}
	ledger := wikiTaxonomyChunkLedger{Chunks: make(map[string]map[string][]string)}
	progressOutput := append(types.JSON(nil), plan.ResolvedOutput...)
	if len(progressOutput) == 0 {
		progressOutput = types.JSON([]byte(`{}`))
	}
	if string(bytes.TrimSpace(progressOutput)) != "{}" {
		if err := json.Unmarshal(progressOutput, &ledger); err != nil {
			return nil, fmt.Errorf("restore taxonomy chunk ledger: %w", err)
		}
		if ledger.Chunks == nil {
			return nil, errors.New("restore taxonomy chunk ledger: chunks are missing")
		}
	}

	result := make(map[string][]string, len(items))
	for ordinal, start := 0, 0; start < len(items); ordinal, start = ordinal+1, start+wikiTaxonomyPlanChunkSize {
		end := start + wikiTaxonomyPlanChunkSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[start:end]
		chunkKey := wikiTaxonomyChunkKey(ordinal, chunk)
		if saved, ok := ledger.Chunks[chunkKey]; ok {
			if err := validateStoredTaxonomyChunk(saved, chunk); err != nil {
				return nil, fmt.Errorf("restore taxonomy chunk %d: %w", ordinal, err)
			}
			for _, item := range chunk {
				path := saved[item.slug]
				result[item.slug] = path
				if len(path) > 0 {
					existing = append(existing, path)
				}
			}
			continue
		}

		tree := formatExistingTaxonomyForPrompt(existing)
		if strings.TrimSpace(tree) == "" {
			tree = wikiTaxonomyEmptyTreeHint
		}

		var itemsBlock strings.Builder
		for _, it := range chunk {
			fmt.Fprintf(&itemsBlock, "- slug: %s | title: %s | type: %s | about: %s\n",
				it.slug, it.title, it.pageType, previewText(it.about, 120))
		}

		raw, err := s.generateWithTemplate(ctx, chatModel, agent.WikiTaxonomyPlanPrompt, map[string]string{
			"ExistingTaxonomy": tree,
			"Items":            itemsBlock.String(),
			"Language":         lang,
		})
		if err != nil {
			return nil, fmt.Errorf("taxonomy plan chunk %d generation failed: %w", ordinal, err)
		}
		assignments, err := parseExactTaxonomyChunkAssignments(raw, chunk)
		if err != nil {
			return nil, fmt.Errorf("taxonomy plan chunk %d validation failed: %w", ordinal, err)
		}
		ledger.Chunks[chunkKey] = assignments
		encodedProgress, err := json.Marshal(ledger)
		if err != nil {
			return nil, fmt.Errorf("encode taxonomy chunk %d checkpoint: %w", ordinal, err)
		}
		if err := store.SaveWikiTaxonomyPlanProgress(ctx, plan.PlanID, progressOutput, types.JSON(encodedProgress)); err != nil {
			return nil, fmt.Errorf("persist taxonomy chunk %d checkpoint: %w", ordinal, err)
		}
		progressOutput = types.JSON(encodedProgress)
		for _, item := range chunk {
			path := assignments[item.slug]
			result[item.slug] = path
			if len(path) > 0 {
				existing = append(existing, path)
			}
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode taxonomy checkpoint: %w", err)
	}
	if err := store.MarkWikiTaxonomyPlanMapped(ctx, plan.PlanID, types.JSON(encoded)); err != nil {
		return nil, fmt.Errorf("persist taxonomy checkpoint: %w", err)
	}
	return result, nil
}

// resolvePlannedFolders reifies the planner's per-slug paths into real
// wiki_folders rows and returns a slug -> folder id map. Folder creation is
// done here, sequentially and before the parallel reduce phase, so reduce only
// assigns pre-resolved ids and never races two goroutines into creating the
// same folder. Distinct paths are resolved once and cached. Blank paths (and
// any resolution failure) map to the root and are simply omitted.
func (s *wikiIngestService) resolvePlannedFolders(
	ctx context.Context, kb *types.KnowledgeBase, planned map[string][]string,
) map[string]string {
	if kb == nil || len(planned) == 0 || s.wikiService == nil {
		return nil
	}
	pathCache := make(map[string]string) // "a/b" -> folder id
	out := make(map[string]string, len(planned))
	for slug, path := range planned {
		clean := types.CleanWikiCategoryPath(path)
		if len(clean) == 0 {
			continue
		}
		key := strings.Join(clean, "/")
		fid, ok := pathCache[key]
		if !ok {
			resolved, _, err := s.wikiService.FindOrCreateFolderPath(ctx, kb.ID, kb.TenantID, clean)
			if err != nil {
				logger.Warnf(ctx, "wiki ingest: resolve folder %q failed: %v", key, err)
				pathCache[key] = "" // negative-cache so we don't retry per slug
				continue
			}
			fid = resolved
			pathCache[key] = fid
		}
		if fid != "" {
			out[slug] = fid
		}
	}
	return out
}

// selectRelevantFolders narrows the existing folder pool to the subset worth
// showing the planner for THIS batch. A healthy navigation directory is small,
// so it is fed whole (perfect reuse recall, no embedding cost). Only once folders
// are numerous does similarity preprocessing kick in: all level-1 folders are
// always kept as coarse anchors, and each item pulls in its nearest deeper
// folders by embedding similarity. KBs without an embedding model (wiki-only)
// fall back to a capped feed-all.
func (s *wikiIngestService) selectRelevantFolders(
	ctx context.Context, kb *types.KnowledgeBase, items []wikiTaxonomyItem, pool [][]string,
) [][]string {
	if len(pool) <= wikiTaxonomyFeedAllMaxFolders {
		return pool
	}

	// Split into always-kept level-1 anchors and the deeper candidate folders
	// that similarity selects among.
	l1Seen := make(map[string]struct{})
	var l1Paths, deeper [][]string
	for _, p := range pool {
		if len(p) == 0 {
			continue
		}
		if _, ok := l1Seen[p[0]]; !ok {
			l1Seen[p[0]] = struct{}{}
			l1Paths = append(l1Paths, []string{p[0]})
		}
		if len(p) >= 2 {
			deeper = append(deeper, p)
		}
	}

	// Gate purely on whether an embedding model is configured — NOT on
	// NeedsEmbeddingModel(), which is false for wiki-only KBs that may still
	// opt into an embedding model purely for directory/taxonomy similarity.
	if strings.TrimSpace(kb.EmbeddingModelID) == "" || len(deeper) == 0 {
		return capFolders(pool, wikiTaxonomyPromptMaxPaths)
	}

	embedder, err := s.modelService.GetEmbeddingModel(ctx, kb.EmbeddingModelID)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: taxonomy plan embed model unavailable, feeding all folders: %v", err)
		return capFolders(pool, wikiTaxonomyPromptMaxPaths)
	}

	folderTexts := make([]string, len(deeper))
	for i, p := range deeper {
		folderTexts[i] = strings.Join(p, " / ")
	}
	itemTexts := make([]string, len(items))
	for i, it := range items {
		itemTexts[i] = strings.TrimSpace(it.title + " " + previewText(it.about, 120))
	}

	folderVecs, err := embedder.BatchEmbed(ctx, folderTexts)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: taxonomy plan folder embed failed, feeding all folders: %v", err)
		return capFolders(pool, wikiTaxonomyPromptMaxPaths)
	}
	itemVecs, err := embedder.BatchEmbed(ctx, itemTexts)
	if err != nil {
		logger.Warnf(ctx, "wiki ingest: taxonomy plan item embed failed, feeding all folders: %v", err)
		return capFolders(pool, wikiTaxonomyPromptMaxPaths)
	}

	selected := append(l1Paths, selectFoldersByVectors(deeper, folderVecs, itemVecs, wikiTaxonomyRelevantTopK)...)
	return capFolders(selected, wikiTaxonomyPromptMaxPaths)
}

// selectFoldersByVectors returns the deeper folders that rank in any item's
// top-K by cosine similarity, preserving the input order for determinism.
func selectFoldersByVectors(deeper [][]string, folderVecs, itemVecs [][]float32, topK int) [][]string {
	if len(deeper) != len(folderVecs) || len(itemVecs) == 0 || topK <= 0 {
		return nil
	}
	chosen := make(map[int]struct{})
	for _, iv := range itemVecs {
		type scored struct {
			idx int
			sim float64
		}
		ranking := make([]scored, 0, len(folderVecs))
		for fi, fv := range folderVecs {
			ranking = append(ranking, scored{fi, cosineSimilarity(iv, fv)})
		}
		sort.SliceStable(ranking, func(a, b int) bool { return ranking[a].sim > ranking[b].sim })
		for k := 0; k < topK && k < len(ranking); k++ {
			chosen[ranking[k].idx] = struct{}{}
		}
	}
	out := make([][]string, 0, len(chosen))
	for i := range deeper {
		if _, ok := chosen[i]; ok {
			out = append(out, deeper[i])
		}
	}
	return out
}

// cosineSimilarity returns the cosine of two equal-length vectors, or 0 for empty
// / mismatched / zero-norm inputs.
func cosineSimilarity(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// capFolders truncates a folder list to at most max entries (max <= 0 = no cap).
func capFolders(paths [][]string, max int) [][]string {
	if max > 0 && len(paths) > max {
		return paths[:max]
	}
	return paths
}

// collectTaxonomyItems extracts the entity/concept pages from a batch's slug
// updates, in deterministic slug order so chunk boundaries are stable. Summary
// and retract-only slugs are skipped (they carry no directory category).
func collectTaxonomyItems(slugUpdates map[string][]SlugUpdate) []wikiTaxonomyItem {
	slugs := make([]string, 0, len(slugUpdates))
	for slug := range slugUpdates {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)

	items := make([]wikiTaxonomyItem, 0, len(slugs))
	for _, slug := range slugs {
		for _, u := range slugUpdates[slug] {
			if u.Type != types.WikiPageTypeEntity && u.Type != types.WikiPageTypeConcept {
				continue
			}
			title := strings.TrimSpace(u.Item.Name)
			if title == "" {
				title = slug
			}
			items = append(items, wikiTaxonomyItem{
				slug:     slug,
				title:    title,
				pageType: u.Type,
				about:    strings.TrimSpace(u.Item.Description),
			})
			break // one entry per slug is enough for classification
		}
	}
	return items
}
