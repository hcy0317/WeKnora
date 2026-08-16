package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/modelcontext"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"golang.org/x/sync/errgroup"
)

const (
	// maxRunesPerWikiExtractionBatch keeps each entity/concept extraction
	// response bounded. A single 32k-rune request can produce tens of thousands
	// of output tokens and be cut off after otherwise-valid JSON has begun;
	// bounded batches retain the existing per-call retry contract, share a
	// rolling canonical candidate inventory, and merge before any page is written.
	maxRunesPerWikiExtractionBatch = 6000

	// maxRunesPerCitationBatch bounds the size of a single chunk-citation
	// batch by rune count (a fast approximation of token count). Small enough
	// that batches stay comfortably inside the LLM's context and output
	// budgets, large enough that most short/medium documents classify in a
	// single batch. Tune via benchmarks if needed.
	maxRunesPerCitationBatch = 12000

	// maxCitationBatchConcurrency limits parallelism across chunk-citation
	// batches so a single long document can't saturate the synthesis model.
	maxCitationBatchConcurrency = 4
)

func runWikiExtractionBatches(
	content string,
	extract func(string, combinedExtraction) (combinedExtraction, error),
) (combinedExtraction, int, error) {
	runes := []rune(content)
	if len(runes) == 0 {
		return combinedExtraction{}, 0, nil
	}
	total := (len(runes) + maxRunesPerWikiExtractionBatch - 1) / maxRunesPerWikiExtractionBatch
	merged := combinedExtraction{}
	for start, index := 0, 0; start < len(runes); start, index = start+maxRunesPerWikiExtractionBatch, index+1 {
		end := min(start+maxRunesPerWikiExtractionBatch, len(runes))
		accumulated := combinedExtraction{
			Entities: append([]extractedItem(nil), merged.Entities...),
			Concepts: append([]extractedItem(nil), merged.Concepts...),
		}
		result, err := extract(string(runes[start:end]), accumulated)
		if err != nil {
			return combinedExtraction{}, index, fmt.Errorf("wiki extraction batch %d/%d: %w", index+1, total, err)
		}
		merged.Entities = append(merged.Entities, result.Entities...)
		merged.Concepts = append(merged.Concepts, result.Concepts...)
		merged.Entities, merged.Concepts = compactExtractedItems(merged.Entities, merged.Concepts)
	}
	return merged, total, nil
}

// renderExtractionPreviousSlugs combines persisted slugs from this source
// document with the canonical candidates accumulated from earlier shards.
// Passing this rolling inventory into every extraction call gives the model a
// chance to reuse the same slug before deterministic compaction is needed.
func renderExtractionPreviousSlugs(oldPageSlugs map[string]bool, accumulated combinedExtraction) string {
	items := make(map[string]string, len(oldPageSlugs)+len(accumulated.Entities)+len(accumulated.Concepts))
	for slug := range oldPageSlugs {
		if strings.HasPrefix(slug, "entity/") || strings.HasPrefix(slug, "concept/") {
			items[slug] = ""
		}
	}
	for _, item := range append(append([]extractedItem(nil), accumulated.Entities...), accumulated.Concepts...) {
		if item.Slug != "" {
			items[item.Slug] = item.Name
		}
	}
	if len(items) == 0 {
		return "(none — this is a new document)"
	}
	slugs := make([]string, 0, len(items))
	for slug := range items {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	var sb strings.Builder
	for _, slug := range slugs {
		if name := strings.TrimSpace(items[slug]); name != "" {
			fmt.Fprintf(&sb, "- %s = %s\n", slug, name)
		} else {
			fmt.Fprintf(&sb, "- %s\n", slug)
		}
	}
	return sb.String()
}

func mergeIndependentFactText(existing, incoming string) string {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	switch {
	case existing == "":
		return incoming
	case incoming == "", incoming == existing, strings.Contains(existing, incoming):
		return existing
	case strings.Contains(incoming, existing):
		return incoming
	default:
		return existing + "\n\n" + incoming
	}
}

func runCitationBatchWorkers(
	ctx context.Context,
	batches []chunkBatch,
	worker func(context.Context, int, chunkBatch) (citationBatchResult, error),
) ([]citationBatchResult, error) {
	results := make([]citationBatchResult, len(batches))
	eg, ectx := errgroup.WithContext(ctx)
	eg.SetLimit(maxCitationBatchConcurrency)
	for index := range batches {
		index := index
		eg.Go(func() error {
			result, err := worker(ectx, index, batches[index])
			if err != nil {
				return fmt.Errorf("citation batch %d/%d: %w", index+1, len(batches), err)
			}
			results[index] = result
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// citationBatchResult is the JSON shape we expect back from one invocation of
// WikiChunkCitationPrompt.
type citationBatchResult struct {
	Citations map[string][]string   `json:"citations"`
	NewSlugs  []newSlugFromCitation `json:"new_slugs"`
}

// newSlugFromCitation is the shape of an entry in the "new_slugs" array of
// WikiChunkCitationPrompt. Mirrors extractedItem but also carries a "type"
// tag because this prompt emits entities and concepts in a single array.
type newSlugFromCitation struct {
	Type         string   `json:"type"`
	Name         string   `json:"name"`
	Slug         string   `json:"slug"`
	Aliases      []string `json:"aliases"`
	Description  string   `json:"description"`
	Details      string   `json:"details"`
	SourceChunks []string `json:"source_chunks"`
}

// citationPipelineOutcome carries the raw numbers produced by the Pass
// 0 + classification flow so callers (mapOneDocument) can log a single
// unified stat line.
type citationPipelineOutcome struct {
	CandidateCount int
	ChunkCount     int
	BatchCount     int
	CitedChunks    int
	UncitedSlugs   int
	NewSlugCount   int
}

// extractCandidateSlugs runs Pass 0 of the chunk-cited pipeline: it scans the
// full (reconstructed) document text and returns a lightweight skeleton of
// every significant entity/concept. Unlike the legacy single-shot extraction,
// this pass explicitly does NOT ask the LLM to paraphrase full facts per item;
// those will come from the chunk-citation pass instead.
//
// Returns (entities, concepts, slugItems, error). On LLM or parse failure it
// returns an error — the caller can then fall back to the legacy extractor.
func (s *wikiIngestService) extractCandidateSlugs(
	ctx context.Context,
	chatModel chat.Chat,
	kbID string,
	content, lang string,
	oldPageSlugs map[string]bool,
	batchCtx *WikiBatchContext,
) ([]extractedItem, []extractedItem, map[string]extractedItem, error) {
	granularity := batchCtx.ExtractionGranularity.Normalize()
	result, batchCount, err := runWikiExtractionBatches(content, func(batch string, accumulated combinedExtraction) (combinedExtraction, error) {
		raw, generateErr := s.generateWithTemplate(ctx, chatModel, agent.WikiCandidateSlugPrompt, map[string]string{
			"Content":             batch,
			"Language":            lang,
			"PreviousSlugs":       renderExtractionPreviousSlugs(oldPageSlugs, accumulated),
			"Granularity":         string(granularity),
			"GranularityGuidance": agent.WikiGranularityGuidance(string(granularity)),
			"CustomInstructions":  batchCtx.ExtractionInstructions,
			"InstructionScope":    "wiki_extraction",
		})
		if generateErr != nil {
			return combinedExtraction{}, generateErr
		}
		raw = cleanLLMJSON(raw)
		var batchResult combinedExtraction
		if unmarshalErr := json.Unmarshal([]byte(raw), &batchResult); unmarshalErr != nil {
			logger.Warnf(ctx, "wiki ingest: failed to parse candidate slug JSON: %v\nRaw: %s", unmarshalErr, raw)
			return combinedExtraction{}, fmt.Errorf("parse candidate slug JSON: %w", unmarshalErr)
		}
		return batchResult, nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("candidate slug extraction failed: %w", err)
	}
	logger.Infof(ctx, "wiki ingest: candidate extraction completed in %d bounded batch(es)", batchCount)

	result.Entities, result.Concepts = s.deduplicateExtractedBatch(
		ctx, chatModel, kbID, result.Entities, result.Concepts,
	)

	slugItems := make(map[string]extractedItem, len(result.Entities)+len(result.Concepts))
	for _, item := range result.Entities {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}
	for _, item := range result.Concepts {
		if item.Slug != "" && item.Name != "" {
			slugItems[item.Slug] = item
		}
	}

	return result.Entities, result.Concepts, slugItems, nil
}

// chunkBatch groups chunks that will be sent in a single WikiChunkCitationPrompt call.
type chunkBatch struct {
	chunks       []*types.Chunk
	handles      *modelcontext.HandleTable
	totalRuneLen int
}

// splitChunksIntoCitationBatches partitions chunks into batches whose total
// rune count stays under maxRunesPerCitationBatch. Chunk order (by ChunkIndex)
// is preserved, and a chunk that by itself exceeds the budget occupies its
// own batch so we never silently drop content. Each chunk is assigned a short
// handle ("c000", "c001", ...) that the prompt uses in place of the raw UUID.
// The invocation-local table translates model output back to stable chunk IDs
// before any result reaches application state.
func splitChunksIntoCitationBatches(chunks []*types.Chunk) []chunkBatch {
	// Only cite text chunks — image/ocr chunks are already merged into the
	// text content via reconstructEnrichedContent and the LLM doesn't see
	// them as standalone units.
	filtered := make([]*types.Chunk, 0, len(chunks))
	for _, c := range chunks {
		if c == nil || c.Content == "" {
			continue
		}
		if c.ChunkType != types.ChunkTypeText && c.ChunkType != "" {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		return nil
	}

	// Preserve document order for human-readable citation
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].ChunkIndex == filtered[j].ChunkIndex {
			return filtered[i].StartAt < filtered[j].StartAt
		}
		return filtered[i].ChunkIndex < filtered[j].ChunkIndex
	})

	var batches []chunkBatch
	newBatch := func() chunkBatch {
		return chunkBatch{handles: modelcontext.NewHandleTable("c", 3, 0)}
	}
	current := newBatch()

	flush := func() {
		if len(current.chunks) == 0 {
			return
		}
		batches = append(batches, current)
		current = newBatch()
	}

	for _, c := range filtered {
		runeLen := len([]rune(c.Content))
		// If adding this chunk would exceed the budget and the current batch
		// isn't empty, flush first so the oversized chunk is not merged with
		// already-queued ones. Oversized chunks still go through — they just
		// get their own batch.
		if len(current.chunks) > 0 && current.totalRuneLen+runeLen > maxRunesPerCitationBatch {
			flush()
		}

		current.handles.Register(c.ID)
		current.chunks = append(current.chunks, c)
		current.totalRuneLen += runeLen
	}
	flush()

	return batches
}

// renderCandidateSlugsXML renders candidate slugs as a compact list suitable
// for the prompt's <candidate_slugs> block.
func renderCandidateSlugsXML(entities, concepts []extractedItem) string {
	var sb strings.Builder
	write := func(item extractedItem, kind string) {
		aliases := ""
		if len(item.Aliases) > 0 {
			aliases = fmt.Sprintf(" aliases=%q", strings.Join(item.Aliases, ", "))
		}
		fmt.Fprintf(&sb, "- slug: %s, type: %s, name: %q%s, description: %s\n",
			item.Slug, kind, item.Name, aliases, item.Description)
	}
	for _, item := range entities {
		if item.Slug == "" || item.Name == "" {
			continue
		}
		write(item, "entity")
	}
	for _, item := range concepts {
		if item.Slug == "" || item.Name == "" {
			continue
		}
		write(item, "concept")
	}
	return sb.String()
}

// renderChunksXML formats one batch's chunks into the <chunks> block, using
// per-batch handles (c000, c001, ...) instead of raw UUIDs.
func renderChunksXML(batch chunkBatch) string {
	var sb strings.Builder
	for _, c := range batch.chunks {
		handle, _ := batch.handles.Handle(c.ID)
		fmt.Fprintf(&sb, "<c id=%q index=\"%d\">\n%s\n</c>\n", handle, c.ChunkIndex, c.Content)
	}
	return sb.String()
}

// classifyChunkCitations runs Pass 1..N of the chunk-cited pipeline: given a
// set of candidate slugs (from Pass 0) and the document's chunks, it asks the
// LLM which chunks substantively discuss each candidate. Results across
// batches are merged into a single slug → union(chunk_id) map, and any
// "new_slugs" that Pass 0 missed are collected separately.
//
// Returns (citations, newSlugs, batchCount, error). citations is keyed by slug and
// contains real chunk UUIDs (already translated from batch handles). newSlugs
// likewise carry real chunk UUIDs in SourceChunks.
func (s *wikiIngestService) classifyChunkCitations(
	ctx context.Context,
	chatModel chat.Chat,
	candidatesXML string,
	chunks []*types.Chunk,
	lang string,
	batchCtx *WikiBatchContext,
) (map[string][]string, []newSlugFromCitation, int, error) {
	batches := splitChunksIntoCitationBatches(chunks)
	if len(batches) == 0 || strings.TrimSpace(candidatesXML) == "" {
		return map[string][]string{}, nil, 0, nil
	}

	parsedBatches, err := runCitationBatchWorkers(ctx, batches,
		func(batchCtx context.Context, batchIdx int, batch chunkBatch) (citationBatchResult, error) {
			raw, generateErr := s.generateWithTemplate(batchCtx, chatModel, agent.WikiChunkCitationPrompt, map[string]string{
				"CandidateSlugs": candidatesXML,
				"ChunksXML":      renderChunksXML(batch),
				"Language":       lang,
			})
			if generateErr != nil {
				return citationBatchResult{}, generateErr
			}
			raw = cleanLLMJSON(raw)
			var parsed citationBatchResult
			if unmarshalErr := json.Unmarshal([]byte(raw), &parsed); unmarshalErr != nil {
				logger.Warnf(batchCtx, "wiki ingest: citation batch %d parse failed: %v\nRaw: %s", batchIdx, unmarshalErr, raw)
				return citationBatchResult{}, fmt.Errorf("parse citation JSON: %w", unmarshalErr)
			}
			return parsed, nil
		})
	if err != nil {
		return nil, nil, len(batches), err
	}

	// Merge only after every batch has generated and parsed successfully.
	// Using sets keyed by (slug, chunkID) deduplicates across batches; order
	// is re-imposed from chunk ChunkIndex at the end.
	citationSet := make(map[string]map[string]bool) // slug → set of real chunk IDs
	var newSlugsAll []newSlugFromCitation
	for batchIdx, parsed := range parsedBatches {
		batch := batches[batchIdx]
		for slug, handleList := range parsed.Citations {
			if slug == "" {
				continue
			}
			set, ok := citationSet[slug]
			if !ok {
				set = make(map[string]bool)
				citationSet[slug] = set
			}
			for _, handle := range handleList {
				realID, known := batch.handles.Resolve(handle)
				if !known {
					logger.Warnf(ctx, "wiki ingest: citation batch %d referenced unknown chunk handle %q for slug %s", batchIdx, handle, slug)
					continue
				}
				set[realID] = true
			}
		}

		for _, ns := range parsed.NewSlugs {
			if ns.Slug == "" || ns.Name == "" {
				continue
			}
			real := make([]string, 0, len(ns.SourceChunks))
			for _, handle := range ns.SourceChunks {
				if id, ok := batch.handles.Resolve(handle); ok {
					real = append(real, id)
				}
			}
			ns.SourceChunks = real
			newSlugsAll = append(newSlugsAll, ns)
		}
	}

	// Build a stable chunk-order so the final citations come out in document order.
	chunkOrder := make(map[string]int, len(chunks))
	for _, c := range chunks {
		chunkOrder[c.ID] = c.ChunkIndex
	}

	out := make(map[string][]string, len(citationSet))
	for slug, set := range citationSet {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		sort.SliceStable(ids, func(i, j int) bool {
			return chunkOrder[ids[i]] < chunkOrder[ids[j]]
		})
		out[slug] = ids
	}

	return out, newSlugsAll, len(batches), nil
}

// resolveCitedChunks loads the content of every chunk referenced by the
// given additions in a single batched query per knowledge ID, returning a
// map[chunkID]content. Missing / out-of-tenant chunk IDs are silently skipped
// (logged at warn level) so the Reduce phase can gracefully fall back to the
// Details paraphrase.
func (s *wikiIngestService) resolveCitedChunks(
	ctx context.Context,
	tenantID uint64,
	additions []SlugUpdate,
) map[string]string {
	// Group chunk IDs by knowledge ID — chunk repo queries are scoped by
	// tenant but not by knowledge, so we could fetch everything in one go.
	// We batch per knowledge anyway to keep the IN(...) list bounded for
	// large multi-knowledge reduces.
	byKnowledge := make(map[string]map[string]bool)
	for _, add := range additions {
		if add.KnowledgeID == "" {
			continue
		}
		for _, chunkID := range add.SourceChunks {
			if chunkID == "" {
				continue
			}
			set, ok := byKnowledge[add.KnowledgeID]
			if !ok {
				set = make(map[string]bool)
				byKnowledge[add.KnowledgeID] = set
			}
			set[chunkID] = true
		}
	}
	if len(byKnowledge) == 0 {
		return nil
	}

	out := make(map[string]string)
	for _, idSet := range byKnowledge {
		ids := make([]string, 0, len(idSet))
		for id := range idSet {
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			continue
		}
		chunks, err := s.chunkRepo.ListChunksByID(ctx, tenantID, ids)
		if err != nil {
			logger.Warnf(ctx, "wiki ingest: failed to resolve cited chunks: %v", err)
			continue
		}
		for _, c := range chunks {
			if c == nil || c.Content == "" {
				continue
			}
			out[c.ID] = c.Content
		}
	}
	return out
}

// collectCitedChunkContent materializes the verbatim content of every
// referenced chunk, concatenated in the order provided. Chunk IDs that can't
// be resolved are silently dropped (logged upstream).
func collectCitedChunkContent(chunkIDs []string, contentByID map[string]string) string {
	if len(chunkIDs) == 0 || len(contentByID) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, id := range chunkIDs {
		content, ok := contentByID[id]
		if !ok || strings.TrimSpace(content) == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString(content)
	}
	return sb.String()
}

// mergeCitationsIntoItems backfills SourceChunks on every extractedItem from
// the citation map, and augments the item slices with any genuinely new slugs
// the citation pass discovered. Items whose slug is not in citations are left
// untouched — Reduce will fall back to Description/Details for them.
//
// Returns the updated entity and concept slices plus a count of candidates
// that ended with zero citations (for observability).
func mergeCitationsIntoItems(
	entities, concepts []extractedItem,
	citations map[string][]string,
	newSlugs []newSlugFromCitation,
) ([]extractedItem, []extractedItem, int) {
	uncited := 0

	for i := range entities {
		ids := citations[entities[i].Slug]
		entities[i].SourceChunks = ids
		if len(ids) == 0 {
			uncited++
		}
	}
	for i := range concepts {
		ids := citations[concepts[i].Slug]
		concepts[i].SourceChunks = ids
		if len(ids) == 0 {
			uncited++
		}
	}

	// Append new_slugs discovered by the citation pass, avoiding duplicates.
	existingSlugs := make(map[string]bool, len(entities)+len(concepts))
	for _, e := range entities {
		existingSlugs[e.Slug] = true
	}
	for _, c := range concepts {
		existingSlugs[c.Slug] = true
	}

	// Aggregate per-slug across batches so the same "newly discovered" slug
	// surfacing in multiple batches merges into a single item.
	type mergedNew struct {
		item extractedItem
		typ  string
	}
	merged := make(map[string]*mergedNew)
	slugOrder := []string{}

	for _, ns := range newSlugs {
		if ns.Slug == "" || ns.Name == "" {
			continue
		}
		if existingSlugs[ns.Slug] {
			// Treat as citation for existing candidate
			continue
		}
		kind := strings.TrimSpace(strings.ToLower(ns.Type))
		if kind == "" {
			if strings.HasPrefix(ns.Slug, "concept/") {
				kind = "concept"
			} else {
				kind = "entity"
			}
		}
		existing, ok := merged[ns.Slug]
		if !ok {
			existing = &mergedNew{
				item: extractedItem{
					Name:         ns.Name,
					Slug:         ns.Slug,
					Aliases:      append([]string(nil), ns.Aliases...),
					Description:  ns.Description,
					Details:      ns.Details,
					SourceChunks: append([]string(nil), ns.SourceChunks...),
				},
				typ: kind,
			}
			merged[ns.Slug] = existing
			slugOrder = append(slugOrder, ns.Slug)
			continue
		}
		// Union source chunks
		seen := make(map[string]bool, len(existing.item.SourceChunks))
		for _, id := range existing.item.SourceChunks {
			seen[id] = true
		}
		for _, id := range ns.SourceChunks {
			if !seen[id] {
				existing.item.SourceChunks = append(existing.item.SourceChunks, id)
				seen[id] = true
			}
		}
	}

	for _, slug := range slugOrder {
		m := merged[slug]
		if m.typ == "concept" {
			concepts = append(concepts, m.item)
		} else {
			entities = append(entities, m.item)
		}
	}

	entities, concepts = compactExtractedItems(entities, concepts)
	return entities, concepts, uncited
}
