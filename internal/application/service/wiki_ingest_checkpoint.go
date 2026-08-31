package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type wikiMappedCheckpoint struct {
	DocTitle string              `json:"doc_title"`
	Summary  string              `json:"summary"`
	Pages    []wikiIngestPageRef `json:"pages"`
	MapStats types.JSONMap       `json:"map_stats"`
	Updates  []SlugUpdate        `json:"updates"`
}

type wikiSlugGeneratedCheckpoint struct {
	Page       *types.WikiPage `json:"page"`
	BaseExists bool            `json:"base_exists"`
}

func wikiCheckpointDigest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(strconv.Itoa(len(part))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func wikiSourceRevisionDigest(chunks []*types.Chunk) string {
	ordered := append([]*types.Chunk(nil), chunks...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i] == nil {
			return false
		}
		if ordered[j] == nil {
			return true
		}
		if ordered[i].ChunkIndex == ordered[j].ChunkIndex {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].ChunkIndex < ordered[j].ChunkIndex
	})
	parts := make([]string, 0, len(ordered)*2)
	for _, chunk := range ordered {
		if chunk == nil {
			continue
		}
		parts = append(parts, chunk.ID, strconv.Itoa(chunk.ContentRevision))
	}
	return wikiCheckpointDigest(parts...)
}

func wikiGenerationContractDigest(batchCtx *WikiBatchContext) string {
	parts := []string{
		agent.WikiCandidateSlugPrompt, agent.WikiKnowledgeExtractPrompt,
		agent.WikiChunkCitationPrompt, agent.WikiSummaryPrompt,
		agent.WikiPageModifySystemPrompt, agent.WikiPageModifyUserPrompt,
		agent.WikiTaxonomyPlanPrompt,
	}
	if batchCtx != nil {
		parts = append(parts, string(batchCtx.ExtractionGranularity.Normalize()),
			batchCtx.ContentInstructions, batchCtx.ExtractionInstructions)
	}
	return wikiCheckpointDigest(parts...)
}

func wikiRuntimeSnapshotDigest(chatModel chat.Chat, lang string) string {
	modelID := ""
	if chatModel != nil {
		modelID = chatModel.GetModelID()
	}
	return wikiCheckpointDigest(modelID, types.ResolveLanguageName(context.Background(), lang))
}

func (s *wikiIngestService) checkpointStore() (interfaces.WikiIngestCheckpointStore, error) {
	store, ok := s.wikiService.(interfaces.WikiIngestCheckpointStore)
	if !ok || store == nil {
		return nil, errors.New("wiki ingest checkpoint store is unavailable")
	}
	return store, nil
}

func (s *wikiIngestService) prepareWikiWorkUnit(
	ctx context.Context,
	chatModel chat.Chat,
	payload WikiIngestPayload,
	knowledgeID, docTitle, lang string,
	chunks []*types.Chunk,
	batchCtx *WikiBatchContext,
	wikiSpan *Span,
) (*types.WikiIngestWorkUnit, error) {
	store, err := s.checkpointStore()
	if err != nil {
		return nil, err
	}
	sourceRevision := wikiSourceRevisionDigest(chunks)
	sourceDocumentKey := wikiCheckpointDigest(strings.TrimSpace(docTitle))
	contract := wikiGenerationContractDigest(batchCtx)
	runtimeSnapshot := wikiRuntimeSnapshotDigest(chatModel, lang)
	workID := wikiCheckpointDigest(strconv.FormatUint(payload.TenantID, 10), payload.KnowledgeBaseID,
		knowledgeID, sourceRevision, sourceDocumentKey, contract, runtimeSnapshot)
	unit := &types.WikiIngestWorkUnit{
		WorkID: workID, TenantID: payload.TenantID, KnowledgeBaseID: payload.KnowledgeBaseID,
		KnowledgeID: knowledgeID, SourceRevisionDigest: sourceRevision,
		SourceDocumentKey:     sourceDocumentKey,
		GenerationContractKey: contract, RuntimeSnapshotKey: runtimeSnapshot,
		State: types.WikiIngestWorkUnitPrepared, MappedOutput: types.JSON([]byte(`{}`)),
	}
	if wikiSpan != nil && wikiSpan.Attempt > 0 {
		return store.PrepareAndBindWikiIngestWorkUnit(ctx, types.WikiIngestWorkBinding{
			KnowledgeID: knowledgeID, Attempt: wikiSpan.Attempt, SpanID: wikiSpan.SpanID,
			SourceRevisionDigest: sourceRevision, SourceDocumentKey: sourceDocumentKey,
		}, unit)
	}
	return store.PrepareWikiIngestWorkUnit(ctx, unit)
}

func wikiGenerationScopeForWorkUnit(
	payload WikiIngestPayload, op WikiPendingOp, unit *types.WikiIngestWorkUnit,
) wikiGenerationScope {
	if unit == nil {
		return wikiGenerationScope{}
	}
	scope := wikiGenerationScope{
		TenantID: payload.TenantID, KnowledgeBaseID: payload.KnowledgeBaseID,
		WorkRevision: unit.WorkID, RuntimeSnapshot: unit.RuntimeSnapshotKey,
	}
	// Partial-repair publication copies the bound work id into the durable Wiki
	// op. That is explicit operator authorization for a new paid-call budget,
	// but only for fragments whose base ledger is ambiguous or terminal.
	// Asynq redelivery keeps the same attempt, so the recovery revision stays
	// stable and cannot multiply calls inside one repair.
	if op.Attempt > 0 && strings.TrimSpace(op.WorkID) == unit.WorkID {
		scope.RecoveryRevision = wikiCheckpointDigest(
			unit.WorkID, "partial-retry", strconv.Itoa(op.Attempt))
	}
	return scope
}

func restoreWikiMappedCheckpoint(
	unit *types.WikiIngestWorkUnit, op WikiPendingOp, wikiSpan *Span,
) (*docIngestResult, []SlugUpdate, error) {
	if unit == nil || unit.State != types.WikiIngestWorkUnitMapped || len(unit.MappedOutput) == 0 {
		return nil, nil, errors.New("restore wiki mapped checkpoint: mapped work unit is required")
	}
	var mapped wikiMappedCheckpoint
	if err := json.Unmarshal(unit.MappedOutput, &mapped); err != nil {
		return nil, nil, fmt.Errorf("restore wiki mapped checkpoint: %w", err)
	}
	for i := range mapped.Updates {
		mapped.Updates[i].WorkID = unit.WorkID
		mapped.Updates[i].Attempt = op.Attempt
	}
	op.WorkID = unit.WorkID
	return &docIngestResult{
		KnowledgeID: op.KnowledgeID, WorkID: unit.WorkID, CheckpointReused: true, SourceOp: op,
		DocTitle: mapped.DocTitle, Summary: mapped.Summary, Pages: mapped.Pages,
		MapStats: mapped.MapStats, WikiSpan: wikiSpan,
	}, mapped.Updates, nil
}

func (s *wikiIngestService) persistWikiMappedCheckpoint(
	ctx context.Context, unit *types.WikiIngestWorkUnit, result *docIngestResult, updates []SlugUpdate,
) error {
	if unit == nil || result == nil {
		return errors.New("persist wiki mapped checkpoint: unit and result are required")
	}
	for i := range updates {
		updates[i].WorkID = unit.WorkID
	}
	result.WorkID = unit.WorkID
	result.SourceOp.WorkID = unit.WorkID
	encoded, err := json.Marshal(wikiMappedCheckpoint{
		DocTitle: result.DocTitle, Summary: result.Summary, Pages: result.Pages,
		MapStats: result.MapStats, Updates: updates,
	})
	if err != nil {
		return fmt.Errorf("marshal wiki mapped checkpoint: %w", err)
	}
	store, err := s.checkpointStore()
	if err != nil {
		return err
	}
	return store.MarkWikiIngestWorkUnitMapped(ctx, unit.WorkID, types.JSON(encoded))
}

func wikiSlugOperationDigest(update SlugUpdate) string {
	copy := update
	copy.Attempt = 0
	copy.WorkID = ""
	copy.ApplicationPlanID = ""
	encoded, _ := json.Marshal(copy)
	return wikiCheckpointDigest(string(encoded))
}

func wikiContributionMarkerKey(workID, slug, operationDigest string) string {
	return strings.Join([]string{workID, slug, operationDigest}, "\x00")
}

func (s *wikiIngestService) filterPublishedWikiContributions(
	ctx context.Context, updatesBySlug map[string][]SlugUpdate,
) (map[string][]SlugUpdate, map[wikiWorkKey]map[string]struct{}, error) {
	workSet := make(map[string]struct{})
	for _, updates := range updatesBySlug {
		for _, update := range updates {
			if update.WorkID != "" {
				workSet[update.WorkID] = struct{}{}
			}
		}
	}
	if len(workSet) == 0 {
		return updatesBySlug, nil, nil
	}
	workIDs := make([]string, 0, len(workSet))
	for workID := range workSet {
		workIDs = append(workIDs, workID)
	}
	sort.Strings(workIDs)
	store, err := s.checkpointStore()
	if err != nil {
		return nil, nil, err
	}
	markers, err := store.ListWikiSlugContributionMarkers(ctx, workIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("load wiki contribution markers: %w", err)
	}
	published := make(map[string]struct{}, len(markers))
	for _, marker := range markers {
		if marker.State == types.WikiSlugApplicationPublished {
			published[wikiContributionMarkerKey(marker.WorkID, marker.Slug, marker.OperationDigest)] = struct{}{}
		}
	}
	filtered := make(map[string][]SlugUpdate, len(updatesBySlug))
	reused := make(map[wikiWorkKey]map[string]struct{})
	for slug, updates := range updatesBySlug {
		for _, update := range updates {
			digest := wikiSlugOperationDigest(update)
			if _, ok := published[wikiContributionMarkerKey(update.WorkID, slug, digest)]; ok {
				workKey := wikiSlugUpdateWorkKey(update)
				if reused[workKey] == nil {
					reused[workKey] = make(map[string]struct{})
				}
				reused[workKey][slug] = struct{}{}
				continue
			}
			filtered[slug] = append(filtered[slug], update)
		}
		if len(filtered[slug]) == 0 {
			delete(filtered, slug)
		}
	}
	return filtered, reused, nil
}

func wikiPageCheckpointHash(page *types.WikiPage, exists bool) string {
	if !exists || page == nil {
		return wikiCheckpointDigest("missing")
	}
	encoded, _ := json.Marshal(struct {
		Version    int               `json:"version"`
		Title      string            `json:"title"`
		Content    string            `json:"content"`
		Summary    string            `json:"summary"`
		PageType   string            `json:"page_type"`
		Status     string            `json:"status"`
		SourceRefs types.StringArray `json:"source_refs"`
		ChunkRefs  types.StringArray `json:"chunk_refs"`
		OutLinks   types.StringArray `json:"out_links"`
	}{
		Version: page.Version, Title: page.Title, Content: page.Content,
		Summary: page.Summary, PageType: page.PageType, Status: string(page.Status),
		SourceRefs: page.SourceRefs, ChunkRefs: page.ChunkRefs, OutLinks: page.OutLinks,
	})
	return wikiCheckpointDigest(string(encoded))
}

func wikiPageMaterialHash(page *types.WikiPage, exists bool) string {
	if !exists || page == nil {
		return wikiCheckpointDigest("missing")
	}
	encoded, _ := json.Marshal(struct {
		Title      string            `json:"title"`
		Content    string            `json:"content"`
		Summary    string            `json:"summary"`
		PageType   string            `json:"page_type"`
		Status     string            `json:"status"`
		FolderID   string            `json:"folder_id"`
		SourceRefs types.StringArray `json:"source_refs"`
		ChunkRefs  types.StringArray `json:"chunk_refs"`
		Aliases    types.StringArray `json:"aliases"`
	}{
		Title: page.Title, Content: page.Content, Summary: page.Summary,
		PageType: page.PageType, Status: string(page.Status), FolderID: page.FolderID,
		SourceRefs: page.SourceRefs, ChunkRefs: page.ChunkRefs, Aliases: page.Aliases,
	})
	return wikiCheckpointDigest(string(encoded))
}

func (s *wikiIngestService) prepareWikiSlugApplication(
	ctx context.Context, tenantID uint64, kbID, slug string, updates []SlugUpdate,
	page *types.WikiPage, exists bool,
) (*types.WikiSlugApplication, []types.WikiSlugContributionMarker, *wikiSlugGeneratedCheckpoint, bool, error) {
	contributionKey, operationDigest, markers := wikiSlugContributionIdentity(slug, updates)
	if len(markers) == 0 {
		return nil, nil, nil, false, nil
	}
	store, err := s.checkpointStore()
	if err != nil {
		return nil, nil, nil, false, err
	}
	prior, err := store.FindWikiSlugApplication(ctx, tenantID, kbID, slug, contributionKey)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if prior != nil && (prior.State == types.WikiSlugApplicationApplying || prior.State == types.WikiSlugApplicationPublished) {
		var generated wikiSlugGeneratedCheckpoint
		if err := json.Unmarshal([]byte(prior.GeneratedOutput), &generated); err != nil {
			return nil, nil, nil, false, fmt.Errorf("restore wiki slug application %s: %w", prior.PlanID, err)
		}
		bindWikiSlugApplicationPlan(updates, prior.PlanID)
		if wikiPageCheckpointHash(page, exists) == prior.ExpectedPageHash {
			return prior, markers, &generated, false, nil
		}
		if wikiPageMaterialHash(page, exists) == wikiPageMaterialHash(generated.Page, generated.Page != nil) {
			return prior, markers, &generated, true, nil
		}
		// The base changed independently. Preparing the new base-specific plan
		// below abandons this stale applying plan under the slug lock.
	}
	expectedVersion := 0
	if exists && page != nil {
		expectedVersion = page.Version
	}
	baseHash := wikiPageCheckpointHash(page, exists)
	planID := wikiCheckpointDigest(fmt.Sprintf("%d", tenantID), kbID, slug, contributionKey,
		baseHash, operationDigest)
	application, err := store.PrepareWikiSlugApplication(ctx, &types.WikiSlugApplication{
		PlanID: planID, TenantID: tenantID, KnowledgeBaseID: kbID, Slug: slug,
		ContributionKey: contributionKey, ExpectedVersion: expectedVersion,
		ExpectedPageHash: baseHash, OperationDigest: operationDigest,
		State: types.WikiSlugApplicationPrepared,
	})
	if err != nil {
		return nil, nil, nil, false, err
	}
	bindWikiSlugApplicationPlan(updates, application.PlanID)
	return application, markers, nil, false, nil
}

func (s *wikiIngestService) markWikiSlugApplicationApplying(
	ctx context.Context, application *types.WikiSlugApplication,
	markers []types.WikiSlugContributionMarker, page *types.WikiPage, exists bool,
) (context.Context, error) {
	if application == nil {
		return ctx, nil
	}
	encoded, err := json.Marshal(wikiSlugGeneratedCheckpoint{Page: page, BaseExists: exists})
	if err != nil {
		return ctx, err
	}
	store, err := s.checkpointStore()
	if err != nil {
		return ctx, err
	}
	if err := store.MarkWikiSlugApplicationApplying(ctx, application.PlanID, string(encoded)); err != nil {
		return ctx, err
	}
	return types.WithWikiSlugApplicationTransition(ctx, types.WikiSlugApplicationTransition{
		PlanID: application.PlanID, State: types.WikiSlugApplicationApplying, Markers: markers,
	}), nil
}

func wikiSlugContributionIdentity(slug string, updates []SlugUpdate) (
	contributionKey, operationDigest string, markers []types.WikiSlugContributionMarker,
) {
	type contribution struct {
		workID string
		digest string
	}
	contributions := make([]contribution, 0, len(updates))
	seen := make(map[string]struct{})
	for _, update := range updates {
		if update.WorkID == "" {
			continue
		}
		digest := wikiSlugOperationDigest(update)
		key := wikiContributionMarkerKey(update.WorkID, slug, digest)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		contributions = append(contributions, contribution{workID: update.WorkID, digest: digest})
	}
	sort.Slice(contributions, func(i, j int) bool {
		if contributions[i].workID == contributions[j].workID {
			return contributions[i].digest < contributions[j].digest
		}
		return contributions[i].workID < contributions[j].workID
	})
	parts := make([]string, 0, len(contributions)*2)
	for _, contribution := range contributions {
		parts = append(parts, contribution.workID, contribution.digest)
		markers = append(markers, types.WikiSlugContributionMarker{
			WorkID: contribution.workID, Slug: slug, OperationDigest: contribution.digest,
		})
	}
	return wikiCheckpointDigest(parts...), wikiCheckpointDigest(append([]string{slug}, parts...)...), markers
}

func bindWikiSlugApplicationPlan(updates []SlugUpdate, planID string) {
	for i := range updates {
		updates[i].ApplicationPlanID = planID
	}
}
