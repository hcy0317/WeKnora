package service

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// cleanupWikiReferences removes the old KB's source links after a knowledge
// transfer while preserving pages that still cite other documents.
func (s *knowledgeService) cleanupWikiReferences(
	ctx context.Context,
	knowledge *types.Knowledge,
	sourceChunkRefs map[string]bool,
) error {
	if knowledge == nil || knowledge.KnowledgeBaseID == "" || knowledge.ID == "" {
		return nil
	}
	if s.wikiRepo == nil || s.wikiService == nil {
		return nil
	}
	kbID, knowledgeID := knowledge.KnowledgeBaseID, knowledge.ID
	var cleanupErr error
	if s.redisClient != nil {
		if err := s.redisClient.Set(ctx, WikiDeletedTombstoneKey(kbID, knowledgeID), "1", wikiDeletedTTL).Err(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if s.taskPendingRepo != nil {
		if err := s.taskPendingRepo.DeleteByDedupKey(ctx, wikiTaskType, wikiTaskScope, kbID, knowledgeID, WikiOpIngest); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	docTitle := knowledge.Title
	if docTitle == "" {
		docTitle = knowledge.FileName
	}
	if docTitle == "" {
		docTitle = knowledgeID
	}
	docSummary := knowledge.Description
	pages, err := s.wikiRepo.ListBySourceRef(ctx, kbID, knowledgeID)
	if err != nil {
		logger.Warnf(ctx, "wiki cleanup: failed to list pages by source ref %s: %v", knowledgeID, err)
		return errors.Join(cleanupErr, err)
	}
	for _, page := range pages {
		if page.PageType == types.WikiPageTypeSummary && page.Summary != "" {
			docSummary = page.Summary
			break
		}
	}

	var pageSlugs, folderIDs []string
	for _, page := range pages {
		if page.PageType != types.WikiPageTypeIndex {
			pageSlugs = append(pageSlugs, page.Slug)
			folderIDs = append(folderIDs, page.FolderID)
		}
	}
	tenantID, _ := types.TenantIDFromContext(ctx)
	if err := enqueueWikiRetract(ctx, s.task, s.taskPendingRepo, WikiRetractPayload{
		TenantID: tenantID, KnowledgeBaseID: kbID, KnowledgeID: knowledgeID,
		DocTitle: docTitle, DocSummary: docSummary, Language: types.LanguageFromContextOrDefault(ctx),
		PageSlugs: pageSlugs, FolderIDs: uniqueWikiFolderIDs(folderIDs),
	}); err != nil {
		return errors.Join(cleanupErr, err)
	}

	var deletedSlugs []string
	for _, page := range pages {
		if page.PageType == types.WikiPageTypeIndex {
			continue
		}
		remaining := removeSourceRef(page.SourceRefs, knowledgeID)
		if len(remaining) == 0 {
			if err := s.wikiService.DeletePage(ctx, kbID, page.Slug); err != nil {
				logger.Warnf(ctx, "wiki cleanup: failed to delete page %s: %v", page.Slug, err)
				cleanupErr = errors.Join(cleanupErr, err)
			} else {
				deletedSlugs = append(deletedSlugs, page.Slug)
			}
			continue
		}
		page.SourceRefs = remaining
		page.ChunkRefs = removeChunkRefs(page.ChunkRefs, sourceChunkRefs)
		if err := s.wikiService.UpdatePageMeta(ctx, page); err != nil {
			logger.Warnf(ctx, "wiki cleanup: failed to update source refs for page %s: %v", page.Slug, err)
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	if len(deletedSlugs) > 0 {
		logger.Infof(ctx, "wiki cleanup: deleted %d pages after knowledge %s deletion: %v",
			len(deletedSlugs), knowledgeID, deletedSlugs)
	}
	return cleanupErr
}
