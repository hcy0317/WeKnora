package service

import (
	"context"
	"strings"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// bindStoredImages grants knowledge resources access to images extracted while
// parsing. The resource catalog is also the authorization source used by file
// proxies, so every image must be bound before shared KB readers can load it.
func (s *knowledgeService) bindStoredImages(
	ctx context.Context, knowledge *types.Knowledge, images []docparser.StoredImage,
) {
	if s.resourceCatalog == nil || knowledge == nil || len(images) == 0 {
		return
	}
	bound := 0
	for _, img := range images {
		ref := strings.TrimSpace(img.ServingURL)
		if ref == "" {
			continue
		}
		resource, err := s.resourceCatalog.Resolve(ctx, ref)
		if err != nil || resource == nil {
			logger.Warnf(ctx, "Skip binding unknown stored image %s to knowledge %s: %v", ref, knowledge.ID, err)
			continue
		}
		if resource.TenantID != knowledge.TenantID {
			logger.Warnf(ctx, "Skip binding cross-workspace stored image %s to knowledge %s", ref, knowledge.ID)
			continue
		}
		if err := s.resourceCatalog.Bind(
			ctx, ref, types.ResourceOwnerKnowledge, knowledge.ID, types.ResourceRelationExtractedImage,
		); err != nil {
			logger.Warnf(ctx, "Failed to bind stored image %s to knowledge %s: %v", ref, knowledge.ID, err)
			continue
		}
		bound++
	}
	if bound > 0 {
		logger.Infof(ctx, "Bound %d/%d stored images to knowledge %s", bound, len(images), knowledge.ID)
	}
}
