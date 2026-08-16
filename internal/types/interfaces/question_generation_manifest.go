package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type QuestionGenerationManifestRepository interface {
	WithQuestionGenerationGuard(
		ctx context.Context, key types.QuestionGenerationManifestKey,
		fn func(context.Context) error,
	) error
	GetOrCreateQuestionGenerationManifest(
		ctx context.Context, candidate *types.QuestionGenerationManifest,
	) (manifest *types.QuestionGenerationManifest, created bool, err error)
	GetQuestionGenerationManifest(
		ctx context.Context, key types.QuestionGenerationManifestKey,
	) (*types.QuestionGenerationManifest, error)
	TransitionQuestionGenerationManifest(
		ctx context.Context, key types.QuestionGenerationManifestKey,
		from, to types.QuestionGenerationManifestState,
	) (bool, error)
	DeleteQuestionGenerationManifest(
		ctx context.Context, key types.QuestionGenerationManifestKey,
	) error
}

type QuestionGenerationManifestKBListRepository interface {
	ListQuestionGenerationManifestsByKnowledgeBase(
		ctx context.Context, tenantID uint64, knowledgeBaseID string,
	) ([]*types.QuestionGenerationManifest, error)
}
